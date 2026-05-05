package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/ranselmo/poc-eci/cell-notificacoes/infra/db"
	"github.com/ranselmo/poc-eci/cell-notificacoes/infra/messaging"
	migrations "github.com/ranselmo/poc-eci/cell-notificacoes/migrations"
	"github.com/ranselmo/poc-eci/shared/audit"
	"github.com/ranselmo/poc-eci/shared/middleware"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

func setupOTel(ctx context.Context) func() {
	svcName := os.Getenv("OTEL_SERVICE_NAME")
	if svcName == "" {
		svcName = "cell-notificacoes"
	}
	ep := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if ep == "" {
		ep = "http://jaeger:4317"
	}
	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(ep), otlptracegrpc.WithInsecure())
	if err != nil {
		return func() {}
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(svcName))),
	)
	otel.SetTracerProvider(tp)
	return func() { _ = tp.Shutdown(context.Background()) }
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer setupOTel(ctx)()

	shardID := os.Getenv("SHARD_ID")
	cellRole := os.Getenv("CELL_ROLE")

	driver, err := iofs.New(migrations.FS, ".")
	if err != nil {
		slog.Error("migrate source", "err", err)
		os.Exit(1)
	}
	m, err := migrate.NewWithSourceInstance("iofs", driver, os.Getenv("DATABASE_URL"))
	if err != nil {
		slog.Error("migrate new", "err", err)
		os.Exit(1)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		slog.Error("migrate up", "err", err)
		os.Exit(1)
	}
	slog.Info("migrations applied")

	store, err := db.New(ctx)
	if err != nil {
		slog.Error("db", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	prod, err := messaging.NewProducer()
	if err != nil {
		slog.Error("kafka producer", "err", err)
		os.Exit(1)
	}
	defer prod.Close()

	al := audit.New("cell-notificacoes", shardID, prod.KafkaProducer())
	cons, err := messaging.NewConsumer(store, prod, al)
	if err != nil {
		slog.Error("kafka consumer", "err", err)
		os.Exit(1)
	}
	go cons.Run(ctx)

	r := gin.New()
	r.Use(gin.Recovery(), otelgin.Middleware("cell-notificacoes"), middleware.RateLimit())
	r.Use(func(c *gin.Context) {
		reqCtx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer cancel()
		c.Request = c.Request.WithContext(reqCtx)
		c.Next()
	})
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.GET("/healthz/live", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok", "shard": shardID, "role": cellRole})
	})
	r.GET("/healthz/ready", func(c *gin.Context) {
		if err := store.Ping(c.Request.Context()); err != nil {
			c.JSON(503, gin.H{"status": "db unhealthy", "error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"status": "ok", "shard": shardID, "role": cellRole})
	})

	if cellRole == "passive" {
		for _, method := range []string{"POST", "PUT", "PATCH", "DELETE"} {
			r.Handle(method, "/*path", func(c *gin.Context) {
				c.JSON(503, gin.H{
					"error":     "cell is passive — writes forbidden",
					"cell_role": "passive",
					"shard_id":  shardID,
				})
			})
		}
		slog.Info("passive cell — write routes blocked", "shard", shardID)
	} else {
		n := r.Group("/notificacoes")
		n.GET("/", func(c *gin.Context) {
			list, err := store.Listar(c.Request.Context())
			if err != nil {
				c.JSON(500, gin.H{"error": err.Error()})
				return
			}
			if list == nil {
				list = []gin.H{}
			}
			c.JSON(200, list)
		})
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	srv := &http.Server{Addr: ":" + port, Handler: r}
	go func() {
		slog.Info("cell-notificacoes listening", "port", port, "shard", shardID, "role", cellRole)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server", "err", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
	ctx2, c2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer c2()
	_ = srv.Shutdown(ctx2)
}
