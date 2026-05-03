package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/ranselmo/poc-eci/cell-pedidos/api"
	"github.com/ranselmo/poc-eci/cell-pedidos/infra/db"
	"github.com/ranselmo/poc-eci/cell-pedidos/infra/messaging"
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
		svcName = "cell-pedidos"
	}
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://jaeger:4317"
	}
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		slog.Warn("otel exporter error", "err", err)
		return func() {}
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(svcName),
		)),
	)
	otel.SetTracerProvider(tp)
	return func() { _ = tp.Shutdown(context.Background()) }
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdown := setupOTel(ctx)
	defer shutdown()

	shardID := os.Getenv("SHARD_ID")
	cellRole := os.Getenv("CELL_ROLE")

	store, err := db.New(ctx)
	if err != nil {
		slog.Error("db connect", "err", err)
		os.Exit(1)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		slog.Error("migrate", "err", err)
		os.Exit(1)
	}

	prod, err := messaging.NewProducer()
	if err != nil {
		slog.Error("kafka producer", "err", err)
		os.Exit(1)
	}
	defer prod.Close()

	h := api.NewHandler(store, prod)

	cons, err := messaging.NewConsumer(h, prod)
	if err != nil {
		slog.Error("kafka consumer", "err", err)
		os.Exit(1)
	}
	go cons.Run(ctx)

	r := gin.New()
	r.Use(gin.Recovery(), otelgin.Middleware("cell-pedidos"))
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
		h.RegisterRoutes(r)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	srv := &http.Server{Addr: fmt.Sprintf(":%s", port), Handler: r}
	go func() {
		slog.Info("cell-pedidos listening", "port", port, "shard", shardID, "role", cellRole)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	slog.Info("shutting down")
	cancel()
	ctx2, c2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer c2()
	_ = srv.Shutdown(ctx2)
}
