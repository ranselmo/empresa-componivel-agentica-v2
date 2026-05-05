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
	sagaapi "github.com/ranselmo/poc-eci/saga-hub/api"
	"github.com/ranselmo/poc-eci/saga-hub/infra/db"
	"github.com/ranselmo/poc-eci/saga-hub/infra/messaging"
	migrations "github.com/ranselmo/poc-eci/saga-hub/migrations"
	"github.com/ranselmo/poc-eci/saga-hub/orchestrator"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

func setupOTel(ctx context.Context, svcName string) func() {
	ep := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if ep == "" {
		ep = "http://jaeger:4317"
	}
	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(ep), otlptracegrpc.WithInsecure())
	if err != nil {
		slog.Warn("otel init", "err", err)
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
	defer setupOTel(ctx, "saga-hub")()

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

	store, err := db.NewSagaStore(ctx)
	if err != nil {
		slog.Error("db", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	prod, err := messaging.NewProducer()
	if err != nil {
		slog.Error("producer", "err", err)
		os.Exit(1)
	}
	defer prod.Close()

	orch := orchestrator.NewPedidoSaga(store, prod)

	cons, err := messaging.NewConsumer(orch.HandleReply)
	if err != nil {
		slog.Error("consumer", "err", err)
		os.Exit(1)
	}
	go cons.Run(ctx)

	r := gin.New()
	r.Use(gin.Recovery(), otelgin.Middleware("saga-hub"))
	r.Use(func(c *gin.Context) {
		reqCtx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer cancel()
		c.Request = c.Request.WithContext(reqCtx)
		c.Next()
	})
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	sagaapi.New(orch, store).Register(r)

	port := os.Getenv("PORT")
	if port == "" {
		port = "9090"
	}
	srv := &http.Server{Addr: ":" + port, Handler: r}
	go func() {
		slog.Info("saga-hub up", "port", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server", "err", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
	ctx2, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()
	_ = srv.Shutdown(ctx2)
}
