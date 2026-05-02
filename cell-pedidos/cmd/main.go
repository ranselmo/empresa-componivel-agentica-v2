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
			semconv.ServiceName("cell-pedidos"),
		)),
	)
	otel.SetTracerProvider(tp)
	return func() { tp.Shutdown(ctx) }
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdown := setupOTel(ctx)
	defer shutdown()

	// Database
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

	// Kafka producer
	prod, err := messaging.NewProducer()
	if err != nil {
		slog.Error("kafka producer", "err", err)
		os.Exit(1)
	}
	defer prod.Close()

	// Kafka consumer (goroutine)
	cons, err := messaging.NewConsumer(store, prod)
	if err != nil {
		slog.Error("kafka consumer", "err", err)
		os.Exit(1)
	}
	go cons.Run(ctx)

	// HTTP server
	r := gin.New()
	r.Use(gin.Recovery(), otelgin.Middleware("cell-pedidos"))
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "cell": "pedidos"})
	})

	h := api.NewHandler(store, prod)
	h.RegisterRoutes(r)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	srv := &http.Server{Addr: fmt.Sprintf(":%s", port), Handler: r}
	go func() {
		slog.Info("cell-pedidos listening", "port", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	slog.Info("shutting down")
	ctx2, c2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer c2()
	srv.Shutdown(ctx2)
	cancel()
}
