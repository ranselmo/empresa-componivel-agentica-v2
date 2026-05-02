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
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/ranselmo/poc-eci/cell-estoque/infra/db"
	"github.com/ranselmo/poc-eci/cell-estoque/infra/messaging"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

func setupOTel(ctx context.Context) func() {
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
		sdktrace.WithResource(resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName("cell-estoque"))),
	)
	otel.SetTracerProvider(tp)
	return func() { tp.Shutdown(ctx) }
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer setupOTel(ctx)()

	store, err := db.New(ctx)
	if err != nil {
		slog.Error("db connect", "err", err)
		os.Exit(1)
	}
	defer store.Close()
	store.Migrate(ctx)

	prod, _ := messaging.NewProducer()
	defer prod.Close()

	cons, err := messaging.NewConsumer(store, prod)
	if err != nil {
		slog.Error("kafka consumer", "err", err)
		os.Exit(1)
	}
	go cons.Run(ctx)

	r := gin.New()
	r.Use(gin.Recovery(), otelgin.Middleware("cell-estoque"))
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok", "cell": "estoque"})
	})

	estoque := r.Group("/estoque")

	estoque.GET("/", func(c *gin.Context) {
		produtos, err := store.Listar(c.Request.Context())
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		result := make([]gin.H, len(produtos))
		for i, p := range produtos {
			result[i] = gin.H{
				"id": p.ID, "nome": p.Nome,
				"quantidade_disponivel": p.QuantidadeDisponivel,
				"preco": p.Preco, "atualizado_em": p.AtualizadoEm,
			}
		}
		c.JSON(200, result)
	})

	estoque.GET("/:id", func(c *gin.Context) {
		id, err := uuid.Parse(c.Param("id"))
		if err != nil {
			c.JSON(400, gin.H{"error": "id inválido"})
			return
		}
		p, err := store.BuscarPorID(c.Request.Context(), id)
		if err != nil {
			c.JSON(404, gin.H{"error": "produto não encontrado"})
			return
		}
		c.JSON(200, gin.H{
			"id": p.ID, "nome": p.Nome,
			"quantidade_disponivel": p.QuantidadeDisponivel,
			"preco": p.Preco, "atualizado_em": p.AtualizadoEm,
		})
	})

	estoque.PUT("/:id/repor", func(c *gin.Context) {
		id, _ := uuid.Parse(c.Param("id"))
		var req struct {
			Quantidade int `json:"quantidade" binding:"required,min=1"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		p, err := store.BuscarPorID(c.Request.Context(), id)
		if err != nil {
			c.JSON(404, gin.H{"error": "produto não encontrado"})
			return
		}
		p.Repor(req.Quantidade)
		store.Salvar(c.Request.Context(), p)
		c.JSON(200, gin.H{
			"mensagem":              fmt.Sprintf("Estoque reposto. Novo total: %d", p.QuantidadeDisponivel),
			"quantidade_disponivel": p.QuantidadeDisponivel,
		})
	})

	estoque.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok", "cell": "estoque", "version": "1.0.0"})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	srv := &http.Server{Addr: ":" + port, Handler: r}
	go func() {
		slog.Info("cell-estoque listening", "port", port)
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
	srv.Shutdown(ctx2)
}
