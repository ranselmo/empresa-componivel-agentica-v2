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
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dbpkg "github.com/ranselmo/poc-eci/cell-estoque/infra/db"
	"github.com/ranselmo/poc-eci/cell-estoque/infra/messaging"
	migrations "github.com/ranselmo/poc-eci/cell-estoque/migrations"
	"github.com/ranselmo/poc-eci/shared/audit"
	"github.com/ranselmo/poc-eci/shared/auth"
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
		svcName = "cell-estoque"
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

// estoqueProcessor implementa messaging.CommandProcessor
type estoqueProcessor struct{ store *dbpkg.Store }

func (ep *estoqueProcessor) ProcessCommand(ctx context.Context, cmd messaging.Command) (messaging.Reply, error) {
	switch cmd.CommandType {
	case "reservar_estoque":
		return ep.reservar(ctx, cmd)
	case "liberar_estoque":
		return ep.liberar(ctx, cmd)
	default:
		return messaging.Reply{}, fmt.Errorf("unknown command: %s", cmd.CommandType)
	}
}

func (ep *estoqueProcessor) reservar(ctx context.Context, cmd messaging.Command) (messaging.Reply, error) {
	itensRaw, _ := cmd.Payload["itens"].([]any)
	if len(itensRaw) == 0 {
		return failReply(cmd, messaging.TopicReplyInsuficiente, "itens required"), nil
	}
	var itens []dbpkg.ReservaItem
	for _, ir := range itensRaw {
		im, _ := ir.(map[string]any)
		prodID, err := uuid.Parse(fmt.Sprintf("%v", im["produto_id"]))
		if err != nil {
			return failReply(cmd, messaging.TopicReplyInsuficiente, "invalid produto_id"), nil
		}
		itens = append(itens, dbpkg.ReservaItem{ProdutoID: prodID, Quantidade: toInt(im["quantidade"])})
	}
	if err := ep.store.ReservarItens(ctx, itens); err != nil {
		return failReply(cmd, messaging.TopicReplyInsuficiente, err.Error()), nil
	}
	return messaging.Reply{
		ReplyID: uuid.New(), CorrelationID: cmd.CorrelationID,
		SagaID: cmd.SagaID, CommandType: cmd.CommandType,
		Status: "success", RepliedAt: time.Now().UTC(),
	}, nil
}

func (ep *estoqueProcessor) liberar(ctx context.Context, cmd messaging.Command) (messaging.Reply, error) {
	itensRaw, _ := cmd.Payload["itens"].([]any)
	if len(itensRaw) == 0 {
		return failReply(cmd, "", "itens required"), nil
	}
	var itens []dbpkg.ReservaItem
	for _, ir := range itensRaw {
		im, _ := ir.(map[string]any)
		prodID, err := uuid.Parse(fmt.Sprintf("%v", im["produto_id"]))
		if err != nil {
			return failReply(cmd, "", "invalid produto_id"), nil
		}
		itens = append(itens, dbpkg.ReservaItem{ProdutoID: prodID, Quantidade: toInt(im["quantidade"])})
	}
	if err := ep.store.LiberarItens(ctx, itens); err != nil {
		return messaging.Reply{}, fmt.Errorf("liberar itens: %w", err)
	}
	return messaging.Reply{
		ReplyID: uuid.New(), CorrelationID: cmd.CorrelationID,
		SagaID: cmd.SagaID, CommandType: cmd.CommandType,
		Status: "success", RepliedAt: time.Now().UTC(),
	}, nil
}

func failReply(cmd messaging.Command, _ string, reason string) messaging.Reply {
	return messaging.Reply{
		ReplyID: uuid.New(), CorrelationID: cmd.CorrelationID,
		SagaID: cmd.SagaID, CommandType: cmd.CommandType,
		Status: "failure", Error: reason, RepliedAt: time.Now().UTC(),
	}
}

func toInt(v any) int {
	if n, ok := v.(float64); ok {
		return int(n)
	}
	return 0
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer setupOTel(ctx)()

	shardID := os.Getenv("SHARD_ID")
	cellRole := os.Getenv("CELL_ROLE")

	store, err := dbpkg.New(ctx)
	if err != nil {
		slog.Error("db connect", "err", err)
		os.Exit(1)
	}
	defer store.Close()

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
	if err := store.Seed(ctx); err != nil {
		slog.Warn("seed", "err", err)
	}
	slog.Info("migrations applied")

	prod, err := messaging.NewProducer()
	if err != nil {
		slog.Error("kafka producer", "err", err)
		os.Exit(1)
	}
	defer prod.Close()

	proc := &estoqueProcessor{store: store}
	cons, err := messaging.NewConsumer(proc, prod)
	if err != nil {
		slog.Error("kafka consumer", "err", err)
		os.Exit(1)
	}
	go cons.Run(ctx)

	al := audit.New("cell-estoque", shardID, prod.KafkaProducer())
	jwtMW := auth.Middleware()

	r := gin.New()
	r.Use(gin.Recovery(), otelgin.Middleware("cell-estoque"), middleware.RateLimit())
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
		estoque := r.Group("/estoque")
		estoque.GET("/stats", func(c *gin.Context) {
			st, err := store.Stats(c.Request.Context())
			if err != nil {
				c.JSON(500, gin.H{"error": err.Error()})
				return
			}
			c.JSON(200, st)
		})
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
		estoque.PUT("/:id/repor", jwtMW, func(c *gin.Context) {
			id, err := uuid.Parse(c.Param("id"))
			if err != nil {
				c.JSON(400, gin.H{"error": "id inválido"})
				return
			}
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
			_ = p.Repor(req.Quantidade)
			_ = store.Salvar(c.Request.Context(), p)

			actorID, _ := c.Get("actor_id")
			if actorID == nil {
				actorID = "anonymous"
			}
			al.Log(c.Request.Context(), "repor_estoque", "produto", id.String(),
				fmt.Sprintf("%v", actorID),
				map[string]any{"quantidade": req.Quantidade, "novo_total": p.QuantidadeDisponivel})

			c.JSON(200, gin.H{
				"mensagem":              fmt.Sprintf("Estoque reposto. Novo total: %d", p.QuantidadeDisponivel),
				"quantidade_disponivel": p.QuantidadeDisponivel,
			})
		})
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	srv := &http.Server{Addr: ":" + port, Handler: r}
	go func() {
		slog.Info("cell-estoque listening", "port", port, "shard", shardID, "role", cellRole)
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
