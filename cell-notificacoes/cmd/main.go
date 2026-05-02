package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// ── Tópicos ────────────────────────────────────────────────────────
const (
	TopicPedidoConfirmado   = "dominio.pedido.confirmado"
	TopicPedidoCancelado    = "dominio.pedido.cancelado"
	TopicNotificacaoEnviada = "dominio.notificacao.enviada"
)

// ── Eventos consumidos ─────────────────────────────────────────────
type PedidoConfirmado struct {
	EventID    uuid.UUID `json:"event_id"`
	EventType  string    `json:"event_type"`
	PedidoID   uuid.UUID `json:"pedido_id"`
	ClienteID  uuid.UUID `json:"cliente_id"`
	ValorTotal float64   `json:"valor_total"`
	Timestamp  time.Time `json:"timestamp"`
}
type PedidoCancelado struct {
	EventID   uuid.UUID `json:"event_id"`
	EventType string    `json:"event_type"`
	PedidoID  uuid.UUID `json:"pedido_id"`
	Motivo    string    `json:"motivo"`
	Timestamp time.Time `json:"timestamp"`
}
type NotificacaoEnviada struct {
	EventID       uuid.UUID `json:"event_id"`
	EventType     string    `json:"event_type"`
	DestinatarioID uuid.UUID `json:"destinatario_id"`
	Tipo          string    `json:"tipo"`
	Canal         string    `json:"canal"`
	Timestamp     time.Time `json:"timestamp"`
}

// ── DB ─────────────────────────────────────────────────────────────
type Store struct{ pool *pgxpool.Pool }

func newStore(ctx context.Context) (*Store, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://notificacoes:notif123@localhost:5435/notificacoes?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS notificacoes (
			id              UUID PRIMARY KEY,
			destinatario_id UUID NOT NULL,
			tipo            TEXT NOT NULL,
			canal           TEXT NOT NULL,
			conteudo        TEXT NOT NULL,
			enviado_em      TIMESTAMPTZ NOT NULL
		);
	`)
	return &Store{pool: pool}, err
}

func (s *Store) Salvar(ctx context.Context, id, destID uuid.UUID, tipo, canal, conteudo string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO notificacoes (id, destinatario_id, tipo, canal, conteudo, enviado_em)
		 VALUES ($1,$2,$3,$4,$5,$6)`,
		id, destID, tipo, canal, conteudo, time.Now().UTC())
	return err
}

func (s *Store) Listar(ctx context.Context) ([]gin.H, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, destinatario_id, tipo, canal, conteudo, enviado_em
		 FROM notificacoes ORDER BY enviado_em DESC LIMIT 50`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []gin.H
	for rows.Next() {
		var id, dest uuid.UUID
		var tipo, canal, conteudo string
		var enviadoEm time.Time
		rows.Scan(&id, &dest, &tipo, &canal, &conteudo, &enviadoEm)
		result = append(result, gin.H{
			"id": id, "destinatario_id": dest, "tipo": tipo,
			"canal": canal, "conteudo": conteudo, "enviado_em": enviadoEm,
		})
	}
	return result, nil
}

// ── Kafka ──────────────────────────────────────────────────────────
func kafkaBrokers() string {
	b := os.Getenv("KAFKA_BROKERS")
	if b == "" {
		return "localhost:9092"
	}
	return b
}

func newProducer() (*kafka.Producer, error) {
	return kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers(),
		"acks":              "all",
	})
}

func runConsumer(ctx context.Context, store *Store, prod *kafka.Producer) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  kafkaBrokers(),
		"group.id":           "cell-notificacoes-group",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true,
	})
	if err != nil {
		slog.Error("kafka consumer", "err", err)
		return
	}
	defer c.Close()

	c.SubscribeTopics([]string{TopicPedidoConfirmado, TopicPedidoCancelado}, nil)
	slog.Info("consumer cell-notificacoes aguardando eventos")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := c.ReadMessage(200)
			if err != nil {
				if !strings.Contains(err.Error(), "timed out") {
					slog.Error("read", "err", err)
				}
				continue
			}
			processMsg(ctx, store, prod, msg)
		}
	}
}

func processMsg(ctx context.Context, store *Store, prod *kafka.Producer, msg *kafka.Message) {
	topic := *msg.TopicPartition.Topic
	var destID uuid.UUID
	var tipo, conteudo string

	switch topic {
	case TopicPedidoConfirmado:
		var ev PedidoConfirmado
		json.Unmarshal(msg.Value, &ev)
		destID = ev.ClienteID
		tipo = "PEDIDO_CONFIRMADO"
		conteudo = fmt.Sprintf("Pedido %s confirmado! Total: R$%.2f", ev.PedidoID, ev.ValorTotal)

	case TopicPedidoCancelado:
		var ev PedidoCancelado
		json.Unmarshal(msg.Value, &ev)
		destID = ev.PedidoID // usa pedido_id como proxy do cliente na demo
		tipo = "PEDIDO_CANCELADO"
		conteudo = fmt.Sprintf("Pedido %s cancelado. Motivo: %s", ev.PedidoID, ev.Motivo)

	default:
		return
	}

	notifID := uuid.New()
	slog.Info("enviando notificação", "tipo", tipo, "destinatario", destID)
	slog.Info("[NOTIFICACAO]", "conteudo", conteudo)

	store.Salvar(ctx, notifID, destID, tipo, "email", conteudo)

	body, _ := json.Marshal(NotificacaoEnviada{
		EventID: uuid.New(), EventType: "NotificacaoEnviada",
		DestinatarioID: destID, Tipo: tipo, Canal: "email",
		Timestamp: time.Now().UTC(),
	})
	prod.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: strPtr(TopicNotificacaoEnviada), Partition: kafka.PartitionAny},
		Value:          body,
	}, nil)
}

func strPtr(s string) *string { return &s }

// ── OTel ───────────────────────────────────────────────────────────
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
		sdktrace.WithResource(resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName("cell-notificacoes"))),
	)
	otel.SetTracerProvider(tp)
	return func() { tp.Shutdown(ctx) }
}

// ── Main ───────────────────────────────────────────────────────────
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer setupOTel(ctx)()

	store, err := newStore(ctx)
	if err != nil {
		slog.Error("db", "err", err)
		os.Exit(1)
	}

	prod, err := newProducer()
	if err != nil {
		slog.Error("kafka producer", "err", err)
		os.Exit(1)
	}
	defer func() { prod.Flush(5000); prod.Close() }()

	go runConsumer(ctx, store, prod)

	r := gin.New()
	r.Use(gin.Recovery(), otelgin.Middleware("cell-notificacoes"))
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok", "cell": "notificacoes", "version": "1.0.0"})
	})

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
	n.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok", "cell": "notificacoes"})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	srv := &http.Server{Addr: ":" + port, Handler: r}
	go func() {
		slog.Info("cell-notificacoes listening", "port", port)
		srv.ListenAndServe()
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
	ctx2, c2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer c2()
	srv.Shutdown(ctx2)
}
