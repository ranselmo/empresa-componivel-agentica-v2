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
	"github.com/ranselmo/poc-eci/cell-notificacoes/infra/messaging"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// Tópicos exclusivos deste PBC
const (
	TopicCmdEnviar      = "commands.notificacoes.enviar"
	TopicReplyEnviada   = "replies.notificacoes.enviada"
	TopicEventEnviada   = "events.notificacoes.enviada"
)

// Command recebido do saga-hub
type Command struct {
	CommandID     uuid.UUID      `json:"command_id"`
	CorrelationID uuid.UUID      `json:"correlation_id"`
	SagaID        uuid.UUID      `json:"saga_id"`
	ShardID       string         `json:"shard_id"`
	CommandType   string         `json:"command_type"`
	Payload       map[string]any `json:"payload"`
	IssuedAt      time.Time      `json:"issued_at"`
}

// Reply enviado de volta ao saga-hub
type Reply struct {
	ReplyID       uuid.UUID      `json:"reply_id"`
	CorrelationID uuid.UUID      `json:"correlation_id"`
	SagaID        uuid.UUID      `json:"saga_id"`
	CommandType   string         `json:"command_type"`
	Status        string         `json:"status"`
	Payload       map[string]any `json:"payload"`
	Error         string         `json:"error,omitempty"`
	RepliedAt     time.Time      `json:"replied_at"`
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
		_ = rows.Scan(&id, &dest, &tipo, &canal, &conteudo, &enviadoEm)
		result = append(result, gin.H{
			"id": id, "destinatario_id": dest, "tipo": tipo,
			"canal": canal, "conteudo": conteudo, "enviado_em": enviadoEm,
		})
	}
	return result, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
func (s *Store) Close()                         { s.pool.Close() }

// ── Kafka ──────────────────────────────────────────────────────────
func kafkaBrokers() string {
	b := os.Getenv("KAFKA_BROKERS")
	if b == "" {
		return "kafka:29092"
	}
	return b
}

func newProducer() (*kafka.Producer, error) {
	return kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers(),
		"acks":              "all",
	})
}

func publishReply(prod *kafka.Producer, reply Reply) {
	b, _ := json.Marshal(reply)
	topic := TopicReplyEnviada
	_ = prod.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(reply.CorrelationID.String()),
		Value:          b,
	}, nil)
}

func runConsumer(ctx context.Context, store *Store, prod *kafka.Producer) {
	shardID := os.Getenv("SHARD_ID")
	cellRole := os.Getenv("CELL_ROLE")

	if cellRole == "passive" {
		slog.Info("passive cell — command consumer disabled", "shard", shardID)
		<-ctx.Done()
		return
	}

	groupID := fmt.Sprintf("cell-notificacoes-%s-%s-cmd", shardID, cellRole)
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  kafkaBrokers(),
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true,
	})
	if err != nil {
		slog.Error("kafka consumer", "err", err)
		return
	}
	defer c.Close()

	_ = c.SubscribeTopics([]string{TopicCmdEnviar}, nil)
	slog.Info("command consumer started", "group", groupID)

	failCount := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := c.ReadMessage(200)
			if err != nil {
				if !strings.Contains(strings.ToLower(err.Error()), "timed out") {
					slog.Error("read", "err", err)
				}
				continue
			}
			var cmd Command
			if err := json.Unmarshal(msg.Value, &cmd); err != nil {
				slog.Error("unmarshal cmd", "err", err)
				continue
			}
			if err := processCmd(ctx, store, prod, cmd); err != nil {
				failCount++
				if failCount >= 3 {
					messaging.SendToDLQ(prod, "notificacoes", *msg.TopicPartition.Topic,
						string(msg.Key), msg.Value, err, failCount)
					slog.Warn("sent to DLQ", "topic", *msg.TopicPartition.Topic, "attempts", failCount)
					failCount = 0
				}
			} else {
				failCount = 0
			}
		}
	}
}

func processCmd(ctx context.Context, store *Store, prod *kafka.Producer, cmd Command) error {
	clienteIDStr := fmt.Sprintf("%v", cmd.Payload["cliente_id"])
	destID, _ := uuid.Parse(clienteIDStr)
	tipo, _ := cmd.Payload["tipo"].(string)
	if tipo == "" {
		tipo = "NOTIFICACAO"
	}

	conteudo := fmt.Sprintf("Notificação tipo=%s para cliente=%s", tipo, clienteIDStr)
	notifID := uuid.New()

	slog.Info("enviando notificação", "tipo", tipo, "destinatario", destID)
	if err := store.Salvar(ctx, notifID, destID, tipo, "email", conteudo); err != nil {
		slog.Error("salvar notificacao", "err", err)
		publishReply(prod, Reply{
			ReplyID: uuid.New(), CorrelationID: cmd.CorrelationID,
			SagaID: cmd.SagaID, CommandType: cmd.CommandType,
			Status: "failure", Error: err.Error(), RepliedAt: time.Now().UTC(),
		})
		return err
	}

	publishReply(prod, Reply{
		ReplyID: uuid.New(), CorrelationID: cmd.CorrelationID,
		SagaID: cmd.SagaID, CommandType: cmd.CommandType,
		Status: "success", RepliedAt: time.Now().UTC(),
		Payload: map[string]any{"notificacao_id": notifID.String()},
	})

	// Publica evento democratizado
	evTopic := TopicEventEnviada
	ev, _ := json.Marshal(map[string]any{
		"event_id": uuid.New().String(), "tipo": tipo,
		"destinatario_id": destID.String(), "occurred_at": time.Now().UTC(),
	})
	_ = prod.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &evTopic, Partition: kafka.PartitionAny},
		Value:          ev,
	}, nil)
	return nil
}

// ── OTel ───────────────────────────────────────────────────────────
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

// ── Main ───────────────────────────────────────────────────────────
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer setupOTel(ctx)()

	shardID := os.Getenv("SHARD_ID")
	cellRole := os.Getenv("CELL_ROLE")

	store, err := newStore(ctx)
	if err != nil {
		slog.Error("db", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	prod, err := newProducer()
	if err != nil {
		slog.Error("kafka producer", "err", err)
		os.Exit(1)
	}
	defer func() { prod.Flush(5000); prod.Close() }()

	go runConsumer(ctx, store, prod)

	r := gin.New()
	r.Use(gin.Recovery(), otelgin.Middleware("cell-notificacoes"))
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
		_ = srv.ListenAndServe()
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
	ctx2, c2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer c2()
	_ = srv.Shutdown(ctx2)
}
