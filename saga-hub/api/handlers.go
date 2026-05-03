package api

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/ranselmo/poc-eci/saga-hub/infra/db"
	"github.com/ranselmo/poc-eci/saga-hub/orchestrator"
)

type Handler struct {
	saga  *orchestrator.PedidoSaga
	store *db.SagaStore
}

func New(saga *orchestrator.PedidoSaga, store *db.SagaStore) *Handler {
	return &Handler{saga: saga, store: store}
}

func (h *Handler) Register(r *gin.Engine) {
	r.POST("/saga/pedido", h.iniciar)
	r.GET("/saga/:id", h.consultar)
	r.GET("/healthz/live", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })
	r.GET("/healthz/ready", h.ready)
}

type IniciarRequest struct {
	ClienteID string         `json:"cliente_id" binding:"required"`
	ShardID   string         `json:"shard_id"   binding:"required"`
	Payload   map[string]any `json:"payload"    binding:"required"`
}

func (h *Handler) iniciar(c *gin.Context) {
	var req IniciarRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	clienteID, err := uuid.Parse(req.ClienteID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cliente_id"})
		return
	}
	saga, err := h.saga.Start(c.Request.Context(), clienteID, req.ShardID, req.Payload)
	if err != nil {
		slog.Error("start saga", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{
		"saga_id":        saga.ID,
		"correlation_id": saga.CorrelationID,
		"status":         saga.Status,
	})
}

func (h *Handler) consultar(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	saga, err := h.store.FindByID(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "saga not found"})
		return
	}
	c.JSON(200, gin.H{
		"saga_id":      saga.ID,
		"status":       saga.Status,
		"current_step": saga.CurrentStep,
		"shard_id":     saga.ShardID,
		"created_at":   saga.CreatedAt,
		"updated_at":   saga.UpdatedAt,
	})
}

func (h *Handler) ready(c *gin.Context) {
	if err := h.store.Ping(c.Request.Context()); err != nil {
		c.JSON(503, gin.H{"status": "db unhealthy", "error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"status": "ok"})
}
