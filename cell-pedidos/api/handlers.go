package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/ranselmo/poc-eci/cell-pedidos/domain"
	"github.com/ranselmo/poc-eci/cell-pedidos/infra/audit"
	"github.com/ranselmo/poc-eci/cell-pedidos/infra/auth"
	"github.com/ranselmo/poc-eci/cell-pedidos/infra/db"
	"github.com/ranselmo/poc-eci/cell-pedidos/infra/messaging"
)

type Handler struct {
	store *db.Store
	prod  *messaging.Producer
	audit *audit.Logger
}

func NewHandler(store *db.Store, prod *messaging.Producer, al *audit.Logger) *Handler {
	return &Handler{store: store, prod: prod, audit: al}
}

// ProcessCommand implementa messaging.CommandProcessor — recebe comandos do saga-hub via Kafka
func (h *Handler) ProcessCommand(ctx context.Context, cmd messaging.Command) (messaging.Reply, error) {
	switch cmd.CommandType {
	case "criar_pedido":
		return h.cmdCriarPedido(ctx, cmd)
	case "cancelar_pedido":
		return h.cmdCancelarPedido(ctx, cmd)
	default:
		return messaging.Reply{}, fmt.Errorf("unknown command: %s", cmd.CommandType)
	}
}

func (h *Handler) cmdCriarPedido(ctx context.Context, cmd messaging.Command) (messaging.Reply, error) {
	clienteID, err := uuid.Parse(fmt.Sprintf("%v", cmd.Payload["cliente_id"]))
	if err != nil {
		return failReply(cmd, "invalid cliente_id"), nil
	}

	itensRaw, _ := cmd.Payload["itens"].([]any)
	if len(itensRaw) == 0 {
		return failReply(cmd, "itens required"), nil
	}
	itens := make([]domain.ItemPedido, 0, len(itensRaw))
	for _, ir := range itensRaw {
		im, _ := ir.(map[string]any)
		prodID, _ := uuid.Parse(fmt.Sprintf("%v", im["produto_id"]))
		qty := toInt(im["quantidade"])
		preco := toFloat(im["preco_unitario"])
		itens = append(itens, domain.ItemPedido{ProdutoID: prodID, Quantidade: qty, PrecoUnit: preco})
	}

	pedido, err := domain.NewPedido(clienteID, itens)
	if err != nil {
		return failReply(cmd, err.Error()), nil
	}
	if err := h.store.Salvar(ctx, pedido); err != nil {
		return messaging.Reply{}, fmt.Errorf("save pedido: %w", err)
	}

	return messaging.Reply{
		ReplyID: uuid.New(), CorrelationID: cmd.CorrelationID,
		SagaID: cmd.SagaID, CommandType: cmd.CommandType,
		Status: "success", RepliedAt: time.Now().UTC(),
		Payload: map[string]any{"pedido_id": pedido.ID.String()},
	}, nil
}

func (h *Handler) cmdCancelarPedido(ctx context.Context, cmd messaging.Command) (messaging.Reply, error) {
	pedidoIDStr, _ := cmd.Payload["pedido_id"].(string)
	if pedidoIDStr == "" {
		return failReply(cmd, "pedido_id required"), nil
	}
	pedidoID, err := uuid.Parse(pedidoIDStr)
	if err != nil {
		return failReply(cmd, "invalid pedido_id"), nil
	}
	pedido, err := h.store.BuscarPorID(ctx, pedidoID)
	if err != nil {
		return failReply(cmd, "pedido not found"), nil
	}
	_ = pedido.Cancelar()
	_ = h.store.Salvar(ctx, pedido)

	return messaging.Reply{
		ReplyID: uuid.New(), CorrelationID: cmd.CorrelationID,
		SagaID: cmd.SagaID, CommandType: cmd.CommandType,
		Status: "success", RepliedAt: time.Now().UTC(),
		Payload: map[string]any{"pedido_id": pedidoID.String()},
	}, nil
}

func failReply(cmd messaging.Command, reason string) messaging.Reply {
	return messaging.Reply{
		ReplyID: uuid.New(), CorrelationID: cmd.CorrelationID,
		SagaID: cmd.SagaID, CommandType: cmd.CommandType,
		Status: "failure", Error: reason, RepliedAt: time.Now().UTC(),
	}
}

type itemReq struct {
	ProdutoID  string  `json:"produto_id" binding:"required"`
	Quantidade int     `json:"quantidade" binding:"required,min=1"`
	PrecoUnit  float64 `json:"preco_unitario" binding:"required,gt=0"`
}

type criarPedidoReq struct {
	ClienteID string    `json:"cliente_id" binding:"required"`
	Itens     []itemReq `json:"itens" binding:"required,min=1"`
}

func (h *Handler) RegisterRoutes(r *gin.Engine) {
	jwtMW := auth.Middleware()
	g := r.Group("/pedidos")
	g.GET("/", h.ListarPedidos)
	g.GET("/stats", h.Stats)
	g.GET("/:id", h.BuscarPedido)
	g.POST("/", jwtMW, h.CriarPedido)
}

func (h *Handler) Stats(c *gin.Context) {
	st, err := h.store.Stats(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, st)
}

func (h *Handler) CriarPedido(c *gin.Context) {
	var req criarPedidoReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": err.Error()})
		return
	}

	clienteID, err := uuid.Parse(req.ClienteID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cliente_id inválido"})
		return
	}

	itens := make([]domain.ItemPedido, len(req.Itens))
	for i, it := range req.Itens {
		pid, err := uuid.Parse(it.ProdutoID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "produto_id inválido"})
			return
		}
		itens[i] = domain.ItemPedido{ProdutoID: pid, Quantidade: it.Quantidade, PrecoUnit: it.PrecoUnit}
	}

	pedido, err := domain.NewPedido(clienteID, itens)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": err.Error()})
		return
	}

	if err := h.store.Salvar(c.Request.Context(), pedido); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "erro ao salvar pedido"})
		return
	}

	actorID, _ := c.Get("actor_id")
	if actorID == nil {
		actorID = "anonymous"
	}
	h.audit.Log(c.Request.Context(), "criar_pedido", "pedido", pedido.ID.String(),
		fmt.Sprintf("%v", actorID),
		map[string]any{"cliente_id": req.ClienteID, "valor_total": pedido.ValorTotal()})

	c.JSON(http.StatusCreated, gin.H{
		"pedido_id":   pedido.ID,
		"status":      pedido.Status,
		"valor_total": pedido.ValorTotal(),
		"mensagem":    "Pedido criado. Use POST /saga/pedido para o fluxo completo com SAGA.",
	})
}

func (h *Handler) BuscarPedido(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id inválido"})
		return
	}
	p, err := h.store.BuscarPorID(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "pedido não encontrado"})
		return
	}
	c.JSON(http.StatusOK, pedidoResp(p))
}

func (h *Handler) ListarPedidos(c *gin.Context) {
	pedidos, err := h.store.Listar(c.Request.Context(), 20)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	result := make([]any, len(pedidos))
	for i, p := range pedidos {
		result[i] = pedidoResp(p)
	}
	c.JSON(http.StatusOK, result)
}

func pedidoResp(p *domain.Pedido) gin.H {
	itens := make([]gin.H, len(p.Itens))
	for i, it := range p.Itens {
		itens[i] = gin.H{"produto_id": it.ProdutoID, "quantidade": it.Quantidade, "preco_unitario": it.PrecoUnit}
	}
	return gin.H{
		"id": p.ID, "cliente_id": p.ClienteID,
		"status": p.Status, "valor_total": p.ValorTotal(),
		"itens": itens, "criado_em": p.CriadoEm, "atualizado_em": p.AtualizadoEm,
	}
}

func toInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	}
	return 0
}

func toFloat(v any) float64 {
	if n, ok := v.(float64); ok {
		return n
	}
	return 0
}
