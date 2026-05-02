package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/ranselmo/poc-eci/cell-pedidos/domain"
	"github.com/ranselmo/poc-eci/cell-pedidos/infra/db"
	"github.com/ranselmo/poc-eci/cell-pedidos/infra/messaging"
)

type Handler struct {
	store *db.Store
	prod  *messaging.Producer
}

func NewHandler(store *db.Store, prod *messaging.Producer) *Handler {
	return &Handler{store: store, prod: prod}
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
	g := r.Group("/pedidos")
	g.POST("/", h.CriarPedido)
	g.GET("/", h.ListarPedidos)
	g.GET("/:id", h.BuscarPedido)
	g.GET("/health", h.Health)
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

	itensPub := make([]domain.ItemEvento, len(pedido.Itens))
	for i, it := range pedido.Itens {
		itensPub[i] = domain.ItemEvento{ProdutoID: it.ProdutoID, Quantidade: it.Quantidade, PrecoUnit: it.PrecoUnit}
	}
	h.prod.Publish(domain.TopicPedidoCriado, pedido.ID.String(), domain.PedidoCriado{
		EventID: uuid.New(), EventType: "PedidoCriado",
		PedidoID: pedido.ID, ClienteID: pedido.ClienteID,
		Itens: itensPub, ValorTotal: pedido.ValorTotal(),
		Timestamp: time.Now().UTC(),
	})

	c.JSON(http.StatusCreated, gin.H{
		"pedido_id":   pedido.ID,
		"status":      pedido.Status,
		"valor_total": pedido.ValorTotal(),
		"mensagem":    "Pedido criado. Aguardando confirmação de estoque via SAGA.",
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

func (h *Handler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "cell": "pedidos", "version": "1.0.0"})
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
