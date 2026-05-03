package domain

// Tópicos democratizados publicados por este PBC (qualquer sistema pode consumir)
// Os tópicos de command/reply são gerenciados em infra/messaging — não importar aqui.
const (
	TopicEventPedidoConfirmado = "events.pedidos.confirmado"
	TopicEventPedidoCancelado  = "events.pedidos.cancelado"
)
