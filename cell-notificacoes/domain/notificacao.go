package domain

import (
	"time"

	"github.com/google/uuid"
)

type Notificacao struct {
	ID             uuid.UUID
	DestinatarioID uuid.UUID
	Tipo           string
	Canal          string
	Conteudo       string
	EnviadoEm      time.Time
}

func NewNotificacao(destinatarioID uuid.UUID, tipo, canal, conteudo string) *Notificacao {
	return &Notificacao{
		ID:             uuid.New(),
		DestinatarioID: destinatarioID,
		Tipo:           tipo,
		Canal:          canal,
		Conteudo:       conteudo,
		EnviadoEm:      time.Now().UTC(),
	}
}
