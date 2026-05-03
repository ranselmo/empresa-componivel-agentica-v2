package infra

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ranselmo/poc-eci/shard-router/domain"
)

type CellEntry struct {
	ID      string
	ShardID domain.ShardID
	PBC     string
	Role    domain.CellRole
	BaseURL string
	Healthy bool
	Updated time.Time
}

type Registry struct {
	mu    sync.RWMutex
	cells map[string]*CellEntry // key: "shard-1:pedidos:active"
}

func NewRegistry() *Registry {
	r := &Registry{cells: make(map[string]*CellEntry)}
	pbcs := []string{"pedidos", "estoque", "notificacoes"}
	roles := []string{domain.RoleActive, domain.RolePassive}
	for i := 1; i <= domain.TotalShards; i++ {
		for _, pbc := range pbcs {
			for _, role := range roles {
				envKey := fmt.Sprintf("SHARD%d_%s_%s_URL",
					i, strings.ToUpper(pbc), strings.ToUpper(role))
				url := os.Getenv(envKey)
				if url == "" {
					continue
				}
				key := fmt.Sprintf("shard-%d:%s:%s", i, pbc, role)
				r.cells[key] = &CellEntry{
					ID:      fmt.Sprintf("cell-%s-s%d-%s", pbc, i, role),
					ShardID: fmt.Sprintf("shard-%d", i),
					PBC:     pbc,
					Role:    role,
					BaseURL: url,
					Healthy: true,
					Updated: time.Now(),
				}
			}
		}
	}
	return r
}

// ActiveCell retorna célula ativa; se down, faz failover para passiva.
func (r *Registry) ActiveCell(shardID, pbc string) (*CellEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e := r.cells[shardID+":"+pbc+":active"]; e != nil && e.Healthy {
		return e, false // false = não é failover
	}
	if e := r.cells[shardID+":"+pbc+":passive"]; e != nil && e.Healthy {
		return e, true // true = failover aconteceu
	}
	return nil, false
}

func (r *Registry) SetHealthy(shardID, pbc, role string, healthy bool) {
	key := shardID + ":" + pbc + ":" + role
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.cells[key]; e != nil {
		e.Healthy = healthy
		e.Updated = time.Now()
	}
}

func (r *Registry) Snapshot() []CellEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]CellEntry, 0, len(r.cells))
	for _, e := range r.cells {
		out = append(out, *e)
	}
	return out
}
