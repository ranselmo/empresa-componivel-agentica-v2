package infra

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type HealthWatcher struct {
	reg    *Registry
	client *http.Client
}

func NewHealthWatcher(reg *Registry) *HealthWatcher {
	return &HealthWatcher{
		reg:    reg,
		client: &http.Client{Timeout: 2 * time.Second},
	}
}

func (w *HealthWatcher) Run(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, cell := range w.reg.Snapshot() {
				go w.check(ctx, cell)
			}
		}
	}
}

func (w *HealthWatcher) check(ctx context.Context, cell CellEntry) {
	url := fmt.Sprintf("%s/healthz/ready", cell.BaseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		w.reg.SetHealthy(cell.ShardID, cell.PBC, cell.Role, false)
		return
	}
	resp, err := w.client.Do(req)
	healthy := err == nil && resp != nil && resp.StatusCode == 200
	if resp != nil {
		resp.Body.Close()
	}
	w.reg.SetHealthy(cell.ShardID, cell.PBC, cell.Role, healthy)
	if !healthy {
		slog.Warn("cell unhealthy", "cell", cell.ID, "shard", cell.ShardID, "role", cell.Role)
	}
}
