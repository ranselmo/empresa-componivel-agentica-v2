package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/ranselmo/poc-eci/data-sync/infra"
)

func passiveDSNs() map[string]string {
	m := make(map[string]string)
	pairs := []struct{ key, env string }{
		{"shard-1:pedidos", "PASSIVE_DSN_SHARD1_PEDIDOS"},
		{"shard-2:pedidos", "PASSIVE_DSN_SHARD2_PEDIDOS"},
		{"shard-3:pedidos", "PASSIVE_DSN_SHARD3_PEDIDOS"},
		{"shard-1:estoque", "PASSIVE_DSN_SHARD1_ESTOQUE"},
		{"shard-2:estoque", "PASSIVE_DSN_SHARD2_ESTOQUE"},
		{"shard-3:estoque", "PASSIVE_DSN_SHARD3_ESTOQUE"},
		{"shard-1:notificacoes", "PASSIVE_DSN_SHARD1_NOTIFICACOES"},
		{"shard-2:notificacoes", "PASSIVE_DSN_SHARD2_NOTIFICACOES"},
		{"shard-3:notificacoes", "PASSIVE_DSN_SHARD3_NOTIFICACOES"},
	}
	for _, p := range pairs {
		if v := os.Getenv(p.env); v != "" {
			m[p.key] = v
		}
	}
	return m
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	app, err := infra.NewApplier(passiveDSNs())
	if err != nil {
		slog.Error("applier", "err", err)
		os.Exit(1)
	}
	go app.Run(ctx)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	srv := &http.Server{Addr: ":9191", Handler: mux}
	go func() { _ = srv.ListenAndServe() }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
}
