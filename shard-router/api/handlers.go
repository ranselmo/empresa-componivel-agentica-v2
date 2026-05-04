package api

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ranselmo/poc-eci/shard-router/domain"
	"github.com/ranselmo/poc-eci/shard-router/infra"
	"github.com/ranselmo/poc-eci/shard-router/infra/resilience"
)

var (
	reqTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shard_router_requests_total",
	}, []string{"shard", "pbc", "role"})

	reqDur = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "shard_router_duration_seconds",
		Buckets: []float64{.001, .005, .01, .05, .1, .5},
	}, []string{"shard", "pbc"})

	failoverTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shard_router_failover_total",
	}, []string{"shard", "pbc"})

	cellHealth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "shard_router_cell_health",
		Help: "1=healthy 0=unhealthy",
	}, []string{"shard", "pbc", "role", "cell_id"})
)

type Handler struct {
	reg      *infra.Registry
	mu       sync.Mutex
	breakers map[string]*resilience.Breaker // keyed by pbc
}

func New(reg *infra.Registry) *Handler {
	h := &Handler{
		reg:      reg,
		breakers: make(map[string]*resilience.Breaker),
	}
	for _, pbc := range []string{"pedidos", "estoque", "notificacoes"} {
		h.breakers[pbc] = resilience.NewBreaker("shard-router", "", pbc+"-proxy")
	}
	return h
}

func (h *Handler) Register(r *gin.Engine) {
	r.Any("/pedidos/*path", h.proxy("pedidos"))
	r.Any("/estoque/*path", h.proxy("estoque"))
	r.Any("/notificacoes/*path", h.proxy("notificacoes"))
	r.Any("/saga/*path", h.proxySaga())
	r.GET("/healthz/live", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })
	r.GET("/healthz/ready", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })
	r.GET("/router/cells", h.cells)
}

func (h *Handler) proxy(pbc string) gin.HandlerFunc {
	breaker := h.breakers[pbc]
	return func(c *gin.Context) {
		t0 := time.Now()

		key := c.GetHeader("X-Client-ID")
		if key == "" {
			key = c.Query("cliente_id")
		}
		if key == "" {
			key = "shard-1-default"
		}

		shard := domain.Route(key)
		cell, isFailover := h.reg.ActiveCell(shard, pbc)
		if cell == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": "no healthy cell", "shard": shard, "pbc": pbc,
			})
			return
		}
		if isFailover {
			failoverTotal.WithLabelValues(shard, pbc).Inc()
			slog.Warn("failover active", "shard", shard, "pbc", pbc, "cell", cell.ID)
		}

		c.Request.Header.Set("X-Shard-ID", shard)
		c.Request.Header.Set("X-Cell-ID", cell.ID)
		c.Request.Header.Set("X-Cell-Role", cell.Role)

		target, _ := url.Parse(cell.BaseURL)

		proxyErr := breaker.Execute(func() error {
			var forwardErr error
			rp := &httputil.ReverseProxy{
				Director: func(req *http.Request) {
					req.URL.Scheme = target.Scheme
					req.URL.Host = target.Host
					req.Host = target.Host
				},
				ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
					slog.Error("proxy error", "cell", cell.ID, "err", err)
					forwardErr = err
					w.WriteHeader(http.StatusBadGateway)
				},
			}
			rp.ServeHTTP(c.Writer, c.Request)
			return forwardErr
		})
		if proxyErr != nil {
			slog.Warn("circuit breaker recorded proxy failure", "pbc", pbc, "cell", cell.ID)
		}

		reqTotal.WithLabelValues(shard, pbc, cell.Role).Inc()
		reqDur.WithLabelValues(shard, pbc).Observe(time.Since(t0).Seconds())
	}
}

func (h *Handler) proxySaga() gin.HandlerFunc {
	sagaURL := os.Getenv("SAGA_HUB_URL")
	if sagaURL == "" {
		sagaURL = "http://saga-hub:9090"
	}
	target, _ := url.Parse(sagaURL)
	return func(c *gin.Context) {
		rp := &httputil.ReverseProxy{
			Director: func(req *http.Request) {
				req.URL.Scheme = target.Scheme
				req.URL.Host = target.Host
				req.Host = target.Host
			},
			ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
				slog.Error("saga proxy error", "err", err)
				w.WriteHeader(http.StatusBadGateway)
			},
		}
		rp.ServeHTTP(c.Writer, c.Request)
	}
}

func (h *Handler) cells(c *gin.Context) {
	snap := h.reg.Snapshot()
	for _, e := range snap {
		v := 0.0
		if e.Healthy {
			v = 1.0
		}
		cellHealth.WithLabelValues(e.ShardID, e.PBC, e.Role, e.ID).Set(v)
	}
	c.JSON(200, gin.H{"cells": snap})
}
