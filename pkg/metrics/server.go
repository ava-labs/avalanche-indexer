package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server serves Prometheus metrics over HTTP.
type Server struct {
	httpServer *http.Server
}

// NewServer creates a new metrics HTTP server.
// The server exposes metrics at /metrics on the given address (e.g., ":9090").
func NewServer(addr string, gatherer prometheus.Gatherer) *Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	return &Server{
		httpServer: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
		},
	}
}

// Start begins serving metrics. This is non-blocking.
// Returns a channel that receives an error if the server fails.
func (s *Server) Start() <-chan error {
	errCh := make(chan error, 1)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("metrics server: %w", err)
		}
		close(errCh)
	}()
	return errCh
}

// StartWithListener begins serving metrics on a provided listener. This is
// useful for tests that need an ephemeral port.
func (s *Server) StartWithListener(listener net.Listener) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		if err := s.httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("metrics server: %w", err)
		}
		close(errCh)
	}()
	return errCh
}

// Shutdown gracefully stops the metrics server, waiting for active connections
// to complete or until the context is cancelled.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
