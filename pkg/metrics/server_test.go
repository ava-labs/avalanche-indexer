package metrics

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestNewServer(t *testing.T) {
	reg := prometheus.NewRegistry()
	server := NewServer(":0", reg) // :0 lets OS pick available port

	require.NotNil(t, server)
	require.NotNil(t, server.httpServer)
	require.Equal(t, ":0", server.httpServer.Addr)
}

func httpGet(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

func waitForStatusOK(ctx context.Context, url string) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	for {
		resp, err := httpGet(ctx, url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func TestServer_StartAndShutdown(t *testing.T) {
	reg := prometheus.NewRegistry()

	// Register some metrics so /metrics has content
	_, err := New(reg)
	require.NoError(t, err)

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := NewServer(listener.Addr().String(), reg)

	// Start the server
	errCh := server.StartWithListener(listener)

	// Verify server is running by hitting health endpoint
	healthURL := "http://" + listener.Addr().String() + "/health"
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	require.NoError(t, waitForStatusOK(ctx, healthURL))
	cancel()

	resp, err := httpGet(t.Context(), healthURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "ok", string(body))

	// Shutdown gracefully
	ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err = server.Shutdown(ctx)
	require.NoError(t, err)

	// Verify no error was sent on errCh (normal shutdown)
	select {
	case err := <-errCh:
		require.NoError(t, err)
	default:
		// Channel may be closed without error, that's fine
	}
}

func TestServer_MetricsEndpoint(t *testing.T) {
	reg := prometheus.NewRegistry()

	// Register metrics
	m, err := New(reg)
	require.NoError(t, err)

	// Update some metrics so they have values
	m.UpdateWindowMetrics(100, 200, 50)
	m.IncError(ErrTypeOutOfWindow)

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := NewServer(listener.Addr().String(), reg)
	errCh := server.StartWithListener(listener)
	defer func() {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		<-errCh
	}()

	// Hit metrics endpoint
	metricsURL := "http://" + listener.Addr().String() + "/metrics"
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	require.NoError(t, waitForStatusOK(ctx, metricsURL))
	cancel()

	resp, err := httpGet(t.Context(), metricsURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// Verify some expected metrics are present
	bodyStr := string(body)
	require.Contains(t, bodyStr, "indexer_lowest")
	require.Contains(t, bodyStr, "indexer_highest")
	require.Contains(t, bodyStr, "indexer_errors_total")
}

func TestServer_HealthEndpoint(t *testing.T) {
	reg := prometheus.NewRegistry()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := NewServer(listener.Addr().String(), reg)

	errCh := server.StartWithListener(listener)
	defer func() {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		<-errCh
	}()

	healthURL := "http://" + listener.Addr().String() + "/health"
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	require.NoError(t, waitForStatusOK(ctx, healthURL))
	cancel()

	resp, err := httpGet(t.Context(), healthURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "ok", string(body))
}
