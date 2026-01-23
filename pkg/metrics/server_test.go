package metrics

import (
	"context"
	"io"
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

func TestServer_StartAndShutdown(t *testing.T) {
	reg := prometheus.NewRegistry()

	// Register some metrics so /metrics has content
	_, err := New(reg)
	require.NoError(t, err)

	server := NewServer("127.0.0.1:19090", reg)

	// Start the server
	errCh := server.Start()

	// Give server time to start
	time.Sleep(50 * time.Millisecond)

	// Verify server is running by hitting health endpoint
	resp, err := httpGet(t.Context(), "http://127.0.0.1:19090/health")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "ok", string(body))

	// Shutdown gracefully
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
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

	server := NewServer("127.0.0.1:19091", reg)
	errCh := server.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		<-errCh
	}()

	// Give server time to start
	time.Sleep(50 * time.Millisecond)

	// Hit metrics endpoint
	resp, err := httpGet(t.Context(), "http://127.0.0.1:19091/metrics")
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
	server := NewServer("127.0.0.1:19092", reg)

	errCh := server.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		<-errCh
	}()

	time.Sleep(50 * time.Millisecond)

	resp, err := httpGet(t.Context(), "http://127.0.0.1:19092/health")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "ok", string(body))
}
