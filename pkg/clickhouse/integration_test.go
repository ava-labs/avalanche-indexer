//go:build integration
// +build integration

package clickhouse

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testClickHouseClient Client

// loadTestEnv loads the .env.test file from the clickhouse directory
func loadTestEnv() error {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return nil // If we can't determine the file, just use defaults
	}

	// Get the directory of the current file (integration_test.go)
	dir := filepath.Dir(currentFile)
	envPath := filepath.Join(dir, ".env.test")
	return godotenv.Load(envPath)
}

// TestMain sets up the ClickHouse client for all integration tests.
// Integration tests require a running ClickHouse instance.
// If ClickHouse is not available, tests will fail (no skipping).
func TestMain(m *testing.M) {
	ctx := context.Background()

	// Load test environment variables for integration tests
	if err := loadTestEnv(); err != nil {
		log.Printf("integration: Note: Could not load .env.test file: %v (using defaults)", err)
	}

	// Load configuration (will use .env.test values if loaded, otherwise defaults)
	cfg := Load()

	// Override with test-friendly settings
	cfg.DialTimeout = 5

	// Create a test logger
	sugar, err := utils.NewSugaredLogger(true) // Use verbose mode for integration tests
	if err != nil {
		log.Fatalf("integration: failed to create logger: %v", err)
	}

	// Initialize ClickHouse client - fail if not available
	chClient, err := New(cfg, sugar)
	if err != nil {
		log.Fatalf("integration: failed to open ClickHouse connection: %v", err)
	}

	testClickHouseClient = chClient

	// Ping ClickHouse to ensure connection works - fail if not available
	if err := testClickHouseClient.Ping(ctx); err != nil {
		log.Fatalf("integration: failed to ping ClickHouse: %v", err)
	}

	// Run all tests
	code := m.Run()

	// Cleanup
	if testClickHouseClient != nil {
		_ = testClickHouseClient.Close()
	}

	os.Exit(code)
}

// TestClientImpl_Methods tests the actual client implementation methods (Conn, Ping, Close)
func TestClientImpl_Methods(t *testing.T) {
	require.NotNil(t, testClickHouseClient, "ClickHouse client must be initialized")

	// Test Conn() method
	conn := testClickHouseClient.Conn()
	assert.NotNil(t, conn, "Conn() should return a non-nil connection")

	// Test Ping() method
	ctx := context.Background()
	err := testClickHouseClient.Ping(ctx)
	assert.NoError(t, err, "Ping() should succeed with valid connection")

	// Test Close() method (create a new client for this test since we need to close it)
	// We'll test Close by creating a temporary client with the same config
	tempCfg := Load()
	tempCfg.DialTimeout = 5

	sugar, err := utils.NewSugaredLogger(true) // Use verbose mode for tests
	require.NoError(t, err)

	tempClient, err := New(tempCfg, sugar)
	require.NoError(t, err, "Should be able to create a temporary client")
	err = tempClient.Close()
	assert.NoError(t, err, "Close() should succeed")
}

// TestNew_ExceptionError tests the clickhouse.Exception error path
// This test exercises the Exception handling code in New's Ping error handler
// by attempting to connect with invalid credentials, which triggers a clickhouse.Exception
func TestNew_ExceptionError(t *testing.T) {
	require.NotNil(t, testClickHouseClient, "ClickHouse client must be initialized")

	// Use a config that will connect but fail authentication, triggering a clickhouse.Exception
	// Start with loaded config and override with invalid credentials
	cfg := Load()
	cfg.Username = "invaliduser"
	cfg.Password = "invalidpass"

	sugar, err := utils.NewSugaredLogger(true) // Use verbose mode for tests
	require.NoError(t, err)

	client, err := New(cfg, sugar)

	// Should fail with authentication error (clickhouse.Exception)
	require.Error(t, err, "New should fail with invalid credentials")
	assert.Nil(t, client, "Client should be nil when creation fails")

	// Verify it's a clickhouse.Exception
	exception, ok := err.(*clickhouse.Exception)
	require.True(t, ok, "Error should be a clickhouse.Exception, got: %T", err)

	// Verify Exception has expected fields
	assert.NotZero(t, exception.Code, "Exception should have a non-zero error code")
	assert.NotEmpty(t, exception.Message, "Exception should have an error message")
}
