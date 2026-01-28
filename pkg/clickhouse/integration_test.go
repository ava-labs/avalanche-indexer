//go:build integration
// +build integration

package clickhouse

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	testClickHouseClient    Client
	testClickHouseContainer testcontainers.Container
)

// setupClickHouseContainer spins up a ClickHouse container for integration tests
func setupClickHouseContainer() (testcontainers.Container, Config, error) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:23.8",
		ExposedPorts: []string{"9000/tcp", "8123/tcp"},
		Env: map[string]string{
			"CLICKHOUSE_DB":       "default",
			"CLICKHOUSE_USER":     "default",
			"CLICKHOUSE_PASSWORD": "",
		},
		WaitingFor: wait.ForHTTP("/ping").
			WithPort("8123/tcp").
			WithStartupTimeout(90 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, Config{}, fmt.Errorf("failed to start ClickHouse container: %w", err)
	}

	// Get the mapped port
	mappedPort, err := container.MappedPort(ctx, "9000")
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, Config{}, fmt.Errorf("failed to get mapped port: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, Config{}, fmt.Errorf("failed to get container host: %w", err)
	}

	// Create config for the test container
	cfg := Config{
		Hosts:       []string{fmt.Sprintf("%s:%d", host, mappedPort.Int())},
		Database:    "default",
		Username:    "default",
		Password:    "",
		DialTimeout: 10,
		Debug:       false,
	}

	return container, cfg, nil
}

// TestMain sets up the ClickHouse client for all integration tests.
// Uses testcontainers-go to spin up a ClickHouse instance automatically.
func TestMain(m *testing.M) {
	ctx := context.Background()

	// Spin up ClickHouse container
	log.Println("Setting up ClickHouse container for integration tests...")
	container, cfg, err := setupClickHouseContainer()
	if err != nil {
		log.Fatalf("Failed to setup ClickHouse container: %v", err)
	}
	testClickHouseContainer = container

	log.Printf("ClickHouse container started at %s", cfg.Hosts[0])

	// Create a test logger
	sugar, err := utils.NewSugaredLogger(true)
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}

	// Initialize ClickHouse client
	chClient, err := New(cfg, sugar)
	if err != nil {
		_ = container.Terminate(ctx)
		log.Fatalf("Failed to create ClickHouse client: %v", err)
	}
	testClickHouseClient = chClient

	// Ping ClickHouse to ensure connection works
	if err := testClickHouseClient.Ping(ctx); err != nil {
		_ = testClickHouseClient.Close()
		_ = container.Terminate(ctx)
		log.Fatalf("Failed to ping ClickHouse: %v", err)
	}

	log.Println("ClickHouse client initialized successfully")

	// Run all tests
	code := m.Run()

	// Cleanup
	log.Println("Cleaning up ClickHouse resources...")
	if testClickHouseClient != nil {
		_ = testClickHouseClient.Close()
	}
	if testClickHouseContainer != nil {
		_ = testClickHouseContainer.Terminate(ctx)
	}

	os.Exit(code)
}

// TestClientImpl_Methods tests the actual client implementation methods (Conn, Ping, Close)
func TestClientImpl_Methods(t *testing.T) {
	require.NotNil(t, testClickHouseClient, "ClickHouse client must be initialized")
	require.NotNil(t, testClickHouseContainer, "ClickHouse container must be running")

	// Test Conn() method
	conn := testClickHouseClient.Conn()
	assert.NotNil(t, conn, "Conn() should return a non-nil connection")

	// Test Ping() method
	ctx := context.Background()
	err := testClickHouseClient.Ping(ctx)
	require.NoError(t, err, "Ping() should succeed with valid connection")

	// Test Close() method (create a new client for this test since we need to close it)
	// Get container connection details to create a temporary client
	mappedPort, err := testClickHouseContainer.MappedPort(ctx, "9000")
	require.NoError(t, err)

	host, err := testClickHouseContainer.Host(ctx)
	require.NoError(t, err)

	tempCfg := Config{
		Hosts:       []string{fmt.Sprintf("%s:%d", host, mappedPort.Int())},
		Database:    "default",
		Username:    "default",
		Password:    "",
		DialTimeout: 5,
	}

	sugar, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)

	tempClient, err := New(tempCfg, sugar)
	require.NoError(t, err, "Should be able to create a temporary client")
	err = tempClient.Close()
	require.NoError(t, err, "Close() should succeed")
}

// TestNew_ExceptionError tests the clickhouse.Exception error path
// This test exercises the Exception handling code in New's Ping error handler
// by attempting to connect with invalid credentials, which triggers a clickhouse.Exception
func TestNew_ExceptionError(t *testing.T) {
	require.NotNil(t, testClickHouseClient, "ClickHouse client must be initialized")
	require.NotNil(t, testClickHouseContainer, "ClickHouse container must be running")

	ctx := context.Background()

	// Get the current container's connection details
	mappedPort, err := testClickHouseContainer.MappedPort(ctx, "9000")
	require.NoError(t, err)

	host, err := testClickHouseContainer.Host(ctx)
	require.NoError(t, err)

	// Create config with invalid credentials to trigger clickhouse.Exception
	cfg := Config{
		Hosts:       []string{fmt.Sprintf("%s:%d", host, mappedPort.Int())},
		Database:    "default",
		Username:    "invaliduser",
		Password:    "invalidpass",
		DialTimeout: 5,
	}

	sugar, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)

	client, err := New(cfg, sugar)

	// Should fail with authentication error (clickhouse.Exception)
	var exception *clickhouse.Exception
	require.ErrorAs(t, err, &exception)
	assert.Nil(t, client, "Client should be nil when creation fails")

	// Verify it's a clickhouse.Exception
	exception, ok := err.(*clickhouse.Exception)
	require.True(t, ok, "Error should be a clickhouse.Exception, got: %T", err)

	// Verify Exception has expected fields
	assert.NotZero(t, exception.Code, "Exception should have a non-zero error code")
	assert.NotEmpty(t, exception.Message, "Exception should have an error message")
}
