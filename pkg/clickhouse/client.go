package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/caarlos0/env/v11"
)

// Client wraps the ClickHouse connection
type Client interface {
	// Conn returns the underlying ClickHouse connection
	Conn() driver.Conn
	// Ping checks the connection to ClickHouse
	Ping(ctx context.Context) error
	// Close closes the connection
	Close() error
}

// ClickHouse setting keys
const (
	maxExecutionTime = "max_execution_time"
	maxBlockSize     = "max_block_size"
)

type client struct {
	conn driver.Conn
}

// ClickhouseConfig holds the configuration for a ClickHouse client
// In ClickHouse, data is processed by blocks, which are sets of column parts.
// The internal processing cycles for a single block are efficient but there are noticeable costs when processing each block
// The max_block_size setting indicates the recommended maximum number of rows to include in a single block when loading data from tables.
// The block size should not be too small to avoid noticeable costs when processing each block.
// see here: https://clickhouse.com/docs/operations/settings/settings
type ClickhouseConfig struct {
	Addresses            []string `env:"CLICKHOUSE_ADDRESSES" envSeparator:"," envDefault:"localhost:9000"`
	Database             string   `env:"CLICKHOUSE_DATABASE" envDefault:"default"`
	Username             string   `env:"CLICKHOUSE_USERNAME" envDefault:"default"`
	Password             string   `env:"CLICKHOUSE_PASSWORD" envDefault:""`
	Debug                bool     `env:"CLICKHOUSE_DEBUG" envDefault:"false"`
	InsecureSkipVerify   bool     `env:"CLICKHOUSE_INSECURE_SKIP_VERIFY" envDefault:"true"`
	MaxExecutionTime     int      `env:"CLICKHOUSE_MAX_EXECUTION_TIME" envDefault:"60"` // seconds
	DialTimeout          int      `env:"CLICKHOUSE_DIAL_TIMEOUT" envDefault:"30"`       // seconds
	MaxOpenConns         int      `env:"CLICKHOUSE_MAX_OPEN_CONNS" envDefault:"5"`
	MaxIdleConns         int      `env:"CLICKHOUSE_MAX_IDLE_CONNS" envDefault:"5"`
	ConnMaxLifetime      int      `env:"CLICKHOUSE_CONN_MAX_LIFETIME" envDefault:"10"` // minutes
	BlockBufferSize      int      `env:"CLICKHOUSE_BLOCK_BUFFER_SIZE" envDefault:"10"`
	MaxBlockSize         int      `env:"CLICKHOUSE_MAX_BLOCK_SIZE" envDefault:"1000"`          // recommended maximum number of rows in a single block
	MaxCompressionBuffer int      `env:"CLICKHOUSE_MAX_COMPRESSION_BUFFER" envDefault:"10240"` // bytes
	ClientName           string   `env:"CLICKHOUSE_CLIENT_NAME" envDefault:"ac-client-name"`   // client name for ClickHouse ClientInfo
	ClientVersion        string   `env:"CLICKHOUSE_CLIENT_VERSION" envDefault:"1.0"`           // client version for ClickHouse ClientInfo
}

// Load loads ClickHouse configuration from environment variables
func Load() ClickhouseConfig {
	var cfg ClickhouseConfig
	if err := env.Parse(&cfg); err != nil {
		slog.Error("failed to parse clickhouse config", "error", err)
		os.Exit(1)
	}
	return cfg
}

// NewClient creates a new ClickHouse client with the provided configuration
func NewClient(cfg ClickhouseConfig) (Client, error) {
	opts := &clickhouse.Options{
		Addr: cfg.Addresses,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
		Settings: clickhouse.Settings{
			maxExecutionTime: cfg.MaxExecutionTime,
			maxBlockSize:     cfg.MaxBlockSize,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:          time.Duration(cfg.DialTimeout) * time.Second,
		MaxOpenConns:         cfg.MaxOpenConns,
		MaxIdleConns:         cfg.MaxIdleConns,
		ConnMaxLifetime:      time.Duration(cfg.ConnMaxLifetime) * time.Minute,
		ConnOpenStrategy:     clickhouse.ConnOpenInOrder,
		BlockBufferSize:      uint8(cfg.BlockBufferSize),
		MaxCompressionBuffer: cfg.MaxCompressionBuffer,
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: cfg.ClientName, Version: cfg.ClientVersion},
			},
		},
		TLS: &tls.Config{
			//nolint:gosec // InsecureSkipVerify is configurable via environment variable for development/testing
			InsecureSkipVerify: cfg.InsecureSkipVerify,
		},
	}

	// Set debug function if debug is enabled
	if cfg.Debug {
		opts.Debugf = func(format string, v ...interface{}) {
			fmt.Printf(format, v...)
		}
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open ClickHouse connection: %w", err)
	}

	// Test the connection. if this fails, the service should not start
	// as ClickHouse is critical for the service to function
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			slog.Error("failed to ping ClickHouse", "error", exception)
		} else {
			slog.Error("failed to ping ClickHouse", "error", err)
		}
		// Close connection to avoid resource leaks, but ignore close errors since we're already failing
		_ = conn.Close()
		return nil, err
	}

	return &client{conn: conn}, nil
}

func (c *client) Conn() driver.Conn {
	return c.conn
}

func (c *client) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

func (c *client) Close() error {
	return c.conn.Close()
}

// newTestClient creates a client with a provided connection for testing purposes
// This allows unit tests to test client methods without requiring a real ClickHouse connection
func newTestClient(conn driver.Conn) Client {
	return &client{conn: conn}
}
