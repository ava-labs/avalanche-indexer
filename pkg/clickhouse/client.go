package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap"
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

// Connection timeout for initial ping during client creation
const (
	defaultPingTimeout = 10 * time.Second
)

type client struct {
	conn   driver.Conn
	logger *zap.SugaredLogger
}

// New creates a new ClickHouse client with the provided configuration
func New(cfg ClickhouseConfig, sugar *zap.SugaredLogger) (Client, error) {
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
	if cfg.Debug && sugar != nil {
		opts.Debugf = func(format string, v ...interface{}) {
			sugar.Debugf(format, v...)
		}
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open ClickHouse connection: %w", err)
	}

	// Test the connection. if this fails, the service should not start
	// as ClickHouse is critical for the service to function
	ctx, cancel := context.WithTimeout(context.Background(), defaultPingTimeout)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			if sugar != nil {
				sugar.Errorw("failed to ping ClickHouse", "error", exception)
			}
		} else {
			if sugar != nil {
				sugar.Errorw("failed to ping ClickHouse", "error", err)
			}
		}
		// Close connection to avoid resource leaks, but ignore close errors since we're already failing
		_ = conn.Close()
		return nil, err
	}

	return &client{conn: conn, logger: sugar}, nil
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
