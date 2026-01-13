package testutils

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Client wraps the ClickHouse connection (matches the interface from pkg/clickhouse)
type Client interface {
	// Conn returns the underlying ClickHouse connection
	Conn() driver.Conn
	// Ping checks the connection to ClickHouse
	Ping(ctx context.Context) error
	// Close closes the connection
	Close() error
}

// NewTestClient creates a client with a provided connection for testing purposes.
// This allows unit tests to test client methods without requiring a real ClickHouse connection.
// It can also be used by other packages that need to test ClickHouse integration.
func NewTestClient(conn driver.Conn) Client {
	return &testClient{conn: conn}
}

// testClient is an internal implementation that matches the client struct
type testClient struct {
	conn driver.Conn
}

func (c *testClient) Conn() driver.Conn {
	return c.conn
}

func (c *testClient) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

func (c *testClient) Close() error {
	return c.conn.Close()
}
