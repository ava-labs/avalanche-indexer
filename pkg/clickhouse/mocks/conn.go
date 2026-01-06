package mocks

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/mock"
)

// MockConn is a mock implementation of driver.Conn for testing
type MockConn struct {
	mock.Mock
}

func (m *MockConn) Contributors() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

func (m *MockConn) ServerVersion() (*driver.ServerVersion, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*driver.ServerVersion), args.Error(1)
}

func (m *MockConn) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	callArgs := []interface{}{ctx, query}
	callArgs = append(callArgs, args...)
	argsResult := m.Called(callArgs...)
	return argsResult.Error(0)
}

func (m *MockConn) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	callArgs := []interface{}{ctx, query}
	callArgs = append(callArgs, args...)
	argsResult := m.Called(callArgs...)
	if argsResult.Get(0) == nil {
		return nil, argsResult.Error(1)
	}
	return argsResult.Get(0).(driver.Rows), argsResult.Error(1)
}

func (m *MockConn) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	callArgs := []interface{}{ctx, query}
	callArgs = append(callArgs, args...)
	argsResult := m.Called(callArgs...)
	if argsResult.Get(0) == nil {
		return nil
	}
	return argsResult.Get(0).(driver.Row)
}

func (m *MockConn) Exec(ctx context.Context, query string, args ...interface{}) error {
	callArgs := []interface{}{ctx, query}
	callArgs = append(callArgs, args...)
	argsResult := m.Called(callArgs...)
	return argsResult.Error(0)
}

func (m *MockConn) AsyncInsert(ctx context.Context, query string, wait bool, args ...interface{}) error {
	callArgs := []interface{}{ctx, query, wait}
	callArgs = append(callArgs, args...)
	argsResult := m.Called(callArgs...)
	return argsResult.Error(0)
}

func (m *MockConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	callArgs := []interface{}{ctx, query}
	for _, opt := range opts {
		callArgs = append(callArgs, opt)
	}
	argsResult := m.Called(callArgs...)
	if argsResult.Get(0) == nil {
		return nil, argsResult.Error(1)
	}
	return argsResult.Get(0).(driver.Batch), argsResult.Error(1)
}

func (m *MockConn) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockConn) Stats() driver.Stats {
	args := m.Called()
	if args.Get(0) == nil {
		return driver.Stats{}
	}
	return args.Get(0).(driver.Stats)
}

func (m *MockConn) Close() error {
	args := m.Called()
	return args.Error(0)
}
