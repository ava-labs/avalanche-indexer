package clickhouse

import (
	"fmt"
	"os"

	"github.com/caarlos0/env/v11"
	"go.uber.org/zap"
)

// ClickhouseConfig holds the configuration for a ClickHouse client
// In ClickHouse, data is processed by blocks, which are sets of column parts.
// The internal processing cycles for a single block are efficient but there are noticeable costs when processing each block
// The max_block_size setting indicates the recommended maximum number of rows to include in a single block when loading data from tables.
// The block size should not be too small to avoid noticeable costs when processing each block.
// see here: https://clickhouse.com/docs/operations/settings/settings
type ClickhouseConfig struct {
	Hosts                []string `env:"CLICKHOUSE_HOSTS" envSeparator:"," envDefault:"localhost:9000"`
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
		// Create a temporary logger for error reporting during config loading
		logger, logErr := zap.NewProduction()
		if logErr == nil {
			logger.Sugar().Errorw("failed to parse clickhouse config", "error", err)
		} else {
			// Fallback to fmt if logger creation fails
			fmt.Fprintf(os.Stderr, "failed to parse clickhouse config: %v\n", err)
		}
		os.Exit(1)
	}
	return cfg
}
