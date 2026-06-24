package clickhouse

// Config holds the configuration for a ClickHouse client
// In ClickHouse, data is processed by blocks, which are sets of column parts.
// The internal processing cycles for a single block are efficient but there are noticeable costs when processing each block
// The max_block_size setting indicates the recommended maximum number of rows to include in a single block when loading data from tables.
// The block size should not be too small to avoid noticeable costs when processing each block.
// see here: https://clickhouse.com/docs/operations/settings/settings
type Config struct {
	Hosts                []string
	Cluster              string
	Database             string
	Username             string
	Password             string
	Debug                bool
	InsecureSkipVerify   bool
	MaxExecutionTime     int // seconds
	DialTimeout          int // seconds
	MaxOpenConns         int
	MaxIdleConns         int
	ConnMaxLifetime      int // minutes
	BlockBufferSize      uint8
	MaxBlockSize         int    // recommended maximum number of rows in a single block
	MaxCompressionBuffer int    // bytes
	ClientName           string // client name for ClickHouse ClientInfo
	ClientVersion        string // client version for ClickHouse ClientInfo
	UseHTTP              bool   // use HTTP protocol instead of native
}
