package dynamodb

// Config holds configuration for the DynamoDB client.
type Config struct {
	Region          string
	Endpoint        string // Optional override for local development (e.g., LocalStack)
	HistoryTable    string // Table name for block/tx/log history
	ERCTable        string // Table name for ERC token metadata
	StatusTable     string // Table name for stream commit status
	MaxRetries      int
	MaxBatchSize    int // Max items per BatchWriteItem call (DynamoDB limit: 25)
	MaxInflight     int // Max concurrent batch write operations
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Region:       "us-east-1",
		HistoryTable: "history",
		ERCTable:     "erc",
		StatusTable:  "status",
		MaxRetries:   10,
		MaxBatchSize: 25,
		MaxInflight:  100,
	}
}
