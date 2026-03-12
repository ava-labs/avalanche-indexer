package dynamodb

// Config holds the configuration for a DynamoDB client
type Config struct {
	EndpointURL     string
	CreateTable     bool
	Region          string
	SecretAccessKey string
	AccessKeyID     string
}
