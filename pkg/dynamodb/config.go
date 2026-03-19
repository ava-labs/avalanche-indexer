package dynamodb

// Config holds the configuration for a DynamoDB client
type Config struct {
	EndpointURL     string
	Region          string
	SecretAccessKey string
	AccessKeyID     string
}
