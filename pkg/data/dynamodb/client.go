package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"go.uber.org/zap"
)

// Client wraps the DynamoDB SDK client with batch write support and retry logic.
type Client struct {
	db  *dynamodb.Client
	cfg Config
	log *zap.SugaredLogger
}

// New creates a new DynamoDB client with the given configuration.
func New(ctx context.Context, cfg Config, log *zap.SugaredLogger) (*Client, error) {
	var opts []func(*awsconfig.LoadOptions) error
	opts = append(opts, awsconfig.WithRegion(cfg.Region))

	if cfg.Endpoint != "" {
		// LocalStack / local development
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	var dbOpts []func(*dynamodb.Options)
	if cfg.Endpoint != "" {
		dbOpts = append(dbOpts, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	db := dynamodb.NewFromConfig(awsCfg, dbOpts...)

	return &Client{
		db:  db,
		cfg: cfg,
		log: log,
	}, nil
}

// BatchWriteItems writes items in batches of up to MaxBatchSize, retrying
// unprocessed items with exponential backoff.
func (c *Client) BatchWriteItems(ctx context.Context, tableName string, requests []types.WriteRequest) error {
	batchSize := c.cfg.MaxBatchSize
	if batchSize <= 0 {
		batchSize = 25
	}

	for i := 0; i < len(requests); i += batchSize {
		end := i + batchSize
		if end > len(requests) {
			end = len(requests)
		}
		batch := requests[i:end]

		if err := c.writeBatchWithRetry(ctx, tableName, batch); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) writeBatchWithRetry(ctx context.Context, tableName string, items []types.WriteRequest) error {
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tableName: items,
		},
	}

	maxRetries := c.cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 10
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		output, err := c.db.BatchWriteItem(ctx, input)
		if err != nil {
			return fmt.Errorf("batch write failed: %w", err)
		}

		unprocessed := output.UnprocessedItems[tableName]
		if len(unprocessed) == 0 {
			return nil
		}

		// Retry unprocessed items with exponential backoff
		input.RequestItems[tableName] = unprocessed
		backoff := time.Duration(math.Pow(2, float64(attempt))) * 100 * time.Millisecond
		if backoff > 2*time.Minute {
			backoff = 2 * time.Minute
		}

		c.log.Debugw("retrying unprocessed items",
			"attempt", attempt+1,
			"unprocessed", len(unprocessed),
			"backoff", backoff,
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}

	return fmt.Errorf("exceeded max retries for batch write (%d unprocessed items)", len(input.RequestItems[tableName]))
}

// UpdateItem executes a single UpdateItem operation.
func (c *Client) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput) error {
	_, err := c.db.UpdateItem(ctx, input)
	return err
}

// CreateTable creates a DynamoDB table if it doesn't exist.
func (c *Client) CreateTable(ctx context.Context, input *dynamodb.CreateTableInput) error {
	_, err := c.db.CreateTable(ctx, input)
	if err != nil {
		// Ignore ResourceInUseException (table already exists)
		var resourceInUse *types.ResourceInUseException
		if errors.As(err, &resourceInUse) {
			return nil
		}
		return fmt.Errorf("failed to create table %s: %w", *input.TableName, err)
	}
	return nil
}

// Inner returns the underlying DynamoDB SDK client for advanced operations.
func (c *Client) Inner() *dynamodb.Client {
	return c.db
}
