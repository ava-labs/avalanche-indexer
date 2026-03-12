package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/checkpointer"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
)

const (
	defaultInitializeTimeout = 30 * time.Second
	defaultDescribeInterval  = 1 * time.Second

	chainIDAttr           = "chain_id"
	modeAttr              = "mode"
	lowestUnprocessedAttr = "lowest_unprocessed_block"
	updatedAtAttr         = "updated_at"
)

type dynamoAPI interface {
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

var (
	_ checkpointer.Checkpointer = (*repository)(nil)
)

type repository struct {
	client    dynamoAPI
	tableName string
	logger    *zap.SugaredLogger
}

// NewRepository builds a DynamoDB-backed checkpoint repository and ensures
// its table exists/ready based on config.
func NewRepository(ctx context.Context, cfg Config) (checkpointer.Checkpointer, error) {
	if strings.TrimSpace(cfg.Region) == "" {
		return nil, errors.New("dynamodb region is required")
	}
	if strings.TrimSpace(cfg.TableName) == "" {
		return nil, errors.New("dynamodb checkpoint table name is required")
	}

	loadOptions := []func(*awscfg.LoadOptions) error{
		awscfg.WithRegion(cfg.Region),
	}

	awsCfg, err := awscfg.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	repo := &repository{
		client: dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
			if cfg.EndpointURL != "" {
				o.BaseEndpoint = aws.String(cfg.EndpointURL)
			}
		}),
		tableName: cfg.TableName,
		logger:    cfg.Logger,
	}

	initCtx, cancel := context.WithTimeout(ctx, defaultInitializeTimeout)
	defer cancel()
	if err := repo.Initialize(initCtx); err != nil {
		return nil, err
	}
	return repo, nil
}

func (r *repository) Initialize(ctx context.Context) error {
	_, err := r.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(r.tableName),
	})
	if err == nil {
		return r.waitForTableActive(ctx)
	}

	var notFound *types.ResourceNotFoundException
	if !errors.As(err, &notFound) {
		return fmt.Errorf("failed to describe checkpoint table %s: %w", r.tableName, err)
	}

	if r.logger != nil {
		r.logger.Infow("creating DynamoDB checkpoint table", "tableName", r.tableName)
	}

	_, err = r.client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(r.tableName),
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(chainIDAttr),
				KeyType:       types.KeyTypeHash,
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(chainIDAttr),
				AttributeType: types.ScalarAttributeTypeN,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		return fmt.Errorf("failed to create checkpoint table %s: %w", r.tableName, err)
	}

	if err := r.waitForTableActive(ctx); err != nil {
		return err
	}
	return nil
}

func (r *repository) waitForTableActive(ctx context.Context) error {
	ticker := time.NewTicker(defaultDescribeInterval)
	defer ticker.Stop()

	for {
		out, err := r.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(r.tableName),
		})
		if err == nil && out.Table != nil && out.Table.TableStatus == types.TableStatusActive {
			return nil
		}

		if err != nil && r.logger != nil {
			r.logger.Warnw("error describing checkpoint table, retrying", "tableName", r.tableName, "error", err)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for checkpoint table %s to become active: %w", r.tableName, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (r *repository) Write(ctx context.Context, evmChainID uint64, mode string, lowestUnprocessed uint64) error {
	_, err := r.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item: map[string]types.AttributeValue{
			chainIDAttr:           &types.AttributeValueMemberN{Value: strconv.FormatUint(evmChainID, 10)},
			modeAttr:              &types.AttributeValueMemberS{Value: mode},
			lowestUnprocessedAttr: &types.AttributeValueMemberN{Value: strconv.FormatUint(lowestUnprocessed, 10)},
			updatedAtAttr:         &types.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().Unix(), 10)},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to write checkpoint: %w", err)
	}
	return nil
}

func (r *repository) Read(ctx context.Context, evmChainID uint64, mode string) (lowestUnprocessed uint64, exists bool, err error) {
	out, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			chainIDAttr: &types.AttributeValueMemberN{Value: strconv.FormatUint(evmChainID, 10)},
			modeAttr:    &types.AttributeValueMemberS{Value: mode},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return 0, false, fmt.Errorf("failed to read checkpoint: %w", err)
	}
	if len(out.Item) == 0 {
		return 0, false, nil
	}

	rawLowest, ok := out.Item[lowestUnprocessedAttr].(*types.AttributeValueMemberN)
	if !ok {
		return 0, false, fmt.Errorf("checkpoint item missing numeric %q attribute", lowestUnprocessedAttr)
	}

	lowest, parseErr := strconv.ParseUint(rawLowest.Value, 10, 64)
	if parseErr != nil {
		return 0, false, fmt.Errorf("failed to parse checkpoint lowest_unprocessed_block: %w", parseErr)
	}
	return lowest, true, nil
}

func (r *repository) Delete(ctx context.Context, chainID uint64, mode string) error {
	_, err := r.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			chainIDAttr: &types.AttributeValueMemberN{Value: strconv.FormatUint(chainID, 10)},
			modeAttr:    &types.AttributeValueMemberS{Value: mode},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to delete checkpoints: %w", err)
	}
	return nil
}
