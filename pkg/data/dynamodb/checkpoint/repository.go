package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/checkpointer"
)

const (
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

type repository struct {
	client    dynamoAPI
	tableName string
	logger    *zap.SugaredLogger
}

// NewRepository builds a DynamoDB-backed checkpoint repository and ensures
// its table exists/ready based on config.
func NewRepository(client *dynamodb.Client, tableName string, log *zap.SugaredLogger) (checkpointer.Checkpointer, error) {

	repo := &repository{
		client:    client,
		tableName: tableName,
		logger:    log,
	}

	if err := repo.Initialize(context.Background()); err != nil {
		return nil, err
	}
	return repo, nil
}

func (r *repository) Initialize(ctx context.Context) error {
	_, err := r.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(r.tableName),
	})

	var notFound *types.ResourceNotFoundException
	if errors.As(err, &notFound) {
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
				{
					AttributeName: aws.String(modeAttr),
					KeyType:       types.KeyTypeRange,
				},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String(chainIDAttr),
					AttributeType: types.ScalarAttributeTypeN,
				},
				{
					AttributeName: aws.String(modeAttr),
					AttributeType: types.ScalarAttributeTypeS,
				},
			},
			BillingMode: types.BillingModePayPerRequest,
		})

		var riue *types.ResourceInUseException
		if err != nil && !errors.As(err, &riue) {
			return fmt.Errorf("failed to create checkpoint table %s: %w", r.tableName, err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to describe checkpoint table %s: %w", r.tableName, err)
	}

	waiter := dynamodb.NewTableExistsWaiter(r.client)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(r.tableName),
	}, 30*time.Second); err != nil {
		return fmt.Errorf("timed out waiting for checkpoint table %s: %w", r.tableName, err)
	}

	return nil
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

// Close does nothing for DynamoDB as DDB API is RESTful
func (r *repository) Close() error {
	return nil
}
