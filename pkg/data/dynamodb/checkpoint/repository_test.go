package checkpoint

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

type mockDynamoClient struct {
	describeTableFn func(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	createTableFn   func(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	putItemFn       func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	getItemFn       func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	deleteItemFn    func(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

func (m *mockDynamoClient) DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	return m.describeTableFn(ctx, params, optFns...)
}

func (m *mockDynamoClient) CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	return m.createTableFn(ctx, params, optFns...)
}

func (m *mockDynamoClient) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return m.putItemFn(ctx, params, optFns...)
}

func (m *mockDynamoClient) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return m.getItemFn(ctx, params, optFns...)
}

func (m *mockDynamoClient) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return m.deleteItemFn(ctx, params, optFns...)
}

func TestRepositoryInitialize_TableExists(t *testing.T) {
	repo := &repository{
		tableName:    "checkpoints",
		createTables: true,
		client: &mockDynamoClient{
			describeTableFn: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
				return &dynamodb.DescribeTableOutput{
					Table: &types.TableDescription{
						TableStatus: types.TableStatusActive,
					},
				}, nil
			},
			createTableFn: func(context.Context, *dynamodb.CreateTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
				t.Fatal("CreateTable should not be called")
				return nil, nil
			},
		},
	}

	require.NoError(t, repo.Initialize(context.Background()))
}

func TestRepositoryInitialize_CreateTable(t *testing.T) {
	describeCalls := 0
	repo := &repository{
		tableName:    "checkpoints",
		createTables: true,
		client: &mockDynamoClient{
			describeTableFn: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
				describeCalls++
				if describeCalls == 1 {
					return nil, &types.ResourceNotFoundException{}
				}
				return &dynamodb.DescribeTableOutput{
					Table: &types.TableDescription{
						TableStatus: types.TableStatusActive,
					},
				}, nil
			},
			createTableFn: func(context.Context, *dynamodb.CreateTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
				return &dynamodb.CreateTableOutput{}, nil
			},
		},
	}

	require.NoError(t, repo.Initialize(context.Background()))
}

func TestRepositoryInitialize_NotFoundCreateDisabled(t *testing.T) {
	repo := &repository{
		tableName:    "checkpoints",
		createTables: false,
		client: &mockDynamoClient{
			describeTableFn: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
				return nil, &types.ResourceNotFoundException{}
			},
		},
	}

	err := repo.Initialize(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "auto-create is disabled")
}

func TestRepositoryWriteReadDelete(t *testing.T) {
	var writeInput *dynamodb.PutItemInput
	repo := &repository{
		tableName: "checkpoints",
		client: &mockDynamoClient{
			describeTableFn: func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
				return &dynamodb.DescribeTableOutput{}, nil
			},
			createTableFn: func(context.Context, *dynamodb.CreateTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
				return &dynamodb.CreateTableOutput{}, nil
			},
			putItemFn: func(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
				writeInput = in
				return &dynamodb.PutItemOutput{}, nil
			},
			getItemFn: func(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
				require.Equal(t, aws.ToString(in.TableName), "checkpoints")
				return &dynamodb.GetItemOutput{
					Item: map[string]types.AttributeValue{
						chainIDAttr:           &types.AttributeValueMemberN{Value: "43114"},
						lowestUnprocessedAttr: &types.AttributeValueMemberN{Value: "123"},
					},
				}, nil
			},
			deleteItemFn: func(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
				return &dynamodb.DeleteItemOutput{}, nil
			},
		},
	}

	require.NoError(t, repo.Write(context.Background(), 43114, 123))
	require.NotNil(t, writeInput)
	require.Equal(t, "43114", writeInput.Item[chainIDAttr].(*types.AttributeValueMemberN).Value)
	require.Equal(t, "123", writeInput.Item[lowestUnprocessedAttr].(*types.AttributeValueMemberN).Value)

	lowest, exists, err := repo.Read(context.Background(), 43114)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(123), lowest)

	require.NoError(t, repo.DeleteCheckpoints(context.Background(), 43114))
}

func TestRepositoryReadNoRows(t *testing.T) {
	repo := &repository{
		tableName: "checkpoints",
		client: &mockDynamoClient{
			getItemFn: func(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
				return &dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{}}, nil
			},
		},
	}

	lowest, exists, err := repo.Read(context.Background(), 1)
	require.NoError(t, err)
	require.False(t, exists)
	require.Zero(t, lowest)
}

func TestRepositoryReadParseError(t *testing.T) {
	repo := &repository{
		tableName: "checkpoints",
		client: &mockDynamoClient{
			getItemFn: func(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
				return &dynamodb.GetItemOutput{
					Item: map[string]types.AttributeValue{
						lowestUnprocessedAttr: &types.AttributeValueMemberN{Value: "not-a-number"},
					},
				}, nil
			},
		},
	}

	_, _, err := repo.Read(context.Background(), 1)
	require.Error(t, err)
}

func TestRepositoryWriteError(t *testing.T) {
	repo := &repository{
		tableName: "checkpoints",
		client: &mockDynamoClient{
			putItemFn: func(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
				return nil, errors.New("boom")
			},
		},
	}

	err := repo.Write(context.Background(), 1, 2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to write checkpoint")
}
