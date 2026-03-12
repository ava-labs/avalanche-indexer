package main

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/checkpointer"
	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"

	chcheckpoint "github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/checkpoint"
	ddbcheckpoint "github.com/ava-labs/avalanche-indexer/pkg/data/dynamodb/checkpoint"
	ddbClient "github.com/ava-labs/avalanche-indexer/pkg/dynamodb"
)

func newCheckpointStore(
	ctx context.Context,
	cfg *Config,
	log *zap.SugaredLogger,
) (checkpointer.Checkpointer, error) {
	switch cfg.CheckpointBackend {
	case checkpointBackendClickHouse:
		return newClickHouseCheckpointStore(ctx, cfg, log)
	case checkpointBackendDynamoDB:
		return newDynamoCheckpointStore(ctx, cfg, log)
	default:
		return nil, fmt.Errorf("unsupported checkpoint backend: %s", cfg.CheckpointBackend)
	}
}

func newClickHouseCheckpointStore(
	_ context.Context,
	cfg *Config,
	log *zap.SugaredLogger,
) (checkpointer.Checkpointer, error) {
	chClient, err := clickhouse.New(cfg.ClickHouse, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse client: %w", err)
	}

	repo, err := chcheckpoint.NewRepository(chClient, cfg.ClickHouse.Cluster, cfg.ClickHouse.Database, cfg.CheckpointTableName)
	if err != nil {
		chClient.Close()
		return nil, fmt.Errorf("failed to create checkpoint repository: %w", err)
	}

	return repo, nil
}

func newDynamoCheckpointStore(
	_ context.Context,
	cfg *Config,
	log *zap.SugaredLogger,
) (checkpointer.Checkpointer, error) {
	ddbClient, err := ddbClient.New(cfg.DynamoDB, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create DynamoDB client: %w", err)
	}

	repo, err := ddbcheckpoint.NewRepository(ddbClient, cfg.CheckpointTableName, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create DynamoDB checkpoint repository: %w", err)
	}

	return repo, nil
}
