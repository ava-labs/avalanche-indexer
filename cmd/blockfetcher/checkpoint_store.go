package main

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/checkpointer"
	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"

	chcheckpoint "github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/checkpoint"
	dyncheckpoint "github.com/ava-labs/avalanche-indexer/pkg/data/dynamodb/checkpoint"
)

type checkpointRemover interface {
	DeleteCheckpoints(ctx context.Context, chainID uint64) error
}

func newCheckpointStore(
	ctx context.Context,
	cfg *Config,
	log *zap.SugaredLogger,
) (checkpointer.Checkpointer, checkpointRemover, func(), error) {
	switch cfg.CheckpointBackend {
	case checkpointBackendClickHouse:
		return newClickHouseCheckpointStore(ctx, cfg, log)
	case checkpointBackendDynamoDB:
		return newDynamoCheckpointStore(ctx, cfg, log)
	default:
		return nil, nil, nil, fmt.Errorf("unsupported checkpoint backend: %s", cfg.CheckpointBackend)
	}
}

func newClickHouseCheckpointStore(
	_ context.Context,
	cfg *Config,
	log *zap.SugaredLogger,
) (checkpointer.Checkpointer, checkpointRemover, func(), error) {
	chClient, err := clickhouse.New(cfg.ClickHouse, log)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create ClickHouse client: %w", err)
	}

	repo, err := chcheckpoint.NewRepository(chClient, cfg.ClickHouse.Cluster, cfg.ClickHouse.Database, cfg.CheckpointTableName)
	if err != nil {
		chClient.Close()
		return nil, nil, nil, fmt.Errorf("failed to create checkpoint repository: %w", err)
	}

	cleanup := func() {
		chClient.Close()
	}
	return repo, repo, cleanup, nil
}

func newDynamoCheckpointStore(
	ctx context.Context,
	cfg *Config,
	log *zap.SugaredLogger,
) (checkpointer.Checkpointer, checkpointRemover, func(), error) {
	repo, err := dyncheckpoint.NewRepository(ctx, dyncheckpoint.Config{
		Region:       cfg.DynamoDBRegion,
		TableName:    cfg.CheckpointTableName,
		CreateTables: cfg.DynamoDBCreateTable,
		EndpointURL:  cfg.DynamoDBEndpointURL,
		Logger:       log,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create DynamoDB checkpoint repository: %w", err)
	}

	return repo, repo, func() {}, nil
}
