package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/ava-labs/avalanche-indexer/pkg/utils"
)

func remove(c *cli.Context) error {
	ctx := context.Background()
	sugar, err := utils.NewSugaredLogger(true)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer sugar.Desugar().Sync() //nolint:errcheck // best-effort flush; ignore sync errors

	evmChainID := c.Uint64("evm-chain-id")
	if evmChainID == 0 {
		return errors.New("evm chain ID is required")
	}

	checkpointCfg, err := buildCheckpointConfig(c, false)
	if err != nil {
		return err
	}

	cfg := &Config{
		ClickHouse:          checkpointCfg.ClickHouseConfig,
		CheckpointBackend:   checkpointCfg.Backend,
		CheckpointTableName: checkpointCfg.TableName,
		DynamoDBRegion:      checkpointCfg.DynamoDBRegion,
		DynamoDBCreateTable: checkpointCfg.DynamoDBCreate,
		DynamoDBEndpointURL: checkpointCfg.DynamoDBEndpoint,
	}

	_, store, cleanupCheckpointStore, err := newCheckpointStore(ctx, cfg, sugar)
	if err != nil {
		return err
	}
	defer cleanupCheckpointStore()

	err = store.DeleteCheckpoints(ctx, evmChainID)
	if err != nil {
		return fmt.Errorf("failed to delete checkpoints: %w", err)
	}

	sugar.Infof("checkpoints successfully removed for chain ID %d", evmChainID)

	return nil
}
