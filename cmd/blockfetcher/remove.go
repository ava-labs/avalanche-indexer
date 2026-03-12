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

	mode := c.String("mode")
	if mode == "" {
		return errors.New("mode is required")
	}

	checkpointCfg, err := buildCheckpointConfig(c, false)
	if err != nil {
		return err
	}

	cfg := &Config{
		ClickHouse:          checkpointCfg.ClickHouseConfig,
		CheckpointBackend:   checkpointCfg.Backend,
		CheckpointTableName: checkpointCfg.TableName,
		DynamoDB:            checkpointCfg.DynamoDBConfig,
	}

	store, err := newCheckpointStore(ctx, cfg, sugar)
	if err != nil {
		return err
	}
	defer store.Close()

	err = store.Delete(ctx, evmChainID, mode)
	if err != nil {
		return fmt.Errorf("failed to delete checkpoints: %w", err)
	}

	sugar.Infof("checkpoints successfully removed for chain ID %d", evmChainID)

	return nil
}
