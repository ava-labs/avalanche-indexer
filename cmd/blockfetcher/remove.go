package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/checkpoint"
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

	checkpointsTableName := c.String("checkpoint-table-name")

	chCfg := clickhouse.Load()
	chClient, err := clickhouse.New(chCfg, sugar)
	if err != nil {
		return fmt.Errorf("failed to create ClickHouse client: %w", err)
	}
	defer chClient.Close()

	repo := checkpoint.NewRepository(chClient, checkpointsTableName)

	err = repo.DeleteCheckpoints(ctx, evmChainID)
	if err != nil {
		return fmt.Errorf("failed to delete checkpoints: %w", err)
	}

	sugar.Infof("checkpoints successfully removed for chain ID %d", evmChainID)

	return nil
}
