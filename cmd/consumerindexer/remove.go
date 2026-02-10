package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/evmrepo"
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

	rawBlocksTableName := c.String("raw-blocks-table-name")
	rawTransactionsTableName := c.String("raw-transactions-table-name")
	rawLogsTableName := c.String("raw-logs-table-name")

	chCfg, err := buildClickHouseConfig(c)
	if err != nil {
		return fmt.Errorf("failed to build ClickHouse config: %w", err)
	}
	chClient, err := clickhouse.New(chCfg, sugar)
	if err != nil {
		return fmt.Errorf("failed to create ClickHouse client: %w", err)
	}
	defer chClient.Close()

	rawBlocksRepo, err := evmrepo.NewBlocks(ctx, chClient, chCfg.Cluster, chCfg.Database, rawBlocksTableName)
	if err != nil {
		return fmt.Errorf("failed to create blocks repository: %w", err)
	}
	rawTransactionsRepo, err := evmrepo.NewTransactions(ctx, chClient, chCfg.Cluster, chCfg.Database, rawTransactionsTableName)
	if err != nil {
		return fmt.Errorf("failed to create transactions repository: %w", err)
	}
	rawLogsRepo, err := evmrepo.NewLogs(ctx, chClient, chCfg.Cluster, chCfg.Database, rawLogsTableName)
	if err != nil {
		return fmt.Errorf("failed to create logs repository: %w", err)
	}

	err = rawBlocksRepo.DeleteBlocks(ctx, evmChainID)
	if err != nil {
		return fmt.Errorf("failed to delete blocks: %w", err)
	}

	err = rawTransactionsRepo.DeleteTransactions(ctx, evmChainID)
	if err != nil {
		return fmt.Errorf("failed to delete transactions: %w", err)
	}

	err = rawLogsRepo.DeleteLogs(ctx, evmChainID)
	if err != nil {
		return fmt.Errorf("failed to delete logs: %w", err)
	}

	sugar.Infof("blocks, transactions, and logs successfully removed for chain ID %d", evmChainID)

	return nil
}
