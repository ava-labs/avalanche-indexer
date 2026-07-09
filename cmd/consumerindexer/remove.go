package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/evmrepo"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/icmrepo"
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
	internalTransactionsTableName := c.String("internal-transactions-table-name")
	icmSendEventsTableName := c.String("icm-send-events-table-name")
	icmReceiveEventsTableName := c.String("icm-receive-events-table-name")
	icmMessageExecutedEventsTableName := c.String("icm-message-executed-events-table-name")
	icmMessageExecutionFailedEventsTableName := c.String("icm-message-execution-failed-events-table-name")
	icmReceiptsEventsTableName := c.String("icm-receipts-events-table-name")
	icmFeeInfoEventsTableName := c.String("icm-fee-info-events-table-name")
	icmFeeRedemptionsEventsTableName := c.String("icm-fee-redemptions-events-table-name")

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
	internalTransactionsRepo, err := evmrepo.NewInternalTransactions(ctx, chClient, chCfg.Cluster, chCfg.Database, internalTransactionsTableName)
	if err != nil {
		return fmt.Errorf("failed to create internal transactions repository: %w", err)
	}
	icmSendRepo, err := icmrepo.NewSendEvents(ctx, chClient, chCfg.Cluster, chCfg.Database, icmSendEventsTableName)
	if err != nil {
		return fmt.Errorf("failed to create ICM send events repository: %w", err)
	}
	icmReceiveRepo, err := icmrepo.NewReceiveEvents(ctx, chClient, chCfg.Cluster, chCfg.Database, icmReceiveEventsTableName)
	if err != nil {
		return fmt.Errorf("failed to create ICM receive events repository: %w", err)
	}
	icmMessageExecutedRepo, err := icmrepo.NewMessageExecutedEvents(ctx, chClient, chCfg.Cluster, chCfg.Database, icmMessageExecutedEventsTableName)
	if err != nil {
		return fmt.Errorf("failed to create ICM message executed events repository: %w", err)
	}
	icmMessageExecutionFailedRepo, err := icmrepo.NewMessageExecutionFailedEvents(ctx, chClient, chCfg.Cluster, chCfg.Database, icmMessageExecutionFailedEventsTableName)
	if err != nil {
		return fmt.Errorf("failed to create ICM message execution failed events repository: %w", err)
	}
	icmReceiptsRepo, err := icmrepo.NewReceiptEvents(ctx, chClient, chCfg.Cluster, chCfg.Database, icmReceiptsEventsTableName)
	if err != nil {
		return fmt.Errorf("failed to create ICM receipt events repository: %w", err)
	}
	icmFeeInfoRepo, err := icmrepo.NewAddFeeEvents(ctx, chClient, chCfg.Cluster, chCfg.Database, icmFeeInfoEventsTableName)
	if err != nil {
		return fmt.Errorf("failed to create ICM add fee events repository: %w", err)
	}
	icmFeeRedemptionsRepo, err := icmrepo.NewRelayerRewardRedeemedEvents(ctx, chClient, chCfg.Cluster, chCfg.Database, icmFeeRedemptionsEventsTableName)
	if err != nil {
		return fmt.Errorf("failed to create ICM relayer reward redeemed events repository: %w", err)
	}

	if err = rawBlocksRepo.DeleteBlocks(ctx, evmChainID); err != nil {
		return fmt.Errorf("failed to delete blocks: %w", err)
	}
	if err = rawTransactionsRepo.DeleteTransactions(ctx, evmChainID); err != nil {
		return fmt.Errorf("failed to delete transactions: %w", err)
	}
	if err = rawLogsRepo.DeleteLogs(ctx, evmChainID); err != nil {
		return fmt.Errorf("failed to delete logs: %w", err)
	}
	if err = internalTransactionsRepo.DeleteInternalTransactions(ctx, evmChainID); err != nil {
		return fmt.Errorf("failed to delete internal transactions: %w", err)
	}
	if err = icmSendRepo.DeleteSendEvents(ctx, evmChainID); err != nil {
		return fmt.Errorf("failed to delete ICM send events: %w", err)
	}
	if err = icmReceiveRepo.DeleteReceiveEvents(ctx, evmChainID); err != nil {
		return fmt.Errorf("failed to delete ICM receive events: %w", err)
	}
	if err = icmMessageExecutedRepo.DeleteMessageExecutedEvents(ctx, evmChainID); err != nil {
		return fmt.Errorf("failed to delete ICM message executed events: %w", err)
	}
	if err = icmMessageExecutionFailedRepo.DeleteMessageExecutionFailedEvents(ctx, evmChainID); err != nil {
		return fmt.Errorf("failed to delete ICM message execution failed events: %w", err)
	}
	if err = icmReceiptsRepo.DeleteReceiptEvents(ctx, evmChainID); err != nil {
		return fmt.Errorf("failed to delete ICM receipt events: %w", err)
	}
	if err = icmFeeInfoRepo.DeleteAddFeeEvents(ctx, evmChainID); err != nil {
		return fmt.Errorf("failed to delete ICM add fee events: %w", err)
	}
	if err = icmFeeRedemptionsRepo.DeleteRelayerRewardRedeemedEvents(ctx, evmChainID); err != nil {
		return fmt.Errorf("failed to delete ICM relayer reward redeemed events: %w", err)
	}

	// icm_messages is intentionally skipped. It is a global cross-chain table whose merge key
	// is (source_blockchain_id, destination_blockchain_id, message_id) — it has no evm_chain_id
	// column and cannot be cleaned by chain. Rows must be removed manually if needed.
	sugar.Infow("tables cleaned for chain ID",
		"evmChainID", evmChainID,
		"skipped", "icm_messages (global cross-chain table, not deletable by chain ID)",
	)

	return nil
}
