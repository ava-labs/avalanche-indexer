package evmrepo

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/data/dynamodb"
	"github.com/ava-labs/avalanche-indexer/pkg/evmparser"
	kafkamsg "github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
)

// Repository handles writing EVM block data to DynamoDB in the format
// that glacier-api expects to read.
type Repository struct {
	client       *dynamodb.Client
	historyTable string
	ercTable     string
	log          *zap.SugaredLogger
}

// NewRepository creates a new EVM DynamoDB repository.
func NewRepository(
	client *dynamodb.Client,
	historyTable string,
	ercTable string,
	log *zap.SugaredLogger,
) *Repository {
	return &Repository{
		client:       client,
		historyTable: historyTable,
		ercTable:     ercTable,
		log:          log,
	}
}

// Initialize ensures the history and ERC tables exist.
func (r *Repository) Initialize(ctx context.Context) error {
	if err := r.client.CreateTable(ctx, HistoryTableDefinition(r.historyTable)); err != nil {
		return fmt.Errorf("create history table: %w", err)
	}
	if err := r.client.CreateTable(ctx, ERCTableDefinition(r.ercTable)); err != nil {
		return fmt.Errorf("create ERC table: %w", err)
	}
	return nil
}

// WriteBlock persists all DynamoDB items for a single block: block header, transactions,
// address interactions, ERC transfers, logs, topics, and ERC table updates.
func (r *Repository) WriteBlock(
	ctx context.Context,
	block *kafkamsg.EVMBlock,
	cumulativeTxs uint64,
) error {
	blockNumber := uint64(0)
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	// Collect all history table write requests
	var historyRequests []types.WriteRequest

	// 1. Block record
	historyRequests = append(historyRequests, CreateBlockWriteRequest(block, cumulativeTxs))

	// Collect ERC table updates (deduplicated)
	var ercUpdates []*ERCUpdateItem

	// 2. Process each transaction
	for i, tx := range block.Transactions {
		txIndex := uint(i)

		// Parse ERC transfers from receipt logs
		erc20s := evmparser.ParseERC20Transfers(tx)
		erc721s := evmparser.ParseERC721Transfers(tx)
		erc1155s := evmparser.ParseERC1155Transfers(tx)

		// Native transaction insert
		historyRequests = append(historyRequests, CreateNativeTxWriteRequest(tx, block, txIndex))

		// Native receivables (sender + receiver)
		historyRequests = append(historyRequests, CreateNativeReceivableWriteRequests(tx, block, txIndex)...)

		// Address interactions
		historyRequests = append(historyRequests, CreateInteractionWriteRequests(tx, block, txIndex, erc20s, erc721s, erc1155s)...)

		// ERC-20 transfers
		for _, transfer := range erc20s {
			historyRequests = append(historyRequests, CreateERC20InsertWriteRequest(transfer, block, txIndex))
			historyRequests = append(historyRequests, CreateERC20ReceivableWriteRequests(transfer, block, txIndex)...)

			// ERC table updates
			ercUpdates = append(ercUpdates,
				CreateERCContractUpdate(r.ercTable, evmparser.ERC20, transfer.ContractAddress),
				CreateERC20InteractionUpdate(r.ercTable, transfer.From, transfer.ContractAddress),
				CreateERC20InteractionUpdate(r.ercTable, transfer.To, transfer.ContractAddress),
			)
		}

		// ERC-721 transfers
		for _, transfer := range erc721s {
			historyRequests = append(historyRequests, CreateERC721InsertWriteRequest(transfer, block, txIndex))
			historyRequests = append(historyRequests, CreateERC721ReceivableWriteRequests(transfer, block, txIndex)...)

			// ERC table updates
			ercUpdates = append(ercUpdates,
				CreateERCContractUpdate(r.ercTable, evmparser.ERC721, transfer.ContractAddress),
				CreateERCTokenExistenceUpdate(r.ercTable, evmparser.ERC721, transfer.ContractAddress, transfer.TokenID),
				CreateERC721OwnerUpdate(r.ercTable, transfer.ContractAddress, transfer.TokenID, transfer.To),
			)
		}

		// ERC-1155 transfers
		for _, transfer := range erc1155s {
			historyRequests = append(historyRequests, CreateERC1155InsertWriteRequest(transfer, block, txIndex))
			historyRequests = append(historyRequests, CreateERC1155ReceivableWriteRequests(transfer, block, txIndex)...)

			// ERC table updates
			ercUpdates = append(ercUpdates,
				CreateERCContractUpdate(r.ercTable, evmparser.ERC1155, transfer.ContractAddress),
				CreateERCTokenExistenceUpdate(r.ercTable, evmparser.ERC1155, transfer.ContractAddress, transfer.TokenID),
				CreateERC1155InteractionUpdate(r.ercTable, transfer.From, transfer.ContractAddress, transfer.TokenID),
				CreateERC1155InteractionUpdate(r.ercTable, transfer.To, transfer.ContractAddress, transfer.TokenID),
			)
		}

		// Note: Log and topic items are NOT written to DynamoDB because
		// glacier-api does not read them. This saves significant DynamoDB write costs
		// (a block with 100 txs and 500 logs would produce ~2500 extra items).
	}

	// Write history table items in batches
	r.log.Debugw("writing block to DynamoDB",
		"blockNumber", blockNumber,
		"historyItems", len(historyRequests),
		"ercUpdates", len(ercUpdates),
	)

	if err := r.client.BatchWriteItems(ctx, r.historyTable, historyRequests); err != nil {
		return fmt.Errorf("batch write history items for block %d: %w", blockNumber, err)
	}

	// ERC table uses UpdateItem (not BatchWrite) because it uses update expressions
	for _, update := range ercUpdates {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		input := &awsdynamodb.UpdateItemInput{
			TableName:                 aws.String(update.TableName),
			Key:                       update.Key,
			UpdateExpression:          aws.String(update.UpdateExpression),
			ExpressionAttributeNames:  update.ExpressionAttributeNames,
			ExpressionAttributeValues: update.ExpressionAttributeValues,
		}
		if err := r.client.UpdateItem(ctx, input); err != nil {
			return fmt.Errorf("update ERC item for block %d: %w", blockNumber, err)
		}
	}

	r.log.Debugw("successfully wrote block to DynamoDB",
		"blockNumber", blockNumber,
		"historyItems", len(historyRequests),
		"ercUpdates", len(ercUpdates),
	)

	return nil
}
