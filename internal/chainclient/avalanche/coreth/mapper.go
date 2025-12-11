package coreth

import (
	"github.com/ava-labs/avalanche-indexer/internal/types"
	"github.com/ava-labs/coreth/plugin/evm/customtypes"
	libevmtypes "github.com/ava-labs/libevm/core/types"
)

func mapToInternalBlock(
	block *libevmtypes.Block,
) *types.EVMBlock {
	txs := mapToInternalTxs(block.Transactions())

	extra := customtypes.GetHeaderExtra(block.Header())
	blockGasCost := extra.BlockGasCost
	var blockTimestampMilliseconds uint64
	var blockMinDelayExcess uint64
	if extra.TimeMilliseconds != nil {
		blockTimestampMilliseconds = *extra.TimeMilliseconds
	}
	if extra.MinDelayExcess != nil {
		blockMinDelayExcess = extra.MinDelayExcess.Delay()
	}

	return &types.EVMBlock{
		BaseFee:               block.BaseFee(),
		GasUsed:               block.GasUsed(),
		GasLimit:              block.GasLimit(),
		BlockGasCost:          blockGasCost,
		Hash:                  block.Hash(),
		Time:                  block.Time(),
		Number:                block.Number(),
		ParentHash:            block.ParentHash(),
		Transactions:          txs,
		TimestampMilliseconds: blockTimestampMilliseconds,
		MinDelayExcess:        blockMinDelayExcess,
	}
}

func mapToInternalTxs(
	txs []*libevmtypes.Transaction,
) []*types.EVMTransaction {
	evmTxs := make([]*types.EVMTransaction, 0, len(txs))

	for _, tx := range txs {
		evmTxs = append(evmTxs, &types.EVMTransaction{
			Type:     tx.Type(),
			Hash:     tx.Hash(),
			To:       tx.To(),
			GasLimit: tx.Gas(),
			Value:    tx.Value(),
			Nonce:    tx.Nonce(),
			Data:     tx.Data(),
		})
	}
	return evmTxs
}
