package worker

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/types"
	"github.com/ava-labs/coreth/plugin/evm/customethclient"
	"github.com/ava-labs/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/coreth/rpc"
	libevmtypes "github.com/ava-labs/libevm/core/types"
)

type CorethWorker struct {
	client         *customethclient.Client
	producer       *kafka.Producer
	schemaRegistry schemaregistry.Client
	serializer     serde.Serializer
	topic          string
}

func NewCorethWorker(
	ctx context.Context,
	url string,
	producer *kafka.Producer,
	schemaRegistry schemaregistry.Client,
	serializer serde.Serializer,
	topic string,
) (*CorethWorker, error) {
	customtypes.Register()

	c, err := rpc.DialContext(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("dial coreth rpc: %w", err)
	}
	return &CorethWorker{
		client:         customethclient.New(c),
		producer:       producer,
		schemaRegistry: schemaRegistry,
		serializer:     serializer,
		topic:          topic,
	}, nil
}

func (w *CorethWorker) Process(ctx context.Context, height uint64) error {
	h := new(big.Int).SetUint64(height)
	block, err := w.client.BlockByNumber(ctx, h)
	if err != nil {
		return fmt.Errorf("fetch block %d: %w", height, err)
	}

	corethBlock := createCorethBlock(block)
	bytes, err := w.serializer.Serialize(w.topic, &corethBlock)
	if err != nil {
		return fmt.Errorf("serialize block %d: %w", height, err)
	}

	err = w.producer.Produce(ctx, kafka.Msg{
		Topic: w.topic,
		Value: bytes,
		Key:   []byte(block.Number().String()),
	})

	if err != nil {
		return fmt.Errorf("failed to produce block %d: %w", height, err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(300 * time.Millisecond):
	}

	fmt.Printf("processed block %d | hash=%s | txs=%d\n",
		height, block.Hash().Hex(), len(block.Transactions()))

	return nil
}

func createCorethBlock(block *libevmtypes.Block) *types.CorethBlock {
	return &types.CorethBlock{
		Number:        block.Number(),
		GasLimit:      block.GasLimit(),
		GasUsed:       block.GasUsed(),
		Difficulty:    block.Difficulty(),
		Time:          block.Time(),
		NumberU64:     block.NumberU64(),
		MixDigest:     block.MixDigest(),
		Nonce:         block.Nonce(),
		Bloom:         block.Bloom(),
		Coinbase:      block.Coinbase(),
		Root:          block.Root(),
		ParentHash:    block.ParentHash(),
		TxHash:        block.TxHash(),
		ReceiptHash:   block.ReceiptHash(),
		UncleHash:     block.UncleHash(),
		Extra:         block.Extra(),
		BaseFee:       block.BaseFee(),
		BeaconRoot:    block.BeaconRoot(),
		ExcessBlobGas: block.ExcessBlobGas(),
		BlobGasUsed:   block.BlobGasUsed(),
		Withdrawals:   block.Withdrawals(),
		Transactions:  createCorethTransactions(block.Transactions()),
	}
}

func createCorethTransactions(transactions []*libevmtypes.Transaction) []*types.CorethTransaction {
	corethTransactions := make([]*types.CorethTransaction, len(transactions))
	for i, transaction := range transactions {
		corethTransactions[i] = &types.CorethTransaction{
			Hash:                 transaction.Hash(),
			To:                   transaction.To(),
			MsgGasPrice:          transaction.GasPrice(),
			MaxPriorityFeePerGas: transaction.GasTipCap(),
			MaxFeePerGas:         transaction.GasFeeCap(),
			Value:                transaction.Value(),
			Type:                 transaction.Type(),
			Nonce:                transaction.Nonce(),
			Data:                 transaction.Data(),
		}
	}
	return corethTransactions
}
