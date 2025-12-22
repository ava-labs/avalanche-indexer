package main

import (
	"context"
	"encoding/json"
	"math/big"
	"net"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/evm/raw"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka/message"
	msgtypes "github.com/ava-labs/avalanche-indexer/pkg/kafka/message/types"
	"github.com/ava-labs/libevm/common"
)

func main() {
	opts := &clickhouse.Options{
		Addr: []string{"addrs"},
		Auth: clickhouse.Auth{
			Database: "database",
			Username: "username",
			Password: "password",
		},
		DialTimeout: 10 * time.Second,
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	}

	conn, _ := clickhouse.Open(opts)

	// pretend to receive kafka message
	msg := []byte(`{"type":"block","version":1,"id":"123","ts":"2024-06-01T12:00:00Z","data":{"number":"100","hash":"0xabc","parent_hash":"0xdef","transactions":[]}}`)
	envelope, _ := message.Open(msg)

	switch envelope.Type {
	case message.BlockType:
		var blk msgtypes.Block
		_ = json.Unmarshal(envelope.Data, &blk)
		_ = insertTx(conn, raw.Transaction{
			Hash: blk.Transactions[0].Hash,
			BlockNumber: *big.NewInt(blk.Number.Int64()),
			To: common.HexToAddress(blk.Transactions[0].To),
			From: common.HexToAddress(blk.Transactions[0].From),
		})
	}
}

// Insert tx into raw transactions table in ClickHouse
func insertTx(conn clickhouse.Conn, tx raw.Transaction) error {
	txRepo := raw.NewRepo(conn)
	txRepo.Insert(raw.Transaction{
		Hash:        common.HexToHash("0x123"),
		BlockNumber: *big.NewInt(100),
		To:          common.HexToAddress("0xabc"),
		From:        common.HexToAddress("0xdef"),
	})
}
