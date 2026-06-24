//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/icmrepo"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka/processor"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"

	kafkamsg "github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	teleportermessenger "github.com/ava-labs/icm-contracts/abi-bindings/go/teleporter/TeleporterMessenger"
	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ICM e2e test constants — stable values shared across helpers and the test body.
const (
	icmE2EContractHex    = "0xaAbBcCdDeEfF001122334455667788990011aAbB"
	icmE2EBlockchainID   = "11111111111111111111111111111111LpoYY"
	icmE2EMessagesTable  = "icm_messages_e2e"
	icmE2ESendTable      = "icm_send_events_e2e"
	icmE2EReceiveTable   = "icm_receive_events_e2e"
	icmE2EExecutedTable  = "icm_message_executed_events_e2e"
	icmE2EExecFailTable  = "icm_message_execution_failed_events_e2e"
	icmE2EReceiptsTable  = "icm_receipts_events_e2e"
	icmE2EFeeInfoTable   = "icm_fee_info_events_e2e"
	icmE2EFeeRedeemTable = "icm_fee_redemptions_events_e2e"
)

var (
	icmE2EContractAddr = common.HexToAddress(icmE2EContractHex)
	icmE2EMsgID        = [32]byte{0x01}
	icmE2EDstChainID   = [32]byte{0x03}
	icmE2ERelayerAddr  = common.HexToAddress("0x1111111111111111111111111111111111111111")
)

// icmE2EEventData ABI-packs the non-indexed arguments for the named Teleporter event.
// Returns nil for events with no non-indexed args (e.g. MessageExecuted).
func icmE2EEventData(t *testing.T, eventName string, args ...any) []byte {
	t.Helper()
	parsedABI, err := teleportermessenger.TeleporterMessengerMetaData.GetAbi()
	require.NoError(t, err)
	event, ok := parsedABI.Events[eventName]
	require.True(t, ok, "event %q not found in ABI", eventName)
	nonIndexed := event.Inputs.NonIndexed()
	if len(nonIndexed) == 0 {
		return nil
	}
	data, err := nonIndexed.Pack(args...)
	require.NoError(t, err)
	return data
}

// icmE2EMinMsg returns a minimal TeleporterMessage for test log construction.
func icmE2EMinMsg() teleportermessenger.TeleporterMessage {
	return teleportermessenger.TeleporterMessage{
		MessageNonce:            big.NewInt(1),
		OriginSenderAddress:     icmE2ERelayerAddr,
		DestinationBlockchainID: icmE2EDstChainID,
		DestinationAddress:      icmE2ERelayerAddr,
		RequiredGasLimit:        big.NewInt(100_000),
		AllowedRelayerAddresses: []common.Address{},
		Receipts:                []teleportermessenger.TeleporterMessageReceipt{},
		Message:                 []byte("e2e"),
	}
}

// icmE2EMinFeeInfo returns a minimal TeleporterFeeInfo for test log construction.
func icmE2EMinFeeInfo() teleportermessenger.TeleporterFeeInfo {
	return teleportermessenger.TeleporterFeeInfo{
		FeeTokenAddress: icmE2ERelayerAddr,
		Amount:          big.NewInt(1_000),
	}
}

// icmE2EBuildSendBlock constructs an EVMBlock whose single transaction contains
// an ABI-encoded SendCrossChainMessage log at the e2e Teleporter contract address.
func icmE2EBuildSendBlock(t *testing.T) *kafkamsg.EVMBlock {
	t.Helper()

	// topic0 = SendCrossChainMessage event signature
	// topic1 = messageID (indexed)
	// topic2 = destinationBlockchainID (indexed)
	l := &kafkamsg.EVMLog{
		Address: icmE2EContractAddr,
		Topics: []common.Hash{
			common.HexToHash("0x2a211ad4a59ab9d003852404f9c57c690704ee755f3c79d2c2812ad32da99df8"),
			common.BytesToHash(icmE2EMsgID[:]),
			common.BytesToHash(icmE2EDstChainID[:]),
		},
		Data: icmE2EEventData(t, "SendCrossChainMessage", icmE2EMinMsg(), icmE2EMinFeeInfo()),
	}

	bcID := icmE2EBlockchainID
	return &kafkamsg.EVMBlock{
		BlockchainID: &bcID,
		EVMChainID:   big.NewInt(43114),
		Number:       big.NewInt(1000),
		Timestamp:    1_700_000_000,
		Transactions: []*kafkamsg.EVMTransaction{{
			Hash:     "0x1111111111111111111111111111111111111111111111111111111111111111",
			GasPrice: big.NewInt(25_000_000_000),
			Receipt: &kafkamsg.EVMTxReceipt{
				GasUsed:           100_000,
				EffectiveGasPrice: big.NewInt(30_000_000_000),
				Logs:              []*kafkamsg.EVMLog{l},
			},
		}},
	}
}

// TestE2EICMConsumerIndexer verifies the full ICM write pipeline end-to-end:
// a SendCrossChainMessage log produced to Kafka is consumed and persisted to both
// icm_send_events and the icm_messages AggregatingMergeTree. The FINAL query on
// icm_messages is the one assertion not reachable from unit tests — it exercises
// the ClickHouse merge engine against a real server.
func TestE2EICMConsumerIndexer(t *testing.T) {
	kafkaBrokers := getEnvStr("KAFKA_BROKERS", "localhost:9092")
	testID := time.Now().UnixNano()
	kafkaTopic := fmt.Sprintf("icm_consumer_test_%d", testID)
	groupID := fmt.Sprintf("e2e-icm-%d", testID)

	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)
	defer log.Desugar().Sync() //nolint:errcheck

	// ---- ClickHouse: create tables, then wipe any stale e2e data ----
	chClient, err := clickhouse.New(clickhouseTestConfig, log)
	require.NoError(t, err, "clickhouse connection failed (is docker-compose up?)")
	defer chClient.Close()

	messagesRepo, err := icmrepo.NewMessages(ctx, chClient, "default", "default", icmE2EMessagesTable)
	require.NoError(t, err)
	sendRepo, err := icmrepo.NewSendEvents(ctx, chClient, "default", "default", icmE2ESendTable)
	require.NoError(t, err)
	receiveRepo, err := icmrepo.NewReceiveEvents(ctx, chClient, "default", "default", icmE2EReceiveTable)
	require.NoError(t, err)
	executedRepo, err := icmrepo.NewMessageExecutedEvents(ctx, chClient, "default", "default", icmE2EExecutedTable)
	require.NoError(t, err)
	execFailedRepo, err := icmrepo.NewMessageExecutionFailedEvents(ctx, chClient, "default", "default", icmE2EExecFailTable)
	require.NoError(t, err)
	receiptsRepo, err := icmrepo.NewReceiptEvents(ctx, chClient, "default", "default", icmE2EReceiptsTable)
	require.NoError(t, err)
	feeInfoRepo, err := icmrepo.NewAddFeeEvents(ctx, chClient, "default", "default", icmE2EFeeInfoTable)
	require.NoError(t, err)
	feeRedemptionsRepo, err := icmrepo.NewRelayerRewardRedeemedEvents(ctx, chClient, "default", "default", icmE2EFeeRedeemTable)
	require.NoError(t, err)

	for _, tbl := range []string{
		icmE2EMessagesTable,
		icmE2ESendTable,
		icmE2EReceiveTable,
		icmE2EExecutedTable,
		icmE2EExecFailTable,
		icmE2EReceiptsTable,
		icmE2EFeeInfoTable,
		icmE2EFeeRedeemTable,
	} {
		require.NoError(t,
			chClient.Conn().Exec(ctx, "TRUNCATE TABLE IF EXISTS "+tbl),
			"truncate %s", tbl,
		)
	}

	// ---- Produce a single SendCrossChainMessage block to Kafka ----
	block := icmE2EBuildSendBlock(t)
	blockJSON, err := json.Marshal(block)
	require.NoError(t, err)

	producer, err := ckafka.NewProducer(&ckafka.ConfigMap{"bootstrap.servers": kafkaBrokers})
	require.NoError(t, err)
	defer producer.Close()

	deliveryCh := make(chan ckafka.Event, 1)
	require.NoError(t, producer.Produce(&ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: &kafkaTopic, Partition: ckafka.PartitionAny},
		Value:          blockJSON,
	}, deliveryCh))
	msg := (<-deliveryCh).(*ckafka.Message)
	require.NoError(t, msg.TopicPartition.Error, "kafka delivery failed")
	producer.Flush(producerFlushTimeout)

	// ---- Start ICM consumer ----
	registry := prometheus.NewRegistry()
	m, err := metrics.NewWithLabels(registry, metrics.Labels{
		EVMChainID:    43114,
		Environment:   "test",
		Region:        "local",
		CloudProvider: "local",
	})
	require.NoError(t, err)

	proc, err := processor.NewICMProcessor(
		log,
		messagesRepo, sendRepo, receiveRepo,
		executedRepo, execFailedRepo,
		receiptsRepo, feeInfoRepo, feeRedemptionsRepo,
		[]string{icmE2EContractHex},
		m,
		nil,
	)
	require.NoError(t, err)

	consumer, err := kafka.NewConsumer(ctx, log, kafka.ConsumerConfig{
		BootstrapServers:            kafkaBrokers,
		GroupID:                     groupID,
		Topic:                       kafkaTopic,
		AutoOffsetReset:             "earliest",
		Concurrency:                 1,
		OffsetManagerCommitInterval: 2 * time.Second,
		PublishToDLQ:                false,
		EnableLogs:                  false,
		SessionTimeout:              durationPtr(10 * time.Second),
		MaxPollInterval:             durationPtr(30 * time.Second),
	}, proc, nil)
	require.NoError(t, err)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return consumer.Start(gctx) })

	// ---- Verify icm_send_events_e2e ----
	require.Eventually(t, func() bool {
		return queryCount(t, ctx, chClient,
			"SELECT COUNT(*) FROM "+icmE2ESendTable) >= 1
	}, 30*time.Second, 500*time.Millisecond,
		"icm_send_events_e2e should have 1 row after Send event is processed")

	// message_id is stored as FixedString(32) raw bytes; compare against the known test value.
	wantMsgID := mustHexToFixed32(t, common.Hash(icmE2EMsgID).Hex())
	require.Equal(t, wantMsgID,
		queryFixedString(t, ctx, chClient, fmt.Sprintf("SELECT message_id FROM %s LIMIT 1", icmE2ESendTable)),
		"message_id mismatch in icm_send_events_e2e")

	// blockchain_id is stored as plain String (CB58).
	require.Equal(t, icmE2EBlockchainID,
		queryString(t, ctx, chClient, fmt.Sprintf("SELECT blockchain_id FROM %s LIMIT 1", icmE2ESendTable)),
		"blockchain_id mismatch in icm_send_events_e2e")

	// ---- Verify icm_messages_e2e FINAL ----
	// The partial-send row written by handleSend must be visible under FINAL, which forces
	// the AggregatingMergeTree to apply its aggregation functions before returning results.
	// This is the assertion that cannot be exercised by unit tests with mock repos.
	require.Eventually(t, func() bool {
		return queryCount(t, ctx, chClient,
			"SELECT COUNT(*) FROM "+icmE2EMessagesTable+" FINAL") >= 1
	}, 10*time.Second, 500*time.Millisecond,
		"icm_messages_e2e FINAL should have 1 merged row")

	require.Equal(t, icmE2EBlockchainID,
		queryString(t, ctx, chClient,
			fmt.Sprintf("SELECT source_blockchain_id FROM %s FINAL LIMIT 1", icmE2EMessagesTable)),
		"source_blockchain_id mismatch in icm_messages_e2e FINAL")

	// ---- Shutdown ----
	cancel()
	require.NoError(t, g.Wait())

	t.Log("ICM consumer indexer e2e test completed successfully")
}
