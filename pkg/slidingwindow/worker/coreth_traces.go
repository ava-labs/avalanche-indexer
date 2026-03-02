package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/ava-labs/coreth/eth/tracers"
	"github.com/ava-labs/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/coreth/rpc"
	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"

	metricslib "github.com/ava-labs/avalanche-indexer/pkg/metrics"
)

type CorethTracesWorker struct {
	client       *rpc.Client
	producer     *kafka.Producer
	topic        string
	evmChainID   *big.Int
	blockchainID *string
	log          *zap.SugaredLogger
	metrics      *metricslib.Metrics
	traceTimeout time.Duration // Timeout for fetching block traces
}

func NewCorethTracesWorker(
	client *rpc.Client,
	producer *kafka.Producer,
	topic string,
	evmChainID uint64,
	blockchainID string,
	log *zap.SugaredLogger,
	metrics *metricslib.Metrics,
	traceTimeout time.Duration,
) (*CorethTracesWorker, error) {
	if metrics == nil {
		metrics = metricslib.NewNoOp()
	}
	RegisterCustomTypesOnce.Do(func() {
		customtypes.Register()
	})

	return &CorethTracesWorker{
		client:       client,
		producer:     producer,
		topic:        topic,
		evmChainID:   new(big.Int).SetUint64(evmChainID),
		blockchainID: &blockchainID,
		log:          log,
		metrics:      metrics,
		traceTimeout: traceTimeout,
	}, nil
}

func (ctw *CorethTracesWorker) Process(ctx context.Context, height uint64) error {
	ctw.log.Debugw("worker starting block processing", "height", height)

	traces, err := ctw.FetchBlockTraces(ctx, height)
	if err != nil {
		return fmt.Errorf("fetch block traces failed %d: %w", height, err)
	}

	ctw.log.Debugw("block traces fetched, serializing", "height", height, "traces", len(traces))
	bytes, err := messages.MarshalEVMBlockTrace(height, traces, ctw.evmChainID, ctw.blockchainID)
	if err != nil {
		return fmt.Errorf("serialize block traces failed %d: %w", height, err)
	}

	ctw.log.Debugw("block traces serialized, producing to kafka", "height", height, "bytes", len(bytes))
	produceStart := time.Now()
	err = ctw.producer.Produce(ctx, kafka.Msg{
		Topic: ctw.topic,
		Value: bytes,
		Key:   []byte(strconv.FormatUint(height, 10)),
	})
	if err != nil {
		return fmt.Errorf("produce block traces failed %d: %w", height, err)
	}
	ctw.log.Debugw("kafka produce completed", "height", height, "duration_ms", time.Since(produceStart).Milliseconds())

	ctw.log.Debugw("processed block traces",
		"height", height,
		"traces", len(traces),
	)
	return nil
}

func (ctw *CorethTracesWorker) FetchBlockTraces(ctx context.Context, height uint64) ([]json.RawMessage, error) {
	const method = "debug_traceBlockByNumber"
	start := time.Now()

	ctw.metrics.IncRPCInFlight()
	defer ctw.metrics.DecRPCInFlight()

	ctxTimeout, cancel := context.WithTimeout(ctx, ctw.traceTimeout)
	defer cancel()

	tracer := "callTracer"
	tracerTimeout := fmt.Sprintf("%ds", ctw.traceTimeout/time.Second)
	traceConfig := &tracers.TraceConfig{
		Timeout: &tracerTimeout,
		Tracer:  &tracer,
	}

	var traces []json.RawMessage
	err := ctw.client.CallContext(ctxTimeout, &traces, method, fmt.Sprintf("0x%x", height), traceConfig)
	rpcDuration := time.Since(start)

	ctw.metrics.RecordRPCCall(method, err, rpcDuration.Seconds())

	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			ctw.log.Debugw("debug_traceBlockByNumber canceled due to context cancellation or deadline exceeded", "height", height, "error", err)
		} else {
			ctw.log.Warnw("debug_traceBlockByNumber failed", "height", height, "error", err, "duration_ms", rpcDuration.Milliseconds())
		}
		return nil, fmt.Errorf("%w for block %d: %w", ErrTracesFetchFailed, height, err)
	}
	return traces, nil
}
