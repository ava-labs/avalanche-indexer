package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/ava-labs/subnet-evm/eth/tracers"
	"github.com/ava-labs/subnet-evm/plugin/evm/customtypes"
	"github.com/ava-labs/subnet-evm/rpc"
	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"

	metricslib "github.com/ava-labs/avalanche-indexer/pkg/metrics"
)

type SubnetEVMTracesWorker struct {
	client       *rpc.Client
	producer     *kafka.Producer
	topic        string
	evmChainID   *big.Int
	blockchainID *string
	log          *zap.SugaredLogger
	metrics      *metricslib.Metrics
	traceTimeout time.Duration // Timeout for fetching block traces
}

func NewSubnetEVMTracesWorker(
	client *rpc.Client,
	producer *kafka.Producer,
	topic string,
	evmChainID uint64,
	blockchainID string,
	log *zap.SugaredLogger,
	metrics *metricslib.Metrics,
	traceTimeout time.Duration,
) (*SubnetEVMTracesWorker, error) {
	if metrics == nil {
		metrics = metricslib.NewNoOp()
	}
	RegisterCustomTypesOnce.Do(func() {
		customtypes.Register()
	})

	return &SubnetEVMTracesWorker{
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

func (stw *SubnetEVMTracesWorker) Process(ctx context.Context, height uint64) error {
	stw.log.Debugw("worker starting block processing", "height", height)

	traces, err := stw.FetchBlockTraces(ctx, height)
	if err != nil {
		return fmt.Errorf("fetch block traces failed %d: %w", height, err)
	}

	stw.log.Debugw("block traces fetched, serializing", "height", height, "traces", len(traces))
	bytes, err := messages.MarshalEVMBlockTrace(height, traces, stw.evmChainID, stw.blockchainID)
	if err != nil {
		return fmt.Errorf("serialize block traces failed %d: %w", height, err)
	}

	stw.log.Debugw("block traces serialized, producing to kafka", "height", height, "bytes", len(bytes))
	produceStart := time.Now()
	err = stw.producer.Produce(ctx, kafka.Msg{
		Topic: stw.topic,
		Value: bytes,
		Key:   []byte(strconv.FormatUint(height, 10)),
	})
	if err != nil {
		return fmt.Errorf("produce block traces failed %d: %w", height, err)
	}
	stw.log.Debugw("kafka produce completed", "height", height, "duration_ms", time.Since(produceStart).Milliseconds())

	stw.log.Debugw("processed block traces",
		"height", height,
		"traces", len(traces),
	)
	return nil
}

func (stw *SubnetEVMTracesWorker) FetchBlockTraces(ctx context.Context, height uint64) ([]json.RawMessage, error) {
	const method = "debug_traceBlockByNumber"
	start := time.Now()

	stw.metrics.IncRPCInFlight()
	defer stw.metrics.DecRPCInFlight()

	ctxTimeout, cancel := context.WithTimeout(ctx, stw.traceTimeout)
	defer cancel()

	tracer := "callTracer"
	tracerTimeout := fmt.Sprintf("%ds", stw.traceTimeout/time.Second)
	traceConfig := &tracers.TraceConfig{
		Timeout: &tracerTimeout,
		Tracer:  &tracer,
	}

	var traces []json.RawMessage
	err := stw.client.CallContext(ctxTimeout, &traces, method, fmt.Sprintf("0x%x", height), traceConfig)
	rpcDuration := time.Since(start)

	stw.metrics.RecordRPCCall(method, err, rpcDuration.Seconds())

	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			stw.log.Debugw("debug_traceBlockByNumber canceled due to context cancellation or deadline exceeded", "height", height, "error", err)
		} else {
			stw.log.Warnw("debug_traceBlockByNumber failed", "height", height, "error", err, "duration_ms", rpcDuration.Milliseconds())
		}
		return nil, fmt.Errorf("%w for block %d: %w", ErrTracesFetchFailed, height, err)
	}
	return traces, nil
}
