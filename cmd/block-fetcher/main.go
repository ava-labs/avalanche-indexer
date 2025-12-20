package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/subscriber"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/worker"
	"github.com/ava-labs/coreth/plugin/evm/customethclient"
	"github.com/ava-labs/coreth/rpc"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var rootCmd = &cobra.Command{
	Use:   "block-fetcher",
	Short: "Fetch blocks continuously from a given RPC endpoint",
	Long:  `Fetch blocks continuously from a given RPC endpoint.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		log, _ := zap.NewProduction()
		defer log.Sync()
		sugar := log.Sugar()
		rpcURL, err := cmd.Flags().GetString("rpc-url")
		if err != nil {
			return fmt.Errorf("failed to get rpc url: %w", err)
		}
		startHeight, err := cmd.Flags().GetUint64("start-height")
		if err != nil {
			return fmt.Errorf("failed to get start height: %w", err)
		}
		endHeight, err := cmd.Flags().GetUint64("end-height")
		if err != nil {
			return fmt.Errorf("failed to get end height: %w", err)
		}
		concurrency, err := cmd.Flags().GetUint64("concurrency")
		if err != nil {
			return fmt.Errorf("failed to get concurrency: %w", err)
		}
		backfillPriority, err := cmd.Flags().GetUint64("backfill-priority")
		if err != nil {
			return fmt.Errorf("failed to get backfill priority: %w", err)
		}
		blocksChCapacity, err := cmd.Flags().GetInt("blocks-ch-capacity")
		if err != nil {
			return fmt.Errorf("failed to get blocks channel capacity: %w", err)
		}
		maxFailures, err := cmd.Flags().GetInt("max-failures")
		if err != nil {
			return fmt.Errorf("failed to get max failures: %w", err)
		}
		sugar.Infof("RPC URL: %s", rpcURL)
		sugar.Infof("Concurrency: %d", concurrency)
		sugar.Infof("Backfill priority: %d", backfillPriority)
		sugar.Infof("Blocks channel capacity: %d", blocksChCapacity)
		sugar.Infof("Max failures per block height: %d", maxFailures)
		sugar.Infof("Start block height: %d", startHeight)

		var fetchLatestHeight bool
		if endHeight == 0 {
			sugar.Infof("End block height: not specified, will fetch until the latest block")
			fetchLatestHeight = true
		} else {
			sugar.Infof("End block height: %d", endHeight)
		}

		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		client, err := rpc.Dial(rpcURL)
		if err != nil {
			return fmt.Errorf("failed to dial rpc: %w", err)
		}
		defer client.Close()

		w, err := worker.NewCorethWorker(ctx, rpcURL)
		if err != nil {
			return fmt.Errorf("failed to create worker: %w", err)
		}

		if fetchLatestHeight {
			endHeight, err = customethclient.New(client).BlockNumber(ctx)
			if err != nil {
				return fmt.Errorf("failed to get latest block height: %w", err)
			}
			sugar.Infof("Latest block height: %d", endHeight)
		}

		s, err := slidingwindow.NewState(startHeight, endHeight)
		if err != nil {
			return fmt.Errorf("failed to create state: %w", err)
		}

		m, err := slidingwindow.NewManager(sugar, s, w, concurrency, backfillPriority, blocksChCapacity, maxFailures)
		if err != nil {
			return fmt.Errorf("failed to create manager: %w", err)
		}

		sub := subscriber.NewCoreth(sugar, customethclient.New(client))
		g, gctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			return sub.Subscribe(gctx, blocksChCapacity, m)
		})
		g.Go(func() error {
			return m.Run(gctx)
		})

		err = g.Wait()
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		if err != nil {
			return err
		}

		sugar.Info("shutting down")
		time.Sleep(250 * time.Millisecond)
		return nil
	},
}

func init() {
	rootCmd.Flags().StringP("rpc-url", "r", "", "The websocket RPC URL to fetch blocks from")
	rootCmd.Flags().Uint64P("start-height", "s", 0, "The start height to fetch blocks from")
	rootCmd.Flags().Uint64P("end-height", "e", 0, "The end height to fetch blocks to")
	rootCmd.Flags().Uint64P("concurrency", "c", 0, "The number of concurrent workers to use")
	rootCmd.Flags().Uint64P("backfill-priority", "b", 0, "The priority of the backfill workers (must be less than concurrency)")
	rootCmd.Flags().IntP("blocks-ch-capacity", "B", 100, "The capacity of the eth_subscribe channel")
	rootCmd.Flags().IntP("max-failures", "f", 3, "The maximum number of block processing failures before stopping")

	rootCmd.MarkFlagRequired("rpc-url")
	rootCmd.MarkFlagRequired("start-height")
	rootCmd.MarkFlagRequired("concurrency")
	rootCmd.MarkFlagRequired("backfill-priority")
}

func main() {
	err := rootCmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
