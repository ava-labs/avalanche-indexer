package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ava-labs/avalanche-indexer/cmd/utils"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "consumerindexer",
		Usage: "Consume blocks from Kafka pipeline",
		Commands: []*cli.Command{
			{
				Name:  "run",
				Usage: "Run the consumer indexer",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:    "verbose",
						Aliases: []string{"v"},
						Usage:   "Enable verbose logging",
					},
					&cli.StringFlag{
						Name:     "bootstrap-servers",
						Aliases:  []string{"b"},
						Usage:    "Kafka bootstrap servers (comma-separated)",
						EnvVars:  []string{"KAFKA_BOOTSTRAP_SERVERS"},
						Required: true,
					},
					&cli.StringFlag{
						Name:     "group-id",
						Aliases:  []string{"g"},
						Usage:    "Kafka consumer group ID",
						EnvVars:  []string{"KAFKA_GROUP_ID"},
						Required: true,
					},
					&cli.StringFlag{
						Name:     "topics",
						Aliases:  []string{"t"},
						Usage:    "Kafka topics to consume from (comma-separated)",
						EnvVars:  []string{"KAFKA_TOPICS"},
						Required: true,
					},
					&cli.StringFlag{
						Name:    "auto-offset-reset",
						Aliases: []string{"o"},
						Usage:   "Kafka auto offset reset policy (earliest, latest, none)",
						EnvVars: []string{"KAFKA_AUTO_OFFSET_RESET"},
						Value:   "earliest",
					},
				},
				Action: run,
			},
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(c *cli.Context) error {
	verbose := c.Bool("verbose")
	bootstrapServers := c.String("bootstrap-servers")
	groupID := c.String("group-id")
	topicsStr := c.String("topics")
	autoOffsetReset := c.String("auto-offset-reset")

	sugar, err := utils.NewSugaredLogger(verbose)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer sugar.Desugar().Sync() //nolint:errcheck // best-effort flush; ignore sync errors
	sugar.Infow("config",
		"verbose", verbose,
		"bootstrapServers", bootstrapServers,
		"groupID", groupID,
		"topics", topicsStr,
		"autoOffsetReset", autoOffsetReset,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Create Kafka consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          groupID,
		"auto.offset.reset": autoOffsetReset,
	})
	if err != nil {
		return fmt.Errorf("failed to create Kafka consumer: %w", err)
	}
	defer consumer.Close()

	sugar.Info("Kafka consumer created successfully")

	// Parse topics
	topics := strings.Split(topicsStr, ",")
	for i, topic := range topics {
		topics[i] = strings.TrimSpace(topic)
	}

	// Rebalance callback to handle partition assignment/revocation
	rebalanceCallback := func(c *kafka.Consumer, event kafka.Event) error {
		switch e := event.(type) {
		case kafka.AssignedPartitions:
			partitions := make([]string, len(e.Partitions))
			for i, p := range e.Partitions {
				partitions[i] = fmt.Sprintf("%s[%d]", *p.Topic, p.Partition)
			}
			sugar.Infow("partitions assigned", "partitions", partitions)
			return c.Assign(e.Partitions)
		case kafka.RevokedPartitions:
			partitions := make([]string, len(e.Partitions))
			for i, p := range e.Partitions {
				partitions[i] = fmt.Sprintf("%s[%d]", *p.Topic, p.Partition)
			}
			sugar.Infow("partitions revoked", "partitions", partitions)
			return c.Unassign()
		default:
			return nil
		}
	}

	// Subscribe to topics
	err = consumer.SubscribeTopics(topics, rebalanceCallback)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	sugar.Infow("subscribed to topics", "topics", topics)

	sugar.Info("hello world from consumer indexer")

	// Consumer loop
	for {
		select {
		case <-ctx.Done():
			sugar.Info("shutting down consumer...")
			return nil
		default:
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				sugar.Debugw("received message",
					"topic", *e.TopicPartition.Topic,
					"partition", e.TopicPartition.Partition,
					"offset", e.TopicPartition.Offset,
				)
				// TODO: Process message here
			case kafka.Error:
				if e.Code() == kafka.ErrPartitionEOF {
					sugar.Debugw("reached end of partition", "error", e)
					continue
				}
				if e.IsFatal() {
					sugar.Errorw("fatal kafka error", "code", fmt.Sprintf("%#x", e.Code()), "error", e)
					return fmt.Errorf("fatal kafka error: %w", e)
				}
				if e.Code() == kafka.ErrAllBrokersDown {
					sugar.Errorw("all brokers down", "code", fmt.Sprintf("%#x", e.Code()), "error", e)
					return fmt.Errorf("all brokers down: %w", e)
				}
				// Non-fatal errors are usually informational
				sugar.Warnw("ignoring unexpected kafka error", "code", fmt.Sprintf("%#x", e.Code()), "error", e)
				continue
			default:
				sugar.Debugw("ignored event", "type", fmt.Sprintf("%T", e))
			}
		}
	}
}
