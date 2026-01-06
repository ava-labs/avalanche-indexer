package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
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

	sugar, err := newSugaredLogger(verbose)
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

	// Subscribe to topics
	err = consumer.SubscribeTopics(topics, nil)
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
				sugar.Errorw("consumer error", "error", e)
				return fmt.Errorf("consumer error: %w", e)
			default:
				sugar.Debugw("ignored event", "type", fmt.Sprintf("%T", e))
			}
		}
	}
}

func newSugaredLogger(verbose bool) (*zap.SugaredLogger, error) {
	if verbose {
		l, err := zap.NewDevelopment()
		if err != nil {
			return nil, fmt.Errorf("failed to create development logger: %w", err)
		}
		return l.Sugar(), nil
	}

	l, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("failed to create production logger: %w", err)
	}
	return l.Sugar(), nil
}
