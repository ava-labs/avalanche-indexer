package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/urfave/cli/v2"

	"github.com/ava-labs/avalanche-indexer/pkg/data/dynamodb"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
)

func remove(c *cli.Context) error {
	sugar, err := utils.NewSugaredLogger(false)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer sugar.Desugar().Sync() //nolint:errcheck

	ctx := context.Background()

	cfg := dynamodb.Config{
		Region:   c.String("dynamodb-region"),
		Endpoint: c.String("dynamodb-endpoint"),
	}

	client, err := dynamodb.New(ctx, cfg, sugar)
	if err != nil {
		return fmt.Errorf("failed to create DynamoDB client: %w", err)
	}

	tables := []string{
		c.String("dynamodb-history-table"),
		c.String("dynamodb-erc-table"),
	}

	for _, table := range tables {
		sugar.Infow("deleting table", "table", table)
		_, err := client.Inner().DeleteTable(ctx, &awsdynamodb.DeleteTableInput{
			TableName: aws.String(table),
		})
		if err != nil {
			sugar.Warnw("failed to delete table (may not exist)", "table", table, "error", err)
		} else {
			sugar.Infow("table deleted", "table", table)
		}
	}

	return nil
}
