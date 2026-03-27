package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "coreconsumer",
		Usage: "Consume blocks from Kafka and persist to DynamoDB for glacier-api",
		Commands: []*cli.Command{
			{
				Name:   "run",
				Usage:  "Run the core consumer",
				Flags:  runFlags(),
				Action: run,
			},
			{
				Name:   "remove",
				Usage:  "Remove DynamoDB tables for a given stream",
				Flags:  removeFlags(),
				Action: remove,
			},
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
