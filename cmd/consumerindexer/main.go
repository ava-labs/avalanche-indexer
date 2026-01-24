package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "consumerindexer",
		Usage: "Consume blocks from Kafka pipeline",
		Commands: []*cli.Command{
			{
				Name:   "run",
				Usage:  "Run the consumer indexer",
				Flags:  flags(),
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
