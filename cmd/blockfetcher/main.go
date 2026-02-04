package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "blockfetcher",
		Usage: "Fetch blocks from a given RPC endpoint",
		Commands: []*cli.Command{
			{
				Name:   "run",
				Usage:  "Run the block fetcher",
				Flags:  runFlags(),
				Action: run,
			},
			{
				Name:   "remove",
				Usage:  "Remove resources for a given EVM chain ID",
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
