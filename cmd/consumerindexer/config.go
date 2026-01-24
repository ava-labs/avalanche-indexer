package main

import (
	"strings"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/urfave/cli/v2"
)

// buildClickHouseConfig builds a ClickhouseConfig from CLI context flags
func buildClickHouseConfig(c *cli.Context) clickhouse.Config {
	// Handle hosts - StringSliceFlag returns []string, but we need to handle comma-separated values
	hosts := c.StringSlice("clickhouse-hosts")
	// If hosts is a single comma-separated string, split it
	if len(hosts) == 1 && strings.Contains(hosts[0], ",") {
		hosts = strings.Split(hosts[0], ",")
		for i, host := range hosts {
			hosts[i] = strings.TrimSpace(host)
		}
	}
	// Safely clamp and convert block buffer size to uint8 range
	return clickhouse.Config{
		Hosts:                hosts,
		Database:             c.String("clickhouse-database"),
		Username:             c.String("clickhouse-username"),
		Password:             c.String("clickhouse-password"),
		Debug:                c.Bool("clickhouse-debug"),
		InsecureSkipVerify:   c.Bool("clickhouse-insecure-skip-verify"),
		MaxExecutionTime:     c.Int("clickhouse-max-execution-time"),
		DialTimeout:          c.Int("clickhouse-dial-timeout"),
		MaxOpenConns:         c.Int("clickhouse-max-open-conns"),
		MaxIdleConns:         c.Int("clickhouse-max-idle-conns"),
		ConnMaxLifetime:      c.Int("clickhouse-conn-max-lifetime"),
		BlockBufferSize:      uint8(c.Int("clickhouse-block-buffer-size")),
		MaxBlockSize:         c.Int("clickhouse-max-block-size"),
		MaxCompressionBuffer: c.Int("clickhouse-max-compression-buffer"),
		ClientName:           c.String("clickhouse-client-name"),
		ClientVersion:        c.String("clickhouse-client-version"),
		UseHTTP:              c.Bool("clickhouse-use-http"),
	}
}
