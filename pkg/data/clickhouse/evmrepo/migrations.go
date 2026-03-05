package evmrepo

import (
	"cmp"
	"context"
	"embed"
	"fmt"
	"io/fs"
	"path"
	"slices"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

//go:embed queries/migrations/block/*.sql
var blocksMigrationsFS embed.FS

//go:embed queries/migrations/transaction/*.sql
var transactionsMigrationsFS embed.FS

//go:embed queries/migrations/log/*.sql
var logsMigrationsFS embed.FS

//go:embed queries/migrations/internal_transaction/*.sql
var internalTransactionsMigrationsFS embed.FS

// RunMigrations reads all .sql files from the given embed.FS directory, sorts
// them lexically, and executes each one against the ClickHouse connection.
//
// Each SQL file must be a single DDL statement with exactly three %s placeholders
// for (database, tableName, cluster). Migrations must be idempotent (use
// ADD COLUMN IF NOT EXISTS, etc.) since they run on every startup.
//
// To add a new migration, drop a numbered .sql file into the appropriate
// queries/migrations/<table>/ directory. No Go code changes required.
//
//	Example: queries/migrations/block/002_add_foo_local.sql
//	         queries/migrations/block/002_add_foo_distributed.sql
func RunMigrations(
	ctx context.Context,
	conn driver.Conn,
	migrationsFS embed.FS,
	dir string,
	database string,
	tableName string,
	cluster string,
) error {
	entries, err := migrationsFS.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read migrations directory %s: %w", dir, err)
	}

	slices.SortFunc(entries, func(a, b fs.DirEntry) int {
		return cmp.Compare(a.Name(), b.Name())
	})

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		content, err := migrationsFS.ReadFile(path.Join(dir, entry.Name()))
		if err != nil {
			return fmt.Errorf("failed to read migration %s: %w", entry.Name(), err)
		}

		query := fmt.Sprintf(string(content), database, tableName, cluster)
		if err := conn.Exec(ctx, query); err != nil {
			return fmt.Errorf("migration %s failed: %w", entry.Name(), err)
		}
	}

	return nil
}
