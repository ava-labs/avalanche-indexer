package checkpoint

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

//go:embed queries/migrations/*.sql
var migrationsFS embed.FS

// RunMigrations reads all .sql files from the migrations directory, sorts
// them lexically, and executes each one against the ClickHouse connection.
//
// Each SQL file must be a single DDL statement with exactly three %s placeholders
// for (database, tableName, cluster). Migrations must be idempotent (use
// ADD COLUMN IF NOT EXISTS, etc.) since they run on every startup.
//
// To add a new migration, drop a numbered .sql file into the
// queries/migrations/ directory. No Go code changes required.
//
//	Example: queries/migrations/001_add_mode_local.sql
//	         queries/migrations/001_add_mode.sql
func RunMigrations(
	ctx context.Context,
	conn driver.Conn,
	database string,
	tableName string,
	cluster string,
) error {
	const migrationsDir = "queries/migrations"
	entries, err := migrationsFS.ReadDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("failed to read migrations directory %s: %w", migrationsDir, err)
	}

	slices.SortFunc(entries, func(a, b fs.DirEntry) int {
		return cmp.Compare(a.Name(), b.Name())
	})

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		content, err := migrationsFS.ReadFile(path.Join(migrationsDir, entry.Name()))
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
