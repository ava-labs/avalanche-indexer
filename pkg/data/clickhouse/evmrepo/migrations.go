package evmrepo

import (
	"embed"
)

//go:embed queries/migrations/block/*.sql
var blocksMigrationsFS embed.FS

//go:embed queries/migrations/transaction/*.sql
var transactionsMigrationsFS embed.FS

//go:embed queries/migrations/log/*.sql
var logsMigrationsFS embed.FS

//go:embed queries/migrations/internal_transaction/*.sql
var internalTransactionsMigrationsFS embed.FS
