package checkpoint

import (
	"embed"
)

//go:embed queries/migrations/*.sql
var migrationsFS embed.FS
