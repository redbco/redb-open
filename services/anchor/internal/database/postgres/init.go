package postgres

import (
	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

func init() {
	// Register PostgreSQL adapter with the global registry
	adapter.Register(NewAdapter())
}
