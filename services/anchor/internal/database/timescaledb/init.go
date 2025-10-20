package timescaledb

import (
	_ "github.com/lib/pq" // PostgreSQL driver for TimescaleDB
	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

func init() {
	// Register TimescaleDB adapter with the global registry
	adapter.Register(NewAdapter())
}
