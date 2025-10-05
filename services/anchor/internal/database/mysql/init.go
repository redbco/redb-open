package mysql

import (
	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

func init() {
	// Register MySQL adapter with the global registry
	adapter.Register(NewAdapter())
}
