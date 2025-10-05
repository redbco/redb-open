package mongodb

import (
	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

func init() {
	// Register MongoDB adapter with the global registry
	adapter.Register(NewAdapter())
}
