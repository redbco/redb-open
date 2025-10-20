package tidb

import "github.com/redbco/redb-open/pkg/anchor/adapter"

func init() {
	// Register TiDB adapter with the global registry
	adapter.Register(NewAdapter())
}
