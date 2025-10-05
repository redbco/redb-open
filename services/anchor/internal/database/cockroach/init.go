package cockroach

import "github.com/redbco/redb-open/pkg/anchor/adapter"

func init() {
	// Register the CockroachDB adapter with the global registry
	adapter.Register(NewAdapter())
}
