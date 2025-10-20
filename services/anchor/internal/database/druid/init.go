package druid

import "github.com/redbco/redb-open/pkg/anchor/adapter"

func init() {
	// Register Druid adapter with the global registry
	adapter.Register(NewAdapter())
}
