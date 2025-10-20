package synapse

import "github.com/redbco/redb-open/pkg/anchor/adapter"

func init() {
	// Register Synapse adapter with the global registry
	adapter.Register(NewAdapter())
}
