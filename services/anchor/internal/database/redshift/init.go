package redshift

import "github.com/redbco/redb-open/pkg/anchor/adapter"

func init() {
	// Register Redshift adapter with the global registry
	adapter.Register(NewAdapter())
}
