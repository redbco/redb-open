package apachepinot

import "github.com/redbco/redb-open/pkg/anchor/adapter"

func init() {
	// Register Pinot adapter with the global registry
	adapter.Register(NewAdapter())
}
