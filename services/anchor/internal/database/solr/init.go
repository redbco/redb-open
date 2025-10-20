package solr

import "github.com/redbco/redb-open/pkg/anchor/adapter"

func init() {
	// Register Solr adapter with the global registry
	adapter.Register(NewAdapter())
}
