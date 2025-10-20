package opensearch

import "github.com/redbco/redb-open/pkg/anchor/adapter"

func init() {
	// Register OpenSearch adapter with the global registry
	adapter.Register(NewAdapter())
}
