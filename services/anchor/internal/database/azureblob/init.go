package azureblob

import "github.com/redbco/redb-open/pkg/anchor/adapter"

func init() {
	// Register Azure Blob adapter with the global registry
	adapter.Register(NewAdapter())
}
