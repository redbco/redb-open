package gcs

import "github.com/redbco/redb-open/pkg/anchor/adapter"

func init() {
	// Register GCS adapter with the global registry
	adapter.Register(NewAdapter())
}
