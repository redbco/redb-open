package minio

import "github.com/redbco/redb-open/pkg/anchor/adapter"

func init() {
	// Register MinIO adapter with the global registry
	adapter.Register(NewAdapter())
}
