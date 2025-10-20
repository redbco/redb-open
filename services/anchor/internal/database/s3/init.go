package s3

import "github.com/redbco/redb-open/pkg/anchor/adapter"

func init() {
	// Register S3 adapter with the global registry
	adapter.Register(NewAdapter())
}
