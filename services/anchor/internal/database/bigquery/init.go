package bigquery

import "github.com/redbco/redb-open/pkg/anchor/adapter"

func init() {
	// Register BigQuery adapter with the global registry
	adapter.Register(NewAdapter())
}
