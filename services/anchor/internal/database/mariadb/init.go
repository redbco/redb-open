package mariadb

import "github.com/redbco/redb-open/pkg/anchor/adapter"

func init() {
	// Register the MariaDB adapter with the global registry
	adapter.Register(NewAdapter())
}
