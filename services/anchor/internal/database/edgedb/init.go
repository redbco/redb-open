package edgedb

import "github.com/redbco/redb-open/pkg/anchor/adapter"

func init() {
	adapter.Register(NewAdapter())
}
