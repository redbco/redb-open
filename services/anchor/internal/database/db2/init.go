//go:build enterprise
// +build enterprise

package db2

import (
	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

func init() {
	adapter.Register(NewAdapter())
}
