//go:build enterprise
// +build enterprise

package oracle

import (
	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

func init() {
	adapter.Register(NewAdapter())
}
