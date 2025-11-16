package kinesis

import (
	"github.com/redbco/redb-open/pkg/stream/adapter"
	"github.com/redbco/redb-open/pkg/streamcapabilities"
)

func init() {
	// Register Kinesis adapter
	adapter.RegisterAdapter(streamcapabilities.Kinesis, func() adapter.StreamAdapter {
		return NewAdapter()
	})
}
