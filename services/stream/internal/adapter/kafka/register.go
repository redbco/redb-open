package kafka

import (
	"github.com/redbco/redb-open/pkg/stream/adapter"
	"github.com/redbco/redb-open/pkg/streamcapabilities"
)

func init() {
	// Register Kafka adapter
	adapter.RegisterAdapter(streamcapabilities.Kafka, func() adapter.StreamAdapter {
		return NewAdapter()
	})
}
