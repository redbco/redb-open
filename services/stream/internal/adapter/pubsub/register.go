package pubsub

import (
	"github.com/redbco/redb-open/pkg/stream/adapter"
	"github.com/redbco/redb-open/pkg/streamcapabilities"
)

func init() {
	// Register Pub/Sub adapter
	adapter.RegisterAdapter(streamcapabilities.PubSub, func() adapter.StreamAdapter {
		return NewAdapter()
	})
}
