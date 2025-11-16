package eventhubs

import (
	"github.com/redbco/redb-open/pkg/stream/adapter"
	"github.com/redbco/redb-open/pkg/streamcapabilities"
)

func init() {
	// Register Event Hubs adapter
	adapter.RegisterAdapter(streamcapabilities.EventHubs, func() adapter.StreamAdapter {
		return NewAdapter()
	})
}
