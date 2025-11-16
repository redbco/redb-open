package mqtt

import (
	"github.com/redbco/redb-open/pkg/stream/adapter"
	"github.com/redbco/redb-open/pkg/streamcapabilities"
)

func init() {
	// Register both client and server adapters
	adapter.RegisterAdapter(streamcapabilities.MQTT, func() adapter.StreamAdapter {
		return NewClientAdapter()
	})

	adapter.RegisterAdapter(streamcapabilities.MQTTServer, func() adapter.StreamAdapter {
		return NewServerAdapter()
	})
}
