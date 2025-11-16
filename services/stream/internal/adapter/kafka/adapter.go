package kafka

import (
	"context"

	"github.com/redbco/redb-open/pkg/stream/adapter"
	"github.com/redbco/redb-open/pkg/streamcapabilities"
)

// Adapter implements the StreamAdapter interface for Kafka
type Adapter struct{}

func init() {
	// Register adapter - would be done via a registry in full implementation
}

func NewAdapter() *Adapter {
	return &Adapter{}
}

func (a *Adapter) Type() streamcapabilities.StreamPlatform {
	return streamcapabilities.Kafka
}

func (a *Adapter) Capabilities() streamcapabilities.Capability {
	cap, _ := streamcapabilities.Get(streamcapabilities.Kafka)
	return cap
}

func (a *Adapter) Connect(ctx context.Context, config adapter.ConnectionConfig) (adapter.Connection, error) {
	return &Connection{
		id:     config.ID,
		config: config,
	}, nil
}

type Connection struct {
	id     string
	config adapter.ConnectionConfig
}

func (c *Connection) ID() string {
	return c.id
}

func (c *Connection) Type() streamcapabilities.StreamPlatform {
	return streamcapabilities.Kafka
}

func (c *Connection) IsConnected() bool {
	return true
}

func (c *Connection) Ping(ctx context.Context) error {
	return nil
}

func (c *Connection) Close() error {
	return nil
}

func (c *Connection) ProducerOperations() adapter.ProducerOperator {
	return &Producer{}
}

func (c *Connection) ConsumerOperations() adapter.ConsumerOperator {
	return &Consumer{}
}

func (c *Connection) AdminOperations() adapter.AdminOperator {
	return &Admin{}
}

func (c *Connection) Raw() interface{} {
	return nil
}

func (c *Connection) Config() adapter.ConnectionConfig {
	return c.config
}

func (c *Connection) Adapter() adapter.StreamAdapter {
	return &Adapter{}
}
