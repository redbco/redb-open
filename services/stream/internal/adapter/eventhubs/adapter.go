package eventhubs

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/redbco/redb-open/pkg/stream/adapter"
	"github.com/redbco/redb-open/pkg/streamcapabilities"
)

type Adapter struct{}

func NewAdapter() *Adapter {
	return &Adapter{}
}

func (a *Adapter) Type() streamcapabilities.StreamPlatform {
	return streamcapabilities.EventHubs
}

func (a *Adapter) Capabilities() streamcapabilities.Capability {
	cap, _ := streamcapabilities.Get(streamcapabilities.EventHubs)
	return cap
}

func (a *Adapter) Connect(ctx context.Context, cfg adapter.ConnectionConfig) (adapter.Connection, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Get connection string or create from components
	connStr := cfg.Configuration["connection_string"]
	if connStr == "" {
		namespace := cfg.Configuration["namespace"]
		eventHub := cfg.Configuration["event_hub"]
		sasKeyName := cfg.Configuration["shared_access_key_name"]
		sasKey := cfg.Configuration["shared_access_key"]

		if namespace == "" || eventHub == "" {
			return nil, fmt.Errorf("namespace and event_hub are required")
		}

		if sasKeyName != "" && sasKey != "" {
			connStr = fmt.Sprintf("Endpoint=sb://%s.servicebus.windows.net/;SharedAccessKeyName=%s;SharedAccessKey=%s;EntityPath=%s",
				namespace, sasKeyName, sasKey, eventHub)
		} else {
			return nil, fmt.Errorf("connection_string or (shared_access_key_name + shared_access_key) required")
		}
	}

	conn := &Connection{
		id:               cfg.ID,
		config:           cfg,
		connectionString: connStr,
	}

	// Test connection by creating a producer client
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("connection test failed: %w", err)
	}

	return conn, nil
}

type Connection struct {
	id               string
	config           adapter.ConnectionConfig
	connectionString string
	producerClient   *azeventhubs.ProducerClient
	consumerClient   *azeventhubs.ConsumerClient
}

func (c *Connection) ID() string {
	return c.id
}

func (c *Connection) Type() streamcapabilities.StreamPlatform {
	return streamcapabilities.EventHubs
}

func (c *Connection) IsConnected() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.Ping(ctx) == nil
}

func (c *Connection) Ping(ctx context.Context) error {
	// Try to create a producer client to test connection
	client, err := azeventhubs.NewProducerClientFromConnectionString(c.connectionString, "", nil)
	if err != nil {
		return err
	}
	defer client.Close(ctx)

	// Get event hub properties as a health check
	_, err = client.GetEventHubProperties(ctx, nil)
	return err
}

func (c *Connection) Close() error {
	var errs []error

	if c.producerClient != nil {
		if err := c.producerClient.Close(context.Background()); err != nil {
			errs = append(errs, err)
		}
	}

	if c.consumerClient != nil {
		if err := c.consumerClient.Close(context.Background()); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing connection: %v", errs)
	}

	return nil
}

func (c *Connection) ProducerOperations() adapter.ProducerOperator {
	return &Producer{conn: c}
}

func (c *Connection) ConsumerOperations() adapter.ConsumerOperator {
	return &Consumer{conn: c}
}

func (c *Connection) AdminOperations() adapter.AdminOperator {
	return &Admin{conn: c}
}

func (c *Connection) Raw() interface{} {
	return map[string]interface{}{
		"producer": c.producerClient,
		"consumer": c.consumerClient,
	}
}

func (c *Connection) Config() adapter.ConnectionConfig {
	return c.config
}

func (c *Connection) Adapter() adapter.StreamAdapter {
	return &Adapter{}
}

func (c *Connection) getOrCreateProducer(ctx context.Context) (*azeventhubs.ProducerClient, error) {
	if c.producerClient != nil {
		return c.producerClient, nil
	}

	client, err := azeventhubs.NewProducerClientFromConnectionString(c.connectionString, "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer client: %w", err)
	}

	c.producerClient = client
	return client, nil
}

func (c *Connection) getOrCreateConsumer(ctx context.Context, consumerGroup string) (*azeventhubs.ConsumerClient, error) {
	if c.consumerClient != nil {
		return c.consumerClient, nil
	}

	if consumerGroup == "" {
		consumerGroup = "$Default"
	}

	client, err := azeventhubs.NewConsumerClientFromConnectionString(c.connectionString, "", consumerGroup, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer client: %w", err)
	}

	c.consumerClient = client
	return client, nil
}
