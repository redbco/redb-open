package pubsub

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/redbco/redb-open/pkg/stream/adapter"
	"github.com/redbco/redb-open/pkg/streamcapabilities"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type Adapter struct{}

func NewAdapter() *Adapter {
	return &Adapter{}
}

func (a *Adapter) Type() streamcapabilities.StreamPlatform {
	return streamcapabilities.PubSub
}

func (a *Adapter) Capabilities() streamcapabilities.Capability {
	cap, _ := streamcapabilities.Get(streamcapabilities.PubSub)
	return cap
}

func (a *Adapter) Connect(ctx context.Context, cfg adapter.ConnectionConfig) (adapter.Connection, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	projectID := cfg.Configuration["project_id"]
	if projectID == "" {
		return nil, fmt.Errorf("project_id is required")
	}

	var opts []option.ClientOption

	// Add credentials if provided
	if cfg.Configuration["credentials_json"] != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(cfg.Configuration["credentials_json"])))
	} else if cfg.Configuration["credentials_file"] != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.Configuration["credentials_file"]))
	}

	// Create Pub/Sub client
	client, err := pubsub.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create pub/sub client: %w", err)
	}

	conn := &Connection{
		id:        cfg.ID,
		config:    cfg,
		client:    client,
		projectID: projectID,
	}

	// Test connection by listing topics (limit to 1)
	if err := conn.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("connection test failed: %w", err)
	}

	return conn, nil
}

type Connection struct {
	id        string
	config    adapter.ConnectionConfig
	client    *pubsub.Client
	projectID string
}

func (c *Connection) ID() string {
	return c.id
}

func (c *Connection) Type() streamcapabilities.StreamPlatform {
	return streamcapabilities.PubSub
}

func (c *Connection) IsConnected() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.Ping(ctx) == nil
}

func (c *Connection) Ping(ctx context.Context) error {
	// Try to list topics as a health check
	it := c.client.Topics(ctx)
	_, err := it.Next()
	if err != nil && err != iterator.Done {
		return err
	}
	return nil
}

func (c *Connection) Close() error {
	return c.client.Close()
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
	return c.client
}

func (c *Connection) Config() adapter.ConnectionConfig {
	return c.config
}

func (c *Connection) Adapter() adapter.StreamAdapter {
	return &Adapter{}
}
