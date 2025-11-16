package kinesis

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/redbco/redb-open/pkg/stream/adapter"
	"github.com/redbco/redb-open/pkg/streamcapabilities"
)

type Adapter struct{}

func NewAdapter() *Adapter {
	return &Adapter{}
}

func (a *Adapter) Type() streamcapabilities.StreamPlatform {
	return streamcapabilities.Kinesis
}

func (a *Adapter) Capabilities() streamcapabilities.Capability {
	cap, _ := streamcapabilities.Get(streamcapabilities.Kinesis)
	return cap
}

func (a *Adapter) Connect(ctx context.Context, cfg adapter.ConnectionConfig) (adapter.Connection, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Build AWS config
	var opts []func(*config.LoadOptions) error

	// Set region
	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	}

	// Set credentials if provided
	if cfg.Configuration["access_key_id"] != "" && cfg.Configuration["secret_access_key"] != "" {
		creds := credentials.NewStaticCredentialsProvider(
			cfg.Configuration["access_key_id"],
			cfg.Configuration["secret_access_key"],
			cfg.Configuration["session_token"], // can be empty
		)
		opts = append(opts, config.WithCredentialsProvider(creds))
	}

	// Load AWS config
	awsConfig, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create Kinesis client
	client := kinesis.NewFromConfig(awsConfig)

	conn := &Connection{
		id:     cfg.ID,
		config: cfg,
		client: client,
	}

	// Test connection by listing streams
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("connection test failed: %w", err)
	}

	return conn, nil
}

type Connection struct {
	id     string
	config adapter.ConnectionConfig
	client *kinesis.Client
}

func (c *Connection) ID() string {
	return c.id
}

func (c *Connection) Type() streamcapabilities.StreamPlatform {
	return streamcapabilities.Kinesis
}

func (c *Connection) IsConnected() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.Ping(ctx) == nil
}

func (c *Connection) Ping(ctx context.Context) error {
	// Try to list streams as a health check
	_, err := c.client.ListStreams(ctx, &kinesis.ListStreamsInput{
		Limit: aws.Int32(1),
	})
	return err
}

func (c *Connection) Close() error {
	// AWS SDK v2 clients don't need explicit closing
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
	return c.client
}

func (c *Connection) Config() adapter.ConnectionConfig {
	return c.config
}

func (c *Connection) Adapter() adapter.StreamAdapter {
	return &Adapter{}
}
