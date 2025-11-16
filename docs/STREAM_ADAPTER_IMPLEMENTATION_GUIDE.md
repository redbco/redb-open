# Stream Adapter Implementation Guide

## Overview

This guide explains how to implement a new streaming platform adapter for the reDB Stream service. Adapters allow the service to connect to different streaming platforms using a unified interface.

## Adapter Interface

All adapters must implement the `StreamAdapter` interface defined in `pkg/stream/adapter/interface.go`:

```go
type StreamAdapter interface {
    Type() streamcapabilities.StreamPlatform
    Capabilities() streamcapabilities.Capability
    Connect(ctx context.Context, config ConnectionConfig) (Connection, error)
}
```

The returned `Connection` must implement:

```go
type Connection interface {
    ID() string
    Type() streamcapabilities.StreamPlatform
    IsConnected() bool
    Ping(ctx context.Context) error
    Close() error
    ProducerOperations() ProducerOperator
    ConsumerOperations() ConsumerOperator
    AdminOperations() AdminOperator
    Raw() interface{}
    Config() ConnectionConfig
    Adapter() StreamAdapter
}
```

## Step-by-Step Implementation

### 1. Define Platform Capabilities

Add your platform to `pkg/streamcapabilities/capabilities.go`:

```go
const (
    // ... existing platforms
    MyPlatform StreamPlatform = "myplatform"
)

var All = map[StreamPlatform]Capability{
    // ... existing platforms
    MyPlatform: {
        Name:                  "My Streaming Platform",
        ID:                    MyPlatform,
        SupportsProducer:      true,
        SupportsConsumer:      true,
        SupportsServerMode:    false,
        SupportsPartitions:    true,
        SupportsConsumerGroups: true,
        SupportsSASL:          true,
        SupportsTLS:           true,
        DefaultPort:           9999,
        DefaultSSLPort:        9998,
        SchemaRegistrySupport: false,
        ConnectionStringTemplate: "myplatform://{{host}}/{{topic}}",
        SupportsTransactions:  false,
        SupportsOrdering:      true,
        SupportsWildcards:     false,
    },
}
```

### 2. Create Adapter Package

Create directory: `services/stream/internal/adapter/myplatform/`

#### adapter.go

```go
package myplatform

import (
    "context"
    "fmt"

    "github.com/redbco/redb-open/pkg/stream/adapter"
    "github.com/redbco/redb-open/pkg/streamcapabilities"
)

type Adapter struct{}

func NewAdapter() *Adapter {
    return &Adapter{}
}

func (a *Adapter) Type() streamcapabilities.StreamPlatform {
    return streamcapabilities.MyPlatform
}

func (a *Adapter) Capabilities() streamcapabilities.Capability {
    cap, _ := streamcapabilities.Get(streamcapabilities.MyPlatform)
    return cap
}

func (a *Adapter) Connect(ctx context.Context, config adapter.ConnectionConfig) (adapter.Connection, error) {
    if err := config.Validate(); err != nil {
        return nil, fmt.Errorf("invalid config: %w", err)
    }

    // Create platform-specific client
    client, err := createClient(config)
    if err != nil {
        return nil, fmt.Errorf("failed to create client: %w", err)
    }

    conn := &Connection{
        id:     config.ID,
        config: config,
        client: client,
    }

    // Test connection
    if err := conn.Ping(ctx); err != nil {
        return nil, fmt.Errorf("connection test failed: %w", err)
    }

    return conn, nil
}

type Connection struct {
    id     string
    config adapter.ConnectionConfig
    client interface{} // Your platform's client type
}

func (c *Connection) ID() string {
    return c.id
}

func (c *Connection) Type() streamcapabilities.StreamPlatform {
    return streamcapabilities.MyPlatform
}

func (c *Connection) IsConnected() bool {
    // Implement connection check
    return true
}

func (c *Connection) Ping(ctx context.Context) error {
    // Implement health check
    return nil
}

func (c *Connection) Close() error {
    // Clean up resources
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
```

#### consumer.go

```go
package myplatform

import (
    "context"
    
    "github.com/redbco/redb-open/pkg/stream/adapter"
)

type Consumer struct {
    conn *Connection
}

func (c *Consumer) Subscribe(ctx context.Context, topics []string, groupID string) error {
    // Implement subscription
    return nil
}

func (c *Consumer) Consume(ctx context.Context, handler adapter.MessageHandler) error {
    // Implement message consumption loop
    // Call handler for each message
    return nil
}

func (c *Consumer) Commit(ctx context.Context) error {
    // Commit offsets/acknowledgments
    return nil
}

func (c *Consumer) Seek(ctx context.Context, topic string, partition int32, offset int64) error {
    // Seek to specific offset
    return nil
}

func (c *Consumer) Close() error {
    // Clean up consumer resources
    return nil
}
```

#### producer.go

```go
package myplatform

import (
    "context"
    
    "github.com/redbco/redb-open/pkg/stream/adapter"
)

type Producer struct {
    conn *Connection
}

func (p *Producer) Produce(ctx context.Context, topic string, messages []adapter.Message) error {
    // Implement synchronous message production
    for _, msg := range messages {
        // Send message to platform
    }
    return nil
}

func (p *Producer) ProduceAsync(ctx context.Context, topic string, messages []adapter.Message, callback func(error)) error {
    // Implement asynchronous production
    go func() {
        err := p.Produce(ctx, topic, messages)
        if callback != nil {
            callback(err)
        }
    }()
    return nil
}

func (p *Producer) Flush(ctx context.Context) error {
    // Flush pending messages
    return nil
}

func (p *Producer) Close() error {
    // Clean up producer resources
    return nil
}
```

#### admin.go

```go
package myplatform

import (
    "context"
    
    "github.com/redbco/redb-open/pkg/stream/adapter"
)

type Admin struct {
    conn *Connection
}

func (a *Admin) ListTopics(ctx context.Context) ([]adapter.TopicInfo, error) {
    // List all topics
    return []adapter.TopicInfo{}, nil
}

func (a *Admin) CreateTopic(ctx context.Context, name string, config adapter.TopicConfig) error {
    // Create new topic
    return nil
}

func (a *Admin) DeleteTopic(ctx context.Context, name string) error {
    // Delete topic
    return nil
}

func (a *Admin) GetTopicMetadata(ctx context.Context, topic string) (adapter.TopicMetadata, error) {
    // Get topic metadata
    return adapter.TopicMetadata{}, nil
}

func (a *Admin) GetTopicConfig(ctx context.Context, topic string) (adapter.TopicConfig, error) {
    // Get topic configuration
    return adapter.TopicConfig{}, nil
}
```

### 3. Register Adapter

Add to `services/stream/cmd/imports.go`:

```go
import (
    // ... existing imports
    _ "github.com/redbco/redb-open/services/stream/internal/adapter/myplatform"
)
```

### 4. Add Dependencies

Update `services/stream/go.mod`:

```go
require (
    // ... existing dependencies
    github.com/my-platform/client-sdk v1.0.0
)
```

## Configuration Mapping

Map `ConnectionConfig` fields to platform-specific settings:

```go
func configToClientConfig(config adapter.ConnectionConfig) ClientConfig {
    return ClientConfig{
        Brokers:  config.Brokers,
        Username: config.Username,
        Password: config.Password,
        TLS: TLSConfig{
            Enabled:    config.TLSEnabled,
            CertFile:   config.CertFile,
            KeyFile:    config.KeyFile,
            CAFile:     config.CAFile,
            SkipVerify: config.TLSSkipVerify,
        },
        // Platform-specific options from Configuration map
        Option1: config.Configuration["option1"],
        Option2: config.Configuration["option2"],
    }
}
```

## Testing Checklist

- [ ] Connection establishment and authentication
- [ ] Message production (single and batch)
- [ ] Message consumption with offset management
- [ ] Consumer group coordination (if supported)
- [ ] Topic creation and deletion
- [ ] Topic metadata retrieval
- [ ] TLS/SSL connectivity
- [ ] SASL authentication
- [ ] Error handling and retry logic
- [ ] Graceful connection closure
- [ ] Resource cleanup
- [ ] Concurrent operations safety

## Example: Adding RabbitMQ Adapter

```go
// pkg/streamcapabilities/capabilities.go
RabbitMQ StreamPlatform = "rabbitmq"

// services/stream/internal/adapter/rabbitmq/adapter.go
import (
    amqp "github.com/rabbitmq/amqp091-go"
)

type Adapter struct{}

func (a *Adapter) Connect(ctx context.Context, config adapter.ConnectionConfig) (adapter.Connection, error) {
    url := fmt.Sprintf("amqp://%s:%s@%s/%s",
        config.Username,
        config.Password,
        config.Endpoint,
        config.Configuration["vhost"],
    )
    
    conn, err := amqp.Dial(url)
    if err != nil {
        return nil, err
    }
    
    return &Connection{
        id:     config.ID,
        config: config,
        conn:   conn,
    }, nil
}
```

## Best Practices

1. **Error Handling**: Wrap platform errors with context
2. **Retries**: Implement exponential backoff for transient failures
3. **Logging**: Use structured logging for debugging
4. **Metrics**: Track connection health and throughput
5. **Configuration Validation**: Validate early in Connect()
6. **Resource Management**: Always clean up in Close()
7. **Concurrency**: Make operations thread-safe
8. **Documentation**: Document platform-specific quirks

## Common Patterns

### Connection Pooling

```go
type ConnectionPool struct {
    sync.Mutex
    connections []*PlatformConnection
    config      adapter.ConnectionConfig
}

func (p *ConnectionPool) Get() (*PlatformConnection, error) {
    p.Lock()
    defer p.Unlock()
    
    if len(p.connections) > 0 {
        conn := p.connections[0]
        p.connections = p.connections[1:]
        return conn, nil
    }
    
    return createNewConnection(p.config)
}
```

### Message Batching

```go
type BatchProducer struct {
    conn      *Connection
    batchSize int
    buffer    []adapter.Message
    mu        sync.Mutex
}

func (b *BatchProducer) Add(msg adapter.Message) error {
    b.mu.Lock()
    defer b.mu.Unlock()
    
    b.buffer = append(b.buffer, msg)
    if len(b.buffer) >= b.batchSize {
        return b.flush()
    }
    return nil
}
```

### Offset Management

```go
type OffsetManager struct {
    offsets map[string]map[int32]int64  // topic -> partition -> offset
    mu      sync.RWMutex
}

func (o *OffsetManager) Commit(topic string, partition int32, offset int64) {
    o.mu.Lock()
    defer o.mu.Unlock()
    
    if _, ok := o.offsets[topic]; !ok {
        o.offsets[topic] = make(map[int32]int64)
    }
    o.offsets[topic][partition] = offset
}
```

## Troubleshooting

### Authentication Issues
- Verify credentials are correct
- Check SASL mechanism compatibility
- Ensure TLS certificates are valid

### Connection Timeouts
- Increase timeout values in ConnectionConfig
- Check network connectivity
- Verify firewall rules

### Message Loss
- Enable acknowledgments
- Use synchronous production for critical messages
- Implement dead letter queue handling

## Support

For questions or issues:
- Review existing adapters in `services/stream/internal/adapter/`
- Check platform SDK documentation
- Test with platform's CLI tools first

