# Stream Service Documentation

## Overview

The Stream service (`redb-stream`) provides connectivity to various data streaming platforms including Apache Kafka, AWS Kinesis, Google Cloud Pub/Sub, Azure Event Hubs, and others. The service automatically discovers and maintains schemas of streaming data, enabling seamless integration with reDB's mapping and transformation capabilities.

## Purpose

The Stream service acts as a bridge between real-time data streaming platforms and reDB's data management capabilities. It:

- Connects to multiple streaming platforms using platform-specific adapters
- Automatically discovers message schemas through sampling
- Tracks schema evolution over time
- Stores discovered schemas in the resource registry
- Supports both message consumption (reading) and production (writing)
- Enables CDC events from Anchor service to be published to streams

## Architecture

### Components

**Engine (`internal/engine`)**: Core service logic, lifecycle management, and integration with Supervisor
- `engine.go`: Main engine with initialization and lifecycle management
- `server.go`: gRPC server implementing StreamService protocol
- `service.go`: Service interface implementation for Supervisor integration

**Adapters (`internal/adapter`)**: Platform-specific implementations
- `kafka/`: Apache Kafka and Redpanda support
- `kinesis/`: AWS Kinesis support
- `pubsub/`: Google Cloud Pub/Sub support
- `eventhubs/`: Azure Event Hubs support

**Schema Discovery (`internal/schema`)**: Automatic schema detection
- `discoverer.go`: Analyzes message payloads and infers schemas
- `tracker.go`: Tracks schema changes and updates resource registry

**Configuration (`internal/config`)**: Database interaction and configuration management
- `repository.go`: CRUD operations on streams table
- `model.go`: Configuration data structures

**State (`internal/state`)**: Global state management for active connections

**Watchers (`internal/watcher`)**: Background monitoring
- `config_watcher.go`: Monitors for configuration changes in database
- `schema_watcher.go`: Periodically updates discovered schemas

### Integration Points

**Core Service**: Manages stream configurations via gRPC
- CRUD operations on stream connections
- Notifies Stream service of configuration changes

**UnifiedModel Service**: Schema registry updates
- Stores discovered schemas as resource containers (topics) and items (fields)

**Anchor Service**: CDC event publishing
- Publishes change data capture events to configured streams

**ClientAPI**: REST interface for stream management
- User-facing endpoints for connection management
- Topic and schema inspection

## Supported Platforms

| Platform | Producer | Consumer | Partitions | Consumer Groups | Schema Registry |
|----------|----------|----------|------------|-----------------|-----------------|
| Apache Kafka | ✓ | ✓ | ✓ | ✓ | ✓ |
| Redpanda | ✓ | ✓ | ✓ | ✓ | ✓ |
| AWS Kinesis | ✓ | ✓ | ✓ | ✗ | ✗ |
| GCP Pub/Sub | ✓ | ✓ | ✗ | ✗ | ✓ |
| Azure Event Hubs | ✓ | ✓ | ✓ | ✓ | ✓ |
| Apache Pulsar | ✓ | ✓ | ✓ | ✓ | ✓ |
| MQTT | ✓ | ✓ | ✗ | ✗ | ✗ |
| RabbitMQ | ✓ | ✓ | ✗ | ✗ | ✗ |

## Configuration

### Service Configuration

In `config.yaml`:

```yaml
services:
  stream:
    enabled: true
    required: true
    executable: ./redb-stream
    args:
      - --port=50061
      - --supervisor=localhost:50000
    dependencies:
      - mesh
      - anchor
    environment:
      SERVICE_NAME: stream
```

### Connection Configuration

Example Kafka connection via ClientAPI:

```bash
curl -X POST http://localhost:8080/default/api/v1/workspaces/my-workspace/streams/connect \
  -H "Content-Type: application/json" \
  -d '{
    "stream_name": "kafka-prod",
    "stream_description": "Production Kafka cluster",
    "stream_platform": "kafka",
    "connection_config": {
      "brokers": ["kafka1:9092", "kafka2:9092"],
      "sasl_mechanism": "SCRAM-SHA-512",
      "username": "redb",
      "password": "secret",
      "tls_enabled": true,
      "group_id": "redb-consumers"
    },
    "monitored_topics": ["orders", "payments", "inventory"],
    "node_id": 1001
  }'
```

## Schema Discovery

### How It Works

1. **Sampling**: The service samples messages from each monitored topic
2. **Inference**: JSON payloads are parsed and field types are inferred
3. **Tracking**: Field presence is tracked (always present = NOT NULL, sometimes = NULLABLE)
4. **Confidence**: Confidence scores increase with more samples
5. **Registry Update**: Schemas are stored as resource containers/items

### Schema Representation

Each topic becomes a **resource container** with URI format:
```
stream://{platform}/{connection_name}/{topic_name}
```

Each field in the message becomes a **resource item** with:
- Data type (string, number, boolean, object, array)
- Nullability (based on occurrence rate)
- Sample values
- Occurrence statistics

### Example

For a Kafka topic `orders` with messages like:
```json
{
  "order_id": "12345",
  "customer_id": "C-789",
  "amount": 99.99,
  "items": ["SKU-A", "SKU-B"]
}
```

The discovered schema becomes:
- **Container**: `stream://kafka/kafka-prod/orders`
- **Items**:
  - `order_id` (string, NOT NULL)
  - `customer_id` (string, NOT NULL)
  - `amount` (number, NOT NULL)
  - `items` (array, NOT NULL)

## gRPC API

### Service Interface

From `api/proto/stream/v1/stream.proto`:

```protobuf
service StreamService {
    rpc ConnectStream(ConnectStreamRequest) returns (ConnectStreamResponse);
    rpc DisconnectStream(DisconnectStreamRequest) returns (DisconnectStreamResponse);
    rpc GetStreamMetadata(GetStreamMetadataRequest) returns (GetStreamMetadataResponse);
    rpc ListTopics(ListTopicsRequest) returns (ListTopicsResponse);
    rpc GetTopicSchema(GetTopicSchemaRequest) returns (GetTopicSchemaResponse);
    rpc ProduceMessages(ProduceMessagesRequest) returns (ProduceMessagesResponse);
    rpc ConsumeMessages(ConsumeMessagesRequest) returns (stream ConsumeMessagesResponse);
}
```

## REST API

### Endpoints

All endpoints are workspace-scoped:

```
GET    /{tenant}/api/v1/workspaces/{workspace}/streams
POST   /{tenant}/api/v1/workspaces/{workspace}/streams/connect
GET    /{tenant}/api/v1/workspaces/{workspace}/streams/{stream_name}
PUT    /{tenant}/api/v1/workspaces/{workspace}/streams/{stream_name}
POST   /{tenant}/api/v1/workspaces/{workspace}/streams/{stream_name}/reconnect
POST   /{tenant}/api/v1/workspaces/{workspace}/streams/{stream_name}/disconnect
GET    /{tenant}/api/v1/workspaces/{workspace}/streams/{stream_name}/topics
GET    /{tenant}/api/v1/workspaces/{workspace}/streams/{stream_name}/topics/{topic}/schema
```

## Lifecycle

### Startup

1. Service starts and registers with Supervisor
2. Connects to internal database
3. Retrieves Node ID from localidentity table
4. Establishes gRPC connections to Core and UnifiedModel services
5. Loads existing stream configurations from database
6. Automatically reconnects to configured streams
7. Starts config and schema watchers

### Graceful Shutdown

1. Receives shutdown signal
2. Stops config and schema watchers
3. For each active connection:
   - Flushes pending messages
   - Commits consumer offsets
   - Closes producers and consumers
   - Updates database status to DISCONNECTED
4. Closes gRPC connections
5. Closes database connection
6. Exits

## Key Files

- `cmd/main.go`: Service entry point
- `internal/engine/engine.go`: Core engine (378 lines)
- `internal/engine/server.go`: gRPC implementation
- `internal/schema/discoverer.go`: Schema discovery logic
- `internal/adapter/kafka/adapter.go`: Kafka adapter
- `pkg/stream/adapter/interface.go`: Adapter interface definition
- `pkg/streamcapabilities/capabilities.go`: Platform capabilities registry

## Dependencies

### Go Modules
- `github.com/segmentio/kafka-go` - Kafka client
- `github.com/aws/aws-sdk-go-v2/service/kinesis` - AWS Kinesis
- `cloud.google.com/go/pubsub` - GCP Pub/Sub
- `github.com/Azure/azure-event-hubs-go` - Azure Event Hubs

## Monitoring

Service health can be checked via:
- Supervisor health check interface
- Number of active connections
- Schema discovery statistics
- Message consumption/production rates

## Troubleshooting

### Connection Issues

Check logs for:
- Authentication failures (SASL, TLS)
- Network connectivity to brokers
- Configuration errors

### Schema Discovery

- Ensure topics have recent messages
- Check that messages are valid JSON
- Verify monitored_topics configuration
- Review confidence scores (low = needs more samples)

## Future Enhancements

- Support for Avro and Protobuf message formats
- Exactly-once processing semantics
- Dead letter queue handling
- Message filtering and routing
- Integration with schema registries (Confluent, AWS Glue)

