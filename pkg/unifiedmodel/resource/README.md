# Resource Addressing Package

The `resource` package provides a comprehensive URI-based addressing system for precisely identifying data storage locations across multiple data sources including databases, data streams, webhooks, and MCP resources.

## Overview

This package enables:
- **Precise Location Addressing**: Point to exact data locations (columns, fields, properties, nested structures)
- **Metadata vs. Data Distinction**: Clearly differentiate between data values and structural metadata
- **Multi-Protocol Support**: Work with databases, streams (Kafka, MQTT, etc.), webhooks, and MCP
- **Nested Structure Navigation**: Navigate complex types (JSON, Avro, Protobuf, embedded documents)
- **Type Safety**: Validate addresses and check compatibility before creating mappings
- **Backward Compatibility**: Support legacy `db://` format

## Core Types

### ResourceAddress

The main type representing a complete resource address:

```go
type ResourceAddress struct {
    Protocol     ResourceProtocol  // redb, stream, webhook, mcp
    Scope        ResourceScope     // data, metadata, schema
    DatabaseID   string            // For redb://
    ConnectionID string            // For stream://, webhook://
    ServerID     string            // For mcp://
    ObjectType   ObjectType        // table, topic, endpoint, etc.
    ObjectName   string
    PathSegments []PathSegment
    Selector     *Selector         // Optional JSONPath, XPath, etc.
}
```

### Protocols

- `ProtocolDatabase` (`redb://`) - Database resources via UnifiedModel
- `ProtocolStream` (`stream://`) - Data streams (Kafka, MQTT, Kinesis, etc.)
- `ProtocolWebhook` (`webhook://`) - HTTP webhook endpoints
- `ProtocolMCP` (`mcp://`) - Model Context Protocol resources/tools

### Scopes

- `ScopeData` - Actual data values (column values, message payloads)
- `ScopeMetadata` - Structural properties (names, types, constraints)
- `ScopeSchema` - Schema definitions

## Usage

### Parsing URIs

```go
import "github.com/redbco/redb-open/pkg/unifiedmodel/resource"

// Parse a database column URI
uri := "redb://data/database/db_123/table/users/column/email"
addr, err := resource.ParseResourceURI(uri)
if err != nil {
    log.Fatal(err)
}

// Parse a Kafka topic URI
streamURI := "stream://kafka/conn_prod/topic/events/schema/avro/field/user_id"
streamAddr, err := resource.ParseResourceURI(streamURI)
if err != nil {
    log.Fatal(err)
}

// Parse a webhook URI
webhookURI := "webhook://endpoint_123/request/body/field/email"
webhookAddr, err := resource.ParseResourceURI(webhookURI)
```

### Building URIs

```go
// Create a database address
addr := resource.NewDatabaseAddress(
    resource.ScopeData,
    "db_postgres_123",
    resource.ObjectTypeTable,
    "users",
).AddPathSegment(resource.SegmentTypeColumn, "email")

// Build URI string
uri, err := resource.BuildResourceURI(addr)
// Result: redb://data/database/db_postgres_123/table/users/column/email

// Create a stream address
streamAddr := resource.NewStreamAddress(
    resource.StreamKafka,
    "conn_kafka_prod",
    resource.ObjectTypeTopic,
    "user-events",
).AddPathSegment(resource.SegmentType("schema"), "avro").
  AddPathSegment(resource.SegmentTypeField, "user_id")

streamURI, _ := resource.BuildResourceURI(streamAddr)
// Result: stream://kafka/conn_kafka_prod/topic/user-events/schema/avro/field/user_id
```

### Validation

```go
// Validate an address
addr, _ := resource.ParseResourceURI("redb://data/database/db_123/table/users/column/email")
if err := resource.ValidateAddress(addr); err != nil {
    log.Printf("Invalid address: %v", err)
}

// Or use MustValidateAddress (panics on error)
resource.MustValidateAddress(addr)
```

### Compatibility Checking

```go
source, _ := resource.ParseResourceURI("stream://kafka/conn_prod/topic/events/field/user_id")
target, _ := resource.ParseResourceURI("redb://data/database/db_warehouse/table/events/column/user_id")

// Check if addresses are compatible for mapping
report, err := resource.CheckCompatibility(source, target)
if err != nil {
    log.Fatal(err)
}

if !report.Compatible {
    log.Printf("Incompatible: %s", report.Reason)
} else {
    // Check warnings
    for _, warning := range report.Warnings {
        log.Printf("Warning: %s", warning)
    }
    
    // Check suggested transformations
    for _, transform := range report.SuggestedTransformations {
        log.Printf("Suggested: %s", transform)
    }
}
```

### Working with Selectors

```go
// Add a JSONPath selector for nested JSON navigation
addr := resource.NewDatabaseAddress(
    resource.ScopeData,
    "db_123",
    resource.ObjectTypeTable,
    "users",
).AddPathSegment(resource.SegmentTypeColumn, "profile").
  WithJSONPathSelector("$.address.city")

// Build URI with selector
uri, _ := resource.BuildResourceURI(addr)
// Result: redb://data/database/db_123/table/users/column/profile#$.address.city
```

### Helper Methods

```go
addr, _ := resource.ParseResourceURI(uri)

// Check protocol type
if addr.IsDatabase() {
    // Handle database resource
}
if addr.IsStream() {
    // Handle streaming resource
}

// Check scope
if addr.IsDataScope() {
    // Accessing data values
}
if addr.IsMetadataScope() {
    // Accessing metadata
}

// Navigate path segments
depth := addr.PathDepth()
lastSegment := addr.LastPathSegment()
```

## URI Format Examples

### Database Resources

```
# Table column
redb://data/database/{id}/table/users/column/email

# Collection field
redb://data/database/{id}/collection/orders/field/customer_id

# Nested JSON
redb://data/database/{id}/table/users/column/profile#$.address.city

# Metadata
redb://metadata/database/{id}/table/users/columns/names
```

### Stream Resources

```
# Kafka topic with Avro schema
stream://kafka/{conn-id}/topic/events/schema/avro/field/user_id

# MQTT topic
stream://mqtt/{conn-id}/topic/sensors/temperature/field/value

# Kinesis stream
stream://kinesis/{conn-id}/stream/logs/field/message

# With partition
stream://kafka/{conn-id}/topic/orders/partition/0/field/order_id
```

### Webhook Resources

```
# Request body field
webhook://{endpoint-id}/request/body/field/email

# Request header
webhook://{endpoint-id}/request/header/Authorization

# Response field
webhook://{endpoint-id}/response/body/field/status
```

### MCP Resources

```
# MCP resource field
mcp://{server-id}/resource/users/field/email

# MCP tool parameter
mcp://{server-id}/tool/search/parameter/query

# MCP tool response
mcp://{server-id}/tool/get_data/response/field/results
```

## Stream Providers

Supported streaming platforms:
- `StreamKafka` - Apache Kafka
- `StreamMQTT` - MQTT
- `StreamKinesis` - AWS Kinesis
- `StreamRabbitMQ` - RabbitMQ
- `StreamPulsar` - Apache Pulsar
- `StreamRedis` - Redis Streams
- `StreamNATS` - NATS
- `StreamEventHub` - Azure Event Hubs

## Schema Formats

Supported schema formats for structured data:
- `SchemaJSON` - JSON Schema
- `SchemaAvro` - Apache Avro
- `SchemaProtobuf` - Protocol Buffers
- `SchemaXML` - XML Schema
- `SchemaThrift` - Apache Thrift
- `SchemaParquet` - Apache Parquet

## Selector Types

- `SelectorJSONPath` - JSONPath expressions (e.g., `$.user.email`)
- `SelectorXPath` - XPath expressions (e.g., `//user/email`)
- `SelectorRegex` - Regular expressions
- `SelectorIndex` - Array index (e.g., `0`, `-1`)
- `SelectorKey` - Map/dictionary key
- `SelectorWildcard` - Wildcard selection (`*`)

## Legacy Format Support

The package maintains backward compatibility with the legacy `db://` format:

```go
// Legacy format is automatically converted
legacyURI := "db://database_id.table.column"
addr, _ := resource.ParseResourceURI(legacyURI)
// Converted to: redb://data/database/database_id/table/table/column/column
```

## Files

- `address.go` - Core types and structures
- `parser.go` - URI parsing implementation
- `builder.go` - URI building and construction
- `validator.go` - Address validation and compatibility checking

## Related Packages

- `pkg/unifiedmodel` - UnifiedModel classification and navigation
- `services/core/internal/services/mapping` - Mapping service integration

## Documentation

- [Resource Addressing Specification](../../docs/RESOURCE_ADDRESSING.md)
- [Resource Addressing Examples](../../docs/RESOURCE_ADDRESSING_EXAMPLES.md)
- [UnifiedModel Overview](../../docs/UNIFIED_MODEL.md)

## License

See the main repository LICENSE file.

