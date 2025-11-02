# Resource Addressing Specification

## Overview

The Resource Addressing System provides a unified URI-based scheme for precisely identifying data storage locations, metadata properties, and schema definitions across multiple data sources: databases, data streams, webhooks, and MCP resources.

## URI Format

### General Structure

```
[protocol]://[scope]/[path]#[selector]
```

**Components:**
- **Protocol**: Resource type identifier (`redb`, `stream`, `webhook`, `mcp`)
- **Scope**: Data classification (`data`, `metadata`, `schema`)
- **Path**: Hierarchical location within the resource
- **Selector** (optional): Fine-grained selection using JSONPath, XPath, etc.

## Database Resources (`redb://`)

### Format

```
redb://[scope]/database/{id}/{object-type}/{name}/[path-segments]#[selector]
```

### Scopes

- `data` - Actual data values (column values, field values)
- `metadata` - Structural properties (names, types, constraints)
- `schema` - Schema definitions

### Examples

#### Basic Column Reference
```
# Column data value
redb://data/database/db_abc123/table/users/column/email

# Column metadata (name as a value)
redb://metadata/database/db_abc123/table/users/column/email/name

# Column type information
redb://metadata/database/db_abc123/table/users/column/email/type
```

#### Nested JSON Fields
```
# JSON field within a JSONB column
redb://data/database/db_abc123/table/users/column/profile/field/address/field/city

# Using JSONPath selector
redb://data/database/db_abc123/table/users/column/profile#$.address.city

# Array element in JSON
redb://data/database/db_abc123/table/orders/column/items#$[0].price
```

#### Document Databases (MongoDB, etc.)
```
# Document field
redb://data/database/db_abc123/collection/users/field/email

# Nested document field
redb://data/database/db_abc123/collection/users/field/address/field/city

# Embedded document
redb://data/database/db_abc123/collection/orders/field/items/element/0/field/product_name
```

#### Graph Databases (Neo4j, etc.)
```
# Node property
redb://data/database/db_abc123/node/Person/property/name

# Relationship property
redb://data/database/db_abc123/relationship/KNOWS/property/since

# Node metadata (all property names)
redb://metadata/database/db_abc123/node/Person/properties/names
```

#### Views and Materialized Views
```
# View column
redb://data/database/db_abc123/view/active_users/column/username

# Materialized view column
redb://data/database/db_abc123/materialized_view/user_stats/column/total_orders
```

#### Table Metadata Operations
```
# All column names as an array
redb://metadata/database/db_abc123/table/users/columns/names

# Column data types as a map
redb://metadata/database/db_abc123/table/users/columns/types

# Primary key columns
redb://metadata/database/db_abc123/table/users/constraints/primary_key/columns
```

## Stream Resources (`stream://`)

### Format

```
stream://{provider}/{connection-id}/{object-type}/{name}/[path-segments]#[selector]
```

### Providers

- `kafka` - Apache Kafka
- `mqtt` - MQTT Message Bus
- `kinesis` - AWS Kinesis
- `rabbitmq` - RabbitMQ
- `pulsar` - Apache Pulsar
- `redis-stream` - Redis Streams
- `nats` - NATS Streaming
- `eventhub` - Azure Event Hubs

### Examples

#### Kafka Topics
```
# Message field with Avro schema
stream://kafka/conn_xyz/topic/user-events/schema/avro/field/user_email

# Message field with JSON schema
stream://kafka/conn_xyz/topic/orders/schema/json/field/order_id

# Specific partition
stream://kafka/conn_xyz/topic/orders/partition/0/field/order_id

# Message key
stream://kafka/conn_xyz/topic/users/key/user_id

# Nested field in Avro message
stream://kafka/conn_xyz/topic/events/schema/avro/field/payload/field/user/field/email
```

#### MQTT Topics
```
# Message payload field
stream://mqtt/conn_abc/topic/sensors/temperature/field/value

# Topic with wildcards (for subscription patterns)
stream://mqtt/conn_abc/topic/sensors/+/temperature/field/value

# Nested JSON field in MQTT message
stream://mqtt/conn_abc/topic/telemetry/field/device/field/location#$.coordinates.lat
```

#### AWS Kinesis
```
# Stream record field
stream://kinesis/conn_aws/stream/clickstream/field/user_id

# Partition key
stream://kinesis/conn_aws/stream/events/partition/key

# Nested field with JSONPath
stream://kinesis/conn_aws/stream/events/field/payload#$.user.email
```

#### RabbitMQ
```
# Queue message body field
stream://rabbitmq/conn_rmq/queue/tasks/field/task_name

# Message with routing key
stream://rabbitmq/conn_rmq/queue/notifications/field/message/field/content
```

#### Stream Metadata
```
# Topic schema version
stream://kafka/conn_xyz/topic/users/schema/avro/metadata/version

# Partition count
stream://kafka/conn_xyz/topic/orders/metadata/partition_count
```

## Webhook Resources (`webhook://`)

### Format

```
webhook://{endpoint-id}/{direction}/{component}/[path-segments]#[selector]
```

### Directions

- `request` - Incoming webhook request
- `response` - Outgoing webhook response

### Examples

#### Request Body
```
# JSON field in request body
webhook://endpoint_123/request/body/field/user/field/email

# Top-level field
webhook://endpoint_123/request/body/field/order_id

# Using JSONPath
webhook://endpoint_123/request/body#$.user.email
```

#### Request Headers
```
# Specific header
webhook://endpoint_123/request/header/X-User-ID

# Authorization header
webhook://endpoint_123/request/header/Authorization
```

#### Query Parameters
```
# Query parameter
webhook://endpoint_123/request/query/user_id

# Multiple query parameters (wildcard)
webhook://endpoint_123/request/query/*
```

#### Response Handling
```
# Response body field
webhook://endpoint_123/response/body/field/status

# Response header
webhook://endpoint_123/response/header/X-Request-ID

# Nested response field
webhook://endpoint_123/response/body/field/data/field/results#$[0].id
```

## MCP Resources (`mcp://`)

### Format

```
mcp://{server-id}/{object-type}/{name}/[path-segments]#[selector]
```

### Object Types

- `resource` - MCP resources
- `tool` - MCP tools
- `prompt` - MCP prompts

### Examples

#### MCP Resources
```
# Resource field
mcp://server_mcp1/resource/users/field/email

# Nested resource field
mcp://server_mcp1/resource/user_profile/field/personal/field/name
```

#### MCP Tools
```
# Tool input parameter
mcp://server_mcp1/tool/search_users/parameter/query

# Tool response field
mcp://server_mcp1/tool/get_user/response/field/email

# Nested tool response
mcp://server_mcp1/tool/fetch_data/response/field/results#$[*].id
```

#### MCP Prompts
```
# Prompt parameter
mcp://server_mcp1/prompt/analyze_text/parameter/text

# Prompt response field
mcp://server_mcp1/prompt/summarize/response/field/summary
```

## Selectors

Selectors provide fine-grained selection within complex data types.

### JSONPath Selectors

```
# Nested object field
#$.user.address.city

# Array element
#$[0]

# All array elements
#$[*]

# Filtered array
#$[?(@.price > 10)]

# Nested array access
#$.orders[0].items[*].name
```

### XPath Selectors (for XML data)

```
# Element selection
#//user/email

# Attribute selection
#//user/@id

# Nested elements
#//order/items/item[1]/name
```

### Index Selectors

```
# Array index
#0

# Negative index (from end)
#-1
```

### Wildcard Selectors

```
# All fields/columns
#*

# Pattern matching (implementation-specific)
#user_*
```

## Path Segment Types

Path segments use typed components to navigate through resource structures:

- `column` - Table column
- `field` - Document/message field
- `property` - Graph node/relationship property
- `element` - Array element
- `key` - Map/dictionary key
- `partition` - Stream partition
- `header` - HTTP/message header
- `query` - Query parameter
- `parameter` - Function/tool parameter
- `body` - Request/response body
- `schema` - Schema definition

## Legacy Format Support

The system supports the legacy `db://` format for backward compatibility:

### Legacy Format

```
db://database_id.table.column
@db://database_id.table.column
```

### Automatic Conversion

Legacy URIs are automatically converted to the new format:

```
db://db_123.users.email  →  redb://data/database/db_123/table/users/column/email
```

## Use Cases

### Simple Column Mapping

Map a source column to a target column:

```
Source: redb://data/database/src_db/table/users/column/email
Target: redb://data/database/tgt_db/table/profiles/column/user_email
```

### Metadata to Data Mapping

Map column names to a JSON array:

```
Source: redb://metadata/database/src_db/table/users/columns/names
Target: redb://data/database/tgt_db/table/metadata/column/table_columns
```

### Stream to Database

Ingest streaming data into a database:

```
Source: stream://kafka/conn_xyz/topic/events/schema/json/field/user_id
Target: redb://data/database/db_123/table/events/column/user_id
```

### Database to Webhook

Send database changes to a webhook:

```
Source: redb://data/database/db_123/table/users/column/email
Target: webhook://endpoint_456/request/body/field/email
```

### Stream to MCP Tool

Process streaming data with an MCP tool:

```
Source: stream://kafka/conn_xyz/topic/logs/schema/json/field/message
Target: mcp://server_mcp1/tool/analyze_log/parameter/log_text
```

## Validation Rules

1. **Protocol**: Must be one of `redb`, `stream`, `webhook`, `mcp`
2. **Scope**: Must be one of `data`, `metadata`, `schema`
3. **IDs**: Required identifiers based on protocol:
   - Database: `database_id`
   - Stream: `connection_id` + `provider`
   - Webhook: `endpoint_id`
   - MCP: `server_id`
4. **Object Type**: Must be appropriate for the protocol
5. **Object Name**: Required for most resources
6. **Path Segments**: Must alternate between segment type and value
7. **Selectors**: Must be valid for the data type (JSONPath for JSON, etc.)

## Best Practices

1. **Use Explicit Paths**: Prefer explicit paths over selectors when possible for better readability
2. **Metadata Scope**: Use metadata scope when mapping structural information
3. **Selectors for Complex Types**: Use selectors for deep navigation in JSON/XML
4. **Stream Schemas**: Always specify schema format for structured streaming data
5. **Backward Compatibility**: Support legacy `db://` format in existing systems
6. **Validation**: Always validate URIs before using them in mappings
7. **Documentation**: Document custom selector patterns used in your organization

