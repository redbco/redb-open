// Package resource provides resource addressing and navigation for UnifiedModel and other data sources.
// It enables precise identification of data storage locations across databases, streams, webhooks, and MCP resources.
package resource

import "fmt"

// ResourceProtocol identifies the type of resource being addressed
type ResourceProtocol string

const (
	// ProtocolDatabase addresses database resources via UnifiedModel
	ProtocolDatabase ResourceProtocol = "redb"
	// ProtocolStream addresses data stream resources (Kafka, MQTT, etc.)
	ProtocolStream ResourceProtocol = "stream"
	// ProtocolWebhook addresses HTTP webhook endpoints
	ProtocolWebhook ResourceProtocol = "webhook"
	// ProtocolMCP addresses Model Context Protocol resources/tools
	ProtocolMCP ResourceProtocol = "mcp"
)

// ResourceScope indicates the type of data being addressed
type ResourceScope string

const (
	// ScopeData addresses actual data values (column values, message payloads, etc.)
	ScopeData ResourceScope = "data"
	// ScopeMetadata addresses metadata properties (column names, types, etc.)
	ScopeMetadata ResourceScope = "metadata"
	// ScopeSchema addresses schema definitions (table schemas, message schemas, etc.)
	ScopeSchema ResourceScope = "schema"
)

// ResourceType categorizes the kind of resource
type ResourceType string

const (
	TypeDatabase ResourceType = "database"
	TypeStream   ResourceType = "stream"
	TypeWebhook  ResourceType = "webhook"
	TypeMCP      ResourceType = "mcp"
)

// ObjectType specifies the type of object within a resource
type ObjectType string

const (
	// Database object types
	ObjectTypeTable            ObjectType = "table"
	ObjectTypeCollection       ObjectType = "collection"
	ObjectTypeView             ObjectType = "view"
	ObjectTypeMaterializedView ObjectType = "materialized_view"
	ObjectTypeNode             ObjectType = "node"
	ObjectTypeRelationship     ObjectType = "relationship"
	ObjectTypeExternalTable    ObjectType = "external_table"
	ObjectTypeForeignTable     ObjectType = "foreign_table"

	// Stream object types
	ObjectTypeTopic     ObjectType = "topic"
	ObjectTypeQueue     ObjectType = "queue"
	ObjectTypeStream    ObjectType = "stream"
	ObjectTypePartition ObjectType = "partition"

	// Webhook object types
	ObjectTypeEndpoint ObjectType = "endpoint"
	ObjectTypeRequest  ObjectType = "request"
	ObjectTypeResponse ObjectType = "response"

	// MCP object types
	ObjectTypeResource ObjectType = "resource"
	ObjectTypeTool     ObjectType = "tool"
	ObjectTypePrompt   ObjectType = "prompt"
)

// SegmentType identifies the type of path segment
type SegmentType string

const (
	SegmentTypeColumn     SegmentType = "column"
	SegmentTypeField      SegmentType = "field"
	SegmentTypeProperty   SegmentType = "property"
	SegmentTypeElement    SegmentType = "element"
	SegmentTypeKey        SegmentType = "key"
	SegmentTypePartition  SegmentType = "partition"
	SegmentTypeHeader     SegmentType = "header"
	SegmentTypeQuery      SegmentType = "query"
	SegmentTypeParameter  SegmentType = "parameter"
	SegmentTypeBody       SegmentType = "body"
	SegmentTypePath       SegmentType = "path"
	SegmentTypeAttributes SegmentType = "attributes"
)

// StreamProvider identifies the streaming platform
type StreamProvider string

const (
	StreamKafka    StreamProvider = "kafka"
	StreamMQTT     StreamProvider = "mqtt"
	StreamKinesis  StreamProvider = "kinesis"
	StreamRabbitMQ StreamProvider = "rabbitmq"
	StreamPulsar   StreamProvider = "pulsar"
	StreamRedis    StreamProvider = "redis-stream"
	StreamNATS     StreamProvider = "nats"
	StreamEventHub StreamProvider = "eventhub"
	StreamCloudRun StreamProvider = "cloudrun"
)

// SchemaFormat specifies the format of structured data
type SchemaFormat string

const (
	SchemaAvro     SchemaFormat = "avro"
	SchemaProtobuf SchemaFormat = "protobuf"
	SchemaJSON     SchemaFormat = "json"
	SchemaXML      SchemaFormat = "xml"
	SchemaThrift   SchemaFormat = "thrift"
	SchemaCSV      SchemaFormat = "csv"
	SchemaParquet  SchemaFormat = "parquet"
)

// SelectorType specifies how to select within complex types
type SelectorType string

const (
	SelectorJSONPath SelectorType = "jsonpath"
	SelectorXPath    SelectorType = "xpath"
	SelectorRegex    SelectorType = "regex"
	SelectorIndex    SelectorType = "index"
	SelectorKey      SelectorType = "key"
	SelectorWildcard SelectorType = "wildcard"
)

// ResourceAddress represents a complete address to a resource location
type ResourceAddress struct {
	// Protocol identifies the addressing scheme (redb, stream, webhook, mcp)
	Protocol ResourceProtocol

	// Scope indicates whether addressing data, metadata, or schema
	Scope ResourceScope

	// Protocol-specific identifiers
	DatabaseID   string // For redb:// - database identifier
	ConnectionID string // For stream://, webhook:// - connection identifier
	ServerID     string // For mcp:// - MCP server identifier

	// Resource type categorization
	ResourceType ResourceType
	ObjectType   ObjectType
	ObjectName   string

	// Stream-specific fields
	StreamProvider StreamProvider
	SchemaFormat   SchemaFormat

	// Path navigation through the resource structure
	PathSegments []PathSegment

	// Optional fine-grained selector (e.g., JSONPath expression)
	Selector *Selector

	// Additional metadata for context
	Metadata map[string]interface{}
}

// PathSegment represents one level in a hierarchical path
type PathSegment struct {
	Type     SegmentType
	Name     string
	Index    *int                   // For array element access
	Metadata map[string]interface{} // Additional context
}

// Selector provides fine-grained selection within complex types
type Selector struct {
	Type       SelectorType
	Expression string
	Compiled   interface{} // Cached compiled selector (e.g., compiled JSONPath)
}

// String returns a human-readable representation of the resource address
func (r *ResourceAddress) String() string {
	if r == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s://%s/%s/%s/%s", r.Protocol, r.Scope, r.ResourceType, r.ObjectType, r.ObjectName)
}

// IsDatabase returns true if this address points to a database resource
func (r *ResourceAddress) IsDatabase() bool {
	return r.Protocol == ProtocolDatabase
}

// IsStream returns true if this address points to a stream resource
func (r *ResourceAddress) IsStream() bool {
	return r.Protocol == ProtocolStream
}

// IsWebhook returns true if this address points to a webhook resource
func (r *ResourceAddress) IsWebhook() bool {
	return r.Protocol == ProtocolWebhook
}

// IsMCP returns true if this address points to an MCP resource
func (r *ResourceAddress) IsMCP() bool {
	return r.Protocol == ProtocolMCP
}

// IsDataScope returns true if this address points to actual data values
func (r *ResourceAddress) IsDataScope() bool {
	return r.Scope == ScopeData
}

// IsMetadataScope returns true if this address points to metadata
func (r *ResourceAddress) IsMetadataScope() bool {
	return r.Scope == ScopeMetadata
}

// IsSchemaScope returns true if this address points to schema definitions
func (r *ResourceAddress) IsSchemaScope() bool {
	return r.Scope == ScopeSchema
}

// HasSelector returns true if this address has a selector
func (r *ResourceAddress) HasSelector() bool {
	return r.Selector != nil
}

// PathDepth returns the number of path segments
func (r *ResourceAddress) PathDepth() int {
	return len(r.PathSegments)
}

// GetPathSegment returns the path segment at the specified index, or nil if out of bounds
func (r *ResourceAddress) GetPathSegment(index int) *PathSegment {
	if index < 0 || index >= len(r.PathSegments) {
		return nil
	}
	return &r.PathSegments[index]
}

// LastPathSegment returns the last path segment, or nil if there are no segments
func (r *ResourceAddress) LastPathSegment() *PathSegment {
	if len(r.PathSegments) == 0 {
		return nil
	}
	return &r.PathSegments[len(r.PathSegments)-1]
}

// Clone creates a deep copy of the resource address
func (r *ResourceAddress) Clone() *ResourceAddress {
	if r == nil {
		return nil
	}

	clone := &ResourceAddress{
		Protocol:       r.Protocol,
		Scope:          r.Scope,
		DatabaseID:     r.DatabaseID,
		ConnectionID:   r.ConnectionID,
		ServerID:       r.ServerID,
		ResourceType:   r.ResourceType,
		ObjectType:     r.ObjectType,
		ObjectName:     r.ObjectName,
		StreamProvider: r.StreamProvider,
		SchemaFormat:   r.SchemaFormat,
		PathSegments:   make([]PathSegment, len(r.PathSegments)),
	}

	copy(clone.PathSegments, r.PathSegments)

	if r.Selector != nil {
		clone.Selector = &Selector{
			Type:       r.Selector.Type,
			Expression: r.Selector.Expression,
			Compiled:   r.Selector.Compiled,
		}
	}

	if r.Metadata != nil {
		clone.Metadata = make(map[string]interface{})
		for k, v := range r.Metadata {
			clone.Metadata[k] = v
		}
	}

	return clone
}

// Validate performs basic validation on the resource address
func (r *ResourceAddress) Validate() error {
	if r == nil {
		return fmt.Errorf("resource address is nil")
	}

	if r.Protocol == "" {
		return fmt.Errorf("protocol is required")
	}

	if r.Scope == "" {
		return fmt.Errorf("scope is required")
	}

	// Protocol-specific validation
	switch r.Protocol {
	case ProtocolDatabase:
		if r.DatabaseID == "" {
			return fmt.Errorf("database_id is required for database protocol")
		}
	case ProtocolStream:
		if r.ConnectionID == "" {
			return fmt.Errorf("connection_id is required for stream protocol")
		}
		if r.StreamProvider == "" {
			return fmt.Errorf("stream_provider is required for stream protocol")
		}
	case ProtocolWebhook:
		if r.ConnectionID == "" {
			return fmt.Errorf("connection_id is required for webhook protocol")
		}
	case ProtocolMCP:
		if r.ServerID == "" {
			return fmt.Errorf("server_id is required for MCP protocol")
		}
	default:
		return fmt.Errorf("unknown protocol: %s", r.Protocol)
	}

	if r.ObjectType == "" {
		return fmt.Errorf("object_type is required")
	}

	if r.ObjectName == "" {
		return fmt.Errorf("object_name is required")
	}

	return nil
}
