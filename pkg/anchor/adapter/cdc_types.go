package adapter

import (
	"fmt"
	"time"
)

// CDCOperation represents the type of change in a CDC event.
type CDCOperation string

const (
	// CDCInsert represents an INSERT operation
	CDCInsert CDCOperation = "INSERT"
	// CDCUpdate represents an UPDATE operation
	CDCUpdate CDCOperation = "UPDATE"
	// CDCDelete represents a DELETE operation
	CDCDelete CDCOperation = "DELETE"
	// CDCTruncate represents a TRUNCATE operation
	CDCTruncate CDCOperation = "TRUNCATE"
)

// CDCEvent represents a standardized CDC event across all database types.
// This is the universal format that all database adapters must produce and consume.
type CDCEvent struct {
	// Operation type (INSERT, UPDATE, DELETE, TRUNCATE)
	Operation CDCOperation `json:"operation"`

	// Target identification
	SchemaName string `json:"schema_name,omitempty"` // Schema/database name (optional)
	TableName  string `json:"table_name"`            // Table/collection name

	// Event data
	Data    map[string]interface{} `json:"data,omitempty"`     // New data for INSERT/UPDATE
	OldData map[string]interface{} `json:"old_data,omitempty"` // Old data for UPDATE/DELETE

	// Event metadata
	Timestamp     time.Time              `json:"timestamp"`                // Event timestamp
	LSN           string                 `json:"lsn,omitempty"`            // Log sequence number (database-specific)
	TransactionID string                 `json:"transaction_id,omitempty"` // Transaction identifier
	Metadata      map[string]interface{} `json:"metadata,omitempty"`       // Additional database-specific metadata
	SourceNode    string                 `json:"source_node,omitempty"`    // Source node ID (for mesh routing)
	TargetNode    string                 `json:"target_node,omitempty"`    // Target node ID (for mesh routing)
}

// Validate checks if the CDC event is valid.
func (e *CDCEvent) Validate() error {
	if e.TableName == "" {
		return fmt.Errorf("table_name is required")
	}

	switch e.Operation {
	case CDCInsert:
		if len(e.Data) == 0 {
			return fmt.Errorf("data is required for INSERT operation")
		}
	case CDCUpdate:
		if len(e.Data) == 0 {
			return fmt.Errorf("data is required for UPDATE operation")
		}
		// OldData is optional but recommended
	case CDCDelete:
		if len(e.OldData) == 0 {
			return fmt.Errorf("old_data is required for DELETE operation")
		}
	case CDCTruncate:
		// No data required for TRUNCATE
	default:
		return fmt.Errorf("unknown operation: %s", e.Operation)
	}

	return nil
}

// TransformationRule defines how to transform data from source to target.
// This is used for field mapping, type conversion, and value transformation.
type TransformationRule struct {
	// Source field identification
	SourceColumn string `json:"source_column"`
	SourceTable  string `json:"source_table,omitempty"`

	// Target field identification
	TargetColumn string `json:"target_column"`
	TargetTable  string `json:"target_table,omitempty"`

	// Transformation configuration
	TransformationType string                 `json:"transformation_type"`           // direct, cast, function, expression
	TransformationName string                 `json:"transformation_name,omitempty"` // Name of transformation function (e.g., "reverse", "uppercase")
	Parameters         map[string]interface{} `json:"parameters,omitempty"`

	// Metadata
	Description string `json:"description,omitempty"`
}

// TransformationType constants
const (
	// TransformDirect - direct field mapping with no transformation
	TransformDirect = "direct"
	// TransformCast - type casting (e.g., string to int)
	TransformCast = "cast"
	// TransformUppercase - convert to uppercase
	TransformUppercase = "uppercase"
	// TransformLowercase - convert to lowercase
	TransformLowercase = "lowercase"
	// TransformFunction - apply a custom function
	TransformFunction = "function"
	// TransformExpression - evaluate an expression
	TransformExpression = "expression"
	// TransformDefault - use default value if source is null
	TransformDefault = "default"
)

// CDCEventFilter defines filtering criteria for CDC events.
// This will be used in future to filter which events are replicated.
type CDCEventFilter struct {
	// Table filtering
	IncludeTables []string `json:"include_tables,omitempty"`
	ExcludeTables []string `json:"exclude_tables,omitempty"`

	// Operation filtering
	IncludeOperations []CDCOperation `json:"include_operations,omitempty"`
	ExcludeOperations []CDCOperation `json:"exclude_operations,omitempty"`

	// Data filtering (future: WHERE clause equivalent)
	Conditions map[string]interface{} `json:"conditions,omitempty"`
}

// CDCStatistics tracks CDC replication statistics.
type CDCStatistics struct {
	EventsProcessed    int64                  `json:"events_processed"`
	EventsFailed       int64                  `json:"events_failed"`
	LastEventTimestamp time.Time              `json:"last_event_timestamp"`
	LastEventLSN       string                 `json:"last_event_lsn,omitempty"`
	OperationCounts    map[CDCOperation]int64 `json:"operation_counts"`
	AverageLatency     time.Duration          `json:"average_latency"`
	CurrentLag         time.Duration          `json:"current_lag,omitempty"`
	BytesProcessed     int64                  `json:"bytes_processed"`
	AdditionalMetrics  map[string]interface{} `json:"additional_metrics,omitempty"`
}

// NewCDCStatistics creates a new CDC statistics tracker.
func NewCDCStatistics() *CDCStatistics {
	return &CDCStatistics{
		OperationCounts:   make(map[CDCOperation]int64),
		AdditionalMetrics: make(map[string]interface{}),
	}
}

// RecordEvent records a successfully processed event.
func (s *CDCStatistics) RecordEvent(event *CDCEvent, latency time.Duration) {
	s.EventsProcessed++
	s.OperationCounts[event.Operation]++
	s.LastEventTimestamp = event.Timestamp
	s.LastEventLSN = event.LSN

	// Update average latency (simple moving average)
	if s.EventsProcessed == 1 {
		s.AverageLatency = latency
	} else {
		s.AverageLatency = (s.AverageLatency*time.Duration(s.EventsProcessed-1) + latency) / time.Duration(s.EventsProcessed)
	}
}

// RecordFailure records a failed event.
func (s *CDCStatistics) RecordFailure() {
	s.EventsFailed++
}
