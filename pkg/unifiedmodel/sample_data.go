package unifiedmodel

import (
	"time"
)

// UnifiedModelSampleData represents sample data collected from database tables and collections.
// This data is used exclusively for privileged data detection and is never persisted.
// It supports all database paradigms: relational, document, key-value, graph, columnar, etc.
type UnifiedModelSampleData struct {
	// Metadata
	SchemaID      string    `json:"schema_id"`
	CollectedAt   time.Time `json:"collected_at"`
	CollectedBy   string    `json:"collected_by"`
	SampleVersion string    `json:"sample_version"`

	// Configuration
	SampleConfig SampleDataConfig `json:"sample_config"`

	// Sample data by database paradigm
	TableSamples      map[string]TableSampleData      `json:"table_samples"`       // Relational tables
	CollectionSamples map[string]CollectionSampleData `json:"collection_samples"`  // Document collections
	KeyValueSamples   map[string]KeyValueSampleData   `json:"key_value_samples"`   // Key-value pairs
	GraphSamples      map[string]GraphSampleData      `json:"graph_samples"`       // Graph nodes/edges
	ColumnSamples     map[string]ColumnSampleData     `json:"column_samples"`      // Columnar data
	SearchSamples     map[string]SearchSampleData     `json:"search_samples"`      // Search index docs
	VectorSamples     map[string]VectorSampleData     `json:"vector_samples"`      // Vector embeddings
	TimeSeriesSamples map[string]TimeSeriesSampleData `json:"time_series_samples"` // Time-series data
	ObjectSamples     map[string]ObjectSampleData     `json:"object_samples"`      // Object storage

	// Cross-paradigm samples
	CustomSamples map[string]CustomSampleData `json:"custom_samples,omitempty"` // Database-specific structures

	// Privacy and security
	ContainsPII      bool     `json:"contains_pii"`
	PiiSummary       []string `json:"pii_summary,omitempty"`
	RedactionApplied bool     `json:"redaction_applied"`
	RedactionLevel   string   `json:"redaction_level,omitempty"`
}

// SampleDataConfig defines how sample data should be collected and processed
type SampleDataConfig struct {
	MaxRowsPerTable      int      `json:"max_rows_per_table"`
	MaxDocsPerCollection int      `json:"max_docs_per_collection"`
	MaxKeysPerNamespace  int      `json:"max_keys_per_namespace"`
	MaxNodesPerGraph     int      `json:"max_nodes_per_graph"`
	MaxValuesPerColumn   int      `json:"max_values_per_column"`
	SamplingStrategy     string   `json:"sampling_strategy"` // "random", "systematic", "stratified"
	IncludeNullValues    bool     `json:"include_null_values"`
	IncludeEmptyValues   bool     `json:"include_empty_values"`
	RedactSensitiveData  bool     `json:"redact_sensitive_data"`
	ExcludePatterns      []string `json:"exclude_patterns,omitempty"`      // Regex patterns to exclude
	IncludeOnlyPatterns  []string `json:"include_only_patterns,omitempty"` // Regex patterns to include only
	MaxStringLength      int      `json:"max_string_length"`               // Truncate long strings
	CollectMetadata      bool     `json:"collect_metadata"`                // Include value length, type info
}

// TableSampleData represents sample data from a relational table
type TableSampleData struct {
	TableName   string                        `json:"table_name"`
	RowCount    int64                         `json:"row_count"`    // Total rows in table
	SampleCount int                           `json:"sample_count"` // Number of sampled rows
	Columns     map[string]ColumnSampleValues `json:"columns"`      // Sample values per column
	Rows        []map[string]interface{}      `json:"rows"`         // Complete row samples
	CollectedAt time.Time                     `json:"collected_at"`
	Warnings    []string                      `json:"warnings,omitempty"`
}

// CollectionSampleData represents sample data from a document collection (MongoDB, CosmosDB)
type CollectionSampleData struct {
	CollectionName string                       `json:"collection_name"`
	DocumentCount  int64                        `json:"document_count"`
	SampleCount    int                          `json:"sample_count"`
	Documents      []map[string]interface{}     `json:"documents"`      // Sample documents
	FieldSamples   map[string]FieldSampleValues `json:"field_samples"`  // Field-level samples
	SchemaProfile  DocumentSchemaProfile        `json:"schema_profile"` // Detected field types/patterns
	CollectedAt    time.Time                    `json:"collected_at"`
	Warnings       []string                     `json:"warnings,omitempty"`
}

// KeyValueSampleData represents sample data from key-value stores (Redis, DynamoDB)
type KeyValueSampleData struct {
	NamespaceName string                     `json:"namespace_name"` // Redis DB, DynamoDB table
	KeyCount      int64                      `json:"key_count"`
	SampleCount   int                        `json:"sample_count"`
	KeySamples    []KeyValueSampleEntry      `json:"key_samples"`
	ValueTypes    map[string]int             `json:"value_types"`   // Type distribution
	KeyPatterns   map[string]KeyPatternInfo  `json:"key_patterns"`  // Detected key patterns
	ValueSamples  map[string]ValueSampleInfo `json:"value_samples"` // Sample values by type
	CollectedAt   time.Time                  `json:"collected_at"`
	Warnings      []string                   `json:"warnings,omitempty"`
}

// GraphSampleData represents sample data from graph databases (Neo4j, EdgeDB)
type GraphSampleData struct {
	GraphName   string                    `json:"graph_name"`
	NodeSamples map[string]NodeSampleData `json:"node_samples"` // Samples by node label
	EdgeSamples map[string]EdgeSampleData `json:"edge_samples"` // Samples by edge type
	NodeCount   int64                     `json:"node_count"`
	EdgeCount   int64                     `json:"edge_count"`
	SampleCount int                       `json:"sample_count"`
	CollectedAt time.Time                 `json:"collected_at"`
	Warnings    []string                  `json:"warnings,omitempty"`
}

// ColumnSampleData represents sample data from columnar stores (ClickHouse, Cassandra wide-column)
type ColumnSampleData struct {
	ColumnFamilyName string                        `json:"column_family_name"`
	RowCount         int64                         `json:"row_count"`
	SampleCount      int                           `json:"sample_count"`
	ColumnSamples    map[string]ColumnSampleValues `json:"column_samples"`
	PartitionSamples []PartitionSampleData         `json:"partition_samples"` // Cassandra partitions
	CollectedAt      time.Time                     `json:"collected_at"`
	Warnings         []string                      `json:"warnings,omitempty"`
}

// SearchSampleData represents sample data from search indexes (Elasticsearch)
type SearchSampleData struct {
	IndexName     string                       `json:"index_name"`
	DocumentCount int64                        `json:"document_count"`
	SampleCount   int                          `json:"sample_count"`
	Documents     []map[string]interface{}     `json:"documents"`
	FieldSamples  map[string]FieldSampleValues `json:"field_samples"`
	MappingInfo   SearchMappingInfo            `json:"mapping_info"`
	CollectedAt   time.Time                    `json:"collected_at"`
	Warnings      []string                     `json:"warnings,omitempty"`
}

// VectorSampleData represents sample data from vector databases (Milvus, Weaviate, Pinecone)
type VectorSampleData struct {
	CollectionName   string                       `json:"collection_name"`
	VectorCount      int64                        `json:"vector_count"`
	SampleCount      int                          `json:"sample_count"`
	VectorSamples    []VectorSample               `json:"vector_samples"`
	MetadataSamples  map[string]FieldSampleValues `json:"metadata_samples"` // Associated metadata
	VectorDimensions int                          `json:"vector_dimensions"`
	CollectedAt      time.Time                    `json:"collected_at"`
	Warnings         []string                     `json:"warnings,omitempty"`
}

// TimeSeriesSampleData represents sample data from time-series databases (specialized views)
type TimeSeriesSampleData struct {
	SeriesName     string                        `json:"series_name"`
	DataPointCount int64                         `json:"data_point_count"`
	SampleCount    int                           `json:"sample_count"`
	TimeRange      TimeRange                     `json:"time_range"`
	DataPoints     []TimeSeriesDataPoint         `json:"data_points"`
	MetricSamples  map[string]ColumnSampleValues `json:"metric_samples"` // Sample values per metric
	TagSamples     map[string]ColumnSampleValues `json:"tag_samples"`    // Sample tag values
	CollectedAt    time.Time                     `json:"collected_at"`
	Warnings       []string                      `json:"warnings,omitempty"`
}

// ObjectSampleData represents sample data from object storage (S3, GCS, Azure Blob)
type ObjectSampleData struct {
	BucketName      string                       `json:"bucket_name"`
	ObjectCount     int64                        `json:"object_count"`
	SampleCount     int                          `json:"sample_count"`
	ObjectSamples   []ObjectSample               `json:"object_samples"`
	MetadataSamples map[string]FieldSampleValues `json:"metadata_samples"` // Object metadata
	ContentSamples  map[string]ContentSample     `json:"content_samples"`  // Content snippets by type
	PathPatterns    []string                     `json:"path_patterns"`
	CollectedAt     time.Time                    `json:"collected_at"`
	Warnings        []string                     `json:"warnings,omitempty"`
}

// CustomSampleData represents database-specific sample data structures
type CustomSampleData struct {
	DatabaseType     string                   `json:"database_type"`
	StructureName    string                   `json:"structure_name"`
	SampleCount      int                      `json:"sample_count"`
	RawSamples       []map[string]interface{} `json:"raw_samples"`
	ProcessedSamples map[string]interface{}   `json:"processed_samples,omitempty"`
	CollectedAt      time.Time                `json:"collected_at"`
	Warnings         []string                 `json:"warnings,omitempty"`
}

// Supporting structures for different sample types

// ColumnSampleValues represents sample values for a single column/field
type ColumnSampleValues struct {
	FieldName       string        `json:"field_name"`
	DataType        string        `json:"data_type"`
	Values          []interface{} `json:"values"`         // Actual sample values
	DistinctCount   int           `json:"distinct_count"` // Number of distinct values
	NullCount       int           `json:"null_count"`
	EmptyCount      int           `json:"empty_count"`
	MinLength       *int          `json:"min_length,omitempty"`       // For strings
	MaxLength       *int          `json:"max_length,omitempty"`       // For strings
	AvgLength       *float64      `json:"avg_length,omitempty"`       // For strings
	MinValue        interface{}   `json:"min_value,omitempty"`        // For numeric/date
	MaxValue        interface{}   `json:"max_value,omitempty"`        // For numeric/date
	ValuePatterns   []string      `json:"value_patterns,omitempty"`   // Detected regex patterns
	CommonValues    []ValueFreq   `json:"common_values,omitempty"`    // Most frequent values
	PiiIndicators   []string      `json:"pii_indicators,omitempty"`   // Detected PII patterns
	SensitivityTags []string      `json:"sensitivity_tags,omitempty"` // Privacy classification
}

// FieldSampleValues extends ColumnSampleValues for document/object fields
type FieldSampleValues struct {
	ColumnSampleValues
	NestedFields map[string]FieldSampleValues `json:"nested_fields,omitempty"` // For nested documents
	ArraySamples []interface{}                `json:"array_samples,omitempty"` // For array fields
}

// KeyValueSampleEntry represents a single key-value sample
type KeyValueSampleEntry struct {
	Key          string      `json:"key"`
	Value        interface{} `json:"value"`
	ValueType    string      `json:"value_type"`
	TTL          *int64      `json:"ttl,omitempty"`  // Time to live (Redis)
	Size         *int        `json:"size,omitempty"` // Value size in bytes
	KeyStructure string      `json:"key_structure"`  // Detected key pattern
}

// Additional supporting structures

type DocumentSchemaProfile struct {
	FieldTypes     map[string]string `json:"field_types"`
	RequiredFields []string          `json:"required_fields"`
	OptionalFields []string          `json:"optional_fields"`
	NestedLevels   int               `json:"nested_levels"`
	ArrayFields    []string          `json:"array_fields"`
}

type KeyPatternInfo struct {
	Pattern    string   `json:"pattern"`
	Count      int      `json:"count"`
	Confidence float64  `json:"confidence"`
	Examples   []string `json:"examples"`
}

type ValueSampleInfo struct {
	ValueType    string        `json:"value_type"`
	SampleValues []interface{} `json:"sample_values"`
	Count        int           `json:"count"`
}

type NodeSampleData struct {
	NodeLabel  string                        `json:"node_label"`
	Count      int64                         `json:"count"`
	Samples    []map[string]interface{}      `json:"samples"`
	Properties map[string]ColumnSampleValues `json:"properties"`
}

type EdgeSampleData struct {
	EdgeType   string                        `json:"edge_type"`
	Count      int64                         `json:"count"`
	Samples    []GraphEdgeSample             `json:"samples"`
	Properties map[string]ColumnSampleValues `json:"properties"`
}

type GraphEdgeSample struct {
	SourceNode map[string]interface{} `json:"source_node"`
	TargetNode map[string]interface{} `json:"target_node"`
	Properties map[string]interface{} `json:"properties"`
}

type PartitionSampleData struct {
	PartitionKey string                        `json:"partition_key"`
	RowCount     int64                         `json:"row_count"`
	Columns      map[string]ColumnSampleValues `json:"columns"`
}

type SearchMappingInfo struct {
	IndexSettings map[string]interface{} `json:"index_settings"`
	FieldMappings map[string]interface{} `json:"field_mappings"`
	Analyzers     map[string]interface{} `json:"analyzers,omitempty"`
}

type VectorSample struct {
	ID         interface{}            `json:"id"`
	Vector     []float64              `json:"vector,omitempty"` // May be redacted for privacy
	Metadata   map[string]interface{} `json:"metadata"`
	Score      *float64               `json:"score,omitempty"`
	VectorHash *string                `json:"vector_hash,omitempty"` // Hash instead of actual vector
}

type TimeRange struct {
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
}

type TimeSeriesDataPoint struct {
	Timestamp time.Time              `json:"timestamp"`
	Metrics   map[string]interface{} `json:"metrics"`
	Tags      map[string]interface{} `json:"tags"`
}

type ObjectSample struct {
	ObjectKey      string                 `json:"object_key"`
	Size           int64                  `json:"size"`
	ContentType    string                 `json:"content_type"`
	Metadata       map[string]interface{} `json:"metadata"`
	ContentPreview *string                `json:"content_preview,omitempty"` // First few bytes/lines
	FileExtension  string                 `json:"file_extension"`
	LastModified   time.Time              `json:"last_modified"`
}

type ContentSample struct {
	ContentType string   `json:"content_type"`
	Samples     []string `json:"samples"` // Content snippets
	Count       int      `json:"count"`
}

type ValueFreq struct {
	Value      interface{} `json:"value"`
	Frequency  int         `json:"frequency"`
	Percentage float64     `json:"percentage"`
}

// NewUnifiedModelSampleData creates a new, empty sample data structure
func NewUnifiedModelSampleData(schemaID string) *UnifiedModelSampleData {
	return &UnifiedModelSampleData{
		SchemaID:          schemaID,
		CollectedAt:       time.Now(),
		SampleVersion:     "1.0",
		SampleConfig:      DefaultSampleDataConfig(),
		TableSamples:      make(map[string]TableSampleData),
		CollectionSamples: make(map[string]CollectionSampleData),
		KeyValueSamples:   make(map[string]KeyValueSampleData),
		GraphSamples:      make(map[string]GraphSampleData),
		ColumnSamples:     make(map[string]ColumnSampleData),
		SearchSamples:     make(map[string]SearchSampleData),
		VectorSamples:     make(map[string]VectorSampleData),
		TimeSeriesSamples: make(map[string]TimeSeriesSampleData),
		ObjectSamples:     make(map[string]ObjectSampleData),
		CustomSamples:     make(map[string]CustomSampleData),
		PiiSummary:        make([]string, 0),
	}
}

// DefaultSampleDataConfig returns sensible defaults for sample data collection
func DefaultSampleDataConfig() SampleDataConfig {
	return SampleDataConfig{
		MaxRowsPerTable:      100,
		MaxDocsPerCollection: 50,
		MaxKeysPerNamespace:  200,
		MaxNodesPerGraph:     100,
		MaxValuesPerColumn:   50,
		SamplingStrategy:     "random",
		IncludeNullValues:    true,
		IncludeEmptyValues:   true,
		RedactSensitiveData:  false, // Set to true in production
		MaxStringLength:      1000,
		CollectMetadata:      true,
	}
}

// PrivacyAwareSampleDataConfig returns configuration for privacy-compliant sample collection
func PrivacyAwareSampleDataConfig() SampleDataConfig {
	return SampleDataConfig{
		MaxRowsPerTable:      20, // Smaller samples
		MaxDocsPerCollection: 10,
		MaxKeysPerNamespace:  50,
		MaxNodesPerGraph:     25,
		MaxValuesPerColumn:   20,
		SamplingStrategy:     "random",
		IncludeNullValues:    true,
		IncludeEmptyValues:   true,
		RedactSensitiveData:  true,
		ExcludePatterns: []string{
			".*ssn.*", ".*social.*", ".*password.*", ".*token.*", ".*key.*",
			".*email.*", ".*phone.*", ".*address.*", ".*credit.*card.*",
		},
		MaxStringLength: 100, // Truncate to reduce exposure
		CollectMetadata: true,
	}
}

// HasSampleData checks if any sample data is present
func (s *UnifiedModelSampleData) HasSampleData() bool {
	return len(s.TableSamples) > 0 ||
		len(s.CollectionSamples) > 0 ||
		len(s.KeyValueSamples) > 0 ||
		len(s.GraphSamples) > 0 ||
		len(s.ColumnSamples) > 0 ||
		len(s.SearchSamples) > 0 ||
		len(s.VectorSamples) > 0 ||
		len(s.TimeSeriesSamples) > 0 ||
		len(s.ObjectSamples) > 0 ||
		len(s.CustomSamples) > 0
}

// GetTotalSampleCount returns the total number of sample records across all paradigms
func (s *UnifiedModelSampleData) GetTotalSampleCount() int {
	total := 0

	for _, table := range s.TableSamples {
		total += table.SampleCount
	}
	for _, collection := range s.CollectionSamples {
		total += collection.SampleCount
	}
	for _, kv := range s.KeyValueSamples {
		total += kv.SampleCount
	}
	for _, graph := range s.GraphSamples {
		total += graph.SampleCount
	}
	for _, column := range s.ColumnSamples {
		total += column.SampleCount
	}
	for _, search := range s.SearchSamples {
		total += search.SampleCount
	}
	for _, vector := range s.VectorSamples {
		total += vector.SampleCount
	}
	for _, ts := range s.TimeSeriesSamples {
		total += ts.SampleCount
	}
	for _, obj := range s.ObjectSamples {
		total += obj.SampleCount
	}
	for _, custom := range s.CustomSamples {
		total += custom.SampleCount
	}

	return total
}

// EstimateMemoryUsage provides a rough estimate of memory usage in bytes
func (s *UnifiedModelSampleData) EstimateMemoryUsage() int64 {
	// This is a rough estimation for monitoring purposes
	var size int64

	// Base structure overhead
	size += 1024

	// Table samples
	for _, table := range s.TableSamples {
		size += int64(len(table.Rows)) * 512 // Rough estimate per row
		for _, col := range table.Columns {
			size += int64(len(col.Values)) * 64 // Rough estimate per value
		}
	}

	// Collection samples
	for _, collection := range s.CollectionSamples {
		size += int64(len(collection.Documents)) * 1024 // Documents tend to be larger
	}

	// Other paradigms would be similar calculations...
	// Simplified for brevity

	return size
}
