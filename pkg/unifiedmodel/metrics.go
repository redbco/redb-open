// Package unifiedmodel metrics provides comprehensive metrics and analytics
// for database schemas represented by UnifiedModel.
//
// DESIGN PRINCIPLES:
//
// 1. SEPARATION OF CONCERNS:
//    - Metrics are completely separate from structural schema (UnifiedModel)
//    - Allows version control diffs to focus on structural changes
//    - Enables schema conversion without metric interference
//
// 2. COMPREHENSIVE METRICS:
//    - Object counts across all database paradigms
//    - Size metrics (bytes, rows, documents)
//    - Performance metrics (query frequency, response times)
//    - Growth trends and capacity planning
//
// 3. FLEXIBLE GRANULARITY:
//    - Global metrics (entire schema)
//    - Object-level metrics (per table, collection, etc.)
//    - Time-series data for trend analysis
//
// 4. LINKAGE PATTERN:
//    - SchemaID links metrics to UnifiedModel instance
//    - Independent versioning for metrics algorithms
//    - Metadata tracking (when/how metrics were generated)

package unifiedmodel

import (
	"time"
)

// UnifiedModelMetrics represents comprehensive metrics and analytics for a UnifiedModel schema.
// This structure is designed to be completely separate from the structural schema,
// enabling clean separation between schema definition and metrics/analytics.
type UnifiedModelMetrics struct {
	// Linkage and metadata (follows enrichment pattern)
	SchemaID       string    `json:"schema_id"`             // Links to UnifiedModel instance
	MetricsVersion string    `json:"metrics_version"`       // Version of metrics algorithms used
	GeneratedAt    time.Time `json:"generated_at"`          // When metrics were computed
	GeneratedBy    string    `json:"generated_by"`          // Service/component that generated metrics
	SnapshotID     string    `json:"snapshot_id,omitempty"` // Optional snapshot identifier for time-series

	// Core metrics categories
	ObjectCounts       ObjectCounts       `json:"object_counts"`       // Counts of all object types
	SizeMetrics        SizeMetrics        `json:"size_metrics"`        // Size information (bytes)
	RowMetrics         RowMetrics         `json:"row_metrics"`         // Row/document counts
	PerformanceMetrics PerformanceMetrics `json:"performance_metrics"` // Query and operation metrics
	TrendMetrics       TrendMetrics       `json:"trend_metrics"`       // Growth and trend analysis
	CapacityMetrics    CapacityMetrics    `json:"capacity_metrics"`    // Resource utilization
	QualityMetrics     QualityMetrics     `json:"quality_metrics"`     // Data quality indicators
}

// ObjectCounts provides comprehensive counts of all objects in a UnifiedModel
type ObjectCounts struct {
	// Structural organization
	Catalogs  int `json:"catalogs,omitempty"`
	Databases int `json:"databases,omitempty"`
	Schemas   int `json:"schemas,omitempty"`

	// Primary Data Containers
	Tables       int `json:"tables,omitempty"`
	Collections  int `json:"collections,omitempty"`
	Nodes        int `json:"nodes,omitempty"`
	MemoryTables int `json:"memory_tables,omitempty"`

	// Temporary Data Containers
	TemporaryTables int `json:"temporary_tables,omitempty"`
	TransientTables int `json:"transient_tables,omitempty"`
	Caches          int `json:"caches,omitempty"`

	// Virtual Data Containers
	Views             int `json:"views,omitempty"`
	LiveViews         int `json:"live_views,omitempty"`
	WindowViews       int `json:"window_views,omitempty"`
	MaterializedViews int `json:"materialized_views,omitempty"`
	ExternalTables    int `json:"external_tables,omitempty"`
	ForeignTables     int `json:"foreign_tables,omitempty"`

	// Graph / Vector / Search abstractions
	Graphs        int `json:"graphs,omitempty"`
	VectorIndexes int `json:"vector_indexes,omitempty"`
	SearchIndexes int `json:"search_indexes,omitempty"`

	// Specialized Data Containers
	Vectors           int   `json:"vectors,omitempty"`
	Embeddings        int   `json:"embeddings,omitempty"`
	Documents         int64 `json:"documents,omitempty"` // int64 for large document counts
	EmbeddedDocuments int   `json:"embedded_documents,omitempty"`
	Relationships     int   `json:"relationships,omitempty"`
	Paths             int   `json:"paths,omitempty"`

	// Data Organization Containers
	Partitions    int `json:"partitions,omitempty"`
	SubPartitions int `json:"sub_partitions,omitempty"`
	Shards        int `json:"shards,omitempty"`
	Namespaces    int `json:"namespaces,omitempty"`

	// Structural definition objects
	Columns      int `json:"columns,omitempty"`
	Types        int `json:"types,omitempty"`
	PropertyKeys int `json:"property_keys,omitempty"`

	// Integrity, performance and identity objects
	Indexes        int `json:"indexes,omitempty"`
	Constraints    int `json:"constraints,omitempty"`
	Sequences      int `json:"sequences,omitempty"`
	Identities     int `json:"identities,omitempty"`
	UUIDGenerators int `json:"uuid_generators,omitempty"`

	// Executable code objects
	Functions     int `json:"functions,omitempty"`
	Procedures    int `json:"procedures,omitempty"`
	Methods       int `json:"methods,omitempty"`
	Triggers      int `json:"triggers,omitempty"`
	EventTriggers int `json:"event_triggers,omitempty"`
	Aggregates    int `json:"aggregates,omitempty"`
	Operators     int `json:"operators,omitempty"`
	Modules       int `json:"modules,omitempty"`
	Packages      int `json:"packages,omitempty"`
	PackageBodies int `json:"package_bodies,omitempty"`
	Macros        int `json:"macros,omitempty"`
	Rules         int `json:"rules,omitempty"`
	WindowFuncs   int `json:"window_functions,omitempty"`

	// Security and access control
	Users    int `json:"users,omitempty"`
	Roles    int `json:"roles,omitempty"`
	Grants   int `json:"grants,omitempty"`
	Policies int `json:"policies,omitempty"`

	// Physical storage and placement
	Tablespaces int `json:"tablespaces,omitempty"`
	Segments    int `json:"segments,omitempty"`
	Extents     int `json:"extents,omitempty"`
	Pages       int `json:"pages,omitempty"`
	Filegroups  int `json:"filegroups,omitempty"`
	Datafiles   int `json:"datafiles,omitempty"`

	// Connectivity and integration
	Servers             int `json:"servers,omitempty"`
	Connections         int `json:"connections,omitempty"`
	Endpoints           int `json:"endpoints,omitempty"`
	ForeignDataWrappers int `json:"foreign_data_wrappers,omitempty"`
	UserMappings        int `json:"user_mappings,omitempty"`
	Federations         int `json:"federations,omitempty"`
	Replicas            int `json:"replicas,omitempty"`
	Clusters            int `json:"clusters,omitempty"`

	// Operational, pipelines and streaming
	Tasks     int `json:"tasks,omitempty"`
	Jobs      int `json:"jobs,omitempty"`
	Schedules int `json:"schedules,omitempty"`
	Pipelines int `json:"pipelines,omitempty"`
	Streams   int `json:"streams,omitempty"`

	// Monitoring and alerting
	Events         int `json:"events,omitempty"`
	Notifications  int `json:"notifications,omitempty"`
	Alerts         int `json:"alerts,omitempty"`
	Statistics     int `json:"statistics,omitempty"`
	Histograms     int `json:"histograms,omitempty"`
	Monitors       int `json:"monitors,omitempty"`
	MonitorMetrics int `json:"monitor_metrics,omitempty"`
	Thresholds     int `json:"thresholds,omitempty"`

	// Text processing / search configuration
	TextSearchComponents int `json:"text_search_components,omitempty"`

	// Metadata and documentation
	Comments    int `json:"comments,omitempty"`
	Annotations int `json:"annotations,omitempty"`
	Tags        int `json:"tags,omitempty"`
	Aliases     int `json:"aliases,omitempty"`
	Synonyms    int `json:"synonyms,omitempty"`
	Labels      int `json:"labels,omitempty"`

	// Backup and recovery, versioning
	Snapshots      int `json:"snapshots,omitempty"`
	Backups        int `json:"backups,omitempty"`
	Archives       int `json:"archives,omitempty"`
	RecoveryPoints int `json:"recovery_points,omitempty"`
	Versions       int `json:"versions,omitempty"`
	Migrations     int `json:"migrations,omitempty"`
	Branches       int `json:"branches,omitempty"`
	TimeTravel     int `json:"time_travel,omitempty"`

	// Extensions and customization
	Extensions       int `json:"extensions,omitempty"`
	Plugins          int `json:"plugins,omitempty"`
	ModuleExtensions int `json:"module_extensions,omitempty"`
	TTLSettings      int `json:"ttl_settings,omitempty"`
	Dimensions       int `json:"dimensions,omitempty"`
	DistanceMetrics  int `json:"distance_metrics,omitempty"`

	// Advanced analytics
	Projections     int `json:"projections,omitempty"`
	AnalyticsAggs   int `json:"analytics_aggregations,omitempty"`
	Transformations int `json:"transformations,omitempty"`
	Enrichments     int `json:"enrichments,omitempty"`
	BufferPools     int `json:"buffer_pools,omitempty"`

	// Replication & distribution
	Publications     int `json:"publications,omitempty"`
	Subscriptions    int `json:"subscriptions,omitempty"`
	ReplicationSlots int `json:"replication_slots,omitempty"`
	FailoverGroups   int `json:"failover_groups,omitempty"`
}

// SizeMetrics provides size information for database objects
type SizeMetrics struct {
	// Global size metrics
	TotalSizeBytes      int64 `json:"total_size_bytes"`
	DataSizeBytes       int64 `json:"data_size_bytes"`
	IndexSizeBytes      int64 `json:"index_size_bytes"`
	TempSizeBytes       int64 `json:"temp_size_bytes,omitempty"`
	LogSizeBytes        int64 `json:"log_size_bytes,omitempty"`
	CompressedSizeBytes int64 `json:"compressed_size_bytes,omitempty"`

	// Object-level size metrics
	TableSizes      map[string]TableSize      `json:"table_sizes,omitempty"`      // Key: table name
	IndexSizes      map[string]IndexSize      `json:"index_sizes,omitempty"`      // Key: index name
	CollectionSizes map[string]CollectionSize `json:"collection_sizes,omitempty"` // Key: collection name
	ViewSizes       map[string]ViewSize       `json:"view_sizes,omitempty"`       // Key: view name (materialized)
	GraphSizes      map[string]GraphSize      `json:"graph_sizes,omitempty"`      // Key: graph name
	VectorSizes     map[string]VectorSize     `json:"vector_sizes,omitempty"`     // Key: vector index name

	// Storage breakdown
	StorageByType    map[string]int64 `json:"storage_by_type,omitempty"`   // storage type -> bytes
	CompressionRatio *float64         `json:"compression_ratio,omitempty"` // compressed/uncompressed ratio
}

// RowMetrics provides row/document count information
type RowMetrics struct {
	// Global row metrics
	TotalRows      int64 `json:"total_rows"`
	TotalDocuments int64 `json:"total_documents"`
	TotalNodes     int64 `json:"total_nodes,omitempty"`
	TotalRelations int64 `json:"total_relations,omitempty"`
	TotalVectors   int64 `json:"total_vectors,omitempty"`

	// Object-level row metrics
	TableRows          map[string]TableRows          `json:"table_rows,omitempty"`          // Key: table name
	CollectionDocs     map[string]CollectionDocs     `json:"collection_docs,omitempty"`     // Key: collection name
	NodeCounts         map[string]NodeCounts         `json:"node_counts,omitempty"`         // Key: node label
	RelationshipCounts map[string]RelationshipCounts `json:"relationship_counts,omitempty"` // Key: relationship type
	VectorCounts       map[string]VectorCounts       `json:"vector_counts,omitempty"`       // Key: vector index name

	// Growth metrics
	DailyGrowthRate   *float64 `json:"daily_growth_rate,omitempty"`   // Rows per day
	WeeklyGrowthRate  *float64 `json:"weekly_growth_rate,omitempty"`  // Rows per week
	MonthlyGrowthRate *float64 `json:"monthly_growth_rate,omitempty"` // Rows per month
}

// PerformanceMetrics provides performance-related metrics
type PerformanceMetrics struct {
	// Query performance
	AvgQueryTime     *float64 `json:"avg_query_time_ms,omitempty"`    // Average query time in milliseconds
	MedianQueryTime  *float64 `json:"median_query_time_ms,omitempty"` // Median query time in milliseconds
	P95QueryTime     *float64 `json:"p95_query_time_ms,omitempty"`    // 95th percentile query time
	P99QueryTime     *float64 `json:"p99_query_time_ms,omitempty"`    // 99th percentile query time
	QueriesPerSecond *float64 `json:"queries_per_second,omitempty"`   // QPS
	SlowQueryCount   int64    `json:"slow_query_count,omitempty"`     // Number of slow queries
	FailedQueryCount int64    `json:"failed_query_count,omitempty"`   // Number of failed queries

	// Object-level performance
	TablePerformance      map[string]TablePerformance      `json:"table_performance,omitempty"`      // Key: table name
	IndexPerformance      map[string]IndexPerformance      `json:"index_performance,omitempty"`      // Key: index name
	CollectionPerformance map[string]CollectionPerformance `json:"collection_performance,omitempty"` // Key: collection name

	// Resource utilization
	CPUUtilization     *float64 `json:"cpu_utilization_percent,omitempty"`    // 0-100
	MemoryUtilization  *float64 `json:"memory_utilization_percent,omitempty"` // 0-100
	IOUtilization      *float64 `json:"io_utilization_percent,omitempty"`     // 0-100
	NetworkUtilization *float64 `json:"network_utilization_mbps,omitempty"`   // Mbps

	// Connection metrics
	ActiveConnections  int `json:"active_connections,omitempty"`
	MaxConnections     int `json:"max_connections,omitempty"`
	ConnectionPoolSize int `json:"connection_pool_size,omitempty"`
}

// TrendMetrics provides historical trend analysis
type TrendMetrics struct {
	// Data growth trends
	DataGrowthTrend   GrowthTrend `json:"data_growth_trend"`
	QueryVolumeTrend  GrowthTrend `json:"query_volume_trend"`
	PerformanceTrend  GrowthTrend `json:"performance_trend"`
	StorageUsageTrend GrowthTrend `json:"storage_usage_trend"`

	// Capacity planning
	EstimatedFullCapacity *time.Time `json:"estimated_full_capacity,omitempty"`  // When storage will be full
	ProjectedGrowth6Month *int64     `json:"projected_growth_6_month,omitempty"` // Projected size increase
	ProjectedGrowth1Year  *int64     `json:"projected_growth_1_year,omitempty"`  // Projected size increase

	// Object evolution
	ObjectCreationRate map[string]float64 `json:"object_creation_rate,omitempty"` // objects created per day by type
	ObjectDeletionRate map[string]float64 `json:"object_deletion_rate,omitempty"` // objects deleted per day by type
}

// CapacityMetrics provides resource capacity and utilization information
type CapacityMetrics struct {
	// Storage capacity
	TotalStorageCapacity int64   `json:"total_storage_capacity_bytes"`
	UsedStorageCapacity  int64   `json:"used_storage_capacity_bytes"`
	StorageUtilization   float64 `json:"storage_utilization_percent"` // 0-100

	// Memory capacity
	TotalMemoryCapacity int64   `json:"total_memory_capacity_bytes,omitempty"`
	UsedMemoryCapacity  int64   `json:"used_memory_capacity_bytes,omitempty"`
	MemoryUtilization   float64 `json:"memory_utilization_percent,omitempty"` // 0-100

	// Compute capacity
	TotalCPUCores  *float64 `json:"total_cpu_cores,omitempty"`
	UsedCPUCores   *float64 `json:"used_cpu_cores,omitempty"`
	CPUUtilization *float64 `json:"cpu_utilization_percent,omitempty"` // 0-100

	// Connection capacity
	MaxConnections        int     `json:"max_connections,omitempty"`
	CurrentConnections    int     `json:"current_connections,omitempty"`
	ConnectionUtilization float64 `json:"connection_utilization_percent,omitempty"` // 0-100

	// Object capacity limits
	ObjectLimits map[string]ObjectLimit `json:"object_limits,omitempty"` // limits by object type
}

// QualityMetrics provides data quality indicators
type QualityMetrics struct {
	// Overall quality scores
	OverallQualityScore float64 `json:"overall_quality_score"` // 0.0-1.0
	CompletenessScore   float64 `json:"completeness_score"`    // 0.0-1.0
	ConsistencyScore    float64 `json:"consistency_score"`     // 0.0-1.0
	AccuracyScore       float64 `json:"accuracy_score"`        // 0.0-1.0
	ValidityScore       float64 `json:"validity_score"`        // 0.0-1.0

	// Quality issues
	DuplicateRecords      int64 `json:"duplicate_records,omitempty"`
	InconsistentRecords   int64 `json:"inconsistent_records,omitempty"`
	MissingRequiredFields int64 `json:"missing_required_fields,omitempty"`
	InvalidDataTypes      int64 `json:"invalid_data_types,omitempty"`
	ConstraintViolations  int64 `json:"constraint_violations,omitempty"`

	// Object-level quality
	TableQuality      map[string]TableQuality      `json:"table_quality,omitempty"`      // Key: table name
	CollectionQuality map[string]CollectionQuality `json:"collection_quality,omitempty"` // Key: collection name
	ColumnQuality     map[string]ColumnQuality     `json:"column_quality,omitempty"`     // Key: "table.column"
}

// Object-specific metric types

type TableSize struct {
	DataSizeBytes  int64 `json:"data_size_bytes"`
	IndexSizeBytes int64 `json:"index_size_bytes"`
	TotalSizeBytes int64 `json:"total_size_bytes"`
}

type IndexSize struct {
	SizeBytes  int64    `json:"size_bytes"`
	Entries    int64    `json:"entries,omitempty"`
	Depth      int      `json:"depth,omitempty"`
	FillFactor *float64 `json:"fill_factor,omitempty"` // 0.0-1.0
}

type CollectionSize struct {
	DataSizeBytes   int64 `json:"data_size_bytes"`
	IndexSizeBytes  int64 `json:"index_size_bytes"`
	TotalSizeBytes  int64 `json:"total_size_bytes"`
	AvgDocumentSize int64 `json:"avg_document_size_bytes,omitempty"`
}

type ViewSize struct {
	SizeBytes        int64 `json:"size_bytes,omitempty"`         // For materialized views
	RefreshSizeBytes int64 `json:"refresh_size_bytes,omitempty"` // Temporary space during refresh
}

type GraphSize struct {
	NodeSizeBytes         int64 `json:"node_size_bytes"`
	RelationshipSizeBytes int64 `json:"relationship_size_bytes"`
	IndexSizeBytes        int64 `json:"index_size_bytes"`
	TotalSizeBytes        int64 `json:"total_size_bytes"`
}

type VectorSize struct {
	VectorSizeBytes int64 `json:"vector_size_bytes"`
	IndexSizeBytes  int64 `json:"index_size_bytes"`
	TotalSizeBytes  int64 `json:"total_size_bytes"`
	Dimensions      int   `json:"dimensions,omitempty"`
}

type TableRows struct {
	RowCount      int64      `json:"row_count"`
	EstimatedRows *int64     `json:"estimated_rows,omitempty"`
	AvgRowSize    *int64     `json:"avg_row_size_bytes,omitempty"`
	GrowthRate    *float64   `json:"growth_rate_rows_per_day,omitempty"`
	LastUpdated   *time.Time `json:"last_updated,omitempty"`
}

type CollectionDocs struct {
	DocumentCount      int64      `json:"document_count"`
	EstimatedDocuments *int64     `json:"estimated_documents,omitempty"`
	AvgDocumentSize    *int64     `json:"avg_document_size_bytes,omitempty"`
	GrowthRate         *float64   `json:"growth_rate_docs_per_day,omitempty"`
	LastUpdated        *time.Time `json:"last_updated,omitempty"`
}

type NodeCounts struct {
	NodeCount   int64      `json:"node_count"`
	GrowthRate  *float64   `json:"growth_rate_nodes_per_day,omitempty"`
	LastUpdated *time.Time `json:"last_updated,omitempty"`
}

type RelationshipCounts struct {
	RelationshipCount int64      `json:"relationship_count"`
	GrowthRate        *float64   `json:"growth_rate_relations_per_day,omitempty"`
	LastUpdated       *time.Time `json:"last_updated,omitempty"`
}

type VectorCounts struct {
	VectorCount int64      `json:"vector_count"`
	GrowthRate  *float64   `json:"growth_rate_vectors_per_day,omitempty"`
	LastUpdated *time.Time `json:"last_updated,omitempty"`
}

type TablePerformance struct {
	QueriesPerSecond *float64   `json:"queries_per_second,omitempty"`
	AvgQueryTime     *float64   `json:"avg_query_time_ms,omitempty"`
	ScanRatio        *float64   `json:"scan_ratio,omitempty"`      // Full table scans / total queries
	CacheHitRatio    *float64   `json:"cache_hit_ratio,omitempty"` // 0.0-1.0
	LastAnalyzed     *time.Time `json:"last_analyzed,omitempty"`
}

type IndexPerformance struct {
	UsageCount   int64      `json:"usage_count,omitempty"`
	HitRatio     *float64   `json:"hit_ratio,omitempty"`   // 0.0-1.0
	Selectivity  *float64   `json:"selectivity,omitempty"` // 0.0-1.0
	LastUsed     *time.Time `json:"last_used,omitempty"`
	LastAnalyzed *time.Time `json:"last_analyzed,omitempty"`
}

type CollectionPerformance struct {
	QueriesPerSecond *float64   `json:"queries_per_second,omitempty"`
	AvgQueryTime     *float64   `json:"avg_query_time_ms,omitempty"`
	IndexHitRatio    *float64   `json:"index_hit_ratio,omitempty"` // 0.0-1.0
	LastAnalyzed     *time.Time `json:"last_analyzed,omitempty"`
}

type GrowthTrend struct {
	Direction  string   `json:"direction"`            // "increasing", "decreasing", "stable", "volatile"
	Rate       *float64 `json:"rate,omitempty"`       // Rate of change per unit time
	Confidence *float64 `json:"confidence,omitempty"` // 0.0-1.0, confidence in prediction
	R2Score    *float64 `json:"r2_score,omitempty"`   // R² coefficient of determination
}

type ObjectLimit struct {
	MaxCount     *int    `json:"max_count,omitempty"`
	CurrentCount int     `json:"current_count"`
	Utilization  float64 `json:"utilization_percent"` // 0-100
}

type TableQuality struct {
	QualityScore       float64    `json:"quality_score"`      // 0.0-1.0
	CompletenessScore  float64    `json:"completeness_score"` // 0.0-1.0
	ConsistencyScore   float64    `json:"consistency_score"`  // 0.0-1.0
	DuplicateCount     int64      `json:"duplicate_count,omitempty"`
	MissingDataPercent float64    `json:"missing_data_percent"` // 0-100
	LastQualityCheck   *time.Time `json:"last_quality_check,omitempty"`
}

type CollectionQuality struct {
	QualityScore       float64    `json:"quality_score"`       // 0.0-1.0
	SchemaCompliance   float64    `json:"schema_compliance"`   // 0.0-1.0
	DocumentValidation float64    `json:"document_validation"` // 0.0-1.0
	InconsistentDocs   int64      `json:"inconsistent_docs,omitempty"`
	LastQualityCheck   *time.Time `json:"last_quality_check,omitempty"`
}

type ColumnQuality struct {
	QualityScore       float64    `json:"quality_score"`        // 0.0-1.0
	NullPercent        float64    `json:"null_percent"`         // 0-100
	UniquePercent      float64    `json:"unique_percent"`       // 0-100
	ValidFormatPercent float64    `json:"valid_format_percent"` // 0-100
	OutlierCount       int64      `json:"outlier_count,omitempty"`
	LastQualityCheck   *time.Time `json:"last_quality_check,omitempty"`
}
