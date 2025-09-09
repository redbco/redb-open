// Package unifiedmodel metrics utilities provides functions for generating,
// analyzing, and managing UnifiedModelMetrics.

package unifiedmodel

import (
	"fmt"
	"time"
)

// NewUnifiedModelMetrics creates a new, initialized UnifiedModelMetrics instance.
func NewUnifiedModelMetrics(schemaID string) *UnifiedModelMetrics {
	return &UnifiedModelMetrics{
		SchemaID:           schemaID,
		MetricsVersion:     "v1.0.0", // Default version
		GeneratedAt:        time.Now(),
		GeneratedBy:        "unifiedmodel",
		ObjectCounts:       ObjectCounts{},
		SizeMetrics:        SizeMetrics{},
		RowMetrics:         RowMetrics{},
		PerformanceMetrics: PerformanceMetrics{},
		TrendMetrics:       TrendMetrics{},
		CapacityMetrics:    CapacityMetrics{},
		QualityMetrics:     QualityMetrics{},
	}
}

// GenerateBasicMetrics generates basic metrics (counts and simple calculations) from a UnifiedModel.
// This provides the foundation metrics that can be computed directly from the schema structure.
func GenerateBasicMetrics(model *UnifiedModel, schemaID string) *UnifiedModelMetrics {
	if model == nil {
		return NewUnifiedModelMetrics(schemaID)
	}

	metrics := NewUnifiedModelMetrics(schemaID)

	// Generate object counts
	metrics.ObjectCounts = CountObjects(model)

	// Initialize empty maps for object-level metrics
	metrics.SizeMetrics.TableSizes = make(map[string]TableSize)
	metrics.SizeMetrics.IndexSizes = make(map[string]IndexSize)
	metrics.SizeMetrics.CollectionSizes = make(map[string]CollectionSize)
	metrics.SizeMetrics.ViewSizes = make(map[string]ViewSize)
	metrics.SizeMetrics.GraphSizes = make(map[string]GraphSize)
	metrics.SizeMetrics.VectorSizes = make(map[string]VectorSize)

	metrics.RowMetrics.TableRows = make(map[string]TableRows)
	metrics.RowMetrics.CollectionDocs = make(map[string]CollectionDocs)
	metrics.RowMetrics.NodeCounts = make(map[string]NodeCounts)
	metrics.RowMetrics.RelationshipCounts = make(map[string]RelationshipCounts)
	metrics.RowMetrics.VectorCounts = make(map[string]VectorCounts)

	metrics.PerformanceMetrics.TablePerformance = make(map[string]TablePerformance)
	metrics.PerformanceMetrics.IndexPerformance = make(map[string]IndexPerformance)
	metrics.PerformanceMetrics.CollectionPerformance = make(map[string]CollectionPerformance)

	metrics.QualityMetrics.TableQuality = make(map[string]TableQuality)
	metrics.QualityMetrics.CollectionQuality = make(map[string]CollectionQuality)
	metrics.QualityMetrics.ColumnQuality = make(map[string]ColumnQuality)

	// Initialize object limits map
	metrics.CapacityMetrics.ObjectLimits = make(map[string]ObjectLimit)

	return metrics
}

// CountObjects counts all objects in a UnifiedModel and returns the counts
func CountObjects(model *UnifiedModel) ObjectCounts {
	if model == nil {
		return ObjectCounts{}
	}

	return ObjectCounts{
		// Structural organization
		Catalogs:  len(model.Catalogs),
		Databases: len(model.Databases),
		Schemas:   len(model.Schemas),

		// Primary Data Containers
		Tables:       len(model.Tables),
		Collections:  len(model.Collections),
		Nodes:        len(model.Nodes),
		MemoryTables: len(model.MemoryTables),

		// Temporary Data Containers
		TemporaryTables: len(model.TemporaryTables),
		TransientTables: len(model.TransientTables),
		Caches:          len(model.Caches),

		// Virtual Data Containers
		Views:             len(model.Views),
		LiveViews:         len(model.LiveViews),
		WindowViews:       len(model.WindowViews),
		MaterializedViews: len(model.MaterializedViews),
		ExternalTables:    len(model.ExternalTables),
		ForeignTables:     len(model.ForeignTables),

		// Graph / Vector / Search abstractions
		Graphs:        len(model.Graphs),
		VectorIndexes: len(model.VectorIndexes),
		SearchIndexes: len(model.SearchIndexes),

		// Specialized Data Containers
		Vectors:           len(model.Vectors),
		Embeddings:        len(model.Embeddings),
		Documents:         int64(len(model.Documents)),
		EmbeddedDocuments: len(model.EmbeddedDocuments),
		Relationships:     len(model.Relationships),
		Paths:             len(model.Paths),

		// Data Organization Containers
		Partitions:    len(model.Partitions),
		SubPartitions: len(model.SubPartitions),
		Shards:        len(model.Shards),
		Namespaces:    len(model.Namespaces),

		// Structural definition objects
		Columns:      len(model.Columns),
		Types:        len(model.Types),
		PropertyKeys: len(model.PropertyKeys),

		// Integrity, performance and identity objects
		Indexes:        len(model.Indexes),
		Constraints:    len(model.Constraints),
		Sequences:      len(model.Sequences),
		Identities:     len(model.Identities),
		UUIDGenerators: len(model.UUIDGenerators),

		// Executable code objects
		Functions:     len(model.Functions),
		Procedures:    len(model.Procedures),
		Methods:       len(model.Methods),
		Triggers:      len(model.Triggers),
		EventTriggers: len(model.EventTriggers),
		Aggregates:    len(model.Aggregates),
		Operators:     len(model.Operators),
		Modules:       len(model.Modules),
		Packages:      len(model.Packages),
		PackageBodies: len(model.PackageBodies),
		Macros:        len(model.Macros),
		Rules:         len(model.Rules),
		WindowFuncs:   len(model.WindowFuncs),

		// Security and access control
		Users:    len(model.Users),
		Roles:    len(model.Roles),
		Grants:   len(model.Grants),
		Policies: len(model.Policies),

		// Physical storage and placement
		Tablespaces: len(model.Tablespaces),
		Segments:    len(model.Segments),
		Extents:     len(model.Extents),
		Pages:       len(model.Pages),
		Filegroups:  len(model.Filegroups),
		Datafiles:   len(model.Datafiles),

		// Connectivity and integration
		Servers:             len(model.Servers),
		Connections:         len(model.Connections),
		Endpoints:           len(model.Endpoints),
		ForeignDataWrappers: len(model.ForeignDataWrappers),
		UserMappings:        len(model.UserMappings),
		Federations:         len(model.Federations),
		Replicas:            len(model.Replicas),
		Clusters:            len(model.Clusters),

		// Operational, pipelines and streaming
		Tasks:     len(model.Tasks),
		Jobs:      len(model.Jobs),
		Schedules: len(model.Schedules),
		Pipelines: len(model.Pipelines),
		Streams:   len(model.Streams),

		// Monitoring and alerting
		Events:         len(model.Events),
		Notifications:  len(model.Notifications),
		Alerts:         len(model.Alerts),
		Statistics:     len(model.Statistics),
		Histograms:     len(model.Histograms),
		Monitors:       len(model.Monitors),
		MonitorMetrics: len(model.MonitorMetrics),
		Thresholds:     len(model.Thresholds),

		// Text processing / search configuration
		TextSearchComponents: len(model.TextSearchComponents),

		// Metadata and documentation
		Comments:    len(model.Comments),
		Annotations: len(model.Annotations),
		Tags:        len(model.Tags),
		Aliases:     len(model.Aliases),
		Synonyms:    len(model.Synonyms),
		Labels:      len(model.Labels),

		// Backup and recovery, versioning
		Snapshots:      len(model.Snapshots),
		Backups:        len(model.Backups),
		Archives:       len(model.Archives),
		RecoveryPoints: len(model.RecoveryPoints),
		Versions:       len(model.Versions),
		Migrations:     len(model.Migrations),
		Branches:       len(model.Branches),
		TimeTravel:     len(model.TimeTravel),

		// Extensions and customization
		Extensions:       len(model.Extensions),
		Plugins:          len(model.Plugins),
		ModuleExtensions: len(model.ModuleExtensions),
		TTLSettings:      len(model.TTLSettings),
		Dimensions:       len(model.Dimensions),
		DistanceMetrics:  len(model.DistanceMetrics),

		// Advanced analytics
		Projections:     len(model.Projections),
		AnalyticsAggs:   len(model.AnalyticsAggs),
		Transformations: len(model.Transformations),
		Enrichments:     len(model.Enrichments),
		BufferPools:     len(model.BufferPools),

		// Replication & distribution
		Publications:     len(model.Publications),
		Subscriptions:    len(model.Subscriptions),
		ReplicationSlots: len(model.ReplicationSlots),
		FailoverGroups:   len(model.FailoverGroups),
	}
}

// GetTotalObjectCount returns the total number of objects across all categories
func (oc *ObjectCounts) GetTotalObjectCount() int64 {
	total := int64(0)

	// Add all int fields
	total += int64(oc.Catalogs + oc.Databases + oc.Schemas)
	total += int64(oc.Tables + oc.Collections + oc.Nodes + oc.MemoryTables)
	total += int64(oc.TemporaryTables + oc.TransientTables + oc.Caches)
	total += int64(oc.Views + oc.LiveViews + oc.WindowViews + oc.MaterializedViews + oc.ExternalTables + oc.ForeignTables)
	total += int64(oc.Graphs + oc.VectorIndexes + oc.SearchIndexes)
	total += int64(oc.Vectors + oc.Embeddings + oc.EmbeddedDocuments + oc.Relationships + oc.Paths)
	total += int64(oc.Partitions + oc.SubPartitions + oc.Shards + oc.Namespaces)
	total += int64(oc.Columns + oc.Types + oc.PropertyKeys)
	total += int64(oc.Indexes + oc.Constraints + oc.Sequences + oc.Identities + oc.UUIDGenerators)
	total += int64(oc.Functions + oc.Procedures + oc.Methods + oc.Triggers + oc.EventTriggers)
	total += int64(oc.Aggregates + oc.Operators + oc.Modules + oc.Packages + oc.PackageBodies)
	total += int64(oc.Macros + oc.Rules + oc.WindowFuncs)
	total += int64(oc.Users + oc.Roles + oc.Grants + oc.Policies)
	total += int64(oc.Tablespaces + oc.Segments + oc.Extents + oc.Pages + oc.Filegroups + oc.Datafiles)
	total += int64(oc.Servers + oc.Connections + oc.Endpoints + oc.ForeignDataWrappers + oc.UserMappings)
	total += int64(oc.Federations + oc.Replicas + oc.Clusters)
	total += int64(oc.Tasks + oc.Jobs + oc.Schedules + oc.Pipelines + oc.Streams)
	total += int64(oc.Events + oc.Notifications + oc.Alerts + oc.Statistics + oc.Histograms)
	total += int64(oc.Monitors + oc.MonitorMetrics + oc.Thresholds)
	total += int64(oc.TextSearchComponents)
	total += int64(oc.Comments + oc.Annotations + oc.Tags + oc.Aliases + oc.Synonyms + oc.Labels)
	total += int64(oc.Snapshots + oc.Backups + oc.Archives + oc.RecoveryPoints + oc.Versions)
	total += int64(oc.Migrations + oc.Branches + oc.TimeTravel)
	total += int64(oc.Extensions + oc.Plugins + oc.ModuleExtensions + oc.TTLSettings)
	total += int64(oc.Dimensions + oc.DistanceMetrics)
	total += int64(oc.Projections + oc.AnalyticsAggs + oc.Transformations + oc.Enrichments + oc.BufferPools)
	total += int64(oc.Publications + oc.Subscriptions + oc.ReplicationSlots + oc.FailoverGroups)

	// Add Documents separately as it's already int64
	total += oc.Documents

	return total
}

// GetDataContainerCount returns the count of primary data containers
func (oc *ObjectCounts) GetDataContainerCount() int {
	return oc.Tables + oc.Collections + oc.Nodes + oc.MemoryTables +
		oc.TemporaryTables + oc.TransientTables +
		oc.Views + oc.MaterializedViews + oc.ExternalTables + oc.ForeignTables
}

// GetExecutableObjectCount returns the count of executable code objects
func (oc *ObjectCounts) GetExecutableObjectCount() int {
	return oc.Functions + oc.Procedures + oc.Methods + oc.Triggers + oc.EventTriggers +
		oc.Aggregates + oc.Operators + oc.Modules + oc.Packages + oc.PackageBodies +
		oc.Macros + oc.Rules + oc.WindowFuncs
}

// GetSecurityObjectCount returns the count of security-related objects
func (oc *ObjectCounts) GetSecurityObjectCount() int {
	return oc.Users + oc.Roles + oc.Grants + oc.Policies
}

// FilterByObjectTypes returns counts filtered to only include specified object types
func (oc *ObjectCounts) FilterByObjectTypes(objectTypes []ObjectType) ObjectCounts {
	filtered := ObjectCounts{}

	typeSet := make(map[ObjectType]bool)
	for _, objType := range objectTypes {
		typeSet[objType] = true
	}

	// Only include counts for specified object types
	if typeSet[ObjectTypeTable] {
		filtered.Tables = oc.Tables
		filtered.TemporaryTables = oc.TemporaryTables
		filtered.TransientTables = oc.TransientTables
		filtered.MemoryTables = oc.MemoryTables
		filtered.ExternalTables = oc.ExternalTables
		filtered.ForeignTables = oc.ForeignTables
	}

	if typeSet[ObjectTypeCollection] {
		filtered.Collections = oc.Collections
	}

	if typeSet[ObjectTypeView] {
		filtered.Views = oc.Views
		filtered.LiveViews = oc.LiveViews
		filtered.WindowViews = oc.WindowViews
	}

	if typeSet[ObjectTypeMaterializedView] {
		filtered.MaterializedViews = oc.MaterializedViews
	}

	if typeSet[ObjectTypeNode] {
		filtered.Nodes = oc.Nodes
	}

	if typeSet[ObjectTypeRelationship] {
		filtered.Relationships = oc.Relationships
	}

	if typeSet[ObjectTypeGraph] {
		filtered.Graphs = oc.Graphs
	}

	if typeSet[ObjectTypeVector] {
		filtered.Vectors = oc.Vectors
	}

	if typeSet[ObjectTypeVectorIndex] {
		filtered.VectorIndexes = oc.VectorIndexes
	}

	if typeSet[ObjectTypeEmbedding] {
		filtered.Embeddings = oc.Embeddings
	}

	if typeSet[ObjectTypeSearchIndex] {
		filtered.SearchIndexes = oc.SearchIndexes
	}

	if typeSet[ObjectTypeDocument] {
		filtered.Documents = oc.Documents
		filtered.EmbeddedDocuments = oc.EmbeddedDocuments
	}

	return filtered
}

// AddTableSize adds size information for a specific table
func (m *UnifiedModelMetrics) AddTableSize(tableName string, dataSize, indexSize int64) {
	if m.SizeMetrics.TableSizes == nil {
		m.SizeMetrics.TableSizes = make(map[string]TableSize)
	}

	m.SizeMetrics.TableSizes[tableName] = TableSize{
		DataSizeBytes:  dataSize,
		IndexSizeBytes: indexSize,
		TotalSizeBytes: dataSize + indexSize,
	}

	// Update global size metrics
	m.SizeMetrics.DataSizeBytes += dataSize
	m.SizeMetrics.IndexSizeBytes += indexSize
	m.SizeMetrics.TotalSizeBytes += dataSize + indexSize
}

// AddTableRows adds row count information for a specific table
func (m *UnifiedModelMetrics) AddTableRows(tableName string, rowCount int64) {
	if m.RowMetrics.TableRows == nil {
		m.RowMetrics.TableRows = make(map[string]TableRows)
	}

	now := time.Now()
	m.RowMetrics.TableRows[tableName] = TableRows{
		RowCount:    rowCount,
		LastUpdated: &now,
	}

	// Update global row metrics
	m.RowMetrics.TotalRows += rowCount
}

// AddCollectionDocs adds document count information for a specific collection
func (m *UnifiedModelMetrics) AddCollectionDocs(collectionName string, docCount int64) {
	if m.RowMetrics.CollectionDocs == nil {
		m.RowMetrics.CollectionDocs = make(map[string]CollectionDocs)
	}

	now := time.Now()
	m.RowMetrics.CollectionDocs[collectionName] = CollectionDocs{
		DocumentCount: docCount,
		LastUpdated:   &now,
	}

	// Update global document metrics
	m.RowMetrics.TotalDocuments += docCount
}

// GetMetricsSummary provides a high-level overview of the metrics data
func (m *UnifiedModelMetrics) GetMetricsSummary() MetricsSummary {
	return MetricsSummary{
		SchemaID:            m.SchemaID,
		GeneratedAt:         m.GeneratedAt,
		TotalObjects:        m.ObjectCounts.GetTotalObjectCount(),
		TotalDataContainers: m.ObjectCounts.GetDataContainerCount(),
		TotalSizeBytes:      m.SizeMetrics.TotalSizeBytes,
		TotalRows:           m.RowMetrics.TotalRows,
		TotalDocuments:      m.RowMetrics.TotalDocuments,
		OverallQualityScore: m.QualityMetrics.OverallQualityScore,
		HasPerformanceData:  m.PerformanceMetrics.AvgQueryTime != nil,
		HasTrendData:        len(m.TrendMetrics.ObjectCreationRate) > 0,
	}
}

// MergeMetrics merges another UnifiedModelMetrics into the current one
// This is useful for combining metrics from different sources or time periods
func (m *UnifiedModelMetrics) MergeMetrics(other *UnifiedModelMetrics) error {
	if other == nil {
		return fmt.Errorf("cannot merge nil metrics")
	}

	if m.SchemaID != other.SchemaID {
		return fmt.Errorf("cannot merge metrics from different schemas: %s != %s", m.SchemaID, other.SchemaID)
	}

	// Update metadata to reflect the merge
	m.GeneratedAt = time.Now()
	m.GeneratedBy = "merged"

	// Merge object counts (take maximum values)
	m.ObjectCounts = mergeObjectCounts(m.ObjectCounts, other.ObjectCounts)

	// Merge size metrics
	m.SizeMetrics = mergeSizeMetrics(m.SizeMetrics, other.SizeMetrics)

	// Merge row metrics
	m.RowMetrics = mergeRowMetrics(m.RowMetrics, other.RowMetrics)

	return nil
}

// ValidateMetrics performs basic validation on the metrics data
func (m *UnifiedModelMetrics) ValidateMetrics() []string {
	var issues []string

	if m.SchemaID == "" {
		issues = append(issues, "SchemaID cannot be empty")
	}

	if m.GeneratedAt.IsZero() {
		issues = append(issues, "GeneratedAt must be set")
	}

	// Validate size consistency
	calculatedTotal := m.SizeMetrics.DataSizeBytes + m.SizeMetrics.IndexSizeBytes
	if m.SizeMetrics.TotalSizeBytes != 0 && calculatedTotal != 0 &&
		abs(m.SizeMetrics.TotalSizeBytes-calculatedTotal) > calculatedTotal/10 { // Allow 10% variance
		issues = append(issues, "Total size inconsistent with data + index sizes")
	}

	// Validate row count consistency
	if m.RowMetrics.TotalRows < 0 {
		issues = append(issues, "Total rows cannot be negative")
	}

	if m.RowMetrics.TotalDocuments < 0 {
		issues = append(issues, "Total documents cannot be negative")
	}

	// Validate quality scores (should be 0.0-1.0)
	if m.QualityMetrics.OverallQualityScore < 0 || m.QualityMetrics.OverallQualityScore > 1 {
		issues = append(issues, "Overall quality score must be between 0.0 and 1.0")
	}

	return issues
}

// MetricsSummary provides a high-level overview of metrics
type MetricsSummary struct {
	SchemaID            string    `json:"schema_id"`
	GeneratedAt         time.Time `json:"generated_at"`
	TotalObjects        int64     `json:"total_objects"`
	TotalDataContainers int       `json:"total_data_containers"`
	TotalSizeBytes      int64     `json:"total_size_bytes"`
	TotalRows           int64     `json:"total_rows"`
	TotalDocuments      int64     `json:"total_documents"`
	OverallQualityScore float64   `json:"overall_quality_score"`
	HasPerformanceData  bool      `json:"has_performance_data"`
	HasTrendData        bool      `json:"has_trend_data"`
}

// Helper functions for merging metrics

func mergeObjectCounts(a, b ObjectCounts) ObjectCounts {
	return ObjectCounts{
		Catalogs:             max(a.Catalogs, b.Catalogs),
		Databases:            max(a.Databases, b.Databases),
		Schemas:              max(a.Schemas, b.Schemas),
		Tables:               max(a.Tables, b.Tables),
		Collections:          max(a.Collections, b.Collections),
		Nodes:                max(a.Nodes, b.Nodes),
		MemoryTables:         max(a.MemoryTables, b.MemoryTables),
		TemporaryTables:      max(a.TemporaryTables, b.TemporaryTables),
		TransientTables:      max(a.TransientTables, b.TransientTables),
		Caches:               max(a.Caches, b.Caches),
		Views:                max(a.Views, b.Views),
		LiveViews:            max(a.LiveViews, b.LiveViews),
		WindowViews:          max(a.WindowViews, b.WindowViews),
		MaterializedViews:    max(a.MaterializedViews, b.MaterializedViews),
		ExternalTables:       max(a.ExternalTables, b.ExternalTables),
		ForeignTables:        max(a.ForeignTables, b.ForeignTables),
		Graphs:               max(a.Graphs, b.Graphs),
		VectorIndexes:        max(a.VectorIndexes, b.VectorIndexes),
		SearchIndexes:        max(a.SearchIndexes, b.SearchIndexes),
		Vectors:              max(a.Vectors, b.Vectors),
		Embeddings:           max(a.Embeddings, b.Embeddings),
		Documents:            max64(a.Documents, b.Documents),
		EmbeddedDocuments:    max(a.EmbeddedDocuments, b.EmbeddedDocuments),
		Relationships:        max(a.Relationships, b.Relationships),
		Paths:                max(a.Paths, b.Paths),
		Partitions:           max(a.Partitions, b.Partitions),
		SubPartitions:        max(a.SubPartitions, b.SubPartitions),
		Shards:               max(a.Shards, b.Shards),
		Namespaces:           max(a.Namespaces, b.Namespaces),
		Columns:              max(a.Columns, b.Columns),
		Types:                max(a.Types, b.Types),
		PropertyKeys:         max(a.PropertyKeys, b.PropertyKeys),
		Indexes:              max(a.Indexes, b.Indexes),
		Constraints:          max(a.Constraints, b.Constraints),
		Sequences:            max(a.Sequences, b.Sequences),
		Identities:           max(a.Identities, b.Identities),
		UUIDGenerators:       max(a.UUIDGenerators, b.UUIDGenerators),
		Functions:            max(a.Functions, b.Functions),
		Procedures:           max(a.Procedures, b.Procedures),
		Methods:              max(a.Methods, b.Methods),
		Triggers:             max(a.Triggers, b.Triggers),
		EventTriggers:        max(a.EventTriggers, b.EventTriggers),
		Aggregates:           max(a.Aggregates, b.Aggregates),
		Operators:            max(a.Operators, b.Operators),
		Modules:              max(a.Modules, b.Modules),
		Packages:             max(a.Packages, b.Packages),
		PackageBodies:        max(a.PackageBodies, b.PackageBodies),
		Macros:               max(a.Macros, b.Macros),
		Rules:                max(a.Rules, b.Rules),
		WindowFuncs:          max(a.WindowFuncs, b.WindowFuncs),
		Users:                max(a.Users, b.Users),
		Roles:                max(a.Roles, b.Roles),
		Grants:               max(a.Grants, b.Grants),
		Policies:             max(a.Policies, b.Policies),
		Tablespaces:          max(a.Tablespaces, b.Tablespaces),
		Segments:             max(a.Segments, b.Segments),
		Extents:              max(a.Extents, b.Extents),
		Pages:                max(a.Pages, b.Pages),
		Filegroups:           max(a.Filegroups, b.Filegroups),
		Datafiles:            max(a.Datafiles, b.Datafiles),
		Servers:              max(a.Servers, b.Servers),
		Connections:          max(a.Connections, b.Connections),
		Endpoints:            max(a.Endpoints, b.Endpoints),
		ForeignDataWrappers:  max(a.ForeignDataWrappers, b.ForeignDataWrappers),
		UserMappings:         max(a.UserMappings, b.UserMappings),
		Federations:          max(a.Federations, b.Federations),
		Replicas:             max(a.Replicas, b.Replicas),
		Clusters:             max(a.Clusters, b.Clusters),
		Tasks:                max(a.Tasks, b.Tasks),
		Jobs:                 max(a.Jobs, b.Jobs),
		Schedules:            max(a.Schedules, b.Schedules),
		Pipelines:            max(a.Pipelines, b.Pipelines),
		Streams:              max(a.Streams, b.Streams),
		Events:               max(a.Events, b.Events),
		Notifications:        max(a.Notifications, b.Notifications),
		Alerts:               max(a.Alerts, b.Alerts),
		Statistics:           max(a.Statistics, b.Statistics),
		Histograms:           max(a.Histograms, b.Histograms),
		Monitors:             max(a.Monitors, b.Monitors),
		MonitorMetrics:       max(a.MonitorMetrics, b.MonitorMetrics),
		Thresholds:           max(a.Thresholds, b.Thresholds),
		TextSearchComponents: max(a.TextSearchComponents, b.TextSearchComponents),
		Comments:             max(a.Comments, b.Comments),
		Annotations:          max(a.Annotations, b.Annotations),
		Tags:                 max(a.Tags, b.Tags),
		Aliases:              max(a.Aliases, b.Aliases),
		Synonyms:             max(a.Synonyms, b.Synonyms),
		Labels:               max(a.Labels, b.Labels),
		Snapshots:            max(a.Snapshots, b.Snapshots),
		Backups:              max(a.Backups, b.Backups),
		Archives:             max(a.Archives, b.Archives),
		RecoveryPoints:       max(a.RecoveryPoints, b.RecoveryPoints),
		Versions:             max(a.Versions, b.Versions),
		Migrations:           max(a.Migrations, b.Migrations),
		Branches:             max(a.Branches, b.Branches),
		TimeTravel:           max(a.TimeTravel, b.TimeTravel),
		Extensions:           max(a.Extensions, b.Extensions),
		Plugins:              max(a.Plugins, b.Plugins),
		ModuleExtensions:     max(a.ModuleExtensions, b.ModuleExtensions),
		TTLSettings:          max(a.TTLSettings, b.TTLSettings),
		Dimensions:           max(a.Dimensions, b.Dimensions),
		DistanceMetrics:      max(a.DistanceMetrics, b.DistanceMetrics),
		Projections:          max(a.Projections, b.Projections),
		AnalyticsAggs:        max(a.AnalyticsAggs, b.AnalyticsAggs),
		Transformations:      max(a.Transformations, b.Transformations),
		Enrichments:          max(a.Enrichments, b.Enrichments),
		BufferPools:          max(a.BufferPools, b.BufferPools),
		Publications:         max(a.Publications, b.Publications),
		Subscriptions:        max(a.Subscriptions, b.Subscriptions),
		ReplicationSlots:     max(a.ReplicationSlots, b.ReplicationSlots),
		FailoverGroups:       max(a.FailoverGroups, b.FailoverGroups),
	}
}

func mergeSizeMetrics(a, b SizeMetrics) SizeMetrics {
	merged := SizeMetrics{
		TotalSizeBytes:      max64(a.TotalSizeBytes, b.TotalSizeBytes),
		DataSizeBytes:       max64(a.DataSizeBytes, b.DataSizeBytes),
		IndexSizeBytes:      max64(a.IndexSizeBytes, b.IndexSizeBytes),
		TempSizeBytes:       max64(a.TempSizeBytes, b.TempSizeBytes),
		LogSizeBytes:        max64(a.LogSizeBytes, b.LogSizeBytes),
		CompressedSizeBytes: max64(a.CompressedSizeBytes, b.CompressedSizeBytes),
	}

	// Merge maps (taking the union and preferring non-zero values)
	merged.TableSizes = mergeMaps(a.TableSizes, b.TableSizes)
	merged.IndexSizes = mergeMaps(a.IndexSizes, b.IndexSizes)
	merged.CollectionSizes = mergeMaps(a.CollectionSizes, b.CollectionSizes)
	merged.ViewSizes = mergeMaps(a.ViewSizes, b.ViewSizes)
	merged.GraphSizes = mergeMaps(a.GraphSizes, b.GraphSizes)
	merged.VectorSizes = mergeMaps(a.VectorSizes, b.VectorSizes)

	return merged
}

func mergeRowMetrics(a, b RowMetrics) RowMetrics {
	merged := RowMetrics{
		TotalRows:      max64(a.TotalRows, b.TotalRows),
		TotalDocuments: max64(a.TotalDocuments, b.TotalDocuments),
		TotalNodes:     max64(a.TotalNodes, b.TotalNodes),
		TotalRelations: max64(a.TotalRelations, b.TotalRelations),
		TotalVectors:   max64(a.TotalVectors, b.TotalVectors),
	}

	// Merge maps
	merged.TableRows = mergeMaps(a.TableRows, b.TableRows)
	merged.CollectionDocs = mergeMaps(a.CollectionDocs, b.CollectionDocs)
	merged.NodeCounts = mergeMaps(a.NodeCounts, b.NodeCounts)
	merged.RelationshipCounts = mergeMaps(a.RelationshipCounts, b.RelationshipCounts)
	merged.VectorCounts = mergeMaps(a.VectorCounts, b.VectorCounts)

	return merged
}

// Generic helper functions

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func mergeMaps[K comparable, V any](a, b map[K]V) map[K]V {
	if a == nil && b == nil {
		return nil
	}

	result := make(map[K]V)

	// Copy from a
	for k, v := range a {
		result[k] = v
	}

	// Copy from b (will overwrite a's values)
	for k, v := range b {
		result[k] = v
	}

	return result
}
