package comparison

import "github.com/redbco/redb-open/pkg/unifiedmodel"

// createEmptyUnifiedModel creates a UnifiedModel with all map fields initialized to prevent nil pointer panics
func (c *UnifiedSchemaComparator) createEmptyUnifiedModel() *unifiedmodel.UnifiedModel {
	return &unifiedmodel.UnifiedModel{
		// Structural organization (optional levels depending on engine)
		Catalogs:  make(map[string]unifiedmodel.Catalog),
		Databases: make(map[string]unifiedmodel.Database),
		Schemas:   make(map[string]unifiedmodel.Schema),

		// Primary Data Containers
		Tables:       make(map[string]unifiedmodel.Table),
		Collections:  make(map[string]unifiedmodel.Collection),
		Nodes:        make(map[string]unifiedmodel.Node),
		MemoryTables: make(map[string]unifiedmodel.MemoryTable),

		// Temporary Data Containers
		TemporaryTables: make(map[string]unifiedmodel.TemporaryTable),
		TransientTables: make(map[string]unifiedmodel.TransientTable),
		Caches:          make(map[string]unifiedmodel.Cache),

		// Virtual Data Containers
		Views:             make(map[string]unifiedmodel.View),
		LiveViews:         make(map[string]unifiedmodel.LiveView),
		WindowViews:       make(map[string]unifiedmodel.WindowView),
		MaterializedViews: make(map[string]unifiedmodel.MaterializedView),
		ExternalTables:    make(map[string]unifiedmodel.ExternalTable),
		ForeignTables:     make(map[string]unifiedmodel.ForeignTable),

		// Graph / Vector / Search abstractions
		Graphs:        make(map[string]unifiedmodel.Graph),
		VectorIndexes: make(map[string]unifiedmodel.VectorIndex),
		SearchIndexes: make(map[string]unifiedmodel.SearchIndex),

		// Specialized Data Containers
		Vectors:           make(map[string]unifiedmodel.Vector),
		Embeddings:        make(map[string]unifiedmodel.Embedding),
		Documents:         make(map[string]unifiedmodel.Document),
		EmbeddedDocuments: make(map[string]unifiedmodel.EmbeddedDocument),
		Relationships:     make(map[string]unifiedmodel.Relationship),
		Paths:             make(map[string]unifiedmodel.Path),

		// Data Organization Containers
		Partitions:    make(map[string]unifiedmodel.Partition),
		SubPartitions: make(map[string]unifiedmodel.SubPartition),
		Shards:        make(map[string]unifiedmodel.Shard),
		Keyspaces:     make(map[string]unifiedmodel.Keyspace),
		Namespaces:    make(map[string]unifiedmodel.Namespace),

		// Structural definition objects
		Columns:      make(map[string]unifiedmodel.Column),
		Types:        make(map[string]unifiedmodel.Type),
		PropertyKeys: make(map[string]unifiedmodel.PropertyKey),

		// Integrity, performance and identity objects
		Indexes:        make(map[string]unifiedmodel.Index),
		Constraints:    make(map[string]unifiedmodel.Constraint),
		Sequences:      make(map[string]unifiedmodel.Sequence),
		Identities:     make(map[string]unifiedmodel.Identity),
		UUIDGenerators: make(map[string]unifiedmodel.UUIDGenerator),

		// Executable code objects
		Functions:     make(map[string]unifiedmodel.Function),
		Procedures:    make(map[string]unifiedmodel.Procedure),
		Methods:       make(map[string]unifiedmodel.Method),
		Triggers:      make(map[string]unifiedmodel.Trigger),
		EventTriggers: make(map[string]unifiedmodel.EventTrigger),
		Aggregates:    make(map[string]unifiedmodel.Aggregate),
		Operators:     make(map[string]unifiedmodel.Operator),
		Modules:       make(map[string]unifiedmodel.Module),
		Packages:      make(map[string]unifiedmodel.Package),
		PackageBodies: make(map[string]unifiedmodel.PackageBody),
		Macros:        make(map[string]unifiedmodel.Macro),
		Rules:         make(map[string]unifiedmodel.Rule),
		WindowFuncs:   make(map[string]unifiedmodel.WindowFunc),

		// Security and access control
		Users:    make(map[string]unifiedmodel.DBUser),
		Roles:    make(map[string]unifiedmodel.DBRole),
		Grants:   make(map[string]unifiedmodel.Grant),
		Policies: make(map[string]unifiedmodel.Policy),

		// Physical storage and placement
		Tablespaces: make(map[string]unifiedmodel.Tablespace),
		Segments:    make(map[string]unifiedmodel.Segment),
		Extents:     make(map[string]unifiedmodel.Extent),
		Pages:       make(map[string]unifiedmodel.Page),
		Filegroups:  make(map[string]unifiedmodel.Filegroup),
		Datafiles:   make(map[string]unifiedmodel.Datafile),

		// Connectivity and integration
		Servers:             make(map[string]unifiedmodel.Server),
		Connections:         make(map[string]unifiedmodel.Connection),
		Endpoints:           make(map[string]unifiedmodel.Endpoint),
		ForeignDataWrappers: make(map[string]unifiedmodel.ForeignDataWrapper),
		UserMappings:        make(map[string]unifiedmodel.UserMapping),
		Federations:         make(map[string]unifiedmodel.Federation),
		Replicas:            make(map[string]unifiedmodel.Replica),
		Clusters:            make(map[string]unifiedmodel.Cluster),

		// Operational, pipelines and streaming
		Tasks:     make(map[string]unifiedmodel.Task),
		Jobs:      make(map[string]unifiedmodel.Job),
		Schedules: make(map[string]unifiedmodel.Schedule),
		Pipelines: make(map[string]unifiedmodel.Pipeline),
		Streams:   make(map[string]unifiedmodel.Stream),

		// Monitoring and alerting
		Events:         make(map[string]unifiedmodel.Event),
		Notifications:  make(map[string]unifiedmodel.Notification),
		Alerts:         make(map[string]unifiedmodel.Alert),
		Statistics:     make(map[string]unifiedmodel.Statistic),
		Histograms:     make(map[string]unifiedmodel.Histogram),
		Monitors:       make(map[string]unifiedmodel.Monitor),
		MonitorMetrics: make(map[string]unifiedmodel.MonitorMetric),
		Thresholds:     make(map[string]unifiedmodel.Threshold),

		// Text processing / search configuration
		TextSearchComponents: make(map[string]unifiedmodel.TextSearchComponent),

		// Metadata and documentation
		Comments:    make(map[string]unifiedmodel.Comment),
		Annotations: make(map[string]unifiedmodel.Annotation),
		Tags:        make(map[string]unifiedmodel.Tag),
		Aliases:     make(map[string]unifiedmodel.Alias),
		Synonyms:    make(map[string]unifiedmodel.Synonym),
		Labels:      make(map[string]unifiedmodel.Label),

		// Backup and recovery, versioning
		Snapshots:      make(map[string]unifiedmodel.Snapshot),
		Backups:        make(map[string]unifiedmodel.Backup),
		Archives:       make(map[string]unifiedmodel.Archive),
		RecoveryPoints: make(map[string]unifiedmodel.RecoveryPoint),
		Versions:       make(map[string]unifiedmodel.VersionNode),
		Migrations:     make(map[string]unifiedmodel.Migration),
		Branches:       make(map[string]unifiedmodel.Branch),
		TimeTravel:     make(map[string]unifiedmodel.TimeTravel),

		// Extensions and customization
		Extensions:       make(map[string]unifiedmodel.Extension),
		Plugins:          make(map[string]unifiedmodel.Plugin),
		ModuleExtensions: make(map[string]unifiedmodel.ModuleExtension),
		TTLSettings:      make(map[string]unifiedmodel.TTLSetting),
		Dimensions:       make(map[string]unifiedmodel.DimensionSpec),
		DistanceMetrics:  make(map[string]unifiedmodel.DistanceMetricSpec),

		// Advanced analytics
		Projections:     make(map[string]unifiedmodel.Projection),
		AnalyticsAggs:   make(map[string]unifiedmodel.AggregationOp),
		Transformations: make(map[string]unifiedmodel.TransformationStep),
		Enrichments:     make(map[string]unifiedmodel.Enrichment),
		BufferPools:     make(map[string]unifiedmodel.BufferPool),

		// Replication & distribution
		Publications:     make(map[string]unifiedmodel.Publication),
		Subscriptions:    make(map[string]unifiedmodel.Subscription),
		ReplicationSlots: make(map[string]unifiedmodel.ReplicationSlot),
		FailoverGroups:   make(map[string]unifiedmodel.FailoverGroup),
	}
}
