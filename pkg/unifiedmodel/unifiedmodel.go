// Package unifiedmodel provides a unified schema representation for all supported database technologies.
// This package models 165+ database object types across all major paradigms:
// Relational, Document, Graph, Vector, Key-Value, Columnar, Wide-Column, Search Index, Time-Series, Object Storage

package unifiedmodel

import "github.com/redbco/redb-open/pkg/dbcapabilities"

// Type safety enums for common object types
type ObjectType string

const (
	// Data container types
	ObjectTypeTable            ObjectType = "table"
	ObjectTypeCollection       ObjectType = "collection"
	ObjectTypeView             ObjectType = "view"
	ObjectTypeMaterializedView ObjectType = "materialized_view"
	ObjectTypeTemporaryTable   ObjectType = "temporary_table"
	ObjectTypeMemoryTable      ObjectType = "memory_table"
	ObjectTypeExternalTable    ObjectType = "external_table"
	ObjectTypeForeignTable     ObjectType = "foreign_table"

	// Graph types
	ObjectTypeNode         ObjectType = "node"
	ObjectTypeRelationship ObjectType = "relationship"
	ObjectTypeGraph        ObjectType = "graph"

	// Vector types
	ObjectTypeVector      ObjectType = "vector"
	ObjectTypeVectorIndex ObjectType = "vector_index"
	ObjectTypeEmbedding   ObjectType = "embedding"

	// Search types
	ObjectTypeSearchIndex ObjectType = "search_index"
	ObjectTypeDocument    ObjectType = "document"

	// Structural definition objects
	ObjectTypeColumn   ObjectType = "column"
	ObjectTypeField    ObjectType = "field"
	ObjectTypeProperty ObjectType = "property"
	ObjectTypeType     ObjectType = "type"
	ObjectTypeSequence ObjectType = "sequence"

	// Integrity and performance objects
	ObjectTypeIndex      ObjectType = "index"
	ObjectTypeConstraint ObjectType = "constraint"

	// Executable code objects
	ObjectTypeFunction  ObjectType = "function"
	ObjectTypeProcedure ObjectType = "procedure"
	ObjectTypeTrigger   ObjectType = "trigger"
	ObjectTypeAggregate ObjectType = "aggregate"
	ObjectTypeOperator  ObjectType = "operator"
	ObjectTypePackage   ObjectType = "package"
	ObjectTypeRule      ObjectType = "rule"

	// Security and access control
	ObjectTypeUser   ObjectType = "user"
	ObjectTypeRole   ObjectType = "role"
	ObjectTypeGrant  ObjectType = "grant"
	ObjectTypePolicy ObjectType = "policy"

	// Physical storage
	ObjectTypeTablespace ObjectType = "tablespace"
	ObjectTypeDatafile   ObjectType = "datafile"

	// Connectivity and integration
	ObjectTypeServer             ObjectType = "server"
	ObjectTypeConnection         ObjectType = "connection"
	ObjectTypeForeignDataWrapper ObjectType = "foreign_data_wrapper"
	ObjectTypeUserMapping        ObjectType = "user_mapping"

	// Extensions and customization
	ObjectTypeExtension ObjectType = "extension"
	ObjectTypePlugin    ObjectType = "plugin"
)

type ConstraintType string

const (
	ConstraintTypePrimaryKey ConstraintType = "primary_key"
	ConstraintTypeForeignKey ConstraintType = "foreign_key"
	ConstraintTypeUnique     ConstraintType = "unique"
	ConstraintTypeCheck      ConstraintType = "check"
	ConstraintTypeNotNull    ConstraintType = "not_null"
	ConstraintTypeExclusion  ConstraintType = "exclusion"
	ConstraintTypeDefault    ConstraintType = "default"
)

type IndexType string

const (
	IndexTypeBTree      IndexType = "btree"
	IndexTypeHash       IndexType = "hash"
	IndexTypeGIN        IndexType = "gin"
	IndexTypeGiST       IndexType = "gist"
	IndexTypeBitmap     IndexType = "bitmap"
	IndexTypeClustered  IndexType = "clustered"
	IndexTypeCovering   IndexType = "covering"
	IndexTypeExpression IndexType = "expression"
	IndexTypeSparse     IndexType = "sparse"
	IndexTypePartial    IndexType = "partial"
	IndexTypeFullText   IndexType = "fulltext"
	IndexTypeSpatial    IndexType = "spatial"
	IndexTypeVector     IndexType = "vector"
)

type PolicyType string

const (
	PolicyTypeRowSecurity   PolicyType = "row_security"
	PolicyTypeColumnMasking PolicyType = "column_masking"
	PolicyTypeDataMasking   PolicyType = "data_masking"
	PolicyTypeAccessControl PolicyType = "access_control"
	PolicyTypeAudit         PolicyType = "audit"
	PolicyTypePassword      PolicyType = "password"
	PolicyTypeSession       PolicyType = "session"
	PolicyTypeEncryption    PolicyType = "encryption"
	PolicyTypeRetention     PolicyType = "retention"
)

type TextSearchComponentType string

const (
	TextSearchTypeParser        TextSearchComponentType = "parser"
	TextSearchTypeDictionary    TextSearchComponentType = "dictionary"
	TextSearchTypeTemplate      TextSearchComponentType = "template"
	TextSearchTypeConfiguration TextSearchComponentType = "configuration"
	TextSearchTypeAnalyzer      TextSearchComponentType = "analyzer"
	TextSearchTypeTokenizer     TextSearchComponentType = "tokenizer"
	TextSearchTypeFilter        TextSearchComponentType = "filter"
	TextSearchTypeNormalizer    TextSearchComponentType = "normalizer"
)

// UnifiedModel is a unified model for all database types
type UnifiedModel struct {
	// Database Structure Type
	DatabaseType dbcapabilities.DatabaseType `json:"database_type"`

	// Structural organization (optional levels depending on engine)
	Catalogs  map[string]Catalog  `json:"catalogs"`
	Databases map[string]Database `json:"databases"`
	Schemas   map[string]Schema   `json:"schemas"`

	// Primary Data Containers
	Tables       map[string]Table       `json:"tables"`
	Collections  map[string]Collection  `json:"collections"`
	Nodes        map[string]Node        `json:"nodes"`
	MemoryTables map[string]MemoryTable `json:"memory_tables"`

	// Temporary Data Containers
	TemporaryTables map[string]TemporaryTable `json:"temporary_tables"`
	TransientTables map[string]TransientTable `json:"transient_tables"`
	CacheMechanism  string                    `json:"cache_mechanism,omitempty"`
	Caches          map[string]Cache          `json:"caches"`

	// Virtual Data Containers
	Views             map[string]View             `json:"views"`
	LiveViews         map[string]LiveView         `json:"live_views"`
	WindowViews       map[string]WindowView       `json:"window_views"`
	MaterializedViews map[string]MaterializedView `json:"materialized_views"`
	ExternalTables    map[string]ExternalTable    `json:"external_tables"`
	ForeignTables     map[string]ForeignTable     `json:"foreign_tables"`

	// Graph / Vector / Search abstractions
	Graphs        map[string]Graph       `json:"graphs"`
	VectorIndexes map[string]VectorIndex `json:"vector_indexes"`
	SearchIndexes map[string]SearchIndex `json:"search_indexes"`

	// Specialized Data Containers
	Vectors           map[string]Vector           `json:"vectors"`
	Embeddings        map[string]Embedding        `json:"embeddings"`
	Documents         map[string]Document         `json:"documents"`
	EmbeddedDocuments map[string]EmbeddedDocument `json:"embedded_documents"`
	Relationships     map[string]Relationship     `json:"relationships"`
	Paths             map[string]Path             `json:"paths"`

	// Data Organization Containers
	Partitions    map[string]Partition    `json:"partitions"`
	SubPartitions map[string]SubPartition `json:"sub_partitions"`
	Shards        map[string]Shard        `json:"shards"`
	Keyspaces     map[string]Keyspace     `json:"keyspaces"`
	Namespaces    map[string]Namespace    `json:"namespaces"`

	// Structural definition objects
	Columns      map[string]Column      `json:"columns"`
	Types        map[string]Type        `json:"types"`
	PropertyKeys map[string]PropertyKey `json:"property_keys"`

	// Integrity, performance and identity objects
	Indexes        map[string]Index         `json:"indexes"`
	Constraints    map[string]Constraint    `json:"constraints"`
	Sequences      map[string]Sequence      `json:"sequences"`
	Identities     map[string]Identity      `json:"identities"`
	UUIDGenerators map[string]UUIDGenerator `json:"uuid_generators"`

	// Executable code objects
	Functions     map[string]Function     `json:"functions"`
	Procedures    map[string]Procedure    `json:"procedures"`
	Methods       map[string]Method       `json:"methods"`
	Triggers      map[string]Trigger      `json:"triggers"`
	EventTriggers map[string]EventTrigger `json:"event_triggers"`
	Aggregates    map[string]Aggregate    `json:"aggregates"`
	Operators     map[string]Operator     `json:"operators"`
	Modules       map[string]Module       `json:"modules"`
	Packages      map[string]Package      `json:"packages"`
	PackageBodies map[string]PackageBody  `json:"package_bodies"`
	Macros        map[string]Macro        `json:"macros"`
	Rules         map[string]Rule         `json:"rules"`
	WindowFuncs   map[string]WindowFunc   `json:"window_functions"`

	// Security and access control
	Users    map[string]DBUser `json:"users"`
	Roles    map[string]DBRole `json:"roles"`
	Grants   map[string]Grant  `json:"grants"`
	Policies map[string]Policy `json:"policies"`

	// Physical storage and placement
	Tablespaces map[string]Tablespace `json:"tablespaces"`
	Segments    map[string]Segment    `json:"segments"`
	Extents     map[string]Extent     `json:"extents"`
	Pages       map[string]Page       `json:"pages"`
	Filegroups  map[string]Filegroup  `json:"filegroups"`
	Datafiles   map[string]Datafile   `json:"datafiles"`

	// Connectivity and integration
	Servers             map[string]Server             `json:"servers"`
	Connections         map[string]Connection         `json:"connections"`
	Endpoints           map[string]Endpoint           `json:"endpoints"`
	ForeignDataWrappers map[string]ForeignDataWrapper `json:"foreign_data_wrappers"`
	UserMappings        map[string]UserMapping        `json:"user_mappings"`
	Federations         map[string]Federation         `json:"federations"`
	Replicas            map[string]Replica            `json:"replicas"`
	Clusters            map[string]Cluster            `json:"clusters"`

	// Operational, pipelines and streaming
	Tasks     map[string]Task     `json:"tasks"`
	Jobs      map[string]Job      `json:"jobs"`
	Schedules map[string]Schedule `json:"schedules"`
	Pipelines map[string]Pipeline `json:"pipelines"`
	Streams   map[string]Stream   `json:"streams"`

	// Monitoring and alerting
	Events         map[string]Event         `json:"events"`
	Notifications  map[string]Notification  `json:"notifications"`
	Alerts         map[string]Alert         `json:"alerts"`
	Statistics     map[string]Statistic     `json:"statistics"`
	Histograms     map[string]Histogram     `json:"histograms"`
	Monitors       map[string]Monitor       `json:"monitors"`
	MonitorMetrics map[string]MonitorMetric `json:"monitor_metrics"`
	Thresholds     map[string]Threshold     `json:"thresholds"`

	// Text processing / search configuration
	TextSearchComponents map[string]TextSearchComponent `json:"text_search_components"`

	// Metadata and documentation
	Comments    map[string]Comment    `json:"comments"`
	Annotations map[string]Annotation `json:"annotations"`
	Tags        map[string]Tag        `json:"tags"`
	Aliases     map[string]Alias      `json:"aliases"`
	Synonyms    map[string]Synonym    `json:"synonyms"`
	Labels      map[string]Label      `json:"labels"`

	// Backup and recovery, versioning
	Snapshots      map[string]Snapshot      `json:"snapshots"`
	Backups        map[string]Backup        `json:"backups"`
	Archives       map[string]Archive       `json:"archives"`
	RecoveryPoints map[string]RecoveryPoint `json:"recovery_points"`
	Versions       map[string]VersionNode   `json:"versions"`
	Migrations     map[string]Migration     `json:"migrations"`
	Branches       map[string]Branch        `json:"branches"`
	TimeTravel     map[string]TimeTravel    `json:"time_travel"`

	// Extensions and customization
	Extensions       map[string]Extension          `json:"extensions"`
	Plugins          map[string]Plugin             `json:"plugins"`
	ModuleExtensions map[string]ModuleExtension    `json:"module_extensions"`
	TTLSettings      map[string]TTLSetting         `json:"ttl_settings"`
	Dimensions       map[string]DimensionSpec      `json:"dimensions"`
	DistanceMetrics  map[string]DistanceMetricSpec `json:"distance_metrics"`

	// Advanced analytics
	Projections     map[string]Projection         `json:"projections"`
	AnalyticsAggs   map[string]AggregationOp      `json:"analytics_aggregations"`
	Transformations map[string]TransformationStep `json:"transformations"`
	Enrichments     map[string]Enrichment         `json:"enrichments"`
	BufferPools     map[string]BufferPool         `json:"buffer_pools"`

	// Replication & distribution
	Publications     map[string]Publication     `json:"publications"`
	Subscriptions    map[string]Subscription    `json:"subscriptions"`
	ReplicationSlots map[string]ReplicationSlot `json:"replication_slots"`
	FailoverGroups   map[string]FailoverGroup   `json:"failover_groups"`
}

// GetBasicMetrics generates basic metrics (counts and simple calculations) from this UnifiedModel
func (um *UnifiedModel) GetBasicMetrics(schemaID string) *UnifiedModelMetrics {
	return GenerateBasicMetrics(um, schemaID)
}

// --- Structural containers ---

type Catalog struct {
	Name        string                `json:"name"`
	Owner       string                `json:"owner,omitempty"`
	Comment     string                `json:"comment,omitempty"`
	Labels      map[string]string     `json:"labels,omitempty"`
	Options     map[string]any        `json:"options,omitempty"`
	Databases   map[string]Database   `json:"databases,omitempty"`
	Schemas     map[string]Schema     `json:"schemas,omitempty"`
	Tables      map[string]Table      `json:"tables,omitempty"` // For engines without schemas
	Collections map[string]Collection `json:"collections,omitempty"`
}

type Database struct {
	Name          string            `json:"name"`
	Owner         string            `json:"owner,omitempty"`
	Comment       string            `json:"comment,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
	Options       map[string]any    `json:"options,omitempty"`
	DefaultSchema string            `json:"default_schema,omitempty"`

	Schemas     map[string]Schema     `json:"schemas,omitempty"`
	Tables      map[string]Table      `json:"tables,omitempty"`
	Collections map[string]Collection `json:"collections,omitempty"`
	Graphs      map[string]Graph      `json:"graphs,omitempty"`
	Namespaces  map[string]Namespace  `json:"namespaces,omitempty"`
	Buckets     map[string]Bucket     `json:"buckets,omitempty"` // Object storage
}

type Schema struct {
	Name       string                      `json:"name"`
	Owner      string                      `json:"owner,omitempty"`
	Comment    string                      `json:"comment,omitempty"`
	Labels     map[string]string           `json:"labels,omitempty"`
	Options    map[string]any              `json:"options,omitempty"`
	Tables     map[string]Table            `json:"tables,omitempty"`
	Views      map[string]View             `json:"views,omitempty"`
	MatViews   map[string]MaterializedView `json:"materialized_views,omitempty"`
	Sequences  map[string]Sequence         `json:"sequences,omitempty"`
	Types      map[string]Type             `json:"types,omitempty"`
	Functions  map[string]Function         `json:"functions,omitempty"`
	Procedures map[string]Procedure        `json:"procedures,omitempty"`
	Triggers   map[string]Trigger          `json:"triggers,omitempty"`
}

// --- Primary and virtual containers ---

type Table struct {
	Name        string                `json:"name"`
	Owner       string                `json:"owner,omitempty"`
	Comment     string                `json:"comment,omitempty"`
	Labels      map[string]string     `json:"labels,omitempty"`
	Options     map[string]any        `json:"options,omitempty"`
	Columns     map[string]Column     `json:"columns"`
	Indexes     map[string]Index      `json:"indexes,omitempty"`
	Constraints map[string]Constraint `json:"constraints,omitempty"`
	Partitions  map[string]Partition  `json:"partitions,omitempty"`
	SubTables   map[string]Table      `json:"sub_tables,omitempty"` // e.g., partition children
}

type Collection struct {
	Name       string            `json:"name"`
	Owner      string            `json:"owner,omitempty"`
	Comment    string            `json:"comment,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
	Options    map[string]any    `json:"options,omitempty"`
	Fields     map[string]Field  `json:"fields,omitempty"`
	Indexes    map[string]Index  `json:"indexes,omitempty"`
	Validation map[string]any    `json:"validation,omitempty"`
	ShardKey   []string          `json:"shard_key,omitempty"`
}

type Node struct {
	Label      string              `json:"label"`
	Properties map[string]Property `json:"properties,omitempty"`
	Indexes    map[string]Index    `json:"indexes,omitempty"`
}

type MemoryTable struct {
	Name    string            `json:"name"`
	Columns map[string]Column `json:"columns,omitempty"`
	Options map[string]any    `json:"options,omitempty"`
}

type TemporaryTable struct {
	Name    string            `json:"name"`
	Scope   string            `json:"scope,omitempty"` // session, transaction
	Columns map[string]Column `json:"columns,omitempty"`
}

type Cache struct {
	Name    string         `json:"name"`
	Scope   string         `json:"scope,omitempty"` // session, global, query, materialized
	Options map[string]any `json:"options,omitempty"`
}

type TransientTable struct {
	Name    string            `json:"name"`
	Columns map[string]Column `json:"columns,omitempty"`
	Options map[string]any    `json:"options,omitempty"`
}

type View struct {
	Name       string            `json:"name"`
	Definition string            `json:"definition"`
	Comment    string            `json:"comment,omitempty"`
	Columns    map[string]Column `json:"columns,omitempty"`
	Options    map[string]any    `json:"options,omitempty"`
}

type LiveView struct {
	Name       string         `json:"name"`
	Definition string         `json:"definition"`
	Options    map[string]any `json:"options,omitempty"`
}

type WindowView struct {
	Name       string         `json:"name"`
	Definition string         `json:"definition"`
	WindowSpec string         `json:"window_spec,omitempty"`
	Options    map[string]any `json:"options,omitempty"`
}

type MaterializedView struct {
	Name        string            `json:"name"`
	Definition  string            `json:"definition"`
	RefreshMode string            `json:"refresh_mode,omitempty"` // immediate, deferred, manual
	RefreshCron string            `json:"refresh_cron,omitempty"`
	Columns     map[string]Column `json:"columns,omitempty"`
	Storage     map[string]any    `json:"storage,omitempty"`
}

type ExternalTable struct {
	Name     string            `json:"name"`
	Location string            `json:"location"`
	Format   string            `json:"format,omitempty"`
	Columns  map[string]Column `json:"columns,omitempty"`
	Options  map[string]any    `json:"options,omitempty"`
}

type ForeignTable struct {
	Name    string            `json:"name"`
	Server  string            `json:"server"`
	Options map[string]any    `json:"options,omitempty"`
	Columns map[string]Column `json:"columns,omitempty"`
}

// Graph and vector/search containers

type Graph struct {
	Name        string                  `json:"name"`
	NodeLabels  map[string]Node         `json:"node_labels,omitempty"`
	RelTypes    map[string]Relationship `json:"relationship_types,omitempty"`
	Indexes     map[string]Index        `json:"indexes,omitempty"`
	Constraints map[string]Constraint   `json:"constraints,omitempty"`
}

type VectorIndex struct {
	Name       string         `json:"name"`
	On         string         `json:"on"` // table/collection name
	Fields     []string       `json:"fields"`
	Metric     string         `json:"metric,omitempty"` // cosine, l2, ip
	Dimension  int            `json:"dimension,omitempty"`
	Parameters map[string]any `json:"parameters,omitempty"`
}

type SearchIndex struct {
	Name     string         `json:"name"`
	On       string         `json:"on"`
	Fields   []string       `json:"fields,omitempty"`
	Analyzer string         `json:"analyzer,omitempty"`
	Options  map[string]any `json:"options,omitempty"`
}

// Specialized data containers

type Vector struct {
	Name      string         `json:"name"`
	Dimension int            `json:"dimension"`
	Metric    string         `json:"metric,omitempty"`
	Options   map[string]any `json:"options,omitempty"`
}

type Embedding struct {
	Name    string         `json:"name"`
	Model   string         `json:"model,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type Document struct {
	Key    string         `json:"key"`
	Fields map[string]any `json:"fields"`
}

type EmbeddedDocument struct {
	Name   string         `json:"name"`
	Fields map[string]any `json:"fields"`
}

type Relationship struct {
	Type       string              `json:"type"`
	FromLabel  string              `json:"from_label"`
	ToLabel    string              `json:"to_label"`
	Properties map[string]Property `json:"properties,omitempty"`
}

type Path struct {
	Name     string   `json:"name"`
	Sequence []string `json:"sequence"` // Relationship type sequence or pattern
}

// Data organization

type Partition struct {
	Name    string         `json:"name"`
	Type    string         `json:"type,omitempty"` // range, list, hash, composite, time
	Key     []string       `json:"key,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type SubPartition struct {
	Name    string         `json:"name"`
	Type    string         `json:"type,omitempty"`
	Key     []string       `json:"key,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type Shard struct {
	Name     string         `json:"name"`
	Strategy string         `json:"strategy,omitempty"`
	Key      []string       `json:"key,omitempty"`
	Options  map[string]any `json:"options,omitempty"`
}

// TODO: This should be refactored (currently used by Cassandra)
type Keyspace struct {
	Name                string            `json:"name"`
	ReplicationStrategy string            `json:"replicationStrategy"`
	ReplicationOptions  map[string]string `json:"replicationOptions"`
	DurableWrites       bool              `json:"durableWrites"`
}

type Namespace struct {
	Name    string            `json:"name"`
	Labels  map[string]string `json:"labels,omitempty"`
	Options map[string]any    `json:"options,omitempty"`
}

// Structural definitions

type Column struct {
	Name                string         `json:"name"`
	DataType            string         `json:"data_type"`
	Nullable            bool           `json:"nullable"`
	Default             string         `json:"default,omitempty"`
	GeneratedExpression string         `json:"generated_expression,omitempty"`
	IsPrimaryKey        bool           `json:"is_primary_key,omitempty"`
	IsPartitionKey      bool           `json:"is_partition_key,omitempty"`
	IsClusteringKey     bool           `json:"is_clustering_key,omitempty"`
	AutoIncrement       bool           `json:"auto_increment,omitempty"`
	Collation           string         `json:"collation,omitempty"`
	Options             map[string]any `json:"options,omitempty"`
}

type Field struct {
	Name     string         `json:"name"`
	Type     string         `json:"type"`
	Required bool           `json:"required,omitempty"`
	Options  map[string]any `json:"options,omitempty"`
}

type Property struct {
	Name    string         `json:"name"`
	Type    string         `json:"type"`
	Options map[string]any `json:"options,omitempty"`
}

type PropertyKey struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Type struct {
	Name       string         `json:"name"`
	Category   string         `json:"category"` // scalar, composite, enum, range, json, xml, spatial, temporal, binary, domain
	Definition map[string]any `json:"definition,omitempty"`
}

// Integrity / performance

type Index struct {
	Name       string         `json:"name"`
	Type       IndexType      `json:"type,omitempty"` // Use IndexType enum for type safety
	Columns    []string       `json:"columns,omitempty"`
	Fields     []string       `json:"fields,omitempty"`
	Expression string         `json:"expression,omitempty"`
	Predicate  string         `json:"predicate,omitempty"` // partial index condition
	Unique     bool           `json:"unique,omitempty"`
	Options    map[string]any `json:"options,omitempty"`
}

type Constraint struct {
	Name       string         `json:"name"`
	Type       ConstraintType `json:"type"` // Use ConstraintType enum for type safety
	Columns    []string       `json:"columns,omitempty"`
	Expression string         `json:"expression,omitempty"`
	Reference  Reference      `json:"reference,omitempty"`
	Options    map[string]any `json:"options,omitempty"`
}

type Reference struct {
	Table    string   `json:"table,omitempty"`
	Columns  []string `json:"columns,omitempty"`
	OnUpdate string   `json:"on_update,omitempty"`
	OnDelete string   `json:"on_delete,omitempty"`
}

type Sequence struct {
	Name      string         `json:"name"`
	Start     int64          `json:"start,omitempty"`
	Increment int64          `json:"increment,omitempty"`
	Min       *int64         `json:"min,omitempty"`
	Max       *int64         `json:"max,omitempty"`
	Cache     *int64         `json:"cache,omitempty"`
	Cycle     bool           `json:"cycle,omitempty"`
	Options   map[string]any `json:"options,omitempty"`
}

type Identity struct {
	Name      string         `json:"name"`
	Table     string         `json:"table,omitempty"`
	Column    string         `json:"column,omitempty"`
	Strategy  string         `json:"strategy,omitempty"` // always, by_default
	Start     int64          `json:"start,omitempty"`
	Increment int64          `json:"increment,omitempty"`
	Cycle     bool           `json:"cycle,omitempty"`
	Options   map[string]any `json:"options,omitempty"`
}

type UUIDGenerator struct {
	Name    string         `json:"name"`
	Version string         `json:"version,omitempty"` // v1, v4, v7
	Options map[string]any `json:"options,omitempty"`
}

// Executable code

type Function struct {
	Name       string         `json:"name"`
	Language   string         `json:"language,omitempty"`
	Returns    string         `json:"returns,omitempty"`
	Arguments  []Argument     `json:"arguments,omitempty"`
	Definition string         `json:"definition"`
	Options    map[string]any `json:"options,omitempty"`
}

type Procedure struct {
	Name       string         `json:"name"`
	Language   string         `json:"language,omitempty"`
	Arguments  []Argument     `json:"arguments,omitempty"`
	Definition string         `json:"definition"`
	Options    map[string]any `json:"options,omitempty"`
}

type Method struct {
	Name       string         `json:"name"`
	OfType     string         `json:"of_type,omitempty"` // object type
	Language   string         `json:"language,omitempty"`
	Arguments  []Argument     `json:"arguments,omitempty"`
	Definition string         `json:"definition"`
	Options    map[string]any `json:"options,omitempty"`
}

type Trigger struct {
	Name      string         `json:"name"`
	Table     string         `json:"table,omitempty"`
	Timing    string         `json:"timing"` // before, after, instead_of
	Events    []string       `json:"events"` // insert, update, delete, truncate
	Procedure string         `json:"procedure"`
	Options   map[string]any `json:"options,omitempty"`
}

type EventTrigger struct {
	Name      string         `json:"name"`
	Scope     string         `json:"scope,omitempty"` // database, schema
	Events    []string       `json:"events"`
	Procedure string         `json:"procedure"`
	Options   map[string]any `json:"options,omitempty"`
}

type Aggregate struct {
	Name       string         `json:"name"`
	InputTypes []string       `json:"input_types,omitempty"`
	StateType  string         `json:"state_type,omitempty"`
	FinalType  string         `json:"final_type,omitempty"`
	Definition map[string]any `json:"definition,omitempty"`
}

type Operator struct {
	Name       string `json:"name"`
	LeftType   string `json:"left_type,omitempty"`
	RightType  string `json:"right_type,omitempty"`
	Returns    string `json:"returns,omitempty"`
	Definition string `json:"definition,omitempty"`
}

type Package struct {
	Name    string         `json:"name"`
	Spec    string         `json:"spec,omitempty"`
	Body    string         `json:"body,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type PackageBody struct {
	Name    string         `json:"name"`
	Body    string         `json:"body"`
	Options map[string]any `json:"options,omitempty"`
}

type Module struct {
	Name     string         `json:"name"`
	Comment  string         `json:"comment,omitempty"`
	Language string         `json:"language,omitempty"`
	Code     string         `json:"code,omitempty"`
	Options  map[string]any `json:"options,omitempty"`
}

type Macro struct {
	Name       string         `json:"name"`
	Definition string         `json:"definition"`
	Options    map[string]any `json:"options,omitempty"`
}

type Rule struct {
	Name       string         `json:"name"`
	Target     string         `json:"target"`
	Definition string         `json:"definition"`
	Options    map[string]any `json:"options,omitempty"`
}

type WindowFunc struct {
	Name       string         `json:"name"`
	Definition string         `json:"definition"`
	Options    map[string]any `json:"options,omitempty"`
}

type Argument struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Security

type DBUser struct {
	Name    string            `json:"name"`
	Roles   []string          `json:"roles,omitempty"`
	Options map[string]any    `json:"options,omitempty"`
	Labels  map[string]string `json:"labels,omitempty"`
}

type DBRole struct {
	Name        string            `json:"name"`
	Members     []string          `json:"members,omitempty"`
	ParentRoles []string          `json:"parent_roles,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
}

type Grant struct {
	Principal string   `json:"principal"` // user/role
	Privilege string   `json:"privilege"`
	Scope     string   `json:"scope"` // database, schema, table, column, function, sequence, collection, index
	Object    string   `json:"object,omitempty"`
	Columns   []string `json:"columns,omitempty"`
}

type Policy struct {
	Name       string         `json:"name"`
	Type       PolicyType     `json:"type"`  // Use PolicyType enum for type safety
	Scope      string         `json:"scope"` // database, schema, table, column, function, etc.
	Object     string         `json:"object,omitempty"`
	Definition string         `json:"definition"`
	Options    map[string]any `json:"options,omitempty"`
}

// Physical storage

type Tablespace struct {
	Name    string         `json:"name"`
	Options map[string]any `json:"options,omitempty"`
}

type Filegroup struct {
	Name    string         `json:"name"`
	Options map[string]any `json:"options,omitempty"`
}

type Datafile struct {
	Name    string         `json:"name"`
	Path    string         `json:"path"`
	Size    int64          `json:"size_bytes,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type Segment struct {
	Name    string         `json:"name"`
	Options map[string]any `json:"options,omitempty"`
}

type Extent struct {
	Name    string         `json:"name"`
	Size    int64          `json:"size_bytes,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type Page struct {
	Number  int            `json:"number"`
	Size    int64          `json:"size_bytes,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

// Connectivity

type Server struct {
	Name    string         `json:"name"`
	Type    string         `json:"type,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type Connection struct {
	Name    string         `json:"name"`
	Driver  string         `json:"driver,omitempty"`
	DSN     string         `json:"dsn,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type Endpoint struct {
	Name    string         `json:"name"`
	Scheme  string         `json:"scheme,omitempty"`
	Host    string         `json:"host,omitempty"`
	Port    int            `json:"port,omitempty"`
	Path    string         `json:"path,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type ForeignDataWrapper struct {
	Name    string         `json:"name"`
	Handler string         `json:"handler,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type UserMapping struct {
	User    string         `json:"user"`
	Server  string         `json:"server"`
	Options map[string]any `json:"options,omitempty"`
}

type Federation struct {
	Name    string         `json:"name"`
	Members []string       `json:"members,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type Replica struct {
	Name    string         `json:"name"`
	Mode    string         `json:"mode,omitempty"` // sync, async
	Options map[string]any `json:"options,omitempty"`
}

type Cluster struct {
	Name    string         `json:"name"`
	Nodes   []string       `json:"nodes,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

// Operational

type Task struct {
	Name       string         `json:"name"`
	Definition string         `json:"definition,omitempty"`
	Schedule   string         `json:"schedule,omitempty"`
	Options    map[string]any `json:"options,omitempty"`
}

type Job struct {
	Name     string         `json:"name"`
	Type     string         `json:"type,omitempty"`
	Schedule string         `json:"schedule,omitempty"`
	Options  map[string]any `json:"options,omitempty"`
}

type Schedule struct {
	Name    string         `json:"name"`
	Cron    string         `json:"cron,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type Pipeline struct {
	Name    string         `json:"name"`
	Steps   []string       `json:"steps,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type Stream struct {
	Name    string         `json:"name"`
	On      string         `json:"on,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

// Monitoring and alerting

type Event struct {
	Name    string         `json:"name"`
	Source  string         `json:"source,omitempty"`
	Payload map[string]any `json:"payload,omitempty"`
}

type Notification struct {
	Name    string         `json:"name"`
	Channel string         `json:"channel,omitempty"`
	Message string         `json:"message,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type Alert struct {
	Name      string         `json:"name"`
	Condition string         `json:"condition"`
	Severity  string         `json:"severity,omitempty"`
	Options   map[string]any `json:"options,omitempty"`
}

type Statistic struct {
	Name   string            `json:"name"`
	Value  any               `json:"value,omitempty"`
	Labels map[string]string `json:"labels,omitempty"`
}

type Histogram struct {
	Name    string             `json:"name"`
	Buckets map[string]float64 `json:"buckets,omitempty"`
}

type Monitor struct {
	Name    string         `json:"name"`
	Scope   string         `json:"scope,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type MonitorMetric struct {
	Name   string            `json:"name"`
	Unit   string            `json:"unit,omitempty"`
	Labels map[string]string `json:"labels,omitempty"`
}

type Threshold struct {
	Name     string         `json:"name"`
	Metric   string         `json:"metric"`
	Operator string         `json:"operator"`
	Value    any            `json:"value"`
	Options  map[string]any `json:"options,omitempty"`
}

// Text processing

type TextSearchComponent struct {
	Name         string                  `json:"name"`
	Type         TextSearchComponentType `json:"type"`                   // parser, dictionary, template, configuration, analyzer, tokenizer, filter, normalizer
	Parser       string                  `json:"parser,omitempty"`       // For configuration type
	Dictionaries []string                `json:"dictionaries,omitempty"` // For configuration type
	Chain        []string                `json:"chain,omitempty"`        // For analyzer type (tokenizer + filters)
	Options      map[string]any          `json:"options,omitempty"`
	Comment      string                  `json:"comment,omitempty"`
}

// Metadata and documentation

type Comment struct {
	On      string `json:"on"` // qualified object name
	Comment string `json:"comment"`
}

type Annotation struct {
	On    string `json:"on"`
	Key   string `json:"key"`
	Value any    `json:"value"`
}

type Tag struct {
	On   string `json:"on"`
	Name string `json:"name"`
}

type Alias struct {
	On    string `json:"on"`
	Alias string `json:"alias"`
}

type Synonym struct {
	On   string `json:"on"`
	Name string `json:"name"`
}

type Label struct {
	On    string            `json:"on"`
	Name  string            `json:"name"`
	Props map[string]string `json:"props,omitempty"`
}

type RelationshipType struct {
	Name       string              `json:"name"`
	Properties map[string]Property `json:"properties,omitempty"`
}

// Backup and recovery

type Snapshot struct {
	Name    string         `json:"name"`
	Scope   string         `json:"scope,omitempty"` // instance, database, schema, table
	Options map[string]any `json:"options,omitempty"`
}

type Backup struct {
	Name    string         `json:"name"`
	Method  string         `json:"method,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type Archive struct {
	Name    string         `json:"name"`
	Format  string         `json:"format,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type RecoveryPoint struct {
	Name  string `json:"name"`
	Point string `json:"point"` // LSN/GTID/TSO/timestamp
}

type VersionNode struct {
	ID      string   `json:"id"`
	Parents []string `json:"parents,omitempty"`
	Message string   `json:"message,omitempty"`
}

type Migration struct {
	ID          string `json:"id"`
	Description string `json:"description,omitempty"`
	Script      string `json:"script,omitempty"`
}

type Branch struct {
	Name string `json:"name"`
	From string `json:"from,omitempty"`
}

type TimeTravel struct {
	Object string `json:"object"`
	AsOf   string `json:"as_of"` // timestamp or version id
}

// Object storage

type Bucket struct {
	Name    string         `json:"name"`
	Region  string         `json:"region,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

// Extensions and customization extras

type TTLSetting struct {
	Name    string         `json:"name"`
	Scope   string         `json:"scope,omitempty"` // table, collection, index
	Policy  string         `json:"policy"`
	Options map[string]any `json:"options,omitempty"`
}

type DimensionSpec struct {
	Name string `json:"name"`
	Size int    `json:"size"`
}

type DistanceMetricSpec struct {
	Name    string         `json:"name"`
	Method  string         `json:"method"` // cosine, l2, ip, hamming
	Options map[string]any `json:"options,omitempty"`
}

// Advanced analytics

type Projection struct {
	Name       string         `json:"name"`
	Definition string         `json:"definition"`
	Options    map[string]any `json:"options,omitempty"`
}

type AggregationOp struct {
	Name       string         `json:"name"`
	Definition string         `json:"definition"`
	Options    map[string]any `json:"options,omitempty"`
}

type TransformationStep struct {
	Name       string         `json:"name"`
	Definition string         `json:"definition"`
	Options    map[string]any `json:"options,omitempty"`
}

type Enrichment struct {
	Name       string         `json:"name"`
	Definition string         `json:"definition"`
	Options    map[string]any `json:"options,omitempty"`
}

type BufferPool struct {
	Name    string         `json:"name"`
	Size    int64          `json:"size_bytes,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

// Replication & distribution

type Publication struct {
	Name    string         `json:"name"`
	Objects []string       `json:"objects,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type Subscription struct {
	Name    string         `json:"name"`
	Source  string         `json:"source"`
	Options map[string]any `json:"options,omitempty"`
}

type ReplicationSlot struct {
	Name    string         `json:"name"`
	Type    string         `json:"type,omitempty"` // logical, physical
	Options map[string]any `json:"options,omitempty"`
}

type FailoverGroup struct {
	Name    string         `json:"name"`
	Members []string       `json:"members"`
	Mode    string         `json:"mode,omitempty"` // active-active, active-passive
	Options map[string]any `json:"options,omitempty"`
}

// Extensions and customization types

type Extension struct {
	Name    string         `json:"name"`
	Version string         `json:"version,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type Plugin struct {
	Name    string         `json:"name"`
	Version string         `json:"version,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}

type ModuleExtension struct {
	Name    string         `json:"name"`
	Module  string         `json:"module,omitempty"`
	Version string         `json:"version,omitempty"`
	Options map[string]any `json:"options,omitempty"`
}
