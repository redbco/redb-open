package metadata

import (
	"time"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// DatabaseID is the canonical identifier for a database technology
// (e.g., "postgres", "mysql", "mongodb", "snowflake", ...).
type DatabaseType = dbcapabilities.DatabaseType

// InstanceMetadata captures node/cluster-level facts about a running database service.
// It is designed to be technology-agnostic while exposing enough structure to model
// relational, NoSQL, graph, vector, and cloud warehouse engines.
type InstanceMetadata struct {
	// Identity
	InstanceID string       `json:"instance_id"`          // Stable ID you assign (e.g., UUID)
	Name       string       `json:"name"`                 // Human-friendly name
	Type       DatabaseType `json:"type"`                 // One of the DatabaseType constants
	UniqueID   string       `json:"unique_id,omitempty"`  // Unique ID for the instance
	Edition    string       `json:"edition,omitempty"`    // e.g., "Community", "Enterprise", "Cloud", "Serverless"
	Version    VersionInfo  `json:"version"`              // Semantic + build info
	ClusterID  string       `json:"cluster_id,omitempty"` // Logical cluster/fleet identifier, if any
	NodeID     string       `json:"node_id,omitempty"`    // Per-node ID within a cluster
	Role       string       `json:"role,omitempty"`       // e.g., "primary", "replica", "coordinator", "data", "standalone"
	IsLeader   bool         `json:"is_leader,omitempty"`  // Whether this node is a leader/primary

	// Deployment & networking
	Deployment    DeploymentInfo    `json:"deployment"`          // Where/how it runs
	Endpoints     []NetworkEndpoint `json:"endpoints,omitempty"` // Connection endpoints (SQL/HTTP/gRPC/…
	TimeStarted   *time.Time        `json:"time_started,omitempty"`
	UptimeSeconds int64             `json:"uptime_seconds,omitempty"`

	// Security & compliance
	Security        SecurityInfo `json:"security"` // TLS/auth/at-rest encryption
	AuditingEnabled *bool        `json:"auditing_enabled,omitempty"`
	License         string       `json:"license,omitempty"` // License name or key ID (do not store secrets)

	// Storage & resources
	Storage   StorageInfo  `json:"storage"`
	Resources ResourceInfo `json:"resources"` // CPU/RAM, max connections, worker pools

	// Data topology / distribution
	Replication  ReplicationInfo  `json:"replication"`  // Physical/logical/async/sync status
	Sharding     ShardingInfo     `json:"sharding"`     // Hash/range/partitioned/etc.
	Partitioning PartitioningInfo `json:"partitioning"` // Table/collection partitioning policies

	// Defaults & localization
	Defaults DefaultsInfo `json:"defaults"` // Encoding/collation/timezone
	Timezone string       `json:"timezone,omitempty"`
	Locale   string       `json:"locale,omitempty"` // e.g., "en_US"

	// Capabilities & limits
	Capabilities CapabilityFlags `json:"capabilities"` // Feature switches inferred from engine
	Limits       LimitInfo       `json:"limits"`       // Effective limits (e.g., max dbs, max collections)

	// Logical containers present on this instance (if the engine provides them)
	SystemDatabases  []string `json:"system_databases,omitempty"`
	LogicalDatabases []string `json:"logical_databases,omitempty"` // Database names / keyspaces / namespaces

	// Extensions / plugins / components
	Plugins []string `json:"plugins,omitempty"` // e.g., Postgres extensions, MongoDB modules, Neo4j plugins

	// Free-form labels/tags and engine-specific extras
	Labels map[string]string `json:"labels,omitempty"`
	Extra  map[string]any    `json:"extra,omitempty"`
}

// DatabaseMetadata captures facts about a single logical database / keyspace / namespace.
// For engines without the concept of multiple logical databases, use a synthetic name
// (e.g., "default", "db0", or the engine’s recommended conventional name).
type DatabaseMetadata struct {
	// Identity & binding
	DatabaseID string       `json:"database_id"`         // Stable ID you assign (e.g., UUID)
	InstanceID string       `json:"instance_id"`         // Foreign key to InstanceMetadata.InstanceID
	Type       DatabaseType `json:"type"`                // One of the DatabaseType constants
	UniqueID   string       `json:"unique_id,omitempty"` // Unique ID for the logical database
	Name       string       `json:"name"`                // Database / keyspace / namespace name
	Owner      string       `json:"owner,omitempty"`     // Owner / principal where applicable
	CreatedAt  *time.Time   `json:"created_at,omitempty"`

	// Localization & defaults (override instance-level where applicable)
	Charset       string   `json:"charset,omitempty"`
	Collation     string   `json:"collation,omitempty"`
	Timezone      string   `json:"timezone,omitempty"`
	DefaultSchema string   `json:"default_schema,omitempty"` // For engines with schemas (e.g., PostgreSQL)
	Schemas       []string `json:"schemas,omitempty"`        // List of schemas / namespaces inside this logical DB

	// Size & object counts (best-effort, can be estimated)
	SizeBytes    int64        `json:"size_bytes,omitempty"`
	ObjectCounts ObjectCounts `json:"object_counts"`

	// Data topology scoped to this logical database
	Replication  ReplicationInfo  `json:"replication"`  // Publications/subscriptions, replicas, consistency
	Sharding     ShardingInfo     `json:"sharding"`     // Shard key/strategy if DB-scoped
	Partitioning PartitioningInfo `json:"partitioning"` // DB-level or default partition policies

	// Change data capture / streams
	CDC CDCInfo `json:"cdc"` // Slots/streams/connectors availability

	// Retention & lifecycle
	TTLPolicy string     `json:"ttl_policy,omitempty"` // e.g., "30d", "INF", or engine-specific JSON
	Backup    BackupInfo `json:"backup"`               // Last backup, PITR window

	// Security
	AccessControls   []PrivilegeGrant `json:"access_controls,omitempty"`
	RowLevelSecurity *bool            `json:"row_level_security,omitempty"`
	EncryptionAtRest *bool            `json:"encryption_at_rest,omitempty"`

	// Extensions / features enabled at DB scope
	Extensions []string `json:"extensions,omitempty"`

	// Free-form labels/tags and engine-specific extras
	Labels map[string]string `json:"labels,omitempty"`
	Extra  map[string]any    `json:"extra,omitempty"`
}

// --- Helper Types ---

type VersionInfo struct {
	Version   string   `json:"version"`             // e.g., "16.3.0"
	Build     string   `json:"build,omitempty"`     // Build number/hash
	Protocols []string `json:"protocols,omitempty"` // e.g., "TDS 7.4", "PostgreSQL 3.0", "MongoDB 6.0"
	Compiler  string   `json:"compiler,omitempty"`  // If exposed by engine
	OS        string   `json:"os,omitempty"`        // Underlying OS/kernel (self-hosted)
}

type DeploymentInfo struct {
	Model         string `json:"model,omitempty"`          // "self-hosted", "managed", "serverless", "edge"
	CloudProvider string `json:"cloud_provider,omitempty"` // "aws", "gcp", "azure", "onprem"
	Region        string `json:"region,omitempty"`
	Zone          string `json:"zone,omitempty"`
	Hostname      string `json:"hostname,omitempty"`
	InstanceType  string `json:"instance_type,omitempty"` // VM type / SKU if known
}

type NetworkEndpoint struct {
	Purpose    string            `json:"purpose"` // "sql", "http", "grpc", "admin", "metrics"
	Scheme     string            `json:"scheme"`  // "postgres", "mysql", "mongodb", "http", "https"
	Host       string            `json:"host"`
	Port       int               `json:"port"`
	Path       string            `json:"path,omitempty"`
	Parameters map[string]string `json:"parameters,omitempty"`
}

type SecurityInfo struct {
	TLS              TLSInfo  `json:"tls"`
	AuthMechanisms   []string `json:"auth_mechanisms,omitempty"` // e.g., "password", "kerberos", "iam", "oauth", "mTLS"
	EncryptionAtRest *bool    `json:"encryption_at_rest,omitempty"`
	KMSProvider      string   `json:"kms_provider,omitempty"` // "aws-kms", "azure-keyvault", etc.
}

type TLSInfo struct {
	Enabled           bool       `json:"enabled"`
	VersionMin        string     `json:"version_min,omitempty"` // e.g., "TLS1.2"
	CipherSuites      []string   `json:"cipher_suites,omitempty"`
	ClientAuth        string     `json:"client_auth,omitempty"` // "none", "optional", "require"
	CertificateExpiry *time.Time `json:"certificate_expiry,omitempty"`
}

type StorageInfo struct {
	Engine         string `json:"engine,omitempty"` // e.g., "innodb", "wiredTiger", "RocksDB", "Delta", "S3"
	DataPath       string `json:"data_path,omitempty"`
	WALPath        string `json:"wal_path,omitempty"`
	VolumeType     string `json:"volume_type,omitempty"` // e.g., "gp3", "premium-ssd", "local-ssd"
	AllocatedBytes int64  `json:"allocated_bytes,omitempty"`
	UsedBytes      int64  `json:"used_bytes,omitempty"`
	Compression    string `json:"compression,omitempty"` // e.g., "lz4", "zstd", "snappy"
}

type ResourceInfo struct {
	CPUCores       float64        `json:"cpu_cores,omitempty"`
	MemoryBytes    int64          `json:"memory_bytes,omitempty"`
	MaxConnections int            `json:"max_connections,omitempty"`
	WorkerPools    map[string]int `json:"worker_pools,omitempty"` // e.g., {"query": 64, "background": 8}
}

type ReplicationInfo struct {
	Mode             string   `json:"mode,omitempty"`        // "physical", "logical", "statement", "row", "mixed"
	SyncCommit       string   `json:"sync_commit,omitempty"` // "sync", "async", "semi-sync"
	LagSeconds       int64    `json:"lag_seconds,omitempty"`
	PrimaryEndpoint  string   `json:"primary_endpoint,omitempty"`
	ReplicaEndpoints []string `json:"replica_endpoints,omitempty"`
	Publications     []string `json:"publications,omitempty"`  // e.g., Postgres publications
	Subscriptions    []string `json:"subscriptions,omitempty"` // e.g., Postgres subscriptions
	Changefeed       string   `json:"changefeed,omitempty"`    // e.g., "debezium", "native", "streams"
}

type ShardingInfo struct {
	Strategy          string              `json:"strategy,omitempty"` // "hash", "range", "consistent-hash", "database-per-tenant"
	ShardCount        int                 `json:"shard_count,omitempty"`
	ShardKey          []string            `json:"shard_key,omitempty"` // Field(s)/column(s) used as shard key
	Routing           string              `json:"routing,omitempty"`   // "coordinator-router", "client-side", "proxy"
	ReplicationFactor int                 `json:"replication_factor,omitempty"`
	Topology          map[string][]string `json:"topology,omitempty"` // shard -> nodes
}

type PartitioningInfo struct {
	Enabled bool     `json:"enabled"`
	Type    string   `json:"type,omitempty"`   // "range", "list", "hash", "composite", "time"
	Key     []string `json:"key,omitempty"`    // Partition key(s)
	Policy  string   `json:"policy,omitempty"` // e.g., "daily", "monthly"
}

type DefaultsInfo struct {
	Encoding   string   `json:"encoding,omitempty"` // e.g., "UTF8"
	Collation  string   `json:"collation,omitempty"`
	Datestyle  string   `json:"datestyle,omitempty"`   // e.g., "ISO, DMY"
	SearchPath []string `json:"search_path,omitempty"` // For schema-aware engines
}

type CapabilityFlags struct {
	// General
	HasLogicalDatabases bool `json:"has_logical_databases"` // Whether engine supports multiple DBs/keyspaces
	HasSchemas          bool `json:"has_schemas"`
	HasCollections      bool `json:"has_collections"`
	HasGraphModel       bool `json:"has_graph_model"`
	HasVectorIndexing   bool `json:"has_vector_indexing"`
	HasFullTextSearch   bool `json:"has_full_text_search"`
	HasColumnarStorage  bool `json:"has_columnar_storage"`

	// Replication / CDC
	SupportsCDC         bool `json:"supports_cdc"`
	SupportsLogicalRepl bool `json:"supports_logical_replication"`
	SupportsPITR        bool `json:"supports_pitr"`

	// Security
	SupportsRowLevelSec bool `json:"supports_row_level_security"`
	SupportsTDE         bool `json:"supports_tde"` // Transparent Data Encryption

	// Transactions & consistency
	IsolationLevels   []string `json:"isolation_levels,omitempty"`   // e.g., "RC", "RR", "SERIALIZABLE"
	ConsistencyModels []string `json:"consistency_models,omitempty"` // e.g., "strong", "eventual", "bounded-staleness"`

	// Misc
	Notes string `json:"notes,omitempty"`
}

type LimitInfo struct {
	MaxDatabases        int `json:"max_databases,omitempty"`
	MaxSchemasPerDB     int `json:"max_schemas_per_db,omitempty"`
	MaxCollectionsPerDB int `json:"max_collections_per_db,omitempty"`
	MaxTablesPerSchema  int `json:"max_tables_per_schema,omitempty"`
	MaxIndexesPerTable  int `json:"max_indexes_per_table,omitempty"`
	MaxConnections      int `json:"max_connections,omitempty"`
}

type ObjectCounts struct {
	// Relational
	Schemas    int `json:"schemas,omitempty"`
	Tables     int `json:"tables,omitempty"`
	Views      int `json:"views,omitempty"`
	MatViews   int `json:"materialized_views,omitempty"`
	Indexes    int `json:"indexes,omitempty"`
	Sequences  int `json:"sequences,omitempty"`
	Functions  int `json:"functions,omitempty"`
	Procedures int `json:"procedures,omitempty"`
	Triggers   int `json:"triggers,omitempty"`
	Types      int `json:"types,omitempty"`

	// NoSQL / Document
	Collections int   `json:"collections,omitempty"`
	Documents   int64 `json:"documents,omitempty"`

	// Graph
	Graphs       int `json:"graphs,omitempty"`
	VertexLabels int `json:"vertex_labels,omitempty"`
	EdgeLabels   int `json:"edge_labels,omitempty"`

	// Vector / Indexes
	VectorIndexes int `json:"vector_indexes,omitempty"`

	// Warehouse / Files
	Stages         int `json:"stages,omitempty"`
	ExternalTables int `json:"external_tables,omitempty"`

	// Streams
	Streams int `json:"streams,omitempty"`
}

type CDCInfo struct {
	Enabled          bool   `json:"enabled"`
	Method           string `json:"method,omitempty"` // "slots", "binlog", "oplog", "stream", "debezium"
	SlotOrStreamName string `json:"slot_or_stream_name,omitempty"`
	Checkpoint       string `json:"checkpoint,omitempty"` // LSN/GTID/TSO/Timestamp
	LagSeconds       int64  `json:"lag_seconds,omitempty"`
}

type BackupInfo struct {
	LastBackupAt    *time.Time `json:"last_backup_at,omitempty"`
	PITRAvailable   bool       `json:"pitr_available"`
	PITRWindowHours int        `json:"pitr_window_hours,omitempty"`
	BackupPolicy    string     `json:"backup_policy,omitempty"`
}

type PrivilegeGrant struct {
	Principal  string `json:"principal"`             // Role/User
	Privilege  string `json:"privilege"`             // e.g., "CONNECT", "USAGE", "SELECT", "ALL"
	Scope      string `json:"scope"`                 // "database", "schema", "table", "collection"
	ObjectName string `json:"object_name,omitempty"` // Optional object qualified name
	Grantable  *bool  `json:"grantable,omitempty"`
}
