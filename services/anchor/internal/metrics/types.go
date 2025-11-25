package metrics

import (
	"time"
)

// InstanceMetrics represents performance metrics collected from a database instance
type InstanceMetrics struct {
	InstanceID  string
	CollectedAt time.Time

	// Connection metrics
	ActiveConnections     *int32
	IdleConnections       *int32
	ConnectionUtilization *float64 // percentage

	// Performance metrics
	QueriesPerSecond      *float64
	TransactionsPerSecond *float64
	CacheHitRatio         *float64 // percentage

	// Resource metrics
	CPUUsage         *float64 // percentage (if available)
	MemoryUsageBytes *int64
	MemoryTotalBytes *int64
	DiskUsageBytes   *int64
	DiskTotalBytes   *int64

	// Latency metrics
	AvgQueryTimeMs *float64
	SlowQueryCount *int32

	// Replication metrics (for replicated instances)
	ReplicationLagSeconds *float64
	IsReplica             *bool

	// Database-specific metrics (stored as JSONB)
	ExtendedMetrics map[string]interface{}
}

// LogicalDatabaseInfo represents metadata about a logical database within an instance
type LogicalDatabaseInfo struct {
	Name      string
	SizeBytes int64
	Owner     string
	Encoding  string
	Collation string
	CreatedAt *time.Time
}

// EnhancedInstanceMetadata represents extended metadata about a database instance
type EnhancedInstanceMetadata struct {
	// Basic metadata (already collected)
	Version          string
	UptimeSeconds    int64
	MaxConnections   int32
	TotalDatabases   int32
	TotalConnections int32

	// New enhanced metadata
	Edition          string                `json:"edition,omitempty"`      // Community/Enterprise/Cloud
	Platform         string                `json:"platform,omitempty"`     // linux/darwin/windows
	Architecture     string                `json:"architecture,omitempty"` // x86_64/arm64
	ClusterInfo      *ClusterInfo          `json:"cluster_info,omitempty"`
	LogicalDatabases []LogicalDatabaseInfo `json:"logical_databases,omitempty"`
	ServerSettings   map[string]string     `json:"server_settings,omitempty"`
	FeaturesEnabled  []string              `json:"features_enabled,omitempty"`
	SSLEnabled       bool                  `json:"ssl_enabled"`
	AuthMethods      []string              `json:"authentication_methods,omitempty"`
}

// ClusterInfo contains information about cluster configuration
type ClusterInfo struct {
	IsClustered bool   `json:"is_clustered"`
	ClusterName string `json:"cluster_name,omitempty"`
	NodeRole    string `json:"node_role"` // primary/replica/standalone
}

// MetricsCollector is the interface for database-specific metrics collectors
type MetricsCollector interface {
	// CollectMetrics collects current performance metrics
	CollectMetrics() (*InstanceMetrics, error)

	// CollectEnhancedMetadata collects extended instance metadata
	CollectEnhancedMetadata() (*EnhancedInstanceMetadata, error)

	// GetDatabaseType returns the type of database this collector handles
	GetDatabaseType() string
}
