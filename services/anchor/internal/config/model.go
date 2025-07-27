package config

import (
	"database/sql"
	"time"
)

type DatabaseObject struct {
	DatabaseID     string
	TenantID       string
	WorkspaceID    string
	DatabaseType   string
	DatabaseVendor string
	InstanceID     string
	Name           string
	Version        string
	SizeBytes      int64
	TablesCount    int
	LastUpdated    time.Time
	Created        time.Time
	Updated        time.Time
}

// InstanceMetadata represents metadata about a connected database instance
type InstanceObject struct {
	InstanceID       string
	TenantID         string
	WorkspaceID      string
	InstanceType     string
	InstanceVendor   string
	Name             string
	Version          string
	UptimeSeconds    int64
	TotalDatabases   int
	TotalConnections int
	MaxConnections   int
	UniqueIdentifier string
	LastUpdated      time.Time
	Created          time.Time
	Updated          time.Time
}

// DatabaseMetadata represents metadata about a connected database
type DatabaseMetadata struct {
	DatabaseID  string
	Version     string
	SizeBytes   int64
	TablesCount int
}

// InstanceMetadata represents metadata about a connected database instance
type InstanceMetadata struct {
	InstanceID       string
	Version          string
	UptimeSeconds    int64
	TotalDatabases   int
	TotalConnections int
	MaxConnections   int
}

// Commit represents a commit stored in PostgreSQL
type Commit struct {
	CommitID        string
	BranchID        string
	Message         string
	ParentCommitID  sql.NullString
	CommitOrder     int64
	IsHead          bool
	SchemaType      string
	SchemaStructure string
	Created         time.Time
	Updated         time.Time
}
