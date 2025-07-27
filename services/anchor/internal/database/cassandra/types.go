package cassandra

import "github.com/redbco/redb-open/services/anchor/internal/database/common"

// CassandraDetails contains information about a Cassandra database
type CassandraDetails struct {
	UniqueIdentifier string `json:"uniqueIdentifier"`
	DatabaseType     string `json:"databaseType"`
	DatabaseEdition  string `json:"databaseEdition"`
	Version          string `json:"version"`
	DatabaseSize     int64  `json:"databaseSize"`
	Keyspaces        int    `json:"keyspaces"`
	Datacenter       string `json:"datacenter"`
	ClusterName      string `json:"clusterName"`
}

// CassandraSchema represents the schema of a Cassandra database
type CassandraSchema struct {
	Keyspaces         []KeyspaceInfo                `json:"keyspaces"`
	Tables            []common.TableInfo            `json:"tables"`
	Types             []CassandraType               `json:"types"`
	Functions         []common.FunctionInfo         `json:"functions"`
	Aggregates        []AggregateInfo               `json:"aggregates"`
	MaterializedViews []common.MaterializedViewInfo `json:"materializedViews"`
}

// KeyspaceInfo represents a Cassandra keyspace
type KeyspaceInfo struct {
	Name                string            `json:"name"`
	ReplicationStrategy string            `json:"replicationStrategy"`
	ReplicationOptions  map[string]string `json:"replicationOptions"`
	DurableWrites       bool              `json:"durableWrites"`
}

// CassandraType represents a Cassandra user-defined type
type CassandraType struct {
	Keyspace string               `json:"keyspace"`
	Name     string               `json:"name"`
	Fields   []CassandraTypeField `json:"fields"`
}

// CassandraTypeField represents a field in a Cassandra user-defined type
type CassandraTypeField struct {
	Name     string `json:"name"`
	DataType string `json:"dataType"`
}

// AggregateInfo represents a Cassandra aggregate function
type AggregateInfo struct {
	Keyspace      string   `json:"keyspace"`
	Name          string   `json:"name"`
	ArgumentTypes []string `json:"argumentTypes"`
	StateType     string   `json:"stateType"`
	FinalFunc     string   `json:"finalFunc"`
	InitCond      string   `json:"initCond"`
	ReturnType    string   `json:"returnType"`
	StateFunc     string   `json:"stateFunc"`
}

// CassandraReplicationSourceDetails contains information about a Cassandra replication source
type CassandraReplicationSourceDetails struct {
	Keyspace   string `json:"keyspace"`
	TableName  string `json:"table_name"`
	DatabaseID string `json:"database_id"`
}

// CassandraReplicationChange represents a change in Cassandra replication
type CassandraReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}
