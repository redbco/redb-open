package cassandra

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CreateCassandraUnifiedModel creates a UnifiedModel for Cassandra with database details
func CreateCassandraUnifiedModel(uniqueIdentifier, version string, databaseSize int64) *unifiedmodel.UnifiedModel {
	um := &unifiedmodel.UnifiedModel{
		DatabaseType:      dbcapabilities.Cassandra,
		Keyspaces:         make(map[string]unifiedmodel.Keyspace),
		Tables:            make(map[string]unifiedmodel.Table),
		Types:             make(map[string]unifiedmodel.Type),
		Functions:         make(map[string]unifiedmodel.Function),
		MaterializedViews: make(map[string]unifiedmodel.MaterializedView),
	}
	return um
}

// ConvertCassandraKeyspace converts KeyspaceInfo to unifiedmodel.Keyspace
func ConvertCassandraKeyspace(keyspaceInfo KeyspaceInfo) unifiedmodel.Keyspace {
	return unifiedmodel.Keyspace{
		Name:                keyspaceInfo.Name,
		ReplicationStrategy: keyspaceInfo.ReplicationStrategy,
		ReplicationOptions:  keyspaceInfo.ReplicationOptions,
		DurableWrites:       keyspaceInfo.DurableWrites,
	}
}

// ConvertCassandraTable converts common.TableInfo to unifiedmodel.Table for Cassandra
func ConvertCassandraTable(tableInfo common.TableInfo) unifiedmodel.Table {
	table := unifiedmodel.Table{
		Name:        tableInfo.Name,
		Comment:     tableInfo.Schema, // In Cassandra, store keyspace in comment for reference
		Columns:     make(map[string]unifiedmodel.Column),
		Indexes:     make(map[string]unifiedmodel.Index),
		Constraints: make(map[string]unifiedmodel.Constraint),
	}

	// Convert columns
	for _, col := range tableInfo.Columns {
		var defaultValue string
		if col.ColumnDefault != nil {
			defaultValue = *col.ColumnDefault
		}
		table.Columns[col.Name] = unifiedmodel.Column{
			Name:         col.Name,
			DataType:     col.DataType,
			Nullable:     col.IsNullable,
			Default:      defaultValue,
			IsPrimaryKey: col.IsPrimaryKey,
		}
	}

	// Convert indexes
	for _, idx := range tableInfo.Indexes {
		table.Indexes[idx.Name] = unifiedmodel.Index{
			Name:    idx.Name,
			Columns: idx.Columns,
			Unique:  idx.IsUnique,
		}
	}

	return table
}

// ConvertCassandraType converts CassandraType to unifiedmodel.Type
func ConvertCassandraType(cassType CassandraType) unifiedmodel.Type {
	fields := make(map[string]unifiedmodel.Property)
	for _, field := range cassType.Fields {
		fields[field.Name] = unifiedmodel.Property{
			Name: field.Name,
			Type: field.DataType,
		}
	}

	return unifiedmodel.Type{
		Name:     cassType.Name,
		Category: "user_defined",
		Definition: map[string]any{
			"keyspace": cassType.Keyspace,
			"fields":   fields,
		},
	}
}

// ConvertCassandraAggregate converts AggregateInfo to unifiedmodel.Function
func ConvertCassandraAggregate(aggregateInfo AggregateInfo) unifiedmodel.Function {
	return unifiedmodel.Function{
		Name:     aggregateInfo.Name,
		Language: "cql", // Cassandra uses CQL
		Returns:  aggregateInfo.ReturnType,
		Definition: fmt.Sprintf("AGGREGATE %s(%s) SFUNC %s STYPE %s FINALFUNC %s INITCOND %s",
			aggregateInfo.Name,
			strings.Join(aggregateInfo.ArgumentTypes, ", "),
			aggregateInfo.StateFunc,
			aggregateInfo.StateType,
			aggregateInfo.FinalFunc,
			aggregateInfo.InitCond),
	}
}

// ConvertCassandraMaterializedView converts common.MaterializedViewInfo to unifiedmodel.MaterializedView
func ConvertCassandraMaterializedView(mvInfo common.MaterializedViewInfo) unifiedmodel.MaterializedView {
	return unifiedmodel.MaterializedView{
		Name:       mvInfo.Name,
		Definition: mvInfo.Definition,
		// Note: Cassandra materialized views don't have direct base table reference in UnifiedModel
		// The keyspace and base table info is embedded in the definition
	}
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
