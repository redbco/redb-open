package cassandra

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

type SchemaOps struct {
	conn *Connection
}

func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	um, err := DiscoverSchema(s.conn.session)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Cassandra, "discover_schema", err)
	}
	return um, nil
}

func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	err := CreateStructure(s.conn.session, model)
	if err != nil {
		return adapter.WrapError(dbcapabilities.Cassandra, "create_structure", err)
	}
	return nil
}

func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	// Get current keyspace
	keyspace := s.conn.session.Query("SELECT keyspace_name FROM system.local").String()

	var tables []string
	iter := s.conn.session.Query("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?", keyspace).WithContext(ctx).Iter()

	var tableName string
	for iter.Scan(&tableName) {
		tables = append(tables, tableName)
	}

	if err := iter.Close(); err != nil {
		return nil, adapter.WrapError(dbcapabilities.Cassandra, "list_tables", err)
	}
	return tables, nil
}

func (s *SchemaOps) GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error) {
	um, err := s.DiscoverSchema(ctx)
	if err != nil {
		return nil, err
	}

	table, exists := um.Tables[tableName]
	if !exists {
		return nil, adapter.NewNotFoundError(dbcapabilities.Cassandra, "table", tableName)
	}
	return &table, nil
}
