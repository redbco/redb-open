package clickhouse

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
	um, err := DiscoverSchema(s.conn.conn)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.ClickHouse, "discover_schema", err)
	}
	return um, nil
}

func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	err := CreateStructure(s.conn.conn, model)
	if err != nil {
		return adapter.WrapError(dbcapabilities.ClickHouse, "create_structure", err)
	}
	return nil
}

func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	rows, err := s.conn.conn.Query(ctx, "SHOW TABLES")
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.ClickHouse, "list_tables", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			tables = append(tables, name)
		}
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
		return nil, adapter.NewNotFoundError(dbcapabilities.ClickHouse, "table", tableName)
	}
	return &table, nil
}
