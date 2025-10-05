package snowflake

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
	um, err := DiscoverSchema(s.conn.db)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Snowflake, "discover_schema", err)
	}
	return um, nil
}

func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	err := CreateStructure(s.conn.db, model)
	if err != nil {
		return adapter.WrapError(dbcapabilities.Snowflake, "create_structure", err)
	}
	return nil
}

func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	rows, err := s.conn.db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Snowflake, "list_tables", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var createdOn, name, databaseName, schemaName, kind, comment, clusterBy, rows_, bytes, owner, retentionTime string
		if err := rows.Scan(&createdOn, &name, &databaseName, &schemaName, &kind, &comment, &clusterBy, &rows_, &bytes, &owner, &retentionTime); err == nil {
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
		return nil, adapter.NewNotFoundError(dbcapabilities.Snowflake, "table", tableName)
	}
	return &table, nil
}
