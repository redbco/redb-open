package dynamodb

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
	um, err := DiscoverSchema(s.conn.client)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.DynamoDB, "discover_schema", err)
	}
	return um, nil
}

func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	err := CreateStructure(s.conn.client, model)
	if err != nil {
		return adapter.WrapError(dbcapabilities.DynamoDB, "create_structure", err)
	}
	return nil
}

func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	result, err := s.conn.client.ListTables(ctx, nil)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.DynamoDB, "list_tables", err)
	}
	return result.TableNames, nil
}

func (s *SchemaOps) GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error) {
	um, err := s.DiscoverSchema(ctx)
	if err != nil {
		return nil, err
	}

	table, exists := um.Tables[tableName]
	if !exists {
		return nil, adapter.NewNotFoundError(dbcapabilities.DynamoDB, "table", tableName)
	}
	return &table, nil
}
