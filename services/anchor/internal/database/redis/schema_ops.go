package redis

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
		return nil, adapter.WrapError(dbcapabilities.Redis, "discover_schema", err)
	}
	return um, nil
}

func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	// Redis is schema-less, structures are created on-the-fly when keys are set
	return adapter.NewUnsupportedOperationError(dbcapabilities.Redis, "create structure", "Redis is schema-less")
}

func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	// Redis doesn't have tables, return key patterns or namespaces
	return []string{}, nil
}

func (s *SchemaOps) GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.Redis, "get table schema", "Redis is schema-less")
}
