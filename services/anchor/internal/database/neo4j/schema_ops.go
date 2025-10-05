package neo4j

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
	um, err := DiscoverSchema(s.conn.driver)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Neo4j, "discover_schema", err)
	}
	return um, nil
}

func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	err := CreateStructure(s.conn.driver, model)
	if err != nil {
		return adapter.WrapError(dbcapabilities.Neo4j, "create_structure", err)
	}
	return nil
}

func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	// In Neo4j, "tables" are node labels - discovery handles this
	um, err := s.DiscoverSchema(ctx)
	if err != nil {
		return nil, err
	}

	var labels []string
	for label := range um.Tables {
		labels = append(labels, label)
	}
	return labels, nil
}

func (s *SchemaOps) GetTableSchema(ctx context.Context, labelName string) (*unifiedmodel.Table, error) {
	um, err := s.DiscoverSchema(ctx)
	if err != nil {
		return nil, err
	}

	table, exists := um.Tables[labelName]
	if !exists {
		return nil, adapter.NewNotFoundError(dbcapabilities.Neo4j, "label", labelName)
	}
	return &table, nil
}
