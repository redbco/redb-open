package cosmosdb

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
		return nil, adapter.WrapError(dbcapabilities.CosmosDB, "discover_schema", err)
	}
	return um, nil
}

func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	err := CreateStructure(s.conn.client, model)
	if err != nil {
		return adapter.WrapError(dbcapabilities.CosmosDB, "create_structure", err)
	}
	return nil
}

func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	// Discover schema to get list of collections (containers)
	um, err := s.DiscoverSchema(ctx)
	if err != nil {
		return nil, err
	}

	var containers []string
	for name := range um.Collections {
		containers = append(containers, name)
	}
	return containers, nil
}

func (s *SchemaOps) GetTableSchema(ctx context.Context, containerName string) (*unifiedmodel.Table, error) {
	um, err := s.DiscoverSchema(ctx)
	if err != nil {
		return nil, err
	}

	// CosmosDB uses Collections in the UnifiedModel
	collection, exists := um.Collections[containerName]
	if !exists {
		return nil, adapter.NewNotFoundError(dbcapabilities.CosmosDB, "container", containerName)
	}

	// Convert Collection to Table for interface compatibility
	table := &unifiedmodel.Table{
		Name: collection.Name,
	}
	return table, nil
}
