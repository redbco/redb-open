package mongodb

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements adapter.SchemaOperator for MongoDB.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema retrieves the complete schema of the database as a UnifiedModel.
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	// Use existing DiscoverSchema function
	um, err := DiscoverSchema(s.conn.db)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MongoDB, "discover_schema", err)
	}
	return um, nil
}

// CreateStructure creates database objects from a UnifiedModel.
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	// Use existing CreateStructure function
	err := CreateStructure(s.conn.db, model)
	if err != nil {
		return adapter.WrapError(dbcapabilities.MongoDB, "create_structure", err)
	}
	return nil
}

// ListTables returns the names of all collections in the database.
func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	collections, err := s.conn.db.ListCollectionNames(ctx, map[string]interface{}{})
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MongoDB, "list_tables", err)
	}
	return collections, nil
}

// GetTableSchema retrieves the schema for a specific collection.
func (s *SchemaOps) GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error) {
	// Discover full schema and extract the requested collection
	um, err := s.DiscoverSchema(ctx)
	if err != nil {
		return nil, err
	}

	// Find the collection in the unified model
	for _, collection := range um.Collections {
		if collection.Name == tableName {
			// Convert Collection to Table format for interface compatibility
			table := &unifiedmodel.Table{
				Name: collection.Name,
				// Note: MongoDB collections are schema-less, so columns are inferred
			}
			return table, nil
		}
	}

	return nil, adapter.NewNotFoundError(dbcapabilities.MongoDB, "collection", tableName)
}
