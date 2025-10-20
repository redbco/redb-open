package druid

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements schema operations for Druid.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema retrieves the schema of Druid (datasources as tables).
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	// Get all datasources
	datasources, err := s.conn.client.ListDatasources(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list datasources: %w", err)
	}

	tablesMap := make(map[string]unifiedmodel.Table)

	// Get schema for each datasource
	for _, datasource := range datasources {
		schema, err := s.conn.client.GetDatasourceSchema(ctx, datasource)
		if err != nil {
			// Log error but continue with other datasources
			continue
		}

		// Build columns map
		columnsMap := map[string]unifiedmodel.Column{
			"__time": {Name: "__time", DataType: "timestamp", Nullable: false},
		}

		// Add dimensions
		for _, dim := range schema.Dimensions {
			columnsMap[dim] = unifiedmodel.Column{
				Name:     dim,
				DataType: "string",
				Nullable: true,
			}
		}

		// Add metrics
		for _, metric := range schema.Metrics {
			columnsMap[metric] = unifiedmodel.Column{
				Name:     metric,
				DataType: "double",
				Nullable: true,
			}
		}

		table := unifiedmodel.Table{
			Name:    datasource,
			Columns: columnsMap,
			Options: map[string]any{
				"datasource_type": "druid",
			},
		}

		tablesMap[datasource] = table
	}

	model := &unifiedmodel.UnifiedModel{
		DatabaseType: s.conn.Type(),
		Tables:       tablesMap,
	}

	return model, nil
}

// CreateStructure is not supported (use ingestion specs).
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	return fmt.Errorf("CreateStructure not supported for Druid (use ingestion specs)")
}

// ListTables lists all datasources (treated as tables).
func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	datasources, err := s.conn.client.ListDatasources(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list datasources: %w", err)
	}

	return datasources, nil
}

// GetTableSchema retrieves the schema for a specific datasource.
func (s *SchemaOps) GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error) {
	schema, err := s.conn.client.GetDatasourceSchema(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get datasource schema: %w", err)
	}

	// Build columns map
	columnsMap := map[string]unifiedmodel.Column{
		"__time": {Name: "__time", DataType: "timestamp", Nullable: false},
	}

	// Add dimensions
	for _, dim := range schema.Dimensions {
		columnsMap[dim] = unifiedmodel.Column{
			Name:     dim,
			DataType: "string",
			Nullable: true,
		}
	}

	// Add metrics
	for _, metric := range schema.Metrics {
		columnsMap[metric] = unifiedmodel.Column{
			Name:     metric,
			DataType: "double",
			Nullable: true,
		}
	}

	table := &unifiedmodel.Table{
		Name:    tableName,
		Columns: columnsMap,
		Options: map[string]any{
			"datasource_type": "druid",
		},
	}

	return table, nil
}
