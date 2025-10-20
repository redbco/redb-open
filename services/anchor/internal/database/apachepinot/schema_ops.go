package apachepinot

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements schema operations for Pinot.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema retrieves the schema of Pinot (tables).
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	// Get all tables
	tables, err := s.conn.client.ListTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}

	tablesMap := make(map[string]unifiedmodel.Table)

	// Get schema for each table
	for _, tableName := range tables {
		schema, err := s.conn.client.GetTableSchema(ctx, tableName)
		if err != nil {
			// Log error but continue with other tables
			continue
		}

		// Build columns map
		columnsMap := make(map[string]unifiedmodel.Column)

		// Add dimension fields
		for _, field := range schema.DimensionFieldSpecs {
			columnsMap[field.Name] = unifiedmodel.Column{
				Name:     field.Name,
				DataType: field.DataType,
				Nullable: true,
			}
		}

		// Add metric fields
		for _, field := range schema.MetricFieldSpecs {
			columnsMap[field.Name] = unifiedmodel.Column{
				Name:     field.Name,
				DataType: field.DataType,
				Nullable: true,
			}
		}

		// Add datetime fields
		for _, field := range schema.DateTimeFieldSpecs {
			columnsMap[field.Name] = unifiedmodel.Column{
				Name:     field.Name,
				DataType: field.DataType,
				Nullable: false,
			}
		}

		table := unifiedmodel.Table{
			Name:    tableName,
			Columns: columnsMap,
			Options: map[string]any{
				"table_type": "pinot",
			},
		}

		tablesMap[tableName] = table
	}

	model := &unifiedmodel.UnifiedModel{
		DatabaseType: s.conn.Type(),
		Tables:       tablesMap,
	}

	return model, nil
}

// CreateStructure is not supported (use table configs and schemas).
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	return fmt.Errorf("CreateStructure not supported for Pinot (use schema and table config definitions)")
}

// ListTables lists all tables.
func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	tables, err := s.conn.client.ListTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}

	return tables, nil
}

// GetTableSchema retrieves the schema for a specific table.
func (s *SchemaOps) GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error) {
	schema, err := s.conn.client.GetTableSchema(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %w", err)
	}

	// Build columns map
	columnsMap := make(map[string]unifiedmodel.Column)

	// Add dimension fields
	for _, field := range schema.DimensionFieldSpecs {
		columnsMap[field.Name] = unifiedmodel.Column{
			Name:     field.Name,
			DataType: field.DataType,
			Nullable: true,
		}
	}

	// Add metric fields
	for _, field := range schema.MetricFieldSpecs {
		columnsMap[field.Name] = unifiedmodel.Column{
			Name:     field.Name,
			DataType: field.DataType,
			Nullable: true,
		}
	}

	// Add datetime fields
	for _, field := range schema.DateTimeFieldSpecs {
		columnsMap[field.Name] = unifiedmodel.Column{
			Name:     field.Name,
			DataType: field.DataType,
			Nullable: false,
		}
	}

	table := &unifiedmodel.Table{
		Name:    tableName,
		Columns: columnsMap,
		Options: map[string]any{
			"table_type":  "pinot",
			"schema_name": schema.SchemaName,
		},
	}

	return table, nil
}
