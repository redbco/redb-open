package bigquery

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"google.golang.org/api/iterator"
)

// SchemaOps implements schema operations for BigQuery.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema retrieves the schema of BigQuery dataset (tables and their schemas).
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	datasetID := s.conn.client.GetDatasetID()
	if datasetID == "" {
		return nil, fmt.Errorf("no dataset specified")
	}

	dataset := s.conn.client.GetDataset()
	it := dataset.Tables(ctx)

	tables := make([]*unifiedmodel.Table, 0)

	for {
		table, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list tables: %w", err)
		}

		// Get table metadata
		metadata, err := table.Metadata(ctx)
		if err != nil {
			continue // Skip tables we can't read
		}

		umTable, err := s.convertBigQueryTableToUnified(table.TableID, metadata)
		if err != nil {
			continue
		}

		tables = append(tables, umTable)
	}

	// Convert tables slice to map
	tablesMap := make(map[string]unifiedmodel.Table)
	for _, t := range tables {
		tablesMap[t.Name] = *t
	}

	model := &unifiedmodel.UnifiedModel{
		DatabaseType: s.conn.Type(),
		Tables:       tablesMap,
	}

	return model, nil
}

// convertBigQueryTableToUnified converts BigQuery table metadata to UnifiedModel table.
func (s *SchemaOps) convertBigQueryTableToUnified(tableID string, metadata *bigquery.TableMetadata) (*unifiedmodel.Table, error) {
	columnsMap := make(map[string]unifiedmodel.Column)

	for _, field := range metadata.Schema {
		column := unifiedmodel.Column{
			Name:     field.Name,
			DataType: s.mapBigQueryType(field.Type),
			Nullable: !field.Required,
		}
		columnsMap[field.Name] = column
	}

	table := &unifiedmodel.Table{
		Name:    tableID,
		Columns: columnsMap,
	}

	return table, nil
}

// mapBigQueryType maps BigQuery field types to unified data types.
func (s *SchemaOps) mapBigQueryType(fieldType bigquery.FieldType) string {
	switch fieldType {
	case bigquery.StringFieldType:
		return "string"
	case bigquery.BytesFieldType:
		return "bytes"
	case bigquery.IntegerFieldType:
		return "integer"
	case bigquery.FloatFieldType:
		return "float"
	case bigquery.BooleanFieldType:
		return "boolean"
	case bigquery.TimestampFieldType:
		return "timestamp"
	case bigquery.DateFieldType:
		return "date"
	case bigquery.TimeFieldType:
		return "time"
	case bigquery.DateTimeFieldType:
		return "datetime"
	case bigquery.RecordFieldType:
		return "record"
	case bigquery.NumericFieldType:
		return "numeric"
	case bigquery.BigNumericFieldType:
		return "bignumeric"
	case bigquery.GeographyFieldType:
		return "geography"
	case bigquery.JSONFieldType:
		return "json"
	default:
		return "string"
	}
}

// CreateStructure creates BigQuery structure from a UnifiedModel.
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	dataset := s.conn.client.GetDataset()

	for tableName, table := range model.Tables {
		// Build BigQuery schema from UnifiedModel table
		schemaFields := make(bigquery.Schema, 0, len(table.Columns))

		for _, col := range table.Columns {
			field := &bigquery.FieldSchema{
				Name:     col.Name,
				Type:     s.mapUnifiedTypeToBigQuery(col.DataType),
				Required: !col.Nullable,
			}
			schemaFields = append(schemaFields, field)
		}

		// Create table
		tableRef := dataset.Table(tableName)
		err := tableRef.Create(ctx, &bigquery.TableMetadata{
			Schema: schemaFields,
		})
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}
	}

	return nil
}

// mapUnifiedTypeToBigQuery maps unified data types to BigQuery field types.
func (s *SchemaOps) mapUnifiedTypeToBigQuery(dataType string) bigquery.FieldType {
	switch dataType {
	case "string", "text", "varchar":
		return bigquery.StringFieldType
	case "integer", "int", "bigint":
		return bigquery.IntegerFieldType
	case "float", "double", "real":
		return bigquery.FloatFieldType
	case "boolean", "bool":
		return bigquery.BooleanFieldType
	case "timestamp":
		return bigquery.TimestampFieldType
	case "date":
		return bigquery.DateFieldType
	case "time":
		return bigquery.TimeFieldType
	case "datetime":
		return bigquery.DateTimeFieldType
	case "bytes", "binary":
		return bigquery.BytesFieldType
	case "numeric", "decimal":
		return bigquery.NumericFieldType
	case "json":
		return bigquery.JSONFieldType
	default:
		return bigquery.StringFieldType
	}
}

// ListTables lists all tables in the dataset.
func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	datasetID := s.conn.client.GetDatasetID()
	if datasetID == "" {
		return nil, fmt.Errorf("no dataset specified")
	}

	dataset := s.conn.client.GetDataset()
	it := dataset.Tables(ctx)

	tables := make([]string, 0)

	for {
		table, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list tables: %w", err)
		}
		tables = append(tables, table.TableID)
	}

	return tables, nil
}

// GetTableSchema retrieves the schema for a specific table.
func (s *SchemaOps) GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error) {
	dataset := s.conn.client.GetDataset()
	table := dataset.Table(tableName)

	metadata, err := table.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}

	umTable, err := s.convertBigQueryTableToUnified(tableName, metadata)
	if err != nil {
		return nil, err
	}

	return umTable, nil
}
