package influxdb

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements schema operations for InfluxDB.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema retrieves the "schema" of InfluxDB (measurements and fields).
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	bucket := s.conn.client.GetBucket()
	if bucket == "" {
		return nil, fmt.Errorf("no bucket specified")
	}

	// Query to get measurements
	query := fmt.Sprintf(`
		import "influxdata/influxdb/schema"
		schema.measurements(bucket: "%s")
	`, bucket)

	queryAPI := s.conn.client.GetQueryAPI()
	result, err := queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to discover schema: %w", err)
	}
	defer result.Close()

	tables := make([]*unifiedmodel.Table, 0)
	measurements := make(map[string]bool)

	for result.Next() {
		if measurement, ok := result.Record().ValueByKey("_value").(string); ok {
			if !measurements[measurement] {
				measurements[measurement] = true

				// Create columns map for this measurement
				columnsMap := map[string]unifiedmodel.Column{
					"_time":        {Name: "_time", DataType: "timestamp", Nullable: false},
					"_measurement": {Name: "_measurement", DataType: "string", Nullable: false},
					"_field":       {Name: "_field", DataType: "string", Nullable: false},
					"_value":       {Name: "_value", DataType: "float", Nullable: true},
				}

				// For each measurement, create a table
				table := &unifiedmodel.Table{
					Name:    measurement,
					Columns: columnsMap,
				}
				tables = append(tables, table)
			}
		}
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("query error: %w", result.Err())
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

// CreateStructure creates InfluxDB "structure" (measurements).
// Note: InfluxDB creates measurements implicitly when data is written.
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	// InfluxDB doesn't require explicit schema creation
	return nil
}

// ListTables lists all "tables" (measurements) in the bucket.
func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	bucket := s.conn.client.GetBucket()
	if bucket == "" {
		return nil, fmt.Errorf("no bucket specified")
	}

	query := fmt.Sprintf(`
		import "influxdata/influxdb/schema"
		schema.measurements(bucket: "%s")
	`, bucket)

	queryAPI := s.conn.client.GetQueryAPI()
	result, err := queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list measurements: %w", err)
	}
	defer result.Close()

	measurements := make([]string, 0)
	seen := make(map[string]bool)

	for result.Next() {
		if measurement, ok := result.Record().ValueByKey("_value").(string); ok {
			if !seen[measurement] {
				seen[measurement] = true
				measurements = append(measurements, measurement)
			}
		}
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("query error: %w", result.Err())
	}

	return measurements, nil
}

// GetTableSchema retrieves the schema for a specific "table" (measurement).
func (s *SchemaOps) GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error) {
	bucket := s.conn.client.GetBucket()
	if bucket == "" {
		return nil, fmt.Errorf("no bucket specified")
	}

	// Query to get fields for this measurement
	query := fmt.Sprintf(`
		import "influxdata/influxdb/schema"
		schema.measurementFieldKeys(
			bucket: "%s",
			measurement: "%s"
		)
	`, bucket, tableName)

	queryAPI := s.conn.client.GetQueryAPI()
	result, err := queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get measurement schema: %w", err)
	}
	defer result.Close()

	columnsMap := map[string]unifiedmodel.Column{
		"_time":        {Name: "_time", DataType: "timestamp", Nullable: false},
		"_measurement": {Name: "_measurement", DataType: "string", Nullable: false},
	}

	for result.Next() {
		if fieldName, ok := result.Record().ValueByKey("_value").(string); ok {
			columnsMap[fieldName] = unifiedmodel.Column{
				Name:     fieldName,
				DataType: "float", // InfluxDB fields are typically numeric
				Nullable: true,
			}
		}
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("query error: %w", result.Err())
	}

	table := &unifiedmodel.Table{
		Name:    tableName,
		Columns: columnsMap,
	}

	return table, nil
}
