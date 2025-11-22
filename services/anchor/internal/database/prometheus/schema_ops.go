package prometheus

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements schema operations for Prometheus.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema retrieves the "schema" of Prometheus (metrics as tables).
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	// Get all metric names
	metrics, err := s.conn.client.GetSeriesNames(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get metrics: %w", err)
	}

	// Get all label names
	labels, err := s.conn.client.GetLabels(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get labels: %w", err)
	}

	// Create a unified model where each metric is a "table"
	tablesMap := make(map[string]unifiedmodel.Table)

	// Standard columns for time series data
	baseColumns := map[string]unifiedmodel.Column{
		"timestamp": {Name: "timestamp", DataType: "timestamp", Nullable: false},
		"value":     {Name: "value", DataType: "float", Nullable: false},
	}

	// Add label columns
	for _, label := range labels {
		baseColumns[label] = unifiedmodel.Column{
			Name:     label,
			DataType: "string",
			Nullable: true,
		}
	}

	// Create a table for each metric
	timeSeriesMap := make(map[string]unifiedmodel.TimeSeriesPoint)

	for _, metric := range metrics {
		table := unifiedmodel.Table{
			Name:    metric,
			Columns: baseColumns,
			Options: map[string]any{
				"metric_type": "time_series",
			},
		}

		tablesMap[metric] = table

		// Also create TimeSeriesPoint representation (primary container for time-series databases)
		fields := make(map[string]unifiedmodel.Field)
		fields["value"] = unifiedmodel.Field{
			Name:     "value",
			Type:     "float",
			Required: true,
		}

		// Add label fields
		for _, label := range labels {
			fields[label] = unifiedmodel.Field{
				Name:     label,
				Type:     "string",
				Required: false,
			}
		}

		tsPoint := unifiedmodel.TimeSeriesPoint{
			Name:        metric,
			Fields:      fields,
			Aggregation: "raw",
			Retention:   "",   // Prometheus retention is configured globally
			Precision:   "ms", // Prometheus uses millisecond precision
			Options: map[string]any{
				"metric_type": "time_series",
				"measurement": metric,
			},
		}
		timeSeriesMap[metric] = tsPoint
	}

	model := &unifiedmodel.UnifiedModel{
		DatabaseType:     s.conn.Type(),
		Tables:           tablesMap,
		TimeSeriesPoints: timeSeriesMap,
	}

	return model, nil
}

// CreateStructure is not supported for Prometheus (read-only for metrics).
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	return fmt.Errorf("CreateStructure not supported for Prometheus (metrics are created via scraping/remote write)")
}

// ListTables lists all metrics (treated as tables).
func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	metrics, err := s.conn.client.GetSeriesNames(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list metrics: %w", err)
	}

	return metrics, nil
}

// GetTableSchema retrieves the schema for a specific metric (table).
func (s *SchemaOps) GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error) {
	// Get labels for this metric
	labels, err := s.conn.client.GetLabels(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get labels: %w", err)
	}

	// Build columns map
	columnsMap := map[string]unifiedmodel.Column{
		"timestamp": {Name: "timestamp", DataType: "timestamp", Nullable: false},
		"value":     {Name: "value", DataType: "float", Nullable: false},
	}

	for _, label := range labels {
		columnsMap[label] = unifiedmodel.Column{
			Name:     label,
			DataType: "string",
			Nullable: true,
		}
	}

	// Get metric metadata
	metadata, err := s.conn.client.GetMetricMetadata(ctx, tableName)
	if err == nil && len(metadata) > 0 {
		columnsMap["__meta_type__"] = unifiedmodel.Column{
			Name:     "__meta_type__",
			DataType: "string",
			Nullable: true,
		}
		columnsMap["__meta_help__"] = unifiedmodel.Column{
			Name:     "__meta_help__",
			DataType: "string",
			Nullable: true,
		}
	}

	table := &unifiedmodel.Table{
		Name:    tableName,
		Columns: columnsMap,
		Options: map[string]any{
			"metric_type": "time_series",
		},
	}

	// Add metadata if available
	if len(metadata) > 0 {
		table.Options["prometheus_type"] = metadata[0].Type
		table.Options["prometheus_help"] = metadata[0].Help
		if metadata[0].Unit != "" {
			table.Options["prometheus_unit"] = metadata[0].Unit
		}
	}

	return table, nil
}
