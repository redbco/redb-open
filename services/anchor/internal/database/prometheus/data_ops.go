package prometheus

import (
	"context"
	"fmt"
	"time"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// DataOps implements data operations for Prometheus.
type DataOps struct {
	conn *Connection
}

// Fetch retrieves time series data for a metric.
func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
	// Query the last hour of data for the metric
	end := time.Now()
	start := end.Add(-1 * time.Hour)

	// Build a PromQL query
	query := table

	result, err := d.conn.client.QueryRange(ctx, query, start, end, 1*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch data: %w", err)
	}

	return d.parseQueryResult(result, limit)
}

// FetchWithColumns retrieves specific columns (labels) from a metric.
func (d *DataOps) FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error) {
	// For Prometheus, we'll fetch all data and filter columns
	rows, err := d.Fetch(ctx, table, limit)
	if err != nil {
		return nil, err
	}

	if len(columns) == 0 {
		return rows, nil
	}

	// Filter to requested columns
	filtered := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		filteredRow := make(map[string]interface{})
		for _, col := range columns {
			if val, exists := row[col]; exists {
				filteredRow[col] = val
			}
		}
		filtered[i] = filteredRow
	}

	return filtered, nil
}

// Insert is not directly supported (use remote_write or federation).
func (d *DataOps) Insert(ctx context.Context, table string, data []map[string]interface{}) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(
		dbcapabilities.Prometheus,
		"insert",
		"Use Prometheus remote_write API or push gateway instead",
	)
}

// Update is not supported for Prometheus.
func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(
		dbcapabilities.Prometheus,
		"update",
		"Prometheus is an append-only time series database",
	)
}

// Upsert is not supported for Prometheus.
func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(
		dbcapabilities.Prometheus,
		"upsert",
		"Prometheus is an append-only time series database",
	)
}

// Delete is not supported for Prometheus (except admin API).
func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(
		dbcapabilities.Prometheus,
		"delete",
		"Prometheus does not support data deletion via standard API",
	)
}

// Stream retrieves time series data in batches.
func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	// For streaming, we'll query data in time chunks
	end := time.Now()
	start := end.Add(-1 * time.Hour)

	query := params.Table

	result, err := d.conn.client.QueryRange(ctx, query, start, end, 1*time.Minute)
	if err != nil {
		return adapter.StreamResult{}, fmt.Errorf("failed to stream data: %w", err)
	}

	rows, err := d.parseQueryResult(result, int(params.BatchSize))
	if err != nil {
		return adapter.StreamResult{}, err
	}

	// For simplicity, assume hasMore is false (would need cursor logic for true streaming)
	return adapter.StreamResult{
		Data:       rows,
		HasMore:    false,
		NextCursor: "",
	}, nil
}

// ExecuteQuery executes a PromQL query.
func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	result, err := d.conn.client.Query(ctx, query, time.Time{})
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	rows, err := d.parseQueryResult(result, 1000)
	if err != nil {
		return nil, err
	}

	// Convert to []interface{}
	results := make([]interface{}, len(rows))
	for i, row := range rows {
		results[i] = row
	}

	return results, nil
}

// ExecuteCountQuery counts the number of series for a metric.
func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	result, err := d.conn.client.Query(ctx, query, time.Time{})
	if err != nil {
		return 0, fmt.Errorf("failed to execute count query: %w", err)
	}

	if result.Data.ResultType == "vector" {
		return int64(len(result.Data.Result)), nil
	}

	return 0, nil
}

// GetRowCount returns the number of series for a metric.
func (d *DataOps) GetRowCount(ctx context.Context, table string, whereClause string) (int64, bool, error) {
	query := table
	if whereClause != "" {
		query = fmt.Sprintf("%s{%s}", table, whereClause)
	}

	count, err := d.ExecuteCountQuery(ctx, query)
	if err != nil {
		return 0, false, err
	}

	return count, true, nil
}

// Wipe is not supported for Prometheus.
func (d *DataOps) Wipe(ctx context.Context) error {
	return adapter.NewUnsupportedOperationError(
		dbcapabilities.Prometheus,
		"wipe",
		"Prometheus does not support bulk data deletion",
	)
}

// parseQueryResult parses Prometheus query results into rows.
func (d *DataOps) parseQueryResult(result *QueryResult, limit int) ([]map[string]interface{}, error) {
	var rows []map[string]interface{}

	for _, item := range result.Data.Result {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		// Parse metric labels
		metric, ok := itemMap["metric"].(map[string]interface{})
		if !ok {
			continue
		}

		// Parse values
		if result.Data.ResultType == "matrix" {
			// Range query - multiple values
			values, ok := itemMap["values"].([]interface{})
			if !ok {
				continue
			}

			for _, val := range values {
				valArray, ok := val.([]interface{})
				if !ok || len(valArray) < 2 {
					continue
				}

				row := make(map[string]interface{})

				// Add timestamp
				if timestamp, ok := valArray[0].(float64); ok {
					row["timestamp"] = time.Unix(int64(timestamp), 0)
				}

				// Add value
				row["value"] = valArray[1]

				// Add labels
				for label, labelVal := range metric {
					row[label] = labelVal
				}

				rows = append(rows, row)

				if len(rows) >= limit {
					return rows, nil
				}
			}
		} else if result.Data.ResultType == "vector" {
			// Instant query - single value
			value, ok := itemMap["value"].([]interface{})
			if !ok || len(value) < 2 {
				continue
			}

			row := make(map[string]interface{})

			// Add timestamp
			if timestamp, ok := value[0].(float64); ok {
				row["timestamp"] = time.Unix(int64(timestamp), 0)
			}

			// Add value
			row["value"] = value[1]

			// Add labels
			for label, labelVal := range metric {
				row[label] = labelVal
			}

			rows = append(rows, row)

			if len(rows) >= limit {
				return rows, nil
			}
		}
	}

	return rows, nil
}
