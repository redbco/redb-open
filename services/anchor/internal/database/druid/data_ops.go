package druid

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// DataOps implements data operations for Druid.
type DataOps struct {
	conn *Connection
}

// Fetch retrieves data from a datasource using SQL.
func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d", table, limit)
	result, err := d.conn.client.QuerySQL(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch data: %w", err)
	}

	return result.Data, nil
}

// FetchWithColumns retrieves specific columns from a datasource.
func (d *DataOps) FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error) {
	if len(columns) == 0 {
		return d.Fetch(ctx, table, limit)
	}

	columnList := ""
	for i, col := range columns {
		if i > 0 {
			columnList += ", "
		}
		columnList += col
	}

	query := fmt.Sprintf("SELECT %s FROM %s LIMIT %d", columnList, table, limit)
	result, err := d.conn.client.QuerySQL(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch data: %w", err)
	}

	return result.Data, nil
}

// Insert is not directly supported (use ingestion).
func (d *DataOps) Insert(ctx context.Context, table string, data []map[string]interface{}) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(
		dbcapabilities.Druid,
		"insert",
		"Use Druid ingestion specs (batch/streaming) instead",
	)
}

// Update is not supported for Druid.
func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(
		dbcapabilities.Druid,
		"update",
		"Druid is an append-only database",
	)
}

// Upsert is not supported for Druid.
func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(
		dbcapabilities.Druid,
		"upsert",
		"Druid is an append-only database",
	)
}

// Delete is not directly supported.
func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(
		dbcapabilities.Druid,
		"delete",
		"Druid uses time-based retention and compaction for data deletion",
	)
}

// Stream retrieves data in batches using SQL.
func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d",
		params.Table, params.BatchSize, params.Offset)

	result, err := d.conn.client.QuerySQL(ctx, query)
	if err != nil {
		return adapter.StreamResult{}, fmt.Errorf("failed to stream data: %w", err)
	}

	hasMore := len(result.Data) == int(params.BatchSize)
	nextOffset := params.Offset + int64(len(result.Data))

	return adapter.StreamResult{
		Data:       result.Data,
		HasMore:    hasMore,
		NextCursor: fmt.Sprintf("%d", nextOffset),
	}, nil
}

// ExecuteQuery executes a SQL query.
func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	result, err := d.conn.client.QuerySQL(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Convert to []interface{}
	results := make([]interface{}, len(result.Data))
	for i, row := range result.Data {
		results[i] = row
	}

	return results, nil
}

// ExecuteCountQuery executes a COUNT query.
func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	result, err := d.conn.client.QuerySQL(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to execute count query: %w", err)
	}

	if len(result.Data) == 0 {
		return 0, nil
	}

	// Extract count from result
	for _, val := range result.Data[0] {
		if count, ok := val.(float64); ok {
			return int64(count), nil
		}
	}

	return 0, nil
}

// GetRowCount returns the number of rows in a datasource.
func (d *DataOps) GetRowCount(ctx context.Context, table string, whereClause string) (int64, bool, error) {
	query := fmt.Sprintf("SELECT COUNT(*) as count FROM %s", table)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	count, err := d.ExecuteCountQuery(ctx, query)
	if err != nil {
		return 0, false, err
	}

	return count, true, nil
}

// Wipe is not supported.
func (d *DataOps) Wipe(ctx context.Context) error {
	return adapter.NewUnsupportedOperationError(
		dbcapabilities.Druid,
		"wipe",
		"Druid uses time-based retention policies for data management",
	)
}

// ExecuteNativeQuery executes a native Druid query.
func (d *DataOps) ExecuteNativeQuery(ctx context.Context, querySpec map[string]interface{}) ([]interface{}, error) {
	return d.conn.client.QueryNative(ctx, querySpec)
}
