package postgres

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// DataOps implements adapter.DataOperator for PostgreSQL.
type DataOps struct {
	conn *Connection
}

// Fetch retrieves data from a table.
func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
	// Use existing FetchData function
	data, err := FetchData(d.conn.pool, table, limit)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "fetch_data", err)
	}
	return data, nil
}

// FetchWithColumns retrieves specific columns from a table.
func (d *DataOps) FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error) {
	// Use existing FetchData and filter columns
	// For now, we'll fetch all and filter in memory
	// TODO: Optimize to only fetch requested columns
	data, err := FetchData(d.conn.pool, table, limit)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "fetch_data_with_columns", err)
	}

	if len(columns) == 0 {
		return data, nil
	}

	// Filter to requested columns
	filtered := make([]map[string]interface{}, len(data))
	for i, row := range data {
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

// Insert inserts data into a table.
func (d *DataOps) Insert(ctx context.Context, table string, data []map[string]interface{}) (int64, error) {
	// Use existing InsertData function
	count, err := InsertData(d.conn.pool, table, data)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.PostgreSQL, "insert_data", err)
	}
	return count, nil
}

// Update updates data in a table.
func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	// Use existing UpdateData function
	count, err := UpdateData(d.conn.pool, table, data, whereColumns)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.PostgreSQL, "update_data", err)
	}
	return count, nil
}

// Upsert inserts or updates data in a table.
func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	// Use existing UpsertData function
	count, err := UpsertData(d.conn.pool, table, data, uniqueColumns)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.PostgreSQL, "upsert_data", err)
	}
	return count, nil
}

// Delete deletes data from a table.
func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
	// PostgreSQL doesn't have a direct Delete function yet
	// Return unsupported for now
	return 0, adapter.NewUnsupportedOperationError(
		dbcapabilities.PostgreSQL,
		"delete with conditions",
		"not yet implemented",
	)
}

// Stream streams data from a table in batches.
func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	// Use existing StreamTableData function
	data, hasMore, nextCursor, err := StreamTableData(
		d.conn.pool,
		params.Table,
		params.BatchSize,
		params.Offset,
		params.Columns,
	)
	if err != nil {
		return adapter.StreamResult{}, adapter.WrapError(dbcapabilities.PostgreSQL, "stream_data", err)
	}

	return adapter.StreamResult{
		Data:       data,
		HasMore:    hasMore,
		NextCursor: nextCursor,
	}, nil
}

// ExecuteQuery executes a query and returns the results.
func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	// Use existing ExecuteQuery function
	result, err := ExecuteQuery(d.conn.pool, query, args...)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "execute_query", err)
	}
	return result, nil
}

// ExecuteCountQuery executes a count query and returns the count.
func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	// Use existing ExecuteCountQuery function
	count, err := ExecuteCountQuery(d.conn.pool, query)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.PostgreSQL, "execute_count_query", err)
	}
	return count, nil
}

// GetRowCount returns the number of rows in a table.
func (d *DataOps) GetRowCount(ctx context.Context, table string, whereClause string) (int64, bool, error) {
	// Use existing GetTableRowCount function
	count, exact, err := GetTableRowCount(d.conn.pool, table, whereClause)
	if err != nil {
		return 0, false, adapter.WrapError(dbcapabilities.PostgreSQL, "get_row_count", err)
	}
	return count, exact, nil
}

// Wipe deletes all data from the database.
func (d *DataOps) Wipe(ctx context.Context) error {
	// Use existing WipeDatabase function
	err := WipeDatabase(d.conn.pool)
	if err != nil {
		return adapter.WrapError(dbcapabilities.PostgreSQL, "wipe_database", err)
	}
	return nil
}
