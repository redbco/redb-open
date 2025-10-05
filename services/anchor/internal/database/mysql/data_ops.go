package mysql

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// DataOps implements adapter.DataOperator for MySQL.
type DataOps struct {
	conn *Connection
}

// Fetch retrieves data from a table with a limit.
func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
	data, err := FetchData(d.conn.db, table, limit, nil)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MySQL, "fetch", err)
	}
	return data, nil
}

// FetchWithColumns retrieves specific columns from a table.
func (d *DataOps) FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error) {
	// MySQL FetchData doesn't support column filtering, so we fetch all and filter
	data, err := d.Fetch(ctx, table, limit)
	if err != nil {
		return nil, err
	}

	// Filter to requested columns if specified
	if len(columns) > 0 {
		filtered := make([]map[string]interface{}, len(data))
		for i, row := range data {
			filteredRow := make(map[string]interface{})
			for _, col := range columns {
				if val, ok := row[col]; ok {
					filteredRow[col] = val
				}
			}
			filtered[i] = filteredRow
		}
		return filtered, nil
	}

	return data, nil
}

// Insert inserts data into a table.
func (d *DataOps) Insert(ctx context.Context, table string, data []map[string]interface{}) (int64, error) {
	rowsAffected, err := InsertData(d.conn.db, table, data, nil)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.MySQL, "insert", err)
	}
	return rowsAffected, nil
}

// Update updates existing data in a table.
func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	rowsAffected, err := UpdateData(d.conn.db, table, data, whereColumns, nil)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.MySQL, "update", err)
	}
	return rowsAffected, nil
}

// Upsert inserts or updates data based on unique columns.
func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	rowsAffected, err := UpsertData(d.conn.db, table, data, uniqueColumns, nil)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.MySQL, "upsert", err)
	}
	return rowsAffected, nil
}

// Delete deletes data from a table based on conditions.
func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
	// MySQL doesn't have a direct delete with conditions function, return unsupported
	return 0, adapter.ErrOperationNotSupported
}

// Stream streams data from a table in batches.
func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	data, hasMore, nextCursor, err := StreamTableData(
		d.conn.db,
		params.Table,
		params.BatchSize,
		params.Offset,
		params.Columns,
	)
	if err != nil {
		return adapter.StreamResult{}, adapter.WrapError(dbcapabilities.MySQL, "stream", err)
	}

	return adapter.StreamResult{
		Data:       data,
		HasMore:    hasMore,
		NextCursor: nextCursor,
	}, nil
}

// ExecuteQuery executes a raw SQL query.
func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	result, err := ExecuteQuery(d.conn.db, query, args...)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MySQL, "execute_query", err)
	}
	return result, nil
}

// ExecuteCountQuery executes a count query.
func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	count, err := ExecuteCountQuery(d.conn.db, query)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.MySQL, "execute_count_query", err)
	}
	return count, nil
}

// GetRowCount returns the number of rows in a table.
func (d *DataOps) GetRowCount(ctx context.Context, table string, whereClause string) (int64, bool, error) {
	count, isExact, err := GetTableRowCount(d.conn.db, table, whereClause)
	if err != nil {
		return 0, false, adapter.WrapError(dbcapabilities.MySQL, "get_row_count", err)
	}
	return count, isExact, nil
}

// Wipe removes all data from the database.
func (d *DataOps) Wipe(ctx context.Context) error {
	err := WipeDatabase(d.conn.db, nil)
	if err != nil {
		return adapter.WrapError(dbcapabilities.MySQL, "wipe", err)
	}
	return nil
}
