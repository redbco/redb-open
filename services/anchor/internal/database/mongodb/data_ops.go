package mongodb

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// DataOps implements adapter.DataOperator for MongoDB.
type DataOps struct {
	conn *Connection
}

// Fetch retrieves data from a collection with a limit.
func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
	data, err := FetchData(d.conn.db, table, limit)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MongoDB, "fetch", err)
	}
	return data, nil
}

// FetchWithColumns retrieves specific fields from a collection.
func (d *DataOps) FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error) {
	// MongoDB FetchData doesn't support field filtering, so we fetch all and filter
	data, err := d.Fetch(ctx, table, limit)
	if err != nil {
		return nil, err
	}

	// Filter to requested fields if specified
	if len(columns) > 0 {
		filtered := make([]map[string]interface{}, len(data))
		for i, doc := range data {
			filteredDoc := make(map[string]interface{})
			for _, field := range columns {
				if val, ok := doc[field]; ok {
					filteredDoc[field] = val
				}
			}
			filtered[i] = filteredDoc
		}
		return filtered, nil
	}

	return data, nil
}

// Insert inserts data into a collection.
func (d *DataOps) Insert(ctx context.Context, table string, data []map[string]interface{}) (int64, error) {
	rowsAffected, err := InsertData(d.conn.db, table, data)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.MongoDB, "insert", err)
	}
	return rowsAffected, nil
}

// Update updates existing data in a collection.
func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	rowsAffected, err := UpdateData(d.conn.db, table, data, whereColumns)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.MongoDB, "update", err)
	}
	return rowsAffected, nil
}

// Upsert inserts or updates data based on unique fields.
func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	rowsAffected, err := UpsertData(d.conn.db, table, data, uniqueColumns)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.MongoDB, "upsert", err)
	}
	return rowsAffected, nil
}

// Delete deletes data from a collection based on conditions.
func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
	// MongoDB doesn't have a direct delete with conditions function in the legacy code
	return 0, adapter.ErrOperationNotSupported
}

// Stream streams data from a collection in batches.
func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	data, hasMore, nextCursor, err := StreamTableData(
		d.conn.db,
		params.Table,
		params.BatchSize,
		params.Offset,
		params.Columns,
	)
	if err != nil {
		return adapter.StreamResult{}, adapter.WrapError(dbcapabilities.MongoDB, "stream", err)
	}

	return adapter.StreamResult{
		Data:       data,
		HasMore:    hasMore,
		NextCursor: nextCursor,
	}, nil
}

// ExecuteQuery executes a raw query (not typically used in MongoDB).
func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	// MongoDB typically uses BSON queries, not SQL strings
	return nil, adapter.ErrOperationNotSupported
}

// ExecuteCountQuery executes a count query.
func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	// MongoDB count operations are done differently
	return 0, adapter.ErrOperationNotSupported
}

// GetRowCount returns the number of documents in a collection.
func (d *DataOps) GetRowCount(ctx context.Context, table string, whereClause string) (int64, bool, error) {
	count, isExact, err := GetTableRowCount(d.conn.db, table, whereClause)
	if err != nil {
		return 0, false, adapter.WrapError(dbcapabilities.MongoDB, "get_row_count", err)
	}
	return count, isExact, nil
}

// Wipe removes all data from the database.
func (d *DataOps) Wipe(ctx context.Context) error {
	err := WipeDatabase(d.conn.db)
	if err != nil {
		return adapter.WrapError(dbcapabilities.MongoDB, "wipe", err)
	}
	return nil
}
