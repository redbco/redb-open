package edgedb

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

type DataOps struct {
	conn *Connection
}

func (d *DataOps) Fetch(ctx context.Context, typeName string, limit int) ([]map[string]interface{}, error) {
	data, err := FetchData(d.conn.client, typeName, limit)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.EdgeDB, "fetch_data", err)
	}
	return data, nil
}

func (d *DataOps) FetchWithColumns(ctx context.Context, typeName string, columns []string, limit int) ([]map[string]interface{}, error) {
	data, err := FetchData(d.conn.client, typeName, limit)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.EdgeDB, "fetch_data_with_columns", err)
	}

	if len(columns) == 0 {
		return data, nil
	}

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

func (d *DataOps) Insert(ctx context.Context, typeName string, data []map[string]interface{}) (int64, error) {
	count, err := InsertData(d.conn.client, typeName, data)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.EdgeDB, "insert_data", err)
	}
	return count, nil
}

func (d *DataOps) Update(ctx context.Context, typeName string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.EdgeDB, "update data", "not yet implemented")
}

func (d *DataOps) Upsert(ctx context.Context, typeName string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.EdgeDB, "upsert data", "not yet implemented")
}

func (d *DataOps) Delete(ctx context.Context, typeName string, conditions map[string]interface{}) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.EdgeDB, "delete with conditions", "not yet implemented")
}

func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	return adapter.StreamResult{}, adapter.NewUnsupportedOperationError(dbcapabilities.EdgeDB, "stream data", "not yet implemented")
}

func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	result, err := ExecuteQuery(d.conn.client, query, args...)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.EdgeDB, "execute_query", err)
	}
	return result, nil
}

func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.EdgeDB, "execute_count_query", "not yet implemented")
}

func (d *DataOps) GetRowCount(ctx context.Context, typeName string, whereClause string) (int64, bool, error) {
	count, exact, err := GetTableRowCount(d.conn.client, typeName, whereClause)
	if err != nil {
		return 0, false, adapter.WrapError(dbcapabilities.EdgeDB, "get_row_count", err)
	}
	return count, exact, nil
}

func (d *DataOps) Wipe(ctx context.Context) error {
	err := WipeDatabase(d.conn.client)
	if err != nil {
		return adapter.WrapError(dbcapabilities.EdgeDB, "wipe_database", err)
	}
	return nil
}
