package mssql

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

type DataOps struct {
	conn *Connection
}

func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
	data, err := FetchData(d.conn.db, table, limit)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.SQLServer, "fetch_data", err)
	}
	return data, nil
}

func (d *DataOps) FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error) {
	data, err := FetchData(d.conn.db, table, limit)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.SQLServer, "fetch_data_with_columns", err)
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

func (d *DataOps) Insert(ctx context.Context, table string, data []map[string]interface{}) (int64, error) {
	count, err := InsertData(d.conn.db, table, data)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.SQLServer, "insert_data", err)
	}
	return count, nil
}

func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.SQLServer, "update data", "not yet implemented")
}

func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.SQLServer, "upsert data", "not yet implemented")
}

func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.SQLServer, "delete with conditions", "not yet implemented")
}

func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	return adapter.StreamResult{}, adapter.NewUnsupportedOperationError(dbcapabilities.SQLServer, "stream data", "not yet implemented")
}

func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	result, err := ExecuteQuery(d.conn.db, query, args...)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.SQLServer, "execute_query", err)
	}
	return result, nil
}

func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.SQLServer, "execute_count_query", "not yet implemented")
}

func (d *DataOps) GetRowCount(ctx context.Context, table string, whereClause string) (int64, bool, error) {
	count, exact, err := GetTableRowCount(d.conn.db, table, whereClause)
	if err != nil {
		return 0, false, adapter.WrapError(dbcapabilities.SQLServer, "get_row_count", err)
	}
	return count, exact, nil
}

func (d *DataOps) Wipe(ctx context.Context) error {
	err := WipeDatabase(d.conn.db)
	if err != nil {
		return adapter.WrapError(dbcapabilities.SQLServer, "wipe_database", err)
	}
	return nil
}
