package iceberg

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

type DataOps struct {
	conn *Connection
}

func (d *DataOps) Fetch(ctx context.Context, tableName string, limit int) ([]map[string]interface{}, error) {
	data, err := FetchData(d.conn.client, tableName, limit)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Iceberg, "fetch_data", err)
	}
	return data, nil
}

func (d *DataOps) FetchWithColumns(ctx context.Context, tableName string, columns []string, limit int) ([]map[string]interface{}, error) {
	data, err := FetchData(d.conn.client, tableName, limit)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Iceberg, "fetch_data_with_columns", err)
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

func (d *DataOps) Insert(ctx context.Context, tableName string, data []map[string]interface{}) (int64, error) {
	count, err := InsertData(d.conn.client, tableName, data)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Iceberg, "insert_data", err)
	}
	return count, nil
}

func (d *DataOps) Update(ctx context.Context, tableName string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Iceberg, "update data", "not yet implemented")
}

func (d *DataOps) Upsert(ctx context.Context, tableName string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Iceberg, "upsert data", "not yet implemented")
}

func (d *DataOps) Delete(ctx context.Context, tableName string, conditions map[string]interface{}) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Iceberg, "delete with conditions", "not yet implemented")
}

func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	return adapter.StreamResult{}, adapter.NewUnsupportedOperationError(dbcapabilities.Iceberg, "stream data", "not yet implemented")
}

func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.Iceberg, "execute_query", "Iceberg uses metadata APIs, not SQL queries")
}

func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Iceberg, "execute_count_query", "Iceberg uses metadata APIs, not SQL queries")
}

func (d *DataOps) GetRowCount(ctx context.Context, tableName string, whereClause string) (int64, bool, error) {
	return 0, false, adapter.NewUnsupportedOperationError(dbcapabilities.Iceberg, "get_row_count", "not yet implemented")
}

func (d *DataOps) Wipe(ctx context.Context) error {
	return adapter.NewUnsupportedOperationError(dbcapabilities.Iceberg, "wipe_database", "not yet implemented")
}
