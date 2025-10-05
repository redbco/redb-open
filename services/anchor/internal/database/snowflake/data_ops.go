package snowflake

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

type DataOps struct {
	conn *Connection
}

func (d *DataOps) Fetch(ctx context.Context, tableName string, limit int) ([]map[string]interface{}, error) {
	data, err := FetchData(d.conn.db, tableName, limit)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Snowflake, "fetch_data", err)
	}
	return data, nil
}

func (d *DataOps) FetchWithColumns(ctx context.Context, tableName string, columns []string, limit int) ([]map[string]interface{}, error) {
	data, err := FetchData(d.conn.db, tableName, limit)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Snowflake, "fetch_data_with_columns", err)
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
	count, err := InsertData(d.conn.db, tableName, data)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Snowflake, "insert_data", err)
	}
	return count, nil
}

func (d *DataOps) Update(ctx context.Context, tableName string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Snowflake, "update data", "not yet implemented")
}

func (d *DataOps) Upsert(ctx context.Context, tableName string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Snowflake, "upsert data", "not yet implemented")
}

func (d *DataOps) Delete(ctx context.Context, tableName string, conditions map[string]interface{}) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Snowflake, "delete with conditions", "not yet implemented")
}

func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	data, hasMore, _, err := StreamTableData(d.conn.db, params.Table, params.BatchSize, params.Offset, params.Columns)
	if err != nil {
		return adapter.StreamResult{}, adapter.WrapError(dbcapabilities.Snowflake, "stream_data", err)
	}
	return adapter.StreamResult{
		Data:    data,
		HasMore: hasMore,
	}, nil
}

func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	result, err := ExecuteQuery(d.conn.db, query, args...)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Snowflake, "execute_query", err)
	}
	return result, nil
}

func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	count, err := ExecuteCountQuery(d.conn.db, query)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Snowflake, "execute_count_query", err)
	}
	return count, nil
}

func (d *DataOps) GetRowCount(ctx context.Context, tableName string, whereClause string) (int64, bool, error) {
	count, exact, err := GetTableRowCount(d.conn.db, tableName, whereClause)
	if err != nil {
		return 0, false, adapter.WrapError(dbcapabilities.Snowflake, "get_row_count", err)
	}
	return count, exact, nil
}

func (d *DataOps) Wipe(ctx context.Context) error {
	err := WipeDatabase(d.conn.db)
	if err != nil {
		return adapter.WrapError(dbcapabilities.Snowflake, "wipe_database", err)
	}
	return nil
}
