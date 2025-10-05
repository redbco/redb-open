package redis

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

type DataOps struct {
	conn *Connection
}

func (d *DataOps) Fetch(ctx context.Context, pattern string, limit int) ([]map[string]interface{}, error) {
	data, err := FetchData(d.conn.client, pattern, limit)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Redis, "fetch_data", err)
	}
	return data, nil
}

func (d *DataOps) FetchWithColumns(ctx context.Context, pattern string, columns []string, limit int) ([]map[string]interface{}, error) {
	// Redis doesn't have columns
	return d.Fetch(ctx, pattern, limit)
}

func (d *DataOps) Insert(ctx context.Context, key string, data []map[string]interface{}) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Redis, "insert data", "use SET operations for Redis")
}

func (d *DataOps) Update(ctx context.Context, key string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Redis, "update data", "use SET operations for Redis")
}

func (d *DataOps) Upsert(ctx context.Context, key string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Redis, "upsert data", "use SET operations for Redis")
}

func (d *DataOps) Delete(ctx context.Context, key string, conditions map[string]interface{}) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Redis, "delete with conditions", "use DEL operations for Redis")
}

func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	// Redis streaming not yet implemented in legacy code
	return adapter.StreamResult{}, adapter.NewUnsupportedOperationError(dbcapabilities.Redis, "stream data", "not yet implemented")
}

func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.Redis, "execute_query", "Redis doesn't use SQL queries")
}

func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Redis, "execute_count_query", "Redis doesn't use SQL queries")
}

func (d *DataOps) GetRowCount(ctx context.Context, pattern string, whereClause string) (int64, bool, error) {
	count, err := d.conn.client.DBSize(ctx).Result()
	if err != nil {
		return 0, false, adapter.WrapError(dbcapabilities.Redis, "get_row_count", err)
	}
	return count, true, nil
}

func (d *DataOps) Wipe(ctx context.Context) error {
	err := d.conn.client.FlushDB(ctx).Err()
	if err != nil {
		return adapter.WrapError(dbcapabilities.Redis, "wipe_database", err)
	}
	return nil
}
