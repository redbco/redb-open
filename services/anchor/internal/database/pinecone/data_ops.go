package pinecone

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

type DataOps struct {
	conn *Connection
}

func (d *DataOps) Fetch(ctx context.Context, indexName string, limit int) ([]map[string]interface{}, error) {
	data, err := FetchData(d.conn.client, indexName, "", limit) // Empty namespace for default
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Pinecone, "fetch_data", err)
	}
	return data, nil
}

func (d *DataOps) FetchWithColumns(ctx context.Context, indexName string, columns []string, limit int) ([]map[string]interface{}, error) {
	// Pinecone doesn't support column filtering at query level
	data, err := d.Fetch(ctx, indexName, limit)
	if err != nil {
		return nil, err
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

func (d *DataOps) Insert(ctx context.Context, indexName string, data []map[string]interface{}) (int64, error) {
	count, err := InsertData(d.conn.client, indexName, "", data) // Empty namespace for default
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Pinecone, "insert_data", err)
	}
	return count, nil
}

func (d *DataOps) Update(ctx context.Context, indexName string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Pinecone, "update data", "not yet implemented")
}

func (d *DataOps) Upsert(ctx context.Context, indexName string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Pinecone, "upsert data", "not yet implemented")
}

func (d *DataOps) Delete(ctx context.Context, indexName string, conditions map[string]interface{}) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Pinecone, "delete with conditions", "not yet implemented")
}

func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	return adapter.StreamResult{}, adapter.NewUnsupportedOperationError(dbcapabilities.Pinecone, "stream data", "not yet implemented")
}

func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	result, err := ExecuteQuery(d.conn.client, query, args...)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Pinecone, "execute_query", err)
	}
	return result, nil
}

func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Pinecone, "execute_count_query", "not supported")
}

func (d *DataOps) GetRowCount(ctx context.Context, indexName string, whereClause string) (int64, bool, error) {
	count, exact, err := GetTableRowCount(d.conn.client, indexName, whereClause)
	if err != nil {
		return 0, false, adapter.WrapError(dbcapabilities.Pinecone, "get_row_count", err)
	}
	return count, exact, nil
}

func (d *DataOps) Wipe(ctx context.Context) error {
	err := WipeDatabase(d.conn.client)
	if err != nil {
		return adapter.WrapError(dbcapabilities.Pinecone, "wipe_database", err)
	}
	return nil
}
