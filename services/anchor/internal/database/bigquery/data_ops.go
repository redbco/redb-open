package bigquery

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"google.golang.org/api/iterator"
)

// DataOps implements data operations for BigQuery.
type DataOps struct {
	conn *Connection
}

// Fetch retrieves rows from BigQuery.
func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
	return d.FetchWithColumns(ctx, table, nil, limit)
}

// FetchWithColumns retrieves rows with specific columns.
func (d *DataOps) FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error) {
	projectID := d.conn.client.GetProjectID()
	datasetID := d.conn.client.GetDatasetID()
	if datasetID == "" {
		return nil, fmt.Errorf("no dataset specified")
	}

	// Build query
	selectClause := "*"
	if len(columns) > 0 {
		selectClause = strings.Join(columns, ", ")
	}

	query := fmt.Sprintf("SELECT %s FROM `%s.%s.%s` LIMIT %d",
		selectClause, projectID, datasetID, table, limit)

	return d.executeQueryToRows(ctx, query)
}

// executeQueryToRows executes a query and returns rows as maps.
func (d *DataOps) executeQueryToRows(ctx context.Context, queryStr string) ([]map[string]interface{}, error) {
	query := d.conn.client.Client().Query(queryStr)
	it, err := query.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	rows := make([]map[string]interface{}, 0)

	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read row: %w", err)
		}

		// Convert bigquery.Value to interface{}
		convertedRow := make(map[string]interface{})
		for k, v := range row {
			convertedRow[k] = v
		}

		rows = append(rows, convertedRow)
	}

	return rows, nil
}

// Insert inserts rows into BigQuery.
func (d *DataOps) Insert(ctx context.Context, table string, data []map[string]interface{}) (int64, error) {
	dataset := d.conn.client.GetDataset()
	tableRef := dataset.Table(table)

	inserter := tableRef.Inserter()

	// Convert data to BigQuery value saver
	items := make([]*bigquery.StructSaver, 0, len(data))
	for _, row := range data {
		items = append(items, &bigquery.StructSaver{
			Schema: nil, // Auto-detect schema
			Struct: row,
		})
	}

	err := inserter.Put(ctx, items)
	if err != nil {
		return 0, fmt.Errorf("failed to insert rows: %w", err)
	}

	return int64(len(items)), nil
}

// Update is not directly supported in BigQuery. Use UPDATE queries instead.
func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	return 0, fmt.Errorf("Update operation requires SQL query - use ExecuteQuery with UPDATE statement")
}

// Upsert performs MERGE operation in BigQuery.
func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	return 0, fmt.Errorf("Upsert operation requires SQL query - use ExecuteQuery with MERGE statement")
}

// Delete deletes rows from BigQuery.
func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
	projectID := d.conn.client.GetProjectID()
	datasetID := d.conn.client.GetDatasetID()
	if datasetID == "" {
		return 0, fmt.Errorf("no dataset specified")
	}

	// Build WHERE clause
	whereClause := buildWhereClause(conditions)

	query := fmt.Sprintf("DELETE FROM `%s.%s.%s` WHERE %s",
		projectID, datasetID, table, whereClause)

	q := d.conn.client.Client().Query(query)
	job, err := q.Run(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to delete rows: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return 0, fmt.Errorf("delete job failed: %w", err)
	}
	if status.Err() != nil {
		return 0, fmt.Errorf("delete job error: %w", status.Err())
	}

	// BigQuery doesn't return affected rows count easily
	return 1, nil
}

// buildWhereClause builds a WHERE clause from conditions.
func buildWhereClause(conditions map[string]interface{}) string {
	clauses := make([]string, 0, len(conditions))
	for k, v := range conditions {
		switch val := v.(type) {
		case string:
			clauses = append(clauses, fmt.Sprintf("%s = '%s'", k, val))
		case int, int64, float64:
			clauses = append(clauses, fmt.Sprintf("%s = %v", k, val))
		default:
			clauses = append(clauses, fmt.Sprintf("%s = '%v'", k, val))
		}
	}
	return strings.Join(clauses, " AND ")
}

// Stream retrieves rows in batches.
func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	projectID := d.conn.client.GetProjectID()
	datasetID := d.conn.client.GetDatasetID()
	if datasetID == "" {
		return adapter.StreamResult{}, fmt.Errorf("no dataset specified")
	}

	selectClause := "*"
	if len(params.Columns) > 0 {
		selectClause = strings.Join(params.Columns, ", ")
	}

	query := fmt.Sprintf("SELECT %s FROM `%s.%s.%s` LIMIT %d OFFSET %d",
		selectClause, projectID, datasetID, params.Table, params.BatchSize, params.Offset)

	rows, err := d.executeQueryToRows(ctx, query)
	if err != nil {
		return adapter.StreamResult{}, err
	}

	hasMore := len(rows) == int(params.BatchSize)

	return adapter.StreamResult{
		Data:       rows,
		HasMore:    hasMore,
		NextCursor: fmt.Sprintf("%d", params.Offset+int64(len(rows))),
	}, nil
}

// ExecuteQuery executes a SQL query.
func (d *DataOps) ExecuteQuery(ctx context.Context, queryStr string, args ...interface{}) ([]interface{}, error) {
	query := d.conn.client.Client().Query(queryStr)
	it, err := query.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	var results []interface{}

	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read row: %w", err)
		}

		// Convert to interface{}
		convertedRow := make(map[string]interface{})
		for k, v := range row {
			convertedRow[k] = v
		}

		results = append(results, convertedRow)
	}

	return results, nil
}

// ExecuteCountQuery counts rows.
func (d *DataOps) ExecuteCountQuery(ctx context.Context, queryStr string) (int64, error) {
	query := d.conn.client.Client().Query(queryStr)
	it, err := query.Read(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to execute count query: %w", err)
	}

	var count int64
	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("failed to read row: %w", err)
		}

		// Get first value as count
		for _, v := range row {
			if val, ok := v.(int64); ok {
				count = val
			}
			break
		}
	}

	return count, nil
}

// GetRowCount returns the number of rows in a table.
func (d *DataOps) GetRowCount(ctx context.Context, table string, whereClause string) (int64, bool, error) {
	projectID := d.conn.client.GetProjectID()
	datasetID := d.conn.client.GetDatasetID()
	if datasetID == "" {
		return 0, false, fmt.Errorf("no dataset specified")
	}

	query := fmt.Sprintf("SELECT COUNT(*) as count FROM `%s.%s.%s`",
		projectID, datasetID, table)

	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	count, err := d.ExecuteCountQuery(ctx, query)
	if err != nil {
		return 0, false, err
	}

	return count, true, nil
}

// Wipe deletes all rows from all tables in the dataset.
func (d *DataOps) Wipe(ctx context.Context) error {
	projectID := d.conn.client.GetProjectID()
	datasetID := d.conn.client.GetDatasetID()
	if datasetID == "" {
		return fmt.Errorf("no dataset specified")
	}

	// List all tables
	dataset := d.conn.client.GetDataset()
	it := dataset.Tables(ctx)

	for {
		table, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list tables: %w", err)
		}

		// Delete all rows from table
		query := fmt.Sprintf("DELETE FROM `%s.%s.%s` WHERE true",
			projectID, datasetID, table.TableID)

		q := d.conn.client.Client().Query(query)
		job, err := q.Run(ctx)
		if err != nil {
			return fmt.Errorf("failed to wipe table %s: %w", table.TableID, err)
		}

		status, err := job.Wait(ctx)
		if err != nil {
			return fmt.Errorf("wipe job failed for table %s: %w", table.TableID, err)
		}
		if status.Err() != nil {
			return fmt.Errorf("wipe job error for table %s: %w", table.TableID, status.Err())
		}
	}

	return nil
}
