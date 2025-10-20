package databricks

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// DataOps implements data operations for Databricks.
type DataOps struct {
	conn *Connection
}

// Fetch retrieves rows from Databricks.
func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
	return d.FetchWithColumns(ctx, table, nil, limit)
}

// FetchWithColumns retrieves rows with specific columns.
func (d *DataOps) FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error) {
	selectClause := "*"
	if len(columns) > 0 {
		selectClause = strings.Join(columns, ", ")
	}

	query := fmt.Sprintf("SELECT %s FROM %s LIMIT %d", selectClause, table, limit)
	return d.executeQueryToRows(ctx, query)
}

// executeQueryToRows executes a query and returns rows as maps.
func (d *DataOps) executeQueryToRows(ctx context.Context, query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := d.conn.client.DB().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return scanRowsToMaps(rows)
}

// scanRowsToMaps scans SQL rows into maps.
func scanRowsToMaps(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	results := make([]map[string]interface{}, 0)

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}

		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}

// Insert inserts rows into Databricks.
func (d *DataOps) Insert(ctx context.Context, table string, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Get columns from first row
	var columns []string
	for col := range data[0] {
		columns = append(columns, col)
	}

	// Build INSERT statement
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table, strings.Join(columns, ", "))

	// Build values clause
	valueClauses := make([]string, 0, len(data))
	for range data {
		placeholders := make([]string, len(columns))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		valueClauses = append(valueClauses, "("+strings.Join(placeholders, ", ")+")")
	}

	query += strings.Join(valueClauses, ", ")

	// Flatten all values
	var allValues []interface{}
	for _, row := range data {
		for _, col := range columns {
			allValues = append(allValues, row[col])
		}
	}

	result, err := d.conn.client.DB().ExecContext(ctx, query, allValues...)
	if err != nil {
		return 0, fmt.Errorf("failed to insert rows: %w", err)
	}

	return result.RowsAffected()
}

// Update updates rows in Databricks.
func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	return 0, fmt.Errorf("Update operation requires SQL query - use ExecuteQuery with UPDATE statement")
}

// Upsert performs upsert operation in Databricks using MERGE.
func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	return 0, fmt.Errorf("Upsert operation requires SQL query - use ExecuteQuery with MERGE statement")
}

// Delete deletes rows from Databricks.
func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
	whereClause := buildWhereClause(conditions)
	query := fmt.Sprintf("DELETE FROM %s WHERE %s", table, whereClause)

	result, err := d.conn.client.DB().ExecContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to delete rows: %w", err)
	}

	return result.RowsAffected()
}

// buildWhereClause builds a WHERE clause from conditions.
func buildWhereClause(conditions map[string]interface{}) string {
	clauses := make([]string, 0, len(conditions))
	for k, v := range conditions {
		switch val := v.(type) {
		case string:
			clauses = append(clauses, fmt.Sprintf("%s = '%s'", k, val))
		default:
			clauses = append(clauses, fmt.Sprintf("%s = %v", k, val))
		}
	}
	return strings.Join(clauses, " AND ")
}

// Stream retrieves rows in batches.
func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	selectClause := "*"
	if len(params.Columns) > 0 {
		selectClause = strings.Join(params.Columns, ", ")
	}

	query := fmt.Sprintf("SELECT %s FROM %s LIMIT %d OFFSET %d",
		selectClause, params.Table, params.BatchSize, params.Offset)

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
func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	rows, err := d.executeQueryToRows(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	results := make([]interface{}, len(rows))
	for i, row := range rows {
		results[i] = row
	}

	return results, nil
}

// ExecuteCountQuery counts rows.
func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	var count int64
	err := d.conn.client.DB().QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to execute count query: %w", err)
	}
	return count, nil
}

// GetRowCount returns the number of rows in a table.
func (d *DataOps) GetRowCount(ctx context.Context, table string, whereClause string) (int64, bool, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	count, err := d.ExecuteCountQuery(ctx, query)
	if err != nil {
		return 0, false, err
	}

	return count, true, nil
}

// Wipe deletes all rows from all tables in the database.
func (d *DataOps) Wipe(ctx context.Context) error {
	// List all tables
	tables, err := d.conn.SchemaOperations().ListTables(ctx)
	if err != nil {
		return err
	}

	// Delete all rows from each table
	for _, table := range tables {
		query := fmt.Sprintf("DELETE FROM %s", table)
		_, err := d.conn.client.DB().ExecContext(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to wipe table %s: %w", table, err)
		}
	}

	return nil
}
