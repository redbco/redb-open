package timescaledb

import (
	"context"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// DataOps implements data operations for TimescaleDB.
type DataOps struct {
	conn *Connection
}

// Fetch retrieves rows from a table.
func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM %s LIMIT $1", table)
	return d.executeQuery(ctx, query, limit)
}

// FetchWithColumns retrieves specific columns from a table.
func (d *DataOps) FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error) {
	if len(columns) == 0 {
		return d.Fetch(ctx, table, limit)
	}

	columnList := strings.Join(columns, ", ")
	query := fmt.Sprintf("SELECT %s FROM %s LIMIT $1", columnList, table)
	return d.executeQuery(ctx, query, limit)
}

// Insert inserts data into a table.
func (d *DataOps) Insert(ctx context.Context, table string, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	var count int64
	for _, row := range data {
		if len(row) == 0 {
			continue
		}

		columns := make([]string, 0, len(row))
		placeholders := make([]string, 0, len(row))
		values := make([]interface{}, 0, len(row))
		i := 1

		for col, val := range row {
			columns = append(columns, col)
			placeholders = append(placeholders, fmt.Sprintf("$%d", i))
			values = append(values, val)
			i++
		}

		query := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			table,
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "),
		)

		result, err := d.conn.db.ExecContext(ctx, query, values...)
		if err != nil {
			return count, fmt.Errorf("failed to insert row: %w", err)
		}

		affected, err := result.RowsAffected()
		if err == nil {
			count += affected
		}
	}

	return count, nil
}

// Update updates data in a table based on where columns.
func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	if len(data) == 0 || len(whereColumns) == 0 {
		return 0, fmt.Errorf("data and whereColumns cannot be empty")
	}

	var totalCount int64

	for _, row := range data {
		if len(row) == 0 {
			continue
		}

		var setClauses []string
		var whereClauses []string
		var values []interface{}
		i := 1

		// Build SET clause
		for col, val := range row {
			isWhereCol := false
			for _, whereCol := range whereColumns {
				if col == whereCol {
					isWhereCol = true
					break
				}
			}

			if !isWhereCol {
				setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, i))
				values = append(values, val)
				i++
			}
		}

		// Build WHERE clause
		for _, whereCol := range whereColumns {
			if val, exists := row[whereCol]; exists {
				whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", whereCol, i))
				values = append(values, val)
				i++
			}
		}

		if len(setClauses) == 0 || len(whereClauses) == 0 {
			continue
		}

		query := fmt.Sprintf(
			"UPDATE %s SET %s WHERE %s",
			table,
			strings.Join(setClauses, ", "),
			strings.Join(whereClauses, " AND "),
		)

		result, err := d.conn.db.ExecContext(ctx, query, values...)
		if err != nil {
			return totalCount, fmt.Errorf("failed to update row: %w", err)
		}

		affected, err := result.RowsAffected()
		if err == nil {
			totalCount += affected
		}
	}

	return totalCount, nil
}

// Upsert inserts or updates data using ON CONFLICT.
func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	if len(data) == 0 || len(uniqueColumns) == 0 {
		return 0, fmt.Errorf("data and uniqueColumns cannot be empty")
	}

	var count int64

	for _, row := range data {
		if len(row) == 0 {
			continue
		}

		columns := make([]string, 0, len(row))
		placeholders := make([]string, 0, len(row))
		values := make([]interface{}, 0, len(row))
		updateClauses := make([]string, 0)
		i := 1

		for col, val := range row {
			columns = append(columns, col)
			placeholders = append(placeholders, fmt.Sprintf("$%d", i))
			values = append(values, val)

			// Add to update clause if not a unique column
			isUnique := false
			for _, uCol := range uniqueColumns {
				if col == uCol {
					isUnique = true
					break
				}
			}

			if !isUnique {
				updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
			}

			i++
		}

		conflictColumns := strings.Join(uniqueColumns, ", ")
		updateClause := ""
		if len(updateClauses) > 0 {
			updateClause = fmt.Sprintf("DO UPDATE SET %s", strings.Join(updateClauses, ", "))
		} else {
			updateClause = "DO NOTHING"
		}

		query := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) %s",
			table,
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "),
			conflictColumns,
			updateClause,
		)

		result, err := d.conn.db.ExecContext(ctx, query, values...)
		if err != nil {
			return count, fmt.Errorf("failed to upsert row: %w", err)
		}

		affected, err := result.RowsAffected()
		if err == nil {
			count += affected
		}
	}

	return count, nil
}

// Delete deletes rows from a table based on conditions.
func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
	if len(conditions) == 0 {
		return 0, fmt.Errorf("conditions cannot be empty")
	}

	var whereClauses []string
	var values []interface{}
	i := 1

	for col, val := range conditions {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", col, i))
		values = append(values, val)
		i++
	}

	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		table,
		strings.Join(whereClauses, " AND "),
	)

	result, err := d.conn.db.ExecContext(ctx, query, values...)
	if err != nil {
		return 0, fmt.Errorf("failed to delete rows: %w", err)
	}

	return result.RowsAffected()
}

// Stream retrieves data in batches.
func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	query := fmt.Sprintf("SELECT * FROM %s", params.Table)

	if params.Offset > 0 {
		// Use offset for pagination
		query += fmt.Sprintf(" OFFSET %d", params.Offset)
	}

	query += fmt.Sprintf(" LIMIT %d", params.BatchSize)

	rows, err := d.executeQuery(ctx, query)
	if err != nil {
		return adapter.StreamResult{}, err
	}

	hasMore := len(rows) == int(params.BatchSize)
	nextOffset := params.Offset + int64(len(rows))

	return adapter.StreamResult{
		Data:       rows,
		HasMore:    hasMore,
		NextCursor: fmt.Sprintf("%d", nextOffset),
	}, nil
}

// ExecuteQuery executes a custom SQL query.
func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	rows, err := d.executeQuery(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, len(rows))
	for i, row := range rows {
		result[i] = row
	}

	return result, nil
}

// ExecuteCountQuery executes a COUNT query.
func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	var count int64
	err := d.conn.db.QueryRowContext(ctx, query).Scan(&count)
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

	var count int64
	err := d.conn.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get row count: %w", err)
	}

	return count, true, nil
}

// Wipe deletes all data from a table.
func (d *DataOps) Wipe(ctx context.Context) error {
	// This would be dangerous in production - require explicit table name
	return fmt.Errorf("wipe operation requires explicit table name")
}

// executeQuery is a helper to execute queries and return results as maps.
func (d *DataOps) executeQuery(ctx context.Context, query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := d.conn.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var result []map[string]interface{}

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
			val := values[i]

			// Convert []byte to string for text fields
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}

		result = append(result, row)
	}

	return result, rows.Err()
}
