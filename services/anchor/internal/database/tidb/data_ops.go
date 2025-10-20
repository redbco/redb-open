package tidb

import (
	"context"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// DataOps implements adapter.DataOperator for TiDB.
type DataOps struct {
	conn *Connection
}

// Fetch retrieves data from a table.
func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
	return d.FetchWithColumns(ctx, table, nil, limit)
}

// FetchWithColumns retrieves specific columns from a table.
func (d *DataOps) FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error) {
	if !d.conn.IsConnected() {
		return nil, adapter.ErrConnectionClosed
	}

	// Build SELECT query
	columnStr := "*"
	if len(columns) > 0 {
		columnStr = strings.Join(columns, ", ")
	}

	query := fmt.Sprintf("SELECT %s FROM `%s` LIMIT ?", columnStr, table)
	rows, err := d.conn.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch data: %w", err)
	}
	defer rows.Close()

	// Get column names
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Scan results
	results := make([]map[string]interface{}, 0)
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range cols {
			val := values[i]
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	return results, rows.Err()
}

// Insert inserts new rows into a table.
func (d *DataOps) Insert(ctx context.Context, table string, data []map[string]interface{}) (int64, error) {
	if !d.conn.IsConnected() {
		return 0, adapter.ErrConnectionClosed
	}

	if len(data) == 0 {
		return 0, nil
	}

	tx, err := d.conn.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var count int64
	for _, row := range data {
		// Build INSERT statement
		columns := make([]string, 0, len(row))
		placeholders := make([]string, 0, len(row))
		values := make([]interface{}, 0, len(row))

		for col, val := range row {
			columns = append(columns, fmt.Sprintf("`%s`", col))
			placeholders = append(placeholders, "?")
			values = append(values, val)
		}

		query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)",
			table, strings.Join(columns, ", "), strings.Join(placeholders, ", "))

		result, err := tx.ExecContext(ctx, query, values...)
		if err != nil {
			return 0, err
		}

		affected, _ := result.RowsAffected()
		count += affected
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return count, nil
}

// Update updates existing rows in a table.
func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	if !d.conn.IsConnected() {
		return 0, adapter.ErrConnectionClosed
	}

	var count int64
	for _, row := range data {
		// Build UPDATE statement
		setClause := []string{}
		whereClause := []string{}
		values := []interface{}{}

		for col, val := range row {
			isWhereColumn := false
			for _, wc := range whereColumns {
				if col == wc {
					isWhereColumn = true
					break
				}
			}

			if isWhereColumn {
				whereClause = append(whereClause, fmt.Sprintf("`%s` = ?", col))
				values = append(values, val)
			} else {
				setClause = append(setClause, fmt.Sprintf("`%s` = ?", col))
			}
		}

		// Add SET values to the end
		for col, val := range row {
			isWhereColumn := false
			for _, wc := range whereColumns {
				if col == wc {
					isWhereColumn = true
					break
				}
			}
			if !isWhereColumn {
				values = append(values, val)
			}
		}

		if len(setClause) == 0 || len(whereClause) == 0 {
			continue
		}

		query := fmt.Sprintf("UPDATE `%s` SET %s WHERE %s",
			table, strings.Join(setClause, ", "), strings.Join(whereClause, " AND "))

		result, err := d.conn.db.ExecContext(ctx, query, values...)
		if err != nil {
			return count, err
		}

		affected, _ := result.RowsAffected()
		count += affected
	}

	return count, nil
}

// Upsert performs an upsert operation.
func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	// MySQL/TiDB supports ON DUPLICATE KEY UPDATE
	if !d.conn.IsConnected() {
		return 0, adapter.ErrConnectionClosed
	}

	var count int64
	for _, row := range data {
		columns := make([]string, 0, len(row))
		placeholders := make([]string, 0, len(row))
		updateClauses := make([]string, 0)
		values := make([]interface{}, 0, len(row))

		for col, val := range row {
			columns = append(columns, fmt.Sprintf("`%s`", col))
			placeholders = append(placeholders, "?")
			values = append(values, val)
			updateClauses = append(updateClauses, fmt.Sprintf("`%s` = VALUES(`%s`)", col, col))
		}

		query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
			table, strings.Join(columns, ", "), strings.Join(placeholders, ", "), strings.Join(updateClauses, ", "))

		result, err := d.conn.db.ExecContext(ctx, query, values...)
		if err != nil {
			return count, err
		}

		affected, _ := result.RowsAffected()
		count += affected
	}

	return count, nil
}

// Delete deletes rows from a table.
func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
	if !d.conn.IsConnected() {
		return 0, adapter.ErrConnectionClosed
	}

	whereClauses := []string{}
	values := []interface{}{}

	for col, val := range conditions {
		whereClauses = append(whereClauses, fmt.Sprintf("`%s` = ?", col))
		values = append(values, val)
	}

	query := fmt.Sprintf("DELETE FROM `%s` WHERE %s", table, strings.Join(whereClauses, " AND "))

	result, err := d.conn.db.ExecContext(ctx, query, values...)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

// Stream streams data from a table.
func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	if !d.conn.IsConnected() {
		return adapter.StreamResult{}, adapter.ErrConnectionClosed
	}

	// Simplified implementation - returns empty result
	return adapter.StreamResult{}, nil
}

// ExecuteQuery executes a generic SQL query.
func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	if !d.conn.IsConnected() {
		return nil, adapter.ErrConnectionClosed
	}

	rows, err := d.conn.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	results := make([]interface{}, 0)
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range cols {
			val := values[i]
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	return results, rows.Err()
}

// ExecuteCountQuery executes a count query.
func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	if !d.conn.IsConnected() {
		return 0, adapter.ErrConnectionClosed
	}

	var count int64
	err := d.conn.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

// GetRowCount returns the number of rows in a table.
func (d *DataOps) GetRowCount(ctx context.Context, table string, whereClause string) (int64, bool, error) {
	if !d.conn.IsConnected() {
		return 0, false, adapter.ErrConnectionClosed
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	var count int64
	err := d.conn.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, false, err
	}

	return count, true, nil
}

// Wipe deletes all data from the database.
func (d *DataOps) Wipe(ctx context.Context) error {
	if !d.conn.IsConnected() {
		return adapter.ErrConnectionClosed
	}

	// Get all tables
	rows, err := d.conn.db.QueryContext(ctx,
		"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?",
		d.conn.config.DatabaseName,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return err
		}
		tables = append(tables, tableName)
	}

	// Truncate all tables
	for _, table := range tables {
		_, err := d.conn.db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE `%s`", table))
		if err != nil {
			return err
		}
	}

	return nil
}
