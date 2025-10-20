//go:build enterprise
// +build enterprise

package oracle

import (
	"context"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// DataOps implements adapter.DataOperator for Oracle.
type DataOps struct {
	conn *Connection
}

// Fetch retrieves data from a table with a limit.
func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM %s", QuoteIdentifier(table))
	if limit > 0 {
		query = fmt.Sprintf("SELECT * FROM %s WHERE ROWNUM <= %d", QuoteIdentifier(table), limit)
	}

	rows, err := d.conn.db.QueryContext(ctx, query)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Oracle, "fetch", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Oracle, "fetch", err)
	}

	var result []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, adapter.WrapError(dbcapabilities.Oracle, "fetch", err)
		}

		entry := make(map[string]interface{})
		for i, col := range columns {
			entry[col] = values[i]
		}
		result = append(result, entry)
	}

	return result, rows.Err()
}

// FetchWithColumns retrieves specific columns from a table.
func (d *DataOps) FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error) {
	if len(columns) == 0 {
		return d.Fetch(ctx, table, limit)
	}

	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = QuoteIdentifier(col)
	}

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(quotedColumns, ", "), QuoteIdentifier(table))
	if limit > 0 {
		query = fmt.Sprintf("SELECT * FROM (%s) WHERE ROWNUM <= %d", query, limit)
	}

	rows, err := d.conn.db.QueryContext(ctx, query)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Oracle, "fetch_with_columns", err)
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, adapter.WrapError(dbcapabilities.Oracle, "fetch_with_columns", err)
		}

		entry := make(map[string]interface{})
		for i, col := range columns {
			entry[col] = values[i]
		}
		result = append(result, entry)
	}

	return result, rows.Err()
}

// Insert inserts data into a table.
func (d *DataOps) Insert(ctx context.Context, table string, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	tx, err := d.conn.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Oracle, "insert", err)
	}
	defer tx.Rollback()

	var totalRowsAffected int64

	columns := make([]string, 0, len(data[0]))
	for col := range data[0] {
		columns = append(columns, col)
	}

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf(":%d", i+1) // Oracle uses :1, :2, :3...
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		QuoteIdentifier(table),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Oracle, "insert", err)
	}
	defer stmt.Close()

	for _, row := range data {
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			values[i] = row[col]
		}

		result, err := stmt.ExecContext(ctx, values...)
		if err != nil {
			return 0, adapter.WrapError(dbcapabilities.Oracle, "insert", err)
		}

		rowsAffected, _ := result.RowsAffected()
		totalRowsAffected += rowsAffected
	}

	if err := tx.Commit(); err != nil {
		return 0, adapter.WrapError(dbcapabilities.Oracle, "insert", err)
	}

	return totalRowsAffected, nil
}

// Update updates data in a table based on where columns.
func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	if len(data) == 0 || len(whereColumns) == 0 {
		return 0, nil
	}

	tx, err := d.conn.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Oracle, "update", err)
	}
	defer tx.Rollback()

	var totalRowsAffected int64

	for _, row := range data {
		var setClauses []string
		var values []interface{}
		paramNum := 1

		for col, val := range row {
			isWhereCol := false
			for _, whereCol := range whereColumns {
				if col == whereCol {
					isWhereCol = true
					break
				}
			}
			if !isWhereCol {
				setClauses = append(setClauses, fmt.Sprintf("%s = :%d", QuoteIdentifier(col), paramNum))
				values = append(values, val)
				paramNum++
			}
		}

		var whereClauses []string
		for _, whereCol := range whereColumns {
			val, exists := row[whereCol]
			if !exists {
				continue
			}
			if val == nil {
				whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", QuoteIdentifier(whereCol)))
			} else {
				whereClauses = append(whereClauses, fmt.Sprintf("%s = :%d", QuoteIdentifier(whereCol), paramNum))
				values = append(values, val)
				paramNum++
			}
		}

		if len(setClauses) == 0 {
			continue
		}

		query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			QuoteIdentifier(table),
			strings.Join(setClauses, ", "),
			strings.Join(whereClauses, " AND "))

		result, err := tx.ExecContext(ctx, query, values...)
		if err != nil {
			return 0, adapter.WrapError(dbcapabilities.Oracle, "update", err)
		}

		rowsAffected, _ := result.RowsAffected()
		totalRowsAffected += rowsAffected
	}

	if err := tx.Commit(); err != nil {
		return 0, adapter.WrapError(dbcapabilities.Oracle, "update", err)
	}

	return totalRowsAffected, nil
}

// Upsert inserts or updates data based on unique columns.
func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	if len(data) == 0 || len(uniqueColumns) == 0 {
		return 0, nil
	}

	tx, err := d.conn.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Oracle, "upsert", err)
	}
	defer tx.Rollback()

	var totalRowsAffected int64

	for _, row := range data {
		// Oracle uses MERGE statement for upsert
		var columns []string
		var values []interface{}
		for col, val := range row {
			columns = append(columns, col)
			values = append(values, val)
		}

		var matchConditions []string
		for _, uniqueCol := range uniqueColumns {
			matchConditions = append(matchConditions, fmt.Sprintf("T.%s = S.%s",
				QuoteIdentifier(uniqueCol), QuoteIdentifier(uniqueCol)))
		}

		var updateClauses []string
		for _, col := range columns {
			isUniqueCol := false
			for _, uniqueCol := range uniqueColumns {
				if col == uniqueCol {
					isUniqueCol = true
					break
				}
			}
			if !isUniqueCol {
				updateClauses = append(updateClauses, fmt.Sprintf("%s = S.%s",
					QuoteIdentifier(col), QuoteIdentifier(col)))
			}
		}

		placeholders := make([]string, len(columns))
		for i := range placeholders {
			placeholders[i] = fmt.Sprintf(":%d", i+1)
		}

		query := fmt.Sprintf(`
			MERGE INTO %s T
			USING (SELECT %s FROM DUAL) S
			ON (%s)
			WHEN MATCHED THEN UPDATE SET %s
			WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)`,
			QuoteIdentifier(table),
			strings.Join(placeholders, ", "),
			strings.Join(matchConditions, " AND "),
			strings.Join(updateClauses, ", "),
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "))

		// Values twice: once for USING, once for INSERT
		allValues := append(values, values...)
		result, err := tx.ExecContext(ctx, query, allValues...)
		if err != nil {
			return 0, adapter.WrapError(dbcapabilities.Oracle, "upsert", err)
		}

		rowsAffected, _ := result.RowsAffected()
		totalRowsAffected += rowsAffected
	}

	if err := tx.Commit(); err != nil {
		return 0, adapter.WrapError(dbcapabilities.Oracle, "upsert", err)
	}

	return totalRowsAffected, nil
}

// Delete deletes data from a table based on conditions.
func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
	if len(conditions) == 0 {
		return 0, adapter.NewDatabaseError(
			dbcapabilities.Oracle,
			"delete",
			adapter.ErrInvalidData,
		).WithContext("error", "conditions cannot be empty")
	}

	var whereClauses []string
	var values []interface{}
	paramNum := 1

	for col, val := range conditions {
		if val == nil {
			whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", QuoteIdentifier(col)))
		} else {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = :%d", QuoteIdentifier(col), paramNum))
			values = append(values, val)
			paramNum++
		}
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s",
		QuoteIdentifier(table),
		strings.Join(whereClauses, " AND "))

	result, err := d.conn.db.ExecContext(ctx, query, values...)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Oracle, "delete", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Oracle, "delete", err)
	}

	return rowsAffected, nil
}

// Stream streams data from a table.
func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	return adapter.StreamResult{}, adapter.NewUnsupportedOperationError(
		dbcapabilities.Oracle,
		"stream",
		"not yet implemented",
	)
}

// ExecuteQuery executes a raw SQL query.
func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	rows, err := d.conn.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Oracle, "execute_query", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Oracle, "execute_query", err)
	}

	var results []interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, adapter.WrapError(dbcapabilities.Oracle, "execute_query", err)
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}

	return results, rows.Err()
}

// ExecuteCountQuery executes a count query and returns the count.
func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	var count int64
	err := d.conn.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Oracle, "execute_count_query", err)
	}
	return count, nil
}

// GetRowCount returns the number of rows in a table matching the where clause.
func (d *DataOps) GetRowCount(ctx context.Context, table string, whereClause string) (int64, bool, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", QuoteIdentifier(table))
	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	var count int64
	err := d.conn.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, false, adapter.WrapError(dbcapabilities.Oracle, "get_row_count", err)
	}

	return count, true, nil
}

// Wipe removes all data from all tables in the database.
func (d *DataOps) Wipe(ctx context.Context) error {
	// Get all user tables
	query := "SELECT table_name FROM user_tables ORDER BY table_name"

	rows, err := d.conn.db.QueryContext(ctx, query)
	if err != nil {
		return adapter.WrapError(dbcapabilities.Oracle, "wipe", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return adapter.WrapError(dbcapabilities.Oracle, "wipe", err)
		}
		tables = append(tables, tableName)
	}

	// Truncate all tables (faster than delete)
	for _, table := range tables {
		_, err := d.conn.db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", QuoteIdentifier(table)))
		if err != nil {
			// If truncate fails, try delete
			_, err = d.conn.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", QuoteIdentifier(table)))
			if err != nil {
				return adapter.WrapError(dbcapabilities.Oracle, "wipe", err)
			}
		}
	}

	return nil
}
