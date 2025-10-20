//go:build enterprise
// +build enterprise

package db2

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// DataOps implements adapter.DataOperator for IBM DB2.
type DataOps struct {
	conn *Connection
}

// Fetch retrieves data from a table with a limit.
func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
	result, err := FetchData(d.conn.db, table, limit)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.DB2, "fetch", err)
	}
	return result, nil
}

// FetchWithColumns retrieves specific columns from a table.
func (d *DataOps) FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error) {
	if table == "" {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.DB2,
			"fetch_with_columns",
			adapter.ErrInvalidData,
		).WithContext("error", "table name cannot be empty")
	}

	if len(columns) == 0 {
		return d.Fetch(ctx, table, limit)
	}

	// Build query with specified columns
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = QuoteIdentifier(col)
	}

	query := fmt.Sprintf("SELECT %s FROM %s",
		strings.Join(quotedColumns, ", "),
		QuoteIdentifier(table))

	if limit > 0 {
		query += fmt.Sprintf(" FETCH FIRST %d ROWS ONLY", limit)
	}

	rows, err := d.conn.db.QueryContext(ctx, query)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.DB2, "fetch_with_columns", err)
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
			return nil, adapter.WrapError(dbcapabilities.DB2, "fetch_with_columns", err)
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
	count, err := InsertData(d.conn.db, table, data)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.DB2, "insert", err)
	}
	return count, nil
}

// Update updates data in a table based on where columns.
func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	if len(whereColumns) == 0 {
		return 0, adapter.NewDatabaseError(
			dbcapabilities.DB2,
			"update",
			adapter.ErrInvalidData,
		).WithContext("error", "where columns cannot be empty")
	}

	tx, err := d.conn.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.DB2, "update", err)
	}
	defer tx.Rollback()

	var totalRowsAffected int64

	for _, row := range data {
		// Build SET clause
		var setClauses []string
		var setValues []interface{}
		for col, val := range row {
			// Skip where columns in SET clause
			isWhereCol := false
			for _, whereCol := range whereColumns {
				if col == whereCol {
					isWhereCol = true
					break
				}
			}
			if !isWhereCol {
				setClauses = append(setClauses, QuoteIdentifier(col)+" = ?")
				setValues = append(setValues, val)
			}
		}

		// Build WHERE clause
		var whereClauses []string
		for _, whereCol := range whereColumns {
			val, exists := row[whereCol]
			if !exists {
				continue
			}
			if val == nil {
				whereClauses = append(whereClauses, QuoteIdentifier(whereCol)+" IS NULL")
			} else {
				whereClauses = append(whereClauses, QuoteIdentifier(whereCol)+" = ?")
				setValues = append(setValues, val)
			}
		}

		if len(setClauses) == 0 {
			continue
		}

		query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			QuoteIdentifier(table),
			strings.Join(setClauses, ", "),
			strings.Join(whereClauses, " AND "))

		result, err := tx.ExecContext(ctx, query, setValues...)
		if err != nil {
			return 0, adapter.WrapError(dbcapabilities.DB2, "update", err)
		}

		rowsAffected, _ := result.RowsAffected()
		totalRowsAffected += rowsAffected
	}

	if err := tx.Commit(); err != nil {
		return 0, adapter.WrapError(dbcapabilities.DB2, "update", err)
	}

	return totalRowsAffected, nil
}

// Upsert inserts or updates data based on unique columns.
func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	if len(uniqueColumns) == 0 {
		return 0, adapter.NewDatabaseError(
			dbcapabilities.DB2,
			"upsert",
			adapter.ErrInvalidData,
		).WithContext("error", "unique columns cannot be empty")
	}

	tx, err := d.conn.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.DB2, "upsert", err)
	}
	defer tx.Rollback()

	var totalRowsAffected int64

	for _, row := range data {
		// DB2 uses MERGE statement for upsert
		// Build column lists
		var columns []string
		var values []interface{}
		for col, val := range row {
			columns = append(columns, col)
			values = append(values, val)
		}

		// Build placeholders
		placeholders := make([]string, len(columns))
		for i := range placeholders {
			placeholders[i] = "?"
		}

		// Build MERGE statement
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

		query := fmt.Sprintf(`
			MERGE INTO %s AS T
			USING (SELECT %s FROM SYSIBM.SYSDUMMY1) AS S (%s)
			ON %s
			WHEN MATCHED THEN UPDATE SET %s
			WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)`,
			QuoteIdentifier(table),
			strings.Join(placeholders, ", "),
			strings.Join(columns, ", "),
			strings.Join(matchConditions, " AND "),
			strings.Join(updateClauses, ", "),
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "))

		// Execute with values twice (once for USING, once for INSERT)
		allValues := append(values, values...)
		result, err := tx.ExecContext(ctx, query, allValues...)
		if err != nil {
			return 0, adapter.WrapError(dbcapabilities.DB2, "upsert", err)
		}

		rowsAffected, _ := result.RowsAffected()
		totalRowsAffected += rowsAffected
	}

	if err := tx.Commit(); err != nil {
		return 0, adapter.WrapError(dbcapabilities.DB2, "upsert", err)
	}

	return totalRowsAffected, nil
}

// Delete deletes data from a table based on conditions.
func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
	if len(conditions) == 0 {
		return 0, adapter.NewDatabaseError(
			dbcapabilities.DB2,
			"delete",
			adapter.ErrInvalidData,
		).WithContext("error", "conditions cannot be empty")
	}

	var whereClauses []string
	var values []interface{}

	for col, val := range conditions {
		if val == nil {
			whereClauses = append(whereClauses, QuoteIdentifier(col)+" IS NULL")
		} else {
			whereClauses = append(whereClauses, QuoteIdentifier(col)+" = ?")
			values = append(values, val)
		}
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s",
		QuoteIdentifier(table),
		strings.Join(whereClauses, " AND "))

	result, err := d.conn.db.ExecContext(ctx, query, values...)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.DB2, "delete", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.DB2, "delete", err)
	}

	return rowsAffected, nil
}

// Stream streams data from a table (not fully implemented for DB2).
func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	return adapter.StreamResult{}, adapter.NewUnsupportedOperationError(dbcapabilities.DB2, "stream", "not yet implemented")
}

// ExecuteQuery executes a raw SQL query.
func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	rows, err := d.conn.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.DB2, "execute_query", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.DB2, "execute_query", err)
	}

	var results []interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, adapter.WrapError(dbcapabilities.DB2, "execute_query", err)
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
		return 0, adapter.WrapError(dbcapabilities.DB2, "execute_count_query", err)
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
		return 0, false, adapter.WrapError(dbcapabilities.DB2, "get_row_count", err)
	}

	return count, true, nil
}

// Wipe removes all data from all tables in the database.
func (d *DataOps) Wipe(ctx context.Context) error {
	// Get all user tables
	query := `
		SELECT TABNAME 
		FROM SYSCAT.TABLES 
		WHERE TABSCHEMA = CURRENT_SCHEMA 
		AND TYPE = 'T'
	`

	rows, err := d.conn.db.QueryContext(ctx, query)
	if err != nil {
		return adapter.WrapError(dbcapabilities.DB2, "wipe", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return adapter.WrapError(dbcapabilities.DB2, "wipe", err)
		}
		tables = append(tables, tableName)
	}

	// Delete all data from each table
	for _, table := range tables {
		_, err := d.conn.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", QuoteIdentifier(table)))
		if err != nil {
			return adapter.WrapError(dbcapabilities.DB2, "wipe", err)
		}
	}

	return nil
}

// FetchData retrieves data from a specified table (helper function)
func FetchData(db *sql.DB, tableName string, limit int) ([]map[string]interface{}, error) {
	if tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	// Get columns for the table
	columns, err := getColumns(db, tableName)
	if err != nil {
		return nil, err
	}

	// Build and execute query
	query := fmt.Sprintf("SELECT %s FROM %s",
		strings.Join(columns, ", "),
		QuoteIdentifier(tableName))
	if limit > 0 {
		query += fmt.Sprintf(" FETCH FIRST %d ROWS ONLY", limit)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying table %s: %v", tableName, err)
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
			return nil, fmt.Errorf("error scanning row: %v", err)
		}

		entry := make(map[string]interface{})
		for i, col := range columns {
			entry[col] = values[i]
		}
		result = append(result, entry)
	}

	return result, nil
}

// InsertData inserts data into a specified table (helper function)
func InsertData(db *sql.DB, tableName string, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var totalRowsAffected int64

	// Get columns from the first row
	columns := make([]string, 0, len(data[0]))
	for col := range data[0] {
		columns = append(columns, col)
	}

	// Create placeholders for the prepared statement
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = "?"
	}

	// Prepare the statement
	stmt, err := tx.Prepare(fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		QuoteIdentifier(tableName),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	))
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	// Insert each row
	for _, row := range data {
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			values[i] = row[col]
		}

		result, err := stmt.Exec(values...)
		if err != nil {
			return 0, err
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		totalRowsAffected += rowsAffected
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return totalRowsAffected, nil
}

func getColumns(db *sql.DB, tableName string) ([]string, error) {
	// Parse schema and table name
	parts := strings.Split(tableName, ".")
	var schema, table string
	if len(parts) > 1 {
		schema = parts[0]
		table = parts[1]
	} else {
		// Use current schema if not specified
		err := db.QueryRow("VALUES CURRENT SCHEMA").Scan(&schema)
		if err != nil {
			return nil, fmt.Errorf("error getting current schema: %v", err)
		}
		table = parts[0]
	}

	query := `
		SELECT COLNAME 
		FROM SYSCAT.COLUMNS 
		WHERE TABSCHEMA = ? AND TABNAME = ? 
		ORDER BY COLNO`

	rows, err := db.Query(query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("error querying columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("error scanning column: %v", err)
		}
		columns = append(columns, column)
	}

	return columns, rows.Err()
}
