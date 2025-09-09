package clickhouse

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

// FetchData retrieves data from a specified table
func FetchData(conn ClickhouseConn, tableName string, limit int) ([]map[string]interface{}, error) {
	if tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	// Get columns for the table
	columns, err := getColumns(conn, tableName)
	if err != nil {
		return nil, err
	}

	// Build and execute query
	query := fmt.Sprintf("SELECT %s FROM %s",
		strings.Join(columns, ", "),
		QuoteIdentifier(tableName))
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("error querying table %s: %v", tableName, err)
	}
	defer rows.Close()

	// Get column types
	columnTypes := rows.ColumnTypes()

	var result []map[string]interface{}
	for rows.Next() {
		// Create a slice of interface{} to hold the values
		valuePtrs := make([]interface{}, len(columns))

		// Initialize with appropriate types based on column types
		for i, ct := range columnTypes {
			switch ct.ScanType().Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				var v int64
				valuePtrs[i] = &v
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				var v uint64
				valuePtrs[i] = &v
			case reflect.Float32, reflect.Float64:
				var v float64
				valuePtrs[i] = &v
			case reflect.Bool:
				var v bool
				valuePtrs[i] = &v
			default:
				var v string
				valuePtrs[i] = &v
			}
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}

		entry := make(map[string]interface{})
		for i, col := range columns {
			entry[col] = valuePtrs[i]
		}
		result = append(result, entry)
	}

	return result, nil
}

// InsertData inserts data into a specified table
func InsertData(conn ClickhouseConn, tableName string, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	ctx := context.Background()

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
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		QuoteIdentifier(tableName),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	// Batch insert
	batch, err := conn.PrepareBatch(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("error preparing batch: %v", err)
	}

	// Insert each row
	for _, row := range data {
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			values[i] = row[col]
		}

		if err := batch.Append(values...); err != nil {
			return 0, fmt.Errorf("error appending to batch: %v", err)
		}
	}

	// Execute the batch
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("error sending batch: %v", err)
	}

	return int64(len(data)), nil
}

// WipeDatabase removes all data from the database
func WipeDatabase(conn ClickhouseConn) error {
	// Get all tables in the current database
	query := `
		SELECT name 
		FROM system.tables 
		WHERE database = currentDatabase()
		AND engine != 'View'
		AND engine != 'MaterializedView'
	`

	rows, err := conn.Query(context.Background(), query)
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
		err = conn.Exec(context.Background(), fmt.Sprintf("TRUNCATE TABLE %s", QuoteIdentifier(table)))
		if err != nil {
			return fmt.Errorf("error truncating table %s: %v", table, err)
		}
	}

	return nil
}

func getColumns(conn ClickhouseConn, tableName string) ([]string, error) {
	query := `
		SELECT name 
		FROM system.columns 
		WHERE database = currentDatabase() 
		AND table = ?
		ORDER BY position
	`

	rows, err := conn.Query(context.Background(), query, tableName)
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
