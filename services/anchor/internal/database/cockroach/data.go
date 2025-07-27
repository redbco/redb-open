package cockroach

import (
	"context"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/anchor/internal/database/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

// FetchData retrieves data from a specified table
func FetchData(pool *pgxpool.Pool, tableName string, limit int) ([]map[string]interface{}, error) {
	if tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	// Get columns for the table
	columns, err := getColumns(pool, tableName)
	if err != nil {
		return nil, err
	}

	// Build and execute query
	query := fmt.Sprintf("SELECT %s FROM %s",
		strings.Join(columns, ", "),
		common.QuoteIdentifier(tableName))
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := pool.Query(context.Background(), query)
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

// InsertData inserts data into a specified table
func InsertData(pool *pgxpool.Pool, tableName string, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Start a transaction
	tx, err := pool.Begin(context.Background())
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background())

	var totalRowsAffected int64

	// Get columns from the first row
	columns := make([]string, 0, len(data[0]))
	for col := range data[0] {
		columns = append(columns, col)
	}

	// Create placeholders for the prepared statement
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	// Prepare the query
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		common.QuoteIdentifier(tableName),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	// Insert each row
	for _, row := range data {
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			values[i] = row[col]
		}

		result, err := tx.Exec(context.Background(), query, values...)
		if err != nil {
			return 0, err
		}

		rowsAffected := result.RowsAffected()
		totalRowsAffected += rowsAffected
	}

	// Commit the transaction
	if err := tx.Commit(context.Background()); err != nil {
		return 0, err
	}

	return totalRowsAffected, nil
}

// WipeDatabase removes all data from the database
func WipeDatabase(pool *pgxpool.Pool) error {
	// Get all tables in the public schema
	query := `
		SELECT tablename 
		FROM pg_tables 
		WHERE schemaname = 'public';`

	rows, err := pool.Query(context.Background(), query)
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
	if len(tables) > 0 {
		tableList := strings.Join(tables, ", ")
		_, err = pool.Exec(context.Background(), fmt.Sprintf("TRUNCATE TABLE %s CASCADE;", tableList))
		if err != nil {
			return err
		}
	}

	return nil
}

func getColumns(pool *pgxpool.Pool, tableName string) ([]string, error) {
	query := "SELECT column_name FROM information_schema.columns WHERE table_name = $1"
	rows, err := pool.Query(context.Background(), query, tableName)
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
