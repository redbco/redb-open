package postgres

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

// UpsertData inserts or updates data in a specified table based on unique constraints
func UpsertData(pool *pgxpool.Pool, tableName string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
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

	// Build the ON CONFLICT clause
	conflictColumns := strings.Join(uniqueColumns, ", ")
	updateSet := make([]string, 0, len(columns)-len(uniqueColumns))
	for _, col := range columns {
		isUnique := false
		for _, uniqueCol := range uniqueColumns {
			if col == uniqueCol {
				isUnique = true
				break
			}
		}
		if !isUnique {
			updateSet = append(updateSet, fmt.Sprintf("%s = EXCLUDED.%s", common.QuoteIdentifier(col), common.QuoteIdentifier(col)))
		}
	}

	// Prepare the query
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
		common.QuoteIdentifier(tableName),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		conflictColumns,
		strings.Join(updateSet, ", "),
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

// UpdateData updates existing data in a specified table based on a condition
func UpdateData(pool *pgxpool.Pool, tableName string, data []map[string]interface{}, whereColumns []string) (int64, error) {
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

	// Build the WHERE clause
	whereConditions := make([]string, len(whereColumns))
	for i, col := range whereColumns {
		whereConditions[i] = fmt.Sprintf("%s = $%d", common.QuoteIdentifier(col), len(columns)+i+1)
	}

	// Build the SET clause
	setClause := make([]string, 0, len(columns)-len(whereColumns))
	for i, col := range columns {
		isWhereColumn := false
		for _, whereCol := range whereColumns {
			if col == whereCol {
				isWhereColumn = true
				break
			}
		}
		if !isWhereColumn {
			setClause = append(setClause, fmt.Sprintf("%s = $%d", common.QuoteIdentifier(col), i+1))
		}
	}

	// Prepare the query
	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		common.QuoteIdentifier(tableName),
		strings.Join(setClause, ", "),
		strings.Join(whereConditions, " AND "),
	)

	// Update each row
	for _, row := range data {
		values := make([]interface{}, 0, len(columns))

		// Add SET values first
		for _, col := range columns {
			isWhereColumn := false
			for _, whereCol := range whereColumns {
				if col == whereCol {
					isWhereColumn = true
					break
				}
			}
			if !isWhereColumn {
				values = append(values, row[col])
			}
		}

		// Add WHERE values
		for _, whereCol := range whereColumns {
			values = append(values, row[whereCol])
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

// WipeDatabase removes all data and objects from the database
func WipeDatabase(pool *pgxpool.Pool) error {
	// Start a transaction
	tx, err := pool.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback(context.Background())

	// Check if PL/pgSQL is available and install it if not
	var plpgsqlExists bool
	err = tx.QueryRow(context.Background(), "SELECT EXISTS(SELECT 1 FROM pg_language WHERE lanname = 'plpgsql')").Scan(&plpgsqlExists)
	if err != nil {
		return fmt.Errorf("error checking for PL/pgSQL: %v", err)
	}
	if !plpgsqlExists {
		_, err = tx.Exec(context.Background(), "CREATE LANGUAGE plpgsql")
		if err != nil {
			return fmt.Errorf("error creating PL/pgSQL language: %v", err)
		}
	}

	// Disable foreign key checks
	_, err = tx.Exec(context.Background(), "SET CONSTRAINTS ALL DEFERRED")
	if err != nil {
		return fmt.Errorf("error disabling foreign key checks: %v", err)
	}

	// Drop tables
	_, err = tx.Exec(context.Background(), `
		DO $$ DECLARE
			r RECORD;
		BEGIN
			FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
				EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
			END LOOP;
		END $$;
	`)
	if err != nil {
		return fmt.Errorf("error dropping tables: %v", err)
	}

	// Drop views
	_, err = tx.Exec(context.Background(), `
		DO $$ DECLARE
			r RECORD;
		BEGIN
			FOR r IN (SELECT table_name FROM information_schema.views WHERE table_schema = 'public') LOOP
				EXECUTE 'DROP VIEW IF EXISTS ' || quote_ident(r.table_name) || ' CASCADE';
			END LOOP;
		END $$;
	`)
	if err != nil {
		return fmt.Errorf("error dropping views: %v", err)
	}

	// Drop materialized views
	_, err = tx.Exec(context.Background(), `
		DO $$ DECLARE
			r RECORD;
		BEGIN
			FOR r IN (SELECT matviewname FROM pg_matviews WHERE schemaname = 'public') LOOP
				EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS ' || quote_ident(r.matviewname) || ' CASCADE';
			END LOOP;
		END $$;
	`)
	if err != nil {
		return fmt.Errorf("error dropping materialized views: %v", err)
	}

	// Drop triggers
	_, err = tx.Exec(context.Background(), `
		DO $$ DECLARE
			r RECORD;
		BEGIN
			FOR r IN (SELECT trigger_name, event_object_table FROM information_schema.triggers WHERE trigger_schema = 'public') LOOP
				EXECUTE 'DROP TRIGGER IF EXISTS ' || quote_ident(r.trigger_name) || ' ON ' || quote_ident(r.event_object_table) || ' CASCADE';
			END LOOP;
		END $$;
	`)
	if err != nil {
		return fmt.Errorf("error dropping triggers: %v", err)
	}

	// Drop sequences
	_, err = tx.Exec(context.Background(), `
		DO $$ DECLARE
			r RECORD;
		BEGIN
			FOR r IN (SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'public') LOOP
				EXECUTE 'DROP SEQUENCE IF EXISTS ' || quote_ident(r.sequence_name) || ' CASCADE';
			END LOOP;
		END $$;
	`)
	if err != nil {
		return fmt.Errorf("error dropping sequences: %v", err)
	}

	// Drop enum types
	_, err = tx.Exec(context.Background(), `
		DO $$ DECLARE
			r RECORD;
		BEGIN
			FOR r IN (SELECT typname FROM pg_type WHERE typtype = 'e' AND typnamespace = 'public'::regnamespace) LOOP
				EXECUTE 'DROP TYPE IF EXISTS ' || quote_ident(r.typname) || ' CASCADE';
			END LOOP;
		END $$;
	`)
	if err != nil {
		return fmt.Errorf("error dropping enum types: %v", err)
	}

	// Drop functions (excluding those from extensions)
	_, err = tx.Exec(context.Background(), `
		DO $$ DECLARE
			r RECORD;
		BEGIN
			FOR r IN (
				SELECT p.proname, p.oid::regprocedure AS fullname
				FROM pg_proc p
				LEFT JOIN pg_extension e ON e.extnamespace = p.pronamespace
				WHERE p.pronamespace = 'public'::regnamespace
				AND e.extname IS NULL  -- This excludes functions from extensions
			) LOOP
				EXECUTE 'DROP FUNCTION IF EXISTS ' || r.fullname || ' CASCADE';
			END LOOP;
		END $$;
	`)
	if err != nil {
		return fmt.Errorf("error dropping functions: %v", err)
	}

	// Drop extensions
	_, err = tx.Exec(context.Background(), `
		DO $$ DECLARE
			r RECORD;
		BEGIN
			FOR r IN (SELECT extname FROM pg_extension) LOOP
				EXECUTE 'DROP EXTENSION IF EXISTS ' || quote_ident(r.extname) || ' CASCADE';
			END LOOP;
		END $$;
	`)
	if err != nil {
		return fmt.Errorf("error dropping extensions: %v", err)
	}

	// Commit the transaction
	if err := tx.Commit(context.Background()); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
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
