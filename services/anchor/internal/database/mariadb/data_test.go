package mariadb

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestUpsertData(t *testing.T) {
	// Skip if no database connection available
	db, err := sql.Open("mysql", "test_user:test_pass@tcp(localhost:3306)/test_db")
	if err != nil {
		t.Skip("Skipping test - no database connection available")
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		t.Skip("Skipping test - no database connection available")
	}

	// Test table creation
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS test_upsert (
			id INT PRIMARY KEY,
			name VARCHAR(255),
			email VARCHAR(255) UNIQUE,
			age INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer db.Exec("DROP TABLE IF EXISTS test_upsert")

	t.Run("Empty data array", func(t *testing.T) {
		rowsAffected, err := UpsertData(db, "test_upsert", []map[string]interface{}{}, []string{"id"})
		if err != nil {
			t.Errorf("Expected no error for empty data, got: %v", err)
		}
		if rowsAffected != 0 {
			t.Errorf("Expected 0 rows affected, got: %d", rowsAffected)
		}
	})

	t.Run("Single row insert", func(t *testing.T) {
		data := []map[string]interface{}{
			{
				"id":    1,
				"name":  "John Doe",
				"email": "john@example.com",
				"age":   30,
			},
		}

		rowsAffected, err := UpsertData(db, "test_upsert", data, []string{"id"})
		if err != nil {
			t.Errorf("Failed to upsert data: %v", err)
		}
		if rowsAffected != 1 {
			t.Errorf("Expected 1 row affected, got: %d", rowsAffected)
		}
	})

	t.Run("Multiple rows insert", func(t *testing.T) {
		data := []map[string]interface{}{
			{
				"id":    2,
				"name":  "Jane Smith",
				"email": "jane@example.com",
				"age":   25,
			},
			{
				"id":    3,
				"name":  "Bob Johnson",
				"email": "bob@example.com",
				"age":   35,
			},
		}

		rowsAffected, err := UpsertData(db, "test_upsert", data, []string{"id"})
		if err != nil {
			t.Errorf("Failed to upsert data: %v", err)
		}
		if rowsAffected != 2 {
			t.Errorf("Expected 2 rows affected, got: %d", rowsAffected)
		}
	})

	t.Run("Update existing row", func(t *testing.T) {
		data := []map[string]interface{}{
			{
				"id":    1,
				"name":  "John Doe Updated",
				"email": "john.updated@example.com",
				"age":   31,
			},
		}

		rowsAffected, err := UpsertData(db, "test_upsert", data, []string{"id"})
		if err != nil {
			t.Errorf("Failed to upsert data: %v", err)
		}
		if rowsAffected != 2 { // 1 row updated
			t.Errorf("Expected 2 rows affected (1 updated), got: %d", rowsAffected)
		}
	})

	t.Run("Multiple unique columns", func(t *testing.T) {
		// Create table with composite unique key
		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS test_composite_upsert (
				id INT,
				email VARCHAR(255),
				name VARCHAR(255),
				age INT,
				UNIQUE KEY unique_id_email (id, email)
			)
		`)
		if err != nil {
			t.Fatalf("Failed to create test table: %v", err)
		}
		defer db.Exec("DROP TABLE IF EXISTS test_composite_upsert")

		data := []map[string]interface{}{
			{
				"id":    1,
				"email": "test1@example.com",
				"name":  "Test User 1",
				"age":   25,
			},
		}

		rowsAffected, err := UpsertData(db, "test_composite_upsert", data, []string{"id", "email"})
		if err != nil {
			t.Errorf("Failed to upsert data with composite key: %v", err)
		}
		if rowsAffected != 1 {
			t.Errorf("Expected 1 row affected, got: %d", rowsAffected)
		}
	})
}

func TestUpdateData(t *testing.T) {
	// Skip if no database connection available
	db, err := sql.Open("mysql", "test_user:test_pass@tcp(localhost:3306)/test_db")
	if err != nil {
		t.Skip("Skipping test - no database connection available")
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		t.Skip("Skipping test - no database connection available")
	}

	// Test table creation
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS test_update (
			id INT PRIMARY KEY,
			name VARCHAR(255),
			email VARCHAR(255),
			age INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer db.Exec("DROP TABLE IF EXISTS test_update")

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO test_update (id, name, email, age) VALUES 
		(1, 'John Doe', 'john@example.com', 30),
		(2, 'Jane Smith', 'jane@example.com', 25),
		(3, 'Bob Johnson', 'bob@example.com', 35)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	t.Run("Empty data array", func(t *testing.T) {
		rowsAffected, err := UpdateData(db, "test_update", []map[string]interface{}{}, []string{"id"})
		if err != nil {
			t.Errorf("Expected no error for empty data, got: %v", err)
		}
		if rowsAffected != 0 {
			t.Errorf("Expected 0 rows affected, got: %d", rowsAffected)
		}
	})

	t.Run("Single row update", func(t *testing.T) {
		data := []map[string]interface{}{
			{
				"id":    1,
				"name":  "John Doe Updated",
				"email": "john.updated@example.com",
				"age":   31,
			},
		}

		rowsAffected, err := UpdateData(db, "test_update", data, []string{"id"})
		if err != nil {
			t.Errorf("Failed to update data: %v", err)
		}
		if rowsAffected != 1 {
			t.Errorf("Expected 1 row affected, got: %d", rowsAffected)
		}
	})

	t.Run("Multiple rows update", func(t *testing.T) {
		data := []map[string]interface{}{
			{
				"id":    2,
				"name":  "Jane Smith Updated",
				"email": "jane.updated@example.com",
				"age":   26,
			},
			{
				"id":    3,
				"name":  "Bob Johnson Updated",
				"email": "bob.updated@example.com",
				"age":   36,
			},
		}

		rowsAffected, err := UpdateData(db, "test_update", data, []string{"id"})
		if err != nil {
			t.Errorf("Failed to update data: %v", err)
		}
		if rowsAffected != 2 {
			t.Errorf("Expected 2 rows affected, got: %d", rowsAffected)
		}
	})

	t.Run("Update with multiple where columns", func(t *testing.T) {
		// Create table with test data
		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS test_multi_where (
				id INT,
				email VARCHAR(255),
				name VARCHAR(255),
				age INT,
				PRIMARY KEY (id, email)
			)
		`)
		if err != nil {
			t.Fatalf("Failed to create test table: %v", err)
		}
		defer db.Exec("DROP TABLE IF EXISTS test_multi_where")

		// Insert test data
		_, err = db.Exec(`
			INSERT INTO test_multi_where (id, email, name, age) VALUES 
			(1, 'test1@example.com', 'User 1', 25),
			(1, 'test2@example.com', 'User 2', 30)
		`)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}

		data := []map[string]interface{}{
			{
				"id":    1,
				"email": "test1@example.com",
				"name":  "User 1 Updated",
				"age":   26,
			},
		}

		rowsAffected, err := UpdateData(db, "test_multi_where", data, []string{"id", "email"})
		if err != nil {
			t.Errorf("Failed to update data with multiple where columns: %v", err)
		}
		if rowsAffected != 1 {
			t.Errorf("Expected 1 row affected, got: %d", rowsAffected)
		}
	})

	t.Run("Update non-existent row", func(t *testing.T) {
		data := []map[string]interface{}{
			{
				"id":    999,
				"name":  "Non-existent User",
				"email": "nonexistent@example.com",
				"age":   50,
			},
		}

		rowsAffected, err := UpdateData(db, "test_update", data, []string{"id"})
		if err != nil {
			t.Errorf("Failed to update non-existent data: %v", err)
		}
		if rowsAffected != 0 {
			t.Errorf("Expected 0 rows affected for non-existent row, got: %d", rowsAffected)
		}
	})
}

func TestFetchData(t *testing.T) {
	// Skip if no database connection available
	db, err := sql.Open("mysql", "test_user:test_pass@tcp(localhost:3306)/test_db")
	if err != nil {
		t.Skip("Skipping test - no database connection available")
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		t.Skip("Skipping test - no database connection available")
	}

	// Test table creation
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS test_fetch (
			id INT PRIMARY KEY,
			name VARCHAR(255),
			email VARCHAR(255)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer db.Exec("DROP TABLE IF EXISTS test_fetch")

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO test_fetch (id, name, email) VALUES 
		(1, 'John Doe', 'john@example.com'),
		(2, 'Jane Smith', 'jane@example.com')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	t.Run("Fetch all data", func(t *testing.T) {
		data, err := FetchData(db, "test_fetch", 0)
		if err != nil {
			t.Errorf("Failed to fetch data: %v", err)
		}
		if len(data) != 2 {
			t.Errorf("Expected 2 rows, got: %d", len(data))
		}
	})

	t.Run("Fetch with limit", func(t *testing.T) {
		data, err := FetchData(db, "test_fetch", 1)
		if err != nil {
			t.Errorf("Failed to fetch data with limit: %v", err)
		}
		if len(data) != 1 {
			t.Errorf("Expected 1 row, got: %d", len(data))
		}
	})

	t.Run("Empty table", func(t *testing.T) {
		// Create empty table
		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS test_empty (
				id INT PRIMARY KEY,
				name VARCHAR(255)
			)
		`)
		if err != nil {
			t.Fatalf("Failed to create empty test table: %v", err)
		}
		defer db.Exec("DROP TABLE IF EXISTS test_empty")

		data, err := FetchData(db, "test_empty", 0)
		if err != nil {
			t.Errorf("Failed to fetch from empty table: %v", err)
		}
		if len(data) != 0 {
			t.Errorf("Expected 0 rows from empty table, got: %d", len(data))
		}
	})
}

func TestInsertData(t *testing.T) {
	// Skip if no database connection available
	db, err := sql.Open("mysql", "test_user:test_pass@tcp(localhost:3306)/test_db")
	if err != nil {
		t.Skip("Skipping test - no database connection available")
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		t.Skip("Skipping test - no database connection available")
	}

	// Test table creation
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS test_insert (
			id INT PRIMARY KEY,
			name VARCHAR(255),
			email VARCHAR(255)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer db.Exec("DROP TABLE IF EXISTS test_insert")

	t.Run("Empty data array", func(t *testing.T) {
		rowsAffected, err := InsertData(db, "test_insert", []map[string]interface{}{})
		if err != nil {
			t.Errorf("Expected no error for empty data, got: %v", err)
		}
		if rowsAffected != 0 {
			t.Errorf("Expected 0 rows affected, got: %d", rowsAffected)
		}
	})

	t.Run("Single row insert", func(t *testing.T) {
		data := []map[string]interface{}{
			{
				"id":    1,
				"name":  "John Doe",
				"email": "john@example.com",
			},
		}

		rowsAffected, err := InsertData(db, "test_insert", data)
		if err != nil {
			t.Errorf("Failed to insert data: %v", err)
		}
		if rowsAffected != 1 {
			t.Errorf("Expected 1 row affected, got: %d", rowsAffected)
		}
	})

	t.Run("Multiple rows insert", func(t *testing.T) {
		data := []map[string]interface{}{
			{
				"id":    2,
				"name":  "Jane Smith",
				"email": "jane@example.com",
			},
			{
				"id":    3,
				"name":  "Bob Johnson",
				"email": "bob@example.com",
			},
		}

		rowsAffected, err := InsertData(db, "test_insert", data)
		if err != nil {
			t.Errorf("Failed to insert data: %v", err)
		}
		if rowsAffected != 2 {
			t.Errorf("Expected 2 rows affected, got: %d", rowsAffected)
		}
	})
}
