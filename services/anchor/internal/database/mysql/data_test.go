package mysql

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func setupTestDB(t *testing.T) *sql.DB {
	// Connect to test database
	db, err := sql.Open("mysql", "root:password@tcp(localhost:3306)/testdb?parseTime=true")
	if err != nil {
		t.Skipf("Skipping test - could not connect to MySQL: %v", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		t.Skipf("Skipping test - could not ping MySQL: %v", err)
	}

	// Create test table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS test_users (
			id INT AUTO_INCREMENT PRIMARY KEY,
			email VARCHAR(255) UNIQUE NOT NULL,
			name VARCHAR(255) NOT NULL,
			age INT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Clear the table
	_, err = db.Exec("DELETE FROM test_users")
	if err != nil {
		t.Fatalf("Failed to clear test table: %v", err)
	}

	return db
}

func TestUpsertData(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	tests := []struct {
		name          string
		data          []map[string]interface{}
		uniqueColumns []string
		expectedRows  int64
		expectError   bool
	}{
		{
			name:          "empty data",
			data:          []map[string]interface{}{},
			uniqueColumns: []string{"email"},
			expectedRows:  0,
			expectError:   false,
		},
		{
			name: "single row insert",
			data: []map[string]interface{}{
				{
					"email": "test1@example.com",
					"name":  "Test User 1",
					"age":   25,
				},
			},
			uniqueColumns: []string{"email"},
			expectedRows:  1,
			expectError:   false,
		},
		{
			name: "multiple rows insert",
			data: []map[string]interface{}{
				{
					"email": "test2@example.com",
					"name":  "Test User 2",
					"age":   30,
				},
				{
					"email": "test3@example.com",
					"name":  "Test User 3",
					"age":   35,
				},
			},
			uniqueColumns: []string{"email"},
			expectedRows:  2,
			expectError:   false,
		},
		{
			name: "upsert existing row",
			data: []map[string]interface{}{
				{
					"email": "test1@example.com",
					"name":  "Updated User 1",
					"age":   26,
				},
			},
			uniqueColumns: []string{"email"},
			expectedRows:  2, // 1 row updated
			expectError:   false,
		},
		{
			name: "multiple unique columns",
			data: []map[string]interface{}{
				{
					"email": "test4@example.com",
					"name":  "Test User 4",
					"age":   40,
				},
			},
			uniqueColumns: []string{"email", "name"},
			expectedRows:  1,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rowsAffected, err := UpsertData(db, "test_users", tt.data, tt.uniqueColumns, nil)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if !tt.expectError && rowsAffected != tt.expectedRows {
				t.Errorf("Expected %d rows affected, got %d", tt.expectedRows, rowsAffected)
			}
		})
	}
}

func TestUpdateData(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Insert some test data first
	insertData := []map[string]interface{}{
		{
			"email": "update1@example.com",
			"name":  "Update User 1",
			"age":   25,
		},
		{
			"email": "update2@example.com",
			"name":  "Update User 2",
			"age":   30,
		},
	}
	_, err := InsertData(db, "test_users", insertData, nil)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	tests := []struct {
		name         string
		data         []map[string]interface{}
		whereColumns []string
		expectedRows int64
		expectError  bool
	}{
		{
			name:         "empty data",
			data:         []map[string]interface{}{},
			whereColumns: []string{"email"},
			expectedRows: 0,
			expectError:  false,
		},
		{
			name: "single row update",
			data: []map[string]interface{}{
				{
					"email": "update1@example.com",
					"name":  "Updated Name 1",
					"age":   26,
				},
			},
			whereColumns: []string{"email"},
			expectedRows: 1,
			expectError:  false,
		},
		{
			name: "multiple rows update",
			data: []map[string]interface{}{
				{
					"email": "update1@example.com",
					"name":  "Updated Name 1 Again",
					"age":   27,
				},
				{
					"email": "update2@example.com",
					"name":  "Updated Name 2",
					"age":   31,
				},
			},
			whereColumns: []string{"email"},
			expectedRows: 2,
			expectError:  false,
		},
		{
			name: "update with multiple where columns",
			data: []map[string]interface{}{
				{
					"email": "update1@example.com",
					"name":  "Updated Name 1",
					"age":   28,
				},
			},
			whereColumns: []string{"email", "name"},
			expectedRows: 1,
			expectError:  false,
		},
		{
			name: "update non-existent row",
			data: []map[string]interface{}{
				{
					"email": "nonexistent@example.com",
					"name":  "Non-existent User",
					"age":   50,
				},
			},
			whereColumns: []string{"email"},
			expectedRows: 0,
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rowsAffected, err := UpdateData(db, "test_users", tt.data, tt.whereColumns, nil)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if !tt.expectError && rowsAffected != tt.expectedRows {
				t.Errorf("Expected %d rows affected, got %d", tt.expectedRows, rowsAffected)
			}
		})
	}
}

func TestFetchData(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Insert test data
	insertData := []map[string]interface{}{
		{
			"email": "fetch1@example.com",
			"name":  "Fetch User 1",
			"age":   25,
		},
		{
			"email": "fetch2@example.com",
			"name":  "Fetch User 2",
			"age":   30,
		},
	}
	_, err := InsertData(db, "test_users", insertData, nil)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	tests := []struct {
		name         string
		limit        int
		expectedRows int
		expectError  bool
	}{
		{
			name:         "fetch all rows",
			limit:        0,
			expectedRows: 2,
			expectError:  false,
		},
		{
			name:         "fetch with limit",
			limit:        1,
			expectedRows: 1,
			expectError:  false,
		},
		{
			name:         "fetch with high limit",
			limit:        10,
			expectedRows: 2,
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := FetchData(db, "test_users", tt.limit, nil)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if !tt.expectError && len(result) != tt.expectedRows {
				t.Errorf("Expected %d rows, got %d", tt.expectedRows, len(result))
			}
		})
	}
}

func TestInsertData(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	tests := []struct {
		name         string
		data         []map[string]interface{}
		expectedRows int64
		expectError  bool
	}{
		{
			name:         "empty data",
			data:         []map[string]interface{}{},
			expectedRows: 0,
			expectError:  false,
		},
		{
			name: "single row insert",
			data: []map[string]interface{}{
				{
					"email": "insert1@example.com",
					"name":  "Insert User 1",
					"age":   25,
				},
			},
			expectedRows: 1,
			expectError:  false,
		},
		{
			name: "multiple rows insert",
			data: []map[string]interface{}{
				{
					"email": "insert2@example.com",
					"name":  "Insert User 2",
					"age":   30,
				},
				{
					"email": "insert3@example.com",
					"name":  "Insert User 3",
					"age":   35,
				},
			},
			expectedRows: 2,
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rowsAffected, err := InsertData(db, "test_users", tt.data, nil)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if !tt.expectError && rowsAffected != tt.expectedRows {
				t.Errorf("Expected %d rows affected, got %d", tt.expectedRows, rowsAffected)
			}
		})
	}
}

func TestUUIDConversion(t *testing.T) {
	// Test UUID conversion
	// UUID: 4d22eced-1222-49c6-8d3c-e2e7370572af
	// Bytes: [77, 34, 236, 237, 18, 34, 73, 198, 141, 60, 226, 231, 55, 5, 114, 175]
	uuidBytes := []byte{77, 34, 236, 237, 18, 34, 73, 198, 141, 60, 226, 231, 55, 5, 114, 175}
	expectedUUID := "4d22eced-1222-49c6-8d3c-e2e7370572af"

	result := sanitizeValue(uuidBytes, nil)
	if result != expectedUUID {
		t.Errorf("Expected UUID %s, got %s", expectedUUID, result)
	}

	// Test the exact bytes mentioned by the user
	// Original in postgres: 4d22eced-1222-49c6-8d3c-e2e7370572af
	// After conversion in mysql: [184,9,105,172,85,171,67,163,181,248,18,157,81,48,141,27]
	userBytes := []byte{184, 9, 105, 172, 85, 171, 67, 163, 181, 248, 18, 157, 81, 48, 141, 27}
	userResult := sanitizeValue(userBytes, nil)
	expectedUserUUID := "b80969ac-55ab-43a3-b5f8-129d51308d1b"

	if userResult != expectedUserUUID {
		t.Errorf("Expected user UUID %s, got %s", expectedUserUUID, userResult)
	}

	// Test that non-UUID bytes are still converted to string
	nonUUIDBytes := []byte{1, 2, 3, 4, 5}
	result2 := sanitizeValue(nonUUIDBytes, nil)
	expectedString := string(nonUUIDBytes)
	if result2 != expectedString {
		t.Errorf("Expected string %s, got %s", expectedString, result2)
	}
}

func TestSanitizeValue(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
	}{
		{
			name:     "nil value",
			input:    nil,
			expected: nil,
		},
		{
			name:     "string value",
			input:    "test string",
			expected: "test string",
		},
		{
			name:     "int value",
			input:    42,
			expected: 42,
		},
		{
			name:     "slice of interface",
			input:    []interface{}{"a", "b", "c"},
			expected: `["a","b","c"]`,
		},
		{
			name:     "map of interface",
			input:    map[string]interface{}{"key1": "value1", "key2": 42},
			expected: `{"key1":"value1","key2":42}`,
		},
		{
			name:     "byte slice",
			input:    []byte("test bytes"),
			expected: "test bytes",
		},
		{
			name:     "nested slice",
			input:    []interface{}{[]interface{}{"nested", "data"}, "outer"},
			expected: `[["nested","data"],"outer"]`,
		},
		{
			name:     "uuid bytes",
			input:    []byte{77, 34, 236, 237, 18, 34, 73, 198, 141, 60, 226, 231, 55, 5, 114, 175},
			expected: "4d22eced-1222-49c6-8d3c-e2e7370572af",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeValue(tt.input, nil)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}
