package postgres

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func TestUpsertData(t *testing.T) {
	// This is a basic test structure - in a real implementation you'd need a test database
	// For now, we'll just test the logic without actual database connection

	t.Run("empty data should return 0", func(t *testing.T) {
		// Mock pool - in real test you'd use a test database
		var pool *pgxpool.Pool

		data := []map[string]interface{}{}
		uniqueColumns := []string{"id"}

		rowsAffected, err := UpsertData(pool, "test_table", data, uniqueColumns)

		assert.NoError(t, err)
		assert.Equal(t, int64(0), rowsAffected)
	})
}

func TestUpdateData(t *testing.T) {
	t.Run("empty data should return 0", func(t *testing.T) {
		// Mock pool - in real test you'd use a test database
		var pool *pgxpool.Pool

		data := []map[string]interface{}{}
		whereColumns := []string{"id"}

		rowsAffected, err := UpdateData(pool, "test_table", data, whereColumns)

		assert.NoError(t, err)
		assert.Equal(t, int64(0), rowsAffected)
	})
}

func TestFetchData(t *testing.T) {
	t.Run("empty table name should return error", func(t *testing.T) {
		// Mock pool - in real test you'd use a test database
		var pool *pgxpool.Pool

		data, err := FetchData(pool, "", 10)

		assert.Error(t, err)
		assert.Nil(t, data)
		assert.Contains(t, err.Error(), "table name cannot be empty")
	})
}

func TestInsertData(t *testing.T) {
	t.Run("empty data should return 0", func(t *testing.T) {
		// Mock pool - in real test you'd use a test database
		var pool *pgxpool.Pool

		data := []map[string]interface{}{}

		rowsAffected, err := InsertData(pool, "test_table", data)

		assert.NoError(t, err)
		assert.Equal(t, int64(0), rowsAffected)
	})
}
