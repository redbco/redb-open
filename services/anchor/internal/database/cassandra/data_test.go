package cassandra

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
)

func TestUpsertData(t *testing.T) {
	// Test with empty data
	session := &gocql.Session{}
	result, err := UpsertData(session, "test_table", []map[string]interface{}{}, []string{"id"})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)
}

func TestUpdateData(t *testing.T) {
	// Test with empty data
	session := &gocql.Session{}
	result, err := UpdateData(session, "test_table", []map[string]interface{}{}, []string{"id"})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)
}

func TestUpsertDataWithEmptyData(t *testing.T) {
	// Test with empty data and different table formats
	session := &gocql.Session{}

	// Test with simple table name
	result, err := UpsertData(session, "test_table", []map[string]interface{}{}, []string{"id"})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// Test with keyspace.table format
	result, err = UpsertData(session, "test_keyspace.test_table", []map[string]interface{}{}, []string{"id"})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)
}

func TestUpdateDataWithEmptyData(t *testing.T) {
	// Test with empty data and different table formats
	session := &gocql.Session{}

	// Test with simple table name
	result, err := UpdateData(session, "test_table", []map[string]interface{}{}, []string{"id"})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// Test with keyspace.table format
	result, err = UpdateData(session, "test_keyspace.test_table", []map[string]interface{}{}, []string{"id"})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)
}
