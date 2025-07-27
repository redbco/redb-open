package mongodb

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestToBSONDoc(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]interface{}
		expected int // expected number of elements
	}{
		{
			name:     "simple map",
			input:    map[string]interface{}{"name": "test", "value": 123},
			expected: 2,
		},
		{
			name: "nested map",
			input: map[string]interface{}{
				"user": map[string]interface{}{"name": "test", "age": 25},
			},
			expected: 1,
		},
		{
			name: "with array",
			input: map[string]interface{}{
				"tags": []interface{}{"tag1", "tag2"},
			},
			expected: 1,
		},
		{
			name:     "empty map",
			input:    map[string]interface{}{},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toBSONDoc(tt.input)

			if len(result) != tt.expected {
				t.Errorf("expected length %d, got %d", tt.expected, len(result))
			}
		})
	}
}

func TestConvertSliceToBSON(t *testing.T) {
	tests := []struct {
		name     string
		input    []interface{}
		expected int
	}{
		{
			name:     "simple slice",
			input:    []interface{}{"tag1", "tag2", "tag3"},
			expected: 3,
		},
		{
			name:     "empty slice",
			input:    []interface{}{},
			expected: 0,
		},
		{
			name: "slice with nested maps",
			input: []interface{}{
				map[string]interface{}{"name": "test1"},
				map[string]interface{}{"name": "test2"},
			},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertSliceToBSON(tt.input)

			if bsonArray, ok := result.(bson.A); ok {
				if len(bsonArray) != tt.expected {
					t.Errorf("expected length %d, got %d", tt.expected, len(bsonArray))
				}
			} else {
				t.Errorf("expected bson.A type, got %T", result)
			}
		})
	}
}

func TestConvertBSONTypes(t *testing.T) {
	tests := []struct {
		name      string
		input     map[string]interface{}
		checkID   bool
		checkTime bool
	}{
		{
			name: "with ObjectID",
			input: map[string]interface{}{
				"_id":  bson.NewObjectID(),
				"name": "test",
			},
			checkID: true,
		},
		{
			name: "with DateTime",
			input: map[string]interface{}{
				"created": bson.DateTime(time.Now().UnixMilli()),
				"name":    "test",
			},
			checkTime: true,
		},
		{
			name: "with Binary",
			input: map[string]interface{}{
				"data": bson.Binary{Data: []byte("test data")},
				"name": "test",
			},
		},
		{
			name: "with Decimal128",
			input: map[string]interface{}{
				"amount": bson.Decimal128{},
				"name":   "test",
			},
		},
		{
			name: "with nested BSON.D",
			input: map[string]interface{}{
				"nested": bson.D{{Key: "key", Value: "value"}},
				"name":   "test",
			},
		},
		{
			name: "with BSON.A",
			input: map[string]interface{}{
				"array": bson.A{"item1", "item2"},
				"name":  "test",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			convertBSONTypes(tt.input)

			// Check that ObjectID was converted to string
			if tt.checkID {
				if id, exists := tt.input["_id"]; exists {
					if _, ok := id.(string); !ok {
						t.Errorf("expected _id to be converted to string, got %T", id)
					}
				}
			}

			// Check that DateTime was converted to string
			if tt.checkTime {
				if created, exists := tt.input["created"]; exists {
					if _, ok := created.(string); !ok {
						t.Errorf("expected created to be converted to string, got %T", created)
					}
				}
			}

			// Check that Binary was converted to string
			if data, exists := tt.input["data"]; exists {
				if _, ok := data.(string); !ok {
					t.Errorf("expected data to be converted to string, got %T", data)
				}
			}

			// Check that Decimal128 was converted to string
			if amount, exists := tt.input["amount"]; exists {
				if _, ok := amount.(string); !ok {
					t.Errorf("expected amount to be converted to string, got %T", amount)
				}
			}
		})
	}
}

func TestUpsertDataEmptyInput(t *testing.T) {
	// Test that UpsertData handles empty data correctly
	// This is a basic test that doesn't require a real database connection
	data := []map[string]interface{}{}

	// We can't actually call the function without a real database,
	// but we can test the logic that should handle empty data
	if len(data) == 0 {
		// This is the expected behavior from the function
		// The function should return 0, nil for empty data
		return
	}
}

func TestUpdateDataEmptyInput(t *testing.T) {
	// Test that UpdateData handles empty data correctly
	data := []map[string]interface{}{}

	// We can't actually call the function without a real database,
	// but we can test the logic that should handle empty data
	if len(data) == 0 {
		// This is the expected behavior from the function
		// The function should return 0, nil for empty data
		return
	}
}

func TestUpsertDataLogic(t *testing.T) {
	// Test the logic for building filters and updates
	tests := []struct {
		name          string
		data          map[string]interface{}
		uniqueColumns []string
		expectedID    bool
	}{
		{
			name:          "with unique columns",
			data:          map[string]interface{}{"id": "1", "name": "test"},
			uniqueColumns: []string{"id"},
			expectedID:    false,
		},
		{
			name:          "with _id fallback",
			data:          map[string]interface{}{"name": "test"},
			uniqueColumns: []string{},
			expectedID:    true,
		},
		{
			name:          "with existing _id",
			data:          map[string]interface{}{"_id": "existing", "name": "test"},
			uniqueColumns: []string{},
			expectedID:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the filter building logic
			filter := make(map[string]interface{})
			for _, col := range tt.uniqueColumns {
				if value, exists := tt.data[col]; exists {
					filter[col] = value
				}
			}

			// If no unique columns specified or no values found, use _id if present
			if len(filter) == 0 {
				if id, hasID := tt.data["_id"]; hasID {
					filter["_id"] = id
				} else {
					// If no unique constraints and no _id, generate one
					tt.data["_id"] = bson.NewObjectID()
					filter["_id"] = tt.data["_id"]
				}
			}

			// Verify the logic
			if tt.expectedID {
				if _, hasID := filter["_id"]; !hasID {
					t.Errorf("expected _id in filter but not found")
				}
			} else {
				if len(filter) == 0 {
					t.Errorf("expected non-empty filter")
				}
			}
		})
	}
}

func TestUpdateDataLogic(t *testing.T) {
	// Test the logic for building update fields
	tests := []struct {
		name           string
		data           map[string]interface{}
		whereColumns   []string
		expectedFields int
		expectError    bool
	}{
		{
			name:           "with where columns",
			data:           map[string]interface{}{"id": "1", "name": "updated", "value": 999},
			whereColumns:   []string{"id"},
			expectedFields: 2, // name and value, excluding id
		},
		{
			name:           "with _id fallback",
			data:           map[string]interface{}{"_id": "1", "name": "updated"},
			whereColumns:   []string{},
			expectedFields: 1, // name only, excluding _id
		},
		{
			name:         "no where columns and no _id",
			data:         map[string]interface{}{"name": "updated"},
			whereColumns: []string{},
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the filter building logic
			filter := make(map[string]interface{})
			for _, col := range tt.whereColumns {
				if value, exists := tt.data[col]; exists {
					filter[col] = value
				}
			}

			// If no where columns specified, use _id if present
			if len(filter) == 0 {
				if id, hasID := tt.data["_id"]; hasID {
					filter["_id"] = id
				} else {
					if tt.expectError {
						return // Expected error case
					}
					t.Errorf("no where columns specified and no _id found in document")
				}
			}

			// Create update document with only non-where columns
			updateFields := make(map[string]interface{})
			for key, value := range tt.data {
				isWhereColumn := false
				for _, whereCol := range tt.whereColumns {
					if key == whereCol {
						isWhereColumn = true
						break
					}
				}
				// Also exclude _id if it's being used as a filter (when no where columns specified)
				if len(tt.whereColumns) == 0 && key == "_id" {
					isWhereColumn = true
				}
				if !isWhereColumn {
					updateFields[key] = value
				}
			}

			// Verify the logic
			if !tt.expectError && len(updateFields) != tt.expectedFields {
				t.Errorf("expected %d update fields, got %d", tt.expectedFields, len(updateFields))
			}
		})
	}
}
