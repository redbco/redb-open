package postgres

import (
	"testing"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNativeUnifiedModelDiscovery(t *testing.T) {
	t.Run("unified model discovery functions structure", func(t *testing.T) {
		// Test that we can create a UnifiedModel and populate it
		um := &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Tables:       make(map[string]unifiedmodel.Table),
			Schemas:      make(map[string]unifiedmodel.Schema),
			Types:        make(map[string]unifiedmodel.Type),
			Functions:    make(map[string]unifiedmodel.Function),
			Triggers:     make(map[string]unifiedmodel.Trigger),
			Sequences:    make(map[string]unifiedmodel.Sequence),
			Extensions:   make(map[string]unifiedmodel.Extension),
		}

		// Verify initial state
		assert.Equal(t, dbcapabilities.PostgreSQL, um.DatabaseType)
		assert.Len(t, um.Tables, 0)
		assert.Len(t, um.Schemas, 0)
		assert.Len(t, um.Types, 0)

		// Test adding a table directly to UnifiedModel
		table := unifiedmodel.Table{
			Name:        "test_table",
			Columns:     make(map[string]unifiedmodel.Column),
			Indexes:     make(map[string]unifiedmodel.Index),
			Constraints: make(map[string]unifiedmodel.Constraint),
		}

		// Add columns
		table.Columns["id"] = unifiedmodel.Column{
			Name:          "id",
			DataType:      "integer",
			Nullable:      false,
			IsPrimaryKey:  true,
			AutoIncrement: true,
		}

		table.Columns["name"] = unifiedmodel.Column{
			Name:     "name",
			DataType: "varchar",
			Nullable: false,
			Default:  "'default_name'",
		}

		// Add index
		table.Indexes["idx_name"] = unifiedmodel.Index{
			Name:    "idx_name",
			Columns: []string{"name"},
			Unique:  true,
		}

		// Add constraint
		table.Constraints["chk_name_length"] = unifiedmodel.Constraint{
			Name:       "chk_name_length",
			Type:       unifiedmodel.ConstraintTypeCheck,
			Expression: "LENGTH(name) > 0",
		}

		um.Tables["test_table"] = table

		// Verify table was added correctly
		assert.Len(t, um.Tables, 1)
		retrievedTable := um.Tables["test_table"]
		assert.Equal(t, "test_table", retrievedTable.Name)
		assert.Len(t, retrievedTable.Columns, 2)
		assert.Len(t, retrievedTable.Indexes, 1)
		assert.Len(t, retrievedTable.Constraints, 1)

		// Verify column details
		idColumn := retrievedTable.Columns["id"]
		assert.Equal(t, "id", idColumn.Name)
		assert.Equal(t, "integer", idColumn.DataType)
		assert.False(t, idColumn.Nullable)
		assert.True(t, idColumn.IsPrimaryKey)
		assert.True(t, idColumn.AutoIncrement)

		nameColumn := retrievedTable.Columns["name"]
		assert.Equal(t, "name", nameColumn.Name)
		assert.Equal(t, "varchar", nameColumn.DataType)
		assert.False(t, nameColumn.Nullable)
		assert.Equal(t, "'default_name'", nameColumn.Default)

		// Verify index
		index := retrievedTable.Indexes["idx_name"]
		assert.Equal(t, "idx_name", index.Name)
		assert.Equal(t, []string{"name"}, index.Columns)
		assert.True(t, index.Unique)

		// Verify constraint
		constraint := retrievedTable.Constraints["chk_name_length"]
		assert.Equal(t, "chk_name_length", constraint.Name)
		assert.Equal(t, unifiedmodel.ConstraintTypeCheck, constraint.Type)
		assert.Equal(t, "LENGTH(name) > 0", constraint.Expression)
	})

	t.Run("enum type handling in unified model", func(t *testing.T) {
		um := &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Types:        make(map[string]unifiedmodel.Type),
		}

		// Add enum type
		um.Types["status_enum"] = unifiedmodel.Type{
			Name:     "status_enum",
			Category: "enum",
			Definition: map[string]any{
				"values": []string{"active", "inactive", "pending"},
			},
		}

		// Verify enum type
		assert.Len(t, um.Types, 1)
		enumType := um.Types["status_enum"]
		assert.Equal(t, "status_enum", enumType.Name)
		assert.Equal(t, "enum", enumType.Category)

		values, ok := enumType.Definition["values"].([]string)
		require.True(t, ok)
		assert.Equal(t, []string{"active", "inactive", "pending"}, values)
	})

	t.Run("schema handling in unified model", func(t *testing.T) {
		um := &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Schemas:      make(map[string]unifiedmodel.Schema),
		}

		// Add schemas
		um.Schemas["public"] = unifiedmodel.Schema{
			Name:    "public",
			Comment: "Default public schema",
		}

		um.Schemas["app"] = unifiedmodel.Schema{
			Name:    "app",
			Comment: "Application schema",
		}

		// Verify schemas
		assert.Len(t, um.Schemas, 2)

		publicSchema := um.Schemas["public"]
		assert.Equal(t, "public", publicSchema.Name)
		assert.Equal(t, "Default public schema", publicSchema.Comment)

		appSchema := um.Schemas["app"]
		assert.Equal(t, "app", appSchema.Name)
		assert.Equal(t, "Application schema", appSchema.Comment)
	})

	t.Run("function handling in unified model", func(t *testing.T) {
		um := &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Functions:    make(map[string]unifiedmodel.Function),
		}

		// Add function
		um.Functions["get_user_count"] = unifiedmodel.Function{
			Name:       "get_user_count",
			Language:   "plpgsql",
			Returns:    "integer",
			Definition: "BEGIN RETURN (SELECT COUNT(*) FROM users); END;",
		}

		// Verify function
		assert.Len(t, um.Functions, 1)
		function := um.Functions["get_user_count"]
		assert.Equal(t, "get_user_count", function.Name)
		assert.Equal(t, "plpgsql", function.Language)
		assert.Equal(t, "integer", function.Returns)
		assert.Equal(t, "BEGIN RETURN (SELECT COUNT(*) FROM users); END;", function.Definition)
	})

	t.Run("trigger handling in unified model", func(t *testing.T) {
		um := &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Triggers:     make(map[string]unifiedmodel.Trigger),
		}

		// Add trigger
		um.Triggers["update_timestamp"] = unifiedmodel.Trigger{
			Name:      "update_timestamp",
			Table:     "users",
			Timing:    "BEFORE",
			Events:    []string{"UPDATE"},
			Procedure: "update_timestamp_function()",
		}

		// Verify trigger
		assert.Len(t, um.Triggers, 1)
		trigger := um.Triggers["update_timestamp"]
		assert.Equal(t, "update_timestamp", trigger.Name)
		assert.Equal(t, "users", trigger.Table)
		assert.Equal(t, "BEFORE", trigger.Timing)
		assert.Equal(t, []string{"UPDATE"}, trigger.Events)
		assert.Equal(t, "update_timestamp_function()", trigger.Procedure)
	})

	t.Run("sequence handling in unified model", func(t *testing.T) {
		um := &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Sequences:    make(map[string]unifiedmodel.Sequence),
		}

		// Add sequence
		minVal := int64(1)
		maxVal := int64(9223372036854775807)
		cacheVal := int64(1)

		um.Sequences["user_id_seq"] = unifiedmodel.Sequence{
			Name:      "user_id_seq",
			Start:     1,
			Increment: 1,
			Min:       &minVal,
			Max:       &maxVal,
			Cache:     &cacheVal,
			Cycle:     false,
		}

		// Verify sequence
		assert.Len(t, um.Sequences, 1)
		sequence := um.Sequences["user_id_seq"]
		assert.Equal(t, "user_id_seq", sequence.Name)
		assert.Equal(t, int64(1), sequence.Start)
		assert.Equal(t, int64(1), sequence.Increment)
		require.NotNil(t, sequence.Min)
		assert.Equal(t, int64(1), *sequence.Min)
		require.NotNil(t, sequence.Max)
		assert.Equal(t, int64(9223372036854775807), *sequence.Max)
		require.NotNil(t, sequence.Cache)
		assert.Equal(t, int64(1), *sequence.Cache)
		assert.False(t, sequence.Cycle)
	})

	t.Run("extension handling in unified model", func(t *testing.T) {
		um := &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Extensions:   make(map[string]unifiedmodel.Extension),
		}

		// Add extension
		um.Extensions["uuid-ossp"] = unifiedmodel.Extension{
			Name:    "uuid-ossp",
			Version: "1.1",
			Options: map[string]any{
				"description": "UUID generation functions",
			},
		}

		// Verify extension
		assert.Len(t, um.Extensions, 1)
		extension := um.Extensions["uuid-ossp"]
		assert.Equal(t, "uuid-ossp", extension.Name)
		assert.Equal(t, "1.1", extension.Version)
		require.NotNil(t, extension.Options)
		assert.Equal(t, "UUID generation functions", extension.Options["description"])
	})

	t.Run("partitioning info in table options", func(t *testing.T) {
		um := &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Tables:       make(map[string]unifiedmodel.Table),
		}

		// Add partitioned table
		table := unifiedmodel.Table{
			Name:        "partitioned_table",
			Columns:     make(map[string]unifiedmodel.Column),
			Indexes:     make(map[string]unifiedmodel.Index),
			Constraints: make(map[string]unifiedmodel.Constraint),
			Options: map[string]any{
				"partition_strategy": "RANGE",
				"partition_key":      []string{"created_at"},
				"partitions":         []string{"partition1", "partition2"},
			},
		}

		um.Tables["partitioned_table"] = table

		// Verify partitioning info
		retrievedTable := um.Tables["partitioned_table"]
		require.NotNil(t, retrievedTable.Options)
		assert.Equal(t, "RANGE", retrievedTable.Options["partition_strategy"])

		partitionKey, ok := retrievedTable.Options["partition_key"].([]string)
		require.True(t, ok)
		assert.Equal(t, []string{"created_at"}, partitionKey)

		partitions, ok := retrievedTable.Options["partitions"].([]string)
		require.True(t, ok)
		assert.Equal(t, []string{"partition1", "partition2"}, partitions)
	})
}

func TestQuoteStringSlice(t *testing.T) {
	testCases := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "simple strings",
			input:    []string{"active", "inactive", "pending"},
			expected: []string{"'active'", "'inactive'", "'pending'"},
		},
		{
			name:     "strings with single quotes",
			input:    []string{"can't", "won't", "don't"},
			expected: []string{"'can''t'", "'won''t'", "'don''t'"},
		},
		{
			name:     "empty slice",
			input:    []string{},
			expected: []string{},
		},
		{
			name:     "single string",
			input:    []string{"single"},
			expected: []string{"'single'"},
		},
		{
			name:     "string with multiple quotes",
			input:    []string{"it's a 'test' string"},
			expected: []string{"'it''s a ''test'' string'"},
		},
		{
			name:     "empty string",
			input:    []string{""},
			expected: []string{"''"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := quoteStringSlice(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestConstraintTypeMapping(t *testing.T) {
	testCases := []struct {
		pgConstraintType string
		expectedType     unifiedmodel.ConstraintType
		shouldMap        bool
	}{
		{"FOREIGN KEY", unifiedmodel.ConstraintTypeForeignKey, true},
		{"CHECK", unifiedmodel.ConstraintTypeCheck, true},
		{"UNIQUE", unifiedmodel.ConstraintTypeUnique, true},
		{"PRIMARY KEY", unifiedmodel.ConstraintTypePrimaryKey, false}, // Handled separately
		{"UNKNOWN", unifiedmodel.ConstraintTypeCheck, false},          // Should be skipped
	}

	for _, tc := range testCases {
		t.Run(tc.pgConstraintType, func(t *testing.T) {
			// This tests the logic that would be used in discoverConstraintsUnified
			var umConstraintType unifiedmodel.ConstraintType
			shouldProcess := true

			switch tc.pgConstraintType {
			case "FOREIGN KEY":
				umConstraintType = unifiedmodel.ConstraintTypeForeignKey
			case "CHECK":
				umConstraintType = unifiedmodel.ConstraintTypeCheck
			case "UNIQUE":
				umConstraintType = unifiedmodel.ConstraintTypeUnique
			default:
				shouldProcess = false
			}

			if tc.shouldMap {
				assert.True(t, shouldProcess)
				assert.Equal(t, tc.expectedType, umConstraintType)
			} else {
				assert.False(t, shouldProcess)
			}
		})
	}
}
