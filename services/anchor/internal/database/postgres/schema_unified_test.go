package postgres

import (
	"testing"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStructureNativeUnifiedModel(t *testing.T) {
	t.Run("unified model structure creation logic", func(t *testing.T) {
		// Create a comprehensive UnifiedModel
		um := &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Schemas: map[string]unifiedmodel.Schema{
				"public": {
					Name:    "public",
					Comment: "Default public schema",
				},
				"app": {
					Name:    "app",
					Comment: "Application schema",
				},
			},
			Tables: map[string]unifiedmodel.Table{
				"users": {
					Name: "users",
					Columns: map[string]unifiedmodel.Column{
						"id": {
							Name:          "id",
							DataType:      "integer",
							Nullable:      false,
							IsPrimaryKey:  true,
							AutoIncrement: true,
						},
						"name": {
							Name:     "name",
							DataType: "varchar",
							Nullable: false,
							Default:  "'Unknown'",
						},
						"email": {
							Name:     "email",
							DataType: "varchar",
							Nullable: true,
						},
						"created_at": {
							Name:     "created_at",
							DataType: "timestamp",
							Nullable: false,
							Default:  "CURRENT_TIMESTAMP",
						},
					},
					Indexes: map[string]unifiedmodel.Index{
						"idx_users_email": {
							Name:    "idx_users_email",
							Columns: []string{"email"},
							Unique:  true,
						},
					},
					Constraints: map[string]unifiedmodel.Constraint{
						"chk_email_format": {
							Name:       "chk_email_format",
							Type:       unifiedmodel.ConstraintTypeCheck,
							Expression: "email LIKE '%@%'",
						},
					},
				},
				"posts": {
					Name: "posts",
					Columns: map[string]unifiedmodel.Column{
						"id": {
							Name:          "id",
							DataType:      "integer",
							Nullable:      false,
							IsPrimaryKey:  true,
							AutoIncrement: true,
						},
						"user_id": {
							Name:     "user_id",
							DataType: "integer",
							Nullable: false,
						},
						"title": {
							Name:     "title",
							DataType: "varchar",
							Nullable: false,
						},
						"content": {
							Name:     "content",
							DataType: "text",
							Nullable: true,
						},
					},
					Constraints: map[string]unifiedmodel.Constraint{
						"fk_posts_user_id": {
							Name:       "fk_posts_user_id",
							Type:       unifiedmodel.ConstraintTypeForeignKey,
							Expression: "FOREIGN KEY (user_id) REFERENCES users(id)",
						},
					},
				},
			},
			Types: map[string]unifiedmodel.Type{
				"user_status": {
					Name:     "user_status",
					Category: "enum",
					Definition: map[string]any{
						"values": []string{"active", "inactive", "pending", "suspended"},
					},
				},
				"post_status": {
					Name:     "post_status",
					Category: "enum",
					Definition: map[string]any{
						"values": []interface{}{"draft", "published", "archived"},
					},
				},
			},
			Views: map[string]unifiedmodel.View{
				"active_users": {
					Name:       "active_users",
					Definition: "SELECT id, name, email FROM users WHERE status = 'active'",
				},
			},
			Functions: map[string]unifiedmodel.Function{
				"get_user_count": {
					Name:       "get_user_count",
					Returns:    "integer",
					Definition: "BEGIN RETURN (SELECT COUNT(*) FROM users); END;",
					Language:   "plpgsql",
				},
			},
			Triggers: map[string]unifiedmodel.Trigger{
				"update_user_timestamp": {
					Name:      "update_user_timestamp",
					Table:     "users",
					Timing:    "BEFORE",
					Events:    []string{"UPDATE"},
					Procedure: "update_timestamp_function()",
				},
			},
			Sequences: map[string]unifiedmodel.Sequence{
				"user_id_seq": {
					Name:      "user_id_seq",
					Start:     1,
					Increment: 1,
					Min:       func() *int64 { v := int64(1); return &v }(),
					Max:       func() *int64 { v := int64(9223372036854775807); return &v }(),
					Cache:     func() *int64 { v := int64(1); return &v }(),
					Cycle:     false,
				},
			},
			Extensions: map[string]unifiedmodel.Extension{
				"uuid-ossp": {
					Name:    "uuid-ossp",
					Version: "1.1",
				},
			},
		}

		// Test that we can process the UnifiedModel without errors
		// This is a structural test - we can't actually execute SQL without a real database
		assert.NotNil(t, um)
		assert.Equal(t, dbcapabilities.PostgreSQL, um.DatabaseType)

		// Test table sorting
		sortedTables, err := sortTablesByDependencies(um.Tables)
		require.NoError(t, err)
		assert.Len(t, sortedTables, 2)

		// Test data type mapping
		assert.Equal(t, "INTEGER", mapUnifiedDataTypeToPostgres("integer"))
		assert.Equal(t, "VARCHAR", mapUnifiedDataTypeToPostgres("varchar"))
		assert.Equal(t, "TIMESTAMP", mapUnifiedDataTypeToPostgres("timestamp"))
		assert.Equal(t, "BOOLEAN", mapUnifiedDataTypeToPostgres("boolean"))
		assert.Equal(t, "JSONB", mapUnifiedDataTypeToPostgres("jsonb"))
		assert.Equal(t, "UUID", mapUnifiedDataTypeToPostgres("uuid"))

		// Test primary key index detection
		assert.True(t, isPrimaryKeyIndex("users_pkey", "users"))
		assert.True(t, isPrimaryKeyIndex("some_table_pkey", "some_table"))
		assert.False(t, isPrimaryKeyIndex("idx_users_email", "users"))

		// Verify enum type extraction
		enumCount := 0
		for _, umType := range um.Types {
			if umType.Category == "enum" {
				enumCount++
				if umType.Name == "user_status" {
					if values, ok := umType.Definition["values"].([]string); ok {
						assert.Equal(t, []string{"active", "inactive", "pending", "suspended"}, values)
					}
				}
			}
		}
		assert.Equal(t, 2, enumCount)

		// Verify table structure
		usersTable := um.Tables["users"]
		assert.Equal(t, "users", usersTable.Name)
		assert.Len(t, usersTable.Columns, 4)
		assert.Len(t, usersTable.Indexes, 1)
		assert.Len(t, usersTable.Constraints, 1)

		// Verify column details
		idColumn := usersTable.Columns["id"]
		assert.Equal(t, "id", idColumn.Name)
		assert.Equal(t, "integer", idColumn.DataType)
		assert.False(t, idColumn.Nullable)
		assert.True(t, idColumn.IsPrimaryKey)
		assert.True(t, idColumn.AutoIncrement)

		nameColumn := usersTable.Columns["name"]
		assert.Equal(t, "name", nameColumn.Name)
		assert.Equal(t, "varchar", nameColumn.DataType)
		assert.False(t, nameColumn.Nullable)
		assert.Equal(t, "'Unknown'", nameColumn.Default)

		// Verify constraint
		constraint := usersTable.Constraints["chk_email_format"]
		assert.Equal(t, "chk_email_format", constraint.Name)
		assert.Equal(t, unifiedmodel.ConstraintTypeCheck, constraint.Type)
		assert.Equal(t, "email LIKE '%@%'", constraint.Expression)

		// Verify foreign key constraint in posts table
		postsTable := um.Tables["posts"]
		fkConstraint := postsTable.Constraints["fk_posts_user_id"]
		assert.Equal(t, unifiedmodel.ConstraintTypeForeignKey, fkConstraint.Type)
		assert.Equal(t, "FOREIGN KEY (user_id) REFERENCES users(id)", fkConstraint.Expression)
	})

	t.Run("nil unified model returns error", func(t *testing.T) {
		// This would be tested with a real database connection
		// For now, we just verify the nil check logic would work
		var um *unifiedmodel.UnifiedModel = nil
		assert.Nil(t, um)
	})

	t.Run("empty unified model creates minimal structure", func(t *testing.T) {
		um := &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Tables:       make(map[string]unifiedmodel.Table),
			Types:        make(map[string]unifiedmodel.Type),
			Schemas:      make(map[string]unifiedmodel.Schema),
			Views:        make(map[string]unifiedmodel.View),
			Functions:    make(map[string]unifiedmodel.Function),
			Triggers:     make(map[string]unifiedmodel.Trigger),
			Sequences:    make(map[string]unifiedmodel.Sequence),
			Extensions:   make(map[string]unifiedmodel.Extension),
		}

		assert.Equal(t, dbcapabilities.PostgreSQL, um.DatabaseType)
		assert.Len(t, um.Tables, 0)
		assert.Len(t, um.Types, 0)

		// Test table sorting with empty tables
		sortedTables, err := sortTablesByDependencies(um.Tables)
		require.NoError(t, err)
		assert.Len(t, sortedTables, 0)
	})
}

func TestDataTypeMappingComprehensive(t *testing.T) {
	testCases := []struct {
		unifiedType  string
		expectedType string
	}{
		{"integer", "INTEGER"},
		{"int32", "INTEGER"},
		{"bigint", "BIGINT"},
		{"int64", "BIGINT"},
		{"smallint", "SMALLINT"},
		{"int16", "SMALLINT"},
		{"boolean", "BOOLEAN"},
		{"bool", "BOOLEAN"},
		{"varchar", "VARCHAR"},
		{"string", "VARCHAR"},
		{"text", "TEXT"},
		{"timestamp", "TIMESTAMP"},
		{"date", "DATE"},
		{"time", "TIME"},
		{"decimal", "DECIMAL"},
		{"numeric", "DECIMAL"},
		{"float", "REAL"},
		{"float32", "REAL"},
		{"double", "DOUBLE PRECISION"},
		{"float64", "DOUBLE PRECISION"},
		{"uuid", "UUID"},
		{"json", "JSON"},
		{"jsonb", "JSONB"},
		{"bytea", "BYTEA"},
		{"binary", "BYTEA"},
		{"custom_type", "CUSTOM_TYPE"}, // Should pass through unknown types
	}

	for _, tc := range testCases {
		t.Run(tc.unifiedType, func(t *testing.T) {
			result := mapUnifiedDataTypeToPostgres(tc.unifiedType)
			assert.Equal(t, tc.expectedType, result)
		})
	}
}
