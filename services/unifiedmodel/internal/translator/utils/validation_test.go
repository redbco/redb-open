package utils

import (
	"testing"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

func TestValidateUnifiedModel_ValidModel(t *testing.T) {
	model := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables: map[string]unifiedmodel.Table{
			"users": {
				Name: "users",
				Columns: map[string]unifiedmodel.Column{
					"id": {
						Name:         "id",
						DataType:     "integer",
						Nullable:     false,
						IsPrimaryKey: true,
					},
					"email": {
						Name:     "email",
						DataType: "varchar(255)",
						Nullable: false,
					},
				},
			},
		},
	}

	errors := ValidateUnifiedModel(model)
	if len(errors) != 0 {
		t.Errorf("Expected no validation errors for valid model, got %d errors", len(errors))
		for _, err := range errors {
			t.Logf("Error: %s", err.Error())
		}
	}
}

func TestValidateUnifiedModel_InvalidModel(t *testing.T) {
	tests := []struct {
		name           string
		model          *unifiedmodel.UnifiedModel
		expectedErrors int
	}{
		{
			name:           "nil model",
			model:          nil,
			expectedErrors: 1,
		},
		{
			name: "empty database type",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: "",
				Tables: map[string]unifiedmodel.Table{
					"test": {
						Name: "test",
						Columns: map[string]unifiedmodel.Column{
							"id": {Name: "id", DataType: "integer", IsPrimaryKey: true},
						},
					},
				},
			},
			expectedErrors: 1,
		},
		{
			name: "table without name",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.PostgreSQL,
				Tables: map[string]unifiedmodel.Table{
					"test": {
						Name: "", // Empty name
						Columns: map[string]unifiedmodel.Column{
							"id": {Name: "id", DataType: "integer", IsPrimaryKey: true},
						},
					},
				},
			},
			expectedErrors: 1,
		},
		{
			name: "table without columns",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.PostgreSQL,
				Tables: map[string]unifiedmodel.Table{
					"test": {
						Name:    "test",
						Columns: map[string]unifiedmodel.Column{}, // Empty columns
					},
				},
			},
			expectedErrors: 2, // Empty columns + no primary key
		},
		{
			name: "column without name",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.PostgreSQL,
				Tables: map[string]unifiedmodel.Table{
					"test": {
						Name: "test",
						Columns: map[string]unifiedmodel.Column{
							"col1": {
								Name:     "", // Empty name
								DataType: "integer",
							},
						},
					},
				},
			},
			expectedErrors: 2, // Empty name + no primary key
		},
		{
			name: "column without data type",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.PostgreSQL,
				Tables: map[string]unifiedmodel.Table{
					"test": {
						Name: "test",
						Columns: map[string]unifiedmodel.Column{
							"col1": {
								Name:     "col1",
								DataType: "", // Empty data type
							},
						},
					},
				},
			},
			expectedErrors: 2, // Empty data type + no primary key
		},
		{
			name: "table without primary key",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.PostgreSQL,
				Tables: map[string]unifiedmodel.Table{
					"test": {
						Name: "test",
						Columns: map[string]unifiedmodel.Column{
							"col1": {
								Name:         "col1",
								DataType:     "varchar(50)",
								IsPrimaryKey: false, // No primary key
							},
						},
					},
				},
			},
			expectedErrors: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errors := ValidateUnifiedModel(tt.model)
			if len(errors) != tt.expectedErrors {
				t.Errorf("Expected %d validation errors, got %d", tt.expectedErrors, len(errors))
				for _, err := range errors {
					t.Logf("Error: %s", err.Error())
				}
			}
		})
	}
}

func TestValidateUnifiedModel_Collections(t *testing.T) {
	model := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.MongoDB,
		Collections: map[string]unifiedmodel.Collection{
			"users": {
				Name: "users",
				Fields: map[string]unifiedmodel.Field{
					"_id": {
						Name: "_id",
						Type: "objectid",
					},
					"email": {
						Name: "email",
						Type: "string",
					},
				},
			},
		},
	}

	errors := ValidateUnifiedModel(model)
	if len(errors) != 0 {
		t.Errorf("Expected no validation errors for valid collection model, got %d errors", len(errors))
		for _, err := range errors {
			t.Logf("Error: %s", err.Error())
		}
	}
}

func TestValidateUnifiedModel_InvalidCollections(t *testing.T) {
	tests := []struct {
		name           string
		model          *unifiedmodel.UnifiedModel
		expectedErrors int
	}{
		{
			name: "collection without name",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.MongoDB,
				Collections: map[string]unifiedmodel.Collection{
					"test": {
						Name: "", // Empty name
						Fields: map[string]unifiedmodel.Field{
							"_id": {Name: "_id", Type: "objectid"},
						},
					},
				},
			},
			expectedErrors: 1,
		},
		{
			name: "collection without fields",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.MongoDB,
				Collections: map[string]unifiedmodel.Collection{
					"test": {
						Name:   "test",
						Fields: map[string]unifiedmodel.Field{}, // Empty fields
					},
				},
			},
			expectedErrors: 1,
		},
		{
			name: "field without name",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.MongoDB,
				Collections: map[string]unifiedmodel.Collection{
					"test": {
						Name: "test",
						Fields: map[string]unifiedmodel.Field{
							"field1": {
								Name: "", // Empty name
								Type: "string",
							},
						},
					},
				},
			},
			expectedErrors: 1,
		},
		{
			name: "field without type",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.MongoDB,
				Collections: map[string]unifiedmodel.Collection{
					"test": {
						Name: "test",
						Fields: map[string]unifiedmodel.Field{
							"field1": {
								Name: "field1",
								Type: "", // Empty type
							},
						},
					},
				},
			},
			expectedErrors: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errors := ValidateUnifiedModel(tt.model)
			if len(errors) != tt.expectedErrors {
				t.Errorf("Expected %d validation errors, got %d", tt.expectedErrors, len(errors))
				for _, err := range errors {
					t.Logf("Error: %s", err.Error())
				}
			}
		})
	}
}

func TestValidateUnifiedModel_Nodes(t *testing.T) {
	model := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.Neo4j,
		Nodes: map[string]unifiedmodel.Node{
			"User": {
				Label: "User",
				Properties: map[string]unifiedmodel.Property{
					"id": {
						Name: "id",
						Type: "integer",
					},
					"email": {
						Name: "email",
						Type: "string",
					},
				},
			},
		},
	}

	errors := ValidateUnifiedModel(model)
	if len(errors) != 0 {
		t.Errorf("Expected no validation errors for valid node model, got %d errors", len(errors))
		for _, err := range errors {
			t.Logf("Error: %s", err.Error())
		}
	}
}

func TestValidateUnifiedModel_InvalidNodes(t *testing.T) {
	tests := []struct {
		name           string
		model          *unifiedmodel.UnifiedModel
		expectedErrors int
	}{
		{
			name: "node without label",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.Neo4j,
				Nodes: map[string]unifiedmodel.Node{
					"test": {
						Label: "", // Empty label
						Properties: map[string]unifiedmodel.Property{
							"id": {Name: "id", Type: "integer"},
						},
					},
				},
			},
			expectedErrors: 1,
		},
		{
			name: "node without properties",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.Neo4j,
				Nodes: map[string]unifiedmodel.Node{
					"test": {
						Label:      "test",
						Properties: map[string]unifiedmodel.Property{}, // Empty properties
					},
				},
			},
			expectedErrors: 1,
		},
		{
			name: "property without name",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.Neo4j,
				Nodes: map[string]unifiedmodel.Node{
					"test": {
						Label: "test",
						Properties: map[string]unifiedmodel.Property{
							"prop1": {
								Name: "", // Empty name
								Type: "string",
							},
						},
					},
				},
			},
			expectedErrors: 1,
		},
		{
			name: "property without type",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.Neo4j,
				Nodes: map[string]unifiedmodel.Node{
					"test": {
						Label: "test",
						Properties: map[string]unifiedmodel.Property{
							"prop1": {
								Name: "prop1",
								Type: "", // Empty type
							},
						},
					},
				},
			},
			expectedErrors: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errors := ValidateUnifiedModel(tt.model)
			if len(errors) != tt.expectedErrors {
				t.Errorf("Expected %d validation errors, got %d", tt.expectedErrors, len(errors))
				for _, err := range errors {
					t.Logf("Error: %s", err.Error())
				}
			}
		})
	}
}

func TestValidateUnifiedModel_Constraints(t *testing.T) {
	model := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables: map[string]unifiedmodel.Table{
			"users": {
				Name: "users",
				Columns: map[string]unifiedmodel.Column{
					"id": {Name: "id", DataType: "integer", IsPrimaryKey: true},
				},
			},
			"orders": {
				Name: "orders",
				Columns: map[string]unifiedmodel.Column{
					"id":      {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"user_id": {Name: "user_id", DataType: "integer"},
				},
			},
		},
		Constraints: map[string]unifiedmodel.Constraint{
			"fk_orders_user": {
				Name:    "fk_orders_user",
				Type:    unifiedmodel.ConstraintTypeForeignKey,
				Columns: []string{"user_id"},
				Reference: unifiedmodel.Reference{
					Table:   "users",
					Columns: []string{"id"},
				},
			},
		},
	}

	errors := ValidateUnifiedModel(model)
	if len(errors) != 0 {
		t.Errorf("Expected no validation errors for valid constraints, got %d errors", len(errors))
		for _, err := range errors {
			t.Logf("Error: %s", err.Error())
		}
	}
}

func TestValidateUnifiedModel_InvalidConstraints(t *testing.T) {
	tests := []struct {
		name           string
		model          *unifiedmodel.UnifiedModel
		expectedErrors int
	}{
		{
			name: "constraint without name",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.PostgreSQL,
				Tables: map[string]unifiedmodel.Table{
					"test": {
						Name: "test",
						Columns: map[string]unifiedmodel.Column{
							"id": {Name: "id", DataType: "integer", IsPrimaryKey: true},
						},
					},
				},
				Constraints: map[string]unifiedmodel.Constraint{
					"test_constraint": {
						Name:    "", // Empty name
						Type:    unifiedmodel.ConstraintTypePrimaryKey,
						Columns: []string{"id"},
					},
				},
			},
			expectedErrors: 1,
		},
		{
			name: "constraint without columns",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.PostgreSQL,
				Tables: map[string]unifiedmodel.Table{
					"test": {
						Name: "test",
						Columns: map[string]unifiedmodel.Column{
							"id": {Name: "id", DataType: "integer", IsPrimaryKey: true},
						},
					},
				},
				Constraints: map[string]unifiedmodel.Constraint{
					"test_constraint": {
						Name:    "test_constraint",
						Type:    unifiedmodel.ConstraintTypePrimaryKey,
						Columns: []string{}, // Empty columns
					},
				},
			},
			expectedErrors: 1,
		},
		{
			name: "foreign key without reference",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.PostgreSQL,
				Tables: map[string]unifiedmodel.Table{
					"test": {
						Name: "test",
						Columns: map[string]unifiedmodel.Column{
							"id":      {Name: "id", DataType: "integer", IsPrimaryKey: true},
							"user_id": {Name: "user_id", DataType: "integer"},
						},
					},
				},
				Constraints: map[string]unifiedmodel.Constraint{
					"fk_test": {
						Name:    "fk_test",
						Type:    unifiedmodel.ConstraintTypeForeignKey,
						Columns: []string{"user_id"},
						// Missing Reference
					},
				},
			},
			expectedErrors: 3, // Missing reference table + missing reference columns + non-existent table
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errors := ValidateUnifiedModel(tt.model)
			if len(errors) != tt.expectedErrors {
				t.Errorf("Expected %d validation errors, got %d", tt.expectedErrors, len(errors))
				for _, err := range errors {
					t.Logf("Error: %s", err.Error())
				}
			}
		})
	}
}

func TestValidateUnifiedModel_MixedObjectTypes(t *testing.T) {
	// Test model with both tables and collections (should be invalid)
	model := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL, // Relational database
		Tables: map[string]unifiedmodel.Table{
			"users": {
				Name: "users",
				Columns: map[string]unifiedmodel.Column{
					"id": {Name: "id", DataType: "integer", IsPrimaryKey: true},
				},
			},
		},
		Collections: map[string]unifiedmodel.Collection{
			"products": { // Collections in relational database should be invalid
				Name: "products",
				Fields: map[string]unifiedmodel.Field{
					"_id": {Name: "_id", Type: "objectid"},
				},
			},
		},
	}

	errors := ValidateUnifiedModel(model)
	if len(errors) == 0 {
		t.Error("Expected validation errors for mixed object types in relational database")
	}
}

func TestValidateConstraintReferences(t *testing.T) {
	tests := []struct {
		name           string
		model          *unifiedmodel.UnifiedModel
		expectedErrors int
	}{
		{
			name: "valid foreign key reference",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.PostgreSQL,
				Tables: map[string]unifiedmodel.Table{
					"users": {
						Name: "users",
						Columns: map[string]unifiedmodel.Column{
							"id": {Name: "id", DataType: "integer", IsPrimaryKey: true},
						},
					},
					"orders": {
						Name: "orders",
						Columns: map[string]unifiedmodel.Column{
							"id":      {Name: "id", DataType: "integer", IsPrimaryKey: true},
							"user_id": {Name: "user_id", DataType: "integer"},
						},
					},
				},
				Constraints: map[string]unifiedmodel.Constraint{
					"fk_orders_user": {
						Name:    "fk_orders_user",
						Type:    unifiedmodel.ConstraintTypeForeignKey,
						Columns: []string{"user_id"},
						Reference: unifiedmodel.Reference{
							Table:   "users",
							Columns: []string{"id"},
						},
					},
				},
			},
			expectedErrors: 0,
		},
		{
			name: "foreign key references non-existent table",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.PostgreSQL,
				Tables: map[string]unifiedmodel.Table{
					"orders": {
						Name: "orders",
						Columns: map[string]unifiedmodel.Column{
							"id":      {Name: "id", DataType: "integer", IsPrimaryKey: true},
							"user_id": {Name: "user_id", DataType: "integer"},
						},
					},
				},
				Constraints: map[string]unifiedmodel.Constraint{
					"fk_orders_user": {
						Name:    "fk_orders_user",
						Type:    unifiedmodel.ConstraintTypeForeignKey,
						Columns: []string{"user_id"},
						Reference: unifiedmodel.Reference{
							Table:   "users", // Non-existent table
							Columns: []string{"id"},
						},
					},
				},
			},
			expectedErrors: 1,
		},
		{
			name: "foreign key references non-existent column",
			model: &unifiedmodel.UnifiedModel{
				DatabaseType: dbcapabilities.PostgreSQL,
				Tables: map[string]unifiedmodel.Table{
					"users": {
						Name: "users",
						Columns: map[string]unifiedmodel.Column{
							"id": {Name: "id", DataType: "integer", IsPrimaryKey: true},
						},
					},
					"orders": {
						Name: "orders",
						Columns: map[string]unifiedmodel.Column{
							"id":      {Name: "id", DataType: "integer", IsPrimaryKey: true},
							"user_id": {Name: "user_id", DataType: "integer"},
						},
					},
				},
				Constraints: map[string]unifiedmodel.Constraint{
					"fk_orders_user": {
						Name:    "fk_orders_user",
						Type:    unifiedmodel.ConstraintTypeForeignKey,
						Columns: []string{"user_id"},
						Reference: unifiedmodel.Reference{
							Table:   "users",
							Columns: []string{"non_existent_id"}, // Non-existent column
						},
					},
				},
			},
			expectedErrors: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errors := ValidateUnifiedModel(tt.model)
			if len(errors) != tt.expectedErrors {
				t.Errorf("Expected %d validation errors, got %d", tt.expectedErrors, len(errors))
				for _, err := range errors {
					t.Logf("Error: %s", err.Error())
				}
			}
		})
	}
}
