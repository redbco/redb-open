package same_paradigm

import (
	"context"
	"testing"
	"time"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/core"
)

func TestSameParadigmTranslator_PostgreSQLToMySQL(t *testing.T) {
	translator := NewSameParadigmTranslator()

	// Create test context
	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MySQL,
		SourceSchema:   createPostgreSQLTestSchema(),
		Preferences: core.TranslationPreferences{
			OptimizeForPerformance: true,
			GenerateComments:       true,
		},
		RequestID:   "same-paradigm-001",
		RequestedAt: time.Now(),
	}

	ctx := core.NewTranslationContext(context.Background(), request)

	// Set up analysis result for same paradigm
	analysis := &core.ParadigmAnalysisResult{
		SourceDatabase:        dbcapabilities.PostgreSQL,
		TargetDatabase:        dbcapabilities.MySQL,
		ConversionApproach:    core.ConversionApproachSameParadigm,
		TranslationComplexity: core.TranslationComplexitySimple,
	}
	ctx.SetAnalysis(analysis)

	// Parse and set source schema
	sourceSchema := &unifiedmodel.UnifiedModel{
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
					"created_at": {
						Name:     "created_at",
						DataType: "timestamp",
						Nullable: true,
					},
				},
			},
			"orders": {
				Name: "orders",
				Columns: map[string]unifiedmodel.Column{
					"id": {
						Name:         "id",
						DataType:     "integer",
						Nullable:     false,
						IsPrimaryKey: true,
					},
					"user_id": {
						Name:     "user_id",
						DataType: "integer",
						Nullable: false,
					},
					"total": {
						Name:     "total",
						DataType: "decimal(10,2)",
						Nullable: false,
					},
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
	ctx.SetSourceSchema(sourceSchema)

	// Perform translation
	err := translator.Translate(ctx)
	if err != nil {
		t.Fatalf("Translation failed: %v", err)
	}

	// Verify results
	targetSchema := ctx.TargetSchema
	if targetSchema == nil {
		t.Fatal("Expected target schema to be set")
	}

	if targetSchema.DatabaseType != dbcapabilities.MySQL {
		t.Errorf("Expected MySQL database type, got %s", targetSchema.DatabaseType)
	}

	// Check that tables were converted
	if len(targetSchema.Tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(targetSchema.Tables))
	}

	// Check users table
	usersTable, exists := targetSchema.Tables["users"]
	if !exists {
		t.Error("Expected users table in target schema")
	} else {
		if len(usersTable.Columns) != 3 {
			t.Errorf("Expected 3 columns in users table, got %d", len(usersTable.Columns))
		}
	}

	// Check orders table
	ordersTable, exists := targetSchema.Tables["orders"]
	if !exists {
		t.Error("Expected orders table in target schema")
	} else {
		if len(ordersTable.Columns) != 3 {
			t.Errorf("Expected 3 columns in orders table, got %d", len(ordersTable.Columns))
		}
	}

	// Check constraints were preserved
	if len(targetSchema.Constraints) != 1 {
		t.Errorf("Expected 1 constraint, got %d", len(targetSchema.Constraints))
	}

	// Check metrics
	if ctx.Metrics.ObjectsProcessed == 0 {
		t.Error("Expected objects to be processed")
	}
	if ctx.Metrics.ObjectsConverted == 0 {
		t.Error("Expected objects to be converted")
	}
}

func TestSameParadigmTranslator_MySQLToPostgreSQL(t *testing.T) {
	translator := NewSameParadigmTranslator()

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.MySQL,
		TargetDatabase: dbcapabilities.PostgreSQL,
		SourceSchema:   createMySQLTestSchema(),
		RequestID:      "same-paradigm-002",
		RequestedAt:    time.Now(),
	}

	ctx := core.NewTranslationContext(context.Background(), request)

	analysis := &core.ParadigmAnalysisResult{
		SourceDatabase:        dbcapabilities.MySQL,
		TargetDatabase:        dbcapabilities.PostgreSQL,
		ConversionApproach:    core.ConversionApproachSameParadigm,
		TranslationComplexity: core.TranslationComplexitySimple,
	}
	ctx.SetAnalysis(analysis)

	sourceSchema := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.MySQL,
		Tables: map[string]unifiedmodel.Table{
			"products": {
				Name: "products",
				Columns: map[string]unifiedmodel.Column{
					"id": {
						Name:         "id",
						DataType:     "int",
						Nullable:     false,
						IsPrimaryKey: true,
					},
					"name": {
						Name:     "name",
						DataType: "varchar(255)",
						Nullable: false,
					},
					"price": {
						Name:     "price",
						DataType: "decimal(10,2)",
						Nullable: false,
					},
				},
			},
		},
	}
	ctx.SetSourceSchema(sourceSchema)

	err := translator.Translate(ctx)
	if err != nil {
		t.Fatalf("Translation failed: %v", err)
	}

	targetSchema := ctx.TargetSchema
	if targetSchema.DatabaseType != dbcapabilities.PostgreSQL {
		t.Errorf("Expected PostgreSQL database type, got %s", targetSchema.DatabaseType)
	}

	if len(targetSchema.Tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(targetSchema.Tables))
	}
}

func TestSameParadigmTranslator_WithUnsupportedObjects(t *testing.T) {
	translator := NewSameParadigmTranslator()

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MySQL,
		SourceSchema: &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Tables:       make(map[string]unifiedmodel.Table),
		},
		RequestID:   "same-paradigm-003",
		RequestedAt: time.Now(),
	}

	ctx := core.NewTranslationContext(context.Background(), request)

	analysis := &core.ParadigmAnalysisResult{
		ConversionApproach: core.ConversionApproachSameParadigm,
	}
	ctx.SetAnalysis(analysis)

	// Create schema with unsupported objects for MySQL
	sourceSchema := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables: map[string]unifiedmodel.Table{
			"test_table": {
				Name: "test_table",
				Columns: map[string]unifiedmodel.Column{
					"id": {Name: "id", DataType: "integer", IsPrimaryKey: true},
				},
			},
		},
		// Add some PostgreSQL-specific objects that might not be supported in MySQL
		Functions: map[string]unifiedmodel.Function{
			"test_function": {
				Name:       "test_function",
				Returns:    "integer",
				Definition: "RETURN 1;",
			},
		},
		Sequences: map[string]unifiedmodel.Sequence{
			"test_seq": {
				Name:      "test_seq",
				Start:     1,
				Increment: 1,
			},
		},
	}
	ctx.SetSourceSchema(sourceSchema)

	err := translator.Translate(ctx)
	if err != nil {
		t.Fatalf("Translation failed: %v", err)
	}

	// Check that warnings were generated for unsupported features
	if len(ctx.Warnings) == 0 {
		t.Error("Expected warnings for unsupported features")
	}

	// Check that some objects were skipped or dropped
	if ctx.Metrics.ObjectsSkipped == 0 && ctx.Metrics.ObjectsDropped == 0 {
		t.Error("Expected some objects to be skipped or dropped due to unsupported features")
	}
}

func TestSameParadigmTranslator_WithExcludedObjects(t *testing.T) {
	translator := NewSameParadigmTranslator()

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MySQL,
		SourceSchema: &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Tables:       make(map[string]unifiedmodel.Table),
		},
		Preferences: core.TranslationPreferences{
			ExcludeObjects: []string{"excluded_table"},
		},
		RequestID:   "same-paradigm-004",
		RequestedAt: time.Now(),
	}

	ctx := core.NewTranslationContext(context.Background(), request)

	analysis := &core.ParadigmAnalysisResult{
		ConversionApproach: core.ConversionApproachSameParadigm,
	}
	ctx.SetAnalysis(analysis)

	sourceSchema := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables: map[string]unifiedmodel.Table{
			"included_table": {
				Name: "included_table",
				Columns: map[string]unifiedmodel.Column{
					"id": {Name: "id", DataType: "integer", IsPrimaryKey: true},
				},
			},
			"excluded_table": {
				Name: "excluded_table",
				Columns: map[string]unifiedmodel.Column{
					"id": {Name: "id", DataType: "integer", IsPrimaryKey: true},
				},
			},
		},
	}
	ctx.SetSourceSchema(sourceSchema)

	err := translator.Translate(ctx)
	if err != nil {
		t.Fatalf("Translation failed: %v", err)
	}

	targetSchema := ctx.TargetSchema

	// Check that only included table is present
	if len(targetSchema.Tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(targetSchema.Tables))
	}

	if _, exists := targetSchema.Tables["included_table"]; !exists {
		t.Error("Expected included_table to be present")
	}

	if _, exists := targetSchema.Tables["excluded_table"]; exists {
		t.Error("Expected excluded_table to be excluded")
	}

	// Check that excluded object was counted as skipped
	if ctx.Metrics.ObjectsSkipped == 0 {
		t.Error("Expected excluded objects to be counted as skipped")
	}
}

func TestSameParadigmTranslator_ValidationError(t *testing.T) {
	translator := NewSameParadigmTranslator()

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MySQL,
		SourceSchema: &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Tables:       make(map[string]unifiedmodel.Table),
		},
		RequestID:   "same-paradigm-005",
		RequestedAt: time.Now(),
	}

	ctx := core.NewTranslationContext(context.Background(), request)

	// Set wrong conversion approach to trigger validation error
	analysis := &core.ParadigmAnalysisResult{
		ConversionApproach: core.ConversionApproachCrossParadigm, // Wrong approach
	}
	ctx.SetAnalysis(analysis)

	sourceSchema := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
	}
	ctx.SetSourceSchema(sourceSchema)

	err := translator.Translate(ctx)
	if err == nil {
		t.Error("Expected validation error for wrong conversion approach")
	}
}

// Helper functions for creating test schemas

func createPostgreSQLTestSchema() *unifiedmodel.UnifiedModel {
	return &unifiedmodel.UnifiedModel{
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
					"created_at": {
						Name:     "created_at",
						DataType: "timestamp",
						Nullable: true,
					},
				},
				Constraints: map[string]unifiedmodel.Constraint{
					"pk_users": {
						Name:    "pk_users",
						Type:    unifiedmodel.ConstraintTypePrimaryKey,
						Columns: []string{"id"},
					},
				},
			},
			"orders": {
				Name: "orders",
				Columns: map[string]unifiedmodel.Column{
					"id": {
						Name:         "id",
						DataType:     "integer",
						Nullable:     false,
						IsPrimaryKey: true,
					},
					"user_id": {
						Name:     "user_id",
						DataType: "integer",
						Nullable: false,
					},
					"total": {
						Name:     "total",
						DataType: "decimal(10,2)",
						Nullable: false,
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
		},
	}
}

func createMySQLTestSchema() *unifiedmodel.UnifiedModel {
	return &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.MySQL,
		Tables: map[string]unifiedmodel.Table{
			"products": {
				Name: "products",
				Columns: map[string]unifiedmodel.Column{
					"id": {
						Name:         "id",
						DataType:     "int",
						Nullable:     false,
						IsPrimaryKey: true,
					},
					"name": {
						Name:     "name",
						DataType: "varchar(255)",
						Nullable: false,
					},
					"price": {
						Name:     "price",
						DataType: "decimal(10,2)",
						Nullable: false,
					},
				},
				Constraints: map[string]unifiedmodel.Constraint{
					"pk_products": {
						Name:    "pk_products",
						Type:    unifiedmodel.ConstraintTypePrimaryKey,
						Columns: []string{"id"},
					},
				},
			},
		},
	}
}
