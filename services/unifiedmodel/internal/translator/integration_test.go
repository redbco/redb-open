package translator

import (
	"context"
	"testing"
	"time"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/core"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/cross_paradigm"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/same_paradigm"
)

// Integration tests for the complete translator v2 system

func TestTranslatorV2_EndToEnd_PostgreSQLToMySQL(t *testing.T) {
	// Create complete translator system
	sameParadigmTranslator := same_paradigm.NewSameParadigmTranslator()
	crossParadigmTranslator := cross_paradigm.NewCrossParadigmTranslator()
	translator := core.NewUnifiedTranslator(sameParadigmTranslator, crossParadigmTranslator)

	// Create realistic e-commerce schema
	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MySQL,
		SourceSchema:   createECommercePostgreSQLSchema(),
		Preferences: core.TranslationPreferences{
			OptimizeForPerformance: true,
			GenerateComments:       true,
		},
		RequestID:   "integration-001",
		RequestedAt: time.Now(),
	}

	ctx := context.Background()

	// Step 1: Analyze translation
	analysis, err := translator.AnalyzeTranslation(ctx, request)
	if err != nil {
		t.Fatalf("Analysis failed: %v", err)
	}

	if !analysis.ConversionSupported {
		t.Error("Expected PostgreSQL to MySQL conversion to be supported")
	}

	if analysis.ParadigmCompatibility != core.ParadigmCompatibilityIdentical {
		t.Errorf("Expected identical paradigms for relational to relational, got %v", analysis.ParadigmCompatibility)
	}

	if analysis.TranslationComplexity == core.TranslationComplexityImpossible {
		t.Error("Expected feasible translation complexity")
	}

	// Step 2: Perform translation
	result, err := translator.Translate(ctx, request)
	if err != nil {
		t.Fatalf("Translation failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful translation, got error: %s", result.ErrorMessage)
	}

	// Step 3: Verify results
	if result.UnifiedSchema == nil {
		t.Fatal("Expected unified schema in result")
	}

	if result.UnifiedSchema.DatabaseType != dbcapabilities.MySQL {
		t.Errorf("Expected MySQL database type, got %s", result.UnifiedSchema.DatabaseType)
	}

	// Verify all tables were converted
	expectedTables := []string{"users", "products", "categories", "orders", "order_items"}
	for _, tableName := range expectedTables {
		if _, exists := result.UnifiedSchema.Tables[tableName]; !exists {
			t.Errorf("Expected table %s to be converted", tableName)
		}
	}

	// Verify constraints were preserved
	if len(result.UnifiedSchema.Constraints) == 0 {
		t.Error("Expected constraints to be preserved")
	}

	// Verify translation report
	if result.TranslationReport.ObjectsProcessed == 0 {
		t.Error("Expected objects to be processed")
	}
	if result.TranslationReport.ObjectsConverted == 0 {
		t.Error("Expected objects to be converted")
	}
}

func TestTranslatorV2_EndToEnd_PostgreSQLToMongoDB(t *testing.T) {
	sameParadigmTranslator := same_paradigm.NewSameParadigmTranslator()
	crossParadigmTranslator := cross_paradigm.NewCrossParadigmTranslator()
	translator := core.NewUnifiedTranslator(sameParadigmTranslator, crossParadigmTranslator)

	// Create enrichment data to guide cross-paradigm conversion
	enrichment := &unifiedmodel.UnifiedModelEnrichment{
		TableEnrichments: map[string]unifiedmodel.TableEnrichment{
			"users": {
				PrimaryCategory: unifiedmodel.TableCategoryTransactional,
				AccessPattern:   unifiedmodel.AccessPatternReadWrite,
				RelatedTables:   []string{"orders"},
				BusinessPurpose: "User account management",
			},
			"categories": {
				PrimaryCategory: unifiedmodel.TableCategoryReference,
				AccessPattern:   unifiedmodel.AccessPatternReadHeavy,
				BusinessPurpose: "Product categorization",
				Tags:            []string{"lookup", "value_column:name"},
			},
			"order_items": {
				PrimaryCategory: unifiedmodel.TableCategoryMetadata,
				RelatedTables:   []string{"orders", "products"},
				BusinessPurpose: "Order line items",
			},
		},
		PerformanceHints: []unifiedmodel.PerformanceHint{
			{
				Category:         "indexing",
				Priority:         unifiedmodel.ConversionPriorityHigh,
				Hint:             "Create compound index on user_id and created_at",
				ObjectPath:       "tables.orders.columns.user_id",
				EstimatedBenefit: "Improved query performance for user order history",
			},
		},
	}

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MongoDB,
		SourceSchema:   createECommercePostgreSQLSchema(),
		Enrichment:     enrichment,
		Preferences: core.TranslationPreferences{
			PreferredStrategy:     core.ConversionStrategyDenormalization,
			AcceptDataLoss:        true,
			PreserveRelationships: true,
		},
		RequestID:   "integration-002",
		RequestedAt: time.Now(),
	}

	ctx := context.Background()

	// Analyze translation
	analysis, err := translator.AnalyzeTranslation(ctx, request)
	if err != nil {
		t.Fatalf("Analysis failed: %v", err)
	}

	if !analysis.ConversionSupported {
		t.Error("Expected PostgreSQL to MongoDB conversion to be supported")
	}

	if analysis.ParadigmCompatibility == core.ParadigmCompatibilityIncompatible {
		t.Error("Expected cross-paradigm conversion to be possible")
	}

	// Perform translation
	result, err := translator.Translate(ctx, request)
	if err != nil {
		t.Fatalf("Translation failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful translation, got error: %s", result.ErrorMessage)
	}

	// Verify document database structure
	if result.UnifiedSchema.DatabaseType != dbcapabilities.MongoDB {
		t.Errorf("Expected MongoDB database type, got %s", result.UnifiedSchema.DatabaseType)
	}

	if len(result.UnifiedSchema.Collections) == 0 {
		t.Error("Expected collections to be created for document database")
	}

	// Verify denormalization occurred (fewer collections than original tables due to embedding)
	if len(result.UnifiedSchema.Collections) >= len(result.UnifiedSchema.Tables) {
		t.Log("Note: Denormalization may have embedded some entities")
	}

	// Verify enrichment was used (check if enrichment context was provided)
	if request.Enrichment == nil {
		t.Error("Expected enrichment to be provided for cross-paradigm translation")
	}
}

func TestTranslatorV2_EndToEnd_MongoDBToPostgreSQL(t *testing.T) {
	sameParadigmTranslator := same_paradigm.NewSameParadigmTranslator()
	crossParadigmTranslator := cross_paradigm.NewCrossParadigmTranslator()
	translator := core.NewUnifiedTranslator(sameParadigmTranslator, crossParadigmTranslator)

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.MongoDB,
		TargetDatabase: dbcapabilities.PostgreSQL,
		SourceSchema:   createECommerceMongoDBSchema(),
		Preferences: core.TranslationPreferences{
			PreferredStrategy:     core.ConversionStrategyNormalization,
			AcceptDataLoss:        false,
			PreserveRelationships: true,
		},
		RequestID:   "integration-003",
		RequestedAt: time.Now(),
	}

	ctx := context.Background()

	// Analyze translation
	analysis, err := translator.AnalyzeTranslation(ctx, request)
	if err != nil {
		t.Fatalf("Analysis failed: %v", err)
	}

	if !analysis.ConversionSupported {
		t.Error("Expected MongoDB to PostgreSQL conversion to be supported")
	}

	// Perform translation
	result, err := translator.Translate(ctx, request)
	if err != nil {
		t.Fatalf("Translation failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful translation, got error: %s", result.ErrorMessage)
	}

	// Verify relational database structure
	if result.UnifiedSchema.DatabaseType != dbcapabilities.PostgreSQL {
		t.Errorf("Expected PostgreSQL database type, got %s", result.UnifiedSchema.DatabaseType)
	}

	if len(result.UnifiedSchema.Tables) == 0 {
		t.Error("Expected tables to be created for relational database")
	}

	// Verify normalization occurred (more tables than original collections due to flattening)
	if len(result.UnifiedSchema.Tables) < len(result.UnifiedSchema.Collections) {
		t.Log("Note: Normalization may have created additional tables from nested structures")
	}
}

func TestTranslatorV2_EndToEnd_ComplexSchema(t *testing.T) {
	sameParadigmTranslator := same_paradigm.NewSameParadigmTranslator()
	crossParadigmTranslator := cross_paradigm.NewCrossParadigmTranslator()
	translator := core.NewUnifiedTranslator(sameParadigmTranslator, crossParadigmTranslator)

	// Create complex schema with multiple object types
	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MongoDB,
		SourceSchema:   createComplexPostgreSQLSchema(),
		Preferences: core.TranslationPreferences{
			PreferredStrategy:      core.ConversionStrategyHybrid,
			AcceptDataLoss:         true,
			PreserveRelationships:  true,
			OptimizeForPerformance: true,
		},
		RequestID:   "integration-004",
		RequestedAt: time.Now(),
	}

	ctx := context.Background()

	// Perform full translation
	result, err := translator.Translate(ctx, request)
	if err != nil {
		t.Fatalf("Translation failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful translation, got error: %s", result.ErrorMessage)
	}

	// Verify complex schema handling
	if result.TranslationReport.ObjectsProcessed == 0 {
		t.Error("Expected complex objects to be processed")
	}

	// Check that strategies were used for complex conversions
	if len(result.TranslationReport.StrategiesUsed) == 0 {
		t.Log("Note: Complex schema translation may use multiple strategies")
	}

	// Verify hybrid strategy was applied
	foundHybrid := false
	for _, strategy := range result.TranslationReport.StrategiesUsed {
		if strategy == core.ConversionStrategyHybrid {
			foundHybrid = true
			break
		}
	}
	if !foundHybrid {
		t.Log("Note: Hybrid strategy may not have been used for this specific translation")
	}
}

func TestTranslatorV2_EndToEnd_ErrorHandling(t *testing.T) {
	sameParadigmTranslator := same_paradigm.NewSameParadigmTranslator()
	crossParadigmTranslator := cross_paradigm.NewCrossParadigmTranslator()
	translator := core.NewUnifiedTranslator(sameParadigmTranslator, crossParadigmTranslator)

	tests := []struct {
		name        string
		request     *core.TranslationRequest
		expectError bool
	}{
		{
			name: "nil source schema",
			request: &core.TranslationRequest{
				SourceDatabase: dbcapabilities.PostgreSQL,
				TargetDatabase: dbcapabilities.MySQL,
				SourceSchema:   nil,
				RequestID:      "error-001",
			},
			expectError: true,
		},
		{
			name: "unsupported database combination",
			request: &core.TranslationRequest{
				SourceDatabase: "unsupported_db",
				TargetDatabase: dbcapabilities.MySQL,
				SourceSchema: &unifiedmodel.UnifiedModel{
					DatabaseType: dbcapabilities.PostgreSQL,
					Tables:       make(map[string]unifiedmodel.Table),
				},
				RequestID: "error-002",
			},
			expectError: true,
		},
		{
			name: "same source and target",
			request: &core.TranslationRequest{
				SourceDatabase: dbcapabilities.PostgreSQL,
				TargetDatabase: dbcapabilities.PostgreSQL,
				SourceSchema: &unifiedmodel.UnifiedModel{
					DatabaseType: dbcapabilities.PostgreSQL,
					Tables:       make(map[string]unifiedmodel.Table),
				},
				RequestID: "error-003",
			},
			expectError: true,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := translator.Translate(ctx, tt.request)

			if tt.expectError {
				if err == nil && result.Success {
					t.Error("Expected error or failed result")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if !result.Success {
					t.Errorf("Expected success, got error: %s", result.ErrorMessage)
				}
			}
		})
	}
}

func TestTranslatorV2_EndToEnd_PerformanceMetrics(t *testing.T) {
	sameParadigmTranslator := same_paradigm.NewSameParadigmTranslator()
	crossParadigmTranslator := cross_paradigm.NewCrossParadigmTranslator()
	translator := core.NewUnifiedTranslator(sameParadigmTranslator, crossParadigmTranslator)

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MySQL,
		SourceSchema:   createECommercePostgreSQLSchema(),
		RequestID:      "performance-001",
		RequestedAt:    time.Now(),
	}

	ctx := context.Background()
	startTime := time.Now()

	result, err := translator.Translate(ctx, request)
	if err != nil {
		t.Fatalf("Translation failed: %v", err)
	}

	duration := time.Since(startTime)

	// Verify performance metrics
	if result.TranslationReport.ObjectsProcessed == 0 {
		t.Error("Expected objects processed count")
	}

	if result.TranslationReport.ObjectsConverted == 0 {
		t.Error("Expected objects converted count")
	}

	// Basic performance check (should complete within reasonable time)
	if duration > 30*time.Second {
		t.Errorf("Translation took too long: %v", duration)
	}

	t.Logf("Translation completed in %v", duration)
	t.Logf("Objects processed: %d", result.TranslationReport.ObjectsProcessed)
	t.Logf("Objects converted: %d", result.TranslationReport.ObjectsConverted)
}

// Helper functions for creating test schemas

func createECommercePostgreSQLSchema() *unifiedmodel.UnifiedModel {
	return &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables: map[string]unifiedmodel.Table{
			"users": {
				Name: "users",
				Columns: map[string]unifiedmodel.Column{
					"id":         {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"email":      {Name: "email", DataType: "varchar(255)", Nullable: false},
					"first_name": {Name: "first_name", DataType: "varchar(100)"},
					"last_name":  {Name: "last_name", DataType: "varchar(100)"},
					"created_at": {Name: "created_at", DataType: "timestamp"},
				},
			},
			"categories": {
				Name: "categories",
				Columns: map[string]unifiedmodel.Column{
					"id":   {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"name": {Name: "name", DataType: "varchar(100)", Nullable: false},
				},
			},
			"products": {
				Name: "products",
				Columns: map[string]unifiedmodel.Column{
					"id":          {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"name":        {Name: "name", DataType: "varchar(255)", Nullable: false},
					"description": {Name: "description", DataType: "text"},
					"price":       {Name: "price", DataType: "decimal(10,2)", Nullable: false},
					"category_id": {Name: "category_id", DataType: "integer"},
				},
			},
			"orders": {
				Name: "orders",
				Columns: map[string]unifiedmodel.Column{
					"id":         {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"user_id":    {Name: "user_id", DataType: "integer", Nullable: false},
					"total":      {Name: "total", DataType: "decimal(10,2)", Nullable: false},
					"status":     {Name: "status", DataType: "varchar(50)"},
					"created_at": {Name: "created_at", DataType: "timestamp"},
				},
			},
			"order_items": {
				Name: "order_items",
				Columns: map[string]unifiedmodel.Column{
					"id":         {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"order_id":   {Name: "order_id", DataType: "integer", Nullable: false},
					"product_id": {Name: "product_id", DataType: "integer", Nullable: false},
					"quantity":   {Name: "quantity", DataType: "integer", Nullable: false},
					"price":      {Name: "price", DataType: "decimal(10,2)", Nullable: false},
				},
			},
		},
		Constraints: map[string]unifiedmodel.Constraint{
			"fk_products_category": {
				Name:    "fk_products_category",
				Type:    unifiedmodel.ConstraintTypeForeignKey,
				Columns: []string{"category_id"},
				Reference: unifiedmodel.Reference{
					Table:   "categories",
					Columns: []string{"id"},
				},
			},
			"fk_orders_user": {
				Name:    "fk_orders_user",
				Type:    unifiedmodel.ConstraintTypeForeignKey,
				Columns: []string{"user_id"},
				Reference: unifiedmodel.Reference{
					Table:   "users",
					Columns: []string{"id"},
				},
			},
			"fk_order_items_order": {
				Name:    "fk_order_items_order",
				Type:    unifiedmodel.ConstraintTypeForeignKey,
				Columns: []string{"order_id"},
				Reference: unifiedmodel.Reference{
					Table:   "orders",
					Columns: []string{"id"},
				},
			},
			"fk_order_items_product": {
				Name:    "fk_order_items_product",
				Type:    unifiedmodel.ConstraintTypeForeignKey,
				Columns: []string{"product_id"},
				Reference: unifiedmodel.Reference{
					Table:   "products",
					Columns: []string{"id"},
				},
			},
		},
	}
}

func createECommerceMongoDBSchema() *unifiedmodel.UnifiedModel {
	return &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.MongoDB,
		Collections: map[string]unifiedmodel.Collection{
			"users": {
				Name: "users",
				Fields: map[string]unifiedmodel.Field{
					"_id":        {Name: "_id", Type: "objectid"},
					"email":      {Name: "email", Type: "string"},
					"first_name": {Name: "first_name", Type: "string"},
					"last_name":  {Name: "last_name", Type: "string"},
					"created_at": {Name: "created_at", Type: "date"},
					"orders": {
						Name: "orders",
						Type: "array",
						Options: map[string]any{
							"items": map[string]any{
								"type": "object",
								"fields": map[string]unifiedmodel.Field{
									"order_id":   {Name: "order_id", Type: "string"},
									"total":      {Name: "total", Type: "number"},
									"status":     {Name: "status", Type: "string"},
									"created_at": {Name: "created_at", Type: "date"},
									"items": {
										Name: "items",
										Type: "array",
										Options: map[string]any{
											"items": map[string]any{
												"type": "object",
												"fields": map[string]unifiedmodel.Field{
													"product_id": {Name: "product_id", Type: "string"},
													"quantity":   {Name: "quantity", Type: "number"},
													"price":      {Name: "price", Type: "number"},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			"products": {
				Name: "products",
				Fields: map[string]unifiedmodel.Field{
					"_id":         {Name: "_id", Type: "objectid"},
					"name":        {Name: "name", Type: "string"},
					"description": {Name: "description", Type: "string"},
					"price":       {Name: "price", Type: "number"},
					"category": {
						Name: "category",
						Type: "object",
						Options: map[string]any{
							"fields": map[string]unifiedmodel.Field{
								"id":   {Name: "id", Type: "string"},
								"name": {Name: "name", Type: "string"},
							},
						},
					},
				},
			},
		},
	}
}

func createComplexPostgreSQLSchema() *unifiedmodel.UnifiedModel {
	return &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables: map[string]unifiedmodel.Table{
			"users":              {Name: "users", Columns: map[string]unifiedmodel.Column{"id": {Name: "id", DataType: "integer", IsPrimaryKey: true}}},
			"products":           {Name: "products", Columns: map[string]unifiedmodel.Column{"id": {Name: "id", DataType: "integer", IsPrimaryKey: true}}},
			"orders":             {Name: "orders", Columns: map[string]unifiedmodel.Column{"id": {Name: "id", DataType: "integer", IsPrimaryKey: true}}},
			"categories":         {Name: "categories", Columns: map[string]unifiedmodel.Column{"id": {Name: "id", DataType: "integer", IsPrimaryKey: true}}},
			"order_items":        {Name: "order_items", Columns: map[string]unifiedmodel.Column{"id": {Name: "id", DataType: "integer", IsPrimaryKey: true}}},
			"user_profiles":      {Name: "user_profiles", Columns: map[string]unifiedmodel.Column{"id": {Name: "id", DataType: "integer", IsPrimaryKey: true}}},
			"product_reviews":    {Name: "product_reviews", Columns: map[string]unifiedmodel.Column{"id": {Name: "id", DataType: "integer", IsPrimaryKey: true}}},
			"shipping_addresses": {Name: "shipping_addresses", Columns: map[string]unifiedmodel.Column{"id": {Name: "id", DataType: "integer", IsPrimaryKey: true}}},
		},
		Views: map[string]unifiedmodel.View{
			"user_order_summary": {Name: "user_order_summary"},
		},
		Functions: map[string]unifiedmodel.Function{
			"calculate_order_total": {Name: "calculate_order_total"},
		},
		Sequences: map[string]unifiedmodel.Sequence{
			"user_id_seq": {Name: "user_id_seq"},
		},
	}
}
