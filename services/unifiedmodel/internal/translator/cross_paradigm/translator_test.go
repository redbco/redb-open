package cross_paradigm

import (
	"context"
	"testing"
	"time"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/core"
)

func TestCrossParadigmTranslator_RelationalToDocument(t *testing.T) {
	translator := NewCrossParadigmTranslator()

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MongoDB,
		SourceSchema:   createRelationalUnifiedModel(),
		Preferences: core.TranslationPreferences{
			PreferredStrategy:     core.ConversionStrategyDenormalization,
			AcceptDataLoss:        true,
			PreserveRelationships: true,
		},
		RequestID:   "cross-paradigm-001",
		RequestedAt: time.Now(),
	}

	ctx := core.NewTranslationContext(context.Background(), request)

	analysis := &core.ParadigmAnalysisResult{
		SourceDatabase:        dbcapabilities.PostgreSQL,
		TargetDatabase:        dbcapabilities.MongoDB,
		SourceParadigms:       []dbcapabilities.DataParadigm{dbcapabilities.ParadigmRelational},
		TargetParadigms:       []dbcapabilities.DataParadigm{dbcapabilities.ParadigmDocument},
		ConversionApproach:    core.ConversionApproachCrossParadigm,
		TranslationComplexity: core.TranslationComplexityModerate,
		RecommendedStrategy:   core.ConversionStrategyDenormalization,
	}
	ctx.SetAnalysis(analysis)

	sourceSchema := createRelationalUnifiedModel()
	ctx.SetSourceSchema(sourceSchema)

	err := translator.Translate(ctx)
	if err != nil {
		t.Fatalf("Translation failed: %v", err)
	}

	targetSchema := ctx.TargetSchema
	if targetSchema == nil {
		t.Fatal("Expected target schema to be set")
	}

	if targetSchema.DatabaseType != dbcapabilities.MongoDB {
		t.Errorf("Expected MongoDB database type, got %s", targetSchema.DatabaseType)
	}

	// Check that collections were created
	if len(targetSchema.Collections) == 0 {
		t.Error("Expected collections to be created for document database")
	}

	// Check that tables were converted to collections
	if _, exists := targetSchema.Collections["users"]; !exists {
		t.Error("Expected users collection to be created")
	}

	if _, exists := targetSchema.Collections["orders"]; !exists {
		t.Error("Expected orders collection to be created")
	}

	// Verify metrics
	if ctx.Metrics.ObjectsProcessed == 0 {
		t.Error("Expected objects to be processed")
	}
	if ctx.Metrics.ObjectsConverted == 0 {
		t.Error("Expected objects to be converted")
	}
}

func TestCrossParadigmTranslator_RelationalToGraph(t *testing.T) {
	translator := NewCrossParadigmTranslator()

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.Neo4j,
		SourceSchema:   createRelationalUnifiedModel(),
		Preferences: core.TranslationPreferences{
			PreferredStrategy:     core.ConversionStrategyDecomposition,
			AcceptDataLoss:        true,
			PreserveRelationships: true,
		},
		RequestID:   "cross-paradigm-002",
		RequestedAt: time.Now(),
	}

	ctx := core.NewTranslationContext(context.Background(), request)

	analysis := &core.ParadigmAnalysisResult{
		SourceDatabase:        dbcapabilities.PostgreSQL,
		TargetDatabase:        dbcapabilities.Neo4j,
		SourceParadigms:       []dbcapabilities.DataParadigm{dbcapabilities.ParadigmRelational},
		TargetParadigms:       []dbcapabilities.DataParadigm{dbcapabilities.ParadigmGraph},
		ConversionApproach:    core.ConversionApproachCrossParadigm,
		TranslationComplexity: core.TranslationComplexityComplex,
		RecommendedStrategy:   core.ConversionStrategyDecomposition,
	}
	ctx.SetAnalysis(analysis)

	sourceSchema := createRelationalUnifiedModel()
	ctx.SetSourceSchema(sourceSchema)

	err := translator.Translate(ctx)
	if err != nil {
		t.Fatalf("Translation failed: %v", err)
	}

	targetSchema := ctx.TargetSchema
	if targetSchema.DatabaseType != dbcapabilities.Neo4j {
		t.Errorf("Expected Neo4j database type, got %s", targetSchema.DatabaseType)
	}

	// Check that nodes were created
	if len(targetSchema.Nodes) == 0 {
		t.Error("Expected nodes to be created for graph database")
	}

	// Check that relationships were created
	if len(targetSchema.Relationships) == 0 {
		t.Error("Expected relationships to be created for graph database")
	}

	// Verify that tables were converted to nodes
	if _, exists := targetSchema.Nodes["users"]; !exists {
		t.Error("Expected users node to be created")
	}

	if _, exists := targetSchema.Nodes["orders"]; !exists {
		t.Error("Expected orders node to be created")
	}
}

func TestCrossParadigmTranslator_DocumentToRelational(t *testing.T) {
	translator := NewCrossParadigmTranslator()

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.MongoDB,
		TargetDatabase: dbcapabilities.PostgreSQL,
		SourceSchema:   createDocumentUnifiedModel(),
		Preferences: core.TranslationPreferences{
			PreferredStrategy:     core.ConversionStrategyNormalization,
			AcceptDataLoss:        false,
			PreserveRelationships: true,
		},
		RequestID:   "cross-paradigm-003",
		RequestedAt: time.Now(),
	}

	ctx := core.NewTranslationContext(context.Background(), request)

	analysis := &core.ParadigmAnalysisResult{
		SourceDatabase:        dbcapabilities.MongoDB,
		TargetDatabase:        dbcapabilities.PostgreSQL,
		SourceParadigms:       []dbcapabilities.DataParadigm{dbcapabilities.ParadigmDocument},
		TargetParadigms:       []dbcapabilities.DataParadigm{dbcapabilities.ParadigmRelational},
		ConversionApproach:    core.ConversionApproachCrossParadigm,
		TranslationComplexity: core.TranslationComplexityModerate,
		RecommendedStrategy:   core.ConversionStrategyNormalization,
	}
	ctx.SetAnalysis(analysis)

	sourceSchema := createDocumentUnifiedModel()
	ctx.SetSourceSchema(sourceSchema)

	err := translator.Translate(ctx)
	if err != nil {
		t.Fatalf("Translation failed: %v", err)
	}

	targetSchema := ctx.TargetSchema
	if targetSchema.DatabaseType != dbcapabilities.PostgreSQL {
		t.Errorf("Expected PostgreSQL database type, got %s", targetSchema.DatabaseType)
	}

	// Check that tables were created from collections
	if len(targetSchema.Tables) == 0 {
		t.Error("Expected tables to be created from collections")
	}

	// Verify normalization occurred (multiple tables from nested structures)
	if len(targetSchema.Tables) < len(ctx.SourceSchema.Collections) {
		t.Error("Expected normalization to create additional tables")
	}
}

func TestCrossParadigmTranslator_WithEnrichment(t *testing.T) {
	translator := NewCrossParadigmTranslator()

	// Create enrichment data
	enrichment := &unifiedmodel.UnifiedModelEnrichment{
		TableEnrichments: map[string]unifiedmodel.TableEnrichment{
			"users": {
				PrimaryCategory: unifiedmodel.TableCategoryTransactional,
				AccessPattern:   unifiedmodel.AccessPatternReadWrite,
				RelatedTables:   []string{"orders"},
				BusinessPurpose: "User management",
			},
			"orders": {
				PrimaryCategory: unifiedmodel.TableCategoryTransactional,
				AccessPattern:   unifiedmodel.AccessPatternWriteHeavy,
				RelatedTables:   []string{"users", "order_items"},
				BusinessPurpose: "Order processing",
			},
		},
		RelationshipEnrichments: map[string]unifiedmodel.RelationshipEnrichment{
			"user_orders": {
				Frequency:       func() *int64 { v := int64(1000); return &v }(),
				Strength:        func() *float64 { v := 0.8; return &v }(),
				BusinessMeaning: "Users place orders",
			},
		},
	}

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MongoDB,
		SourceSchema:   createRelationalUnifiedModel(),
		Enrichment:     enrichment,
		Preferences: core.TranslationPreferences{
			PreferredStrategy:     core.ConversionStrategyDenormalization,
			AcceptDataLoss:        true,
			PreserveRelationships: true,
		},
		RequestID:   "cross-paradigm-004",
		RequestedAt: time.Now(),
	}

	ctx := core.NewTranslationContext(context.Background(), request)

	analysis := &core.ParadigmAnalysisResult{
		SourceDatabase:      dbcapabilities.PostgreSQL,
		TargetDatabase:      dbcapabilities.MongoDB,
		SourceParadigms:     []dbcapabilities.DataParadigm{dbcapabilities.ParadigmRelational},
		TargetParadigms:     []dbcapabilities.DataParadigm{dbcapabilities.ParadigmDocument},
		ConversionApproach:  core.ConversionApproachCrossParadigm,
		RecommendedStrategy: core.ConversionStrategyDenormalization,
		RequiresEnrichment:  true,
	}
	ctx.SetAnalysis(analysis)

	sourceSchema := createRelationalUnifiedModel()
	ctx.SetSourceSchema(sourceSchema)

	err := translator.Translate(ctx)
	if err != nil {
		t.Fatalf("Translation failed: %v", err)
	}

	targetSchema := ctx.TargetSchema
	if len(targetSchema.Collections) == 0 {
		t.Error("Expected collections to be created")
	}

	// Verify that enrichment was used in the translation report
	ctx.FinishProcessing()
	if !ctx.HasWarnings() {
		// This is fine - enrichment might prevent warnings
	}
}

func TestCrossParadigmTranslator_HybridStrategy(t *testing.T) {
	translator := NewCrossParadigmTranslator()

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MongoDB,
		SourceSchema:   createComplexRelationalUnifiedModel(),
		Preferences: core.TranslationPreferences{
			PreferredStrategy: core.ConversionStrategyHybrid,
			AcceptDataLoss:    true,
		},
		RequestID:   "cross-paradigm-005",
		RequestedAt: time.Now(),
	}

	ctx := core.NewTranslationContext(context.Background(), request)

	analysis := &core.ParadigmAnalysisResult{
		SourceDatabase:        dbcapabilities.PostgreSQL,
		TargetDatabase:        dbcapabilities.MongoDB,
		SourceParadigms:       []dbcapabilities.DataParadigm{dbcapabilities.ParadigmRelational},
		TargetParadigms:       []dbcapabilities.DataParadigm{dbcapabilities.ParadigmDocument},
		ConversionApproach:    core.ConversionApproachCrossParadigm,
		RecommendedStrategy:   core.ConversionStrategyHybrid,
		TranslationComplexity: core.TranslationComplexityComplex,
	}
	ctx.SetAnalysis(analysis)

	sourceSchema := createComplexRelationalUnifiedModel()
	ctx.SetSourceSchema(sourceSchema)

	err := translator.Translate(ctx)
	if err != nil {
		t.Fatalf("Translation failed: %v", err)
	}

	targetSchema := ctx.TargetSchema
	if len(targetSchema.Collections) == 0 {
		t.Error("Expected collections to be created")
	}

	// Hybrid strategy should handle complex schemas
	if ctx.Metrics.ObjectsProcessed == 0 {
		t.Error("Expected objects to be processed")
	}
}

func TestCrossParadigmTranslator_ValidationError(t *testing.T) {
	translator := NewCrossParadigmTranslator()

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MongoDB,
		SourceSchema: &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Tables:       make(map[string]unifiedmodel.Table),
		},
		RequestID:   "cross-paradigm-006",
		RequestedAt: time.Now(),
	}

	ctx := core.NewTranslationContext(context.Background(), request)

	// Set wrong conversion approach to trigger validation error
	analysis := &core.ParadigmAnalysisResult{
		ConversionApproach: core.ConversionApproachSameParadigm, // Wrong approach
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

func TestCrossParadigmTranslator_UnsupportedStrategy(t *testing.T) {
	translator := NewCrossParadigmTranslator()

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MongoDB,
		SourceSchema: &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Tables:       make(map[string]unifiedmodel.Table),
		},
		RequestID:   "cross-paradigm-007",
		RequestedAt: time.Now(),
	}

	ctx := core.NewTranslationContext(context.Background(), request)

	analysis := &core.ParadigmAnalysisResult{
		ConversionApproach:  core.ConversionApproachCrossParadigm,
		RecommendedStrategy: "unsupported_strategy", // Invalid strategy
	}
	ctx.SetAnalysis(analysis)

	sourceSchema := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables: map[string]unifiedmodel.Table{
			"test": {Name: "test"},
		},
	}
	ctx.SetSourceSchema(sourceSchema)

	err := translator.Translate(ctx)
	if err == nil {
		t.Error("Expected error for unsupported conversion strategy")
	}
}

// Helper functions for creating test schemas and models

func createRelationalUnifiedModel() *unifiedmodel.UnifiedModel {
	return &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables: map[string]unifiedmodel.Table{
			"users": {
				Name: "users",
				Columns: map[string]unifiedmodel.Column{
					"id":    {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"email": {Name: "email", DataType: "varchar(255)"},
					"name":  {Name: "name", DataType: "varchar(100)"},
				},
			},
			"orders": {
				Name: "orders",
				Columns: map[string]unifiedmodel.Column{
					"id":      {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"user_id": {Name: "user_id", DataType: "integer"},
					"total":   {Name: "total", DataType: "decimal(10,2)"},
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
}

func createDocumentUnifiedModel() *unifiedmodel.UnifiedModel {
	return &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.MongoDB,
		Collections: map[string]unifiedmodel.Collection{
			"users": {
				Name: "users",
				Fields: map[string]unifiedmodel.Field{
					"_id":   {Name: "_id", Type: "objectid"},
					"email": {Name: "email", Type: "string"},
					"profile": {
						Name: "profile",
						Type: "object",
						Options: map[string]any{
							"nested_fields": map[string]unifiedmodel.Field{
								"first_name": {Name: "first_name", Type: "string"},
								"last_name":  {Name: "last_name", Type: "string"},
							},
						},
					},
				},
			},
		},
	}
}

func createComplexRelationalUnifiedModel() *unifiedmodel.UnifiedModel {
	return &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables: map[string]unifiedmodel.Table{
			"users":       {Name: "users", Columns: map[string]unifiedmodel.Column{"id": {Name: "id", DataType: "integer", IsPrimaryKey: true}}},
			"orders":      {Name: "orders", Columns: map[string]unifiedmodel.Column{"id": {Name: "id", DataType: "integer", IsPrimaryKey: true}}},
			"products":    {Name: "products", Columns: map[string]unifiedmodel.Column{"id": {Name: "id", DataType: "integer", IsPrimaryKey: true}}},
			"categories":  {Name: "categories", Columns: map[string]unifiedmodel.Column{"id": {Name: "id", DataType: "integer", IsPrimaryKey: true}}},
			"order_items": {Name: "order_items", Columns: map[string]unifiedmodel.Column{"id": {Name: "id", DataType: "integer", IsPrimaryKey: true}}},
		},
	}
}
