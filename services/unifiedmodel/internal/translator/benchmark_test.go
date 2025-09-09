package translator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/core"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/cross_paradigm"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/same_paradigm"
)

// Benchmark tests for translator v2 performance

func BenchmarkTranslatorV2_SameParadigm_Small(b *testing.B) {
	sameParadigmTranslator := same_paradigm.NewSameParadigmTranslator()
	crossParadigmTranslator := cross_paradigm.NewCrossParadigmTranslator()
	translator := core.NewUnifiedTranslator(sameParadigmTranslator, crossParadigmTranslator)

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MySQL,
		SourceSchema:   createSmallSchema(),
		RequestID:      "benchmark-small",
		RequestedAt:    time.Now(),
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := translator.Translate(ctx, request)
		if err != nil {
			b.Fatalf("Translation failed: %v", err)
		}
	}
}

func BenchmarkTranslatorV2_SameParadigm_Medium(b *testing.B) {
	sameParadigmTranslator := same_paradigm.NewSameParadigmTranslator()
	crossParadigmTranslator := cross_paradigm.NewCrossParadigmTranslator()
	translator := core.NewUnifiedTranslator(sameParadigmTranslator, crossParadigmTranslator)

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MySQL,
		SourceSchema:   createMediumSchema(),
		RequestID:      "benchmark-medium",
		RequestedAt:    time.Now(),
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := translator.Translate(ctx, request)
		if err != nil {
			b.Fatalf("Translation failed: %v", err)
		}
	}
}

func BenchmarkTranslatorV2_SameParadigm_Large(b *testing.B) {
	sameParadigmTranslator := same_paradigm.NewSameParadigmTranslator()
	crossParadigmTranslator := cross_paradigm.NewCrossParadigmTranslator()
	translator := core.NewUnifiedTranslator(sameParadigmTranslator, crossParadigmTranslator)

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MySQL,
		SourceSchema:   createLargeSchema(),
		RequestID:      "benchmark-large",
		RequestedAt:    time.Now(),
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := translator.Translate(ctx, request)
		if err != nil {
			b.Fatalf("Translation failed: %v", err)
		}
	}
}

func BenchmarkTranslatorV2_CrossParadigm_RelationalToDocument(b *testing.B) {
	sameParadigmTranslator := same_paradigm.NewSameParadigmTranslator()
	crossParadigmTranslator := cross_paradigm.NewCrossParadigmTranslator()
	translator := core.NewUnifiedTranslator(sameParadigmTranslator, crossParadigmTranslator)

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MongoDB,
		SourceSchema:   createMediumSchema(),
		Preferences: core.TranslationPreferences{
			PreferredStrategy: core.ConversionStrategyDenormalization,
			AcceptDataLoss:    true,
		},
		RequestID:   "benchmark-cross-paradigm",
		RequestedAt: time.Now(),
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := translator.Translate(ctx, request)
		if err != nil {
			b.Fatalf("Translation failed: %v", err)
		}
	}
}

func BenchmarkTranslatorV2_CrossParadigm_WithEnrichment(b *testing.B) {
	sameParadigmTranslator := same_paradigm.NewSameParadigmTranslator()
	crossParadigmTranslator := cross_paradigm.NewCrossParadigmTranslator()
	translator := core.NewUnifiedTranslator(sameParadigmTranslator, crossParadigmTranslator)

	enrichment := createBenchmarkEnrichment()

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MongoDB,
		SourceSchema:   createMediumSchema(),
		Enrichment:     enrichment,
		Preferences: core.TranslationPreferences{
			PreferredStrategy: core.ConversionStrategyDenormalization,
			AcceptDataLoss:    true,
		},
		RequestID:   "benchmark-enrichment",
		RequestedAt: time.Now(),
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := translator.Translate(ctx, request)
		if err != nil {
			b.Fatalf("Translation failed: %v", err)
		}
	}
}

func BenchmarkTranslatorV2_Analysis_Only(b *testing.B) {
	sameParadigmTranslator := same_paradigm.NewSameParadigmTranslator()
	crossParadigmTranslator := cross_paradigm.NewCrossParadigmTranslator()
	translator := core.NewUnifiedTranslator(sameParadigmTranslator, crossParadigmTranslator)

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MongoDB,
		SourceSchema:   createMediumSchema(),
		RequestID:      "benchmark-analysis",
		RequestedAt:    time.Now(),
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := translator.AnalyzeTranslation(ctx, request)
		if err != nil {
			b.Fatalf("Analysis failed: %v", err)
		}
	}
}

func BenchmarkEnrichmentAnalyzer_WithoutEnrichment(b *testing.B) {
	analyzer := cross_paradigm.NewEnrichmentAnalyzer()

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MongoDB,
		RequestID:      "benchmark-enrichment-analyzer",
		RequestedAt:    time.Now(),
	}

	ctx := core.NewTranslationContext(context.Background(), request)

	sourceSchema := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables:       createBenchmarkTables(),
	}
	ctx.SetSourceSchema(sourceSchema)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := analyzer.AnalyzeEnrichment(ctx)
		if err != nil {
			b.Fatalf("Enrichment analysis failed: %v", err)
		}
	}
}

func BenchmarkEnrichmentAnalyzer_WithEnrichment(b *testing.B) {
	analyzer := cross_paradigm.NewEnrichmentAnalyzer()

	enrichment := createBenchmarkEnrichment()

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MongoDB,
		Enrichment:     enrichment,
		RequestID:      "benchmark-enrichment-analyzer-with-data",
		RequestedAt:    time.Now(),
	}

	ctx := core.NewTranslationContext(context.Background(), request)

	sourceSchema := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables:       createBenchmarkTables(),
	}
	ctx.SetSourceSchema(sourceSchema)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := analyzer.AnalyzeEnrichment(ctx)
		if err != nil {
			b.Fatalf("Enrichment analysis failed: %v", err)
		}
	}
}

// Memory allocation benchmarks

func BenchmarkTranslatorV2_MemoryAllocation(b *testing.B) {
	sameParadigmTranslator := same_paradigm.NewSameParadigmTranslator()
	crossParadigmTranslator := cross_paradigm.NewCrossParadigmTranslator()
	translator := core.NewUnifiedTranslator(sameParadigmTranslator, crossParadigmTranslator)

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MySQL,
		SourceSchema:   createMediumSchema(),
		RequestID:      "benchmark-memory",
		RequestedAt:    time.Now(),
	}

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := translator.Translate(ctx, request)
		if err != nil {
			b.Fatalf("Translation failed: %v", err)
		}
		// Prevent compiler optimization
		_ = result
	}
}

// Concurrent translation benchmarks

func BenchmarkTranslatorV2_Concurrent(b *testing.B) {
	sameParadigmTranslator := same_paradigm.NewSameParadigmTranslator()
	crossParadigmTranslator := cross_paradigm.NewCrossParadigmTranslator()
	translator := core.NewUnifiedTranslator(sameParadigmTranslator, crossParadigmTranslator)

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MySQL,
		SourceSchema:   createMediumSchema(),
		RequestID:      "benchmark-concurrent",
		RequestedAt:    time.Now(),
	}

	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := translator.Translate(ctx, request)
			if err != nil {
				b.Fatalf("Translation failed: %v", err)
			}
		}
	})
}

// Helper functions for creating benchmark schemas

func createSmallSchema() *unifiedmodel.UnifiedModel {
	return &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables: map[string]unifiedmodel.Table{
			"users": {
				Name: "users",
				Columns: map[string]unifiedmodel.Column{
					"id": {
						Name:         "id",
						DataType:     "integer",
						IsPrimaryKey: true,
					},
					"email": {
						Name:     "email",
						DataType: "varchar(255)",
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
		},
	}
}

func createMediumSchema() *unifiedmodel.UnifiedModel {
	return &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables: map[string]unifiedmodel.Table{
			"users": {
				Name: "users",
				Columns: map[string]unifiedmodel.Column{
					"id":         {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"email":      {Name: "email", DataType: "varchar(255)"},
					"first_name": {Name: "first_name", DataType: "varchar(100)"},
					"last_name":  {Name: "last_name", DataType: "varchar(100)"},
					"created_at": {Name: "created_at", DataType: "timestamp"},
				},
			},
			"products": {
				Name: "products",
				Columns: map[string]unifiedmodel.Column{
					"id":          {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"name":        {Name: "name", DataType: "varchar(255)"},
					"description": {Name: "description", DataType: "text"},
					"price":       {Name: "price", DataType: "decimal(10,2)"},
					"category_id": {Name: "category_id", DataType: "integer"},
				},
			},
			"categories": {
				Name: "categories",
				Columns: map[string]unifiedmodel.Column{
					"id":   {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"name": {Name: "name", DataType: "varchar(100)"},
				},
			},
			"orders": {
				Name: "orders",
				Columns: map[string]unifiedmodel.Column{
					"id":         {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"user_id":    {Name: "user_id", DataType: "integer"},
					"total":      {Name: "total", DataType: "decimal(10,2)"},
					"status":     {Name: "status", DataType: "varchar(50)"},
					"created_at": {Name: "created_at", DataType: "timestamp"},
				},
			},
			"order_items": {
				Name: "order_items",
				Columns: map[string]unifiedmodel.Column{
					"id":         {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"order_id":   {Name: "order_id", DataType: "integer"},
					"product_id": {Name: "product_id", DataType: "integer"},
					"quantity":   {Name: "quantity", DataType: "integer"},
					"price":      {Name: "price", DataType: "decimal(10,2)"},
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

func createLargeSchema() *unifiedmodel.UnifiedModel {
	tables := make(map[string]unifiedmodel.Table)
	constraints := make(map[string]unifiedmodel.Constraint)

	// Create 20 tables with varying complexity
	for i := 1; i <= 20; i++ {
		tableName := fmt.Sprintf("table_%d", i)
		columns := make(map[string]unifiedmodel.Column)

		// Add primary key
		columns["id"] = unifiedmodel.Column{
			Name: "id", DataType: "integer", IsPrimaryKey: true,
		}

		// Add 5-15 columns per table
		numColumns := 5 + (i % 10)
		for j := 1; j <= numColumns; j++ {
			columnName := fmt.Sprintf("column_%d", j)
			dataType := []string{"varchar(255)", "integer", "decimal(10,2)", "timestamp", "text"}[j%5]
			columns[columnName] = unifiedmodel.Column{
				Name: columnName, DataType: dataType,
			}
		}

		tables[tableName] = unifiedmodel.Table{
			Name: tableName, Columns: columns,
		}

		// Add some foreign key constraints
		if i > 1 && i%3 == 0 {
			constraintName := fmt.Sprintf("fk_%s_ref", tableName)
			refTable := fmt.Sprintf("table_%d", i-1)
			constraints[constraintName] = unifiedmodel.Constraint{
				Name:    constraintName,
				Type:    unifiedmodel.ConstraintTypeForeignKey,
				Columns: []string{"column_1"},
				Reference: unifiedmodel.Reference{
					Table:   refTable,
					Columns: []string{"id"},
				},
			}
		}
	}

	return &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables:       tables,
		Constraints:  constraints,
	}
}

func createBenchmarkTables() map[string]unifiedmodel.Table {
	tables := make(map[string]unifiedmodel.Table)

	for i := 1; i <= 10; i++ {
		tableName := fmt.Sprintf("table_%d", i)
		columns := make(map[string]unifiedmodel.Column)

		columns["id"] = unifiedmodel.Column{
			Name: "id", DataType: "integer", IsPrimaryKey: true,
		}

		for j := 1; j <= 5; j++ {
			columnName := fmt.Sprintf("column_%d", j)
			columns[columnName] = unifiedmodel.Column{
				Name: columnName, DataType: "varchar(255)",
			}
		}

		tables[tableName] = unifiedmodel.Table{
			Name: tableName, Columns: columns,
		}
	}

	return tables
}

func createBenchmarkEnrichment() *unifiedmodel.UnifiedModelEnrichment {
	tableEnrichments := make(map[string]unifiedmodel.TableEnrichment)

	for i := 1; i <= 10; i++ {
		tableName := fmt.Sprintf("table_%d", i)
		tableEnrichments[tableName] = unifiedmodel.TableEnrichment{
			PrimaryCategory:          unifiedmodel.TableCategoryTransactional,
			ClassificationConfidence: 0.9,
			AccessPattern:            unifiedmodel.AccessPatternReadWrite,
			BusinessPurpose:          fmt.Sprintf("Business purpose for %s", tableName),
		}
	}

	return &unifiedmodel.UnifiedModelEnrichment{
		TableEnrichments: tableEnrichments,
		PerformanceHints: []unifiedmodel.PerformanceHint{
			{
				Category:         "indexing",
				Priority:         unifiedmodel.ConversionPriorityMedium,
				Hint:             "Create index for better performance",
				ObjectPath:       "tables.table_1.columns.column_1",
				EstimatedBenefit: "20% performance improvement",
			},
		},
	}
}
