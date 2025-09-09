package translator

import (
	"context"
	"fmt"
	"time"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/core"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/utils"
)

// ExampleUsage demonstrates how to use the new translator
func ExampleUsage() {
	// Create a translator instance
	translator := NewUnifiedTranslator()

	// Create a sample translation request
	request := createSampleTranslationRequest()

	// Validate the request
	validator := utils.NewSchemaValidator()
	validationResult := validator.ValidateTranslationRequest(request)

	if !validationResult.IsValid {
		fmt.Printf("Request validation failed with %d errors\n", len(validationResult.Errors))
		for _, err := range validationResult.Errors {
			fmt.Printf("  - %s: %s\n", err.Type, err.Message)
		}
		return
	}

	fmt.Println("Request validation passed")

	// Analyze translation feasibility
	ctx := context.Background()
	analysis, err := translator.AnalyzeTranslation(ctx, request)
	if err != nil {
		fmt.Printf("Translation analysis failed: %v\n", err)
		return
	}

	fmt.Printf("Translation Analysis:\n")
	fmt.Printf("  Supported: %t\n", analysis.ConversionSupported)
	fmt.Printf("  Complexity: %s\n", analysis.TranslationComplexity)
	fmt.Printf("  Paradigm Compatibility: %s\n", analysis.ParadigmCompatibility)
	fmt.Printf("  Requires User Input: %t\n", analysis.RequiresUserInput)
	fmt.Printf("  Requires Enrichment: %t\n", analysis.RequiresEnrichment)
	fmt.Printf("  Estimated Duration: %s\n", analysis.EstimatedDuration)
	fmt.Printf("  Estimated Success Rate: %.2f%%\n", analysis.EstimatedSuccessRate*100)

	if len(analysis.UnsupportedFeatures) > 0 {
		fmt.Printf("  Unsupported Features:\n")
		for _, feature := range analysis.UnsupportedFeatures {
			fmt.Printf("    - %s: %s\n", feature.FeatureType, feature.Description)
		}
	}

	if len(analysis.Recommendations) > 0 {
		fmt.Printf("  Recommendations:\n")
		for _, rec := range analysis.Recommendations {
			fmt.Printf("    - %s\n", rec)
		}
	}

	// Perform the translation if supported
	if analysis.ConversionSupported {
		fmt.Println("\nPerforming translation...")

		result, err := translator.Translate(ctx, request)
		if err != nil {
			fmt.Printf("Translation failed: %v\n", err)
			return
		}

		if result.Success {
			fmt.Printf("Translation completed successfully in %v\n", result.ProcessingTime)
			fmt.Printf("Objects processed: %d\n", result.TranslationReport.ObjectsProcessed)
			fmt.Printf("Objects converted: %d\n", result.TranslationReport.ObjectsConverted)
			fmt.Printf("Objects skipped: %d\n", result.TranslationReport.ObjectsSkipped)
			fmt.Printf("Objects dropped: %d\n", result.TranslationReport.ObjectsDropped)

			if len(result.Warnings) > 0 {
				fmt.Printf("Warnings (%d):\n", len(result.Warnings))
				for _, warning := range result.Warnings {
					fmt.Printf("  - %s: %s\n", warning.WarningType, warning.Message)
				}
			}

			// Validate the translation result
			resultValidation := validator.ValidateTranslationResult(result)
			fmt.Printf("Result validation: %t\n", resultValidation.IsValid)
			if resultValidation.SchemaHealth.OverallScore > 0 {
				fmt.Printf("Schema health score: %.1f/100\n", resultValidation.SchemaHealth.OverallScore)
			}
		} else {
			fmt.Printf("Translation failed: %s\n", result.ErrorMessage)
		}
	} else {
		fmt.Println("Translation is not supported for this database pair")
	}
}

// ExampleSameParadigmTranslation demonstrates same-paradigm translation
func ExampleSameParadigmTranslation() {
	fmt.Println("=== Same-Paradigm Translation Example (PostgreSQL → MySQL) ===")

	translator := NewUnifiedTranslator()

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MySQL,
		SourceSchema:   createSamplePostgreSQLSchema(),
		Preferences: core.TranslationPreferences{
			AcceptDataLoss:         false,
			OptimizeForPerformance: true,
			PreserveRelationships:  true,
			GenerateComments:       true,
		},
		RequestID:   "same-paradigm-example",
		RequestedAt: time.Now(),
	}

	ctx := context.Background()
	result, err := translator.Translate(ctx, request)
	if err != nil {
		fmt.Printf("Translation failed: %v\n", err)
		return
	}

	if result.Success {
		fmt.Printf("✓ Same-paradigm translation completed successfully\n")
		fmt.Printf("  Processing time: %v\n", result.ProcessingTime)
		fmt.Printf("  Paradigm compatibility: %s\n", result.TranslationReport.ParadigmCompatibility)
	}
}

// ExampleCrossParadigmTranslation demonstrates cross-paradigm translation
func ExampleCrossParadigmTranslation() {
	fmt.Println("=== Cross-Paradigm Translation Example (PostgreSQL → MongoDB) ===")

	translator := NewUnifiedTranslator()

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MongoDB,
		SourceSchema:   createSamplePostgreSQLSchema(),
		Preferences: core.TranslationPreferences{
			AcceptDataLoss:        true, // Cross-paradigm may require some data loss
			PreferredStrategy:     core.ConversionStrategyDenormalization,
			PreserveRelationships: true,
			GenerateComments:      true,
		},
		RequestID:   "cross-paradigm-example",
		RequestedAt: time.Now(),
	}

	ctx := context.Background()
	result, err := translator.Translate(ctx, request)
	if err != nil {
		fmt.Printf("Translation failed: %v\n", err)
		return
	}

	if result.Success {
		fmt.Printf("✓ Cross-paradigm translation completed successfully\n")
		fmt.Printf("  Processing time: %v\n", result.ProcessingTime)
		fmt.Printf("  Paradigm compatibility: %s\n", result.TranslationReport.ParadigmCompatibility)
		fmt.Printf("  Strategies used: %v\n", result.TranslationReport.StrategiesUsed)

		if len(result.Warnings) > 0 {
			fmt.Printf("  Warnings: %d (expected for cross-paradigm)\n", len(result.Warnings))
		}
	}
}

// ExampleWithEnrichment demonstrates translation with enrichment data
func ExampleWithEnrichment() {
	fmt.Println("=== Translation with Enrichment Example (PostgreSQL → Neo4j) ===")

	translator := NewUnifiedTranslator()

	// Create enrichment data to guide the translation
	enrichment := createSampleEnrichmentData()

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.Neo4j,
		SourceSchema:   createSamplePostgreSQLSchema(),
		Enrichment:     enrichment,
		Preferences: core.TranslationPreferences{
			AcceptDataLoss:        true,
			PreferredStrategy:     core.ConversionStrategyDecomposition,
			PreserveRelationships: true,
			GenerateComments:      true,
		},
		RequestID:   "enrichment-example",
		RequestedAt: time.Now(),
	}

	ctx := context.Background()

	// First analyze with enrichment
	analysis, err := translator.AnalyzeTranslation(ctx, request)
	if err != nil {
		fmt.Printf("Analysis failed: %v\n", err)
		return
	}

	fmt.Printf("Analysis with enrichment:\n")
	fmt.Printf("  Enrichment used: %t\n", analysis.RequiresEnrichment)
	fmt.Printf("  Required enrichment types: %v\n", analysis.RequiredEnrichmentTypes)

	// Perform translation
	result, err := translator.Translate(ctx, request)
	if err != nil {
		fmt.Printf("Translation failed: %v\n", err)
		return
	}

	if result.Success {
		fmt.Printf("✓ Enrichment-guided translation completed successfully\n")
		fmt.Printf("  Enrichment used: %t\n", result.TranslationReport.EnrichmentUsed)
	}
}

// Helper functions to create sample data

func createSampleTranslationRequest() *core.TranslationRequest {
	return &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MongoDB,
		SourceSchema:   createSamplePostgreSQLSchema(),
		Preferences: core.TranslationPreferences{
			AcceptDataLoss:         false,
			OptimizeForPerformance: true,
			PreserveRelationships:  true,
			IncludeMetadata:        true,
			GenerateComments:       true,
		},
		RequestID:   "example-translation-001",
		RequestedBy: "example-user",
		RequestedAt: time.Now(),
	}
}

func createSamplePostgreSQLSchema() *unifiedmodel.UnifiedModel {
	// This is a simplified sample schema
	return &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
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
					"email": {
						Name:     "email",
						DataType: "varchar(255)",
						Nullable: false,
					},
					"name": {
						Name:     "name",
						DataType: "varchar(100)",
						Nullable: false,
					},
					"created_at": {
						Name:     "created_at",
						DataType: "timestamp",
						Nullable: false,
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
					"total": {
						Name:     "total",
						DataType: "decimal(10,2)",
						Nullable: false,
					},
					"status": {
						Name:     "status",
						DataType: "varchar(50)",
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

func createSampleEnrichmentData() *unifiedmodel.UnifiedModelEnrichment {
	// This would contain enrichment data to guide translation
	// For now, return a basic structure
	return &unifiedmodel.UnifiedModelEnrichment{
		// Fields would be populated based on the actual structure
		// when UnifiedModelEnrichment is fully defined
	}
}

// RunAllExamples runs all the example functions
func RunAllExamples() {
	fmt.Println("🚀 Running Unified Translator v2 Examples")

	ExampleUsage()
	fmt.Println()

	ExampleSameParadigmTranslation()
	fmt.Println()

	ExampleCrossParadigmTranslation()
	fmt.Println()

	ExampleWithEnrichment()
	fmt.Println()

	fmt.Println("✅ All examples completed!")
}
