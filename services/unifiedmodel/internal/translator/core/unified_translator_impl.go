package core

import (
	"context"
	"fmt"
	"time"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// UnifiedTranslatorImpl is the main implementation of the UnifiedTranslator interface
type UnifiedTranslatorImpl struct {
	paradigmAnalyzer        *ParadigmAnalyzer
	conversionEngine        *unifiedmodel.ConversionEngine
	sameParadigmTranslator  SameParadigmTranslator
	crossParadigmTranslator CrossParadigmTranslator
}

// SameParadigmTranslator interface for same-paradigm translations
type SameParadigmTranslator interface {
	Translate(ctx *TranslationContext) error
}

// CrossParadigmTranslator interface for cross-paradigm translations
type CrossParadigmTranslator interface {
	Translate(ctx *TranslationContext) error
}

// NewUnifiedTranslator creates a new unified translator
func NewUnifiedTranslator(sameParadigmTranslator SameParadigmTranslator, crossParadigmTranslator CrossParadigmTranslator) *UnifiedTranslatorImpl {
	return &UnifiedTranslatorImpl{
		paradigmAnalyzer:        NewParadigmAnalyzer(),
		conversionEngine:        unifiedmodel.NewConversionEngine(),
		sameParadigmTranslator:  sameParadigmTranslator,
		crossParadigmTranslator: crossParadigmTranslator,
	}
}

// Translate performs schema translation between databases
func (ut *UnifiedTranslatorImpl) Translate(ctx context.Context, request *TranslationRequest) (*TranslationResult, error) {
	// Validate request
	if validationErrors := ut.ValidateRequest(request); len(validationErrors) > 0 {
		return ut.createErrorResult(request, fmt.Errorf("validation failed: %d errors", len(validationErrors))), nil
	}

	// Create translation context
	translationCtx := NewTranslationContext(ctx, request)

	// Analyze paradigms
	analysis, err := ut.paradigmAnalyzer.AnalyzeParadigms(request.SourceDatabase, request.TargetDatabase)
	if err != nil {
		return ut.createErrorResult(request, fmt.Errorf("paradigm analysis failed: %w", err)), nil
	}
	translationCtx.SetAnalysis(analysis)

	// Set source schema directly (no parsing needed since it's already a UnifiedModel)
	if request.SourceSchema == nil {
		return ut.createErrorResult(request, fmt.Errorf("source schema is required")), nil
	}
	translationCtx.SetSourceSchema(request.SourceSchema)

	// Check if conversion is possible
	if analysis.ConversionApproach == ConversionApproachImpossible {
		return ut.createErrorResult(request, fmt.Errorf("conversion from %s to %s is not supported", request.SourceDatabase, request.TargetDatabase)), nil
	}

	// Perform translation based on approach
	var translationErr error
	switch analysis.ConversionApproach {
	case ConversionApproachSameParadigm:
		translationErr = ut.sameParadigmTranslator.Translate(translationCtx)
	case ConversionApproachCrossParadigm:
		translationErr = ut.crossParadigmTranslator.Translate(translationCtx)
	case ConversionApproachMultiStep:
		translationErr = ut.performMultiStepTranslation(translationCtx)
	default:
		translationErr = fmt.Errorf("unsupported conversion approach: %s", analysis.ConversionApproach)
	}

	if translationErr != nil {
		return ut.createErrorResult(request, translationErr), nil
	}

	// Finalize processing
	translationCtx.FinishProcessing()

	// Create result
	return ut.createSuccessResult(request, translationCtx), nil
}

// AnalyzeTranslation analyzes translation feasibility without performing it
func (ut *UnifiedTranslatorImpl) AnalyzeTranslation(ctx context.Context, request *TranslationRequest) (*TranslationAnalysis, error) {
	// Validate request
	if validationErrors := ut.ValidateRequest(request); len(validationErrors) > 0 {
		return nil, fmt.Errorf("validation failed: %d errors", len(validationErrors))
	}

	// Analyze paradigms
	analysis, err := ut.paradigmAnalyzer.AnalyzeParadigms(request.SourceDatabase, request.TargetDatabase)
	if err != nil {
		return nil, fmt.Errorf("paradigm analysis failed: %w", err)
	}

	// Use source schema directly for analysis
	if request.SourceSchema == nil {
		return nil, fmt.Errorf("source schema is required")
	}
	sourceSchema := request.SourceSchema

	// Analyze unsupported features
	unsupportedFeatures := ut.analyzeUnsupportedFeatures(sourceSchema, request.SourceDatabase, request.TargetDatabase)

	// Create analysis result
	result := &TranslationAnalysis{
		ConversionSupported:     analysis.ConversionApproach != ConversionApproachImpossible,
		ParadigmCompatibility:   analysis.ParadigmCompatibility,
		TranslationComplexity:   analysis.TranslationComplexity,
		RequiresUserInput:       ut.requiresUserInput(analysis),
		RequiresEnrichment:      analysis.RequiresEnrichment,
		RequiredEnrichmentTypes: analysis.RequiredEnrichmentTypes,
		UnsupportedFeatures:     unsupportedFeatures,
		AvailableStrategies:     analysis.AvailableStrategies,
		RecommendedStrategy:     analysis.RecommendedStrategy,
		EstimatedDuration:       ut.estimateDuration(analysis.TranslationComplexity, sourceSchema),
		EstimatedSuccessRate:    analysis.EstimatedSuccessRate,
		Recommendations:         analysis.Recommendations,
		Warnings:                ut.generateAnalysisWarnings(analysis, unsupportedFeatures),
		BestPractices:           ut.generateBestPractices(analysis),
	}

	return result, nil
}

// GetSupportedConversions returns supported conversion paths
func (ut *UnifiedTranslatorImpl) GetSupportedConversions() []ConversionPath {
	var paths []ConversionPath

	// Get all database types
	allDatabases := dbcapabilities.IDs()

	// Generate conversion paths for all database pairs
	for _, sourceDB := range allDatabases {
		for _, targetDB := range allDatabases {
			if sourceDB == targetDB {
				continue
			}

			analysis, err := ut.paradigmAnalyzer.AnalyzeParadigms(sourceDB, targetDB)
			if err != nil {
				continue
			}

			path := ConversionPath{
				SourceDatabase: sourceDB,
				TargetDatabase: targetDB,
				Complexity:     analysis.TranslationComplexity,
				Supported:      analysis.ConversionApproach != ConversionApproachImpossible,
				Description:    ut.generateConversionDescription(analysis),
			}

			paths = append(paths, path)
		}
	}

	return paths
}

// ValidateRequest validates a translation request
func (ut *UnifiedTranslatorImpl) ValidateRequest(request *TranslationRequest) []ValidationError {
	var errors []ValidationError

	// Check required fields
	if request.SourceDatabase == "" {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorCritical,
			Field:   "source_database",
			Message: "Source database type is required",
		})
	}

	if request.TargetDatabase == "" {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorCritical,
			Field:   "target_database",
			Message: "Target database type is required",
		})
	}

	if request.SourceSchema == nil {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorCritical,
			Field:   "source_schema",
			Message: "Source schema is required",
		})
	}

	if request.RequestID == "" {
		errors = append(errors, ValidationError{
			Type:       ValidationErrorWarning,
			Field:      "request_id",
			Message:    "Request ID is recommended for tracking",
			Suggestion: "Provide a unique request ID for better tracking",
		})
	}

	// Check if databases are the same
	if request.SourceDatabase == request.TargetDatabase {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorCritical,
			Field:   "target_database",
			Message: "Source and target databases cannot be the same",
		})
	}

	// Check if databases are supported (only if they are not empty)
	if request.SourceDatabase != "" {
		if _, exists := dbcapabilities.Get(request.SourceDatabase); !exists {
			errors = append(errors, ValidationError{
				Type:    ValidationErrorCritical,
				Field:   "source_database",
				Message: fmt.Sprintf("Unsupported source database: %s", request.SourceDatabase),
			})
		}
	}

	if request.TargetDatabase != "" {
		if _, exists := dbcapabilities.Get(request.TargetDatabase); !exists {
			errors = append(errors, ValidationError{
				Type:    ValidationErrorCritical,
				Field:   "target_database",
				Message: fmt.Sprintf("Unsupported target database: %s", request.TargetDatabase),
			})
		}
	}

	// Source schema validation (UnifiedModel is already validated by Go type system)
	// No additional validation needed since it's a typed struct

	return errors
}

// Helper methods

func (ut *UnifiedTranslatorImpl) performMultiStepTranslation(ctx *TranslationContext) error {
	// Multi-step translation through an intermediate database (typically relational)
	// This is a simplified implementation - could be enhanced with actual multi-step logic

	// For now, treat as cross-paradigm translation
	return ut.crossParadigmTranslator.Translate(ctx)
}

func (ut *UnifiedTranslatorImpl) analyzeUnsupportedFeatures(schema *unifiedmodel.UnifiedModel, sourceDB, targetDB dbcapabilities.DatabaseType) []UnsupportedFeature {
	var unsupported []UnsupportedFeature

	// Get database capabilities
	sourceCapability, _ := dbcapabilities.Get(sourceDB)
	targetCapability, _ := dbcapabilities.Get(targetDB)

	// Check paradigm compatibility
	if !ut.hasParadigmOverlap(sourceCapability.Paradigms, targetCapability.Paradigms) {
		// Different paradigms - some features will be unsupported
		unsupported = append(unsupported, UnsupportedFeature{
			FeatureType:  "paradigm_mismatch",
			Description:  fmt.Sprintf("Paradigm differences between %s and %s may require structural changes", sourceDB, targetDB),
			Alternatives: []string{"Use enrichment data for better conversion", "Consider intermediate conversion steps"},
		})
	}

	// Analyze specific object types (simplified)
	if len(schema.Functions) > 0 && targetDB == dbcapabilities.MongoDB {
		unsupported = append(unsupported, UnsupportedFeature{
			FeatureType:  "functions",
			ObjectType:   "function",
			Description:  "Database functions are not supported in MongoDB",
			Alternatives: []string{"Convert to application logic", "Use MongoDB aggregation pipeline"},
		})
	}

	if len(schema.Triggers) > 0 && targetDB == dbcapabilities.Redis {
		unsupported = append(unsupported, UnsupportedFeature{
			FeatureType:  "triggers",
			ObjectType:   "trigger",
			Description:  "Database triggers are not supported in Redis",
			Alternatives: []string{"Implement in application logic", "Use Redis modules"},
		})
	}

	return unsupported
}

func (ut *UnifiedTranslatorImpl) requiresUserInput(analysis *ParadigmAnalysisResult) bool {
	return analysis.TranslationComplexity == TranslationComplexityComplex ||
		analysis.ParadigmCompatibility == ParadigmCompatibilityPartial ||
		analysis.ParadigmCompatibility == ParadigmCompatibilityIncompatible
}

func (ut *UnifiedTranslatorImpl) estimateDuration(complexity TranslationComplexity, schema *unifiedmodel.UnifiedModel) string {
	objectCount := ut.countObjects(schema)

	switch complexity {
	case TranslationComplexityTrivial:
		return "seconds"
	case TranslationComplexitySimple:
		if objectCount > 100 {
			return "minutes"
		}
		return "seconds"
	case TranslationComplexityModerate:
		if objectCount > 50 {
			return "hours"
		}
		return "minutes"
	case TranslationComplexityComplex:
		return "hours to days"
	case TranslationComplexityImpossible:
		return "not feasible"
	default:
		return "unknown"
	}
}

func (ut *UnifiedTranslatorImpl) generateAnalysisWarnings(analysis *ParadigmAnalysisResult, unsupportedFeatures []UnsupportedFeature) []string {
	var warnings []string

	if analysis.TranslationComplexity == TranslationComplexityComplex {
		warnings = append(warnings, "This is a complex conversion that may require significant manual intervention")
	}

	if len(unsupportedFeatures) > 0 {
		warnings = append(warnings, fmt.Sprintf("%d features are not supported in the target database", len(unsupportedFeatures)))
	}

	if analysis.RequiresEnrichment {
		warnings = append(warnings, "Enrichment data is recommended for optimal conversion quality")
	}

	return warnings
}

func (ut *UnifiedTranslatorImpl) generateBestPractices(analysis *ParadigmAnalysisResult) []string {
	var practices []string

	practices = append(practices, "Always backup your data before performing schema conversion")
	practices = append(practices, "Test the conversion with a subset of data first")

	if analysis.RequiresEnrichment {
		practices = append(practices, "Provide enrichment data to improve conversion accuracy")
	}

	if analysis.ParadigmCompatibility != ParadigmCompatibilityIdentical {
		practices = append(practices, "Review the converted schema carefully for paradigm-specific optimizations")
	}

	return practices
}

func (ut *UnifiedTranslatorImpl) generateConversionDescription(analysis *ParadigmAnalysisResult) string {
	switch analysis.ConversionApproach {
	case ConversionApproachSameParadigm:
		return fmt.Sprintf("Direct conversion within %s paradigm", analysis.SourceParadigms[0])
	case ConversionApproachCrossParadigm:
		return fmt.Sprintf("Cross-paradigm conversion from %s to %s", analysis.SourceParadigms[0], analysis.TargetParadigms[0])
	case ConversionApproachMultiStep:
		return "Multi-step conversion through intermediate database"
	case ConversionApproachImpossible:
		return "Conversion not supported"
	default:
		return "Unknown conversion approach"
	}
}

func (ut *UnifiedTranslatorImpl) createErrorResult(request *TranslationRequest, err error) *TranslationResult {
	return &TranslationResult{
		TranslationReport: TranslationReport{
			RequestID:      request.RequestID,
			SourceDatabase: request.SourceDatabase,
			TargetDatabase: request.TargetDatabase,
			ProcessedAt:    time.Now(),
		},
		ProcessingTime:      0,
		Success:             false,
		ErrorMessage:        err.Error(),
		Warnings:            []TranslationWarning{},
		UnsupportedFeatures: []UnsupportedFeature{},
		UserDecisions:       []PendingUserDecision{},
	}
}

func (ut *UnifiedTranslatorImpl) createSuccessResult(request *TranslationRequest, ctx *TranslationContext) *TranslationResult {
	return &TranslationResult{
		TargetSchema:        ctx.TargetSchema,
		UnifiedSchema:       ctx.TargetSchema,
		TranslationReport:   ut.createTranslationReport(request, ctx),
		UserDecisions:       ctx.UserDecisions,
		Warnings:            ctx.Warnings,
		UnsupportedFeatures: []UnsupportedFeature{}, // TODO: Extract from context
		ProcessingTime:      ctx.Metrics.ProcessingTime,
		Success:             true,
	}
}

func (ut *UnifiedTranslatorImpl) createTranslationReport(request *TranslationRequest, ctx *TranslationContext) TranslationReport {
	var sourceParadigms, targetParadigms []dbcapabilities.DataParadigm
	var paradigmCompatibility ParadigmCompatibility
	var complexity TranslationComplexity
	var strategies []ConversionStrategy

	if ctx.Analysis != nil {
		sourceParadigms = ctx.Analysis.SourceParadigms
		targetParadigms = ctx.Analysis.TargetParadigms
		paradigmCompatibility = ctx.Analysis.ParadigmCompatibility
		complexity = ctx.Analysis.TranslationComplexity
		strategies = ctx.Analysis.AvailableStrategies
	}

	return TranslationReport{
		RequestID:             request.RequestID,
		SourceDatabase:        request.SourceDatabase,
		TargetDatabase:        request.TargetDatabase,
		SourceParadigms:       sourceParadigms,
		TargetParadigms:       targetParadigms,
		ParadigmCompatibility: paradigmCompatibility,
		TranslationComplexity: complexity,
		StrategiesUsed:        strategies,
		ObjectsProcessed:      ctx.Metrics.ObjectsProcessed,
		ObjectsConverted:      ctx.Metrics.ObjectsConverted,
		ObjectsSkipped:        ctx.Metrics.ObjectsSkipped,
		ObjectsDropped:        ctx.Metrics.ObjectsDropped,
		ObjectsSummary:        make(map[string]ObjectConversionStats),
		EnrichmentUsed:        ctx.Enrichment != nil,
		SampleDataUsed:        ctx.SampleData != nil,
		ProcessedAt:           ctx.Metrics.StartTime,
		ProcessingDuration:    ctx.Metrics.ProcessingTime,
	}
}

// Utility functions

func (ut *UnifiedTranslatorImpl) hasParadigmOverlap(source, target []dbcapabilities.DataParadigm) bool {
	sourceMap := make(map[dbcapabilities.DataParadigm]bool)
	for _, paradigm := range source {
		sourceMap[paradigm] = true
	}

	for _, paradigm := range target {
		if sourceMap[paradigm] {
			return true
		}
	}

	return false
}

func (ut *UnifiedTranslatorImpl) countObjects(schema *unifiedmodel.UnifiedModel) int {
	count := 0
	count += len(schema.Tables)
	count += len(schema.Collections)
	count += len(schema.Nodes)
	count += len(schema.Views)
	count += len(schema.MaterializedViews)
	count += len(schema.Functions)
	count += len(schema.Procedures)
	count += len(schema.Triggers)
	count += len(schema.Indexes)
	count += len(schema.Constraints)
	count += len(schema.Sequences)
	count += len(schema.Types)
	return count
}
