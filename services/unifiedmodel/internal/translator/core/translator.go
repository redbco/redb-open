package core

import (
	"context"
	"time"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// UnifiedTranslator is the main interface for the new paradigm-aware translator
type UnifiedTranslator interface {
	// Translate performs schema translation between databases
	Translate(ctx context.Context, request *TranslationRequest) (*TranslationResult, error)

	// AnalyzeTranslation analyzes translation feasibility without performing it
	AnalyzeTranslation(ctx context.Context, request *TranslationRequest) (*TranslationAnalysis, error)

	// GetSupportedConversions returns supported conversion paths
	GetSupportedConversions() []ConversionPath

	// ValidateRequest validates a translation request
	ValidateRequest(request *TranslationRequest) []ValidationError
}

// TranslationRequest defines parameters for schema translation
type TranslationRequest struct {
	// Source configuration
	SourceDatabase dbcapabilities.DatabaseType `json:"source_database"`
	SourceSchema   *unifiedmodel.UnifiedModel  `json:"source_schema"`

	// Target configuration
	TargetDatabase dbcapabilities.DatabaseType `json:"target_database"`
	TargetFormat   string                      `json:"target_format,omitempty"` // "unified", "native", "sql"

	// Enhancement data
	Enrichment *unifiedmodel.UnifiedModelEnrichment `json:"enrichment,omitempty"`
	SampleData *unifiedmodel.UnifiedModelSampleData `json:"sample_data,omitempty"`

	// Translation preferences
	Preferences TranslationPreferences `json:"preferences"`

	// Request metadata
	RequestID   string    `json:"request_id"`
	RequestedBy string    `json:"requested_by,omitempty"`
	RequestedAt time.Time `json:"requested_at"`
}

// TranslationPreferences defines user preferences for translation
type TranslationPreferences struct {
	// Conversion strategy preferences
	PreferredStrategy      ConversionStrategy `json:"preferred_strategy,omitempty"`
	AcceptDataLoss         bool               `json:"accept_data_loss"`
	OptimizeForPerformance bool               `json:"optimize_for_performance"`
	OptimizeForStorage     bool               `json:"optimize_for_storage"`
	PreserveRelationships  bool               `json:"preserve_relationships"`
	IncludeMetadata        bool               `json:"include_metadata"`

	// Interactive mode settings
	InteractiveMode   bool              `json:"interactive_mode"`
	AutoApproveSimple bool              `json:"auto_approve_simple"`
	CustomMappings    map[string]string `json:"custom_mappings,omitempty"`
	ExcludeObjects    []string          `json:"exclude_objects,omitempty"`

	// Output preferences
	GenerateComments     bool `json:"generate_comments"`
	IncludeOriginalNames bool `json:"include_original_names"`
	UseQualifiedNames    bool `json:"use_qualified_names"`
	PreserveCaseStyle    bool `json:"preserve_case_style"`
}

// TranslationResult contains the results of schema translation
type TranslationResult struct {
	// Result data
	TargetSchema  interface{}                `json:"target_schema,omitempty"`
	UnifiedSchema *unifiedmodel.UnifiedModel `json:"unified_schema,omitempty"`

	// Translation metadata
	TranslationReport   TranslationReport     `json:"translation_report"`
	UserDecisions       []PendingUserDecision `json:"user_decisions,omitempty"`
	Warnings            []TranslationWarning  `json:"warnings"`
	UnsupportedFeatures []UnsupportedFeature  `json:"unsupported_features"`

	// Processing information
	ProcessingTime time.Duration `json:"processing_time"`
	Success        bool          `json:"success"`
	ErrorMessage   string        `json:"error_message,omitempty"`
}

// TranslationReport provides detailed information about the translation
type TranslationReport struct {
	RequestID      string                      `json:"request_id"`
	SourceDatabase dbcapabilities.DatabaseType `json:"source_database"`
	TargetDatabase dbcapabilities.DatabaseType `json:"target_database"`

	// Paradigm information
	SourceParadigms       []dbcapabilities.DataParadigm `json:"source_paradigms"`
	TargetParadigms       []dbcapabilities.DataParadigm `json:"target_paradigms"`
	ParadigmCompatibility ParadigmCompatibility         `json:"paradigm_compatibility"`

	// Translation complexity and strategy
	TranslationComplexity TranslationComplexity `json:"translation_complexity"`
	StrategiesUsed        []ConversionStrategy  `json:"strategies_used"`

	// Object processing statistics
	ObjectsProcessed int                              `json:"objects_processed"`
	ObjectsConverted int                              `json:"objects_converted"`
	ObjectsSkipped   int                              `json:"objects_skipped"`
	ObjectsDropped   int                              `json:"objects_dropped"`
	ObjectsSummary   map[string]ObjectConversionStats `json:"objects_summary"`

	// Enhancement usage
	EnrichmentUsed bool `json:"enrichment_used"`
	SampleDataUsed bool `json:"sample_data_used"`

	// Processing metadata
	ProcessedAt        time.Time     `json:"processed_at"`
	ProcessingDuration time.Duration `json:"processing_duration"`
}

// TranslationAnalysis provides analysis of translation feasibility
type TranslationAnalysis struct {
	// Basic compatibility
	ConversionSupported   bool                  `json:"conversion_supported"`
	ParadigmCompatibility ParadigmCompatibility `json:"paradigm_compatibility"`
	TranslationComplexity TranslationComplexity `json:"translation_complexity"`

	// Requirements
	RequiresUserInput       bool             `json:"requires_user_input"`
	RequiresEnrichment      bool             `json:"requires_enrichment"`
	RequiredEnrichmentTypes []EnrichmentType `json:"required_enrichment_types,omitempty"`

	// Feature analysis
	UnsupportedFeatures []UnsupportedFeature `json:"unsupported_features"`
	AvailableStrategies []ConversionStrategy `json:"available_strategies"`
	RecommendedStrategy ConversionStrategy   `json:"recommended_strategy"`

	// Estimates
	EstimatedDuration    string  `json:"estimated_duration"`
	EstimatedSuccessRate float64 `json:"estimated_success_rate"`

	// Guidance
	Recommendations []string `json:"recommendations"`
	Warnings        []string `json:"warnings"`
	BestPractices   []string `json:"best_practices,omitempty"`
}

// Supporting types

type ConversionPath struct {
	SourceDatabase dbcapabilities.DatabaseType `json:"source_database"`
	TargetDatabase dbcapabilities.DatabaseType `json:"target_database"`
	Complexity     TranslationComplexity       `json:"complexity"`
	Supported      bool                        `json:"supported"`
	Description    string                      `json:"description"`
}

type ValidationError struct {
	Type       ValidationErrorType `json:"type"`
	Field      string              `json:"field"`
	Message    string              `json:"message"`
	Suggestion string              `json:"suggestion,omitempty"`
}

type ValidationErrorType string

const (
	ValidationErrorCritical ValidationErrorType = "critical"
	ValidationErrorWarning  ValidationErrorType = "warning"
	ValidationErrorInfo     ValidationErrorType = "info"
)

type PendingUserDecision struct {
	DecisionID   string       `json:"decision_id"`
	ObjectType   string       `json:"object_type"`
	ObjectName   string       `json:"object_name"`
	DecisionType DecisionType `json:"decision_type"`
	Context      string       `json:"context"`
	Options      []string     `json:"options"`
	Recommended  string       `json:"recommended,omitempty"`
}

type DecisionType string

const (
	DecisionTypeObjectMapping DecisionType = "object_mapping"
	DecisionTypeDataType      DecisionType = "data_type"
	DecisionTypeRelationship  DecisionType = "relationship"
	DecisionTypeStructure     DecisionType = "structure"
	DecisionTypePerformance   DecisionType = "performance"
)

type TranslationWarning struct {
	WarningType WarningType `json:"warning_type"`
	ObjectType  string      `json:"object_type"`
	ObjectName  string      `json:"object_name,omitempty"`
	Message     string      `json:"message"`
	Severity    string      `json:"severity"`
	Suggestion  string      `json:"suggestion,omitempty"`
}

type WarningType string

const (
	WarningTypeDataLoss      WarningType = "data_loss"
	WarningTypeFeatureLoss   WarningType = "feature_loss"
	WarningTypePerformance   WarningType = "performance"
	WarningTypeCompatibility WarningType = "compatibility"
	WarningTypeSecurity      WarningType = "security"
)

type UnsupportedFeature struct {
	FeatureType   string   `json:"feature_type"`
	ObjectType    string   `json:"object_type"`
	ObjectName    string   `json:"object_name,omitempty"`
	Description   string   `json:"description"`
	Alternatives  []string `json:"alternatives,omitempty"`
	WorkaroundURL string   `json:"workaround_url,omitempty"`
}

type ObjectConversionStats struct {
	SourceCount    int     `json:"source_count"`
	ConvertedCount int     `json:"converted_count"`
	SkippedCount   int     `json:"skipped_count"`
	DroppedCount   int     `json:"dropped_count"`
	SuccessRate    float64 `json:"success_rate"`
}

// Enums and constants

type ParadigmCompatibility string

const (
	ParadigmCompatibilityIdentical    ParadigmCompatibility = "identical"
	ParadigmCompatibilityCompatible   ParadigmCompatibility = "compatible"
	ParadigmCompatibilityPartial      ParadigmCompatibility = "partial"
	ParadigmCompatibilityIncompatible ParadigmCompatibility = "incompatible"
)

type TranslationComplexity string

const (
	TranslationComplexityTrivial    TranslationComplexity = "trivial"
	TranslationComplexitySimple     TranslationComplexity = "simple"
	TranslationComplexityModerate   TranslationComplexity = "moderate"
	TranslationComplexityComplex    TranslationComplexity = "complex"
	TranslationComplexityImpossible TranslationComplexity = "impossible"
)

type ConversionStrategy string

const (
	ConversionStrategyDirect          ConversionStrategy = "direct"
	ConversionStrategyTransform       ConversionStrategy = "transform"
	ConversionStrategyNormalization   ConversionStrategy = "normalization"
	ConversionStrategyDenormalization ConversionStrategy = "denormalization"
	ConversionStrategyDecomposition   ConversionStrategy = "decomposition"
	ConversionStrategyAggregation     ConversionStrategy = "aggregation"
	ConversionStrategyHybrid          ConversionStrategy = "hybrid"
)

type EnrichmentType string

const (
	EnrichmentTypeDataClassification EnrichmentType = "data_classification"
	EnrichmentTypeRelationships      EnrichmentType = "relationships"
	EnrichmentTypeAccessPatterns     EnrichmentType = "access_patterns"
	EnrichmentTypeBusinessRules      EnrichmentType = "business_rules"
	EnrichmentTypePerformanceHints   EnrichmentType = "performance_hints"
	EnrichmentTypeDataFlow           EnrichmentType = "data_flow"
)
