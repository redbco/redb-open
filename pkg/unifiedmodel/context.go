// Package unifiedmodel context provides comparison and conversion context types.
// This module handles the contextual information needed for intelligent schema
// comparison and conversion operations that leverage enrichment metadata.

package unifiedmodel

import (
	"time"
)

// ComparisonMode determines how enrichment is used in schema comparison
type ComparisonMode string

const (
	ComparisonModeStructural ComparisonMode = "structural" // Pure structural comparison (traditional)
	ComparisonModeGuided     ComparisonMode = "guided"     // Use enrichment to guide structural comparison
	ComparisonModeEnriched   ComparisonMode = "enriched"   // Include enrichment in comparison results
)

// EnrichmentCategory specifies which types of enrichment to include
type EnrichmentCategory string

const (
	EnrichmentCategoryPrivacy        EnrichmentCategory = "privacy"
	EnrichmentCategoryClassification EnrichmentCategory = "classification"
	EnrichmentCategoryPerformance    EnrichmentCategory = "performance"
	EnrichmentCategoryCompliance     EnrichmentCategory = "compliance"
	EnrichmentCategoryDataQuality    EnrichmentCategory = "data_quality"
	EnrichmentCategoryUsage          EnrichmentCategory = "usage"
	EnrichmentCategoryRelationships  EnrichmentCategory = "relationships"
)

// Change types for structural differences
type ChangeType string

const (
	ChangeTypeAdded    ChangeType = "added"
	ChangeTypeRemoved  ChangeType = "removed"
	ChangeTypeModified ChangeType = "modified"
	ChangeTypeRenamed  ChangeType = "renamed"
	ChangeTypeMoved    ChangeType = "moved"
)

// Severity levels for changes
type ChangeSeverity string

const (
	ChangeSeverityMinor    ChangeSeverity = "minor"    // Cosmetic changes
	ChangeSeverityMajor    ChangeSeverity = "major"    // Significant changes
	ChangeSeverityCritical ChangeSeverity = "critical" // Breaking changes
)

// Migration complexity levels
type MigrationComplexity string

const (
	MigrationComplexityNone   MigrationComplexity = "none"   // No migration needed
	MigrationComplexityLow    MigrationComplexity = "low"    // Simple changes
	MigrationComplexityMedium MigrationComplexity = "medium" // Moderate effort
	MigrationComplexityHigh   MigrationComplexity = "high"   // Complex migration
)

// Enrichment impact levels
type EnrichmentImpact string

const (
	EnrichmentImpactNone     EnrichmentImpact = "none"     // No impact
	EnrichmentImpactLow      EnrichmentImpact = "low"      // Minimal impact
	EnrichmentImpactMedium   EnrichmentImpact = "medium"   // Moderate impact
	EnrichmentImpactHigh     EnrichmentImpact = "high"     // Significant impact
	EnrichmentImpactCritical EnrichmentImpact = "critical" // Critical impact
)

// ComparisonOptions configures how schema comparison is performed
type ComparisonOptions struct {
	// Comparison behavior
	Mode                 ComparisonMode       `json:"mode"`
	IncludeEnrichment    bool                 `json:"include_enrichment"`
	EnrichmentCategories []EnrichmentCategory `json:"enrichment_categories,omitempty"`
	UseEnrichmentContext bool                 `json:"use_enrichment_context"`

	// Structural comparison settings
	IgnoreComments    bool `json:"ignore_comments"`
	IgnoreWhitespace  bool `json:"ignore_whitespace"`
	CaseSensitive     bool `json:"case_sensitive"`
	IgnoreObjectOrder bool `json:"ignore_object_order"`

	// Enrichment comparison settings
	PrivacyWeightThreshold  float64 `json:"privacy_weight_threshold"`  // 0.0-1.0
	ScoreToleranceThreshold float64 `json:"score_tolerance_threshold"` // 0.0-1.0

	// Object filtering
	IncludeObjectTypes []ObjectType `json:"include_object_types,omitempty"`
	ExcludeObjectTypes []ObjectType `json:"exclude_object_types,omitempty"`
	IncludeObjectNames []string     `json:"include_object_names,omitempty"`
	ExcludeObjectNames []string     `json:"exclude_object_names,omitempty"`
}

// DefaultComparisonOptions returns sensible defaults for schema comparison
func DefaultComparisonOptions() ComparisonOptions {
	return ComparisonOptions{
		Mode:                    ComparisonModeStructural,
		IncludeEnrichment:       false,
		UseEnrichmentContext:    false,
		IgnoreComments:          true,
		IgnoreWhitespace:        true,
		CaseSensitive:           false,
		IgnoreObjectOrder:       true,
		PrivacyWeightThreshold:  0.7,
		ScoreToleranceThreshold: 0.1,
	}
}

// EnrichedComparisonOptions returns options optimized for enrichment-aware comparison
func EnrichedComparisonOptions() ComparisonOptions {
	return ComparisonOptions{
		Mode:              ComparisonModeEnriched,
		IncludeEnrichment: true,
		EnrichmentCategories: []EnrichmentCategory{
			EnrichmentCategoryPrivacy,
			EnrichmentCategoryClassification,
			EnrichmentCategoryCompliance,
		},
		UseEnrichmentContext:    true,
		IgnoreComments:          true,
		IgnoreWhitespace:        true,
		CaseSensitive:           false,
		IgnoreObjectOrder:       true,
		PrivacyWeightThreshold:  0.5,
		ScoreToleranceThreshold: 0.15,
	}
}

// ComparisonResult represents the result of comparing two schemas
type ComparisonResult struct {
	// Basic comparison metadata
	SourceSchema   string         `json:"source_schema"`
	TargetSchema   string         `json:"target_schema"`
	ComparedAt     time.Time      `json:"compared_at"`
	ComparisonMode ComparisonMode `json:"comparison_mode"`

	// Structural differences
	HasStructuralChanges bool               `json:"has_structural_changes"`
	StructuralChanges    []StructuralChange `json:"structural_changes"`
	AddedObjects         []ObjectChange     `json:"added_objects"`
	RemovedObjects       []ObjectChange     `json:"removed_objects"`
	ModifiedObjects      []ObjectChange     `json:"modified_objects"`

	// Enrichment differences (only included if enrichment comparison enabled)
	HasEnrichmentChanges bool               `json:"has_enrichment_changes"`
	EnrichmentChanges    []EnrichmentChange `json:"enrichment_changes,omitempty"`

	// Analysis and recommendations
	OverallSimilarity         float64                    `json:"overall_similarity"`              // 0.0-1.0
	StructuralSimilarity      float64                    `json:"structural_similarity"`           // 0.0-1.0
	EnrichmentSimilarity      *float64                   `json:"enrichment_similarity,omitempty"` // 0.0-1.0
	CompatibilityScore        float64                    `json:"compatibility_score"`             // 0.0-1.0
	MigrationComplexity       MigrationComplexity        `json:"migration_complexity"`
	ConversionRecommendations []ConversionRecommendation `json:"conversion_recommendations"`

	// Warnings and issues
	Warnings           []string            `json:"warnings"`
	ComplianceIssues   []ComplianceIssue   `json:"compliance_issues,omitempty"`
	PerformanceImpacts []PerformanceImpact `json:"performance_impacts,omitempty"`
}

// StructuralChange represents a structural difference between schemas
type StructuralChange struct {
	ChangeType  ChangeType     `json:"change_type"`
	ObjectType  ObjectType     `json:"object_type"`
	ObjectPath  string         `json:"object_path"` // e.g., "tables.users.columns.email"
	SourceValue *string        `json:"source_value,omitempty"`
	TargetValue *string        `json:"target_value,omitempty"`
	Description string         `json:"description"`
	Severity    ChangeSeverity `json:"severity"`
	IsBreaking  bool           `json:"is_breaking"` // Breaking change for applications
}

// EnrichmentChange represents a difference in enrichment metadata
type EnrichmentChange struct {
	ChangeType         ChangeType         `json:"change_type"`
	EnrichmentCategory EnrichmentCategory `json:"enrichment_category"`
	ObjectPath         string             `json:"object_path"`
	SourceEnrichment   map[string]any     `json:"source_enrichment,omitempty"`
	TargetEnrichment   map[string]any     `json:"target_enrichment,omitempty"`
	Description        string             `json:"description"`
	Impact             EnrichmentImpact   `json:"impact"`
}

// ObjectChange represents an added, removed, or modified object
type ObjectChange struct {
	ObjectType       ObjectType         `json:"object_type"`
	ObjectName       string             `json:"object_name"`
	ObjectPath       string             `json:"object_path"`
	ChangeType       ChangeType         `json:"change_type"`
	Details          []StructuralChange `json:"details,omitempty"` // For modified objects
	HasEnrichment    bool               `json:"has_enrichment"`
	EnrichmentImpact *EnrichmentImpact  `json:"enrichment_impact,omitempty"`
}

// MigrationComplexity constants (continued from earlier definition)
const (
	MigrationComplexityTrivial  MigrationComplexity = "trivial"
	MigrationComplexitySimple   MigrationComplexity = "simple"
	MigrationComplexityModerate MigrationComplexity = "moderate"
	MigrationComplexityComplex  MigrationComplexity = "complex"
	MigrationComplexityExtreme  MigrationComplexity = "extreme"
)

// ConversionContext provides context and guidance for schema conversion
type ConversionContext struct {
	// Source and target information
	SourceDatabase     string                  `json:"source_database"`
	TargetDatabase     string                  `json:"target_database"`
	SourceEnrichment   *UnifiedModelEnrichment `json:"source_enrichment,omitempty"`
	TargetCapabilities map[string]any          `json:"target_capabilities"` // Database capabilities

	// Conversion preferences
	PreservationRules      []DataPreservationRule `json:"preservation_rules"`
	ConversionHints        []ConversionHint       `json:"conversion_hints"`
	OptimizationGoals      []OptimizationGoal     `json:"optimization_goals"`
	ComplianceRequirements []ComplianceFramework  `json:"compliance_requirements"`

	// Migration constraints
	MaxDowntime             *time.Duration           `json:"max_downtime,omitempty"`
	MaxDataLoss             DataLossLimit            `json:"max_data_loss"`
	BudgetConstraints       *BudgetConstraints       `json:"budget_constraints,omitempty"`
	PerformanceRequirements *PerformanceRequirements `json:"performance_requirements,omitempty"`

	// Context metadata
	MigrationReason  string            `json:"migration_reason,omitempty"`
	BusinessContext  map[string]string `json:"business_context,omitempty"`
	TechnicalContext map[string]string `json:"technical_context,omitempty"`
}

// ConversionHint provides specific guidance for converting schema objects
type ConversionHint struct {
	ObjectPath         string             `json:"object_path"` // e.g., "tables.users.columns.email"
	HintType           ConversionHintType `json:"hint_type"`
	RecommendedAction  string             `json:"recommended_action"`
	Reason             string             `json:"reason"`
	Priority           ConversionPriority `json:"priority"`
	TargetDatabase     string             `json:"target_database,omitempty"` // If specific to a database
	EstimatedImpact    string             `json:"estimated_impact"`
	AlternativeActions []string           `json:"alternative_actions,omitempty"`
	Context            map[string]string  `json:"context,omitempty"`
}

// ConversionHintType categorizes the type of conversion hint
type ConversionHintType string

const (
	ConversionHintTypeDataType    ConversionHintType = "data_type"
	ConversionHintTypeIndex       ConversionHintType = "index"
	ConversionHintTypeConstraint  ConversionHintType = "constraint"
	ConversionHintTypePartition   ConversionHintType = "partition"
	ConversionHintTypeSecurity    ConversionHintType = "security"
	ConversionHintTypePerformance ConversionHintType = "performance"
	ConversionHintTypeCompliance  ConversionHintType = "compliance"
)

// DataPreservationRule specifies how data should be preserved during migration
type DataPreservationRule struct {
	RuleType             PreservationRuleType  `json:"rule_type"`
	ObjectPattern        string                `json:"object_pattern"` // Regex pattern for object names
	PreservationLevel    PreservationLevel     `json:"preservation_level"`
	EncryptionRequired   bool                  `json:"encryption_required"`
	MaskingRequired      bool                  `json:"masking_required"`
	AuditingRequired     bool                  `json:"auditing_required"`
	BackupRequired       bool                  `json:"backup_required"`
	ValidateIntegrity    bool                  `json:"validate_integrity"`
	ComplianceFrameworks []ComplianceFramework `json:"compliance_frameworks,omitempty"`
}

// PreservationRuleType categorizes preservation rules
type PreservationRuleType string

const (
	PreservationRuleTypeGeneral   PreservationRuleType = "general"
	PreservationRuleTypePrivacy   PreservationRuleType = "privacy"
	PreservationRuleTypeFinancial PreservationRuleType = "financial"
	PreservationRuleTypeMedical   PreservationRuleType = "medical"
	PreservationRuleTypeAudit     PreservationRuleType = "audit"
)

// PreservationLevel indicates how strictly data must be preserved
type PreservationLevel string

const (
	PreservationLevelExact      PreservationLevel = "exact"       // Bit-for-bit preservation
	PreservationLevelLogical    PreservationLevel = "logical"     // Logical equivalent acceptable
	PreservationLevelSemantic   PreservationLevel = "semantic"    // Same meaning, format may change
	PreservationLevelBestEffort PreservationLevel = "best_effort" // Preserve what's possible
)

// OptimizationGoal specifies what to optimize for during conversion
type OptimizationGoal struct {
	Goal         OptimizationGoalType `json:"goal"`
	Priority     ConversionPriority   `json:"priority"`
	TargetMetric string               `json:"target_metric,omitempty"` // e.g., "query_latency_p95"
	TargetValue  *float64             `json:"target_value,omitempty"`  // Target value for metric
	Weight       float64              `json:"weight"`                  // Relative importance 0.0-1.0
}

// OptimizationGoalType defines what aspect to optimize
type OptimizationGoalType string

const (
	OptimizationGoalQueryPerformance OptimizationGoalType = "query_performance"
	OptimizationGoalStorageSize      OptimizationGoalType = "storage_size"
	OptimizationGoalMigrationSpeed   OptimizationGoalType = "migration_speed"
	OptimizationGoalCost             OptimizationGoalType = "cost"
	OptimizationGoalAvailability     OptimizationGoalType = "availability"
	OptimizationGoalSecurity         OptimizationGoalType = "security"
	OptimizationGoalCompliance       OptimizationGoalType = "compliance"
	OptimizationGoalMaintainability  OptimizationGoalType = "maintainability"
)

// DataLossLimit specifies acceptable data loss during migration
type DataLossLimit string

const (
	DataLossLimitNone       DataLossLimit = "none"       // Zero data loss required
	DataLossLimitMinimal    DataLossLimit = "minimal"    // <0.01% acceptable
	DataLossLimitLow        DataLossLimit = "low"        // <0.1% acceptable
	DataLossLimitModerate   DataLossLimit = "moderate"   // <1% acceptable
	DataLossLimitAcceptable DataLossLimit = "acceptable" // <5% acceptable
)

// BudgetConstraints specifies financial limits for migration
type BudgetConstraints struct {
	MaxTotalCost     *float64 `json:"max_total_cost,omitempty"`
	MaxMonthlyCost   *float64 `json:"max_monthly_cost,omitempty"`
	Currency         string   `json:"currency"`
	CostOptimization bool     `json:"cost_optimization"` // Optimize for cost over performance
}

// PerformanceRequirements specifies performance constraints
type PerformanceRequirements struct {
	MaxQueryLatencyP95   *time.Duration `json:"max_query_latency_p95,omitempty"`
	MinThroughputQPS     *float64       `json:"min_throughput_qps,omitempty"`
	MaxStorageGrowthRate *float64       `json:"max_storage_growth_rate,omitempty"` // GB/day
	RequiredAvailability *float64       `json:"required_availability,omitempty"`   // 0.99999 for 99.999%
}

// ConversionRecommendation provides specific recommendations for schema conversion
type ConversionRecommendation struct {
	ID                    string                `json:"id"`
	Type                  RecommendationType    `json:"type"`
	Priority              ConversionPriority    `json:"priority"`
	Title                 string                `json:"title"`
	Description           string                `json:"description"`
	ObjectPath            string                `json:"object_path,omitempty"`
	RecommendedAction     string                `json:"recommended_action"`
	Rationale             string                `json:"rationale"`
	EstimatedImpact       string                `json:"estimated_impact"`
	ImplementationSteps   []string              `json:"implementation_steps"`
	RequiredCapabilities  []string              `json:"required_capabilities,omitempty"`
	RiskFactors           []string              `json:"risk_factors,omitempty"`
	AlternativeApproaches []AlternativeApproach `json:"alternative_approaches,omitempty"`
}

// AlternativeApproach represents an alternative way to achieve the same goal
type AlternativeApproach struct {
	Name            string              `json:"name"`
	Description     string              `json:"description"`
	Pros            []string            `json:"pros"`
	Cons            []string            `json:"cons"`
	ComplexityLevel MigrationComplexity `json:"complexity_level"`
	EstimatedCost   string              `json:"estimated_cost,omitempty"`
}

// ComplianceIssue represents a compliance concern identified during comparison
type ComplianceIssue struct {
	Framework      ComplianceFramework `json:"framework"`
	Severity       RiskLevel           `json:"severity"`
	ObjectPath     string              `json:"object_path"`
	Issue          string              `json:"issue"`
	Requirement    string              `json:"requirement"`
	RecommendedFix string              `json:"recommended_fix"`
	MustFix        bool                `json:"must_fix"` // Required vs. recommended
}

// PerformanceImpact represents a potential performance impact from changes
type PerformanceImpact struct {
	ObjectPath         string   `json:"object_path"`
	ImpactType         string   `json:"impact_type"`         // positive, negative, neutral
	AffectedOperations []string `json:"affected_operations"` // read, write, update, delete
	EstimatedChange    string   `json:"estimated_change"`    // e.g., "+20% query time"
	Mitigation         string   `json:"mitigation,omitempty"`
}
