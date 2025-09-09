package unifiedmodel

import (
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// ConversionMatrix defines conversion rules between database pairs
type ConversionMatrix struct {
	SourceDatabase        dbcapabilities.DatabaseType         `json:"source_database"`
	TargetDatabase        dbcapabilities.DatabaseType         `json:"target_database"`
	ConversionComplexity  ConversionComplexity                `json:"conversion_complexity"`
	ParadigmCompatibility ParadigmCompatibility               `json:"paradigm_compatibility"`
	ObjectConversions     map[ObjectType]ObjectConversionRule `json:"object_conversions"`
	RequiresUserInput     bool                                `json:"requires_user_input"`
	RequiresEnrichment    bool                                `json:"requires_enrichment"`
	UnsupportedFeatures   []string                            `json:"unsupported_features"`
	ConversionStrategies  []ConversionStrategy                `json:"conversion_strategies"`
	EstimatedDuration     string                              `json:"estimated_duration,omitempty"`
	SuccessRate           float64                             `json:"success_rate,omitempty"`
}

// ConversionComplexity indicates the overall difficulty of conversion
type ConversionComplexity string

const (
	ConversionComplexityTrivial    ConversionComplexity = "trivial"    // Same paradigm, minor differences
	ConversionComplexitySimple     ConversionComplexity = "simple"     // Same paradigm, some mapping needed
	ConversionComplexityModerate   ConversionComplexity = "moderate"   // Cross-paradigm, automated possible
	ConversionComplexityComplex    ConversionComplexity = "complex"    // Requires user decisions
	ConversionComplexityImpossible ConversionComplexity = "impossible" // Fundamental incompatibility
)

// ParadigmCompatibility indicates how well paradigms align
type ParadigmCompatibility string

const (
	ParadigmCompatibilityIdentical    ParadigmCompatibility = "identical"    // Same paradigm
	ParadigmCompatibilityCompatible   ParadigmCompatibility = "compatible"   // Different but compatible
	ParadigmCompatibilityPartial      ParadigmCompatibility = "partial"      // Some compatibility
	ParadigmCompatibilityIncompatible ParadigmCompatibility = "incompatible" // Fundamentally different
)

// ObjectConversionRule defines how to convert a specific object type
type ObjectConversionRule struct {
	SourceObject    ObjectType      `json:"source_object"`
	TargetObjects   []ObjectType    `json:"target_objects"` // May map to multiple objects
	ConversionType  ConversionType  `json:"conversion_type"`
	RequiredContext []string        `json:"required_context,omitempty"`
	UserDecisions   []UserDecision  `json:"user_decisions,omitempty"`
	AutomationLevel AutomationLevel `json:"automation_level"`
	Notes           string          `json:"notes,omitempty"`
}

// ConversionType defines the nature of the conversion
type ConversionType string

const (
	ConversionTypeDirect    ConversionType = "direct"    // 1:1 mapping
	ConversionTypeSplit     ConversionType = "split"     // 1:N mapping
	ConversionTypeMerge     ConversionType = "merge"     // N:1 mapping
	ConversionTypeTransform ConversionType = "transform" // Structural change
	ConversionTypeEmulate   ConversionType = "emulate"   // Simulate with other objects
	ConversionTypeDrop      ConversionType = "drop"      // Cannot convert
)

// AutomationLevel indicates how much automation is possible
type AutomationLevel string

const (
	AutomationLevelFull       AutomationLevel = "full"       // Fully automated
	AutomationLevelPartial    AutomationLevel = "partial"    // Some automation possible
	AutomationLevelManual     AutomationLevel = "manual"     // Requires manual intervention
	AutomationLevelImpossible AutomationLevel = "impossible" // Cannot be automated
)

// UserDecision represents a decision that requires user input
type UserDecision struct {
	DecisionType   DecisionType `json:"decision_type"`
	Question       string       `json:"question"`
	Options        []string     `json:"options"`
	DefaultOption  string       `json:"default_option,omitempty"`
	Impact         string       `json:"impact"`
	Recommendation string       `json:"recommendation,omitempty"`
}

// DecisionType categorizes the type of user decision needed
type DecisionType string

const (
	DecisionTypeMapping     DecisionType = "mapping"     // How to map objects
	DecisionTypeStrategy    DecisionType = "strategy"    // Which conversion strategy
	DecisionTypeDataLoss    DecisionType = "data_loss"   // Accept potential data loss
	DecisionTypePerformance DecisionType = "performance" // Performance vs accuracy tradeoff
	DecisionTypeStructural  DecisionType = "structural"  // Structural changes
)

// ConversionStrategy defines high-level conversion approaches
type ConversionStrategy string

const (
	StrategyDirect          ConversionStrategy = "direct"          // Direct object mapping
	StrategyNormalization   ConversionStrategy = "normalization"   // Normalize then convert
	StrategyDenormalization ConversionStrategy = "denormalization" // Denormalize then convert
	StrategyDecomposition   ConversionStrategy = "decomposition"   // Break into components
	StrategyAggregation     ConversionStrategy = "aggregation"     // Combine components
	StrategyHybrid          ConversionStrategy = "hybrid"          // Multiple strategies
)

// ConversionPair represents a source-target database pair
type ConversionPair struct {
	Source dbcapabilities.DatabaseType
	Target dbcapabilities.DatabaseType
}

// ConversionMatrixRegistry is deprecated - use dynamic generation instead
// Kept for backward compatibility during transition
var ConversionMatrixRegistry = map[ConversionPair]ConversionMatrix{}

// GetConversionMatrix is deprecated - use ConversionUtils.GenerateConversionMatrix instead
func GetConversionMatrix(source, target dbcapabilities.DatabaseType) (ConversionMatrix, bool) {
	// Always return false to force dynamic generation
	return ConversionMatrix{}, false
}

// GetConversionComplexity is deprecated - use dynamic generation instead
func GetConversionComplexity(source, target dbcapabilities.DatabaseType) ConversionComplexity {
	// Use dynamic generation through ConversionUtils
	utils := NewConversionUtils()
	matrix, err := utils.GenerateConversionMatrix(source, target)
	if err != nil {
		return ConversionComplexityImpossible
	}
	return matrix.ConversionComplexity
}

// GetObjectConversionRule is deprecated - use dynamic generation instead
func GetObjectConversionRule(source, target dbcapabilities.DatabaseType, objType ObjectType) (ObjectConversionRule, bool) {
	// Use dynamic generation through ConversionUtils
	utils := NewConversionUtils()
	matrix, err := utils.GenerateConversionMatrix(source, target)
	if err != nil {
		return ObjectConversionRule{}, false
	}

	rule, exists := matrix.ObjectConversions[objType]
	return rule, exists
}

// IsConversionPossible checks if conversion between databases is possible using dynamic generation
func IsConversionPossible(source, target dbcapabilities.DatabaseType) bool {
	complexity := GetConversionComplexity(source, target)
	return complexity != ConversionComplexityImpossible
}

// RequiresUserInteraction checks if conversion requires user decisions using dynamic generation
func RequiresUserInteraction(source, target dbcapabilities.DatabaseType) bool {
	utils := NewConversionUtils()
	matrix, err := utils.GenerateConversionMatrix(source, target)
	if err != nil {
		return true // Assume user interaction needed if generation fails
	}
	return matrix.RequiresUserInput
}

// RequiresEnrichmentData checks if conversion requires enrichment data using dynamic generation
func RequiresEnrichmentData(source, target dbcapabilities.DatabaseType) bool {
	utils := NewConversionUtils()
	matrix, err := utils.GenerateConversionMatrix(source, target)
	if err != nil {
		return true // Assume enrichment needed if generation fails
	}
	return matrix.RequiresEnrichment
}

// GetUnsupportedFeatures returns features that cannot be converted using dynamic generation
func GetUnsupportedFeatures(source, target dbcapabilities.DatabaseType) []string {
	utils := NewConversionUtils()
	matrix, err := utils.GenerateConversionMatrix(source, target)
	if err != nil {
		return []string{"conversion not supported"}
	}
	return matrix.UnsupportedFeatures
}

// GetConversionStrategies returns available conversion strategies using dynamic generation
func GetConversionStrategies(source, target dbcapabilities.DatabaseType) []ConversionStrategy {
	utils := NewConversionUtils()
	matrix, err := utils.GenerateConversionMatrix(source, target)
	if err != nil {
		return []ConversionStrategy{StrategyDirect} // Default strategy
	}
	return matrix.ConversionStrategies
}

// Helper functions for creating conversion rules

// DirectConversion creates a direct 1:1 object conversion rule
func DirectConversion(sourceObj, targetObj ObjectType) ObjectConversionRule {
	return ObjectConversionRule{
		SourceObject:    sourceObj,
		TargetObjects:   []ObjectType{targetObj},
		ConversionType:  ConversionTypeDirect,
		AutomationLevel: AutomationLevelFull,
	}
}

// SplitConversion creates a 1:N object conversion rule
func SplitConversion(sourceObj ObjectType, targetObjs []ObjectType, userDecisions []UserDecision) ObjectConversionRule {
	automationLevel := AutomationLevelFull
	if len(userDecisions) > 0 {
		automationLevel = AutomationLevelPartial
	}

	return ObjectConversionRule{
		SourceObject:    sourceObj,
		TargetObjects:   targetObjs,
		ConversionType:  ConversionTypeSplit,
		UserDecisions:   userDecisions,
		AutomationLevel: automationLevel,
	}
}

// EmulatedConversion creates a rule for emulating unsupported objects
func EmulatedConversion(sourceObj ObjectType, emulationObjs []ObjectType, notes string) ObjectConversionRule {
	return ObjectConversionRule{
		SourceObject:    sourceObj,
		TargetObjects:   emulationObjs,
		ConversionType:  ConversionTypeEmulate,
		AutomationLevel: AutomationLevelPartial,
		Notes:           notes,
	}
}

// DroppedConversion creates a rule for objects that cannot be converted
func DroppedConversion(sourceObj ObjectType, reason string) ObjectConversionRule {
	return ObjectConversionRule{
		SourceObject:    sourceObj,
		TargetObjects:   []ObjectType{},
		ConversionType:  ConversionTypeDrop,
		AutomationLevel: AutomationLevelImpossible,
		Notes:           reason,
	}
}

// WithUserDecision adds a user decision to a conversion rule
func (rule ObjectConversionRule) WithUserDecision(decision UserDecision) ObjectConversionRule {
	rule.UserDecisions = append(rule.UserDecisions, decision)
	if rule.AutomationLevel == AutomationLevelFull {
		rule.AutomationLevel = AutomationLevelPartial
	}
	return rule
}

// WithRequiredContext adds required context to a conversion rule
func (rule ObjectConversionRule) WithRequiredContext(context []string) ObjectConversionRule {
	rule.RequiredContext = context
	return rule
}
