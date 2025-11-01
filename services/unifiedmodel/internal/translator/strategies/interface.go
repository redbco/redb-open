package strategies

import (
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/core"
)

// ParadigmConversionStrategy defines the interface for paradigm-pair conversion strategies
type ParadigmConversionStrategy interface {
	// Identity methods
	Name() string
	SourceParadigm() dbcapabilities.DataParadigm
	TargetParadigm() dbcapabilities.DataParadigm

	// Conversion method - enrichmentData is provided as an interface{} to avoid import cycles
	// Strategies should type-assert to the specific enrichment type they need
	Convert(ctx *core.TranslationContext, enrichmentData interface{}) (*ConversionResult, error)

	// Requirement checks
	RequiresSampleData() bool
	RequiresEnrichment() bool
	SupportedSourceTypes() []unifiedmodel.ObjectType

	// Decision support
	GetUserDecisions(ctx *core.TranslationContext) []core.PendingUserDecision
}

// ConversionResult holds the results of a paradigm conversion
type ConversionResult struct {
	TargetSchema *unifiedmodel.UnifiedModel
	Mappings     []GeneratedMapping
	Warnings     []core.TranslationWarning
}

// GeneratedMapping represents a mapping created during translation
type GeneratedMapping struct {
	SourceIdentifier string                 `json:"source_identifier"`
	TargetIdentifier string                 `json:"target_identifier"`
	MappingType      string                 `json:"mapping_type"` // "direct", "hybrid_property", "transformation", "junction", etc.
	MappingRules     []GeneratedMappingRule `json:"mapping_rules"`
	Metadata         map[string]interface{} `json:"metadata,omitempty"`
}

// GeneratedMappingRule represents a single field-to-field mapping rule
type GeneratedMappingRule struct {
	RuleID                string                 `json:"rule_id"`
	SourceField           string                 `json:"source_field"`
	TargetField           string                 `json:"target_field"`
	SourceType            string                 `json:"source_type"`
	TargetType            string                 `json:"target_type"`
	Cardinality           string                 `json:"cardinality"` // "one-to-one", "one-to-many", "many-to-one"
	TransformationID      string                 `json:"transformation_id,omitempty"`
	TransformationName    string                 `json:"transformation_name,omitempty"`
	TransformationOptions map[string]interface{} `json:"transformation_options,omitempty"`
	IsRequired            bool                   `json:"is_required"`
	DefaultValue          interface{}            `json:"default_value,omitempty"`
	Metadata              map[string]interface{} `json:"metadata,omitempty"`
}

// PropertyMappingStrategy defines how properties should be mapped to columns
type PropertyMappingStrategy string

const (
	// PropertyMappingAll maps all properties to individual columns
	PropertyMappingAll PropertyMappingStrategy = "all_to_columns"
	// PropertyMappingCore maps main/common properties to columns, rest to JSONB
	PropertyMappingCore PropertyMappingStrategy = "core_to_columns"
	// PropertyMappingMinimal maps only ID to column, rest to JSONB
	PropertyMappingMinimal PropertyMappingStrategy = "minimal_to_columns"
	// PropertyMappingCustom uses user-defined property split
	PropertyMappingCustom PropertyMappingStrategy = "custom"
)

// RelationshipMappingStrategy defines how relationships should be mapped
type RelationshipMappingStrategy string

const (
	// RelationshipMappingForeignKey uses foreign key columns
	RelationshipMappingForeignKey RelationshipMappingStrategy = "foreign_key"
	// RelationshipMappingJunction uses junction/bridge tables
	RelationshipMappingJunction RelationshipMappingStrategy = "junction_table"
	// RelationshipMappingHybrid uses foreign keys for simple, junction for complex
	RelationshipMappingHybrid RelationshipMappingStrategy = "hybrid"
)

// StrategyConfig holds configuration for a conversion strategy
type StrategyConfig struct {
	PropertyMappingStrategy     PropertyMappingStrategy     `json:"property_mapping_strategy"`
	RelationshipMappingStrategy RelationshipMappingStrategy `json:"relationship_mapping_strategy"`
	CoreProperties              []string                    `json:"core_properties,omitempty"` // For custom property mapping
	PreservePropertyNames       bool                        `json:"preserve_property_names"`
	GenerateMappings            bool                        `json:"generate_mappings"`
	UseSampleData               bool                        `json:"use_sample_data"`
	CustomOptions               map[string]interface{}      `json:"custom_options,omitempty"`
}

// DefaultStrategyConfig returns sensible defaults for strategy configuration
func DefaultStrategyConfig() StrategyConfig {
	return StrategyConfig{
		PropertyMappingStrategy:     PropertyMappingCore,
		RelationshipMappingStrategy: RelationshipMappingHybrid,
		PreservePropertyNames:       true,
		GenerateMappings:            true,
		UseSampleData:               true,
		CustomOptions:               make(map[string]interface{}),
	}
}
