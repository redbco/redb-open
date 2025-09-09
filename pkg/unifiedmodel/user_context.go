package unifiedmodel

import (
	"fmt"
	"time"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// UserConversionContext represents user-provided context for schema conversions
type UserConversionContext struct {
	// Metadata
	ContextID   string    `json:"context_id"`
	UserID      string    `json:"user_id"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Description string    `json:"description,omitempty"`

	// Conversion scope
	SourceDatabase dbcapabilities.DatabaseType `json:"source_database"`
	TargetDatabase dbcapabilities.DatabaseType `json:"target_database"`

	// User preferences and decisions
	GlobalPreferences   ConversionPreferences        `json:"global_preferences"`
	ObjectMappings      map[string]UserObjectMapping `json:"object_mappings"`
	FieldMappings       map[string]UserFieldMapping  `json:"field_mappings"`
	CustomRules         []UserConversionRule         `json:"custom_rules"`
	IgnoredObjects      []string                     `json:"ignored_objects"`
	RequiredValidations []UserValidationRule         `json:"required_validations"`
}

// UserObjectMapping defines how a specific object should be converted
type UserObjectMapping struct {
	SourceObjectName string                 `json:"source_object_name"`
	SourceObjectType ObjectType             `json:"source_object_type"`
	TargetObjectName string                 `json:"target_object_name,omitempty"`
	TargetObjectType ObjectType             `json:"target_object_type"`
	ConversionType   ConversionType         `json:"conversion_type"`
	UserDecisions    []UserDecisionResponse `json:"user_decisions,omitempty"`
	CustomProperties map[string]interface{} `json:"custom_properties,omitempty"`
	Notes            string                 `json:"notes,omitempty"`
}

// UserFieldMapping defines field-level conversion rules
type UserFieldMapping struct {
	SourceObjectName string               `json:"source_object_name"`
	SourceFieldName  string               `json:"source_field_name"`
	TargetObjectName string               `json:"target_object_name,omitempty"`
	TargetFieldName  string               `json:"target_field_name,omitempty"`
	DataTypeMapping  UserDataTypeMapping  `json:"data_type_mapping"`
	TransformRules   []UserTransformRule  `json:"transform_rules,omitempty"`
	ValidationRules  []UserValidationRule `json:"validation_rules,omitempty"`
	DefaultValue     interface{}          `json:"default_value,omitempty"`
	IsRequired       bool                 `json:"is_required"`
}

// UserDataTypeMapping defines how data types should be converted
type UserDataTypeMapping struct {
	SourceType      string                 `json:"source_type"`
	TargetType      string                 `json:"target_type"`
	Parameters      map[string]interface{} `json:"parameters,omitempty"`
	LossyConversion bool                   `json:"lossy_conversion"`
	Notes           string                 `json:"notes,omitempty"`
}

// UserTransformRule defines data transformation rules
type UserTransformRule struct {
	RuleType    TransformRuleType      `json:"rule_type"`
	Expression  string                 `json:"expression,omitempty"`
	Parameters  map[string]interface{} `json:"parameters,omitempty"`
	Description string                 `json:"description,omitempty"`
}

type TransformRuleType string

const (
	TransformRuleTypeExpression TransformRuleType = "expression"
	TransformRuleTypeFunction   TransformRuleType = "function"
	TransformRuleTypeLookup     TransformRuleType = "lookup"
	TransformRuleTypeDefault    TransformRuleType = "default"
	TransformRuleTypeConcat     TransformRuleType = "concat"
	TransformRuleTypeSplit      TransformRuleType = "split"
	TransformRuleTypeFormat     TransformRuleType = "format"
)

// UserValidationRule defines validation requirements
type UserValidationRule struct {
	RuleType    ValidationRuleType     `json:"rule_type"`
	Expression  string                 `json:"expression,omitempty"`
	Parameters  map[string]interface{} `json:"parameters,omitempty"`
	ErrorLevel  ValidationErrorType    `json:"error_level"`
	Message     string                 `json:"message"`
	Description string                 `json:"description,omitempty"`
}

type ValidationRuleType string

const (
	ValidationRuleTypeRequired  ValidationRuleType = "required"
	ValidationRuleTypeRange     ValidationRuleType = "range"
	ValidationRuleTypePattern   ValidationRuleType = "pattern"
	ValidationRuleTypeUnique    ValidationRuleType = "unique"
	ValidationRuleTypeReference ValidationRuleType = "reference"
	ValidationRuleTypeCustom    ValidationRuleType = "custom"
)

// UserConversionRule defines custom conversion logic
type UserConversionRule struct {
	RuleID      string              `json:"rule_id"`
	Name        string              `json:"name"`
	Description string              `json:"description"`
	Conditions  []UserRuleCondition `json:"conditions"`
	Actions     []UserRuleAction    `json:"actions"`
	Priority    int                 `json:"priority"`
	Enabled     bool                `json:"enabled"`
}

// UserRuleCondition defines when a rule should apply
type UserRuleCondition struct {
	Field    string      `json:"field"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
}

// UserRuleAction defines what action to take when rule conditions are met
type UserRuleAction struct {
	ActionType string                 `json:"action_type"`
	Parameters map[string]interface{} `json:"parameters"`
}

// UserDecisionResponse represents a user's response to a conversion decision
type UserDecisionResponse struct {
	DecisionID       string                 `json:"decision_id"`
	SelectedOption   string                 `json:"selected_option"`
	CustomParameters map[string]interface{} `json:"custom_parameters,omitempty"`
	Reasoning        string                 `json:"reasoning,omitempty"`
	ApplyToSimilar   bool                   `json:"apply_to_similar"`
}

// ConversionContextRequest represents a request to apply user context to a conversion
type ConversionContextRequest struct {
	ConversionRequest ConversionRequest      `json:"conversion_request"`
	UserContext       *UserConversionContext `json:"user_context,omitempty"`
	ContextMode       ContextApplicationMode `json:"context_mode"`
}

type ContextApplicationMode string

const (
	ContextModeStrict     ContextApplicationMode = "strict"     // Fail if context can't be applied
	ContextModePermissive ContextApplicationMode = "permissive" // Skip invalid context, warn user
	ContextModeAdvisory   ContextApplicationMode = "advisory"   // Use context as hints only
)

// ApplyUserContext applies user-provided context to a conversion matrix
func ApplyUserContext(matrix *ConversionMatrix, context *UserConversionContext) (*ConversionMatrix, []ContextApplicationWarning, error) {
	if context == nil {
		return matrix, nil, nil
	}

	warnings := []ContextApplicationWarning{}
	enhancedMatrix := *matrix // Copy the matrix

	// Apply global preferences
	if context.GlobalPreferences.PreferredStrategy != "" {
		enhancedMatrix.ConversionStrategies = []ConversionStrategy{context.GlobalPreferences.PreferredStrategy}
	}

	// Apply object mappings
	for objectName, mapping := range context.ObjectMappings {
		if rule, exists := enhancedMatrix.ObjectConversions[mapping.SourceObjectType]; exists {
			// Enhance existing rule with user decisions
			for _, decision := range mapping.UserDecisions {
				rule = rule.WithUserDecision(UserDecision{
					DecisionType:  DecisionTypeMapping,
					Question:      "User-defined mapping for " + objectName,
					Options:       []string{decision.SelectedOption},
					DefaultOption: decision.SelectedOption,
				})
			}
			enhancedMatrix.ObjectConversions[mapping.SourceObjectType] = rule
		} else {
			warnings = append(warnings, ContextApplicationWarning{
				Type:    WarningTypeObjectNotFound,
				Message: fmt.Sprintf("Object %s not found in conversion matrix", objectName),
				Context: objectName,
			})
		}
	}

	// Apply custom rules
	for _, customRule := range context.CustomRules {
		if customRule.Enabled {
			// Apply custom rule logic (implementation depends on rule type)
			warnings = append(warnings, applyCustomRule(&enhancedMatrix, customRule)...)
		}
	}

	return &enhancedMatrix, warnings, nil
}

// ContextApplicationWarning represents warnings when applying user context
type ContextApplicationWarning struct {
	Type    ContextWarningType `json:"type"`
	Message string             `json:"message"`
	Context string             `json:"context,omitempty"`
}

type ContextWarningType string

const (
	WarningTypeObjectNotFound    ContextWarningType = "object_not_found"
	WarningTypeInvalidMapping    ContextWarningType = "invalid_mapping"
	WarningTypeConflictingRules  ContextWarningType = "conflicting_rules"
	WarningTypeUnsupportedAction ContextWarningType = "unsupported_action"
)

// Helper function to apply custom rules (placeholder implementation)
func applyCustomRule(matrix *ConversionMatrix, rule UserConversionRule) []ContextApplicationWarning {
	warnings := []ContextApplicationWarning{}

	// Implementation would depend on the specific rule type and conditions
	// This is where the microservice would implement business logic

	return warnings
}
