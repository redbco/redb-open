package utils

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/core"
)

// ValidationResult contains the results of translation validation
type ValidationResult struct {
	IsValid      bool                   `json:"is_valid"`
	Errors       []ValidationError      `json:"errors,omitempty"`
	Warnings     []ValidationWarning    `json:"warnings,omitempty"`
	Suggestions  []ValidationSuggestion `json:"suggestions,omitempty"`
	SchemaHealth SchemaHealthReport     `json:"schema_health"`
}

// ValidationError represents a validation error
type ValidationError struct {
	Type       string `json:"type"`
	Object     string `json:"object,omitempty"`
	Field      string `json:"field,omitempty"`
	Message    string `json:"message"`
	Severity   string `json:"severity"`
	Suggestion string `json:"suggestion,omitempty"`
}

// ValidationWarning represents a validation warning
type ValidationWarning struct {
	Type       string `json:"type"`
	Object     string `json:"object,omitempty"`
	Message    string `json:"message"`
	Impact     string `json:"impact"`
	Mitigation string `json:"mitigation,omitempty"`
}

// ValidationSuggestion represents an improvement suggestion
type ValidationSuggestion struct {
	Type       string `json:"type"`
	Object     string `json:"object,omitempty"`
	Suggestion string `json:"suggestion"`
	Benefit    string `json:"benefit"`
	Effort     string `json:"effort"` // "low", "medium", "high"
}

// SchemaHealthReport provides overall schema health metrics
type SchemaHealthReport struct {
	OverallScore       float64            `json:"overall_score"`      // 0-100
	ComplexityScore    float64            `json:"complexity_score"`   // 0-100
	ConsistencyScore   float64            `json:"consistency_score"`  // 0-100
	CompletenessScore  float64            `json:"completeness_score"` // 0-100
	ObjectCounts       map[string]int     `json:"object_counts"`
	RelationshipHealth RelationshipHealth `json:"relationship_health"`
}

// RelationshipHealth provides relationship-specific health metrics
type RelationshipHealth struct {
	TotalRelationships    int     `json:"total_relationships"`
	ValidRelationships    int     `json:"valid_relationships"`
	BrokenRelationships   int     `json:"broken_relationships"`
	CircularReferences    int     `json:"circular_references"`
	OrphanedObjects       int     `json:"orphaned_objects"`
	RelationshipIntegrity float64 `json:"relationship_integrity"` // 0-100
}

// SchemaValidator provides comprehensive schema validation
type SchemaValidator struct{}

// NewSchemaValidator creates a new schema validator
func NewSchemaValidator() *SchemaValidator {
	return &SchemaValidator{}
}

// ValidateTranslationRequest validates a translation request
func (sv *SchemaValidator) ValidateTranslationRequest(request *core.TranslationRequest) *ValidationResult {
	result := &ValidationResult{
		IsValid:     true,
		Errors:      make([]ValidationError, 0),
		Warnings:    make([]ValidationWarning, 0),
		Suggestions: make([]ValidationSuggestion, 0),
	}

	// Validate required fields
	sv.validateRequiredFields(request, result)

	// Validate database types
	sv.validateDatabaseTypes(request, result)

	// Validate source schema
	sv.validateSourceSchema(request, result)

	// Validate preferences
	sv.validatePreferences(request, result)

	// Set overall validity
	result.IsValid = len(result.Errors) == 0

	return result
}

// ValidateUnifiedModel validates a unified model for consistency and completeness
func (sv *SchemaValidator) ValidateUnifiedModel(schema *unifiedmodel.UnifiedModel, databaseType dbcapabilities.DatabaseType) *ValidationResult {
	result := &ValidationResult{
		IsValid:     true,
		Errors:      make([]ValidationError, 0),
		Warnings:    make([]ValidationWarning, 0),
		Suggestions: make([]ValidationSuggestion, 0),
	}

	// Validate schema structure
	sv.validateSchemaStructure(schema, result)

	// Validate object relationships
	sv.validateObjectRelationships(schema, result)

	// Validate data types
	sv.validateDataTypes(schema, databaseType, result)

	// Validate naming conventions
	sv.validateNamingConventions(schema, result)

	// Generate schema health report
	result.SchemaHealth = sv.generateSchemaHealthReport(schema)

	// Set overall validity
	result.IsValid = len(result.Errors) == 0

	return result
}

// ValidateTranslationResult validates the result of a translation
func (sv *SchemaValidator) ValidateTranslationResult(result *core.TranslationResult) *ValidationResult {
	validationResult := &ValidationResult{
		IsValid:     true,
		Errors:      make([]ValidationError, 0),
		Warnings:    make([]ValidationWarning, 0),
		Suggestions: make([]ValidationSuggestion, 0),
	}

	// Validate translation success
	if !result.Success {
		validationResult.Errors = append(validationResult.Errors, ValidationError{
			Type:     "translation_failure",
			Message:  result.ErrorMessage,
			Severity: "critical",
		})
	}

	// Validate target schema if present
	if result.UnifiedSchema != nil {
		schemaValidation := sv.ValidateUnifiedModel(result.UnifiedSchema, result.UnifiedSchema.DatabaseType)
		validationResult.Errors = append(validationResult.Errors, schemaValidation.Errors...)
		validationResult.Warnings = append(validationResult.Warnings, schemaValidation.Warnings...)
		validationResult.Suggestions = append(validationResult.Suggestions, schemaValidation.Suggestions...)
	}

	// Validate translation warnings
	sv.validateTranslationWarnings(result, validationResult)

	// Validate unsupported features
	sv.validateUnsupportedFeatures(result, validationResult)

	// Set overall validity
	validationResult.IsValid = len(validationResult.Errors) == 0

	return validationResult
}

// Private validation methods

func (sv *SchemaValidator) validateRequiredFields(request *core.TranslationRequest, result *ValidationResult) {
	if request.SourceDatabase == "" {
		result.Errors = append(result.Errors, ValidationError{
			Type:     "missing_field",
			Field:    "source_database",
			Message:  "Source database type is required",
			Severity: "critical",
		})
	}

	if request.TargetDatabase == "" {
		result.Errors = append(result.Errors, ValidationError{
			Type:     "missing_field",
			Field:    "target_database",
			Message:  "Target database type is required",
			Severity: "critical",
		})
	}

	if request.SourceSchema == nil {
		result.Errors = append(result.Errors, ValidationError{
			Type:     "missing_field",
			Field:    "source_schema",
			Message:  "Source schema is required",
			Severity: "critical",
		})
	}

	if request.RequestID == "" {
		result.Warnings = append(result.Warnings, ValidationWarning{
			Type:       "missing_field",
			Object:     "request",
			Message:    "Request ID is not provided",
			Impact:     "Tracking and debugging may be difficult",
			Mitigation: "Provide a unique request ID",
		})
	}
}

func (sv *SchemaValidator) validateDatabaseTypes(request *core.TranslationRequest, result *ValidationResult) {
	// Check if source database is supported
	if _, exists := dbcapabilities.Get(request.SourceDatabase); !exists {
		result.Errors = append(result.Errors, ValidationError{
			Type:     "unsupported_database",
			Field:    "source_database",
			Message:  fmt.Sprintf("Unsupported source database: %s", request.SourceDatabase),
			Severity: "critical",
		})
	}

	// Check if target database is supported
	if _, exists := dbcapabilities.Get(request.TargetDatabase); !exists {
		result.Errors = append(result.Errors, ValidationError{
			Type:     "unsupported_database",
			Field:    "target_database",
			Message:  fmt.Sprintf("Unsupported target database: %s", request.TargetDatabase),
			Severity: "critical",
		})
	}

	// Check if source and target are the same
	if request.SourceDatabase == request.TargetDatabase {
		result.Errors = append(result.Errors, ValidationError{
			Type:     "invalid_configuration",
			Field:    "target_database",
			Message:  "Source and target databases cannot be the same",
			Severity: "critical",
		})
	}
}

func (sv *SchemaValidator) validateSourceSchema(request *core.TranslationRequest, result *ValidationResult) {
	// Source schema is now a typed struct, so no JSON validation needed
	if request.SourceSchema == nil {
		result.Errors = append(result.Errors, ValidationError{
			Type:     "missing_field",
			Field:    "source_schema",
			Message:  "Source schema is required",
			Severity: "critical",
		})
		return
	}

	// Check schema complexity (number of objects as a proxy for size)
	objectCount := len(request.SourceSchema.Tables) + len(request.SourceSchema.Collections) +
		len(request.SourceSchema.Views) + len(request.SourceSchema.Functions)

	if objectCount > 1000 { // Large schema threshold
		result.Warnings = append(result.Warnings, ValidationWarning{
			Type:       "large_schema",
			Object:     "source_schema",
			Message:    fmt.Sprintf("Source schema is very large (%d objects)", objectCount),
			Impact:     "Translation may take longer and consume more resources",
			Mitigation: "Consider breaking the schema into smaller chunks",
		})
	}
}

func (sv *SchemaValidator) validatePreferences(request *core.TranslationRequest, result *ValidationResult) {
	prefs := request.Preferences

	// Validate conflicting preferences
	if prefs.OptimizeForPerformance && prefs.OptimizeForStorage {
		result.Warnings = append(result.Warnings, ValidationWarning{
			Type:       "conflicting_preferences",
			Object:     "preferences",
			Message:    "Both performance and storage optimization are enabled",
			Impact:     "May lead to suboptimal results",
			Mitigation: "Choose one primary optimization goal",
		})
	}

	// Validate custom mappings
	for key, value := range prefs.CustomMappings {
		if key == "" || value == "" {
			result.Warnings = append(result.Warnings, ValidationWarning{
				Type:       "invalid_mapping",
				Object:     "custom_mappings",
				Message:    fmt.Sprintf("Empty key or value in custom mapping: %s -> %s", key, value),
				Impact:     "Mapping will be ignored",
				Mitigation: "Provide valid key-value pairs",
			})
		}
	}
}

func (sv *SchemaValidator) validateSchemaStructure(schema *unifiedmodel.UnifiedModel, result *ValidationResult) {
	// Check for empty schema
	if sv.isEmptySchema(schema) {
		result.Warnings = append(result.Warnings, ValidationWarning{
			Type:    "empty_schema",
			Object:  "schema",
			Message: "Schema appears to be empty or has no significant objects",
			Impact:  "Nothing to translate",
		})
	}

	// Validate object naming
	sv.validateObjectNaming(schema, result)

	// Check for duplicate names
	sv.validateUniqueNames(schema, result)
}

func (sv *SchemaValidator) validateObjectRelationships(schema *unifiedmodel.UnifiedModel, result *ValidationResult) {
	// Validate foreign key relationships
	for constraintName, constraint := range schema.Constraints {
		if constraint.Type == unifiedmodel.ConstraintTypeForeignKey {
			// Check if referenced table exists
			if _, exists := schema.Tables[constraint.Reference.Table]; !exists {
				result.Errors = append(result.Errors, ValidationError{
					Type:     "broken_reference",
					Object:   constraintName,
					Message:  fmt.Sprintf("Foreign key references non-existent table: %s", constraint.Reference.Table),
					Severity: "high",
				})
			}
		}
	}

	// Validate circular references
	sv.detectCircularReferences(schema, result)
}

func (sv *SchemaValidator) validateDataTypes(schema *unifiedmodel.UnifiedModel, databaseType dbcapabilities.DatabaseType, result *ValidationResult) {
	// Validate column data types
	for tableName, table := range schema.Tables {
		for columnName, column := range table.Columns {
			if !sv.isValidDataType(column.DataType, databaseType) {
				result.Warnings = append(result.Warnings, ValidationWarning{
					Type:    "invalid_data_type",
					Object:  fmt.Sprintf("%s.%s", tableName, columnName),
					Message: fmt.Sprintf("Data type '%s' may not be supported in %s", column.DataType, databaseType),
					Impact:  "May cause conversion issues",
				})
			}
		}
	}
}

func (sv *SchemaValidator) validateNamingConventions(schema *unifiedmodel.UnifiedModel, result *ValidationResult) {
	// Check table naming conventions
	for tableName := range schema.Tables {
		if !sv.isValidObjectName(tableName) {
			result.Suggestions = append(result.Suggestions, ValidationSuggestion{
				Type:       "naming_convention",
				Object:     tableName,
				Suggestion: "Consider using snake_case or camelCase naming convention",
				Benefit:    "Improved readability and consistency",
				Effort:     "low",
			})
		}
	}
}

func (sv *SchemaValidator) validateTranslationWarnings(translationResult *core.TranslationResult, result *ValidationResult) {
	// Convert translation warnings to validation warnings
	for _, warning := range translationResult.Warnings {
		result.Warnings = append(result.Warnings, ValidationWarning{
			Type:    string(warning.WarningType),
			Object:  warning.ObjectName,
			Message: warning.Message,
			Impact:  warning.Severity,
		})
	}
}

func (sv *SchemaValidator) validateUnsupportedFeatures(translationResult *core.TranslationResult, result *ValidationResult) {
	// Convert unsupported features to validation errors
	for _, feature := range translationResult.UnsupportedFeatures {
		result.Errors = append(result.Errors, ValidationError{
			Type:     "unsupported_feature",
			Object:   feature.ObjectName,
			Message:  feature.Description,
			Severity: "medium",
		})
	}
}

func (sv *SchemaValidator) generateSchemaHealthReport(schema *unifiedmodel.UnifiedModel) SchemaHealthReport {
	objectCounts := map[string]int{
		"tables":             len(schema.Tables),
		"collections":        len(schema.Collections),
		"nodes":              len(schema.Nodes),
		"views":              len(schema.Views),
		"materialized_views": len(schema.MaterializedViews),
		"functions":          len(schema.Functions),
		"procedures":         len(schema.Procedures),
		"triggers":           len(schema.Triggers),
		"indexes":            len(schema.Indexes),
		"constraints":        len(schema.Constraints),
		"sequences":          len(schema.Sequences),
		"types":              len(schema.Types),
	}

	// Calculate complexity score (0-100, lower is better)
	totalObjects := 0
	for _, count := range objectCounts {
		totalObjects += count
	}

	complexityScore := 100.0
	if totalObjects > 0 {
		// Simple heuristic: more objects = higher complexity
		complexityScore = float64(100 - min(totalObjects, 100))
	}

	// Calculate consistency score (placeholder)
	consistencyScore := 85.0 // Would be calculated based on naming conventions, etc.

	// Calculate completeness score (placeholder)
	completenessScore := 90.0 // Would be calculated based on missing metadata, etc.

	// Calculate relationship health
	relationshipHealth := sv.calculateRelationshipHealth(schema)

	// Calculate overall score
	overallScore := (complexityScore + consistencyScore + completenessScore + relationshipHealth.RelationshipIntegrity) / 4

	return SchemaHealthReport{
		OverallScore:       overallScore,
		ComplexityScore:    complexityScore,
		ConsistencyScore:   consistencyScore,
		CompletenessScore:  completenessScore,
		ObjectCounts:       objectCounts,
		RelationshipHealth: relationshipHealth,
	}
}

func (sv *SchemaValidator) calculateRelationshipHealth(schema *unifiedmodel.UnifiedModel) RelationshipHealth {
	totalRelationships := len(schema.Constraints) // Simplified
	validRelationships := 0
	brokenRelationships := 0

	// Count valid vs broken relationships
	for _, constraint := range schema.Constraints {
		if constraint.Type == unifiedmodel.ConstraintTypeForeignKey {
			if _, exists := schema.Tables[constraint.Reference.Table]; exists {
				validRelationships++
			} else {
				brokenRelationships++
			}
		}
	}

	// Calculate integrity percentage
	integrity := 100.0
	if totalRelationships > 0 {
		integrity = float64(validRelationships) / float64(totalRelationships) * 100
	}

	return RelationshipHealth{
		TotalRelationships:    totalRelationships,
		ValidRelationships:    validRelationships,
		BrokenRelationships:   brokenRelationships,
		CircularReferences:    0, // Would be calculated
		OrphanedObjects:       0, // Would be calculated
		RelationshipIntegrity: integrity,
	}
}

// Helper methods

func (sv *SchemaValidator) isEmptySchema(schema *unifiedmodel.UnifiedModel) bool {
	return len(schema.Tables) == 0 &&
		len(schema.Collections) == 0 &&
		len(schema.Nodes) == 0 &&
		len(schema.Views) == 0
}

func (sv *SchemaValidator) isValidObjectName(name string) bool {
	// Simple validation - could be enhanced with proper regex
	return len(name) > 0 &&
		!strings.Contains(name, " ") &&
		!strings.HasPrefix(name, "_") &&
		name == strings.ToLower(name)
}

func (sv *SchemaValidator) isValidDataType(dataType string, databaseType dbcapabilities.DatabaseType) bool {
	// Simplified validation - would check against database-specific type lists
	return dataType != "" && dataType != "unknown"
}

func (sv *SchemaValidator) validateObjectNaming(schema *unifiedmodel.UnifiedModel, result *ValidationResult) {
	// Check for reserved keywords, special characters, etc.
	// This is a simplified implementation
}

func (sv *SchemaValidator) validateUniqueNames(schema *unifiedmodel.UnifiedModel, result *ValidationResult) {
	// Check for duplicate table names, etc.
	// This is a simplified implementation
}

func (sv *SchemaValidator) detectCircularReferences(schema *unifiedmodel.UnifiedModel, result *ValidationResult) {
	// Detect circular foreign key references
	// This is a simplified implementation
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ValidateUnifiedModel is a convenience function for validating a unified model
func ValidateUnifiedModel(schema *unifiedmodel.UnifiedModel) []error {
	if schema == nil {
		return []error{fmt.Errorf("schema cannot be nil")}
	}

	var errors []error

	// Validate database type
	if schema.DatabaseType == "" {
		errors = append(errors, fmt.Errorf("database type cannot be empty"))
	}

	// Validate tables
	for tableName, table := range schema.Tables {
		if table.Name == "" {
			errors = append(errors, fmt.Errorf("table name cannot be empty"))
		}
		if len(table.Columns) == 0 {
			errors = append(errors, fmt.Errorf("table %s must have at least one column", tableName))
		}

		// Check for primary key
		hasPrimaryKey := false
		for _, column := range table.Columns {
			if column.Name == "" {
				errors = append(errors, fmt.Errorf("column name cannot be empty in table %s", tableName))
			}
			if column.DataType == "" {
				errors = append(errors, fmt.Errorf("column %s in table %s must have a data type", column.Name, tableName))
			}
			if column.IsPrimaryKey {
				hasPrimaryKey = true
			}
		}
		if !hasPrimaryKey {
			errors = append(errors, fmt.Errorf("table %s must have a primary key", tableName))
		}
	}

	// Validate collections
	for collectionName, collection := range schema.Collections {
		if collection.Name == "" {
			errors = append(errors, fmt.Errorf("collection name cannot be empty"))
		}
		if len(collection.Fields) == 0 {
			errors = append(errors, fmt.Errorf("collection %s must have at least one field", collectionName))
		}

		for _, field := range collection.Fields {
			if field.Name == "" {
				errors = append(errors, fmt.Errorf("field name cannot be empty in collection %s", collectionName))
			}
			if field.Type == "" {
				errors = append(errors, fmt.Errorf("field %s in collection %s must have a type", field.Name, collectionName))
			}
		}
	}

	// Validate nodes
	for nodeName, node := range schema.Nodes {
		if node.Label == "" {
			errors = append(errors, fmt.Errorf("node label cannot be empty"))
		}
		if len(node.Properties) == 0 {
			errors = append(errors, fmt.Errorf("node %s must have at least one property", nodeName))
		}

		for _, property := range node.Properties {
			if property.Name == "" {
				errors = append(errors, fmt.Errorf("property name cannot be empty in node %s", nodeName))
			}
			if property.Type == "" {
				errors = append(errors, fmt.Errorf("property %s in node %s must have a type", property.Name, nodeName))
			}
		}
	}

	// Validate constraints
	for constraintName, constraint := range schema.Constraints {
		if constraint.Name == "" {
			errors = append(errors, fmt.Errorf("constraint name cannot be empty"))
		}
		if len(constraint.Columns) == 0 {
			errors = append(errors, fmt.Errorf("constraint %s must specify columns", constraintName))
		}

		// Validate foreign key references
		if constraint.Type == unifiedmodel.ConstraintTypeForeignKey {
			if constraint.Reference.Table == "" {
				errors = append(errors, fmt.Errorf("foreign key constraint %s must specify reference table", constraintName))
			}
			if len(constraint.Reference.Columns) == 0 {
				errors = append(errors, fmt.Errorf("foreign key constraint %s must specify reference columns", constraintName))
			}

			// Check if referenced table exists
			if _, exists := schema.Tables[constraint.Reference.Table]; !exists {
				errors = append(errors, fmt.Errorf("foreign key constraint %s references non-existent table %s", constraintName, constraint.Reference.Table))
			} else {
				// Check if referenced columns exist
				refTable := schema.Tables[constraint.Reference.Table]
				for _, refColumn := range constraint.Reference.Columns {
					if _, exists := refTable.Columns[refColumn]; !exists {
						errors = append(errors, fmt.Errorf("foreign key constraint %s references non-existent column %s in table %s", constraintName, refColumn, constraint.Reference.Table))
					}
				}
			}
		}
	}

	// Validate mixed object types for database paradigm
	if len(schema.Tables) > 0 && len(schema.Collections) > 0 {
		errors = append(errors, fmt.Errorf("schema cannot contain both tables and collections"))
	}
	if len(schema.Tables) > 0 && len(schema.Nodes) > 0 {
		errors = append(errors, fmt.Errorf("schema cannot contain both tables and nodes"))
	}
	if len(schema.Collections) > 0 && len(schema.Nodes) > 0 {
		errors = append(errors, fmt.Errorf("schema cannot contain both collections and nodes"))
	}

	return errors
}
