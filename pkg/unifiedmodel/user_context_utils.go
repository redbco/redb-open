package unifiedmodel

import (
	"fmt"
	"time"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// UserContextManager provides utilities for managing user conversion contexts
type UserContextManager struct {
	// In a real implementation, this would connect to a database or storage service
	// For now, we define the interface that the microservice would implement
}

// NewUserContextManager creates a new user context manager
func NewUserContextManager() *UserContextManager {
	return &UserContextManager{}
}

// CreateUserContext creates a new user conversion context
func (ucm *UserContextManager) CreateUserContext(userID string, sourceDB, targetDB dbcapabilities.DatabaseType) *UserConversionContext {
	return &UserConversionContext{
		ContextID:      generateContextID(),
		UserID:         userID,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
		SourceDatabase: sourceDB,
		TargetDatabase: targetDB,
		GlobalPreferences: ConversionPreferences{
			AcceptDataLoss:         false,
			OptimizeForPerformance: true,
			OptimizeForStorage:     false,
			PreserveRelationships:  true,
			IncludeMetadata:        true,
		},
		ObjectMappings:      make(map[string]UserObjectMapping),
		FieldMappings:       make(map[string]UserFieldMapping),
		CustomRules:         []UserConversionRule{},
		IgnoredObjects:      []string{},
		RequiredValidations: []UserValidationRule{},
	}
}

// ValidateUserContext validates a user context for consistency and completeness
func (ucm *UserContextManager) ValidateUserContext(context *UserConversionContext) []ValidationError {
	errors := []ValidationError{}

	if context == nil {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorCritical,
			Field:   "context",
			Message: "User context cannot be nil",
		})
		return errors
	}

	// Validate required fields
	if context.UserID == "" {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorCritical,
			Field:   "user_id",
			Message: "User ID is required",
		})
	}

	if context.SourceDatabase == "" {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorCritical,
			Field:   "source_database",
			Message: "Source database is required",
		})
	}

	if context.TargetDatabase == "" {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorCritical,
			Field:   "target_database",
			Message: "Target database is required",
		})
	}

	// Validate object mappings
	for objectName, mapping := range context.ObjectMappings {
		if mapping.SourceObjectName == "" {
			errors = append(errors, ValidationError{
				Type:    ValidationErrorWarning,
				Field:   fmt.Sprintf("object_mappings.%s.source_object_name", objectName),
				Message: "Source object name should not be empty",
			})
		}

		if mapping.SourceObjectType == "" {
			errors = append(errors, ValidationError{
				Type:    ValidationErrorCritical,
				Field:   fmt.Sprintf("object_mappings.%s.source_object_type", objectName),
				Message: "Source object type is required",
			})
		}

		if mapping.TargetObjectType == "" {
			errors = append(errors, ValidationError{
				Type:    ValidationErrorCritical,
				Field:   fmt.Sprintf("object_mappings.%s.target_object_type", objectName),
				Message: "Target object type is required",
			})
		}
	}

	// Validate field mappings
	for fieldKey, mapping := range context.FieldMappings {
		if mapping.SourceObjectName == "" || mapping.SourceFieldName == "" {
			errors = append(errors, ValidationError{
				Type:    ValidationErrorCritical,
				Field:   fmt.Sprintf("field_mappings.%s", fieldKey),
				Message: "Source object name and field name are required",
			})
		}

		if mapping.DataTypeMapping.SourceType == "" || mapping.DataTypeMapping.TargetType == "" {
			errors = append(errors, ValidationError{
				Type:    ValidationErrorCritical,
				Field:   fmt.Sprintf("field_mappings.%s.data_type_mapping", fieldKey),
				Message: "Source and target data types are required",
			})
		}
	}

	// Validate custom rules
	for i, rule := range context.CustomRules {
		if rule.Name == "" {
			errors = append(errors, ValidationError{
				Type:    ValidationErrorWarning,
				Field:   fmt.Sprintf("custom_rules[%d].name", i),
				Message: "Custom rule name should not be empty",
			})
		}

		if len(rule.Conditions) == 0 {
			errors = append(errors, ValidationError{
				Type:    ValidationErrorWarning,
				Field:   fmt.Sprintf("custom_rules[%d].conditions", i),
				Message: "Custom rule should have at least one condition",
			})
		}

		if len(rule.Actions) == 0 {
			errors = append(errors, ValidationError{
				Type:    ValidationErrorWarning,
				Field:   fmt.Sprintf("custom_rules[%d].actions", i),
				Message: "Custom rule should have at least one action",
			})
		}
	}

	return errors
}

// MergeUserContexts merges multiple user contexts, with later contexts taking precedence
func (ucm *UserContextManager) MergeUserContexts(contexts ...*UserConversionContext) (*UserConversionContext, error) {
	if len(contexts) == 0 {
		return nil, fmt.Errorf("no contexts provided for merging")
	}

	// Start with the first context as base
	merged := *contexts[0]
	merged.UpdatedAt = time.Now()

	// Merge each subsequent context
	for i := 1; i < len(contexts); i++ {
		context := contexts[i]
		if context == nil {
			continue
		}

		// Validate compatibility
		if context.SourceDatabase != merged.SourceDatabase || context.TargetDatabase != merged.TargetDatabase {
			return nil, fmt.Errorf("cannot merge contexts with different database pairs")
		}

		// Merge global preferences (later takes precedence)
		if context.GlobalPreferences.PreferredStrategy != "" {
			merged.GlobalPreferences.PreferredStrategy = context.GlobalPreferences.PreferredStrategy
		}
		merged.GlobalPreferences.AcceptDataLoss = context.GlobalPreferences.AcceptDataLoss
		merged.GlobalPreferences.OptimizeForPerformance = context.GlobalPreferences.OptimizeForPerformance
		merged.GlobalPreferences.OptimizeForStorage = context.GlobalPreferences.OptimizeForStorage
		merged.GlobalPreferences.PreserveRelationships = context.GlobalPreferences.PreserveRelationships
		merged.GlobalPreferences.IncludeMetadata = context.GlobalPreferences.IncludeMetadata

		// Merge object mappings
		for key, mapping := range context.ObjectMappings {
			merged.ObjectMappings[key] = mapping
		}

		// Merge field mappings
		for key, mapping := range context.FieldMappings {
			merged.FieldMappings[key] = mapping
		}

		// Merge custom rules (append, don't replace)
		merged.CustomRules = append(merged.CustomRules, context.CustomRules...)

		// Merge ignored objects (union)
		for _, obj := range context.IgnoredObjects {
			if !contains(merged.IgnoredObjects, obj) {
				merged.IgnoredObjects = append(merged.IgnoredObjects, obj)
			}
		}

		// Merge validation rules (append)
		merged.RequiredValidations = append(merged.RequiredValidations, context.RequiredValidations...)
	}

	return &merged, nil
}

// GenerateContextSummary creates a human-readable summary of the user context
func (ucm *UserContextManager) GenerateContextSummary(context *UserConversionContext) string {
	if context == nil {
		return "No user context provided"
	}

	summary := fmt.Sprintf("Conversion Context: %s → %s\n", context.SourceDatabase, context.TargetDatabase)
	summary += fmt.Sprintf("User: %s, Created: %s\n", context.UserID, context.CreatedAt.Format("2006-01-02 15:04:05"))

	if context.Description != "" {
		summary += fmt.Sprintf("Description: %s\n", context.Description)
	}

	summary += fmt.Sprintf("Object Mappings: %d\n", len(context.ObjectMappings))
	summary += fmt.Sprintf("Field Mappings: %d\n", len(context.FieldMappings))
	summary += fmt.Sprintf("Custom Rules: %d\n", len(context.CustomRules))
	summary += fmt.Sprintf("Ignored Objects: %d\n", len(context.IgnoredObjects))

	// Global preferences
	summary += "Preferences:\n"
	if context.GlobalPreferences.PreferredStrategy != "" {
		summary += fmt.Sprintf("  - Preferred Strategy: %s\n", context.GlobalPreferences.PreferredStrategy)
	}
	summary += fmt.Sprintf("  - Accept Data Loss: %t\n", context.GlobalPreferences.AcceptDataLoss)
	summary += fmt.Sprintf("  - Optimize for Performance: %t\n", context.GlobalPreferences.OptimizeForPerformance)
	summary += fmt.Sprintf("  - Preserve Relationships: %t\n", context.GlobalPreferences.PreserveRelationships)

	return summary
}

// ApplyContextToConversionRequest applies user context to a conversion request
func (ucm *UserContextManager) ApplyContextToConversionRequest(request *ConversionRequest, context *UserConversionContext) (*ConversionRequest, []ContextApplicationWarning, error) {
	if context == nil {
		return request, nil, nil
	}

	warnings := []ContextApplicationWarning{}
	enhancedRequest := *request

	// Apply global preferences
	enhancedRequest.UserPreferences = context.GlobalPreferences

	// Apply custom mappings to user preferences
	if context.GlobalPreferences.CustomMappings == nil {
		enhancedRequest.UserPreferences.CustomMappings = make(map[string]string)
	}

	// Convert object mappings to custom mappings
	for objectName, mapping := range context.ObjectMappings {
		if mapping.TargetObjectName != "" {
			enhancedRequest.UserPreferences.CustomMappings[objectName] = mapping.TargetObjectName
		}
	}

	// Apply ignored objects
	enhancedRequest.UserPreferences.ExcludeObjects = append(
		enhancedRequest.UserPreferences.ExcludeObjects,
		convertIgnoredObjectsToObjectTypes(context.IgnoredObjects)...,
	)

	return &enhancedRequest, warnings, nil
}

// Helper functions

func generateContextID() string {
	// In a real implementation, this would generate a proper UUID
	return fmt.Sprintf("ctx_%d", time.Now().UnixNano())
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func convertIgnoredObjectsToObjectTypes(ignored []string) []ObjectType {
	result := []ObjectType{}
	for _, obj := range ignored {
		result = append(result, ObjectType(obj))
	}
	return result
}

// UserContextTemplate provides templates for common conversion scenarios
type UserContextTemplate struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Template    *UserConversionContext `json:"template"`
}

// GetCommonTemplates returns pre-defined templates for common conversion scenarios
func GetCommonTemplates() []UserContextTemplate {
	return []UserContextTemplate{
		{
			Name:        "Conservative Migration",
			Description: "Prioritizes data integrity and minimal data loss",
			Template: &UserConversionContext{
				GlobalPreferences: ConversionPreferences{
					AcceptDataLoss:         false,
					OptimizeForPerformance: false,
					OptimizeForStorage:     false,
					PreserveRelationships:  true,
					IncludeMetadata:        true,
				},
			},
		},
		{
			Name:        "Performance Optimized",
			Description: "Optimizes for query performance in target database",
			Template: &UserConversionContext{
				GlobalPreferences: ConversionPreferences{
					AcceptDataLoss:         true,
					OptimizeForPerformance: true,
					OptimizeForStorage:     false,
					PreserveRelationships:  false,
					IncludeMetadata:        false,
				},
			},
		},
		{
			Name:        "Storage Optimized",
			Description: "Minimizes storage requirements in target database",
			Template: &UserConversionContext{
				GlobalPreferences: ConversionPreferences{
					AcceptDataLoss:         true,
					OptimizeForPerformance: false,
					OptimizeForStorage:     true,
					PreserveRelationships:  false,
					IncludeMetadata:        false,
				},
			},
		},
	}
}
