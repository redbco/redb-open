package unifiedmodel

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// UnifiedDataType represents a database-agnostic data type
type UnifiedDataType string

const (
	// Numeric types
	UnifiedTypeBoolean UnifiedDataType = "boolean"
	UnifiedTypeInt8    UnifiedDataType = "int8"
	UnifiedTypeInt16   UnifiedDataType = "int16"
	UnifiedTypeInt32   UnifiedDataType = "int32"
	UnifiedTypeInt64   UnifiedDataType = "int64"
	UnifiedTypeUInt8   UnifiedDataType = "uint8"
	UnifiedTypeUInt16  UnifiedDataType = "uint16"
	UnifiedTypeUInt32  UnifiedDataType = "uint32"
	UnifiedTypeUInt64  UnifiedDataType = "uint64"
	UnifiedTypeFloat32 UnifiedDataType = "float32"
	UnifiedTypeFloat64 UnifiedDataType = "float64"
	UnifiedTypeDecimal UnifiedDataType = "decimal"
	UnifiedTypeNumeric UnifiedDataType = "numeric"
	UnifiedTypeMoney   UnifiedDataType = "money"

	// String types
	UnifiedTypeString  UnifiedDataType = "string"
	UnifiedTypeText    UnifiedDataType = "text"
	UnifiedTypeChar    UnifiedDataType = "char"
	UnifiedTypeVarchar UnifiedDataType = "varchar"
	UnifiedTypeClob    UnifiedDataType = "clob"
	UnifiedTypeBlob    UnifiedDataType = "blob"

	// Date/Time types
	UnifiedTypeDate      UnifiedDataType = "date"
	UnifiedTypeTime      UnifiedDataType = "time"
	UnifiedTypeTimestamp UnifiedDataType = "timestamp"
	UnifiedTypeInterval  UnifiedDataType = "interval"

	// Binary types
	UnifiedTypeBinary    UnifiedDataType = "binary"
	UnifiedTypeVarbinary UnifiedDataType = "varbinary"
	UnifiedTypeUUID      UnifiedDataType = "uuid"

	// JSON/Document types
	UnifiedTypeJSON     UnifiedDataType = "json"
	UnifiedTypeJSONB    UnifiedDataType = "jsonb"
	UnifiedTypeDocument UnifiedDataType = "document"
	UnifiedTypeObject   UnifiedDataType = "object"

	// Array/Collection types
	UnifiedTypeArray UnifiedDataType = "array"
	UnifiedTypeList  UnifiedDataType = "list"
	UnifiedTypeSet   UnifiedDataType = "set"
	UnifiedTypeMap   UnifiedDataType = "map"

	// Geospatial types
	UnifiedTypePoint      UnifiedDataType = "point"
	UnifiedTypeLineString UnifiedDataType = "linestring"
	UnifiedTypePolygon    UnifiedDataType = "polygon"
	UnifiedTypeGeometry   UnifiedDataType = "geometry"
	UnifiedTypeGeography  UnifiedDataType = "geography"

	// Vector types
	UnifiedTypeVector    UnifiedDataType = "vector"
	UnifiedTypeEmbedding UnifiedDataType = "embedding"

	// Graph types
	UnifiedTypeNode     UnifiedDataType = "node"
	UnifiedTypeEdge     UnifiedDataType = "edge"
	UnifiedTypeRelation UnifiedDataType = "relation"

	// Special types
	UnifiedTypeEnum    UnifiedDataType = "enum"
	UnifiedTypeRange   UnifiedDataType = "range"
	UnifiedTypeXML     UnifiedDataType = "xml"
	UnifiedTypeUnknown UnifiedDataType = "unknown"
)

// DataTypeConversionRule defines how to convert between specific data types
type DataTypeConversionRule struct {
	SourceDatabase dbcapabilities.DatabaseType `json:"source_database"`
	TargetDatabase dbcapabilities.DatabaseType `json:"target_database"`
	SourceType     string                      `json:"source_type"`
	TargetType     string                      `json:"target_type"`
	UnifiedType    UnifiedDataType             `json:"unified_type"`

	ConversionType    ConversionType `json:"conversion_type"`
	IsLossyConversion bool           `json:"is_lossy_conversion"`
	RequiresUserInput bool           `json:"requires_user_input"`

	// Conversion notes and warnings
	ConversionNotes string   `json:"conversion_notes,omitempty"`
	Warnings        []string `json:"warnings,omitempty"`
}

// DataTypeConversionResult represents the result of a data type conversion
type DataTypeConversionResult struct {
	OriginalType       string          `json:"original_type"`
	ConvertedType      string          `json:"converted_type"`
	UnifiedType        UnifiedDataType `json:"unified_type"`
	ConversionType     ConversionType  `json:"conversion_type"`
	IsLossyConversion  bool            `json:"is_lossy_conversion"`
	RequiresValidation bool            `json:"requires_validation"`
	ConversionNotes    string          `json:"conversion_notes,omitempty"`
	Warnings           []string        `json:"warnings,omitempty"`
}

// DataTypeConversionKey uniquely identifies a type conversion
type DataTypeConversionKey struct {
	SourceDB   dbcapabilities.DatabaseType
	TargetDB   dbcapabilities.DatabaseType
	SourceType string
}

// DataTypeRegistry holds type conversion rules for all database pairs
var DataTypeRegistry = map[DataTypeConversionKey]DataTypeConversionRule{}

// TypeConverter provides data type conversion functionality
type TypeConverter struct {
	registry          map[DataTypeConversionKey]DataTypeConversionRule
	scalableConverter *ScalableTypeConverter
}

// NewTypeConverter creates a new type converter
func NewTypeConverter() *TypeConverter {
	converter := &TypeConverter{
		registry: make(map[DataTypeConversionKey]DataTypeConversionRule),
	}
	// Initialize scalable converter (metadata-driven approach)
	converter.scalableConverter = NewScalableTypeConverter()

	return converter
}

// ConvertDataType converts a data type from source to target database
func (tc *TypeConverter) ConvertDataType(sourceDB, targetDB dbcapabilities.DatabaseType, sourceType string) (*DataTypeConversionResult, error) {
	// Use scalable converter as primary method (metadata-driven approach)
	if tc.scalableConverter != nil {
		result, err := tc.scalableConverter.ConvertPrimitiveType(sourceDB, targetDB, sourceType)
		if err == nil {
			return result, nil
		}
		// If scalable converter fails, try with base type (strip parameters)
		baseType := tc.extractBaseType(strings.ToLower(strings.TrimSpace(sourceType)))
		if baseType != strings.ToLower(strings.TrimSpace(sourceType)) {
			result, err := tc.scalableConverter.ConvertPrimitiveType(sourceDB, targetDB, baseType)
			if err == nil {
				// Preserve original parameters in the target type if applicable
				result.ConvertedType = tc.preserveTypeParameters(sourceType, result.ConvertedType)
				result.OriginalType = sourceType
				return result, nil
			}
		}
	}

	// Fallback to legacy registry for backward compatibility (only for specific edge cases)
	normalizedSourceType := strings.ToLower(strings.TrimSpace(sourceType))
	key := DataTypeConversionKey{
		SourceDB:   sourceDB,
		TargetDB:   targetDB,
		SourceType: normalizedSourceType,
	}

	if rule, exists := tc.registry[key]; exists {
		return &DataTypeConversionResult{
			OriginalType:       sourceType,
			ConvertedType:      rule.TargetType,
			UnifiedType:        rule.UnifiedType,
			ConversionType:     rule.ConversionType,
			IsLossyConversion:  rule.IsLossyConversion,
			RequiresValidation: rule.RequiresUserInput,
			ConversionNotes:    rule.ConversionNotes + " (legacy rule)",
			Warnings:           rule.Warnings,
		}, nil
	}

	// No conversion found
	return nil, fmt.Errorf("no conversion rule found for %s.%s -> %s", sourceDB, sourceType, targetDB)
}

// ConvertColumn converts a column's data type for schema conversion
func (tc *TypeConverter) ConvertColumn(column Column, sourceDB, targetDB dbcapabilities.DatabaseType) (Column, error) {
	result, err := tc.ConvertDataType(sourceDB, targetDB, column.DataType)
	if err != nil {
		return column, fmt.Errorf("failed to convert column %s data type: %w", column.Name, err)
	}

	// Create converted column
	convertedColumn := column
	convertedColumn.DataType = result.ConvertedType

	// Add conversion metadata to options
	if convertedColumn.Options == nil {
		convertedColumn.Options = make(map[string]any)
	}
	convertedColumn.Options["original_type"] = result.OriginalType
	convertedColumn.Options["unified_type"] = string(result.UnifiedType)
	convertedColumn.Options["conversion_type"] = string(result.ConversionType)
	convertedColumn.Options["is_lossy_conversion"] = result.IsLossyConversion

	if result.ConversionNotes != "" {
		convertedColumn.Options["conversion_notes"] = result.ConversionNotes
	}

	if len(result.Warnings) > 0 {
		convertedColumn.Options["conversion_warnings"] = result.Warnings
	}

	return convertedColumn, nil
}

// ConvertField converts a field's data type for schema conversion
func (tc *TypeConverter) ConvertField(field Field, sourceDB, targetDB dbcapabilities.DatabaseType) (Field, error) {
	result, err := tc.ConvertDataType(sourceDB, targetDB, field.Type)
	if err != nil {
		return field, fmt.Errorf("failed to convert field %s data type: %w", field.Name, err)
	}

	// Create converted field
	convertedField := field
	convertedField.Type = result.ConvertedType

	// Add conversion metadata to options
	if convertedField.Options == nil {
		convertedField.Options = make(map[string]any)
	}
	convertedField.Options["original_type"] = result.OriginalType
	convertedField.Options["unified_type"] = string(result.UnifiedType)
	convertedField.Options["conversion_type"] = string(result.ConversionType)
	convertedField.Options["is_lossy_conversion"] = result.IsLossyConversion

	if result.ConversionNotes != "" {
		convertedField.Options["conversion_notes"] = result.ConversionNotes
	}

	if len(result.Warnings) > 0 {
		convertedField.Options["conversion_warnings"] = result.Warnings
	}

	return convertedField, nil
}

// ConvertProperty converts a property's data type for schema conversion
func (tc *TypeConverter) ConvertProperty(property Property, sourceDB, targetDB dbcapabilities.DatabaseType) (Property, error) {
	result, err := tc.ConvertDataType(sourceDB, targetDB, property.Type)
	if err != nil {
		return property, fmt.Errorf("failed to convert property %s data type: %w", property.Name, err)
	}

	// Create converted property
	convertedProperty := property
	convertedProperty.Type = result.ConvertedType

	// Add conversion metadata to options
	if convertedProperty.Options == nil {
		convertedProperty.Options = make(map[string]any)
	}
	convertedProperty.Options["original_type"] = result.OriginalType
	convertedProperty.Options["unified_type"] = string(result.UnifiedType)
	convertedProperty.Options["conversion_type"] = string(result.ConversionType)
	convertedProperty.Options["is_lossy_conversion"] = result.IsLossyConversion

	if result.ConversionNotes != "" {
		convertedProperty.Options["conversion_notes"] = result.ConversionNotes
	}

	if len(result.Warnings) > 0 {
		convertedProperty.Options["conversion_warnings"] = result.Warnings
	}

	return convertedProperty, nil
}

// ConvertCustomType converts a custom/user-defined type
func (tc *TypeConverter) ConvertCustomType(customType Type, sourceDB, targetDB dbcapabilities.DatabaseType) (*CustomTypeConversionResult, error) {
	// Use scalable converter (metadata-driven approach only)
	if tc.scalableConverter == nil {
		return nil, fmt.Errorf("scalable converter not initialized")
	}
	return tc.scalableConverter.ConvertCustomTypeScalable(customType, sourceDB, targetDB)
}

// ValidateCustomTypeConversion validates a custom type conversion
func (tc *TypeConverter) ValidateCustomTypeConversion(customType Type, sourceDB, targetDB dbcapabilities.DatabaseType) (*CustomTypeValidation, error) {
	// For now, return a basic validation - can be enhanced later if needed
	return &CustomTypeValidation{
		IsSupported:       true,
		Strategy:          CustomTypeStrategyDirect,
		RequiresUserInput: false,
		Warnings:          []string{"Using metadata-driven validation"},
	}, nil
}

// IsCustomType determines if a type name refers to a custom type in the schema
func (tc *TypeConverter) IsCustomType(typeName string, schema *UnifiedModel) bool {
	if schema == nil {
		return false
	}

	// Check if type exists in schema's custom types
	_, exists := schema.Types[typeName]
	return exists
}

// ConvertDataTypeWithCustomTypes converts a data type, checking for custom types first
func (tc *TypeConverter) ConvertDataTypeWithCustomTypes(sourceDB, targetDB dbcapabilities.DatabaseType, sourceType string, schema *UnifiedModel) (*DataTypeConversionResult, error) {
	// First check if it's a custom type
	if tc.IsCustomType(sourceType, schema) {
		customType := schema.Types[sourceType]
		customResult, err := tc.ConvertCustomType(customType, sourceDB, targetDB)
		if err != nil {
			return nil, fmt.Errorf("failed to convert custom type %s: %w", sourceType, err)
		}

		// Convert custom type result to standard data type result
		result := &DataTypeConversionResult{
			OriginalType:       sourceType,
			ConvertedType:      sourceType,      // Will be updated based on strategy
			UnifiedType:        UnifiedTypeEnum, // Default for custom types
			ConversionType:     ConversionTypeTransform,
			IsLossyConversion:  len(customResult.Warnings) > 0,
			RequiresValidation: customResult.RequiresUserInput,
			ConversionNotes:    customResult.ConversionNotes,
			Warnings:           customResult.Warnings,
		}

		// Update converted type based on conversion strategy
		switch customResult.ConversionStrategy {
		case CustomTypeStrategyDirect:
			if len(customResult.ConvertedTypes) > 0 {
				result.ConvertedType = customResult.ConvertedTypes[0].Name
				result.ConversionType = ConversionTypeDirect
			}
		case CustomTypeStrategyEmulate, CustomTypeStrategyString:
			result.ConvertedType = "text" // Default string type
			if targetDB == dbcapabilities.MySQL {
				result.ConvertedType = "varchar(255)"
			}
		case CustomTypeStrategyJSON:
			result.ConvertedType = "jsonb"
			switch targetDB {
			case dbcapabilities.MySQL:
				result.ConvertedType = "json"
			case dbcapabilities.MongoDB:
				result.ConvertedType = "object"
			}
		case CustomTypeStrategyTable:
			result.ConvertedType = "integer" // Reference to lookup table
			result.ConversionNotes += " (converted to lookup table reference)"
		case CustomTypeStrategyFlatten:
			result.ConversionNotes += " (flattened to multiple columns)"
			result.ConvertedType = "flattened" // Special marker
		}

		return result, nil
	}

	// Fall back to standard data type conversion
	return tc.ConvertDataType(sourceDB, targetDB, sourceType)
}

// extractBaseType extracts the base type from a parameterized type
// e.g., "varchar(255)" -> "varchar", "decimal(10,2)" -> "decimal"
func (tc *TypeConverter) extractBaseType(dataType string) string {
	// Find the first opening parenthesis
	if idx := strings.Index(dataType, "("); idx != -1 {
		return dataType[:idx]
	}
	return dataType
}

// preserveTypeParameters preserves parameters from source type in target type
// e.g., source: "varchar(255)", target: "varchar" -> "varchar(255)"
func (tc *TypeConverter) preserveTypeParameters(sourceType, targetType string) string {
	// If target type already has parameters, don't add more
	if strings.Contains(targetType, "(") {
		return targetType
	}

	// Extract parameters from source type
	if idx := strings.Index(sourceType, "("); idx != -1 {
		parameters := sourceType[idx:]
		// Check if target type can accept parameters
		if tc.canAcceptParameters(targetType) {
			return targetType + parameters
		}
	}
	return targetType
}

// canAcceptParameters checks if a data type can accept parameters
func (tc *TypeConverter) canAcceptParameters(dataType string) bool {
	baseType := strings.ToLower(dataType)
	parameterizedTypes := []string{
		"varchar", "char", "decimal", "numeric", "float", "double", "precision",
		"bit", "varbinary", "binary", "time", "timestamp", "datetime",
	}

	for _, paramType := range parameterizedTypes {
		if strings.Contains(baseType, paramType) {
			return true
		}
	}
	return false
}

// GetSupportedConversions returns all supported type conversions for a database pair
func (tc *TypeConverter) GetSupportedConversions(sourceDB, targetDB dbcapabilities.DatabaseType) []DataTypeConversionRule {
	var conversions []DataTypeConversionRule

	// Then, generate conversions from the ScalableTypeConverter metadata
	if tc.scalableConverter != nil {
		sourceMetadata, sourceExists := tc.scalableConverter.metadata[sourceDB]
		targetMetadata, targetExists := tc.scalableConverter.metadata[targetDB]

		if sourceExists && targetExists {
			// Generate conversions for all source types that can be converted
			for sourceTypeName, sourceTypeInfo := range sourceMetadata.PrimitiveTypes {
				// Find target type for this unified type
				if targetTypeName, found := tc.scalableConverter.findTargetType(sourceTypeInfo.UnifiedType, targetMetadata); found {
					rule := DataTypeConversionRule{
						SourceDatabase:    sourceDB,
						TargetDatabase:    targetDB,
						SourceType:        sourceTypeName,
						TargetType:        targetTypeName,
						UnifiedType:       sourceTypeInfo.UnifiedType,
						ConversionType:    ConversionTypeDirect,
						IsLossyConversion: false,
						RequiresUserInput: false,
						ConversionNotes:   fmt.Sprintf("Generated from unified type %s", sourceTypeInfo.UnifiedType),
					}
					conversions = append(conversions, rule)
				}
			}
		}
	}

	return conversions
}

// ValidateTypeConversion checks if a type conversion is possible and safe
func (tc *TypeConverter) ValidateTypeConversion(sourceDB, targetDB dbcapabilities.DatabaseType, sourceType string) (*TypeConversionValidation, error) {
	result, err := tc.ConvertDataType(sourceDB, targetDB, sourceType)
	if err != nil {
		return &TypeConversionValidation{
			IsSupported:     false,
			ErrorMessage:    err.Error(),
			Recommendations: []string{"Consider using a different target type", "Check if manual conversion is needed"},
		}, nil
	}

	validation := &TypeConversionValidation{
		IsSupported:       true,
		ConversionResult:  result,
		IsLossyConversion: result.IsLossyConversion,
		RequiresUserInput: result.RequiresValidation,
	}

	if result.IsLossyConversion {
		validation.Warnings = append(validation.Warnings, "This conversion may result in data loss")
		validation.Recommendations = append(validation.Recommendations, "Review data before conversion", "Consider data validation after conversion")
	}

	if len(result.Warnings) > 0 {
		validation.Warnings = append(validation.Warnings, result.Warnings...)
	}

	return validation, nil
}

// TypeConversionValidation represents validation results for a type conversion
type TypeConversionValidation struct {
	IsSupported       bool                      `json:"is_supported"`
	ConversionResult  *DataTypeConversionResult `json:"conversion_result,omitempty"`
	IsLossyConversion bool                      `json:"is_lossy_conversion"`
	RequiresUserInput bool                      `json:"requires_user_input"`
	Warnings          []string                  `json:"warnings,omitempty"`
	Recommendations   []string                  `json:"recommendations,omitempty"`
	ErrorMessage      string                    `json:"error_message,omitempty"`
}
