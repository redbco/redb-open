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
	// Keep legacy rules for backward compatibility
	converter.initializeConversionRules()
	return converter
}

// ConvertDataType converts a data type from source to target database
func (tc *TypeConverter) ConvertDataType(sourceDB, targetDB dbcapabilities.DatabaseType, sourceType string) (*DataTypeConversionResult, error) {
	// Use scalable converter first (metadata-driven approach)
	if tc.scalableConverter != nil {
		result, err := tc.scalableConverter.ConvertPrimitiveType(sourceDB, targetDB, sourceType)
		if err == nil {
			return result, nil
		}
		// If scalable converter fails, continue with legacy approach
	}

	// Fallback to legacy approach for backward compatibility
	normalizedSourceType := strings.ToLower(strings.TrimSpace(sourceType))

	// Try direct conversion rule first
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
			ConversionNotes:    rule.ConversionNotes,
			Warnings:           rule.Warnings,
		}, nil
	}

	// Try conversion through unified type (legacy)
	unifiedType := tc.mapNativeTypeToUnified(sourceDB, normalizedSourceType)
	if unifiedType != UnifiedTypeUnknown {
		targetType := tc.mapUnifiedTypeToNative(targetDB, unifiedType)
		if targetType != "" {
			return &DataTypeConversionResult{
				OriginalType:       sourceType,
				ConvertedType:      targetType,
				UnifiedType:        unifiedType,
				ConversionType:     ConversionTypeDirect,
				IsLossyConversion:  false,
				RequiresValidation: false,
				ConversionNotes:    fmt.Sprintf("Converted via unified type %s (legacy)", unifiedType),
			}, nil
		}
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
			if targetDB == dbcapabilities.MySQL {
				result.ConvertedType = "json"
			} else if targetDB == dbcapabilities.MongoDB {
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

// initializeConversionRules populates the conversion registry with common type mappings
func (tc *TypeConverter) initializeConversionRules() {
	// MongoDB -> PostgreSQL conversions
	tc.addConversionRule(dbcapabilities.MongoDB, dbcapabilities.PostgreSQL, "int32", "integer", UnifiedTypeInt32, ConversionTypeDirect, false, false, "MongoDB Int32 maps directly to PostgreSQL integer")
	tc.addConversionRule(dbcapabilities.MongoDB, dbcapabilities.PostgreSQL, "int64", "bigint", UnifiedTypeInt64, ConversionTypeDirect, false, false, "MongoDB Int64 maps directly to PostgreSQL bigint")
	tc.addConversionRule(dbcapabilities.MongoDB, dbcapabilities.PostgreSQL, "string", "text", UnifiedTypeString, ConversionTypeDirect, false, false, "MongoDB String maps to PostgreSQL text")
	tc.addConversionRule(dbcapabilities.MongoDB, dbcapabilities.PostgreSQL, "boolean", "boolean", UnifiedTypeBoolean, ConversionTypeDirect, false, false, "MongoDB Boolean maps directly to PostgreSQL boolean")
	tc.addConversionRule(dbcapabilities.MongoDB, dbcapabilities.PostgreSQL, "date", "timestamp", UnifiedTypeTimestamp, ConversionTypeDirect, false, false, "MongoDB Date maps to PostgreSQL timestamp")
	tc.addConversionRule(dbcapabilities.MongoDB, dbcapabilities.PostgreSQL, "objectid", "varchar(24)", UnifiedTypeString, ConversionTypeTransform, false, false, "MongoDB ObjectId converted to 24-character string")
	tc.addConversionRule(dbcapabilities.MongoDB, dbcapabilities.PostgreSQL, "double", "double precision", UnifiedTypeFloat64, ConversionTypeDirect, false, false, "MongoDB Double maps to PostgreSQL double precision")
	tc.addConversionRule(dbcapabilities.MongoDB, dbcapabilities.PostgreSQL, "object", "jsonb", UnifiedTypeJSON, ConversionTypeDirect, false, false, "MongoDB Object/Document maps to PostgreSQL JSONB")
	tc.addConversionRule(dbcapabilities.MongoDB, dbcapabilities.PostgreSQL, "array", "jsonb", UnifiedTypeArray, ConversionTypeDirect, false, false, "MongoDB Array maps to PostgreSQL JSONB array")

	// PostgreSQL -> MongoDB conversions
	tc.addConversionRule(dbcapabilities.PostgreSQL, dbcapabilities.MongoDB, "integer", "int32", UnifiedTypeInt32, ConversionTypeDirect, false, false, "PostgreSQL integer maps directly to MongoDB Int32")
	tc.addConversionRule(dbcapabilities.PostgreSQL, dbcapabilities.MongoDB, "int4", "int32", UnifiedTypeInt32, ConversionTypeDirect, false, false, "PostgreSQL int4 maps directly to MongoDB Int32")
	tc.addConversionRule(dbcapabilities.PostgreSQL, dbcapabilities.MongoDB, "bigint", "int64", UnifiedTypeInt64, ConversionTypeDirect, false, false, "PostgreSQL bigint maps directly to MongoDB Int64")
	tc.addConversionRule(dbcapabilities.PostgreSQL, dbcapabilities.MongoDB, "int8", "int64", UnifiedTypeInt64, ConversionTypeDirect, false, false, "PostgreSQL int8 maps directly to MongoDB Int64")
	tc.addConversionRule(dbcapabilities.PostgreSQL, dbcapabilities.MongoDB, "text", "string", UnifiedTypeString, ConversionTypeDirect, false, false, "PostgreSQL text maps to MongoDB String")
	tc.addConversionRule(dbcapabilities.PostgreSQL, dbcapabilities.MongoDB, "varchar", "string", UnifiedTypeString, ConversionTypeDirect, false, false, "PostgreSQL varchar maps to MongoDB String")
	tc.addConversionRule(dbcapabilities.PostgreSQL, dbcapabilities.MongoDB, "boolean", "boolean", UnifiedTypeBoolean, ConversionTypeDirect, false, false, "PostgreSQL boolean maps directly to MongoDB Boolean")
	tc.addConversionRule(dbcapabilities.PostgreSQL, dbcapabilities.MongoDB, "timestamp", "date", UnifiedTypeTimestamp, ConversionTypeDirect, false, false, "PostgreSQL timestamp maps to MongoDB Date")
	tc.addConversionRule(dbcapabilities.PostgreSQL, dbcapabilities.MongoDB, "timestamptz", "date", UnifiedTypeTimestamp, ConversionTypeDirect, false, false, "PostgreSQL timestamptz maps to MongoDB Date")
	tc.addConversionRule(dbcapabilities.PostgreSQL, dbcapabilities.MongoDB, "uuid", "string", UnifiedTypeUUID, ConversionTypeTransform, false, false, "PostgreSQL UUID converted to string representation")
	tc.addConversionRule(dbcapabilities.PostgreSQL, dbcapabilities.MongoDB, "double precision", "double", UnifiedTypeFloat64, ConversionTypeDirect, false, false, "PostgreSQL double precision maps to MongoDB Double")
	tc.addConversionRule(dbcapabilities.PostgreSQL, dbcapabilities.MongoDB, "float8", "double", UnifiedTypeFloat64, ConversionTypeDirect, false, false, "PostgreSQL float8 maps to MongoDB Double")
	tc.addConversionRule(dbcapabilities.PostgreSQL, dbcapabilities.MongoDB, "jsonb", "object", UnifiedTypeJSON, ConversionTypeDirect, false, false, "PostgreSQL JSONB maps to MongoDB Object/Document")
	tc.addConversionRule(dbcapabilities.PostgreSQL, dbcapabilities.MongoDB, "json", "object", UnifiedTypeJSON, ConversionTypeDirect, false, false, "PostgreSQL JSON maps to MongoDB Object/Document")

	// Add more database pairs as needed...
	// MySQL -> PostgreSQL, Oracle -> MySQL, etc.
}

// addConversionRule adds a conversion rule to the registry
func (tc *TypeConverter) addConversionRule(sourceDB, targetDB dbcapabilities.DatabaseType, sourceType, targetType string, unifiedType UnifiedDataType, conversionType ConversionType, isLossy, requiresInput bool, notes string) {
	key := DataTypeConversionKey{
		SourceDB:   sourceDB,
		TargetDB:   targetDB,
		SourceType: strings.ToLower(strings.TrimSpace(sourceType)),
	}

	rule := DataTypeConversionRule{
		SourceDatabase:    sourceDB,
		TargetDatabase:    targetDB,
		SourceType:        sourceType,
		TargetType:        targetType,
		UnifiedType:       unifiedType,
		ConversionType:    conversionType,
		IsLossyConversion: isLossy,
		RequiresUserInput: requiresInput,
		ConversionNotes:   notes,
	}

	tc.registry[key] = rule
}

// mapNativeTypeToUnified maps a database-specific type to unified type
func (tc *TypeConverter) mapNativeTypeToUnified(dbType dbcapabilities.DatabaseType, nativeType string) UnifiedDataType {
	normalizedType := strings.ToLower(strings.TrimSpace(nativeType))

	switch dbType {
	case dbcapabilities.MongoDB:
		switch normalizedType {
		case "int32":
			return UnifiedTypeInt32
		case "int64":
			return UnifiedTypeInt64
		case "string":
			return UnifiedTypeString
		case "boolean":
			return UnifiedTypeBoolean
		case "date":
			return UnifiedTypeTimestamp
		case "objectid":
			return UnifiedTypeString
		case "double":
			return UnifiedTypeFloat64
		case "object", "document":
			return UnifiedTypeJSON
		case "array":
			return UnifiedTypeArray
		}
	case dbcapabilities.PostgreSQL:
		switch normalizedType {
		case "integer", "int4":
			return UnifiedTypeInt32
		case "bigint", "int8":
			return UnifiedTypeInt64
		case "varchar", "text":
			return UnifiedTypeString
		case "boolean":
			return UnifiedTypeBoolean
		case "timestamp", "timestamptz":
			return UnifiedTypeTimestamp
		case "uuid":
			return UnifiedTypeUUID
		case "double precision", "float8":
			return UnifiedTypeFloat64
		case "jsonb", "json":
			return UnifiedTypeJSON
		}
	case dbcapabilities.MySQL:
		switch normalizedType {
		case "int", "integer":
			return UnifiedTypeInt32
		case "bigint":
			return UnifiedTypeInt64
		case "varchar", "text":
			return UnifiedTypeString
		case "boolean", "bool":
			return UnifiedTypeBoolean
		case "datetime", "timestamp":
			return UnifiedTypeTimestamp
		case "double":
			return UnifiedTypeFloat64
		case "json":
			return UnifiedTypeJSON
		}
	}
	return UnifiedTypeUnknown
}

// mapUnifiedTypeToNative maps a unified type to database-specific type
func (tc *TypeConverter) mapUnifiedTypeToNative(dbType dbcapabilities.DatabaseType, unifiedType UnifiedDataType) string {
	switch dbType {
	case dbcapabilities.MongoDB:
		switch unifiedType {
		case UnifiedTypeInt32:
			return "int32"
		case UnifiedTypeInt64:
			return "int64"
		case UnifiedTypeString:
			return "string"
		case UnifiedTypeBoolean:
			return "boolean"
		case UnifiedTypeTimestamp:
			return "date"
		case UnifiedTypeFloat64:
			return "double"
		case UnifiedTypeJSON:
			return "object"
		case UnifiedTypeArray:
			return "array"
		}
	case dbcapabilities.PostgreSQL:
		switch unifiedType {
		case UnifiedTypeInt32:
			return "integer"
		case UnifiedTypeInt64:
			return "bigint"
		case UnifiedTypeString:
			return "text"
		case UnifiedTypeBoolean:
			return "boolean"
		case UnifiedTypeTimestamp:
			return "timestamp"
		case UnifiedTypeUUID:
			return "uuid"
		case UnifiedTypeFloat64:
			return "double precision"
		case UnifiedTypeJSON:
			return "jsonb"
		case UnifiedTypeArray:
			return "jsonb"
		}
	case dbcapabilities.MySQL:
		switch unifiedType {
		case UnifiedTypeInt32:
			return "int"
		case UnifiedTypeInt64:
			return "bigint"
		case UnifiedTypeString:
			return "text"
		case UnifiedTypeBoolean:
			return "boolean"
		case UnifiedTypeTimestamp:
			return "datetime"
		case UnifiedTypeFloat64:
			return "double"
		case UnifiedTypeJSON:
			return "json"
		case UnifiedTypeArray:
			return "json"
		}
	}
	return ""
}

// GetSupportedConversions returns all supported type conversions for a database pair
func (tc *TypeConverter) GetSupportedConversions(sourceDB, targetDB dbcapabilities.DatabaseType) []DataTypeConversionRule {
	var conversions []DataTypeConversionRule

	for key, rule := range tc.registry {
		if key.SourceDB == sourceDB && key.TargetDB == targetDB {
			conversions = append(conversions, rule)
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
