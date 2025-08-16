package unifiedmodel

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// CustomTypeCategory represents the category of a custom type
type CustomTypeCategory string

const (
	CustomTypeCategoryEnum      CustomTypeCategory = "enum"
	CustomTypeCategoryComposite CustomTypeCategory = "composite"
	CustomTypeCategoryDomain    CustomTypeCategory = "domain"
	CustomTypeCategoryArray     CustomTypeCategory = "array"
	CustomTypeCategoryRange     CustomTypeCategory = "range"
	CustomTypeCategorySpatial   CustomTypeCategory = "spatial"
	CustomTypeCategoryTemporal  CustomTypeCategory = "temporal"
	CustomTypeCategoryBinary    CustomTypeCategory = "binary"
	CustomTypeCategoryOther     CustomTypeCategory = "other"
)

// CustomTypeStrategy defines how a custom type is converted
type CustomTypeStrategy string

const (
	CustomTypeStrategyDirect      CustomTypeStrategy = "direct"           // Native support in target
	CustomTypeStrategyEmulate     CustomTypeStrategy = "emulate"          // Emulate with constraints/triggers
	CustomTypeStrategyString      CustomTypeStrategy = "string"           // Convert to string type
	CustomTypeStrategyJSON        CustomTypeStrategy = "json"             // Convert to JSON/document type
	CustomTypeStrategyTable       CustomTypeStrategy = "lookup_table"     // Convert to a separate lookup table
	CustomTypeStrategyFlatten     CustomTypeStrategy = "flatten"          // Flatten composite types to multiple columns
	CustomTypeStrategyCheck       CustomTypeStrategy = "check_constraint" // Apply base type with check constraints
	CustomTypeStrategyUnsupported CustomTypeStrategy = "unsupported"      // Cannot be converted
)

// CustomTypeConversionResult holds the result of a custom type conversion
type CustomTypeConversionResult struct {
	SourceType           Type               `json:"source_type"`
	ConvertedTypes       []Type             `json:"converted_types"`
	ConvertedColumns     []Column           `json:"converted_columns,omitempty"`
	ConvertedConstraints []Constraint       `json:"converted_constraints,omitempty"`
	AdditionalObjects    map[string]any     `json:"additional_objects,omitempty"` // e.g., lookup tables
	ConversionStrategy   CustomTypeStrategy `json:"conversion_strategy"`
	UsedFallbackStrategy bool               `json:"used_fallback_strategy"`
	RequiresUserInput    bool               `json:"requires_user_input"`
	ConversionNotes      string             `json:"conversion_notes,omitempty"`
	Warnings             []string           `json:"warnings,omitempty"`
}

// CustomTypeValidation represents the result of custom type validation
type CustomTypeValidation struct {
	IsSupported       bool                        `json:"is_supported"`
	ConversionResult  *CustomTypeConversionResult `json:"conversion_result,omitempty"`
	Strategy          CustomTypeStrategy          `json:"strategy"`
	RequiresUserInput bool                        `json:"requires_user_input"`
	Warnings          []string                    `json:"warnings,omitempty"`
}

// DatabaseTypeMetadata contains comprehensive metadata about a database's type system
type DatabaseTypeMetadata struct {
	DatabaseType      dbcapabilities.DatabaseType  `json:"database_type"`
	PrimitiveTypes    map[string]PrimitiveTypeInfo `json:"primitive_types"`
	CustomTypeSupport CustomTypeSupportInfo        `json:"custom_type_support"`
	TypeConversions   TypeConversionCapabilities   `json:"type_conversions"`
	NamingConventions NamingConventions            `json:"naming_conventions"`
	ConstraintSupport ConstraintSupportInfo        `json:"constraint_support"`
	DefaultMappings   map[UnifiedDataType]string   `json:"default_mappings"`
}

// PrimitiveTypeInfo describes a primitive data type in a specific database
type PrimitiveTypeInfo struct {
	NativeName       string            `json:"native_name"`
	UnifiedType      UnifiedDataType   `json:"unified_type"`
	HasLength        bool              `json:"has_length"`
	HasPrecision     bool              `json:"has_precision"`
	HasScale         bool              `json:"has_scale"`
	SupportsUnsigned bool              `json:"supports_unsigned"`
	SupportsNull     bool              `json:"supports_null"`
	DefaultLength    *int64            `json:"default_length,omitempty"`
	MaxLength        *int64            `json:"max_length,omitempty"`
	DefaultPrecision *int64            `json:"default_precision,omitempty"`
	MaxPrecision     *int64            `json:"max_precision,omitempty"`
	DefaultScale     *int64            `json:"default_scale,omitempty"`
	MaxScale         *int64            `json:"max_scale,omitempty"`
	Aliases          []string          `json:"aliases,omitempty"`
	ConversionHints  map[string]string `json:"conversion_hints,omitempty"`
}

// CustomTypeSupportInfo describes what custom types a database supports
type CustomTypeSupportInfo struct {
	SupportsEnum            bool                     `json:"supports_enum"`
	SupportsComposite       bool                     `json:"supports_composite"`
	SupportsDomain          bool                     `json:"supports_domain"`
	SupportsArray           bool                     `json:"supports_array"`
	SupportsRange           bool                     `json:"supports_range"`
	SupportsJSON            bool                     `json:"supports_json"`
	SupportsXML             bool                     `json:"supports_xml"`
	SupportsSpatial         bool                     `json:"supports_spatial"`
	EnumImplementation      CustomTypeImplementation `json:"enum_implementation"`
	CompositeImplementation CustomTypeImplementation `json:"composite_implementation"`
	DomainImplementation    CustomTypeImplementation `json:"domain_implementation"`
	ArrayImplementation     CustomTypeImplementation `json:"array_implementation"`
	JSONImplementation      CustomTypeImplementation `json:"json_implementation"`
}

// CustomTypeImplementation describes how a database implements custom types
type CustomTypeImplementation struct {
	IsNative          bool     `json:"is_native"`
	RequiresEmulation bool     `json:"requires_emulation"`
	EmulationStrategy string   `json:"emulation_strategy,omitempty"`
	Syntax            string   `json:"syntax,omitempty"`
	Limitations       []string `json:"limitations,omitempty"`
	Examples          []string `json:"examples,omitempty"`
}

// TypeConversionCapabilities describes how types can be converted
type TypeConversionCapabilities struct {
	CanCastBetweenNumeric bool              `json:"can_cast_between_numeric"`
	CanCastToString       bool              `json:"can_cast_to_string"`
	CanCastFromString     bool              `json:"can_cast_from_string"`
	SupportsImplicitCast  bool              `json:"supports_implicit_cast"`
	SupportsExplicitCast  bool              `json:"supports_explicit_cast"`
	CastFunctions         map[string]string `json:"cast_functions,omitempty"`
	ConversionFunctions   map[string]string `json:"conversion_functions,omitempty"`
}

// NamingConventions describes database naming rules
type NamingConventions struct {
	CaseSensitive       bool     `json:"case_sensitive"`
	MaxIdentifierLength int      `json:"max_identifier_length"`
	AllowedCharacters   string   `json:"allowed_characters"`
	ReservedWords       []string `json:"reserved_words,omitempty"`
	QuoteCharacter      string   `json:"quote_character"`
	PreferredCase       string   `json:"preferred_case"` // "lower", "upper", "mixed"
}

// ConstraintSupportInfo describes constraint support
type ConstraintSupportInfo struct {
	SupportsPrimaryKey    bool `json:"supports_primary_key"`
	SupportsForeignKey    bool `json:"supports_foreign_key"`
	SupportsUnique        bool `json:"supports_unique"`
	SupportsCheck         bool `json:"supports_check"`
	SupportsNotNull       bool `json:"supports_not_null"`
	SupportsDefault       bool `json:"supports_default"`
	SupportsAutoIncrement bool `json:"supports_auto_increment"`
}

// DatabaseMetadataRegistry holds metadata for all supported databases
var DatabaseMetadataRegistry = make(map[dbcapabilities.DatabaseType]DatabaseTypeMetadata)

// ScalableTypeConverter provides database-agnostic type conversion
type ScalableTypeConverter struct {
	metadata map[dbcapabilities.DatabaseType]DatabaseTypeMetadata
}

// NewScalableTypeConverter creates a new scalable type converter
func NewScalableTypeConverter() *ScalableTypeConverter {
	converter := &ScalableTypeConverter{
		metadata: make(map[dbcapabilities.DatabaseType]DatabaseTypeMetadata),
	}
	converter.initializeMetadata()
	return converter
}

// ConvertPrimitiveType converts a primitive type between any two databases
func (stc *ScalableTypeConverter) ConvertPrimitiveType(sourceDB, targetDB dbcapabilities.DatabaseType, sourceType string) (*DataTypeConversionResult, error) {
	// Get source database metadata
	sourceMetadata, sourceExists := stc.metadata[sourceDB]
	if !sourceExists {
		return nil, fmt.Errorf("no metadata found for source database %s", sourceDB)
	}

	// Get target database metadata
	targetMetadata, targetExists := stc.metadata[targetDB]
	if !targetExists {
		return nil, fmt.Errorf("no metadata found for target database %s", targetDB)
	}

	// Normalize source type name
	normalizedSourceType := stc.normalizeTypeName(sourceType)

	// Find source type info (including aliases)
	sourceTypeInfo, found := stc.findTypeInfo(normalizedSourceType, sourceMetadata)
	if !found {
		return nil, fmt.Errorf("unknown type %s in database %s", sourceType, sourceDB)
	}

	// Find target type for the unified type
	targetTypeName, found := stc.findTargetType(sourceTypeInfo.UnifiedType, targetMetadata)
	if !found {
		return nil, fmt.Errorf("no equivalent type for %s in database %s", sourceTypeInfo.UnifiedType, targetDB)
	}

	// Create conversion result
	result := &DataTypeConversionResult{
		OriginalType:      sourceType,
		ConvertedType:     targetTypeName,
		UnifiedType:       sourceTypeInfo.UnifiedType,
		ConversionType:    stc.determineConversionType(sourceTypeInfo, targetMetadata, targetTypeName),
		IsLossyConversion: stc.isLossyConversion(sourceTypeInfo, targetMetadata, targetTypeName),
		ConversionNotes:   fmt.Sprintf("Converted %s.%s to %s.%s via unified type %s", sourceDB, sourceType, targetDB, targetTypeName, sourceTypeInfo.UnifiedType),
	}

	// Add warnings if necessary
	result.Warnings = stc.generateConversionWarnings(sourceTypeInfo, targetMetadata, targetTypeName)

	return result, nil
}

// ConvertCustomTypeScalable converts custom types between any two databases
func (stc *ScalableTypeConverter) ConvertCustomTypeScalable(customType Type, sourceDB, targetDB dbcapabilities.DatabaseType) (*CustomTypeConversionResult, error) {
	// Get source and target metadata
	sourceMetadata, sourceExists := stc.metadata[sourceDB]
	if !sourceExists {
		return nil, fmt.Errorf("no metadata found for source database %s", sourceDB)
	}

	targetMetadata, targetExists := stc.metadata[targetDB]
	if !targetExists {
		return nil, fmt.Errorf("no metadata found for target database %s", targetDB)
	}

	// Parse custom type category
	category := CustomTypeCategory(customType.Category)

	// Determine conversion strategy based on target database capabilities
	strategy, fallbackStrategy := stc.determineCustomTypeStrategy(category, sourceMetadata, targetMetadata)

	// Apply conversion strategy
	result, err := stc.applyCustomTypeStrategy(customType, category, strategy, sourceDB, targetDB, sourceMetadata, targetMetadata)
	if err != nil && fallbackStrategy != "" {
		// Try fallback strategy
		result, err = stc.applyCustomTypeStrategy(customType, category, fallbackStrategy, sourceDB, targetDB, sourceMetadata, targetMetadata)
		if err != nil {
			return nil, fmt.Errorf("both primary and fallback strategies failed: %w", err)
		}
		result.UsedFallbackStrategy = true
	}

	return result, err
}

// Helper methods

// normalizeTypeName normalizes type names for comparison
func (stc *ScalableTypeConverter) normalizeTypeName(typeName string) string {
	// Remove whitespace and convert to lowercase
	normalized := strings.ToLower(strings.TrimSpace(typeName))

	// Remove common modifiers that don't affect the base type
	normalized = strings.ReplaceAll(normalized, " unsigned", "")
	normalized = strings.ReplaceAll(normalized, " signed", "")

	return normalized
}

// findTypeInfo finds type information including aliases
func (stc *ScalableTypeConverter) findTypeInfo(typeName string, metadata DatabaseTypeMetadata) (PrimitiveTypeInfo, bool) {
	// Direct match
	if typeInfo, exists := metadata.PrimitiveTypes[typeName]; exists {
		return typeInfo, true
	}

	// Check aliases
	for _, typeInfo := range metadata.PrimitiveTypes {
		for _, alias := range typeInfo.Aliases {
			if strings.ToLower(alias) == typeName {
				return typeInfo, true
			}
		}
	}

	return PrimitiveTypeInfo{}, false
}

// findTargetType finds the best target type for a unified type
func (stc *ScalableTypeConverter) findTargetType(unifiedType UnifiedDataType, targetMetadata DatabaseTypeMetadata) (string, bool) {
	// Check default mappings first
	if targetType, exists := targetMetadata.DefaultMappings[unifiedType]; exists {
		return targetType, true
	}

	// Find any type that maps to this unified type
	for nativeName, typeInfo := range targetMetadata.PrimitiveTypes {
		if typeInfo.UnifiedType == unifiedType {
			return nativeName, true
		}
	}

	return "", false
}

// determineConversionType determines the type of conversion needed
func (stc *ScalableTypeConverter) determineConversionType(sourceType PrimitiveTypeInfo, targetMetadata DatabaseTypeMetadata, targetTypeName string) ConversionType {
	targetType, exists := targetMetadata.PrimitiveTypes[targetTypeName]
	if !exists {
		return ConversionTypeTransform
	}

	// Same unified type = direct conversion
	if sourceType.UnifiedType == targetType.UnifiedType {
		return ConversionTypeDirect
	}

	// Different unified types = transform
	return ConversionTypeTransform
}

// isLossyConversion determines if conversion might lose data
func (stc *ScalableTypeConverter) isLossyConversion(sourceType PrimitiveTypeInfo, targetMetadata DatabaseTypeMetadata, targetTypeName string) bool {
	targetType, exists := targetMetadata.PrimitiveTypes[targetTypeName]
	if !exists {
		return true // Unknown target type = potentially lossy
	}

	// Check precision/scale loss
	if sourceType.HasPrecision && targetType.HasPrecision {
		if sourceType.MaxPrecision != nil && targetType.MaxPrecision != nil {
			if *sourceType.MaxPrecision > *targetType.MaxPrecision {
				return true
			}
		}
	}

	// Check length loss
	if sourceType.HasLength && targetType.HasLength {
		if sourceType.MaxLength != nil && targetType.MaxLength != nil {
			if *sourceType.MaxLength > *targetType.MaxLength {
				return true
			}
		}
	}

	return false
}

// generateConversionWarnings generates warnings for potentially problematic conversions
func (stc *ScalableTypeConverter) generateConversionWarnings(sourceType PrimitiveTypeInfo, targetMetadata DatabaseTypeMetadata, targetTypeName string) []string {
	var warnings []string

	targetType, exists := targetMetadata.PrimitiveTypes[targetTypeName]
	if !exists {
		warnings = append(warnings, "Target type information not available")
		return warnings
	}

	// Check for precision loss
	if sourceType.HasPrecision && targetType.HasPrecision {
		if sourceType.MaxPrecision != nil && targetType.MaxPrecision != nil {
			if *sourceType.MaxPrecision > *targetType.MaxPrecision {
				warnings = append(warnings, fmt.Sprintf("Precision may be reduced from %d to %d", *sourceType.MaxPrecision, *targetType.MaxPrecision))
			}
		}
	}

	// Check for length loss
	if sourceType.HasLength && targetType.HasLength {
		if sourceType.MaxLength != nil && targetType.MaxLength != nil {
			if *sourceType.MaxLength > *targetType.MaxLength {
				warnings = append(warnings, fmt.Sprintf("Length may be reduced from %d to %d", *sourceType.MaxLength, *targetType.MaxLength))
			}
		}
	}

	// Check for unsigned support
	if sourceType.SupportsUnsigned && !targetType.SupportsUnsigned {
		warnings = append(warnings, "Target type does not support unsigned values")
	}

	return warnings
}

// determineCustomTypeStrategy determines the best strategy for custom type conversion
func (stc *ScalableTypeConverter) determineCustomTypeStrategy(category CustomTypeCategory, sourceMetadata, targetMetadata DatabaseTypeMetadata) (CustomTypeStrategy, CustomTypeStrategy) {
	var primaryStrategy, fallbackStrategy CustomTypeStrategy

	switch category {
	case CustomTypeCategoryEnum:
		if targetMetadata.CustomTypeSupport.SupportsEnum && targetMetadata.CustomTypeSupport.EnumImplementation.IsNative {
			primaryStrategy = CustomTypeStrategyDirect
		} else if targetMetadata.ConstraintSupport.SupportsCheck {
			primaryStrategy = CustomTypeStrategyEmulate
		} else {
			primaryStrategy = CustomTypeStrategyString
		}
		fallbackStrategy = CustomTypeStrategyString

	case CustomTypeCategoryComposite:
		if targetMetadata.CustomTypeSupport.SupportsComposite && targetMetadata.CustomTypeSupport.CompositeImplementation.IsNative {
			primaryStrategy = CustomTypeStrategyDirect
		} else if targetMetadata.CustomTypeSupport.SupportsJSON {
			primaryStrategy = CustomTypeStrategyJSON
		} else {
			primaryStrategy = CustomTypeStrategyFlatten
		}
		fallbackStrategy = CustomTypeStrategyFlatten

	case CustomTypeCategoryDomain:
		if targetMetadata.CustomTypeSupport.SupportsDomain && targetMetadata.CustomTypeSupport.DomainImplementation.IsNative {
			primaryStrategy = CustomTypeStrategyDirect
		} else if targetMetadata.ConstraintSupport.SupportsCheck {
			primaryStrategy = CustomTypeStrategyCheck
		} else {
			primaryStrategy = CustomTypeStrategyString
		}
		fallbackStrategy = CustomTypeStrategyString

	case CustomTypeCategoryArray:
		if targetMetadata.CustomTypeSupport.SupportsArray && targetMetadata.CustomTypeSupport.ArrayImplementation.IsNative {
			primaryStrategy = CustomTypeStrategyDirect
		} else if targetMetadata.CustomTypeSupport.SupportsJSON {
			primaryStrategy = CustomTypeStrategyJSON
		} else {
			primaryStrategy = CustomTypeStrategyString
		}
		fallbackStrategy = CustomTypeStrategyJSON

	default:
		if targetMetadata.CustomTypeSupport.SupportsJSON {
			primaryStrategy = CustomTypeStrategyJSON
		} else {
			primaryStrategy = CustomTypeStrategyString
		}
		fallbackStrategy = CustomTypeStrategyString
	}

	return primaryStrategy, fallbackStrategy
}

// applyCustomTypeStrategy applies a specific conversion strategy
func (stc *ScalableTypeConverter) applyCustomTypeStrategy(customType Type, category CustomTypeCategory, strategy CustomTypeStrategy, sourceDB, targetDB dbcapabilities.DatabaseType, sourceMetadata, targetMetadata DatabaseTypeMetadata) (*CustomTypeConversionResult, error) {
	result := &CustomTypeConversionResult{
		SourceType:         customType,
		ConversionStrategy: strategy,
	}

	switch strategy {
	case CustomTypeStrategyDirect:
		return stc.applyDirectCustomStrategy(customType, category, targetMetadata, result)
	case CustomTypeStrategyEmulate:
		return stc.applyEmulateCustomStrategy(customType, category, targetMetadata, result)
	case CustomTypeStrategyJSON:
		return stc.applyJSONCustomStrategy(customType, category, targetMetadata, result)
	case CustomTypeStrategyString:
		return stc.applyStringCustomStrategy(customType, category, targetMetadata, result)
	case CustomTypeStrategyFlatten:
		return stc.applyFlattenCustomStrategy(customType, category, sourceDB, targetDB, result)
	case CustomTypeStrategyCheck:
		return stc.applyCheckCustomStrategy(customType, category, targetMetadata, result)
	case CustomTypeStrategyTable:
		return stc.applyTableCustomStrategy(customType, category, targetMetadata, result)
	default:
		return nil, fmt.Errorf("unsupported conversion strategy: %s", strategy)
	}
}

// Strategy implementation methods (simplified for brevity)
func (stc *ScalableTypeConverter) applyDirectCustomStrategy(customType Type, category CustomTypeCategory, targetMetadata DatabaseTypeMetadata, result *CustomTypeConversionResult) (*CustomTypeConversionResult, error) {
	// Create equivalent type in target database
	result.ConvertedTypes = []Type{customType}
	result.ConversionNotes = fmt.Sprintf("Direct %s conversion", category)
	return result, nil
}

func (stc *ScalableTypeConverter) applyEmulateCustomStrategy(customType Type, category CustomTypeCategory, targetMetadata DatabaseTypeMetadata, result *CustomTypeConversionResult) (*CustomTypeConversionResult, error) {
	if category == CustomTypeCategoryEnum {
		// Get string type for target database
		stringType, found := stc.findTargetType(UnifiedTypeString, targetMetadata)
		if !found {
			stringType = "text" // fallback
		}

		// Create check constraint if enum values are available
		if values, exists := customType.Definition["values"]; exists {
			if valueSlice, ok := values.([]interface{}); ok {
				var enumValues []string
				for _, v := range valueSlice {
					if str, ok := v.(string); ok {
						enumValues = append(enumValues, fmt.Sprintf("'%s'", strings.ReplaceAll(str, "'", "''")))
					}
				}

				if len(enumValues) > 0 {
					checkConstraint := fmt.Sprintf("CHECK (%s IN (%s))", customType.Name, strings.Join(enumValues, ", "))
					constraint := Constraint{
						Name:       fmt.Sprintf("chk_%s_enum", customType.Name),
						Type:       ConstraintTypeCheck,
						Expression: checkConstraint,
					}
					result.ConvertedConstraints = []Constraint{constraint}
				}
			}
		}

		result.ConversionNotes = fmt.Sprintf("Enum emulated as %s with check constraint", stringType)
		result.Warnings = append(result.Warnings, "Enum emulated - native enum features not available")
	}
	return result, nil
}

func (stc *ScalableTypeConverter) applyJSONCustomStrategy(customType Type, category CustomTypeCategory, targetMetadata DatabaseTypeMetadata, result *CustomTypeConversionResult) (*CustomTypeConversionResult, error) {
	// Find JSON type for target database
	jsonType, found := stc.findTargetType(UnifiedTypeJSON, targetMetadata)
	if !found {
		jsonType = "text" // fallback to text
	}

	result.ConversionNotes = fmt.Sprintf("Custom type %s converted to %s", category, jsonType)
	result.Warnings = append(result.Warnings, "Custom type converted to JSON - type safety may be reduced")
	return result, nil
}

func (stc *ScalableTypeConverter) applyStringCustomStrategy(customType Type, category CustomTypeCategory, targetMetadata DatabaseTypeMetadata, result *CustomTypeConversionResult) (*CustomTypeConversionResult, error) {
	// Find string type for target database
	stringType, found := stc.findTargetType(UnifiedTypeString, targetMetadata)
	if !found {
		stringType = "text" // fallback
	}

	result.ConversionNotes = fmt.Sprintf("Custom type %s converted to %s", category, stringType)
	result.Warnings = append(result.Warnings, "Custom type converted to string - type safety lost")
	return result, nil
}

func (stc *ScalableTypeConverter) applyFlattenCustomStrategy(customType Type, category CustomTypeCategory, sourceDB, targetDB dbcapabilities.DatabaseType, result *CustomTypeConversionResult) (*CustomTypeConversionResult, error) {
	if category != CustomTypeCategoryComposite {
		return nil, fmt.Errorf("flatten strategy only applicable to composite types")
	}

	// Extract fields and convert each one
	if fields, exists := customType.Definition["fields"]; exists {
		if fieldsMap, ok := fields.(map[string]interface{}); ok {
			var convertedColumns []Column
			for fieldName, fieldDef := range fieldsMap {
				if fieldDefMap, ok := fieldDef.(map[string]interface{}); ok {
					dataType := "text" // default
					if dt, ok := fieldDefMap["data_type"].(string); ok {
						// Convert the field's data type
						convertedType, err := stc.ConvertPrimitiveType(sourceDB, targetDB, dt)
						if err == nil {
							dataType = convertedType.ConvertedType
						}
					}

					column := Column{
						Name:     fmt.Sprintf("%s_%s", customType.Name, fieldName),
						DataType: dataType,
						Nullable: true, // default for flattened fields
					}
					convertedColumns = append(convertedColumns, column)
				}
			}
			result.ConvertedColumns = convertedColumns
		}
	}

	result.ConversionNotes = fmt.Sprintf("Composite type flattened into %d columns", len(result.ConvertedColumns))
	result.Warnings = append(result.Warnings, "Composite type flattened - original structure lost")
	return result, nil
}

func (stc *ScalableTypeConverter) applyCheckCustomStrategy(customType Type, category CustomTypeCategory, targetMetadata DatabaseTypeMetadata, result *CustomTypeConversionResult) (*CustomTypeConversionResult, error) {
	if category == CustomTypeCategoryDomain {
		// Convert domain to base type with constraints
		if baseType, exists := customType.Definition["base_type"]; exists {
			if _, ok := baseType.(string); ok {
				// Find target type for base type
				targetType, found := stc.findTargetType(UnifiedTypeString, targetMetadata) // simplified
				if !found {
					targetType = "text"
				}

				// Create constraints if available
				if constraints, exists := customType.Definition["constraints"]; exists {
					if constraintSlice, ok := constraints.([]interface{}); ok {
						var convertedConstraints []Constraint
						for i, c := range constraintSlice {
							if constraintStr, ok := c.(string); ok {
								constraint := Constraint{
									Name:       fmt.Sprintf("chk_%s_%d", customType.Name, i+1),
									Type:       ConstraintTypeCheck,
									Expression: constraintStr,
								}
								convertedConstraints = append(convertedConstraints, constraint)
							}
						}
						result.ConvertedConstraints = convertedConstraints
					}
				}

				result.ConversionNotes = fmt.Sprintf("Domain type converted to %s with %d constraints", targetType, len(result.ConvertedConstraints))
			}
		}
	}
	return result, nil
}

func (stc *ScalableTypeConverter) applyTableCustomStrategy(customType Type, category CustomTypeCategory, targetMetadata DatabaseTypeMetadata, result *CustomTypeConversionResult) (*CustomTypeConversionResult, error) {
	// Create lookup table (simplified implementation)
	result.ConversionNotes = "Custom type converted to lookup table"
	result.Warnings = append(result.Warnings, "Custom type converted to lookup table - requires additional setup")
	return result, nil
}

// initializeMetadata initializes metadata for all supported databases
func (stc *ScalableTypeConverter) initializeMetadata() {
	// Use the comprehensive initialization from the registry
	stc.initializeAllDatabaseMetadata()
}

// Database-specific metadata creation methods are now in database_metadata_registry.go
