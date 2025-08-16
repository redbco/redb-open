package unifiedmodel

import (
	"fmt"
	"time"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// ConversionEngine executes conversions using dynamic generation
type ConversionEngine struct {
	featureMatrix map[dbcapabilities.DatabaseType]DatabaseFeatureSupport
	utils         *ConversionUtils
	typeConverter *TypeConverter
}

// NewConversionEngine creates a new conversion engine
func NewConversionEngine() *ConversionEngine {
	engine := &ConversionEngine{
		featureMatrix: DatabaseFeatureRegistry,
		typeConverter: NewTypeConverter(),
	}
	// Create utils with a reference to avoid circular dependency
	engine.utils = &ConversionUtils{engine: engine}
	return engine
}

// ConversionRequest defines parameters for a schema conversion
type ConversionRequest struct {
	SourceSchema    *UnifiedModel               `json:"source_schema"`
	SourceDatabase  dbcapabilities.DatabaseType `json:"source_database"`
	TargetDatabase  dbcapabilities.DatabaseType `json:"target_database"`
	Enrichment      *UnifiedModelEnrichment     `json:"enrichment,omitempty"`
	SampleData      *UnifiedModelSampleData     `json:"sample_data,omitempty"`
	UserPreferences ConversionPreferences       `json:"user_preferences"`
	ConversionMode  ConversionMode              `json:"conversion_mode"`
	RequestID       string                      `json:"request_id"`
	RequestedBy     string                      `json:"requested_by"`
	RequestedAt     time.Time                   `json:"requested_at"`
}

// ConversionPreferences defines user preferences for conversion
type ConversionPreferences struct {
	PreferredStrategy      ConversionStrategy `json:"preferred_strategy,omitempty"`
	AcceptDataLoss         bool               `json:"accept_data_loss"`
	OptimizeForPerformance bool               `json:"optimize_for_performance"`
	OptimizeForStorage     bool               `json:"optimize_for_storage"`
	PreserveRelationships  bool               `json:"preserve_relationships"`
	IncludeMetadata        bool               `json:"include_metadata"`
	CustomMappings         map[string]string  `json:"custom_mappings,omitempty"`
	ExcludeObjects         []ObjectType       `json:"exclude_objects,omitempty"`
}

// ConversionMode defines how the conversion should be executed
type ConversionMode string

const (
	ConversionModeAutomatic    ConversionMode = "automatic"     // Fully automated
	ConversionModeInteractive  ConversionMode = "interactive"   // Prompt for decisions
	ConversionModeAnalysisOnly ConversionMode = "analysis_only" // Just analyze, don't convert
	ConversionModeDryRun       ConversionMode = "dry_run"       // Simulate conversion
)

// ConversionResult contains the results of a schema conversion
type ConversionResult struct {
	TargetSchema        *UnifiedModel         `json:"target_schema,omitempty"`
	ConversionReport    ConversionReport      `json:"conversion_report"`
	UserDecisions       []PendingUserDecision `json:"user_decisions,omitempty"`
	Warnings            []ConversionWarning   `json:"warnings"`
	UnsupportedFeatures []UnsupportedFeature  `json:"unsupported_features"`
	ProcessingTime      time.Duration         `json:"processing_time"`
	Success             bool                  `json:"success"`
	ErrorMessage        string                `json:"error_message,omitempty"`
}

// ConversionReport provides detailed information about the conversion
type ConversionReport struct {
	RequestID            string                         `json:"request_id"`
	SourceDatabase       dbcapabilities.DatabaseType    `json:"source_database"`
	TargetDatabase       dbcapabilities.DatabaseType    `json:"target_database"`
	ConversionComplexity ConversionComplexity           `json:"conversion_complexity"`
	ObjectsProcessed     int                            `json:"objects_processed"`
	ObjectsConverted     int                            `json:"objects_converted"`
	ObjectsSkipped       int                            `json:"objects_skipped"`
	ObjectsDropped       int                            `json:"objects_dropped"`
	ConversionSummary    map[ObjectType]ConversionStats `json:"conversion_summary"`
	StrategiesUsed       []ConversionStrategy           `json:"strategies_used"`
	EnrichmentUsed       bool                           `json:"enrichment_used"`
	SampleDataUsed       bool                           `json:"sample_data_used"`
	ProcessedAt          time.Time                      `json:"processed_at"`
}

// ConversionStats provides statistics for object type conversions
type ConversionStats struct {
	SourceCount    int     `json:"source_count"`
	ConvertedCount int     `json:"converted_count"`
	SkippedCount   int     `json:"skipped_count"`
	DroppedCount   int     `json:"dropped_count"`
	SuccessRate    float64 `json:"success_rate"`
}

// PendingUserDecision represents a decision that needs user input
type PendingUserDecision struct {
	DecisionID   string       `json:"decision_id"`
	ObjectType   ObjectType   `json:"object_type"`
	ObjectName   string       `json:"object_name"`
	UserDecision UserDecision `json:"user_decision"`
	Context      string       `json:"context"`
}

// UnsupportedFeature represents a feature that cannot be converted
type UnsupportedFeature struct {
	FeatureType   string     `json:"feature_type"`
	ObjectType    ObjectType `json:"object_type"`
	ObjectName    string     `json:"object_name"`
	Description   string     `json:"description"`
	Alternatives  []string   `json:"alternatives,omitempty"`
	WorkaroundURL string     `json:"workaround_url,omitempty"`
}

// Convert performs a schema conversion
func (ce *ConversionEngine) Convert(request ConversionRequest) (*ConversionResult, error) {
	startTime := time.Now()

	result := &ConversionResult{
		ConversionReport: ConversionReport{
			RequestID:      request.RequestID,
			SourceDatabase: request.SourceDatabase,
			TargetDatabase: request.TargetDatabase,
			ProcessedAt:    startTime,
		},
		Warnings:            make([]ConversionWarning, 0),
		UnsupportedFeatures: make([]UnsupportedFeature, 0),
		UserDecisions:       make([]PendingUserDecision, 0),
	}

	// Validate conversion request
	if err := ce.validateRequest(request); err != nil {
		result.Success = false
		result.ErrorMessage = err.Error()
		result.ProcessingTime = time.Since(startTime)
		return result, err
	}

	// Get conversion matrix
	conversionMatrix, exists := ce.getConversionMatrix(request.SourceDatabase, request.TargetDatabase)
	if !exists {
		err := fmt.Errorf("conversion from %s to %s is not supported", request.SourceDatabase, request.TargetDatabase)
		result.Success = false
		result.ErrorMessage = err.Error()
		result.ProcessingTime = time.Since(startTime)
		return result, err
	}

	result.ConversionReport.ConversionComplexity = conversionMatrix.ConversionComplexity

	// Check if conversion is possible
	if conversionMatrix.ConversionComplexity == ConversionComplexityImpossible {
		err := fmt.Errorf("conversion complexity is marked as impossible")
		result.Success = false
		result.ErrorMessage = err.Error()
		result.ProcessingTime = time.Since(startTime)
		return result, err
	}

	// Analysis-only mode
	if request.ConversionMode == ConversionModeAnalysisOnly {
		return ce.analyzeConversion(request, conversionMatrix, result)
	}

	// Perform actual conversion
	targetSchema, err := ce.performConversion(request, conversionMatrix, result)
	if err != nil {
		result.Success = false
		result.ErrorMessage = err.Error()
		result.ProcessingTime = time.Since(startTime)
		return result, err
	}

	result.TargetSchema = targetSchema
	result.Success = true
	result.ProcessingTime = time.Since(startTime)

	return result, nil
}

// AnalyzeConversion analyzes conversion feasibility without performing it
func (ce *ConversionEngine) AnalyzeConversion(request ConversionRequest) (*ConversionResult, error) {
	request.ConversionMode = ConversionModeAnalysisOnly
	return ce.Convert(request)
}

// validateRequest validates the conversion request
func (ce *ConversionEngine) validateRequest(request ConversionRequest) error {
	if request.SourceSchema == nil {
		return fmt.Errorf("source schema is required")
	}

	if request.SourceDatabase == "" {
		return fmt.Errorf("source database type is required")
	}

	if request.TargetDatabase == "" {
		return fmt.Errorf("target database type is required")
	}

	if request.SourceDatabase == request.TargetDatabase {
		return fmt.Errorf("source and target databases cannot be the same")
	}

	return nil
}

// getConversionMatrix retrieves conversion matrix for database pair using dynamic generation
func (ce *ConversionEngine) getConversionMatrix(source, target dbcapabilities.DatabaseType) (ConversionMatrix, bool) {
	matrix, err := ce.utils.GenerateConversionMatrix(source, target)
	if err != nil {
		return ConversionMatrix{}, false
	}
	return *matrix, true
}

// analyzeConversion performs conversion analysis without actual conversion
func (ce *ConversionEngine) analyzeConversion(request ConversionRequest, matrix ConversionMatrix, result *ConversionResult) (*ConversionResult, error) {
	// Count objects in source schema
	objectCounts := CountObjects(request.SourceSchema)

	// Analyze each object type
	conversionSummary := make(map[ObjectType]ConversionStats)

	// Analyze tables
	if objectCounts.Tables > 0 {
		stats := ce.analyzeObjectConversion(ObjectTypeTable, objectCounts.Tables, matrix, request)
		conversionSummary[ObjectTypeTable] = stats
	}

	// Analyze other object types...
	// (Implementation would continue for all object types)

	result.ConversionReport.ConversionSummary = conversionSummary
	result.ConversionReport.EnrichmentUsed = request.Enrichment != nil
	result.ConversionReport.SampleDataUsed = request.SampleData != nil

	// Check for unsupported features
	result.UnsupportedFeatures = ce.identifyUnsupportedFeatures(request.SourceSchema, matrix)

	// Generate warnings
	result.Warnings = ce.generateWarnings(request.SourceSchema, matrix)

	result.Success = true
	return result, nil
}

// analyzeObjectConversion analyzes conversion for a specific object type
func (ce *ConversionEngine) analyzeObjectConversion(objType ObjectType, count int, matrix ConversionMatrix, request ConversionRequest) ConversionStats {
	rule, exists := matrix.ObjectConversions[objType]
	if !exists {
		return ConversionStats{
			SourceCount:    count,
			ConvertedCount: 0,
			SkippedCount:   0,
			DroppedCount:   count,
			SuccessRate:    0.0,
		}
	}

	switch rule.ConversionType {
	case ConversionTypeDirect, ConversionTypeTransform:
		return ConversionStats{
			SourceCount:    count,
			ConvertedCount: count,
			SkippedCount:   0,
			DroppedCount:   0,
			SuccessRate:    1.0,
		}
	case ConversionTypeEmulate:
		return ConversionStats{
			SourceCount:    count,
			ConvertedCount: count,
			SkippedCount:   0,
			DroppedCount:   0,
			SuccessRate:    0.8, // Emulated conversions have lower success rate
		}
	case ConversionTypeDrop:
		return ConversionStats{
			SourceCount:    count,
			ConvertedCount: 0,
			SkippedCount:   0,
			DroppedCount:   count,
			SuccessRate:    0.0,
		}
	default:
		return ConversionStats{
			SourceCount:    count,
			ConvertedCount: 0,
			SkippedCount:   count,
			DroppedCount:   0,
			SuccessRate:    0.0,
		}
	}
}

// performConversion performs the actual schema conversion
func (ce *ConversionEngine) performConversion(request ConversionRequest, matrix ConversionMatrix, result *ConversionResult) (*UnifiedModel, error) {
	// Create target schema
	targetSchema := &UnifiedModel{
		DatabaseType: request.TargetDatabase,
		Tables:       make(map[string]Table),
		Collections:  make(map[string]Collection),
		Nodes:        make(map[string]Node),
		// Initialize other maps...
	}

	// Convert each object type based on conversion rules
	// This is a simplified implementation - actual implementation would be much more detailed

	for objType, rule := range matrix.ObjectConversions {
		switch objType {
		case ObjectTypeTable:
			err := ce.convertTables(request.SourceSchema, targetSchema, rule, result)
			if err != nil {
				return nil, err
			}
			// Handle other object types...
		}
	}

	// Convert data types for all schema objects (tables, collections, nodes)
	convertedSchema, typeWarnings, err := ce.convertSchemaDataTypes(targetSchema, request.SourceDatabase, request.TargetDatabase)
	if err != nil {
		return nil, fmt.Errorf("failed to convert schema data types: %w", err)
	}

	// Add type conversion warnings to the result
	result.Warnings = append(result.Warnings, typeWarnings...)

	return convertedSchema, nil
}

// convertTables converts table objects based on conversion rule
func (ce *ConversionEngine) convertTables(sourceSchema, targetSchema *UnifiedModel, rule ObjectConversionRule, result *ConversionResult) error {
	for tableName, table := range sourceSchema.Tables {
		switch rule.ConversionType {
		case ConversionTypeDirect:
			// Direct table to table conversion
			targetSchema.Tables[tableName] = table
		case ConversionTypeTransform:
			// Transform table structure
			transformedTable := ce.transformTable(table, rule)
			targetSchema.Tables[tableName] = transformedTable
		case ConversionTypeDrop:
			// Add to unsupported features
			result.UnsupportedFeatures = append(result.UnsupportedFeatures, UnsupportedFeature{
				FeatureType: "table",
				ObjectType:  ObjectTypeTable,
				ObjectName:  tableName,
				Description: "Tables are not supported in target database",
			})
		}
	}

	return nil
}

// transformTable transforms a table based on conversion rules
func (ce *ConversionEngine) transformTable(table Table, rule ObjectConversionRule) Table {
	// Simplified transformation - actual implementation would be much more complex
	return table
}

// identifyUnsupportedFeatures identifies features that cannot be converted
func (ce *ConversionEngine) identifyUnsupportedFeatures(schema *UnifiedModel, matrix ConversionMatrix) []UnsupportedFeature {
	var unsupported []UnsupportedFeature

	// Check each object type against conversion rules
	for objType, rule := range matrix.ObjectConversions {
		if rule.ConversionType == ConversionTypeDrop {
			// Count objects of this type
			count := ce.countObjectsOfType(schema, objType)
			if count > 0 {
				unsupported = append(unsupported, UnsupportedFeature{
					FeatureType: string(objType),
					ObjectType:  objType,
					Description: fmt.Sprintf("%s objects cannot be converted to target database", objType),
				})
			}
		}
	}

	return unsupported
}

// generateWarnings generates conversion warnings
func (ce *ConversionEngine) generateWarnings(schema *UnifiedModel, matrix ConversionMatrix) []ConversionWarning {
	var warnings []ConversionWarning

	// Generate warnings for emulated conversions
	for objType, rule := range matrix.ObjectConversions {
		if rule.ConversionType == ConversionTypeEmulate {
			count := ce.countObjectsOfType(schema, objType)
			if count > 0 {
				warnings = append(warnings, ConversionWarning{
					WarningType: WarningTypeFeatureLoss,
					ObjectType:  objType,
					Message:     fmt.Sprintf("%s objects will be emulated, some functionality may be lost", objType),
					Severity:    "medium",
					Suggestion:  "Review emulated objects after conversion",
				})
			}
		}
	}

	return warnings
}

// convertSchemaDataTypes converts all data types in a schema from source to target database
func (ce *ConversionEngine) convertSchemaDataTypes(schema *UnifiedModel, sourceDB, targetDB dbcapabilities.DatabaseType) (*UnifiedModel, []ConversionWarning, error) {
	convertedSchema := *schema // Create a copy
	var warnings []ConversionWarning

	// Convert table columns
	convertedTables := make(map[string]Table)
	for tableName, table := range schema.Tables {
		convertedTable := table
		convertedColumns := make(map[string]Column)

		for columnName, column := range table.Columns {
			// Use enhanced conversion that handles custom types
			result, err := ce.typeConverter.ConvertDataTypeWithCustomTypes(sourceDB, targetDB, column.DataType, schema)
			if err != nil {
				// Fall back to standard column conversion
				convertedColumn, fallbackErr := ce.typeConverter.ConvertColumn(column, sourceDB, targetDB)
				if fallbackErr != nil {
					warnings = append(warnings, ConversionWarning{
						WarningType: WarningTypeDataTypeLoss,
						ObjectType:  ObjectTypeColumn,
						ObjectName:  fmt.Sprintf("%s.%s", tableName, columnName),
						Message:     fmt.Sprintf("Failed to convert column data type: %s", err.Error()),
						Severity:    "high",
						Suggestion:  "Manual data type mapping may be required",
					})
					// Keep original column if conversion fails
					convertedColumns[columnName] = column
				} else {
					convertedColumns[columnName] = convertedColumn
				}
			} else {
				// Apply the conversion result to the column
				convertedColumn := column
				convertedColumn.DataType = result.ConvertedType

				// Add conversion metadata
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

				convertedColumns[columnName] = convertedColumn

				// Add warning if conversion is lossy
				if result.IsLossyConversion {
					warnings = append(warnings, ConversionWarning{
						WarningType: WarningTypeDataTypeLoss,
						ObjectType:  ObjectTypeColumn,
						ObjectName:  fmt.Sprintf("%s.%s", tableName, columnName),
						Message:     fmt.Sprintf("Lossy data type conversion: %s -> %s", column.DataType, convertedColumn.DataType),
						Severity:    "medium",
						Suggestion:  "Verify data integrity after conversion",
					})
				}

				// Add warnings from conversion result
				for _, warning := range result.Warnings {
					warnings = append(warnings, ConversionWarning{
						WarningType: WarningTypeDataTypeLoss,
						ObjectType:  ObjectTypeColumn,
						ObjectName:  fmt.Sprintf("%s.%s", tableName, columnName),
						Message:     warning,
						Severity:    "medium",
						Suggestion:  "Review custom type conversion",
					})
				}
			}
		}

		convertedTable.Columns = convertedColumns
		convertedTables[tableName] = convertedTable
	}
	convertedSchema.Tables = convertedTables

	// Convert collection fields
	convertedCollections := make(map[string]Collection)
	for collectionName, collection := range schema.Collections {
		convertedCollection := collection
		convertedFields := make(map[string]Field)

		for fieldName, field := range collection.Fields {
			convertedField, err := ce.typeConverter.ConvertField(field, sourceDB, targetDB)
			if err != nil {
				warnings = append(warnings, ConversionWarning{
					WarningType: WarningTypeDataTypeLoss,
					ObjectType:  ObjectTypeField,
					ObjectName:  fmt.Sprintf("%s.%s", collectionName, fieldName),
					Message:     fmt.Sprintf("Failed to convert field data type: %s", err.Error()),
					Severity:    "high",
					Suggestion:  "Manual data type mapping may be required",
				})
				// Keep original field if conversion fails
				convertedFields[fieldName] = field
			} else {
				convertedFields[fieldName] = convertedField

				// Add warning if conversion is lossy
				if convertedField.Options != nil {
					if isLossy, exists := convertedField.Options["is_lossy_conversion"].(bool); exists && isLossy {
						warnings = append(warnings, ConversionWarning{
							WarningType: WarningTypeDataTypeLoss,
							ObjectType:  ObjectTypeField,
							ObjectName:  fmt.Sprintf("%s.%s", collectionName, fieldName),
							Message:     fmt.Sprintf("Lossy data type conversion: %s -> %s", field.Type, convertedField.Type),
							Severity:    "medium",
							Suggestion:  "Verify data integrity after conversion",
						})
					}
				}
			}
		}

		convertedCollection.Fields = convertedFields
		convertedCollections[collectionName] = convertedCollection
	}
	convertedSchema.Collections = convertedCollections

	// Convert node properties
	convertedNodes := make(map[string]Node)
	for nodeName, node := range schema.Nodes {
		convertedNode := node
		convertedProperties := make(map[string]Property)

		for propertyName, property := range node.Properties {
			convertedProperty, err := ce.typeConverter.ConvertProperty(property, sourceDB, targetDB)
			if err != nil {
				warnings = append(warnings, ConversionWarning{
					WarningType: WarningTypeDataTypeLoss,
					ObjectType:  ObjectTypeProperty,
					ObjectName:  fmt.Sprintf("%s.%s", nodeName, propertyName),
					Message:     fmt.Sprintf("Failed to convert property data type: %s", err.Error()),
					Severity:    "high",
					Suggestion:  "Manual data type mapping may be required",
				})
				// Keep original property if conversion fails
				convertedProperties[propertyName] = property
			} else {
				convertedProperties[propertyName] = convertedProperty

				// Add warning if conversion is lossy
				if convertedProperty.Options != nil {
					if isLossy, exists := convertedProperty.Options["is_lossy_conversion"].(bool); exists && isLossy {
						warnings = append(warnings, ConversionWarning{
							WarningType: WarningTypeDataTypeLoss,
							ObjectType:  ObjectTypeProperty,
							ObjectName:  fmt.Sprintf("%s.%s", nodeName, propertyName),
							Message:     fmt.Sprintf("Lossy data type conversion: %s -> %s", property.Type, convertedProperty.Type),
							Severity:    "medium",
							Suggestion:  "Verify data integrity after conversion",
						})
					}
				}
			}
		}

		convertedNode.Properties = convertedProperties
		convertedNodes[nodeName] = convertedNode
	}
	convertedSchema.Nodes = convertedNodes

	return &convertedSchema, warnings, nil
}

// countObjectsOfType counts objects of a specific type in the schema
func (ce *ConversionEngine) countObjectsOfType(schema *UnifiedModel, objType ObjectType) int {
	switch objType {
	case ObjectTypeTable:
		return len(schema.Tables)
	case ObjectTypeCollection:
		return len(schema.Collections)
	case ObjectTypeNode:
		return len(schema.Nodes)
	// Add other object types...
	default:
		return 0
	}
}
