package same_paradigm

import (
	"fmt"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/core"
)

// SameParadigmTranslatorImpl handles translations within the same database paradigm
type SameParadigmTranslatorImpl struct {
	objectMapper     *ObjectMapper
	capabilityFilter *CapabilityFilter
	typeConverter    *unifiedmodel.TypeConverter
}

// NewSameParadigmTranslator creates a new same-paradigm translator
func NewSameParadigmTranslator() *SameParadigmTranslatorImpl {
	return &SameParadigmTranslatorImpl{
		objectMapper:     NewObjectMapper(),
		capabilityFilter: NewCapabilityFilter(),
		typeConverter:    unifiedmodel.NewTypeConverter(),
	}
}

// Translate performs same-paradigm translation
func (spt *SameParadigmTranslatorImpl) Translate(ctx *core.TranslationContext) error {
	// Validate that this is indeed a same-paradigm translation
	if err := spt.validateSameParadigm(ctx); err != nil {
		return fmt.Errorf("same-paradigm validation failed: %w", err)
	}

	// Create target schema structure
	targetSchema := &unifiedmodel.UnifiedModel{
		DatabaseType:      ctx.TargetDatabase,
		Catalogs:          make(map[string]unifiedmodel.Catalog),
		Databases:         make(map[string]unifiedmodel.Database),
		Schemas:           make(map[string]unifiedmodel.Schema),
		Tables:            make(map[string]unifiedmodel.Table),
		Collections:       make(map[string]unifiedmodel.Collection),
		Nodes:             make(map[string]unifiedmodel.Node),
		Views:             make(map[string]unifiedmodel.View),
		MaterializedViews: make(map[string]unifiedmodel.MaterializedView),
		Functions:         make(map[string]unifiedmodel.Function),
		Procedures:        make(map[string]unifiedmodel.Procedure),
		Triggers:          make(map[string]unifiedmodel.Trigger),
		Indexes:           make(map[string]unifiedmodel.Index),
		Constraints:       make(map[string]unifiedmodel.Constraint),
		Sequences:         make(map[string]unifiedmodel.Sequence),
		Types:             make(map[string]unifiedmodel.Type),
	}

	// Process each object type
	if err := spt.processTables(ctx, targetSchema); err != nil {
		return fmt.Errorf("failed to process tables: %w", err)
	}

	if err := spt.processCollections(ctx, targetSchema); err != nil {
		return fmt.Errorf("failed to process collections: %w", err)
	}

	if err := spt.processNodes(ctx, targetSchema); err != nil {
		return fmt.Errorf("failed to process nodes: %w", err)
	}

	if err := spt.processViews(ctx, targetSchema); err != nil {
		return fmt.Errorf("failed to process views: %w", err)
	}

	if err := spt.processMaterializedViews(ctx, targetSchema); err != nil {
		return fmt.Errorf("failed to process materialized views: %w", err)
	}

	if err := spt.processFunctions(ctx, targetSchema); err != nil {
		return fmt.Errorf("failed to process functions: %w", err)
	}

	if err := spt.processProcedures(ctx, targetSchema); err != nil {
		return fmt.Errorf("failed to process procedures: %w", err)
	}

	if err := spt.processTriggers(ctx, targetSchema); err != nil {
		return fmt.Errorf("failed to process triggers: %w", err)
	}

	if err := spt.processIndexes(ctx, targetSchema); err != nil {
		return fmt.Errorf("failed to process indexes: %w", err)
	}

	if err := spt.processConstraints(ctx, targetSchema); err != nil {
		return fmt.Errorf("failed to process constraints: %w", err)
	}

	if err := spt.processSequences(ctx, targetSchema); err != nil {
		return fmt.Errorf("failed to process sequences: %w", err)
	}

	if err := spt.processTypes(ctx, targetSchema); err != nil {
		return fmt.Errorf("failed to process types: %w", err)
	}

	// Set the target schema
	ctx.SetTargetSchema(targetSchema)

	return nil
}

// validateSameParadigm ensures this is a valid same-paradigm translation
func (spt *SameParadigmTranslatorImpl) validateSameParadigm(ctx *core.TranslationContext) error {
	if ctx.Analysis == nil {
		return fmt.Errorf("paradigm analysis is required")
	}

	if ctx.Analysis.ConversionApproach != core.ConversionApproachSameParadigm {
		return fmt.Errorf("expected same-paradigm conversion, got %s", ctx.Analysis.ConversionApproach)
	}

	return nil
}

// processTables handles table conversion
func (spt *SameParadigmTranslatorImpl) processTables(ctx *core.TranslationContext, targetSchema *unifiedmodel.UnifiedModel) error {
	for tableName, table := range ctx.SourceSchema.Tables {
		ctx.IncrementObjectProcessed()

		// Check if object should be excluded
		if ctx.IsObjectExcluded(tableName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Check if tables are supported in target database
		if !spt.capabilityFilter.IsObjectTypeSupported(ctx.TargetDatabase, unifiedmodel.ObjectTypeTable) {
			ctx.AddWarning(
				core.WarningTypeFeatureLoss,
				"table",
				tableName,
				"Tables are not supported in target database",
				"high",
				"Consider converting to an alternative structure",
			)
			ctx.IncrementObjectDropped()
			continue
		}

		// Map the table
		mappedTable, err := spt.objectMapper.MapTable(table, ctx.SourceDatabase, ctx.TargetDatabase)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeCompatibility,
				"table",
				tableName,
				fmt.Sprintf("Failed to map table: %s", err.Error()),
				"medium",
				"Review table structure manually",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		// Convert data types
		convertedTable, err := spt.convertTableDataTypes(mappedTable, ctx)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeDataLoss,
				"table",
				tableName,
				fmt.Sprintf("Failed to convert data types: %s", err.Error()),
				"high",
				"Review data type mappings",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.Tables[tableName] = convertedTable
		ctx.IncrementObjectConverted()
	}

	return nil
}

// processCollections handles collection conversion
func (spt *SameParadigmTranslatorImpl) processCollections(ctx *core.TranslationContext, targetSchema *unifiedmodel.UnifiedModel) error {
	for collectionName, collection := range ctx.SourceSchema.Collections {
		ctx.IncrementObjectProcessed()

		// Check if object should be excluded
		if ctx.IsObjectExcluded(collectionName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Check if collections are supported in target database
		if !spt.capabilityFilter.IsObjectTypeSupported(ctx.TargetDatabase, unifiedmodel.ObjectTypeCollection) {
			ctx.AddWarning(
				core.WarningTypeFeatureLoss,
				"collection",
				collectionName,
				"Collections are not supported in target database",
				"high",
				"Consider converting to tables or alternative structure",
			)
			ctx.IncrementObjectDropped()
			continue
		}

		// Map the collection
		mappedCollection, err := spt.objectMapper.MapCollection(collection, ctx.SourceDatabase, ctx.TargetDatabase)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeCompatibility,
				"collection",
				collectionName,
				fmt.Sprintf("Failed to map collection: %s", err.Error()),
				"medium",
				"Review collection structure manually",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		// Convert field data types
		convertedCollection, err := spt.convertCollectionDataTypes(mappedCollection, ctx)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeDataLoss,
				"collection",
				collectionName,
				fmt.Sprintf("Failed to convert field types: %s", err.Error()),
				"high",
				"Review field type mappings",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.Collections[collectionName] = convertedCollection
		ctx.IncrementObjectConverted()
	}

	return nil
}

// processNodes handles node conversion
func (spt *SameParadigmTranslatorImpl) processNodes(ctx *core.TranslationContext, targetSchema *unifiedmodel.UnifiedModel) error {
	for nodeName, node := range ctx.SourceSchema.Nodes {
		ctx.IncrementObjectProcessed()

		// Check if object should be excluded
		if ctx.IsObjectExcluded(nodeName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Check if nodes are supported in target database
		if !spt.capabilityFilter.IsObjectTypeSupported(ctx.TargetDatabase, unifiedmodel.ObjectTypeNode) {
			ctx.AddWarning(
				core.WarningTypeFeatureLoss,
				"node",
				nodeName,
				"Nodes are not supported in target database",
				"high",
				"Consider converting to tables or alternative structure",
			)
			ctx.IncrementObjectDropped()
			continue
		}

		// Map the node
		mappedNode, err := spt.objectMapper.MapNode(node, ctx.SourceDatabase, ctx.TargetDatabase)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeCompatibility,
				"node",
				nodeName,
				fmt.Sprintf("Failed to map node: %s", err.Error()),
				"medium",
				"Review node structure manually",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		// Convert property data types
		convertedNode, err := spt.convertNodeDataTypes(mappedNode, ctx)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeDataLoss,
				"node",
				nodeName,
				fmt.Sprintf("Failed to convert property types: %s", err.Error()),
				"high",
				"Review property type mappings",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.Nodes[nodeName] = convertedNode
		ctx.IncrementObjectConverted()
	}

	return nil
}

// processViews handles view conversion
func (spt *SameParadigmTranslatorImpl) processViews(ctx *core.TranslationContext, targetSchema *unifiedmodel.UnifiedModel) error {
	for viewName, view := range ctx.SourceSchema.Views {
		ctx.IncrementObjectProcessed()

		// Check if object should be excluded
		if ctx.IsObjectExcluded(viewName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Check if views are supported in target database
		if !spt.capabilityFilter.IsObjectTypeSupported(ctx.TargetDatabase, unifiedmodel.ObjectTypeView) {
			ctx.AddWarning(
				core.WarningTypeFeatureLoss,
				"view",
				viewName,
				"Views are not supported in target database",
				"medium",
				"Consider materializing the view or converting to a table",
			)
			ctx.IncrementObjectDropped()
			continue
		}

		// Map the view
		mappedView, err := spt.objectMapper.MapView(view, ctx.SourceDatabase, ctx.TargetDatabase)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeCompatibility,
				"view",
				viewName,
				fmt.Sprintf("Failed to map view: %s", err.Error()),
				"medium",
				"Review view definition manually",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.Views[viewName] = mappedView
		ctx.IncrementObjectConverted()
	}

	return nil
}

// processMaterializedViews handles materialized view conversion
func (spt *SameParadigmTranslatorImpl) processMaterializedViews(ctx *core.TranslationContext, targetSchema *unifiedmodel.UnifiedModel) error {
	for mvName, mv := range ctx.SourceSchema.MaterializedViews {
		ctx.IncrementObjectProcessed()

		// Check if object should be excluded
		if ctx.IsObjectExcluded(mvName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Check if materialized views are supported in target database
		if !spt.capabilityFilter.IsObjectTypeSupported(ctx.TargetDatabase, unifiedmodel.ObjectTypeMaterializedView) {
			ctx.AddWarning(
				core.WarningTypeFeatureLoss,
				"materialized_view",
				mvName,
				"Materialized views are not supported in target database",
				"medium",
				"Consider converting to a regular table with refresh logic",
			)
			ctx.IncrementObjectDropped()
			continue
		}

		// Map the materialized view
		mappedMV, err := spt.objectMapper.MapMaterializedView(mv, ctx.SourceDatabase, ctx.TargetDatabase)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeCompatibility,
				"materialized_view",
				mvName,
				fmt.Sprintf("Failed to map materialized view: %s", err.Error()),
				"medium",
				"Review materialized view definition manually",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.MaterializedViews[mvName] = mappedMV
		ctx.IncrementObjectConverted()
	}

	return nil
}

// processFunctions handles function conversion
func (spt *SameParadigmTranslatorImpl) processFunctions(ctx *core.TranslationContext, targetSchema *unifiedmodel.UnifiedModel) error {
	for functionName, function := range ctx.SourceSchema.Functions {
		ctx.IncrementObjectProcessed()

		// Check if object should be excluded
		if ctx.IsObjectExcluded(functionName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Check if functions are supported in target database
		if !spt.capabilityFilter.IsObjectTypeSupported(ctx.TargetDatabase, unifiedmodel.ObjectTypeFunction) {
			ctx.AddWarning(
				core.WarningTypeFeatureLoss,
				"function",
				functionName,
				"Functions are not supported in target database",
				"high",
				"Consider implementing in application logic",
			)
			ctx.IncrementObjectDropped()
			continue
		}

		// Map the function
		mappedFunction, err := spt.objectMapper.MapFunction(function, ctx.SourceDatabase, ctx.TargetDatabase)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeCompatibility,
				"function",
				functionName,
				fmt.Sprintf("Failed to map function: %s", err.Error()),
				"high",
				"Review function definition manually",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.Functions[functionName] = mappedFunction
		ctx.IncrementObjectConverted()
	}

	return nil
}

// processProcedures handles procedure conversion
func (spt *SameParadigmTranslatorImpl) processProcedures(ctx *core.TranslationContext, targetSchema *unifiedmodel.UnifiedModel) error {
	for procedureName, procedure := range ctx.SourceSchema.Procedures {
		ctx.IncrementObjectProcessed()

		// Check if object should be excluded
		if ctx.IsObjectExcluded(procedureName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Check if procedures are supported in target database
		if !spt.capabilityFilter.IsObjectTypeSupported(ctx.TargetDatabase, unifiedmodel.ObjectTypeProcedure) {
			ctx.AddWarning(
				core.WarningTypeFeatureLoss,
				"procedure",
				procedureName,
				"Procedures are not supported in target database",
				"high",
				"Consider implementing in application logic",
			)
			ctx.IncrementObjectDropped()
			continue
		}

		// Map the procedure
		mappedProcedure, err := spt.objectMapper.MapProcedure(procedure, ctx.SourceDatabase, ctx.TargetDatabase)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeCompatibility,
				"procedure",
				procedureName,
				fmt.Sprintf("Failed to map procedure: %s", err.Error()),
				"high",
				"Review procedure definition manually",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.Procedures[procedureName] = mappedProcedure
		ctx.IncrementObjectConverted()
	}

	return nil
}

// processTriggers handles trigger conversion
func (spt *SameParadigmTranslatorImpl) processTriggers(ctx *core.TranslationContext, targetSchema *unifiedmodel.UnifiedModel) error {
	for triggerName, trigger := range ctx.SourceSchema.Triggers {
		ctx.IncrementObjectProcessed()

		// Check if object should be excluded
		if ctx.IsObjectExcluded(triggerName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Check if triggers are supported in target database
		if !spt.capabilityFilter.IsObjectTypeSupported(ctx.TargetDatabase, unifiedmodel.ObjectTypeTrigger) {
			ctx.AddWarning(
				core.WarningTypeFeatureLoss,
				"trigger",
				triggerName,
				"Triggers are not supported in target database",
				"high",
				"Consider implementing in application logic",
			)
			ctx.IncrementObjectDropped()
			continue
		}

		// Map the trigger
		mappedTrigger, err := spt.objectMapper.MapTrigger(trigger, ctx.SourceDatabase, ctx.TargetDatabase)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeCompatibility,
				"trigger",
				triggerName,
				fmt.Sprintf("Failed to map trigger: %s", err.Error()),
				"high",
				"Review trigger definition manually",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.Triggers[triggerName] = mappedTrigger
		ctx.IncrementObjectConverted()
	}

	return nil
}

// processIndexes handles index conversion
func (spt *SameParadigmTranslatorImpl) processIndexes(ctx *core.TranslationContext, targetSchema *unifiedmodel.UnifiedModel) error {
	for indexName, index := range ctx.SourceSchema.Indexes {
		ctx.IncrementObjectProcessed()

		// Check if object should be excluded
		if ctx.IsObjectExcluded(indexName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Check if indexes are supported in target database
		if !spt.capabilityFilter.IsObjectTypeSupported(ctx.TargetDatabase, unifiedmodel.ObjectTypeIndex) {
			ctx.AddWarning(
				core.WarningTypePerformance,
				"index",
				indexName,
				"Indexes are not supported in target database",
				"medium",
				"Performance may be impacted",
			)
			ctx.IncrementObjectDropped()
			continue
		}

		// Map the index
		mappedIndex, err := spt.objectMapper.MapIndex(index, ctx.SourceDatabase, ctx.TargetDatabase)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeCompatibility,
				"index",
				indexName,
				fmt.Sprintf("Failed to map index: %s", err.Error()),
				"medium",
				"Review index definition manually",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.Indexes[indexName] = mappedIndex
		ctx.IncrementObjectConverted()
	}

	return nil
}

// processConstraints handles constraint conversion
func (spt *SameParadigmTranslatorImpl) processConstraints(ctx *core.TranslationContext, targetSchema *unifiedmodel.UnifiedModel) error {
	for constraintName, constraint := range ctx.SourceSchema.Constraints {
		ctx.IncrementObjectProcessed()

		// Check if object should be excluded
		if ctx.IsObjectExcluded(constraintName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Check if constraints are supported in target database
		if !spt.capabilityFilter.IsObjectTypeSupported(ctx.TargetDatabase, unifiedmodel.ObjectTypeConstraint) {
			ctx.AddWarning(
				core.WarningTypeDataLoss,
				"constraint",
				constraintName,
				"Constraints are not supported in target database",
				"high",
				"Data integrity may be compromised",
			)
			ctx.IncrementObjectDropped()
			continue
		}

		// Map the constraint
		mappedConstraint, err := spt.objectMapper.MapConstraint(constraint, ctx.SourceDatabase, ctx.TargetDatabase)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeCompatibility,
				"constraint",
				constraintName,
				fmt.Sprintf("Failed to map constraint: %s", err.Error()),
				"high",
				"Review constraint definition manually",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.Constraints[constraintName] = mappedConstraint
		ctx.IncrementObjectConverted()
	}

	return nil
}

// processSequences handles sequence conversion
func (spt *SameParadigmTranslatorImpl) processSequences(ctx *core.TranslationContext, targetSchema *unifiedmodel.UnifiedModel) error {
	for sequenceName, sequence := range ctx.SourceSchema.Sequences {
		ctx.IncrementObjectProcessed()

		// Check if object should be excluded
		if ctx.IsObjectExcluded(sequenceName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Check if sequences are supported in target database
		if !spt.capabilityFilter.IsObjectTypeSupported(ctx.TargetDatabase, unifiedmodel.ObjectTypeSequence) {
			ctx.AddWarning(
				core.WarningTypeFeatureLoss,
				"sequence",
				sequenceName,
				"Sequences are not supported in target database",
				"medium",
				"Consider using auto-increment or application-generated IDs",
			)
			ctx.IncrementObjectDropped()
			continue
		}

		// Map the sequence
		mappedSequence, err := spt.objectMapper.MapSequence(sequence, ctx.SourceDatabase, ctx.TargetDatabase)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeCompatibility,
				"sequence",
				sequenceName,
				fmt.Sprintf("Failed to map sequence: %s", err.Error()),
				"medium",
				"Review sequence definition manually",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.Sequences[sequenceName] = mappedSequence
		ctx.IncrementObjectConverted()
	}

	return nil
}

// processTypes handles custom type conversion
func (spt *SameParadigmTranslatorImpl) processTypes(ctx *core.TranslationContext, targetSchema *unifiedmodel.UnifiedModel) error {
	for typeName, customType := range ctx.SourceSchema.Types {
		ctx.IncrementObjectProcessed()

		// Check if object should be excluded
		if ctx.IsObjectExcluded(typeName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Check if custom types are supported in target database
		if !spt.capabilityFilter.IsObjectTypeSupported(ctx.TargetDatabase, unifiedmodel.ObjectTypeType) {
			ctx.AddWarning(
				core.WarningTypeFeatureLoss,
				"type",
				typeName,
				"Custom types are not supported in target database",
				"high",
				"Consider using built-in types or application-level validation",
			)
			ctx.IncrementObjectDropped()
			continue
		}

		// Map the type
		mappedType, err := spt.objectMapper.MapType(customType, ctx.SourceDatabase, ctx.TargetDatabase)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeCompatibility,
				"type",
				typeName,
				fmt.Sprintf("Failed to map type: %s", err.Error()),
				"high",
				"Review type definition manually",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.Types[typeName] = mappedType
		ctx.IncrementObjectConverted()
	}

	return nil
}

// Data type conversion helpers

func (spt *SameParadigmTranslatorImpl) convertTableDataTypes(table unifiedmodel.Table, ctx *core.TranslationContext) (unifiedmodel.Table, error) {
	convertedColumns := make(map[string]unifiedmodel.Column)

	for columnName, column := range table.Columns {
		convertedColumn, err := spt.typeConverter.ConvertColumn(column, ctx.SourceDatabase, ctx.TargetDatabase)
		if err != nil {
			return table, fmt.Errorf("failed to convert column %s: %w", columnName, err)
		}

		convertedColumns[columnName] = convertedColumn
		ctx.IncrementTypeConverted()

		// Check for lossy conversion
		if convertedColumn.Options != nil {
			if isLossy, exists := convertedColumn.Options["is_lossy_conversion"].(bool); exists && isLossy {
				ctx.IncrementLossyConversion()
			}
		}
	}

	table.Columns = convertedColumns
	return table, nil
}

func (spt *SameParadigmTranslatorImpl) convertCollectionDataTypes(collection unifiedmodel.Collection, ctx *core.TranslationContext) (unifiedmodel.Collection, error) {
	convertedFields := make(map[string]unifiedmodel.Field)

	for fieldName, field := range collection.Fields {
		convertedField, err := spt.typeConverter.ConvertField(field, ctx.SourceDatabase, ctx.TargetDatabase)
		if err != nil {
			return collection, fmt.Errorf("failed to convert field %s: %w", fieldName, err)
		}

		convertedFields[fieldName] = convertedField
		ctx.IncrementTypeConverted()

		// Check for lossy conversion
		if convertedField.Options != nil {
			if isLossy, exists := convertedField.Options["is_lossy_conversion"].(bool); exists && isLossy {
				ctx.IncrementLossyConversion()
			}
		}
	}

	collection.Fields = convertedFields
	return collection, nil
}

func (spt *SameParadigmTranslatorImpl) convertNodeDataTypes(node unifiedmodel.Node, ctx *core.TranslationContext) (unifiedmodel.Node, error) {
	convertedProperties := make(map[string]unifiedmodel.Property)

	for propertyName, property := range node.Properties {
		convertedProperty, err := spt.typeConverter.ConvertProperty(property, ctx.SourceDatabase, ctx.TargetDatabase)
		if err != nil {
			return node, fmt.Errorf("failed to convert property %s: %w", propertyName, err)
		}

		convertedProperties[propertyName] = convertedProperty
		ctx.IncrementTypeConverted()

		// Check for lossy conversion
		if convertedProperty.Options != nil {
			if isLossy, exists := convertedProperty.Options["is_lossy_conversion"].(bool); exists && isLossy {
				ctx.IncrementLossyConversion()
			}
		}
	}

	node.Properties = convertedProperties
	return node, nil
}
