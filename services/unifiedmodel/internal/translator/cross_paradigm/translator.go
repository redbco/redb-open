package cross_paradigm

import (
	"fmt"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/core"
)

// CrossParadigmTranslatorImpl handles translations between different database paradigms
type CrossParadigmTranslatorImpl struct {
	enrichmentAnalyzer   *EnrichmentAnalyzer
	structureTransformer *StructureTransformer
	relationshipMapper   *RelationshipMapper
	typeConverter        *unifiedmodel.TypeConverter
}

// NewCrossParadigmTranslator creates a new cross-paradigm translator
func NewCrossParadigmTranslator() *CrossParadigmTranslatorImpl {
	return &CrossParadigmTranslatorImpl{
		enrichmentAnalyzer:   NewEnrichmentAnalyzer(),
		structureTransformer: NewStructureTransformer(),
		relationshipMapper:   NewRelationshipMapper(),
		typeConverter:        unifiedmodel.NewTypeConverter(),
	}
}

// Translate performs cross-paradigm translation
func (cpt *CrossParadigmTranslatorImpl) Translate(ctx *core.TranslationContext) error {
	// Validate that this is indeed a cross-paradigm translation
	if err := cpt.validateCrossParadigm(ctx); err != nil {
		return fmt.Errorf("cross-paradigm validation failed: %w", err)
	}

	// Analyze enrichment data
	enrichmentContext, err := cpt.enrichmentAnalyzer.AnalyzeEnrichment(ctx)
	if err != nil {
		return fmt.Errorf("enrichment analysis failed: %w", err)
	}

	// Determine conversion strategy based on paradigms
	strategy, err := cpt.determineConversionStrategy(ctx)
	if err != nil {
		return fmt.Errorf("failed to determine conversion strategy: %w", err)
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
		Relationships:     make(map[string]unifiedmodel.Relationship),
	}

	// Apply conversion strategy
	switch strategy {
	case core.ConversionStrategyNormalization:
		err = cpt.performNormalization(ctx, enrichmentContext, targetSchema)
	case core.ConversionStrategyDenormalization:
		err = cpt.performDenormalization(ctx, enrichmentContext, targetSchema)
	case core.ConversionStrategyDecomposition:
		err = cpt.performDecomposition(ctx, enrichmentContext, targetSchema)
	case core.ConversionStrategyAggregation:
		err = cpt.performAggregation(ctx, enrichmentContext, targetSchema)
	case core.ConversionStrategyHybrid:
		err = cpt.performHybridConversion(ctx, enrichmentContext, targetSchema)
	default:
		err = fmt.Errorf("unsupported conversion strategy: %s", strategy)
	}

	if err != nil {
		return fmt.Errorf("conversion strategy failed: %w", err)
	}

	// Map relationships
	if err := cpt.relationshipMapper.MapRelationships(ctx, enrichmentContext, targetSchema); err != nil {
		return fmt.Errorf("relationship mapping failed: %w", err)
	}

	// Set the target schema
	ctx.SetTargetSchema(targetSchema)

	return nil
}

// validateCrossParadigm ensures this is a valid cross-paradigm translation
func (cpt *CrossParadigmTranslatorImpl) validateCrossParadigm(ctx *core.TranslationContext) error {
	if ctx.Analysis == nil {
		return fmt.Errorf("paradigm analysis is required")
	}

	if ctx.Analysis.ConversionApproach != core.ConversionApproachCrossParadigm {
		return fmt.Errorf("expected cross-paradigm conversion, got %s", ctx.Analysis.ConversionApproach)
	}

	return nil
}

// determineConversionStrategy determines the best conversion strategy for the paradigm pair
func (cpt *CrossParadigmTranslatorImpl) determineConversionStrategy(ctx *core.TranslationContext) (core.ConversionStrategy, error) {
	if ctx.Analysis == nil {
		return core.ConversionStrategyHybrid, fmt.Errorf("paradigm analysis is required")
	}

	// Use the recommended strategy from analysis
	if ctx.Analysis.RecommendedStrategy != "" {
		return ctx.Analysis.RecommendedStrategy, nil
	}

	// Fallback to paradigm-based strategy selection
	sourceParadigms := ctx.Analysis.SourceParadigms
	targetParadigms := ctx.Analysis.TargetParadigms

	if len(sourceParadigms) == 0 || len(targetParadigms) == 0 {
		return core.ConversionStrategyHybrid, nil
	}

	sourceParadigm := sourceParadigms[0]
	targetParadigm := targetParadigms[0]

	switch {
	case sourceParadigm == dbcapabilities.ParadigmRelational && targetParadigm == dbcapabilities.ParadigmDocument:
		return core.ConversionStrategyDenormalization, nil
	case sourceParadigm == dbcapabilities.ParadigmDocument && targetParadigm == dbcapabilities.ParadigmRelational:
		return core.ConversionStrategyNormalization, nil
	case sourceParadigm == dbcapabilities.ParadigmRelational && targetParadigm == dbcapabilities.ParadigmGraph:
		return core.ConversionStrategyDecomposition, nil
	case sourceParadigm == dbcapabilities.ParadigmGraph && targetParadigm == dbcapabilities.ParadigmRelational:
		return core.ConversionStrategyAggregation, nil
	case targetParadigm == dbcapabilities.ParadigmVector:
		return core.ConversionStrategyDecomposition, nil
	default:
		return core.ConversionStrategyHybrid, nil
	}
}

// performNormalization converts from document/denormalized to relational
func (cpt *CrossParadigmTranslatorImpl) performNormalization(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext, targetSchema *unifiedmodel.UnifiedModel) error {
	// Convert collections to tables with normalization
	for collectionName, collection := range ctx.SourceSchema.Collections {
		ctx.IncrementObjectProcessed()

		if ctx.IsObjectExcluded(collectionName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Use structure transformer to normalize the collection
		tables, err := cpt.structureTransformer.NormalizeCollection(collection, enrichmentCtx)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeCompatibility,
				"collection",
				collectionName,
				fmt.Sprintf("Failed to normalize collection: %s", err.Error()),
				"medium",
				"Review collection structure manually",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		// Add normalized tables to target schema
		for tableName, table := range tables {
			// Convert data types
			convertedTable, err := cpt.convertTableDataTypes(table, ctx)
			if err != nil {
				ctx.AddWarning(
					core.WarningTypeDataLoss,
					"table",
					tableName,
					fmt.Sprintf("Failed to convert data types: %s", err.Error()),
					"high",
					"Review data type mappings",
				)
				continue
			}

			targetSchema.Tables[tableName] = convertedTable
			ctx.IncrementObjectConverted()
		}
	}

	return nil
}

// performDenormalization converts from relational to document/denormalized
func (cpt *CrossParadigmTranslatorImpl) performDenormalization(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext, targetSchema *unifiedmodel.UnifiedModel) error {
	// Check if source schema exists
	if ctx.SourceSchema == nil {
		return fmt.Errorf("source schema is nil")
	}

	// Convert tables to collections with denormalization
	for tableName, table := range ctx.SourceSchema.Tables {
		ctx.IncrementObjectProcessed()

		if ctx.IsObjectExcluded(tableName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Use structure transformer to denormalize the table
		collection, err := cpt.structureTransformer.DenormalizeTable(table, enrichmentCtx, ctx.SourceSchema)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeCompatibility,
				"table",
				tableName,
				fmt.Sprintf("Failed to denormalize table: %s", err.Error()),
				"medium",
				"Review table structure manually",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		// Convert field data types
		convertedCollection, err := cpt.convertCollectionDataTypes(collection, ctx)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeDataLoss,
				"collection",
				tableName,
				fmt.Sprintf("Failed to convert field types: %s", err.Error()),
				"high",
				"Review field type mappings",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.Collections[tableName] = convertedCollection
		ctx.IncrementObjectConverted()
	}

	return nil
}

// performDecomposition converts to graph or vector structures
func (cpt *CrossParadigmTranslatorImpl) performDecomposition(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext, targetSchema *unifiedmodel.UnifiedModel) error {
	targetParadigm := ctx.Analysis.TargetParadigms[0]

	switch targetParadigm {
	case dbcapabilities.ParadigmGraph:
		return cpt.performGraphDecomposition(ctx, enrichmentCtx, targetSchema)
	case dbcapabilities.ParadigmVector:
		return cpt.performVectorDecomposition(ctx, enrichmentCtx, targetSchema)
	default:
		return fmt.Errorf("unsupported decomposition target paradigm: %s", targetParadigm)
	}
}

// performGraphDecomposition converts relational structures to graph
func (cpt *CrossParadigmTranslatorImpl) performGraphDecomposition(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext, targetSchema *unifiedmodel.UnifiedModel) error {
	// Convert tables to nodes
	for tableName, table := range ctx.SourceSchema.Tables {
		ctx.IncrementObjectProcessed()

		if ctx.IsObjectExcluded(tableName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Use structure transformer to convert table to node
		node, err := cpt.structureTransformer.TableToNode(table, enrichmentCtx)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeCompatibility,
				"table",
				tableName,
				fmt.Sprintf("Failed to convert table to node: %s", err.Error()),
				"medium",
				"Review table structure manually",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		// Convert property data types
		convertedNode, err := cpt.convertNodeDataTypes(node, ctx)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeDataLoss,
				"node",
				tableName,
				fmt.Sprintf("Failed to convert property types: %s", err.Error()),
				"high",
				"Review property type mappings",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.Nodes[tableName] = convertedNode
		ctx.IncrementObjectConverted()
	}

	return nil
}

// performVectorDecomposition converts documents to vector representations
func (cpt *CrossParadigmTranslatorImpl) performVectorDecomposition(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext, targetSchema *unifiedmodel.UnifiedModel) error {
	// Convert collections/tables to vector indexes
	for collectionName, collection := range ctx.SourceSchema.Collections {
		ctx.IncrementObjectProcessed()

		if ctx.IsObjectExcluded(collectionName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Use structure transformer to convert collection to vector index
		vectorIndex, err := cpt.structureTransformer.CollectionToVectorIndex(collection, enrichmentCtx)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeCompatibility,
				"collection",
				collectionName,
				fmt.Sprintf("Failed to convert collection to vector index: %s", err.Error()),
				"medium",
				"Review collection structure manually",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.VectorIndexes[collectionName] = vectorIndex
		ctx.IncrementObjectConverted()
	}

	return nil
}

// performAggregation converts from graph to relational/document
func (cpt *CrossParadigmTranslatorImpl) performAggregation(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext, targetSchema *unifiedmodel.UnifiedModel) error {
	targetParadigm := ctx.Analysis.TargetParadigms[0]

	switch targetParadigm {
	case dbcapabilities.ParadigmRelational:
		return cpt.performRelationalAggregation(ctx, enrichmentCtx, targetSchema)
	case dbcapabilities.ParadigmDocument:
		return cpt.performDocumentAggregation(ctx, enrichmentCtx, targetSchema)
	default:
		return fmt.Errorf("unsupported aggregation target paradigm: %s", targetParadigm)
	}
}

// performRelationalAggregation converts graph structures to relational
func (cpt *CrossParadigmTranslatorImpl) performRelationalAggregation(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext, targetSchema *unifiedmodel.UnifiedModel) error {
	// Convert nodes to tables
	for nodeName, node := range ctx.SourceSchema.Nodes {
		ctx.IncrementObjectProcessed()

		if ctx.IsObjectExcluded(nodeName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Use structure transformer to convert node to table
		table, err := cpt.structureTransformer.NodeToTable(node, enrichmentCtx)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeCompatibility,
				"node",
				nodeName,
				fmt.Sprintf("Failed to convert node to table: %s", err.Error()),
				"medium",
				"Review node structure manually",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		// Convert data types
		convertedTable, err := cpt.convertTableDataTypes(table, ctx)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeDataLoss,
				"table",
				nodeName,
				fmt.Sprintf("Failed to convert data types: %s", err.Error()),
				"high",
				"Review data type mappings",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.Tables[nodeName] = convertedTable
		ctx.IncrementObjectConverted()
	}

	return nil
}

// performDocumentAggregation converts graph structures to document
func (cpt *CrossParadigmTranslatorImpl) performDocumentAggregation(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext, targetSchema *unifiedmodel.UnifiedModel) error {
	// Convert nodes to collections
	for nodeName, node := range ctx.SourceSchema.Nodes {
		ctx.IncrementObjectProcessed()

		if ctx.IsObjectExcluded(nodeName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Use structure transformer to convert node to collection
		collection, err := cpt.structureTransformer.NodeToCollection(node, enrichmentCtx)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeCompatibility,
				"node",
				nodeName,
				fmt.Sprintf("Failed to convert node to collection: %s", err.Error()),
				"medium",
				"Review node structure manually",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		// Convert field data types
		convertedCollection, err := cpt.convertCollectionDataTypes(collection, ctx)
		if err != nil {
			ctx.AddWarning(
				core.WarningTypeDataLoss,
				"collection",
				nodeName,
				fmt.Sprintf("Failed to convert field types: %s", err.Error()),
				"high",
				"Review field type mappings",
			)
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.Collections[nodeName] = convertedCollection
		ctx.IncrementObjectConverted()
	}

	return nil
}

// performHybridConversion handles complex conversions that require multiple strategies
func (cpt *CrossParadigmTranslatorImpl) performHybridConversion(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext, targetSchema *unifiedmodel.UnifiedModel) error {
	// This is a simplified hybrid approach - in practice, this would be more sophisticated

	// Try denormalization first for relational to document
	if len(ctx.SourceSchema.Tables) > 0 && cpt.supportsCollections(ctx.TargetDatabase) {
		if err := cpt.performDenormalization(ctx, enrichmentCtx, targetSchema); err == nil {
			return nil
		}
	}

	// Try normalization for document to relational
	if len(ctx.SourceSchema.Collections) > 0 && cpt.supportsTables(ctx.TargetDatabase) {
		if err := cpt.performNormalization(ctx, enrichmentCtx, targetSchema); err == nil {
			return nil
		}
	}

	// Fallback to decomposition
	return cpt.performDecomposition(ctx, enrichmentCtx, targetSchema)
}

// Helper methods for data type conversion

func (cpt *CrossParadigmTranslatorImpl) convertTableDataTypes(table unifiedmodel.Table, ctx *core.TranslationContext) (unifiedmodel.Table, error) {
	convertedColumns := make(map[string]unifiedmodel.Column)

	for columnName, column := range table.Columns {
		convertedColumn, err := cpt.typeConverter.ConvertColumn(column, ctx.SourceDatabase, ctx.TargetDatabase)
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

func (cpt *CrossParadigmTranslatorImpl) convertCollectionDataTypes(collection unifiedmodel.Collection, ctx *core.TranslationContext) (unifiedmodel.Collection, error) {
	convertedFields := make(map[string]unifiedmodel.Field)

	for fieldName, field := range collection.Fields {
		convertedField, err := cpt.typeConverter.ConvertField(field, ctx.SourceDatabase, ctx.TargetDatabase)
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

func (cpt *CrossParadigmTranslatorImpl) convertNodeDataTypes(node unifiedmodel.Node, ctx *core.TranslationContext) (unifiedmodel.Node, error) {
	convertedProperties := make(map[string]unifiedmodel.Property)

	for propertyName, property := range node.Properties {
		convertedProperty, err := cpt.typeConverter.ConvertProperty(property, ctx.SourceDatabase, ctx.TargetDatabase)
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

// Helper methods for database capability checks

func (cpt *CrossParadigmTranslatorImpl) supportsCollections(db dbcapabilities.DatabaseType) bool {
	capability, exists := dbcapabilities.Get(db)
	if !exists {
		return false
	}

	for _, paradigm := range capability.Paradigms {
		if paradigm == dbcapabilities.ParadigmDocument {
			return true
		}
	}

	return false
}

func (cpt *CrossParadigmTranslatorImpl) supportsTables(db dbcapabilities.DatabaseType) bool {
	capability, exists := dbcapabilities.Get(db)
	if !exists {
		return false
	}

	for _, paradigm := range capability.Paradigms {
		if paradigm == dbcapabilities.ParadigmRelational {
			return true
		}
	}

	return false
}
