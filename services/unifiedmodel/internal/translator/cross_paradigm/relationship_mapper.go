package cross_paradigm

import (
	"fmt"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/core"
)

// RelationshipMapper handles mapping of relationships between different paradigms
type RelationshipMapper struct{}

// NewRelationshipMapper creates a new relationship mapper
func NewRelationshipMapper() *RelationshipMapper {
	return &RelationshipMapper{}
}

// MapRelationships maps relationships from source to target paradigm
func (rm *RelationshipMapper) MapRelationships(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext, targetSchema *unifiedmodel.UnifiedModel) error {
	// Check if analysis and paradigms are available
	if ctx.Analysis == nil || len(ctx.Analysis.SourceParadigms) == 0 || len(ctx.Analysis.TargetParadigms) == 0 {
		// Fallback to database-based paradigm detection
		sourceParadigm := rm.getParadigmFromDatabase(ctx.SourceDatabase)
		targetParadigm := rm.getParadigmFromDatabase(ctx.TargetDatabase)
		return rm.mapByParadigms(sourceParadigm, targetParadigm, ctx, enrichmentCtx, targetSchema)
	}

	sourceParadigm := ctx.Analysis.SourceParadigms[0]
	targetParadigm := ctx.Analysis.TargetParadigms[0]

	return rm.mapByParadigms(sourceParadigm, targetParadigm, ctx, enrichmentCtx, targetSchema)
}

// mapByParadigms performs the actual mapping based on paradigms
func (rm *RelationshipMapper) mapByParadigms(sourceParadigm, targetParadigm dbcapabilities.DataParadigm, ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext, targetSchema *unifiedmodel.UnifiedModel) error {

	switch {
	case sourceParadigm == dbcapabilities.ParadigmRelational && targetParadigm == dbcapabilities.ParadigmDocument:
		return rm.mapRelationalToDocument(ctx, enrichmentCtx, targetSchema)
	case sourceParadigm == dbcapabilities.ParadigmRelational && targetParadigm == dbcapabilities.ParadigmGraph:
		return rm.mapRelationalToGraph(ctx, enrichmentCtx, targetSchema)
	case sourceParadigm == dbcapabilities.ParadigmDocument && targetParadigm == dbcapabilities.ParadigmRelational:
		return rm.mapDocumentToRelational(ctx, enrichmentCtx, targetSchema)
	case sourceParadigm == dbcapabilities.ParadigmGraph && targetParadigm == dbcapabilities.ParadigmRelational:
		return rm.mapGraphToRelational(ctx, enrichmentCtx, targetSchema)
	case sourceParadigm == dbcapabilities.ParadigmGraph && targetParadigm == dbcapabilities.ParadigmDocument:
		return rm.mapGraphToDocument(ctx, enrichmentCtx, targetSchema)
	default:
		// For other paradigm combinations, use generic mapping
		return rm.mapGenericRelationships(ctx, enrichmentCtx, targetSchema)
	}
}

// mapRelationalToDocument maps foreign key relationships to document references/embeddings
func (rm *RelationshipMapper) mapRelationalToDocument(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext, targetSchema *unifiedmodel.UnifiedModel) error {
	for _, fkInfo := range enrichmentCtx.ForeignKeys {
		// Determine embedding strategy based on enrichment data
		strategy := rm.determineEmbeddingStrategy(fkInfo, enrichmentCtx)

		switch strategy {
		case "embed":
			if err := rm.embedRelatedDocument(fkInfo, targetSchema, ctx); err != nil {
				ctx.AddWarning(
					core.WarningTypeCompatibility,
					"foreign_key",
					fmt.Sprintf("%s_%s", fkInfo.SourceTable, fkInfo.TargetTable),
					fmt.Sprintf("Failed to embed related document: %s", err.Error()),
					"medium",
					"Consider using reference strategy instead",
				)
			}
		case "reference":
			if err := rm.createDocumentReference(fkInfo, targetSchema, ctx); err != nil {
				ctx.AddWarning(
					core.WarningTypeCompatibility,
					"foreign_key",
					fmt.Sprintf("%s_%s", fkInfo.SourceTable, fkInfo.TargetTable),
					fmt.Sprintf("Failed to create document reference: %s", err.Error()),
					"medium",
					"Review relationship mapping manually",
				)
			}
		case "array":
			if err := rm.createDocumentArray(fkInfo, targetSchema, ctx); err != nil {
				ctx.AddWarning(
					core.WarningTypeCompatibility,
					"foreign_key",
					fmt.Sprintf("%s_%s", fkInfo.SourceTable, fkInfo.TargetTable),
					fmt.Sprintf("Failed to create document array: %s", err.Error()),
					"medium",
					"Review relationship mapping manually",
				)
			}
		}
	}

	return nil
}

// mapRelationalToGraph maps foreign key relationships to graph edges
func (rm *RelationshipMapper) mapRelationalToGraph(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext, targetSchema *unifiedmodel.UnifiedModel) error {
	for _, fkInfo := range enrichmentCtx.ForeignKeys {
		// Create graph relationship
		relationship := unifiedmodel.Relationship{
			Type:       rm.deriveRelationshipType(fkInfo, enrichmentCtx),
			FromLabel:  fkInfo.SourceTable,
			ToLabel:    fkInfo.TargetTable,
			Properties: make(map[string]unifiedmodel.Property),
		}

		// Add relationship properties based on foreign key metadata
		if fkInfo.OnUpdate != "" {
			relationship.Properties["on_update"] = unifiedmodel.Property{
				Name:    "on_update",
				Type:    "string",
				Options: map[string]any{"value": fkInfo.OnUpdate},
			}
		}

		if fkInfo.OnDelete != "" {
			relationship.Properties["on_delete"] = unifiedmodel.Property{
				Name:    "on_delete",
				Type:    "string",
				Options: map[string]any{"value": fkInfo.OnDelete},
			}
		}

		relationshipName := fmt.Sprintf("%s_%s_%s", fkInfo.SourceTable, relationship.Type, fkInfo.TargetTable)
		targetSchema.Relationships[relationshipName] = relationship
	}

	return nil
}

// mapDocumentToRelational maps document references to foreign keys
func (rm *RelationshipMapper) mapDocumentToRelational(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext, targetSchema *unifiedmodel.UnifiedModel) error {
	// Analyze document structure to identify references
	for collectionName, collection := range ctx.SourceSchema.Collections {
		if targetTable, exists := targetSchema.Tables[collectionName]; exists {
			for fieldName, field := range collection.Fields {
				if rm.isReferenceField(field) {
					// Create foreign key constraint
					constraintName := fmt.Sprintf("fk_%s_%s", collectionName, fieldName)
					constraint := unifiedmodel.Constraint{
						Name:    constraintName,
						Type:    unifiedmodel.ConstraintTypeForeignKey,
						Columns: []string{fieldName},
						Reference: unifiedmodel.Reference{
							Table:   rm.extractReferenceTable(field),
							Columns: []string{"id"}, // Assume primary key is 'id'
						},
					}

					targetTable.Constraints[constraintName] = constraint
					targetSchema.Tables[collectionName] = targetTable
				}
			}
		}
	}

	return nil
}

// mapGraphToRelational maps graph edges to foreign key relationships
func (rm *RelationshipMapper) mapGraphToRelational(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext, targetSchema *unifiedmodel.UnifiedModel) error {
	for _, relationship := range ctx.SourceSchema.Relationships {
		// Create foreign key in the source table pointing to target table
		if sourceTable, exists := targetSchema.Tables[relationship.FromLabel]; exists {
			// Add foreign key column
			fkColumnName := fmt.Sprintf("%s_id", relationship.ToLabel)
			sourceTable.Columns[fkColumnName] = unifiedmodel.Column{
				Name:     fkColumnName,
				DataType: "integer",
				Nullable: true,
			}

			// Add foreign key constraint
			constraintName := fmt.Sprintf("fk_%s_%s", relationship.FromLabel, relationship.ToLabel)
			sourceTable.Constraints[constraintName] = unifiedmodel.Constraint{
				Name:    constraintName,
				Type:    unifiedmodel.ConstraintTypeForeignKey,
				Columns: []string{fkColumnName},
				Reference: unifiedmodel.Reference{
					Table:   relationship.ToLabel,
					Columns: []string{"id"},
				},
			}

			targetSchema.Tables[relationship.FromLabel] = sourceTable
		}

		// If relationship has properties, create junction table
		if len(relationship.Properties) > 0 {
			junctionTableName := fmt.Sprintf("%s_%s_%s", relationship.FromLabel, relationship.Type, relationship.ToLabel)
			junctionTable := rm.createJunctionTable(relationship, junctionTableName)
			targetSchema.Tables[junctionTableName] = junctionTable
		}
	}

	return nil
}

// mapGraphToDocument maps graph structures to document embeddings/references
func (rm *RelationshipMapper) mapGraphToDocument(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext, targetSchema *unifiedmodel.UnifiedModel) error {
	for relationshipName, relationship := range ctx.SourceSchema.Relationships {
		// Determine how to represent the relationship in document form
		strategy := rm.determineGraphToDocumentStrategy(relationship, enrichmentCtx)

		switch strategy {
		case "embed":
			if err := rm.embedGraphRelationship(relationship, targetSchema, ctx); err != nil {
				ctx.AddWarning(
					core.WarningTypeCompatibility,
					"relationship",
					relationshipName,
					fmt.Sprintf("Failed to embed graph relationship: %s", err.Error()),
					"medium",
					"Consider using reference strategy instead",
				)
			}
		case "reference":
			if err := rm.referenceGraphRelationship(relationship, targetSchema, ctx); err != nil {
				ctx.AddWarning(
					core.WarningTypeCompatibility,
					"relationship",
					relationshipName,
					fmt.Sprintf("Failed to create graph relationship reference: %s", err.Error()),
					"medium",
					"Review relationship mapping manually",
				)
			}
		}
	}

	return nil
}

// mapGenericRelationships provides generic relationship mapping for other paradigm combinations
func (rm *RelationshipMapper) mapGenericRelationships(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext, targetSchema *unifiedmodel.UnifiedModel) error {
	// This is a fallback implementation that preserves relationship information in metadata

	// Store relationship information in schema comments (since UnifiedModel doesn't have Options field)
	relationships := make(map[string]any)
	for fkName, fkInfo := range enrichmentCtx.ForeignKeys {
		relationships[fkName] = map[string]any{
			"source_table":   fkInfo.SourceTable,
			"source_columns": fkInfo.SourceColumns,
			"target_table":   fkInfo.TargetTable,
			"target_columns": fkInfo.TargetColumns,
			"type":           "foreign_key",
		}
	}

	for relName, relInfo := range enrichmentCtx.Relationships {
		relationships[relName] = map[string]any{
			"source_entity":     relInfo.SourceEntity,
			"target_entity":     relInfo.TargetEntity,
			"relationship_type": relInfo.RelationshipType,
			"semantics":         relInfo.Semantics,
			"type":              "semantic_relationship",
		}
	}

	// Store as comment for now - in practice, this would be stored in metadata
	_ = relationships // Suppress unused variable warning

	return nil
}

// Helper methods for determining strategies

func (rm *RelationshipMapper) determineEmbeddingStrategy(fkInfo ForeignKeyInfo, enrichmentCtx *EnrichmentContext) string {
	// Check if there's explicit guidance in enrichment data
	if entityInfo, exists := enrichmentCtx.EntityTables[fkInfo.TargetTable]; exists {
		return entityInfo.EmbedStrategy
	}

	// Use heuristics based on relationship characteristics
	if fkInfo.OnDelete == "CASCADE" {
		return "embed" // Strong relationship suggests embedding
	}

	if rm.isLookupTable(fkInfo.TargetTable, enrichmentCtx) {
		return "embed" // Lookup tables are good candidates for embedding
	}

	return "reference" // Default to reference for loose coupling
}

func (rm *RelationshipMapper) deriveRelationshipType(fkInfo ForeignKeyInfo, enrichmentCtx *EnrichmentContext) string {
	// Check enrichment data for explicit relationship type
	for _, relInfo := range enrichmentCtx.Relationships {
		if relInfo.SourceEntity == fkInfo.SourceTable && relInfo.TargetEntity == fkInfo.TargetTable {
			return relInfo.RelationshipType
		}
	}

	// Use heuristics based on foreign key characteristics
	if len(fkInfo.SourceColumns) == 1 && len(fkInfo.TargetColumns) == 1 {
		return "REFERENCES" // Simple reference relationship
	}

	return "RELATED_TO" // Generic relationship type
}

func (rm *RelationshipMapper) determineGraphToDocumentStrategy(relationship unifiedmodel.Relationship, enrichmentCtx *EnrichmentContext) string {
	// If relationship has many properties, embed it
	if len(relationship.Properties) > 2 {
		return "embed"
	}

	// For simple relationships, use references
	return "reference"
}

// Helper methods for creating specific relationship structures

func (rm *RelationshipMapper) embedRelatedDocument(fkInfo ForeignKeyInfo, targetSchema *unifiedmodel.UnifiedModel, ctx *core.TranslationContext) error {
	// Find the source collection
	sourceCollection, exists := targetSchema.Collections[fkInfo.SourceTable]
	if !exists {
		return fmt.Errorf("source collection %s not found", fkInfo.SourceTable)
	}

	// Find the target collection to embed
	targetCollection, exists := targetSchema.Collections[fkInfo.TargetTable]
	if !exists {
		return fmt.Errorf("target collection %s not found", fkInfo.TargetTable)
	}

	// Create embedded field in source collection with target collection metadata
	embeddedFieldName := fkInfo.TargetTable
	embeddedOptions := map[string]any{
		"embedded_from": fkInfo.TargetTable,
		"embed_type":    "full_document",
	}

	// Include target collection schema information for better embedding
	if len(targetCollection.Fields) > 0 {
		embeddedOptions["target_fields"] = len(targetCollection.Fields)

		// Add field names for reference
		fieldNames := make([]string, 0, len(targetCollection.Fields))
		for fieldName := range targetCollection.Fields {
			fieldNames = append(fieldNames, fieldName)
		}
		embeddedOptions["target_field_names"] = fieldNames
	}

	// Include target collection comment if available
	if targetCollection.Comment != "" {
		embeddedOptions["target_description"] = targetCollection.Comment
	}

	sourceCollection.Fields[embeddedFieldName] = unifiedmodel.Field{
		Name:    embeddedFieldName,
		Type:    "object",
		Options: embeddedOptions,
	}

	targetSchema.Collections[fkInfo.SourceTable] = sourceCollection
	return nil
}

func (rm *RelationshipMapper) createDocumentReference(fkInfo ForeignKeyInfo, targetSchema *unifiedmodel.UnifiedModel, ctx *core.TranslationContext) error {
	// Find the source collection
	sourceCollection, exists := targetSchema.Collections[fkInfo.SourceTable]
	if !exists {
		return fmt.Errorf("source collection %s not found", fkInfo.SourceTable)
	}

	// Create reference field
	refFieldName := fkInfo.TargetTable + "_ref"
	sourceCollection.Fields[refFieldName] = unifiedmodel.Field{
		Name: refFieldName,
		Type: "reference",
		Options: map[string]any{
			"reference_collection": fkInfo.TargetTable,
			"reference_type":       "document_id",
		},
	}

	targetSchema.Collections[fkInfo.SourceTable] = sourceCollection
	return nil
}

func (rm *RelationshipMapper) createDocumentArray(fkInfo ForeignKeyInfo, targetSchema *unifiedmodel.UnifiedModel, ctx *core.TranslationContext) error {
	// This would be used for one-to-many relationships where the "many" side is embedded as an array
	targetCollection, exists := targetSchema.Collections[fkInfo.TargetTable]
	if !exists {
		return fmt.Errorf("target collection %s not found", fkInfo.TargetTable)
	}

	// Create array field for related documents
	arrayFieldName := fkInfo.SourceTable + "_items"
	targetCollection.Fields[arrayFieldName] = unifiedmodel.Field{
		Name: arrayFieldName,
		Type: "array",
		Options: map[string]any{
			"array_type":    "object",
			"embedded_from": fkInfo.SourceTable,
		},
	}

	targetSchema.Collections[fkInfo.TargetTable] = targetCollection
	return nil
}

func (rm *RelationshipMapper) createJunctionTable(relationship unifiedmodel.Relationship, tableName string) unifiedmodel.Table {
	table := unifiedmodel.Table{
		Name:        tableName,
		Columns:     make(map[string]unifiedmodel.Column),
		Constraints: make(map[string]unifiedmodel.Constraint),
	}

	// Add foreign key columns
	fromColumnName := relationship.FromLabel + "_id"
	toColumnName := relationship.ToLabel + "_id"

	table.Columns[fromColumnName] = unifiedmodel.Column{
		Name:     fromColumnName,
		DataType: "integer",
		Nullable: false,
	}

	table.Columns[toColumnName] = unifiedmodel.Column{
		Name:     toColumnName,
		DataType: "integer",
		Nullable: false,
	}

	// Add relationship properties as columns
	for propName, property := range relationship.Properties {
		table.Columns[propName] = unifiedmodel.Column{
			Name:     propName,
			DataType: rm.mapPropertyTypeToColumnType(property.Type),
			Nullable: true,
			Options:  property.Options,
		}
	}

	// Add foreign key constraints
	table.Constraints["fk_"+fromColumnName] = unifiedmodel.Constraint{
		Name:    "fk_" + fromColumnName,
		Type:    unifiedmodel.ConstraintTypeForeignKey,
		Columns: []string{fromColumnName},
		Reference: unifiedmodel.Reference{
			Table:   relationship.FromLabel,
			Columns: []string{"id"},
		},
	}

	table.Constraints["fk_"+toColumnName] = unifiedmodel.Constraint{
		Name:    "fk_" + toColumnName,
		Type:    unifiedmodel.ConstraintTypeForeignKey,
		Columns: []string{toColumnName},
		Reference: unifiedmodel.Reference{
			Table:   relationship.ToLabel,
			Columns: []string{"id"},
		},
	}

	return table
}

func (rm *RelationshipMapper) embedGraphRelationship(relationship unifiedmodel.Relationship, targetSchema *unifiedmodel.UnifiedModel, ctx *core.TranslationContext) error {
	// Find the source collection
	sourceCollection, exists := targetSchema.Collections[relationship.FromLabel]
	if !exists {
		return fmt.Errorf("source collection %s not found", relationship.FromLabel)
	}

	// Create embedded relationship field
	relFieldName := relationship.Type + "_to_" + relationship.ToLabel
	sourceCollection.Fields[relFieldName] = unifiedmodel.Field{
		Name: relFieldName,
		Type: "object",
		Options: map[string]any{
			"relationship_type": relationship.Type,
			"target_label":      relationship.ToLabel,
			"properties":        relationship.Properties,
		},
	}

	targetSchema.Collections[relationship.FromLabel] = sourceCollection
	return nil
}

func (rm *RelationshipMapper) referenceGraphRelationship(relationship unifiedmodel.Relationship, targetSchema *unifiedmodel.UnifiedModel, ctx *core.TranslationContext) error {
	// Find the source collection
	sourceCollection, exists := targetSchema.Collections[relationship.FromLabel]
	if !exists {
		return fmt.Errorf("source collection %s not found", relationship.FromLabel)
	}

	// Create reference field
	refFieldName := relationship.Type + "_refs"
	sourceCollection.Fields[refFieldName] = unifiedmodel.Field{
		Name: refFieldName,
		Type: "array",
		Options: map[string]any{
			"array_type":           "reference",
			"reference_collection": relationship.ToLabel,
			"relationship_type":    relationship.Type,
		},
	}

	targetSchema.Collections[relationship.FromLabel] = sourceCollection
	return nil
}

// Utility methods

func (rm *RelationshipMapper) isReferenceField(field unifiedmodel.Field) bool {
	return field.Type == "reference" ||
		(field.Options != nil && field.Options["reference_type"] != nil)
}

func (rm *RelationshipMapper) extractReferenceTable(field unifiedmodel.Field) string {
	if field.Options != nil {
		if refTable, exists := field.Options["reference_collection"]; exists {
			if table, ok := refTable.(string); ok {
				return table
			}
		}
	}
	return "unknown_table"
}

func (rm *RelationshipMapper) isLookupTable(tableName string, enrichmentCtx *EnrichmentContext) bool {
	_, exists := enrichmentCtx.LookupTables[tableName]
	return exists
}

func (rm *RelationshipMapper) mapPropertyTypeToColumnType(propertyType string) string {
	switch propertyType {
	case "string":
		return "varchar(255)"
	case "integer":
		return "integer"
	case "number", "float":
		return "double precision"
	case "boolean":
		return "boolean"
	case "date":
		return "timestamp"
	default:
		return "text"
	}
}

// getParadigmFromDatabase determines the paradigm based on database type
func (rm *RelationshipMapper) getParadigmFromDatabase(dbType dbcapabilities.DatabaseType) dbcapabilities.DataParadigm {
	switch dbType {
	case dbcapabilities.PostgreSQL, dbcapabilities.MySQL, dbcapabilities.SQLServer, dbcapabilities.Oracle, dbcapabilities.MariaDB:
		return dbcapabilities.ParadigmRelational
	case dbcapabilities.MongoDB:
		return dbcapabilities.ParadigmDocument
	case dbcapabilities.Neo4j:
		return dbcapabilities.ParadigmGraph
	case dbcapabilities.Redis:
		return dbcapabilities.ParadigmKeyValue
	case dbcapabilities.Cassandra:
		return dbcapabilities.ParadigmWideColumn
	default:
		return dbcapabilities.ParadigmRelational // Default fallback
	}
}
