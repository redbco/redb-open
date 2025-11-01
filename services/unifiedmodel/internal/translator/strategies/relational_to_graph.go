package strategies

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/core"
)

// Register RelationalToGraph strategy with the global registry on init
func init() {
	if err := RegisterStrategy(NewRelationalToGraphStrategy()); err != nil {
		fmt.Printf("Warning: Failed to register RelationalToGraph strategy: %v\n", err)
	}
}

// RelationalToGraphStrategy implements conversion from relational to graph paradigm
type RelationalToGraphStrategy struct {
	*BaseStrategy
}

// NewRelationalToGraphStrategy creates a new RelationalToGraph strategy
func NewRelationalToGraphStrategy() *RelationalToGraphStrategy {
	return &RelationalToGraphStrategy{
		BaseStrategy: NewBaseStrategy("RelationalToGraph", dbcapabilities.ParadigmRelational, dbcapabilities.ParadigmGraph, DefaultStrategyConfig()),
	}
}

// NewRelationalToGraphStrategyWithConfig creates a new strategy with custom config
func NewRelationalToGraphStrategyWithConfig(config StrategyConfig) *RelationalToGraphStrategy {
	return &RelationalToGraphStrategy{
		BaseStrategy: NewBaseStrategy("RelationalToGraph", dbcapabilities.ParadigmRelational, dbcapabilities.ParadigmGraph, config),
	}
}

// RequiresSampleData returns false as sample data is optional
func (s *RelationalToGraphStrategy) RequiresSampleData() bool {
	return false
}

// RequiresEnrichment returns true as enrichment helps identify relationships
func (s *RelationalToGraphStrategy) RequiresEnrichment() bool {
	return true
}

// SupportedSourceTypes returns the object types this strategy can convert
func (s *RelationalToGraphStrategy) SupportedSourceTypes() []unifiedmodel.ObjectType {
	return []unifiedmodel.ObjectType{
		unifiedmodel.ObjectTypeTable,
		unifiedmodel.ObjectTypeView,
	}
}

// GetUserDecisions returns decisions that need user input
func (s *RelationalToGraphStrategy) GetUserDecisions(ctx *core.TranslationContext) []core.PendingUserDecision {
	decisions := make([]core.PendingUserDecision, 0)

	// Junction table handling decision
	decisions = append(decisions, core.PendingUserDecision{
		DecisionID:   "junction_table_handling",
		ObjectType:   "strategy",
		ObjectName:   "relational_to_graph",
		DecisionType: "configuration",
		Context:      "How should junction tables be converted?",
		Options: []string{
			"as_relationships", // Junction tables → Relationships with properties
			"as_nodes",         // Junction tables → Node types
			"hybrid",           // Decide per table based on properties
		},
		Recommended: "as_relationships",
	})

	// Relationship naming strategy
	decisions = append(decisions, core.PendingUserDecision{
		DecisionID:   "relationship_naming",
		ObjectType:   "strategy",
		ObjectName:   "relational_to_graph",
		DecisionType: "configuration",
		Context:      "How should relationship types be named?",
		Options: []string{
			"foreign_key_name", // Use FK constraint name
			"table_name",       // Use source/target table names
			"custom",           // User provides names
		},
		Recommended: "table_name",
	})

	return decisions
}

// Convert performs the relational to graph conversion
func (s *RelationalToGraphStrategy) Convert(ctx *core.TranslationContext, enrichmentData interface{}) (*ConversionResult, error) {
	if ctx.SourceSchema == nil {
		return nil, fmt.Errorf("source schema is nil")
	}

	// Create target schema
	targetSchema := unifiedmodel.NewUnifiedModel(ctx.TargetDatabase)

	// Storage for generated mappings
	mappings := make([]GeneratedMapping, 0)
	warnings := make([]core.TranslationWarning, 0)

	// Classify tables (entity vs junction)
	entityTables, junctionTables := s.classifyTables(ctx)

	// Convert entity tables to nodes
	for tableName, table := range ctx.SourceSchema.Tables {
		ctx.IncrementObjectProcessed()

		if ctx.IsObjectExcluded(tableName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Check if this is an entity table
		if !contains(entityTables, tableName) {
			// Skip junction tables for now, they'll be processed as relationships
			continue
		}

		// Convert table to node
		node, nodeMapping, nodeWarnings, err := s.convertTableToNode(table, tableName, ctx)
		if err != nil {
			warnings = append(warnings, s.CreateWarning(
				core.WarningTypeCompatibility,
				"table",
				tableName,
				fmt.Sprintf("Failed to convert table: %s", err.Error()),
				"medium",
				"Review table structure manually",
			))
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.Nodes[node.Label] = node

		if nodeMapping != nil {
			mappings = append(mappings, *nodeMapping)
		}

		warnings = append(warnings, nodeWarnings...)
		ctx.IncrementObjectConverted()
	}

	// Convert foreign keys to relationships
	for tableName, table := range ctx.SourceSchema.Tables {
		if !contains(entityTables, tableName) {
			continue
		}

		// Process foreign keys
		for _, constraint := range table.Constraints {
			if constraint.Type == unifiedmodel.ConstraintTypeForeignKey {
				ctx.IncrementObjectProcessed()

				relationship, relMapping, relWarnings, err := s.convertForeignKeyToRelationship(
					tableName,
					constraint,
					ctx,
					targetSchema,
				)
				if err != nil {
					warnings = append(warnings, s.CreateWarning(
						core.WarningTypeCompatibility,
						"foreign_key",
						constraint.Name,
						fmt.Sprintf("Failed to convert foreign key: %s", err.Error()),
						"low",
						"Review foreign key manually",
					))
					ctx.IncrementObjectSkipped()
					continue
				}

				targetSchema.Relationships[relationship.Type] = relationship

				if relMapping != nil {
					mappings = append(mappings, *relMapping)
				}

				warnings = append(warnings, relWarnings...)
				ctx.IncrementObjectConverted()
			}
		}
	}

	// Process junction tables as relationships
	for _, junctionTableName := range junctionTables {
		ctx.IncrementObjectProcessed()

		junctionTable := ctx.SourceSchema.Tables[junctionTableName]
		relationship, relMapping, relWarnings, err := s.convertJunctionTableToRelationship(
			junctionTable,
			junctionTableName,
			ctx,
			targetSchema,
		)
		if err != nil {
			warnings = append(warnings, s.CreateWarning(
				core.WarningTypeCompatibility,
				"junction_table",
				junctionTableName,
				fmt.Sprintf("Failed to convert junction table: %s", err.Error()),
				"medium",
				"Consider converting as node type instead",
			))
			ctx.IncrementObjectSkipped()
			continue
		}

		targetSchema.Relationships[relationship.Type] = relationship

		if relMapping != nil {
			mappings = append(mappings, *relMapping)
		}

		warnings = append(warnings, relWarnings...)
		ctx.IncrementObjectConverted()
	}

	return &ConversionResult{
		TargetSchema: targetSchema,
		Mappings:     mappings,
		Warnings:     warnings,
	}, nil
}

// convertTableToNode converts a relational table to a graph node
func (s *RelationalToGraphStrategy) convertTableToNode(
	table unifiedmodel.Table,
	tableName string,
	ctx *core.TranslationContext,
) (unifiedmodel.Node, *GeneratedMapping, []core.TranslationWarning, error) {

	warnings := make([]core.TranslationWarning, 0)

	// Create node with label from table name
	nodeLabel := s.sanitizeNodeLabel(tableName)

	node := unifiedmodel.Node{
		Label:      nodeLabel,
		Properties: make(map[string]unifiedmodel.Property),
	}

	// Create mapping
	var mapping *GeneratedMapping
	if s.config.GenerateMappings {
		mapping = &GeneratedMapping{
			SourceIdentifier: fmt.Sprintf("table:%s", tableName),
			TargetIdentifier: fmt.Sprintf("node:%s", nodeLabel),
			MappingType:      "direct",
			MappingRules:     make([]GeneratedMappingRule, 0),
			Metadata: map[string]interface{}{
				"source_paradigm": "relational",
				"target_paradigm": "graph",
				"table_name":      tableName,
			},
		}
	}

	// Convert columns to properties
	for columnName, column := range table.Columns {
		// Skip foreign key columns (they become relationships)
		if s.isForeignKeyColumn(column, table) {
			continue
		}

		property, mappingRule, err := s.convertColumnToProperty(columnName, column, ctx)
		if err != nil {
			warnings = append(warnings, s.CreateWarning(
				core.WarningTypeDataLoss,
				"column",
				columnName,
				fmt.Sprintf("Failed to convert column: %s", err.Error()),
				"low",
				"Column may be lost",
			))
			continue
		}

		node.Properties[property.Name] = property

		if mapping != nil && mappingRule != nil {
			mapping.MappingRules = append(mapping.MappingRules, *mappingRule)
		}
	}

	return node, mapping, warnings, nil
}

// convertColumnToProperty converts a table column to a node property
func (s *RelationalToGraphStrategy) convertColumnToProperty(
	columnName string,
	column unifiedmodel.Column,
	ctx *core.TranslationContext,
) (unifiedmodel.Property, *GeneratedMappingRule, error) {

	propertyName := s.sanitizePropertyName(columnName)

	// Convert data type
	targetType, isLossy, err := s.ConvertDataType(column.DataType, ctx.SourceDatabase, ctx.TargetDatabase)
	if err != nil {
		return unifiedmodel.Property{}, nil, err
	}

	property := unifiedmodel.Property{
		Name:    propertyName,
		Type:    targetType,
		Options: map[string]any{"source_column": columnName, "is_lossy_conversion": isLossy},
	}

	// Create mapping rule
	var mappingRule *GeneratedMappingRule
	if s.config.GenerateMappings {
		mappingRule = &GeneratedMappingRule{
			RuleID:      s.GenerateRuleID(columnName, propertyName),
			SourceField: columnName,
			TargetField: propertyName,
			SourceType:  column.DataType,
			TargetType:  targetType,
			Cardinality: "one-to-one",
			IsRequired:  !column.Nullable,
			Metadata: map[string]interface{}{
				"is_lossy":       isLossy,
				"is_primary_key": column.IsPrimaryKey,
			},
		}
	}

	if isLossy {
		ctx.IncrementLossyConversion()
	}
	ctx.IncrementTypeConverted()

	return property, mappingRule, nil
}

// convertForeignKeyToRelationship converts a foreign key constraint to a graph relationship
func (s *RelationalToGraphStrategy) convertForeignKeyToRelationship(
	tableName string,
	constraint unifiedmodel.Constraint,
	ctx *core.TranslationContext,
	targetSchema *unifiedmodel.UnifiedModel,
) (unifiedmodel.Relationship, *GeneratedMapping, []core.TranslationWarning, error) {

	warnings := make([]core.TranslationWarning, 0)

	// Determine relationship type name
	relType := s.generateRelationshipType(tableName, constraint.Reference.Table, constraint.Name)

	// Get source and target node labels
	sourceLabel := s.sanitizeNodeLabel(tableName)
	targetLabel := s.sanitizeNodeLabel(constraint.Reference.Table)

	relationship := unifiedmodel.Relationship{
		Type:       relType,
		FromLabel:  sourceLabel,
		ToLabel:    targetLabel,
		Properties: make(map[string]unifiedmodel.Property),
	}

	// Create mapping
	var mapping *GeneratedMapping
	if s.config.GenerateMappings {
		mapping = &GeneratedMapping{
			SourceIdentifier: fmt.Sprintf("foreign_key:%s.%s", tableName, constraint.Name),
			TargetIdentifier: fmt.Sprintf("relationship:%s", relType),
			MappingType:      "foreign_key",
			MappingRules: []GeneratedMappingRule{
				{
					RuleID:      s.GenerateRuleID(constraint.Columns[0], "source_id"),
					SourceField: constraint.Columns[0],
					TargetField: "source_id",
					Cardinality: "many-to-one",
					IsRequired:  true,
					Metadata: map[string]interface{}{
						"relationship_type": relType,
					},
				},
			},
			Metadata: map[string]interface{}{
				"from_label": sourceLabel,
				"to_label":   targetLabel,
			},
		}
	}

	return relationship, mapping, warnings, nil
}

// convertJunctionTableToRelationship converts a junction table to a relationship
func (s *RelationalToGraphStrategy) convertJunctionTableToRelationship(
	table unifiedmodel.Table,
	tableName string,
	ctx *core.TranslationContext,
	targetSchema *unifiedmodel.UnifiedModel,
) (unifiedmodel.Relationship, *GeneratedMapping, []core.TranslationWarning, error) {

	warnings := make([]core.TranslationWarning, 0)

	// Find the two foreign keys
	var sourceFKs []unifiedmodel.Constraint
	for _, constraint := range table.Constraints {
		if constraint.Type == unifiedmodel.ConstraintTypeForeignKey {
			sourceFKs = append(sourceFKs, constraint)
		}
	}

	if len(sourceFKs) < 2 {
		return unifiedmodel.Relationship{}, nil, warnings, fmt.Errorf("junction table must have at least 2 foreign keys")
	}

	// Take first two FKs as source and target
	sourceFK := sourceFKs[0]
	targetFK := sourceFKs[1]

	// Generate relationship type
	relType := s.generateRelationshipType(sourceFK.Reference.Table, targetFK.Reference.Table, tableName)

	// Get node labels
	sourceLabel := s.sanitizeNodeLabel(sourceFK.Reference.Table)
	targetLabel := s.sanitizeNodeLabel(targetFK.Reference.Table)

	relationship := unifiedmodel.Relationship{
		Type:       relType,
		FromLabel:  sourceLabel,
		ToLabel:    targetLabel,
		Properties: make(map[string]unifiedmodel.Property),
	}

	// Convert additional columns to relationship properties
	for columnName, column := range table.Columns {
		// Skip FK columns
		if s.isForeignKeyColumn(column, table) {
			continue
		}

		property, _, err := s.convertColumnToProperty(columnName, column, ctx)
		if err != nil {
			warnings = append(warnings, s.CreateWarning(
				core.WarningTypeDataLoss,
				"column",
				columnName,
				fmt.Sprintf("Failed to convert junction table column: %s", err.Error()),
				"low",
				"Property may be lost",
			))
			continue
		}

		relationship.Properties[property.Name] = property
	}

	// Create mapping
	var mapping *GeneratedMapping
	if s.config.GenerateMappings {
		mapping = &GeneratedMapping{
			SourceIdentifier: fmt.Sprintf("junction_table:%s", tableName),
			TargetIdentifier: fmt.Sprintf("relationship:%s", relType),
			MappingType:      "junction_table",
			MappingRules: []GeneratedMappingRule{
				{
					RuleID:      s.GenerateRuleID(sourceFK.Columns[0], "source_id"),
					SourceField: sourceFK.Columns[0],
					TargetField: "source_id",
					Cardinality: "one-to-one",
					IsRequired:  true,
				},
				{
					RuleID:      s.GenerateRuleID(targetFK.Columns[0], "target_id"),
					SourceField: targetFK.Columns[0],
					TargetField: "target_id",
					Cardinality: "one-to-one",
					IsRequired:  true,
				},
			},
			Metadata: map[string]interface{}{
				"from_label": sourceLabel,
				"to_label":   targetLabel,
			},
		}
	}

	return relationship, mapping, warnings, nil
}

// Helper methods

func (s *RelationalToGraphStrategy) classifyTables(ctx *core.TranslationContext) ([]string, []string) {
	entityTables := make([]string, 0)
	junctionTables := make([]string, 0)

	for tableName, table := range ctx.SourceSchema.Tables {
		if s.isJunctionTable(table) {
			junctionTables = append(junctionTables, tableName)
		} else {
			entityTables = append(entityTables, tableName)
		}
	}

	return entityTables, junctionTables
}

func (s *RelationalToGraphStrategy) isJunctionTable(table unifiedmodel.Table) bool {
	// Heuristic: A junction table typically has:
	// 1. Two or more foreign keys
	// 2. A composite primary key
	// 3. Few or no additional columns

	fkCount := 0
	for _, constraint := range table.Constraints {
		if constraint.Type == unifiedmodel.ConstraintTypeForeignKey {
			fkCount++
		}
	}

	// Must have at least 2 FKs
	if fkCount < 2 {
		return false
	}

	// Count non-FK columns
	nonFKColumns := 0
	for _, column := range table.Columns {
		if !s.isForeignKeyColumn(column, table) && !column.IsPrimaryKey {
			nonFKColumns++
		}
	}

	// Junction tables typically have few additional columns
	return nonFKColumns <= 3
}

func (s *RelationalToGraphStrategy) isForeignKeyColumn(column unifiedmodel.Column, table unifiedmodel.Table) bool {
	for _, constraint := range table.Constraints {
		if constraint.Type == unifiedmodel.ConstraintTypeForeignKey {
			for _, fkColumn := range constraint.Columns {
				if fkColumn == column.Name {
					return true
				}
			}
		}
	}
	return false
}

func (s *RelationalToGraphStrategy) sanitizeNodeLabel(tableName string) string {
	// Convert table name to PascalCase for node label
	parts := strings.Split(tableName, "_")
	for i, part := range parts {
		if len(part) > 0 {
			parts[i] = strings.ToUpper(part[:1]) + strings.ToLower(part[1:])
		}
	}
	return strings.Join(parts, "")
}

func (s *RelationalToGraphStrategy) sanitizePropertyName(columnName string) string {
	// Convert to camelCase for property name
	parts := strings.Split(columnName, "_")
	if len(parts) == 0 {
		return columnName
	}

	result := strings.ToLower(parts[0])
	for i := 1; i < len(parts); i++ {
		if len(parts[i]) > 0 {
			result += strings.ToUpper(parts[i][:1]) + strings.ToLower(parts[i][1:])
		}
	}
	return result
}

func (s *RelationalToGraphStrategy) generateRelationshipType(sourceTable, targetTable, constraintName string) string {
	// Use constraint name if available and meaningful
	if constraintName != "" && !strings.HasPrefix(constraintName, "fk_") {
		return strings.ToUpper(constraintName)
	}

	// Generate from table names: user_orders → USER_HAS_ORDER
	source := strings.ToUpper(s.singularize(sourceTable))
	target := strings.ToUpper(s.singularize(targetTable))
	return fmt.Sprintf("%s_HAS_%s", source, target)
}

func (s *RelationalToGraphStrategy) singularize(word string) string {
	// Simple singularization (can be improved)
	if strings.HasSuffix(word, "ies") {
		return word[:len(word)-3] + "y"
	}
	if strings.HasSuffix(word, "es") {
		return word[:len(word)-2]
	}
	if strings.HasSuffix(word, "s") {
		return word[:len(word)-1]
	}
	return word
}

// Helper function
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
