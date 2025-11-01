package strategies

import (
	"fmt"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/core"
)

// Register GraphToRelational strategy with the global registry on init
func init() {
	if err := RegisterStrategy(NewGraphToRelationalStrategy()); err != nil {
		// Log error but don't panic - allow application to continue
		fmt.Printf("Warning: Failed to register GraphToRelational strategy: %v\n", err)
	}
}

// GraphToRelationalStrategy implements conversion from graph to relational paradigm
type GraphToRelationalStrategy struct {
	*BaseStrategy
	// We'll use BaseStrategy's AnalyzePropertyDistribution instead of a separate analyzer
}

// NewGraphToRelationalStrategy creates a new GraphToRelational strategy
func NewGraphToRelationalStrategy() *GraphToRelationalStrategy {
	return &GraphToRelationalStrategy{
		BaseStrategy: NewBaseStrategy("GraphToRelational", dbcapabilities.ParadigmGraph, dbcapabilities.ParadigmRelational, DefaultStrategyConfig()),
	}
}

// NewGraphToRelationalStrategyWithConfig creates a new strategy with custom config
func NewGraphToRelationalStrategyWithConfig(config StrategyConfig) *GraphToRelationalStrategy {
	return &GraphToRelationalStrategy{
		BaseStrategy: NewBaseStrategy("GraphToRelational", dbcapabilities.ParadigmGraph, dbcapabilities.ParadigmRelational, config),
	}
}

// RequiresSampleData returns true as sample data is highly recommended for accurate property mapping
func (s *GraphToRelationalStrategy) RequiresSampleData() bool {
	return true
}

// RequiresEnrichment returns false as enrichment is optional
func (s *GraphToRelationalStrategy) RequiresEnrichment() bool {
	return false
}

// SupportedSourceTypes returns the object types this strategy can convert
func (s *GraphToRelationalStrategy) SupportedSourceTypes() []unifiedmodel.ObjectType {
	return []unifiedmodel.ObjectType{
		unifiedmodel.ObjectTypeNode,
		unifiedmodel.ObjectTypeRelationship,
		unifiedmodel.ObjectTypeGraph,
	}
}

// GetUserDecisions returns decisions that need user input
func (s *GraphToRelationalStrategy) GetUserDecisions(ctx *core.TranslationContext) []core.PendingUserDecision {
	decisions := make([]core.PendingUserDecision, 0)

	// Property mapping strategy decision
	decisions = append(decisions, core.PendingUserDecision{
		DecisionID:   "property_mapping_strategy",
		ObjectType:   "strategy",
		ObjectName:   "graph_to_relational",
		DecisionType: "configuration",
		Context:      "How should graph node properties be mapped to relational columns?",
		Options: []string{
			"all_to_columns",     // All properties → columns
			"core_to_columns",    // Common properties → columns, rest → JSONB
			"minimal_to_columns", // ID only → column, rest → JSONB
		},
		Recommended: "core_to_columns",
	})

	// Relationship mapping strategy decision
	decisions = append(decisions, core.PendingUserDecision{
		DecisionID:   "relationship_mapping_strategy",
		ObjectType:   "strategy",
		ObjectName:   "graph_to_relational",
		DecisionType: "configuration",
		Context:      "How should graph relationships be mapped?",
		Options: []string{
			"foreign_key",    // Use foreign key columns
			"junction_table", // Use junction/bridge tables
			"hybrid",         // Foreign keys for simple, junction for complex
		},
		Recommended: "hybrid",
	})

	return decisions
}

// Convert performs the graph to relational conversion
func (s *GraphToRelationalStrategy) Convert(ctx *core.TranslationContext, enrichmentData interface{}) (*ConversionResult, error) {
	if ctx.SourceSchema == nil {
		return nil, fmt.Errorf("source schema is nil")
	}

	// Note: enrichmentData is provided as interface{} to avoid import cycles
	// We don't currently use it directly, but access sample data from context

	// Create target schema
	targetSchema := unifiedmodel.NewUnifiedModel(ctx.TargetDatabase)

	// Storage for generated mappings
	mappings := make([]GeneratedMapping, 0)
	warnings := make([]core.TranslationWarning, 0)

	// Analyze sample data if available
	var nodePropertyAnalyses map[string][]PropertyDistribution
	if ctx.SampleData != nil && s.config.UseSampleData {
		nodePropertyAnalyses = analyzeGraphSamples(ctx.SampleData)
	}

	// Convert nodes to tables
	for nodeName, node := range ctx.SourceSchema.Nodes {
		ctx.IncrementObjectProcessed()

		if ctx.IsObjectExcluded(nodeName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Convert node to table
		table, nodeMapping, nodeWarnings, err := s.convertNodeToTable(node, nodeName, ctx, nodePropertyAnalyses)
		if err != nil {
			warnings = append(warnings, s.CreateWarning(
				core.WarningTypeCompatibility,
				"node",
				nodeName,
				fmt.Sprintf("Failed to convert node: %s", err.Error()),
				"medium",
				"Review node structure manually",
			))
			ctx.IncrementObjectSkipped()
			continue
		}

		// Add table to target schema
		targetSchema.Tables[table.Name] = table

		// Add mapping if generated
		if nodeMapping != nil {
			mappings = append(mappings, *nodeMapping)
		}

		// Add warnings
		warnings = append(warnings, nodeWarnings...)

		ctx.IncrementObjectConverted()
	}

	// Convert relationships to tables/foreign keys
	for relName, relationship := range ctx.SourceSchema.Relationships {
		ctx.IncrementObjectProcessed()

		if ctx.IsObjectExcluded(relName) {
			ctx.IncrementObjectSkipped()
			continue
		}

		// Convert relationship
		relMappings, relWarnings, err := s.convertRelationship(relationship, relName, ctx, targetSchema)
		if err != nil {
			warnings = append(warnings, s.CreateWarning(
				core.WarningTypeCompatibility,
				"relationship",
				relName,
				fmt.Sprintf("Failed to convert relationship: %s", err.Error()),
				"medium",
				"Review relationship structure manually",
			))
			ctx.IncrementObjectSkipped()
			continue
		}

		mappings = append(mappings, relMappings...)
		warnings = append(warnings, relWarnings...)
		ctx.IncrementObjectConverted()
	}

	return &ConversionResult{
		TargetSchema: targetSchema,
		Mappings:     mappings,
		Warnings:     warnings,
	}, nil
}

// convertNodeToTable converts a graph node to a relational table
func (s *GraphToRelationalStrategy) convertNodeToTable(
	node unifiedmodel.Node,
	nodeName string,
	ctx *core.TranslationContext,
	nodePropertyAnalyses map[string][]PropertyDistribution,
) (unifiedmodel.Table, *GeneratedMapping, []core.TranslationWarning, error) {

	warnings := make([]core.TranslationWarning, 0)

	// Create table with sanitized name
	tableName := s.SanitizeTableName(nodeName)

	table := unifiedmodel.Table{
		Name:        tableName,
		Owner:       "",
		Comment:     fmt.Sprintf("Converted from graph node: %s", nodeName),
		Labels:      map[string]string{"source_type": "graph_node", "source_label": nodeName},
		Options:     map[string]any{"original_node_label": nodeName},
		Columns:     make(map[string]unifiedmodel.Column),
		Indexes:     make(map[string]unifiedmodel.Index),
		Constraints: make(map[string]unifiedmodel.Constraint),
	}

	// Add node ID column as primary key
	idColumn := unifiedmodel.Column{
		Name:         "node_id",
		DataType:     s.determineIDType(ctx.TargetDatabase),
		Nullable:     false,
		IsPrimaryKey: true,
		Options:      map[string]any{"source_property": "id", "is_graph_id": true, "comment": "Original node ID from graph database"},
	}
	table.Columns["node_id"] = idColumn

	// Create mapping
	var mapping *GeneratedMapping
	if s.config.GenerateMappings {
		mapping = &GeneratedMapping{
			SourceIdentifier: fmt.Sprintf("node:%s", nodeName),
			TargetIdentifier: fmt.Sprintf("table:%s", tableName),
			MappingType:      s.determineMappingType(),
			MappingRules:     make([]GeneratedMappingRule, 0),
			Metadata: map[string]interface{}{
				"source_paradigm": "graph",
				"target_paradigm": "relational",
				"node_label":      nodeName,
			},
		}

		// Add ID mapping rule
		mapping.MappingRules = append(mapping.MappingRules, GeneratedMappingRule{
			RuleID:      s.GenerateRuleID("id", "node_id"),
			SourceField: "id",
			TargetField: "node_id",
			SourceType:  "node_id",
			TargetType:  idColumn.DataType,
			Cardinality: "one-to-one",
			IsRequired:  true,
			Metadata: map[string]interface{}{
				"is_primary_key": true,
			},
		})
	}

	// Determine property mapping strategy
	propertyStrategy := s.config.PropertyMappingStrategy
	var coreProperties []string

	// Override with sample data analysis if available
	if nodePropertyAnalyses != nil {
		if analyses, exists := nodePropertyAnalyses[nodeName]; exists && len(analyses) > 0 {
			// Suggest strategy based on property distribution
			commonCount := 0
			for _, analysis := range analyses {
				if analysis.Frequency >= 0.7 {
					commonCount++
					coreProperties = append(coreProperties, analysis.PropertyName)
				}
			}

			// Determine strategy based on analysis
			totalCount := len(analyses)
			if totalCount > 0 {
				if float64(commonCount)/float64(totalCount) > 0.7 {
					propertyStrategy = PropertyMappingAll
				} else if commonCount > 0 {
					propertyStrategy = PropertyMappingCore
				} else {
					propertyStrategy = PropertyMappingMinimal
				}
			}
		}
	}

	// Check for strategy override
	if override, exists := ctx.GetStrategyOverride("property_mapping_strategy"); exists {
		if stratStr, ok := override.(string); ok {
			propertyStrategy = PropertyMappingStrategy(stratStr)
		}
	}

	// Convert properties based on strategy
	switch propertyStrategy {
	case PropertyMappingAll:
		// Map all properties to columns
		for propName, prop := range node.Properties {
			column, mappingRule, err := s.convertPropertyToColumn(propName, prop, ctx)
			if err != nil {
				warnings = append(warnings, s.CreateWarning(
					core.WarningTypeDataLoss,
					"property",
					propName,
					fmt.Sprintf("Failed to convert property: %s", err.Error()),
					"low",
					"Property may be lost",
				))
				continue
			}
			table.Columns[column.Name] = column

			if mapping != nil && mappingRule != nil {
				mapping.MappingRules = append(mapping.MappingRules, *mappingRule)
			}
		}

	case PropertyMappingCore:
		// Map core properties to columns, rest to JSONB
		additionalProps := make([]string, 0)

		for propName, prop := range node.Properties {
			// Check if this is a core property
			isCore := false
			for _, coreProp := range coreProperties {
				if coreProp == propName {
					isCore = true
					break
				}
			}

			if isCore || len(coreProperties) == 0 {
				// Map to column
				column, mappingRule, err := s.convertPropertyToColumn(propName, prop, ctx)
				if err != nil {
					warnings = append(warnings, s.CreateWarning(
						core.WarningTypeDataLoss,
						"property",
						propName,
						fmt.Sprintf("Failed to convert property: %s", err.Error()),
						"low",
						"Property will be stored in JSONB column",
					))
					additionalProps = append(additionalProps, propName)
					continue
				}
				table.Columns[column.Name] = column

				if mapping != nil && mappingRule != nil {
					mapping.MappingRules = append(mapping.MappingRules, *mappingRule)
				}
			} else {
				// Store in JSONB
				additionalProps = append(additionalProps, propName)
			}
		}

		// Add JSONB column for additional properties
		if len(additionalProps) > 0 {
			jsonbColumn := s.CreateHybridPropertyColumn(ctx.TargetDatabase)
			table.Columns[jsonbColumn.Name] = jsonbColumn

			// Create hybrid mapping rule
			if mapping != nil {
				mapping.MappingRules = append(mapping.MappingRules, GeneratedMappingRule{
					RuleID:      s.GenerateRuleID("additional_properties", "additional_properties"),
					SourceField: "*",
					TargetField: "additional_properties",
					SourceType:  "properties",
					TargetType:  jsonbColumn.DataType,
					Cardinality: "many-to-one",
					IsRequired:  false,
					Metadata: map[string]interface{}{
						"is_hybrid":    true,
						"properties":   additionalProps,
						"mapping_type": "json_aggregate",
					},
				})
			}

			warnings = append(warnings, s.CreateWarning(
				core.WarningTypeCompatibility,
				"node",
				nodeName,
				fmt.Sprintf("%d properties mapped to JSONB column", len(additionalProps)),
				"low",
				"",
			))
		}

	case PropertyMappingMinimal:
		// Only ID column, all properties to JSONB
		jsonbColumn := s.CreateHybridPropertyColumn(ctx.TargetDatabase)
		table.Columns[jsonbColumn.Name] = jsonbColumn

		propNames := make([]string, 0, len(node.Properties))
		for propName := range node.Properties {
			propNames = append(propNames, propName)
		}

		if mapping != nil {
			mapping.MappingRules = append(mapping.MappingRules, GeneratedMappingRule{
				RuleID:      s.GenerateRuleID("all_properties", "additional_properties"),
				SourceField: "*",
				TargetField: "additional_properties",
				SourceType:  "properties",
				TargetType:  jsonbColumn.DataType,
				Cardinality: "many-to-one",
				IsRequired:  false,
				Metadata: map[string]interface{}{
					"is_hybrid":    true,
					"properties":   propNames,
					"mapping_type": "json_aggregate",
				},
			})
		}
	}

	// Add primary key constraint
	table.Constraints["pk_"+tableName] = unifiedmodel.Constraint{
		Name:    "pk_" + tableName,
		Type:    unifiedmodel.ConstraintTypePrimaryKey,
		Columns: []string{"node_id"},
	}

	// Add index on node_id
	table.Indexes["idx_"+tableName+"_node_id"] = unifiedmodel.Index{
		Name:    "idx_" + tableName + "_node_id",
		Type:    unifiedmodel.IndexTypeBTree,
		Columns: []string{"node_id"},
		Unique:  true,
	}

	return table, mapping, warnings, nil
}

// convertPropertyToColumn converts a graph property to a table column
func (s *GraphToRelationalStrategy) convertPropertyToColumn(
	propName string,
	prop unifiedmodel.Property,
	ctx *core.TranslationContext,
) (unifiedmodel.Column, *GeneratedMappingRule, error) {

	columnName := s.SanitizeColumnName(propName)

	// Convert data type
	targetType, isLossy, err := s.ConvertDataType(prop.Type, ctx.SourceDatabase, ctx.TargetDatabase)
	if err != nil {
		return unifiedmodel.Column{}, nil, err
	}

	column := unifiedmodel.Column{
		Name:     columnName,
		DataType: targetType,
		Nullable: true, // Graph properties are typically optional
		Options:  map[string]any{"source_property": propName, "is_lossy_conversion": isLossy, "comment": fmt.Sprintf("Converted from node property: %s", propName)},
	}

	// Create mapping rule
	var mappingRule *GeneratedMappingRule
	if s.config.GenerateMappings {
		mappingRule = &GeneratedMappingRule{
			RuleID:      s.GenerateRuleID(propName, columnName),
			SourceField: propName,
			TargetField: columnName,
			SourceType:  prop.Type,
			TargetType:  targetType,
			Cardinality: "one-to-one",
			IsRequired:  false,
			Metadata: map[string]interface{}{
				"is_lossy": isLossy,
			},
		}
	}

	if isLossy {
		ctx.IncrementLossyConversion()
	}
	ctx.IncrementTypeConverted()

	return column, mappingRule, nil
}

// convertRelationship converts a graph relationship to foreign keys or junction tables
func (s *GraphToRelationalStrategy) convertRelationship(
	relationship unifiedmodel.Relationship,
	relName string,
	ctx *core.TranslationContext,
	targetSchema *unifiedmodel.UnifiedModel,
) ([]GeneratedMapping, []core.TranslationWarning, error) {

	warnings := make([]core.TranslationWarning, 0)
	mappings := make([]GeneratedMapping, 0)

	// Determine relationship mapping strategy
	relStrategy := s.config.RelationshipMappingStrategy
	if override, exists := ctx.GetStrategyOverride("relationship_mapping_strategy"); exists {
		if stratStr, ok := override.(string); ok {
			relStrategy = RelationshipMappingStrategy(stratStr)
		}
	}

	// Check if relationship has properties
	hasProperties := len(relationship.Properties) > 0

	switch relStrategy {
	case RelationshipMappingForeignKey:
		// Use foreign key columns
		return s.convertRelationshipToForeignKey(relationship, relName, ctx, targetSchema)

	case RelationshipMappingJunction:
		// Use junction table
		return s.convertRelationshipToJunctionTable(relationship, relName, ctx, targetSchema, hasProperties)

	case RelationshipMappingHybrid:
		// Simple relationships → foreign keys, complex → junction tables
		if hasProperties || s.isComplexRelationship(relationship) {
			return s.convertRelationshipToJunctionTable(relationship, relName, ctx, targetSchema, hasProperties)
		}
		return s.convertRelationshipToForeignKey(relationship, relName, ctx, targetSchema)
	}

	return mappings, warnings, nil
}

// convertRelationshipToForeignKey converts a relationship to a foreign key column
func (s *GraphToRelationalStrategy) convertRelationshipToForeignKey(
	relationship unifiedmodel.Relationship,
	relName string,
	ctx *core.TranslationContext,
	targetSchema *unifiedmodel.UnifiedModel,
) ([]GeneratedMapping, []core.TranslationWarning, error) {

	warnings := make([]core.TranslationWarning, 0)
	mappings := make([]GeneratedMapping, 0)

	// Find source and target tables
	sourceTableName := s.SanitizeTableName(relationship.FromLabel)
	targetTableName := s.SanitizeTableName(relationship.ToLabel)

	sourceTable, sourceExists := targetSchema.Tables[sourceTableName]
	_, targetExists := targetSchema.Tables[targetTableName]

	if !sourceExists || !targetExists {
		return mappings, warnings, fmt.Errorf("source or target table not found")
	}

	// Add foreign key column to source table
	fkColumnName := s.SanitizeColumnName(relName + "_id")
	fkColumn := unifiedmodel.Column{
		Name:     fkColumnName,
		DataType: s.determineIDType(ctx.TargetDatabase),
		Nullable: true,
		Options:  map[string]any{"source_relationship": relName, "relationship_type": relationship.Type, "comment": fmt.Sprintf("Foreign key from relationship: %s", relName)},
	}

	sourceTable.Columns[fkColumnName] = fkColumn

	// Add foreign key constraint
	constraintName := "fk_" + sourceTableName + "_" + relName
	sourceTable.Constraints[constraintName] = unifiedmodel.Constraint{
		Name:    constraintName,
		Type:    unifiedmodel.ConstraintTypeForeignKey,
		Columns: []string{fkColumnName},
		Reference: unifiedmodel.Reference{
			Table:    targetTableName,
			Columns:  []string{"node_id"},
			OnUpdate: "CASCADE",
			OnDelete: "SET NULL",
		},
	}

	// Update table in schema
	targetSchema.Tables[sourceTableName] = sourceTable

	// Create mapping
	if s.config.GenerateMappings {
		mapping := GeneratedMapping{
			SourceIdentifier: fmt.Sprintf("relationship:%s", relName),
			TargetIdentifier: fmt.Sprintf("foreign_key:%s.%s", sourceTableName, fkColumnName),
			MappingType:      "foreign_key",
			MappingRules: []GeneratedMappingRule{
				{
					RuleID:      s.GenerateRuleID(relName+"_target", fkColumnName),
					SourceField: "target_id",
					TargetField: fkColumnName,
					SourceType:  "node_id",
					TargetType:  fkColumn.DataType,
					Cardinality: "many-to-one",
					IsRequired:  false,
					Metadata: map[string]interface{}{
						"relationship_type": relationship.Type,
					},
				},
			},
			Metadata: map[string]interface{}{
				"relationship_type": relationship.Type,
				"from_label":        relationship.FromLabel,
				"to_label":          relationship.ToLabel,
			},
		}
		mappings = append(mappings, mapping)
	}

	return mappings, warnings, nil
}

// convertRelationshipToJunctionTable converts a relationship to a junction/bridge table
func (s *GraphToRelationalStrategy) convertRelationshipToJunctionTable(
	relationship unifiedmodel.Relationship,
	relName string,
	ctx *core.TranslationContext,
	targetSchema *unifiedmodel.UnifiedModel,
	hasProperties bool,
) ([]GeneratedMapping, []core.TranslationWarning, error) {

	warnings := make([]core.TranslationWarning, 0)
	mappings := make([]GeneratedMapping, 0)

	// Create junction table name
	junctionTableName := s.SanitizeTableName(relName)

	junctionTable := unifiedmodel.Table{
		Name:    junctionTableName,
		Comment: fmt.Sprintf("Junction table for relationship: %s", relName),
		Labels:  map[string]string{"source_type": "graph_relationship", "relationship_type": relationship.Type},
		Options: map[string]any{
			"original_relationship": relName,
			"from_label":            relationship.FromLabel,
			"to_label":              relationship.ToLabel,
		},
		Columns:     make(map[string]unifiedmodel.Column),
		Indexes:     make(map[string]unifiedmodel.Index),
		Constraints: make(map[string]unifiedmodel.Constraint),
	}

	idType := s.determineIDType(ctx.TargetDatabase)

	// Add source node ID column
	sourceColumn := unifiedmodel.Column{
		Name:     "source_node_id",
		DataType: idType,
		Nullable: false,
		Options:  map[string]any{"comment": fmt.Sprintf("Reference to %s", relationship.FromLabel)},
	}
	junctionTable.Columns["source_node_id"] = sourceColumn

	// Add target node ID column
	targetColumn := unifiedmodel.Column{
		Name:     "target_node_id",
		DataType: idType,
		Nullable: false,
		Options:  map[string]any{"comment": fmt.Sprintf("Reference to %s", relationship.ToLabel)},
	}
	junctionTable.Columns["target_node_id"] = targetColumn

	// Add relationship properties as columns if they exist
	if hasProperties {
		for propName, prop := range relationship.Properties {
			column, _, err := s.convertPropertyToColumn(propName, prop, ctx)
			if err != nil {
				warnings = append(warnings, s.CreateWarning(
					core.WarningTypeDataLoss,
					"relationship_property",
					propName,
					fmt.Sprintf("Failed to convert property: %s", err.Error()),
					"low",
					"Property may be lost",
				))
				continue
			}
			junctionTable.Columns[column.Name] = column
		}
	}

	// Add composite primary key
	junctionTable.Constraints["pk_"+junctionTableName] = unifiedmodel.Constraint{
		Name:    "pk_" + junctionTableName,
		Type:    unifiedmodel.ConstraintTypePrimaryKey,
		Columns: []string{"source_node_id", "target_node_id"},
	}

	// Add foreign key constraints
	sourceTableName := s.SanitizeTableName(relationship.FromLabel)
	targetTableName := s.SanitizeTableName(relationship.ToLabel)

	junctionTable.Constraints["fk_"+junctionTableName+"_source"] = unifiedmodel.Constraint{
		Name:    "fk_" + junctionTableName + "_source",
		Type:    unifiedmodel.ConstraintTypeForeignKey,
		Columns: []string{"source_node_id"},
		Reference: unifiedmodel.Reference{
			Table:    sourceTableName,
			Columns:  []string{"node_id"},
			OnUpdate: "CASCADE",
			OnDelete: "CASCADE",
		},
	}

	junctionTable.Constraints["fk_"+junctionTableName+"_target"] = unifiedmodel.Constraint{
		Name:    "fk_" + junctionTableName + "_target",
		Type:    unifiedmodel.ConstraintTypeForeignKey,
		Columns: []string{"target_node_id"},
		Reference: unifiedmodel.Reference{
			Table:    targetTableName,
			Columns:  []string{"node_id"},
			OnUpdate: "CASCADE",
			OnDelete: "CASCADE",
		},
	}

	// Add indexes
	junctionTable.Indexes["idx_"+junctionTableName+"_source"] = unifiedmodel.Index{
		Name:    "idx_" + junctionTableName + "_source",
		Type:    unifiedmodel.IndexTypeBTree,
		Columns: []string{"source_node_id"},
	}

	junctionTable.Indexes["idx_"+junctionTableName+"_target"] = unifiedmodel.Index{
		Name:    "idx_" + junctionTableName + "_target",
		Type:    unifiedmodel.IndexTypeBTree,
		Columns: []string{"target_node_id"},
	}

	// Add junction table to schema
	targetSchema.Tables[junctionTableName] = junctionTable

	// Create mapping
	if s.config.GenerateMappings {
		mapping := GeneratedMapping{
			SourceIdentifier: fmt.Sprintf("relationship:%s", relName),
			TargetIdentifier: fmt.Sprintf("junction_table:%s", junctionTableName),
			MappingType:      "junction_table",
			MappingRules: []GeneratedMappingRule{
				{
					RuleID:      s.GenerateRuleID(relName+"_source", "source_node_id"),
					SourceField: "source_id",
					TargetField: "source_node_id",
					SourceType:  "node_id",
					TargetType:  idType,
					Cardinality: "one-to-one",
					IsRequired:  true,
				},
				{
					RuleID:      s.GenerateRuleID(relName+"_target", "target_node_id"),
					SourceField: "target_id",
					TargetField: "target_node_id",
					SourceType:  "node_id",
					TargetType:  idType,
					Cardinality: "one-to-one",
					IsRequired:  true,
				},
			},
			Metadata: map[string]interface{}{
				"relationship_type": relationship.Type,
				"from_label":        relationship.FromLabel,
				"to_label":          relationship.ToLabel,
				"has_properties":    hasProperties,
			},
		}
		mappings = append(mappings, mapping)
	}

	return mappings, warnings, nil
}

// Helper methods

func (s *GraphToRelationalStrategy) determineIDType(targetDB dbcapabilities.DatabaseType) string {
	switch targetDB {
	case dbcapabilities.PostgreSQL, dbcapabilities.CockroachDB:
		return "bigint"
	case dbcapabilities.MySQL, dbcapabilities.MariaDB:
		return "bigint"
	case dbcapabilities.SQLServer:
		return "bigint"
	case dbcapabilities.Oracle:
		return "number(19)"
	default:
		return "bigint"
	}
}

func (s *GraphToRelationalStrategy) determineMappingType() string {
	switch s.config.PropertyMappingStrategy {
	case PropertyMappingAll:
		return "direct"
	case PropertyMappingCore, PropertyMappingMinimal:
		return "hybrid_property"
	default:
		return "direct"
	}
}

func (s *GraphToRelationalStrategy) isComplexRelationship(relationship unifiedmodel.Relationship) bool {
	// Consider a relationship complex if it has properties or if it's a many-to-many
	return len(relationship.Properties) > 0
}

// Helper function to analyze graph samples
func analyzeGraphSamples(sampleData *unifiedmodel.UnifiedModelSampleData) map[string][]PropertyDistribution {
	if sampleData == nil || len(sampleData.GraphSamples) == 0 {
		return nil
	}

	result := make(map[string][]PropertyDistribution)

	for _, graphSample := range sampleData.GraphSamples {
		for nodeLabel, nodeSample := range graphSample.NodeSamples {
			// Analyze property distribution for this node label
			propertyStats := make(map[string]*PropertyDistribution)
			totalSamples := len(nodeSample.Samples)

			if totalSamples == 0 {
				continue
			}

			for _, sample := range nodeSample.Samples {
				for propName, propValue := range sample {
					if _, exists := propertyStats[propName]; !exists {
						propertyStats[propName] = &PropertyDistribution{
							PropertyName: propName,
							DataTypes:    make(map[string]int),
						}
					}

					stat := propertyStats[propName]
					stat.Occurrences++

					// Track data type
					dataType := inferGraphDataType(propValue)
					stat.DataTypes[dataType]++
				}
			}

			// Calculate frequencies
			analyses := make([]PropertyDistribution, 0, len(propertyStats))
			for _, stat := range propertyStats {
				stat.TotalSamples = totalSamples
				stat.Frequency = float64(stat.Occurrences) / float64(totalSamples)
				analyses = append(analyses, *stat)
			}

			result[nodeLabel] = analyses
		}
	}

	return result
}

func inferGraphDataType(value interface{}) string {
	if value == nil {
		return "null"
	}

	switch value.(type) {
	case bool:
		return "boolean"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "integer"
	case float32, float64:
		return "float"
	case string:
		return "string"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	default:
		return "unknown"
	}
}
