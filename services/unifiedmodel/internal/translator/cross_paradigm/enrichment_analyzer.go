package cross_paradigm

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/core"
)

// EnrichmentAnalyzer analyzes enrichment data to guide cross-paradigm conversion
type EnrichmentAnalyzer struct{}

// NewEnrichmentAnalyzer creates a new enrichment analyzer
func NewEnrichmentAnalyzer() *EnrichmentAnalyzer {
	return &EnrichmentAnalyzer{}
}

// EnrichmentContext holds analyzed enrichment data for conversion
type EnrichmentContext struct {
	// Data classification
	EntityTables   map[string]EntityClassification   `json:"entity_tables"`
	JunctionTables map[string]JunctionClassification `json:"junction_tables"`
	LookupTables   map[string]LookupClassification   `json:"lookup_tables"`

	// Relationship information
	Relationships map[string]RelationshipInfo `json:"relationships"`
	ForeignKeys   map[string]ForeignKeyInfo   `json:"foreign_keys"`

	// Access patterns
	AccessPatterns map[string]AccessPattern `json:"access_patterns"`

	// Business rules
	BusinessRules map[string]BusinessRule `json:"business_rules"`

	// Performance hints
	PerformanceHints map[string]PerformanceHint `json:"performance_hints"`

	// Data flow information
	DataFlows map[string]DataFlow `json:"data_flows"`

	// Original enrichment data
	OriginalEnrichment *unifiedmodel.UnifiedModelEnrichment `json:"original_enrichment,omitempty"`
}

// EntityClassification describes how an entity table should be handled
type EntityClassification struct {
	TableName     string   `json:"table_name"`
	EntityType    string   `json:"entity_type"` // "primary", "secondary", "dependent"
	RelatedTables []string `json:"related_tables"`
	EmbedStrategy string   `json:"embed_strategy"` // "embed", "reference", "separate"
	Priority      int      `json:"priority"`       // Higher priority entities are processed first
}

// JunctionClassification describes how junction tables should be handled
type JunctionClassification struct {
	TableName      string `json:"table_name"`
	LeftEntity     string `json:"left_entity"`
	RightEntity    string `json:"right_entity"`
	ConversionType string `json:"conversion_type"` // "array", "reference", "edge", "embedded"
	AdditionalData bool   `json:"additional_data"` // Whether junction table has additional columns
}

// LookupClassification describes how lookup/reference tables should be handled
type LookupClassification struct {
	TableName      string `json:"table_name"`
	LookupType     string `json:"lookup_type"`     // "enum", "reference", "dimension"
	ConversionType string `json:"conversion_type"` // "embed", "normalize", "enum"
	ValueColumn    string `json:"value_column"`
	DisplayColumn  string `json:"display_column"`
}

// RelationshipInfo describes relationships between entities
type RelationshipInfo struct {
	SourceEntity     string `json:"source_entity"`
	TargetEntity     string `json:"target_entity"`
	RelationshipType string `json:"relationship_type"` // "one_to_one", "one_to_many", "many_to_many"
	Cardinality      string `json:"cardinality"`
	Semantics        string `json:"semantics"` // Business meaning of the relationship
	Strength         string `json:"strength"`  // "strong", "weak", "optional"
}

// ForeignKeyInfo describes foreign key relationships
type ForeignKeyInfo struct {
	SourceTable   string   `json:"source_table"`
	SourceColumns []string `json:"source_columns"`
	TargetTable   string   `json:"target_table"`
	TargetColumns []string `json:"target_columns"`
	OnUpdate      string   `json:"on_update"`
	OnDelete      string   `json:"on_delete"`
	Deferrable    bool     `json:"deferrable"`
}

// AccessPattern describes how data is typically accessed
type AccessPattern struct {
	ObjectName     string   `json:"object_name"`
	PatternType    string   `json:"pattern_type"`    // "read_heavy", "write_heavy", "analytical", "transactional"
	Frequency      string   `json:"frequency"`       // "high", "medium", "low"
	JoinPatterns   []string `json:"join_patterns"`   // Common join patterns
	FilterPatterns []string `json:"filter_patterns"` // Common filter patterns
	SortPatterns   []string `json:"sort_patterns"`   // Common sort patterns
}

// BusinessRule describes business logic constraints
type BusinessRule struct {
	RuleName       string   `json:"rule_name"`
	RuleType       string   `json:"rule_type"` // "validation", "calculation", "workflow"
	Scope          []string `json:"scope"`     // Objects affected by the rule
	Description    string   `json:"description"`
	Implementation string   `json:"implementation"` // How to implement in target paradigm
}

// PerformanceHint provides performance optimization guidance
type PerformanceHint struct {
	ObjectName     string `json:"object_name"`
	HintType       string `json:"hint_type"` // "index", "partition", "cache", "denormalize"
	Priority       string `json:"priority"`  // "critical", "important", "nice_to_have"
	Description    string `json:"description"`
	Implementation string `json:"implementation"`
}

// DataFlow describes data movement and transformation patterns
type DataFlow struct {
	FlowName        string   `json:"flow_name"`
	SourceObjects   []string `json:"source_objects"`
	TargetObjects   []string `json:"target_objects"`
	FlowType        string   `json:"flow_type"` // "etl", "streaming", "batch", "real_time"
	Frequency       string   `json:"frequency"`
	Transformations []string `json:"transformations"`
}

// AnalyzeEnrichment analyzes the enrichment data and creates an enrichment context
func (ea *EnrichmentAnalyzer) AnalyzeEnrichment(ctx *core.TranslationContext) (*EnrichmentContext, error) {
	enrichmentCtx := &EnrichmentContext{
		EntityTables:       make(map[string]EntityClassification),
		JunctionTables:     make(map[string]JunctionClassification),
		LookupTables:       make(map[string]LookupClassification),
		Relationships:      make(map[string]RelationshipInfo),
		ForeignKeys:        make(map[string]ForeignKeyInfo),
		AccessPatterns:     make(map[string]AccessPattern),
		BusinessRules:      make(map[string]BusinessRule),
		PerformanceHints:   make(map[string]PerformanceHint),
		DataFlows:          make(map[string]DataFlow),
		OriginalEnrichment: ctx.Enrichment,
	}

	// If no enrichment data is provided, use heuristic analysis
	if ctx.Enrichment == nil {
		return ea.performHeuristicAnalysis(ctx, enrichmentCtx)
	}

	// Analyze provided enrichment data
	if err := ea.analyzeProvidedEnrichment(ctx, enrichmentCtx); err != nil {
		return nil, fmt.Errorf("failed to analyze provided enrichment: %w", err)
	}

	// Supplement with heuristic analysis for missing information
	if err := ea.supplementWithHeuristics(ctx, enrichmentCtx); err != nil {
		return nil, fmt.Errorf("failed to supplement with heuristics: %w", err)
	}

	return enrichmentCtx, nil
}

// performHeuristicAnalysis performs analysis based on schema structure when no enrichment is provided
func (ea *EnrichmentAnalyzer) performHeuristicAnalysis(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext) (*EnrichmentContext, error) {
	// Analyze table structures to classify entities
	if err := ea.classifyTablesHeuristically(ctx, enrichmentCtx); err != nil {
		return nil, fmt.Errorf("failed to classify tables: %w", err)
	}

	// Analyze foreign key relationships
	if err := ea.analyzeForeignKeysHeuristically(ctx, enrichmentCtx); err != nil {
		return nil, fmt.Errorf("failed to analyze foreign keys: %w", err)
	}

	// Generate default access patterns
	ea.generateDefaultAccessPatterns(ctx, enrichmentCtx)

	return enrichmentCtx, nil
}

// analyzeProvidedEnrichment analyzes explicitly provided enrichment data
func (ea *EnrichmentAnalyzer) analyzeProvidedEnrichment(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext) error {
	enrichment := ctx.Enrichment
	if enrichment == nil {
		return nil
	}

	// Analyze table enrichments for entity classification
	if len(enrichment.TableEnrichments) > 0 {
		ea.analyzeTableEnrichments(enrichment.TableEnrichments, enrichmentCtx)
	}

	// Analyze relationship enrichments
	if len(enrichment.RelationshipEnrichments) > 0 {
		ea.analyzeRelationshipEnrichments(enrichment.RelationshipEnrichments, enrichmentCtx)
	}

	// Analyze performance hints from enrichment
	if len(enrichment.PerformanceHints) > 0 {
		ea.analyzePerformanceHints(enrichment.PerformanceHints, enrichmentCtx)
	}

	// Analyze compliance and risk data for business rules
	ea.analyzeComplianceData(enrichment.ComplianceSummary, enrichmentCtx)

	return nil
}

// supplementWithHeuristics fills in missing information using heuristic analysis
func (ea *EnrichmentAnalyzer) supplementWithHeuristics(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext) error {
	// Fill in missing entity classifications
	if len(enrichmentCtx.EntityTables) == 0 {
		if err := ea.classifyTablesHeuristically(ctx, enrichmentCtx); err != nil {
			return err
		}
	}

	// Fill in missing foreign key analysis
	if len(enrichmentCtx.ForeignKeys) == 0 {
		if err := ea.analyzeForeignKeysHeuristically(ctx, enrichmentCtx); err != nil {
			return err
		}
	}

	// Fill in missing access patterns
	if len(enrichmentCtx.AccessPatterns) == 0 {
		ea.generateDefaultAccessPatterns(ctx, enrichmentCtx)
	}

	return nil
}

// classifyTablesHeuristically classifies tables based on structure and naming patterns
func (ea *EnrichmentAnalyzer) classifyTablesHeuristically(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext) error {
	if ctx.SourceSchema == nil {
		return nil // Nothing to classify
	}

	for tableName, table := range ctx.SourceSchema.Tables {
		classification := ea.classifyTableByStructure(tableName, table, ctx.SourceSchema)

		switch classification.EntityType {
		case "entity":
			enrichmentCtx.EntityTables[tableName] = EntityClassification{
				TableName:     tableName,
				EntityType:    "primary",
				EmbedStrategy: "separate",
				Priority:      1,
			}
		case "junction":
			enrichmentCtx.JunctionTables[tableName] = JunctionClassification{
				TableName:      tableName,
				ConversionType: "array",
				AdditionalData: len(table.Columns) > 2, // More than just the two foreign keys
			}
		case "lookup":
			enrichmentCtx.LookupTables[tableName] = LookupClassification{
				TableName:      tableName,
				LookupType:     "reference",
				ConversionType: "embed",
			}
		}
	}

	return nil
}

// classifyTableByStructure determines table type based on its structure
func (ea *EnrichmentAnalyzer) classifyTableByStructure(tableName string, table unifiedmodel.Table, schema *unifiedmodel.UnifiedModel) struct {
	EntityType string
} {
	result := struct {
		EntityType string
	}{}

	// Count foreign keys
	foreignKeyCount := 0
	primaryKeyCount := 0
	totalColumns := len(table.Columns)

	for _, column := range table.Columns {
		if column.IsPrimaryKey {
			primaryKeyCount++
		}
		// This is a simplified check - in practice, you'd analyze constraints
		if ea.isForeignKeyColumn(column.Name) {
			foreignKeyCount++
		}
	}

	// Classification logic
	switch {
	case foreignKeyCount >= 2 && totalColumns <= 4:
		// Likely a junction table
		result.EntityType = "junction"
	case totalColumns <= 3 && ea.isLookupTableName(tableName):
		// Likely a lookup table
		result.EntityType = "lookup"
	default:
		// Default to entity table
		result.EntityType = "entity"
	}

	return result
}

// analyzeForeignKeysHeuristically analyzes foreign key relationships
func (ea *EnrichmentAnalyzer) analyzeForeignKeysHeuristically(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext) error {
	if ctx.SourceSchema == nil {
		return nil // Nothing to analyze
	}

	for constraintName, constraint := range ctx.SourceSchema.Constraints {
		if constraint.Type == unifiedmodel.ConstraintTypeForeignKey {
			fkInfo := ForeignKeyInfo{
				SourceTable:   constraint.Reference.Table,
				SourceColumns: constraint.Columns,
				TargetTable:   constraint.Reference.Table,
				TargetColumns: constraint.Reference.Columns,
				OnUpdate:      constraint.Reference.OnUpdate,
				OnDelete:      constraint.Reference.OnDelete,
			}
			enrichmentCtx.ForeignKeys[constraintName] = fkInfo

			// Create relationship info
			relationshipKey := fmt.Sprintf("%s_%s", constraint.Reference.Table, constraint.Reference.Table)
			enrichmentCtx.Relationships[relationshipKey] = RelationshipInfo{
				SourceEntity:     constraint.Reference.Table,
				TargetEntity:     constraint.Reference.Table,
				RelationshipType: "one_to_many", // Default assumption
				Strength:         "strong",
			}
		}
	}

	return nil
}

// generateDefaultAccessPatterns creates default access patterns for all objects
func (ea *EnrichmentAnalyzer) generateDefaultAccessPatterns(ctx *core.TranslationContext, enrichmentCtx *EnrichmentContext) {
	if ctx.SourceSchema == nil {
		return // Nothing to generate
	}

	for tableName := range ctx.SourceSchema.Tables {
		enrichmentCtx.AccessPatterns[tableName] = AccessPattern{
			ObjectName:  tableName,
			PatternType: "transactional", // Default assumption
			Frequency:   "medium",
		}
	}

	for collectionName := range ctx.SourceSchema.Collections {
		enrichmentCtx.AccessPatterns[collectionName] = AccessPattern{
			ObjectName:  collectionName,
			PatternType: "read_heavy", // Default assumption for document stores
			Frequency:   "medium",
		}
	}
}

// Helper methods for analyzing specific enrichment types

// analyzeTableEnrichments processes table enrichment data for entity classification
func (ea *EnrichmentAnalyzer) analyzeTableEnrichments(tableEnrichments map[string]unifiedmodel.TableEnrichment, enrichmentCtx *EnrichmentContext) {
	for tableName, enrichment := range tableEnrichments {
		// Classify table based on enrichment data
		entityType := "primary"
		embedStrategy := "separate"
		priority := 1

		// Use primary category to determine entity type
		switch enrichment.PrimaryCategory {
		case unifiedmodel.TableCategoryReference, unifiedmodel.TableCategoryConfiguration:
			entityType = "lookup"
			embedStrategy = "embed"
			priority = 3

			// Create lookup table classification
			enrichmentCtx.LookupTables[tableName] = LookupClassification{
				TableName:      tableName,
				LookupType:     string(enrichment.PrimaryCategory),
				ConversionType: "embed",
				ValueColumn:    ea.findValueColumnFromTags(enrichment.Tags),
				DisplayColumn:  ea.findDisplayColumnFromTags(enrichment.Tags),
			}
		case unifiedmodel.TableCategoryMetadata:
			// Treat metadata tables as potential junction tables
			if len(enrichment.RelatedTables) >= 2 {
				entityType = "junction"
				priority = 2

				// Create junction table classification
				enrichmentCtx.JunctionTables[tableName] = JunctionClassification{
					TableName:      tableName,
					ConversionType: "array",
					AdditionalData: len(enrichment.RelatedTables) > 2, // More than just two FK relationships
				}
			} else {
				entityType = "primary"
			}
		default:
			entityType = "primary"
		}

		// Use access patterns to refine embed strategy
		if enrichment.AccessPattern == unifiedmodel.AccessPatternReadHeavy {
			embedStrategy = "embed"
		}

		// Create entity classification
		enrichmentCtx.EntityTables[tableName] = EntityClassification{
			TableName:     tableName,
			EntityType:    entityType,
			EmbedStrategy: embedStrategy,
			Priority:      priority,
			RelatedTables: enrichment.RelatedTables,
		}

		// Extract access patterns
		enrichmentCtx.AccessPatterns[tableName] = AccessPattern{
			ObjectName:   tableName,
			PatternType:  string(enrichment.AccessPattern),
			Frequency:    ea.deriveFrequencyFromPattern(enrichment.AccessPattern),
			JoinPatterns: ea.extractJoinPatterns(enrichment.RelatedTables),
		}
	}
}

// analyzeRelationshipEnrichments processes relationship enrichment data
func (ea *EnrichmentAnalyzer) analyzeRelationshipEnrichments(relationshipEnrichments map[string]unifiedmodel.RelationshipEnrichment, enrichmentCtx *EnrichmentContext) {
	for relationshipName, enrichment := range relationshipEnrichments {
		// Extract relationship info from context and business meaning
		strengthStr := "medium"
		if enrichment.Strength != nil {
			if *enrichment.Strength > 0.7 {
				strengthStr = "strong"
			} else if *enrichment.Strength < 0.3 {
				strengthStr = "weak"
			}
		}

		// Create relationship info from enrichment
		relInfo := RelationshipInfo{
			SourceEntity:     ea.extractSourceFromContext(enrichment.Context),
			TargetEntity:     ea.extractTargetFromContext(enrichment.Context),
			RelationshipType: "related_to", // Default, could be enhanced from BusinessMeaning
			Cardinality:      ea.extractCardinalityFromContext(enrichment.Context),
			Semantics:        enrichment.BusinessMeaning,
			Strength:         strengthStr,
		}
		enrichmentCtx.Relationships[relationshipName] = relInfo
	}
}

// analyzePerformanceHints processes performance hints from enrichment
func (ea *EnrichmentAnalyzer) analyzePerformanceHints(performanceHints []unifiedmodel.PerformanceHint, enrichmentCtx *EnrichmentContext) {
	for i, hint := range performanceHints {
		// Convert unifiedmodel.PerformanceHint to our internal PerformanceHint
		internalHint := PerformanceHint{
			ObjectName:     ea.extractObjectNameFromPath(hint.ObjectPath),
			HintType:       hint.Category,
			Priority:       string(hint.Priority),
			Description:    hint.Hint,
			Implementation: hint.EstimatedBenefit,
		}

		hintKey := fmt.Sprintf("%s_%s_%d", internalHint.ObjectName, hint.Category, i)
		enrichmentCtx.PerformanceHints[hintKey] = internalHint
	}
}

// analyzeComplianceData processes compliance and risk data for business rules
func (ea *EnrichmentAnalyzer) analyzeComplianceData(complianceSummary unifiedmodel.ComplianceSummary, enrichmentCtx *EnrichmentContext) {
	// Create business rules from required frameworks
	for _, framework := range complianceSummary.RequiredFrameworks {
		businessRule := BusinessRule{
			RuleName:       fmt.Sprintf("compliance_%s", framework),
			RuleType:       "compliance",
			Scope:          []string{"all"}, // Apply to all objects
			Description:    fmt.Sprintf("Compliance requirements for %s framework", framework),
			Implementation: "Apply framework-specific data handling rules",
		}
		enrichmentCtx.BusinessRules[string(framework)] = businessRule
	}

	// Create business rules from risk level
	if complianceSummary.OverallRiskLevel != "" {
		businessRule := BusinessRule{
			RuleName:       "risk_management",
			RuleType:       "risk",
			Scope:          []string{"all"},
			Description:    fmt.Sprintf("Overall risk level: %s", complianceSummary.OverallRiskLevel),
			Implementation: "Apply risk-appropriate security measures",
		}
		enrichmentCtx.BusinessRules["risk_management"] = businessRule
	}
}

// Helper methods for enrichment analysis

func (ea *EnrichmentAnalyzer) findValueColumnFromTags(tags []string) string {
	// Look for value column hints in tags
	for _, tag := range tags {
		if strings.HasPrefix(tag, "value_column:") {
			return strings.TrimPrefix(tag, "value_column:")
		}
	}
	return "value" // Default fallback
}

func (ea *EnrichmentAnalyzer) findDisplayColumnFromTags(tags []string) string {
	// Look for display column hints in tags
	for _, tag := range tags {
		if strings.HasPrefix(tag, "display_column:") {
			return strings.TrimPrefix(tag, "display_column:")
		}
	}
	return "name" // Default fallback
}

func (ea *EnrichmentAnalyzer) extractJoinPatterns(relatedTables []string) []string {
	// Convert related tables to join patterns
	patterns := make([]string, len(relatedTables))
	for i, table := range relatedTables {
		patterns[i] = fmt.Sprintf("JOIN %s", table)
	}
	return patterns
}

func (ea *EnrichmentAnalyzer) extractSourceFromContext(context map[string]string) string {
	if source, exists := context["source_entity"]; exists {
		return source
	}
	if source, exists := context["from_table"]; exists {
		return source
	}
	return "unknown_source"
}

func (ea *EnrichmentAnalyzer) extractTargetFromContext(context map[string]string) string {
	if target, exists := context["target_entity"]; exists {
		return target
	}
	if target, exists := context["to_table"]; exists {
		return target
	}
	return "unknown_target"
}

func (ea *EnrichmentAnalyzer) extractCardinalityFromContext(context map[string]string) string {
	if cardinality, exists := context["cardinality"]; exists {
		return cardinality
	}
	return "one_to_many" // Default assumption
}

func (ea *EnrichmentAnalyzer) deriveFrequencyFromPattern(pattern unifiedmodel.AccessPattern) string {
	switch pattern {
	case unifiedmodel.AccessPatternReadHeavy:
		return "high"
	case unifiedmodel.AccessPatternWriteHeavy:
		return "high"
	case unifiedmodel.AccessPatternBatch:
		return "low"
	case unifiedmodel.AccessPatternRealTime:
		return "high"
	default:
		return "medium"
	}
}

func (ea *EnrichmentAnalyzer) extractObjectNameFromPath(objectPath string) string {
	// Extract object name from path like "tables.users.columns.email"
	parts := strings.Split(objectPath, ".")
	if len(parts) >= 2 {
		return parts[1] // Return the object name (e.g., "users")
	}
	return "unknown_object"
}

// Helper methods for heuristic analysis

func (ea *EnrichmentAnalyzer) isForeignKeyColumn(columnName string) bool {
	// Simple heuristic based on naming patterns
	lowerName := strings.ToLower(columnName)
	// Don't treat plain "id" as foreign key - it's usually a primary key
	if lowerName == "id" {
		return false
	}
	return strings.HasSuffix(lowerName, "_id") ||
		strings.HasSuffix(lowerName, "_ref")
}

func (ea *EnrichmentAnalyzer) isLookupTableName(tableName string) bool {
	// Simple heuristic based on naming patterns
	lowerName := strings.ToLower(tableName)
	return strings.Contains(lowerName, "type") ||
		strings.Contains(lowerName, "status") ||
		strings.Contains(lowerName, "category") ||
		strings.Contains(lowerName, "lookup") ||
		strings.Contains(lowerName, "reference") ||
		strings.Contains(lowerName, "role") ||
		strings.HasSuffix(lowerName, "categories")
}
