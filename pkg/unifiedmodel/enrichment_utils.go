// Package unifiedmodel enrichment utilities provide helper functions for working
// with enrichment metadata. This module handles common operations like:
//
// - Creating and validating enrichment structures
// - Merging and filtering enrichment data
// - Generating enrichment keys and paths
// - Converting between enrichment formats

package unifiedmodel

import (
	"fmt"
	"strings"
	"time"
)

// NewUnifiedModelEnrichment creates a new enrichment structure with default values
func NewUnifiedModelEnrichment(schemaID string) *UnifiedModelEnrichment {
	return &UnifiedModelEnrichment{
		SchemaID:          schemaID,
		EnrichmentVersion: "1.0.0", // Default version
		GeneratedAt:       time.Now(),
		GeneratedBy:       "unifiedmodel",

		// Initialize empty maps
		TableEnrichments:        make(map[string]TableEnrichment),
		ColumnEnrichments:       make(map[string]ColumnEnrichment),
		IndexEnrichments:        make(map[string]IndexEnrichment),
		ViewEnrichments:         make(map[string]ViewEnrichment),
		NodeEnrichments:         make(map[string]NodeEnrichment),
		RelationshipEnrichments: make(map[string]RelationshipEnrichment),
		CollectionEnrichments:   make(map[string]CollectionEnrichment),
		DocumentEnrichments:     make(map[string]DocumentEnrichment),

		// Initialize analysis structures
		ComplianceSummary: ComplianceSummary{
			OverallRiskLevel:   RiskLevelLow,
			RequiredFrameworks: []ComplianceFramework{},
			RecommendedActions: []string{},
			ComplianceScore:    1.0,
		},
		RiskAssessment: RiskAssessment{
			OverallRiskScore:     0.0,
			PrivacyRiskScore:     0.0,
			SecurityRiskScore:    0.0,
			ComplianceRiskScore:  0.0,
			HighRiskObjects:      []RiskObject{},
			CriticalFindings:     []string{},
			MitigationStrategies: []string{},
		},
		Recommendations:  []Recommendation{},
		PerformanceHints: []PerformanceHint{},
	}
}

// GenerateColumnKey creates a consistent key for column enrichments
func GenerateColumnKey(tableName, columnName string) string {
	return fmt.Sprintf("%s.%s", tableName, columnName)
}

// GenerateObjectPath creates a hierarchical path for an object
func GenerateObjectPath(objectType ObjectType, names ...string) string {
	parts := []string{string(objectType)}
	parts = append(parts, names...)
	return strings.Join(parts, ".")
}

// ParseObjectPath extracts object type and names from a path
func ParseObjectPath(path string) (ObjectType, []string, error) {
	parts := strings.Split(path, ".")
	if len(parts) < 1 {
		return "", nil, fmt.Errorf("invalid object path: %s", path)
	}

	objectType := ObjectType(parts[0])
	names := parts[1:]

	return objectType, names, nil
}

// AddTableEnrichment adds enrichment data for a table
func (e *UnifiedModelEnrichment) AddTableEnrichment(tableName string, enrichment TableEnrichment) {
	if e.TableEnrichments == nil {
		e.TableEnrichments = make(map[string]TableEnrichment)
	}
	e.TableEnrichments[tableName] = enrichment
}

// AddColumnEnrichment adds enrichment data for a column
func (e *UnifiedModelEnrichment) AddColumnEnrichment(tableName, columnName string, enrichment ColumnEnrichment) {
	if e.ColumnEnrichments == nil {
		e.ColumnEnrichments = make(map[string]ColumnEnrichment)
	}
	key := GenerateColumnKey(tableName, columnName)
	e.ColumnEnrichments[key] = enrichment
}

// GetColumnEnrichment retrieves enrichment data for a column
func (e *UnifiedModelEnrichment) GetColumnEnrichment(tableName, columnName string) (ColumnEnrichment, bool) {
	if e.ColumnEnrichments == nil {
		return ColumnEnrichment{}, false
	}
	key := GenerateColumnKey(tableName, columnName)
	enrichment, exists := e.ColumnEnrichments[key]
	return enrichment, exists
}

// GetTableEnrichment retrieves enrichment data for a table
func (e *UnifiedModelEnrichment) GetTableEnrichment(tableName string) (TableEnrichment, bool) {
	if e.TableEnrichments == nil {
		return TableEnrichment{}, false
	}
	enrichment, exists := e.TableEnrichments[tableName]
	return enrichment, exists
}

// HasPrivilegedData checks if any object contains privileged data
func (e *UnifiedModelEnrichment) HasPrivilegedData() bool {
	// Check table enrichments
	for _, table := range e.TableEnrichments {
		if table.HasPrivilegedData {
			return true
		}
	}

	// Check column enrichments
	for _, column := range e.ColumnEnrichments {
		if column.IsPrivilegedData {
			return true
		}
	}

	// Check node enrichments
	for _, node := range e.NodeEnrichments {
		if node.HasPrivilegedData {
			return true
		}
	}

	// Check document enrichments
	for _, doc := range e.DocumentEnrichments {
		if doc.HasPrivilegedData {
			return true
		}
	}

	return false
}

// GetPrivilegedObjects returns all objects containing privileged data
func (e *UnifiedModelEnrichment) GetPrivilegedObjects() []RiskObject {
	var privilegedObjects []RiskObject

	// Check tables
	for tableName, table := range e.TableEnrichments {
		if table.HasPrivilegedData {
			riskLevel := e.calculateTableRiskLevel(table)
			privilegedObjects = append(privilegedObjects, RiskObject{
				ObjectType:  string(ObjectTypeTable),
				ObjectName:  tableName,
				RiskLevel:   riskLevel,
				RiskFactors: []string{"contains_privileged_data"},
				ImpactScore: table.DataSensitivity,
			})
		}
	}

	// Check columns
	for columnKey, column := range e.ColumnEnrichments {
		if column.IsPrivilegedData {
			parts := strings.Split(columnKey, ".")
			if len(parts) == 2 {
				privilegedObjects = append(privilegedObjects, RiskObject{
					ObjectType:  "column",
					ObjectName:  columnKey,
					RiskLevel:   column.RiskLevel,
					RiskFactors: []string{fmt.Sprintf("data_category_%s", column.DataCategory)},
					ImpactScore: column.PrivilegedConfidence,
				})
			}
		}
	}

	return privilegedObjects
}

// calculateTableRiskLevel determines risk level based on table enrichment
func (e *UnifiedModelEnrichment) calculateTableRiskLevel(table TableEnrichment) RiskLevel {
	if table.DataSensitivity >= 0.8 {
		return RiskLevelCritical
	} else if table.DataSensitivity >= 0.6 {
		return RiskLevelHigh
	} else if table.DataSensitivity >= 0.4 {
		return RiskLevelMedium
	} else if table.DataSensitivity >= 0.2 {
		return RiskLevelLow
	}
	return RiskLevelMinimal
}

// GetComplianceRequirements returns all compliance frameworks that apply to this schema
func (e *UnifiedModelEnrichment) GetComplianceRequirements() []ComplianceFramework {
	frameworkSet := make(map[ComplianceFramework]bool)

	// Collect from column enrichments
	for _, column := range e.ColumnEnrichments {
		for _, framework := range column.ComplianceImpact {
			frameworkSet[framework] = true
		}
	}

	// Collect from node enrichments
	for _, node := range e.NodeEnrichments {
		for _, framework := range node.ComplianceImpact {
			frameworkSet[framework] = true
		}
	}

	// Collect from document enrichments
	for _, doc := range e.DocumentEnrichments {
		for _, framework := range doc.ComplianceImpact {
			frameworkSet[framework] = true
		}
	}

	// Convert set to slice
	var frameworks []ComplianceFramework
	for framework := range frameworkSet {
		frameworks = append(frameworks, framework)
	}

	return frameworks
}

// FilterByCategory returns enrichment data filtered by category
func (e *UnifiedModelEnrichment) FilterByCategory(categories []EnrichmentCategory) *UnifiedModelEnrichment {
	filtered := NewUnifiedModelEnrichment(e.SchemaID)
	filtered.EnrichmentVersion = e.EnrichmentVersion
	filtered.GeneratedAt = e.GeneratedAt
	filtered.GeneratedBy = e.GeneratedBy

	categorySet := make(map[EnrichmentCategory]bool)
	for _, cat := range categories {
		categorySet[cat] = true
	}

	// Filter based on categories
	if categorySet[EnrichmentCategoryPrivacy] || categorySet[EnrichmentCategoryCompliance] {
		// Include privacy and compliance related enrichments
		for key, column := range e.ColumnEnrichments {
			if column.IsPrivilegedData || len(column.ComplianceImpact) > 0 {
				filtered.ColumnEnrichments[key] = column
			}
		}

		for key, table := range e.TableEnrichments {
			if table.HasPrivilegedData {
				filtered.TableEnrichments[key] = table
			}
		}
	}

	if categorySet[EnrichmentCategoryClassification] {
		// Include classification data
		for key, table := range e.TableEnrichments {
			if table.PrimaryCategory != "" {
				existing := filtered.TableEnrichments[key]
				existing.PrimaryCategory = table.PrimaryCategory
				existing.ClassificationConfidence = table.ClassificationConfidence
				existing.ClassificationScores = table.ClassificationScores
				filtered.TableEnrichments[key] = existing
			}
		}
	}

	if categorySet[EnrichmentCategoryPerformance] {
		// Include performance-related enrichments
		for key, table := range e.TableEnrichments {
			if table.AccessPattern != "" || len(table.RecommendedIndexes) > 0 {
				existing := filtered.TableEnrichments[key]
				existing.AccessPattern = table.AccessPattern
				existing.EstimatedRows = table.EstimatedRows
				existing.RecommendedIndexes = table.RecommendedIndexes
				existing.RecommendedPartitions = table.RecommendedPartitions
				filtered.TableEnrichments[key] = existing
			}
		}

		filtered.PerformanceHints = e.PerformanceHints
	}

	if categorySet[EnrichmentCategoryDataQuality] {
		// Include data quality metrics
		for key, column := range e.ColumnEnrichments {
			if column.DataQualityScore != nil || column.CompletenessScore != nil {
				existing := filtered.ColumnEnrichments[key]
				existing.DataQualityScore = column.DataQualityScore
				existing.CompletenessScore = column.CompletenessScore
				existing.ConsistencyScore = column.ConsistencyScore
				filtered.ColumnEnrichments[key] = existing
			}
		}
	}

	return filtered
}

// MergeEnrichment merges another enrichment into this one
func (e *UnifiedModelEnrichment) MergeEnrichment(other *UnifiedModelEnrichment) error {
	if other == nil {
		return fmt.Errorf("cannot merge nil enrichment")
	}

	if e.SchemaID != other.SchemaID {
		return fmt.Errorf("cannot merge enrichments for different schemas: %s vs %s", e.SchemaID, other.SchemaID)
	}

	// Update metadata to latest
	if other.GeneratedAt.After(e.GeneratedAt) {
		e.GeneratedAt = other.GeneratedAt
		e.EnrichmentVersion = other.EnrichmentVersion
		e.GeneratedBy = other.GeneratedBy
	}

	// Merge table enrichments
	for key, enrichment := range other.TableEnrichments {
		e.TableEnrichments[key] = enrichment
	}

	// Merge column enrichments
	for key, enrichment := range other.ColumnEnrichments {
		e.ColumnEnrichments[key] = enrichment
	}

	// Merge index enrichments
	for key, enrichment := range other.IndexEnrichments {
		e.IndexEnrichments[key] = enrichment
	}

	// Merge view enrichments
	for key, enrichment := range other.ViewEnrichments {
		e.ViewEnrichments[key] = enrichment
	}

	// Merge node enrichments
	for key, enrichment := range other.NodeEnrichments {
		e.NodeEnrichments[key] = enrichment
	}

	// Merge relationship enrichments
	for key, enrichment := range other.RelationshipEnrichments {
		e.RelationshipEnrichments[key] = enrichment
	}

	// Merge collection enrichments
	for key, enrichment := range other.CollectionEnrichments {
		e.CollectionEnrichments[key] = enrichment
	}

	// Merge document enrichments
	for key, enrichment := range other.DocumentEnrichments {
		e.DocumentEnrichments[key] = enrichment
	}

	// Merge global enrichments (take the latest)
	e.ComplianceSummary = other.ComplianceSummary
	e.RiskAssessment = other.RiskAssessment
	e.Recommendations = append(e.Recommendations, other.Recommendations...)
	e.PerformanceHints = append(e.PerformanceHints, other.PerformanceHints...)

	return nil
}

// ValidateEnrichment validates the enrichment structure for consistency
func (e *UnifiedModelEnrichment) ValidateEnrichment() []string {
	var warnings []string

	// Validate schema ID
	if e.SchemaID == "" {
		warnings = append(warnings, "schema_id is required")
	}

	// Validate enrichment version
	if e.EnrichmentVersion == "" {
		warnings = append(warnings, "enrichment_version is required")
	}

	// Validate generated timestamp
	if e.GeneratedAt.IsZero() {
		warnings = append(warnings, "generated_at timestamp is required")
	}

	// Validate confidence scores
	for key, column := range e.ColumnEnrichments {
		if column.PrivilegedConfidence < 0.0 || column.PrivilegedConfidence > 1.0 {
			warnings = append(warnings, fmt.Sprintf("invalid privileged_confidence for column %s: %f", key, column.PrivilegedConfidence))
		}

		if column.DataQualityScore != nil && (*column.DataQualityScore < 0.0 || *column.DataQualityScore > 1.0) {
			warnings = append(warnings, fmt.Sprintf("invalid data_quality_score for column %s: %f", key, *column.DataQualityScore))
		}
	}

	for key, table := range e.TableEnrichments {
		if table.ClassificationConfidence < 0.0 || table.ClassificationConfidence > 1.0 {
			warnings = append(warnings, fmt.Sprintf("invalid classification_confidence for table %s: %f", key, table.ClassificationConfidence))
		}

		if table.DataSensitivity < 0.0 || table.DataSensitivity > 1.0 {
			warnings = append(warnings, fmt.Sprintf("invalid data_sensitivity for table %s: %f", key, table.DataSensitivity))
		}
	}

	// Validate risk assessment scores
	if e.RiskAssessment.OverallRiskScore < 0.0 || e.RiskAssessment.OverallRiskScore > 1.0 {
		warnings = append(warnings, fmt.Sprintf("invalid overall_risk_score: %f", e.RiskAssessment.OverallRiskScore))
	}

	if e.ComplianceSummary.ComplianceScore < 0.0 || e.ComplianceSummary.ComplianceScore > 1.0 {
		warnings = append(warnings, fmt.Sprintf("invalid compliance_score: %f", e.ComplianceSummary.ComplianceScore))
	}

	return warnings
}

// GetEnrichmentSummary returns a summary of the enrichment data
func (e *UnifiedModelEnrichment) GetEnrichmentSummary() EnrichmentSummary {
	privilegedColumns := 0
	privilegedTables := 0
	totalColumns := len(e.ColumnEnrichments)
	totalTables := len(e.TableEnrichments)

	for _, column := range e.ColumnEnrichments {
		if column.IsPrivilegedData {
			privilegedColumns++
		}
	}

	for _, table := range e.TableEnrichments {
		if table.HasPrivilegedData {
			privilegedTables++
		}
	}

	return EnrichmentSummary{
		SchemaID:             e.SchemaID,
		TotalTables:          totalTables,
		TotalColumns:         totalColumns,
		PrivilegedTables:     privilegedTables,
		PrivilegedColumns:    privilegedColumns,
		ComplianceFrameworks: e.GetComplianceRequirements(),
		OverallRiskLevel:     e.ComplianceSummary.OverallRiskLevel,
		HasRecommendations:   len(e.Recommendations) > 0,
		HasPerformanceHints:  len(e.PerformanceHints) > 0,
		LastUpdated:          e.GeneratedAt,
	}
}

// EnrichmentSummary provides a high-level overview of enrichment data
type EnrichmentSummary struct {
	SchemaID             string                `json:"schema_id"`
	TotalTables          int                   `json:"total_tables"`
	TotalColumns         int                   `json:"total_columns"`
	PrivilegedTables     int                   `json:"privileged_tables"`
	PrivilegedColumns    int                   `json:"privileged_columns"`
	ComplianceFrameworks []ComplianceFramework `json:"compliance_frameworks"`
	OverallRiskLevel     RiskLevel             `json:"overall_risk_level"`
	HasRecommendations   bool                  `json:"has_recommendations"`
	HasPerformanceHints  bool                  `json:"has_performance_hints"`
	LastUpdated          time.Time             `json:"last_updated"`
}

// GetTablesWithPrivilegedData returns table names that contain privileged data
func (e *UnifiedModelEnrichment) GetTablesWithPrivilegedData() []string {
	var tables []string

	for tableName, table := range e.TableEnrichments {
		if table.HasPrivilegedData {
			tables = append(tables, tableName)
		}
	}

	return tables
}

// GetColumnsWithPrivilegedData returns column keys that contain privileged data
func (e *UnifiedModelEnrichment) GetColumnsWithPrivilegedData() []string {
	var columns []string

	for columnKey, column := range e.ColumnEnrichments {
		if column.IsPrivilegedData {
			columns = append(columns, columnKey)
		}
	}

	return columns
}

// GetRecommendationsByType returns recommendations filtered by type
func (e *UnifiedModelEnrichment) GetRecommendationsByType(recommendationType RecommendationType) []Recommendation {
	var filtered []Recommendation

	for _, rec := range e.Recommendations {
		if rec.Type == recommendationType {
			filtered = append(filtered, rec)
		}
	}

	return filtered
}

// GetRecommendationsByPriority returns recommendations filtered by priority
func (e *UnifiedModelEnrichment) GetRecommendationsByPriority(priority ConversionPriority) []Recommendation {
	var filtered []Recommendation

	for _, rec := range e.Recommendations {
		if rec.Priority == priority {
			filtered = append(filtered, rec)
		}
	}

	return filtered
}
