package matching

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/classifier"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/detection"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator"
)

// EnrichedColumnStructure represents an enriched column with classification and privileged data info
type EnrichedColumnStructure struct {
	TableName       string   `json:"tableName"`
	Name            string   `json:"name"`
	DataType        string   `json:"dataType"`
	IsNullable      bool     `json:"isNullable"`
	IsPrimaryKey    bool     `json:"isPrimaryKey"`
	IsForeignKey    bool     `json:"isForeignKey"`
	IsArray         bool     `json:"isArray"`
	IsUnique        bool     `json:"isUnique"`
	IsAutoIncrement bool     `json:"isAutoIncrement"`
	ColumnDefault   *string  `json:"columnDefault"`
	VarcharLength   *int     `json:"varcharLength"`
	Indexes         []string `json:"indexes"`
	// Privileged data detection fields
	IsPrivilegedData      bool    `json:"isPrivilegedData"`
	DataCategory          string  `json:"dataCategory"`
	PrivilegedConfidence  float64 `json:"privilegedConfidence"`
	PrivilegedDescription string  `json:"privilegedDescription"`
}

// EnrichedTableStructure represents an enriched table with classification info
type EnrichedTableStructure struct {
	Engine     string                    `json:"engine"`
	Schema     string                    `json:"schema"`
	Name       string                    `json:"name"`
	TableType  string                    `json:"tableType"`
	Columns    []EnrichedColumnStructure `json:"columns"`
	Properties map[string]string         `json:"properties"`
	// Classification fields
	PrimaryCategory          string          `json:"primaryCategory"`
	ClassificationConfidence float64         `json:"classificationConfidence"`
	ClassificationScores     []CategoryScore `json:"classificationScores"`
}

// CategoryScore represents a classification category and its score
type CategoryScore struct {
	Category string  `json:"category"`
	Score    float64 `json:"score"`
	Reason   string  `json:"reason"`
}

// EnrichedColumnMatchResult represents the result of matching two enriched columns
type EnrichedColumnMatchResult struct {
	SourceTable              string  `json:"sourceTable"`
	TargetTable              string  `json:"targetTable"`
	SourceColumn             string  `json:"sourceColumn"`
	TargetColumn             string  `json:"targetColumn"`
	Score                    float64 `json:"score"`
	IsTypeCompatible         bool    `json:"isTypeCompatible"`
	IsPoorMatch              bool    `json:"isPoorMatch"`
	IsUnmatched              bool    `json:"isUnmatched"`
	PrivilegedDataMatch      bool    `json:"privilegedDataMatch"`
	DataCategoryMatch        string  `json:"dataCategoryMatch"`
	PrivilegedConfidenceDiff float64 `json:"privilegedConfidenceDiff"`
}

// EnrichedTableMatchResult represents the result of matching two enriched tables
type EnrichedTableMatchResult struct {
	SourceTable                  string                      `json:"sourceTable"`
	TargetTable                  string                      `json:"targetTable"`
	Score                        float64                     `json:"score"`
	IsPoorMatch                  bool                        `json:"isPoorMatch"`
	IsUnmatched                  bool                        `json:"isUnmatched"`
	ClassificationMatch          string                      `json:"classificationMatch"`
	ClassificationConfidenceDiff float64                     `json:"classificationConfidenceDiff"`
	MatchedColumns               int                         `json:"matchedColumns"`
	TotalSourceColumns           int                         `json:"totalSourceColumns"`
	TotalTargetColumns           int                         `json:"totalTargetColumns"`
	ColumnMatches                []EnrichedColumnMatchResult `json:"columnMatches"`
}

// EnrichedMatchOptions configures the enhanced matching algorithm
type EnrichedMatchOptions struct {
	NameSimilarityThreshold  float64 `json:"nameSimilarityThreshold"`
	PoorMatchThreshold       float64 `json:"poorMatchThreshold"`
	NameWeight               float64 `json:"nameWeight"`
	TypeWeight               float64 `json:"typeWeight"`
	ClassificationWeight     float64 `json:"classificationWeight"`
	PrivilegedDataWeight     float64 `json:"privilegedDataWeight"`
	TableStructureWeight     float64 `json:"tableStructureWeight"`
	EnableCrossTableMatching bool    `json:"enableCrossTableMatching"`
}

// DefaultEnrichedMatchOptions returns default enhanced matching options
func DefaultEnrichedMatchOptions() EnrichedMatchOptions {
	return EnrichedMatchOptions{
		NameSimilarityThreshold:  0.3,
		PoorMatchThreshold:       0.4,
		NameWeight:               0.4,
		TypeWeight:               0.2,
		ClassificationWeight:     0.2,
		PrivilegedDataWeight:     0.15,
		TableStructureWeight:     0.05,
		EnableCrossTableMatching: true,
	}
}

// EnrichedSchemaMatcher handles enhanced matching of database schemas
type EnrichedSchemaMatcher struct {
	detector   *detection.PrivilegedDataDetector
	classifier *classifier.TableClassifier
}

// NewEnrichedSchemaMatcher creates a new enhanced schema matcher
func NewEnrichedSchemaMatcher() *EnrichedSchemaMatcher {
	return &EnrichedSchemaMatcher{
		detector:   detection.NewPrivilegedDataDetector(),
		classifier: classifier.NewTableClassifier(),
	}
}

// MatchSchemasEnriched performs enhanced matching between two schemas
func (m *EnrichedSchemaMatcher) MatchSchemasEnriched(sourceSchemaType string, sourceSchema json.RawMessage,
	targetSchemaType string, targetSchema json.RawMessage, options *EnrichedMatchOptions) (*EnrichedSchemaMatchResult, error) {

	if options == nil {
		defaultOptions := DefaultEnrichedMatchOptions()
		options = &defaultOptions
	}

	// Convert schemas to enriched table structures
	sourceTables, sourceWarnings, err := m.convertToEnrichedTables(sourceSchemaType, sourceSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to convert source schema: %w", err)
	}

	targetTables, targetWarnings, err := m.convertToEnrichedTables(targetSchemaType, targetSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to convert target schema: %w", err)
	}

	warnings := append(sourceWarnings, targetWarnings...)

	// Perform table matching
	tableMatches, unmatchedColumns := m.matchTables(sourceTables, targetTables, options)

	// Calculate overall similarity score
	overallScore := m.calculateOverallSimilarity(tableMatches)

	return &EnrichedSchemaMatchResult{
		TableMatches:      tableMatches,
		UnmatchedColumns:  unmatchedColumns,
		Warnings:          warnings,
		OverallSimilarity: overallScore,
	}, nil
}

// MatchTablesEnriched performs enhanced matching between enriched table metadata
func (m *EnrichedSchemaMatcher) MatchTablesEnriched(sourceTables, targetTables []EnrichedTableStructure,
	options *EnrichedMatchOptions) ([]EnrichedTableMatchResult, error) {

	if options == nil {
		defaultOptions := DefaultEnrichedMatchOptions()
		options = &defaultOptions
	}

	matches, _ := m.matchTables(sourceTables, targetTables, options)

	return matches, nil
}

// matchTables performs table-level matching
func (m *EnrichedSchemaMatcher) matchTables(sourceTables, targetTables []EnrichedTableStructure,
	options *EnrichedMatchOptions) ([]EnrichedTableMatchResult, []EnrichedColumnMatchResult) {

	var tableMatches []EnrichedTableMatchResult
	var unmatchedColumns []EnrichedColumnMatchResult
	usedTargetTables := make(map[int]bool)

	// Calculate scores for all table pairs
	type tableScore struct {
		sourceIndex int
		targetIndex int
		score       float64
	}

	var tableScores []tableScore
	for sourceIndex, sourceTable := range sourceTables {
		for targetIndex, targetTable := range targetTables {
			score := m.calculateTableSimilarity(sourceTable, targetTable, options)
			tableScores = append(tableScores, tableScore{
				sourceIndex: sourceIndex,
				targetIndex: targetIndex,
				score:       score,
			})
		}
	}

	// Sort table scores in descending order
	sort.Slice(tableScores, func(i, j int) bool {
		return tableScores[i].score > tableScores[j].score
	})

	// Match tables based on scores
	for _, score := range tableScores {
		if !usedTargetTables[score.targetIndex] && score.score >= options.NameSimilarityThreshold {
			sourceTable := sourceTables[score.sourceIndex]
			targetTable := targetTables[score.targetIndex]

			// Perform column matching within matched tables
			columnMatches := m.matchColumnsWithinTables(sourceTable, targetTable, options)

			tableMatch := EnrichedTableMatchResult{
				SourceTable:                  sourceTable.Name,
				TargetTable:                  targetTable.Name,
				Score:                        score.score,
				IsPoorMatch:                  score.score < options.PoorMatchThreshold,
				IsUnmatched:                  false,
				ClassificationMatch:          m.getClassificationMatch(sourceTable, targetTable),
				ClassificationConfidenceDiff: math.Abs(sourceTable.ClassificationConfidence - targetTable.ClassificationConfidence),
				MatchedColumns:               len(columnMatches),
				TotalSourceColumns:           len(sourceTable.Columns),
				TotalTargetColumns:           len(targetTable.Columns),
				ColumnMatches:                columnMatches,
			}

			tableMatches = append(tableMatches, tableMatch)
			usedTargetTables[score.targetIndex] = true
		}
	}

	// Handle unmatched tables
	for _, sourceTable := range sourceTables {
		isMatched := false
		for _, match := range tableMatches {
			if match.SourceTable == sourceTable.Name {
				isMatched = true
				break
			}
		}
		if !isMatched {
			// Add unmatched columns from unmatched source tables
			for _, column := range sourceTable.Columns {
				unmatchedColumns = append(unmatchedColumns, EnrichedColumnMatchResult{
					SourceTable:              sourceTable.Name,
					TargetTable:              "",
					SourceColumn:             column.Name,
					TargetColumn:             "",
					Score:                    0,
					IsTypeCompatible:         false,
					IsPoorMatch:              true,
					IsUnmatched:              true,
					PrivilegedDataMatch:      false,
					DataCategoryMatch:        "",
					PrivilegedConfidenceDiff: 0,
				})
			}
		}
	}

	// Add unmatched target tables
	for targetIndex, targetTable := range targetTables {
		if !usedTargetTables[targetIndex] {
			for _, column := range targetTable.Columns {
				unmatchedColumns = append(unmatchedColumns, EnrichedColumnMatchResult{
					SourceTable:              "",
					TargetTable:              targetTable.Name,
					SourceColumn:             "",
					TargetColumn:             column.Name,
					Score:                    0,
					IsTypeCompatible:         false,
					IsPoorMatch:              true,
					IsUnmatched:              true,
					PrivilegedDataMatch:      false,
					DataCategoryMatch:        "",
					PrivilegedConfidenceDiff: 0,
				})
			}
		}
	}

	return tableMatches, unmatchedColumns
}

// calculateTableSimilarity calculates similarity between two tables
func (m *EnrichedSchemaMatcher) calculateTableSimilarity(sourceTable, targetTable EnrichedTableStructure,
	options *EnrichedMatchOptions) float64 {

	// Name similarity
	nameSimilarity := calculateNameSimilarity(sourceTable.Name, targetTable.Name)

	// Classification similarity
	classificationSimilarity := m.calculateClassificationSimilarity(sourceTable, targetTable)

	// Structure similarity (based on column count and types)
	structureSimilarity := m.calculateStructureSimilarity(sourceTable, targetTable)

	// Weighted score
	score := (nameSimilarity * options.NameWeight) +
		(classificationSimilarity * options.ClassificationWeight) +
		(structureSimilarity * options.TableStructureWeight)

	// Normalize weights if they don't sum to 1
	totalWeight := options.NameWeight + options.ClassificationWeight + options.TableStructureWeight
	if totalWeight > 0 {
		score = score / totalWeight
	}

	return score
}

// calculateClassificationSimilarity calculates similarity based on table classification
func (m *EnrichedSchemaMatcher) calculateClassificationSimilarity(sourceTable, targetTable EnrichedTableStructure) float64 {
	if sourceTable.PrimaryCategory == "" || targetTable.PrimaryCategory == "" {
		return 0.0
	}

	if sourceTable.PrimaryCategory == targetTable.PrimaryCategory {
		// Same primary category - use confidence difference
		confidenceDiff := math.Abs(sourceTable.ClassificationConfidence - targetTable.ClassificationConfidence)
		return 1.0 - (confidenceDiff * 0.5) // Reduce score based on confidence difference
	}

	// Different primary categories - check for secondary matches
	for _, sourceScore := range sourceTable.ClassificationScores {
		if sourceScore.Category == targetTable.PrimaryCategory {
			return sourceScore.Score * 0.7 // Secondary match gets 70% of the score
		}
	}

	return 0.0
}

// calculateStructureSimilarity calculates similarity based on table structure
func (m *EnrichedSchemaMatcher) calculateStructureSimilarity(sourceTable, targetTable EnrichedTableStructure) float64 {
	sourceColumnCount := len(sourceTable.Columns)
	targetColumnCount := len(targetTable.Columns)

	if sourceColumnCount == 0 && targetColumnCount == 0 {
		return 1.0
	}

	// Column count similarity
	maxColumns := math.Max(float64(sourceColumnCount), float64(targetColumnCount))
	minColumns := math.Min(float64(sourceColumnCount), float64(targetColumnCount))
	countSimilarity := minColumns / maxColumns

	// Data type distribution similarity
	sourceTypeDistribution := m.getDataTypeDistribution(sourceTable.Columns)
	targetTypeDistribution := m.getDataTypeDistribution(targetTable.Columns)
	typeSimilarity := m.calculateDistributionSimilarity(sourceTypeDistribution, targetTypeDistribution)

	return (countSimilarity + typeSimilarity) / 2.0
}

// getDataTypeDistribution returns the distribution of data types in columns
func (m *EnrichedSchemaMatcher) getDataTypeDistribution(columns []EnrichedColumnStructure) map[string]float64 {
	distribution := make(map[string]float64)
	total := float64(len(columns))

	if total == 0 {
		return distribution
	}

	for _, column := range columns {
		dataType := normalizeString(column.DataType)
		distribution[dataType]++
	}

	// Normalize to percentages
	for dataType := range distribution {
		distribution[dataType] = distribution[dataType] / total
	}

	return distribution
}

// calculateDistributionSimilarity calculates similarity between two distributions
func (m *EnrichedSchemaMatcher) calculateDistributionSimilarity(dist1, dist2 map[string]float64) float64 {
	// Get all unique keys
	allKeys := make(map[string]bool)
	for key := range dist1 {
		allKeys[key] = true
	}
	for key := range dist2 {
		allKeys[key] = true
	}

	if len(allKeys) == 0 {
		return 1.0
	}

	// Calculate cosine similarity
	var dotProduct, norm1, norm2 float64
	for key := range allKeys {
		val1 := dist1[key]
		val2 := dist2[key]

		dotProduct += val1 * val2
		norm1 += val1 * val1
		norm2 += val2 * val2
	}

	if norm1 == 0 || norm2 == 0 {
		return 0.0
	}

	return dotProduct / (math.Sqrt(norm1) * math.Sqrt(norm2))
}

// matchColumnsWithinTables performs column matching within matched tables
func (m *EnrichedSchemaMatcher) matchColumnsWithinTables(sourceTable, targetTable EnrichedTableStructure,
	options *EnrichedMatchOptions) []EnrichedColumnMatchResult {

	var matches []EnrichedColumnMatchResult
	usedTargetColumns := make(map[int]bool)

	// Calculate scores for all column pairs
	type columnScore struct {
		sourceIndex int
		targetIndex int
		score       float64
	}

	var columnScores []columnScore
	for sourceIndex, sourceColumn := range sourceTable.Columns {
		for targetIndex, targetColumn := range targetTable.Columns {
			score := m.calculateEnrichedColumnSimilarity(sourceColumn, targetColumn, options)
			columnScores = append(columnScores, columnScore{
				sourceIndex: sourceIndex,
				targetIndex: targetIndex,
				score:       score,
			})
		}
	}

	// Sort column scores in descending order
	sort.Slice(columnScores, func(i, j int) bool {
		return columnScores[i].score > columnScores[j].score
	})

	// Match columns based on scores
	for _, score := range columnScores {
		if !usedTargetColumns[score.targetIndex] && score.score >= options.NameSimilarityThreshold {
			sourceColumn := sourceTable.Columns[score.sourceIndex]
			targetColumn := targetTable.Columns[score.targetIndex]

			match := EnrichedColumnMatchResult{
				SourceTable:              sourceTable.Name,
				TargetTable:              targetTable.Name,
				SourceColumn:             sourceColumn.Name,
				TargetColumn:             targetColumn.Name,
				Score:                    score.score,
				IsTypeCompatible:         areTypesCompatible(sourceColumn.DataType, targetColumn.DataType),
				IsPoorMatch:              score.score < options.PoorMatchThreshold,
				IsUnmatched:              false,
				PrivilegedDataMatch:      sourceColumn.IsPrivilegedData && targetColumn.IsPrivilegedData,
				DataCategoryMatch:        m.getDataCategoryMatch(sourceColumn, targetColumn),
				PrivilegedConfidenceDiff: math.Abs(sourceColumn.PrivilegedConfidence - targetColumn.PrivilegedConfidence),
			}

			matches = append(matches, match)
			usedTargetColumns[score.targetIndex] = true
		}
	}

	return matches
}

// calculateEnrichedColumnSimilarity calculates enhanced similarity between two columns
func (m *EnrichedSchemaMatcher) calculateEnrichedColumnSimilarity(sourceColumn, targetColumn EnrichedColumnStructure,
	options *EnrichedMatchOptions) float64 {

	// Name similarity
	nameSimilarity := calculateNameSimilarity(sourceColumn.Name, targetColumn.Name)

	// Type similarity
	typeSimilarity := calculateTypeSimilarity(sourceColumn.DataType, targetColumn.DataType)

	// Privileged data similarity
	privilegedSimilarity := m.calculatePrivilegedDataSimilarity(sourceColumn, targetColumn)

	// Weighted score
	score := (nameSimilarity * options.NameWeight) +
		(typeSimilarity * options.TypeWeight) +
		(privilegedSimilarity * options.PrivilegedDataWeight)

	// Normalize weights
	totalWeight := options.NameWeight + options.TypeWeight + options.PrivilegedDataWeight
	if totalWeight > 0 {
		score = score / totalWeight
	}

	return score
}

// calculatePrivilegedDataSimilarity calculates similarity based on privileged data classification
func (m *EnrichedSchemaMatcher) calculatePrivilegedDataSimilarity(sourceColumn, targetColumn EnrichedColumnStructure) float64 {
	// Both have privileged data
	if sourceColumn.IsPrivilegedData && targetColumn.IsPrivilegedData {
		if sourceColumn.DataCategory == targetColumn.DataCategory {
			// Same category - use confidence difference
			confidenceDiff := math.Abs(sourceColumn.PrivilegedConfidence - targetColumn.PrivilegedConfidence)
			return 1.0 - (confidenceDiff * 0.3) // Reduce score based on confidence difference
		}
		// Different categories but both privileged
		return 0.6
	}

	// Neither has privileged data
	if !sourceColumn.IsPrivilegedData && !targetColumn.IsPrivilegedData {
		return 0.8 // High similarity for non-privileged columns
	}

	// One has privileged data, one doesn't
	return 0.2
}

// getClassificationMatch returns the classification match type
func (m *EnrichedSchemaMatcher) getClassificationMatch(sourceTable, targetTable EnrichedTableStructure) string {
	if sourceTable.PrimaryCategory == "" || targetTable.PrimaryCategory == "" {
		return "unknown"
	}

	if sourceTable.PrimaryCategory == targetTable.PrimaryCategory {
		return "exact"
	}

	// Check for secondary matches
	for _, sourceScore := range sourceTable.ClassificationScores {
		if sourceScore.Category == targetTable.PrimaryCategory {
			return "partial"
		}
	}

	return "different"
}

// getDataCategoryMatch returns the data category match type
func (m *EnrichedSchemaMatcher) getDataCategoryMatch(sourceColumn, targetColumn EnrichedColumnStructure) string {
	if !sourceColumn.IsPrivilegedData && !targetColumn.IsPrivilegedData {
		return "none"
	}

	if sourceColumn.IsPrivilegedData && targetColumn.IsPrivilegedData {
		if sourceColumn.DataCategory == targetColumn.DataCategory {
			return "exact"
		}
		return "different_category"
	}

	return "partial"
}

// calculateOverallSimilarity calculates overall similarity score for schema matching
func (m *EnrichedSchemaMatcher) calculateOverallSimilarity(tableMatches []EnrichedTableMatchResult) float64 {
	if len(tableMatches) == 0 {
		return 0.0
	}

	var totalScore float64
	var totalTables int

	for _, match := range tableMatches {
		if !match.IsUnmatched {
			totalScore += match.Score
			totalTables++
		}
	}

	if totalTables == 0 {
		return 0.0
	}

	return totalScore / float64(totalTables)
}

// convertToEnrichedTables converts schema to enriched table structures
func (m *EnrichedSchemaMatcher) convertToEnrichedTables(schemaType string, schema json.RawMessage) ([]EnrichedTableStructure, []string, error) {
	// Create translator and register adapters
	translatorInstance := translator.NewSchemaTranslator()
	m.registerAdapters(translatorInstance)

	// Translate to unified model
	translationResult, err := translatorInstance.Translate(schemaType, "unified", schema)
	if err != nil {
		return nil, nil, fmt.Errorf("schema translation failed: %w", err)
	}

	// Run privileged data detection
	detectionResult, err := m.detector.DetectPrivilegedData(schemaType, schema)
	if err != nil {
		return nil, translationResult.Warnings, fmt.Errorf("privileged data detection failed: %w", err)
	}

	// Extract unified model from translation result
	unifiedModel, ok := translationResult.ConvertedStructure.(struct {
		SchemaType string `json:"schemaType"`
		*models.UnifiedModel
	})
	if !ok {
		return nil, translationResult.Warnings, fmt.Errorf("failed to extract unified model from translation result")
	}

	// Create privileged data lookup map
	privilegedDataMap := make(map[string]map[string]*detection.PrivilegedDataFinding)
	for _, finding := range detectionResult.Findings {
		if _, exists := privilegedDataMap[finding.TableName]; !exists {
			privilegedDataMap[finding.TableName] = make(map[string]*detection.PrivilegedDataFinding)
		}
		privilegedDataMap[finding.TableName][finding.ColumnName] = &finding
	}

	// Convert to enriched table structures
	var enrichedTables []EnrichedTableStructure
	allWarnings := append(translationResult.Warnings, detectionResult.Warnings...)

	for _, table := range unifiedModel.Tables {
		// Classify the table
		tableMetadata := m.convertToClassifierTableMetadata(table, schemaType)
		options := &classifier.ClassificationOptions{
			TopN:      3,
			Threshold: 0.1,
		}

		classificationResult, err := m.classifier.ClassifyTable(*tableMetadata, options)
		if err != nil {
			allWarnings = append(allWarnings, fmt.Sprintf("classification failed for table %s: %v", table.Name, err))
			classificationResult = &classifier.ClassificationResult{
				PrimaryCategory: "",
				Confidence:      0.0,
				Scores:          []classifier.CategoryScore{},
			}
		}

		// Convert classification scores
		var classificationScores []CategoryScore
		for _, score := range classificationResult.Scores {
			classificationScores = append(classificationScores, CategoryScore{
				Category: score.Category,
				Score:    score.Score,
				Reason:   score.Reason,
			})
		}

		// Convert columns to enriched structure
		var enrichedColumns []EnrichedColumnStructure
		for _, column := range table.Columns {
			enrichedColumn := EnrichedColumnStructure{
				TableName:       table.Name,
				Name:            column.Name,
				DataType:        column.DataType.Name,
				IsNullable:      column.IsNullable,
				IsPrimaryKey:    column.IsPrimaryKey,
				IsArray:         column.DataType.IsArray,
				IsUnique:        column.IsUnique,
				IsAutoIncrement: column.IsAutoIncrement,
			}

			// Add optional fields
			if column.DefaultValue != nil {
				enrichedColumn.ColumnDefault = column.DefaultValue
			}

			if column.DataType.Length > 0 {
				length := int(column.DataType.Length)
				enrichedColumn.VarcharLength = &length
			}

			// Add index information
			enrichedColumn.Indexes = m.extractColumnIndexes(column, table)
			enrichedColumn.IsForeignKey = m.isColumnForeignKey(column, table)

			// Add privileged data information
			if tablePrivileged, exists := privilegedDataMap[table.Name]; exists {
				if finding, exists := tablePrivileged[column.Name]; exists {
					enrichedColumn.IsPrivilegedData = true
					enrichedColumn.DataCategory = finding.DataCategory
					enrichedColumn.PrivilegedConfidence = finding.Confidence
					enrichedColumn.PrivilegedDescription = finding.Description
				}
			}

			enrichedColumns = append(enrichedColumns, enrichedColumn)
		}

		// Create enriched table structure
		enrichedTable := EnrichedTableStructure{
			Engine:                   schemaType,
			Schema:                   table.Schema,
			Name:                     table.Name,
			TableType:                table.TableType,
			Columns:                  enrichedColumns,
			Properties:               make(map[string]string),
			PrimaryCategory:          classificationResult.PrimaryCategory,
			ClassificationConfidence: classificationResult.Confidence,
			ClassificationScores:     classificationScores,
		}

		// Add table properties
		if table.Comment != "" {
			enrichedTable.Properties["comment"] = table.Comment
		}

		enrichedTables = append(enrichedTables, enrichedTable)
	}

	return enrichedTables, allWarnings, nil
}

// registerAdapters registers adapters for the translator
func (m *EnrichedSchemaMatcher) registerAdapters(translatorInstance *translator.SchemaTranslator) {
	// Register adapters for specific schema types if needed
	// For example, if you have a MySQL adapter:
	// translatorInstance.RegisterAdapter("mysql", &mysql.MySQLAdapter{})
	// translatorInstance.RegisterAdapter("postgres", &postgres.PostgresAdapter{})
	// translatorInstance.RegisterAdapter("sqlite", &sqlite.SQLiteAdapter{})
}

// convertToClassifierTableMetadata converts a unified model table to classifier metadata
func (m *EnrichedSchemaMatcher) convertToClassifierTableMetadata(table models.Table, schemaType string) *classifier.TableMetadata {
	// Convert columns to classifier column metadata
	var columns []classifier.ColumnMetadata
	for _, column := range table.Columns {
		colMetadata := classifier.ColumnMetadata{
			Name:            column.Name,
			DataType:        column.DataType.Name,
			IsNullable:      column.IsNullable,
			IsPrimaryKey:    column.IsPrimaryKey,
			IsUnique:        column.IsUnique,
			IsAutoIncrement: column.IsAutoIncrement,
			IsArray:         column.DataType.IsArray,
		}

		// Add optional fields
		if column.DefaultValue != nil {
			colMetadata.ColumnDefault = column.DefaultValue
		}

		if column.DataType.Length > 0 {
			length := int(column.DataType.Length)
			colMetadata.VarcharLength = &length
		}

		if column.DataType.IsArray && column.DataType.BaseType != "" {
			colMetadata.ArrayElementType = &column.DataType.BaseType
		}

		if column.DataType.CustomTypeName != "" {
			colMetadata.CustomTypeName = &column.DataType.CustomTypeName
		}

		columns = append(columns, colMetadata)
	}

	// Convert indexes to classifier index metadata
	var indexes []classifier.IndexMetadata
	for _, index := range table.Indexes {
		indexMetadata := classifier.IndexMetadata{
			Name:      index.Name,
			IsUnique:  index.IsUnique,
			IsPrimary: strings.Contains(strings.ToLower(index.IndexMethod), "primary"),
		}

		// Extract column names from index columns
		for _, indexCol := range index.Columns {
			indexMetadata.Columns = append(indexMetadata.Columns, indexCol.ColumnName)
		}

		indexes = append(indexes, indexMetadata)
	}

	// Convert constraints to string slice
	var constraints []string
	for _, constraint := range table.Constraints {
		constraints = append(constraints, constraint.Type)
	}

	// Create tags map for additional properties
	tags := make(map[string]string)
	if table.Comment != "" {
		tags["comment"] = table.Comment
	}
	tags["schema_type"] = schemaType
	tags["table_type"] = table.TableType

	metadata := &classifier.TableMetadata{
		Name:        table.Name,
		Columns:     columns,
		Indexes:     indexes,
		Constraints: constraints,
		Tags:        tags,
	}

	return metadata
}

// extractColumnIndexes extracts index information from a unified model column
func (m *EnrichedSchemaMatcher) extractColumnIndexes(column models.Column, table models.Table) []string {
	var indexes []string
	if column.IsPrimaryKey {
		indexes = append(indexes, "PK")
	}
	if column.IsUnique {
		indexes = append(indexes, "UQ")
	}
	if column.IsAutoIncrement {
		indexes = append(indexes, "AI")
	}

	// Check table-level indexes
	for _, index := range table.Indexes {
		for _, indexCol := range index.Columns {
			if indexCol.ColumnName == column.Name {
				indexes = append(indexes, index.Name)
				break
			}
		}
	}

	return indexes
}

// isColumnForeignKey checks if a column is a foreign key
func (m *EnrichedSchemaMatcher) isColumnForeignKey(column models.Column, table models.Table) bool {
	for _, constraint := range table.Constraints {
		if constraint.Type == "FOREIGN KEY" {
			for _, constraintCol := range constraint.Columns {
				if constraintCol == column.Name {
					return true
				}
			}
		}
	}
	return false
}

// EnrichedSchemaMatchResult represents the result of enhanced schema matching
type EnrichedSchemaMatchResult struct {
	TableMatches      []EnrichedTableMatchResult  `json:"tableMatches"`
	UnmatchedColumns  []EnrichedColumnMatchResult `json:"unmatchedColumns"`
	Warnings          []string                    `json:"warnings"`
	OverallSimilarity float64                     `json:"overallSimilarity"`
}
