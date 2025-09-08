package matching

import (
	"fmt"
	"math"
	"strings"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// UnifiedModelMatcher handles matching between UnifiedModel instances with enrichments
type UnifiedModelMatcher struct {
}

// NewUnifiedModelMatcher creates a new unified model matcher
func NewUnifiedModelMatcher() *UnifiedModelMatcher {
	return &UnifiedModelMatcher{}
}

// UnifiedMatchOptions configures the unified model matching algorithm
type UnifiedMatchOptions struct {
	NameSimilarityThreshold  float64 `json:"nameSimilarityThreshold"`
	PoorMatchThreshold       float64 `json:"poorMatchThreshold"`
	NameWeight               float64 `json:"nameWeight"`
	TypeWeight               float64 `json:"typeWeight"`
	ClassificationWeight     float64 `json:"classificationWeight"`
	PrivilegedDataWeight     float64 `json:"privilegedDataWeight"`
	TableStructureWeight     float64 `json:"tableStructureWeight"`
	EnableCrossTableMatching bool    `json:"enableCrossTableMatching"`
}

// DefaultUnifiedMatchOptions returns default unified matching options
func DefaultUnifiedMatchOptions() UnifiedMatchOptions {
	return UnifiedMatchOptions{
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

// UnifiedColumnMatch represents a column match result using shared types
type UnifiedColumnMatch struct {
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

// UnifiedTableMatch represents a table match result using shared types
type UnifiedTableMatch struct {
	SourceTable                  string               `json:"sourceTable"`
	TargetTable                  string               `json:"targetTable"`
	Score                        float64              `json:"score"`
	IsPoorMatch                  bool                 `json:"isPoorMatch"`
	IsUnmatched                  bool                 `json:"isUnmatched"`
	ClassificationMatch          string               `json:"classificationMatch"`
	ClassificationConfidenceDiff float64              `json:"classificationConfidenceDiff"`
	MatchedColumns               int                  `json:"matchedColumns"`
	TotalSourceColumns           int                  `json:"totalSourceColumns"`
	TotalTargetColumns           int                  `json:"totalTargetColumns"`
	ColumnMatches                []UnifiedColumnMatch `json:"columnMatches"`
}

// UnifiedMatchResult represents the complete matching result
type UnifiedMatchResult struct {
	TableMatches           []UnifiedTableMatch  `json:"tableMatches"`
	UnmatchedColumns       []UnifiedColumnMatch `json:"unmatchedColumns"`
	Warnings               []string             `json:"warnings"`
	OverallSimilarityScore float64              `json:"overallSimilarityScore"`
}

// MatchUnifiedModels performs matching between two UnifiedModel instances with their enrichments
func (m *UnifiedModelMatcher) MatchUnifiedModels(
	sourceModel *unifiedmodel.UnifiedModel,
	sourceEnrichment *unifiedmodel.UnifiedModelEnrichment,
	targetModel *unifiedmodel.UnifiedModel,
	targetEnrichment *unifiedmodel.UnifiedModelEnrichment,
	options *UnifiedMatchOptions,
) (*UnifiedMatchResult, error) {
	if sourceModel == nil {
		return nil, fmt.Errorf("source unified model cannot be nil")
	}
	if targetModel == nil {
		return nil, fmt.Errorf("target unified model cannot be nil")
	}
	if options == nil {
		defaultOptions := DefaultUnifiedMatchOptions()
		options = &defaultOptions
	}

	var warnings []string
	var tableMatches []UnifiedTableMatch
	var unmatchedColumns []UnifiedColumnMatch

	// Create table matching matrix
	sourceTableNames := make([]string, 0, len(sourceModel.Tables))
	for tableName := range sourceModel.Tables {
		sourceTableNames = append(sourceTableNames, tableName)
	}

	targetTableNames := make([]string, 0, len(targetModel.Tables))
	for tableName := range targetModel.Tables {
		targetTableNames = append(targetTableNames, tableName)
	}

	// Calculate table similarity matrix
	tableScores := make(map[string]map[string]float64)
	for _, sourceTableName := range sourceTableNames {
		tableScores[sourceTableName] = make(map[string]float64)
		sourceTable := sourceModel.Tables[sourceTableName]

		var sourceTableEnrichment *unifiedmodel.TableEnrichment
		if sourceEnrichment != nil {
			if enrichment, exists := sourceEnrichment.TableEnrichments[sourceTableName]; exists {
				sourceTableEnrichment = &enrichment
			}
		}

		for _, targetTableName := range targetTableNames {
			targetTable := targetModel.Tables[targetTableName]

			var targetTableEnrichment *unifiedmodel.TableEnrichment
			if targetEnrichment != nil {
				if enrichment, exists := targetEnrichment.TableEnrichments[targetTableName]; exists {
					targetTableEnrichment = &enrichment
				}
			}

			score := m.calculateTableSimilarity(
				sourceTable, sourceTableEnrichment,
				targetTable, targetTableEnrichment,
				options,
			)
			tableScores[sourceTableName][targetTableName] = score
		}
	}

	// Find best table matches using Hungarian algorithm (simplified greedy approach)
	usedTargetTables := make(map[string]bool)

	for _, sourceTableName := range sourceTableNames {
		bestTargetTable := ""
		bestScore := 0.0

		for targetTableName, score := range tableScores[sourceTableName] {
			if !usedTargetTables[targetTableName] && score > bestScore {
				bestScore = score
				bestTargetTable = targetTableName
			}
		}

		if bestTargetTable != "" && bestScore > 0.0 {
			usedTargetTables[bestTargetTable] = true

			// Create detailed table match
			sourceTable := sourceModel.Tables[sourceTableName]
			targetTable := targetModel.Tables[bestTargetTable]

			var sourceTableEnrichment *unifiedmodel.TableEnrichment
			if sourceEnrichment != nil {
				if enrichment, exists := sourceEnrichment.TableEnrichments[sourceTableName]; exists {
					sourceTableEnrichment = &enrichment
				}
			}

			var targetTableEnrichment *unifiedmodel.TableEnrichment
			if targetEnrichment != nil {
				if enrichment, exists := targetEnrichment.TableEnrichments[bestTargetTable]; exists {
					targetTableEnrichment = &enrichment
				}
			}

			tableMatch := m.createTableMatch(
				sourceTableName, sourceTable, sourceTableEnrichment,
				bestTargetTable, targetTable, targetTableEnrichment,
				sourceEnrichment, targetEnrichment,
				options,
			)

			tableMatches = append(tableMatches, tableMatch)
		}
	}

	// Calculate overall similarity score
	overallScore := m.calculateOverallSimilarity(tableMatches, len(sourceModel.Tables), len(targetModel.Tables))

	return &UnifiedMatchResult{
		TableMatches:           tableMatches,
		UnmatchedColumns:       unmatchedColumns,
		Warnings:               warnings,
		OverallSimilarityScore: overallScore,
	}, nil
}

// calculateTableSimilarity calculates similarity between two tables with enrichments
func (m *UnifiedModelMatcher) calculateTableSimilarity(
	sourceTable unifiedmodel.Table,
	sourceEnrichment *unifiedmodel.TableEnrichment,
	targetTable unifiedmodel.Table,
	targetEnrichment *unifiedmodel.TableEnrichment,
	options *UnifiedMatchOptions,
) float64 {
	// Name similarity
	nameScore := m.calculateStringSimilarity(sourceTable.Name, targetTable.Name)

	// Structure similarity (column count, types)
	structureScore := m.calculateStructureSimilarity(sourceTable, targetTable)

	// Classification similarity (if enrichments available)
	classificationScore := 0.0
	if sourceEnrichment != nil && targetEnrichment != nil {
		classificationScore = m.calculateClassificationSimilarity(sourceEnrichment, targetEnrichment)
	}

	// Weighted combination - ensure weights are normalized
	totalWeight := options.NameWeight + options.TableStructureWeight + options.ClassificationWeight
	if totalWeight == 0 {
		totalWeight = 1.0
	}

	totalScore := (nameScore*options.NameWeight +
		structureScore*options.TableStructureWeight +
		classificationScore*options.ClassificationWeight) / totalWeight

	return math.Min(1.0, totalScore)
}

// calculateStringSimilarity calculates similarity between two strings using Levenshtein distance
func (m *UnifiedModelMatcher) calculateStringSimilarity(s1, s2 string) float64 {
	s1Lower := strings.ToLower(s1)
	s2Lower := strings.ToLower(s2)

	if s1Lower == s2Lower {
		return 1.0
	}

	// Simple substring matching for now
	if strings.Contains(s1Lower, s2Lower) || strings.Contains(s2Lower, s1Lower) {
		shorter := len(s1Lower)
		if len(s2Lower) < shorter {
			shorter = len(s2Lower)
		}
		longer := len(s1Lower)
		if len(s2Lower) > longer {
			longer = len(s2Lower)
		}
		return float64(shorter) / float64(longer)
	}

	// Check for common words/patterns
	if strings.Contains(s1Lower, "user") && strings.Contains(s2Lower, "user") {
		return 0.7
	}

	if strings.Contains(s1Lower, "email") && strings.Contains(s2Lower, "email") {
		return 0.8
	}

	if strings.Contains(s1Lower, "id") && strings.Contains(s2Lower, "id") {
		return 0.6
	}

	return 0.0
}

// calculateStructureSimilarity calculates structural similarity between tables
func (m *UnifiedModelMatcher) calculateStructureSimilarity(sourceTable, targetTable unifiedmodel.Table) float64 {
	sourceColumnCount := len(sourceTable.Columns)
	targetColumnCount := len(targetTable.Columns)

	if sourceColumnCount == 0 && targetColumnCount == 0 {
		return 1.0
	}

	// Column count similarity
	countDiff := math.Abs(float64(sourceColumnCount - targetColumnCount))
	maxCount := math.Max(float64(sourceColumnCount), float64(targetColumnCount))
	countSimilarity := 1.0 - (countDiff / maxCount)

	// Type similarity (count of matching data types)
	sourceTypes := make(map[string]int)
	for _, col := range sourceTable.Columns {
		sourceTypes[col.DataType]++
	}

	targetTypes := make(map[string]int)
	for _, col := range targetTable.Columns {
		targetTypes[col.DataType]++
	}

	matchingTypes := 0
	totalTypes := 0
	for dataType, sourceCount := range sourceTypes {
		targetCount := targetTypes[dataType]
		matchingTypes += int(math.Min(float64(sourceCount), float64(targetCount)))
		totalTypes += sourceCount
	}
	for dataType, targetCount := range targetTypes {
		if _, exists := sourceTypes[dataType]; !exists {
			totalTypes += targetCount
		}
	}

	typeSimilarity := 0.0
	if totalTypes > 0 {
		typeSimilarity = float64(matchingTypes) / float64(totalTypes)
	}

	return (countSimilarity + typeSimilarity) / 2.0
}

// calculateClassificationSimilarity calculates similarity between table classifications
func (m *UnifiedModelMatcher) calculateClassificationSimilarity(
	sourceEnrichment, targetEnrichment *unifiedmodel.TableEnrichment,
) float64 {
	// Primary category match
	categoryMatch := 0.0
	if sourceEnrichment.PrimaryCategory == targetEnrichment.PrimaryCategory {
		categoryMatch = 1.0
	}

	// Confidence similarity
	confidenceDiff := math.Abs(sourceEnrichment.ClassificationConfidence - targetEnrichment.ClassificationConfidence)
	confidenceSimilarity := 1.0 - confidenceDiff

	// Access pattern similarity
	accessPatternMatch := 0.0
	if sourceEnrichment.AccessPattern == targetEnrichment.AccessPattern {
		accessPatternMatch = 1.0
	}

	return (categoryMatch + confidenceSimilarity + accessPatternMatch) / 3.0
}

// createTableMatch creates a detailed table match result
func (m *UnifiedModelMatcher) createTableMatch(
	sourceTableName string, sourceTable unifiedmodel.Table, sourceEnrichment *unifiedmodel.TableEnrichment,
	targetTableName string, targetTable unifiedmodel.Table, targetEnrichment *unifiedmodel.TableEnrichment,
	sourceModelEnrichment, targetModelEnrichment *unifiedmodel.UnifiedModelEnrichment,
	options *UnifiedMatchOptions,
) UnifiedTableMatch {
	// Calculate table-level score
	tableScore := m.calculateTableSimilarity(sourceTable, sourceEnrichment, targetTable, targetEnrichment, options)

	// Match columns
	columnMatches := m.matchColumns(
		sourceTableName, sourceTable,
		targetTableName, targetTable,
		sourceModelEnrichment, targetModelEnrichment,
		options,
	)

	// Calculate classification match info
	classificationMatch := "unknown"
	classificationConfidenceDiff := 0.0
	if sourceEnrichment != nil && targetEnrichment != nil {
		if sourceEnrichment.PrimaryCategory == targetEnrichment.PrimaryCategory {
			classificationMatch = string(sourceEnrichment.PrimaryCategory)
		} else {
			classificationMatch = fmt.Sprintf("%s->%s", sourceEnrichment.PrimaryCategory, targetEnrichment.PrimaryCategory)
		}
		classificationConfidenceDiff = math.Abs(sourceEnrichment.ClassificationConfidence - targetEnrichment.ClassificationConfidence)
	}

	matchedColumns := 0
	for _, match := range columnMatches {
		if !match.IsUnmatched {
			matchedColumns++
		}
	}

	return UnifiedTableMatch{
		SourceTable:                  sourceTableName,
		TargetTable:                  targetTableName,
		Score:                        tableScore,
		IsPoorMatch:                  tableScore < options.PoorMatchThreshold,
		IsUnmatched:                  false,
		ClassificationMatch:          classificationMatch,
		ClassificationConfidenceDiff: classificationConfidenceDiff,
		MatchedColumns:               matchedColumns,
		TotalSourceColumns:           len(sourceTable.Columns),
		TotalTargetColumns:           len(targetTable.Columns),
		ColumnMatches:                columnMatches,
	}
}

// matchColumns matches columns between two tables
func (m *UnifiedModelMatcher) matchColumns(
	sourceTableName string, sourceTable unifiedmodel.Table,
	targetTableName string, targetTable unifiedmodel.Table,
	sourceEnrichment, targetEnrichment *unifiedmodel.UnifiedModelEnrichment,
	options *UnifiedMatchOptions,
) []UnifiedColumnMatch {
	var matches []UnifiedColumnMatch

	// Create column similarity matrix
	sourceColumns := make([]string, 0, len(sourceTable.Columns))
	for columnName := range sourceTable.Columns {
		sourceColumns = append(sourceColumns, columnName)
	}

	targetColumns := make([]string, 0, len(targetTable.Columns))
	for columnName := range targetTable.Columns {
		targetColumns = append(targetColumns, columnName)
	}

	usedTargetColumns := make(map[string]bool)

	for _, sourceColumnName := range sourceColumns {
		sourceColumn := sourceTable.Columns[sourceColumnName]

		bestTargetColumn := ""
		bestScore := 0.0

		for _, targetColumnName := range targetColumns {
			if usedTargetColumns[targetColumnName] {
				continue
			}

			targetColumn := targetTable.Columns[targetColumnName]
			score := m.calculateColumnSimilarity(
				sourceColumn, targetColumn,
				sourceTableName, sourceColumnName,
				targetTableName, targetColumnName,
				sourceEnrichment, targetEnrichment,
				options,
			)

			if score > bestScore {
				bestScore = score
				bestTargetColumn = targetColumnName
			}
		}

		if bestTargetColumn != "" && bestScore > 0.0 {
			usedTargetColumns[bestTargetColumn] = true
			targetColumn := targetTable.Columns[bestTargetColumn]

			match := m.createColumnMatch(
				sourceTableName, sourceColumnName, sourceColumn,
				targetTableName, bestTargetColumn, targetColumn,
				sourceEnrichment, targetEnrichment,
				bestScore, options,
			)
			matches = append(matches, match)
		} else {
			// Unmatched source column
			match := UnifiedColumnMatch{
				SourceTable:  sourceTableName,
				SourceColumn: sourceColumnName,
				IsUnmatched:  true,
				Score:        0.0,
			}
			matches = append(matches, match)
		}
	}

	return matches
}

// calculateColumnSimilarity calculates similarity between two columns
func (m *UnifiedModelMatcher) calculateColumnSimilarity(
	sourceColumn, targetColumn unifiedmodel.Column,
	sourceTableName, sourceColumnName string,
	targetTableName, targetColumnName string,
	sourceEnrichment, targetEnrichment *unifiedmodel.UnifiedModelEnrichment,
	options *UnifiedMatchOptions,
) float64 {
	// Name similarity
	nameScore := m.calculateStringSimilarity(sourceColumnName, targetColumnName)

	// Type compatibility
	typeScore := 0.0
	if m.areTypesCompatible(sourceColumn.DataType, targetColumn.DataType) {
		typeScore = 1.0
	}

	// Privileged data similarity
	privilegedScore := 0.0
	if sourceEnrichment != nil && targetEnrichment != nil {
		sourceKey := fmt.Sprintf("%s.%s", sourceTableName, sourceColumnName)
		targetKey := fmt.Sprintf("%s.%s", targetTableName, targetColumnName)

		if sourceColEnrichment, exists := sourceEnrichment.ColumnEnrichments[sourceKey]; exists {
			if targetColEnrichment, exists := targetEnrichment.ColumnEnrichments[targetKey]; exists {
				privilegedScore = m.calculatePrivilegedDataSimilarity(sourceColEnrichment, targetColEnrichment)
			}
		}
	}

	// Weighted combination - ensure weights are normalized
	totalWeight := options.NameWeight + options.TypeWeight + options.PrivilegedDataWeight
	if totalWeight == 0 {
		totalWeight = 1.0
	}

	totalScore := (nameScore*options.NameWeight +
		typeScore*options.TypeWeight +
		privilegedScore*options.PrivilegedDataWeight) / totalWeight

	return math.Min(1.0, totalScore)
}

// areTypesCompatible checks if two data types are compatible
func (m *UnifiedModelMatcher) areTypesCompatible(sourceType, targetType string) bool {
	if sourceType == targetType {
		return true
	}

	// Define type compatibility groups
	integerTypes := map[string]bool{
		"integer": true, "int": true, "bigint": true, "smallint": true,
		"int4": true, "int8": true, "int2": true,
	}

	floatTypes := map[string]bool{
		"float": true, "double": true, "real": true, "decimal": true, "numeric": true,
		"float4": true, "float8": true,
	}

	stringTypes := map[string]bool{
		"varchar": true, "text": true, "char": true, "string": true,
		"character": true, "character varying": true,
	}

	dateTypes := map[string]bool{
		"date": true, "datetime": true, "timestamp": true, "time": true,
		"timestamptz": true, "timetz": true,
	}

	sourceTypeLower := strings.ToLower(sourceType)
	targetTypeLower := strings.ToLower(targetType)

	// Check if both types are in the same compatibility group
	if (integerTypes[sourceTypeLower] && integerTypes[targetTypeLower]) ||
		(floatTypes[sourceTypeLower] && floatTypes[targetTypeLower]) ||
		(stringTypes[sourceTypeLower] && stringTypes[targetTypeLower]) ||
		(dateTypes[sourceTypeLower] && dateTypes[targetTypeLower]) {
		return true
	}

	// Integer and float are somewhat compatible
	if (integerTypes[sourceTypeLower] && floatTypes[targetTypeLower]) ||
		(floatTypes[sourceTypeLower] && integerTypes[targetTypeLower]) {
		return true
	}

	return false
}

// calculatePrivilegedDataSimilarity calculates similarity between column enrichments
func (m *UnifiedModelMatcher) calculatePrivilegedDataSimilarity(
	sourceEnrichment, targetEnrichment unifiedmodel.ColumnEnrichment,
) float64 {
	// Data category match
	categoryMatch := 0.0
	if sourceEnrichment.DataCategory == targetEnrichment.DataCategory {
		categoryMatch = 1.0
	}

	// Risk level similarity
	riskMatch := 0.0
	if sourceEnrichment.RiskLevel == targetEnrichment.RiskLevel {
		riskMatch = 1.0
	}

	// Privileged data flag match
	privilegedMatch := 0.0
	if sourceEnrichment.IsPrivilegedData == targetEnrichment.IsPrivilegedData {
		privilegedMatch = 1.0
	}

	return (categoryMatch + riskMatch + privilegedMatch) / 3.0
}

// createColumnMatch creates a detailed column match result
func (m *UnifiedModelMatcher) createColumnMatch(
	sourceTableName, sourceColumnName string, sourceColumn unifiedmodel.Column,
	targetTableName, targetColumnName string, targetColumn unifiedmodel.Column,
	sourceEnrichment, targetEnrichment *unifiedmodel.UnifiedModelEnrichment,
	score float64, options *UnifiedMatchOptions,
) UnifiedColumnMatch {
	isTypeCompatible := m.areTypesCompatible(sourceColumn.DataType, targetColumn.DataType)

	// Calculate privileged data match info
	privilegedDataMatch := false
	dataCategoryMatch := "unknown"
	privilegedConfidenceDiff := 0.0

	if sourceEnrichment != nil && targetEnrichment != nil {
		sourceKey := fmt.Sprintf("%s.%s", sourceTableName, sourceColumnName)
		targetKey := fmt.Sprintf("%s.%s", targetTableName, targetColumnName)

		if sourceColEnrichment, exists := sourceEnrichment.ColumnEnrichments[sourceKey]; exists {
			if targetColEnrichment, exists := targetEnrichment.ColumnEnrichments[targetKey]; exists {
				privilegedDataMatch = sourceColEnrichment.IsPrivilegedData && targetColEnrichment.IsPrivilegedData
				if sourceColEnrichment.DataCategory == targetColEnrichment.DataCategory {
					dataCategoryMatch = string(sourceColEnrichment.DataCategory)
				} else {
					dataCategoryMatch = fmt.Sprintf("%s->%s", sourceColEnrichment.DataCategory, targetColEnrichment.DataCategory)
				}
				privilegedConfidenceDiff = math.Abs(sourceColEnrichment.PrivilegedConfidence - targetColEnrichment.PrivilegedConfidence)
			}
		}
	}

	return UnifiedColumnMatch{
		SourceTable:              sourceTableName,
		TargetTable:              targetTableName,
		SourceColumn:             sourceColumnName,
		TargetColumn:             targetColumnName,
		Score:                    score,
		IsTypeCompatible:         isTypeCompatible,
		IsPoorMatch:              score < options.PoorMatchThreshold,
		IsUnmatched:              false,
		PrivilegedDataMatch:      privilegedDataMatch,
		DataCategoryMatch:        dataCategoryMatch,
		PrivilegedConfidenceDiff: privilegedConfidenceDiff,
	}
}

// calculateOverallSimilarity calculates the overall similarity score
func (m *UnifiedModelMatcher) calculateOverallSimilarity(
	tableMatches []UnifiedTableMatch,
	sourceTableCount, targetTableCount int,
) float64 {
	if len(tableMatches) == 0 {
		return 0.0
	}

	totalScore := 0.0
	for _, match := range tableMatches {
		totalScore += match.Score
	}

	averageScore := totalScore / float64(len(tableMatches))

	// Penalize for unmatched tables
	maxTables := math.Max(float64(sourceTableCount), float64(targetTableCount))
	matchRatio := float64(len(tableMatches)) / maxTables

	return averageScore * matchRatio
}
