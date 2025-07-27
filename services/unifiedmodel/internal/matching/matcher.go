package matching

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/adapters"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

// ColumnStructure represents the structure of a database column
type ColumnStructure struct {
	Name             string  `json:"name"`
	DataType         string  `json:"dataType"`
	IsNullable       bool    `json:"isNullable"`
	IsPrimaryKey     bool    `json:"isPrimaryKey"`
	IsArray          bool    `json:"isArray"`
	IsUnique         bool    `json:"isUnique"`
	IsAutoIncrement  bool    `json:"isAutoIncrement"`
	ColumnDefault    *string `json:"columnDefault"`
	ArrayElementType *string `json:"arrayElementType"`
	CustomTypeName   *string `json:"customTypeName"`
	VarcharLength    *int    `json:"varcharLength"`
}

// MatchResult represents the result of matching two columns
type MatchResult struct {
	SourceColumn     string  `json:"sourceColumn"`
	TargetColumn     string  `json:"targetColumn"`
	Score            float64 `json:"score"`
	IsTypeCompatible bool    `json:"isTypeCompatible"`
	IsPoorMatch      bool    `json:"isPoorMatch"`
	IsUnmatched      bool    `json:"isUnmatched"`
}

// MatchOptions configures the matching algorithm
type MatchOptions struct {
	NameSimilarityThreshold float64 `json:"nameSimilarityThreshold"`
	PoorMatchThreshold      float64 `json:"poorMatchThreshold"`
	NameWeight              float64 `json:"nameWeight"`
	TypeWeight              float64 `json:"typeWeight"`
}

// DefaultMatchOptions returns default matching options
func DefaultMatchOptions() MatchOptions {
	return MatchOptions{
		NameSimilarityThreshold: 0.3,
		PoorMatchThreshold:      0.5,
		NameWeight:              0.7,
		TypeWeight:              0.3,
	}
}

// Data type compatibility matrix
var dataTypeCompatibility = map[string][]string{
	"varchar":   {"string", "char", "text", "varchar2"},
	"string":    {"varchar", "char", "text", "varchar2"},
	"integer":   {"int", "int4", "number", "numeric"},
	"int":       {"integer", "int4", "number", "numeric"},
	"boolean":   {"bool", "bit"},
	"float":     {"double", "real", "decimal", "number"},
	"timestamp": {"datetime", "timestamp with time zone"},
	"date":      {"datetime", "timestamp"},
}

// normalizeString normalizes strings for comparison
func normalizeString(str string) string {
	// Convert to lowercase
	str = strings.ToLower(str)

	// Remove non-alphanumeric characters
	reg := regexp.MustCompile(`[^a-z0-9]`)
	str = reg.ReplaceAllString(str, "")

	// Trim whitespace
	return strings.TrimSpace(str)
}

// levenshteinDistance calculates the Levenshtein distance between two strings
func levenshteinDistance(str1, str2 string) int {
	len1 := len(str1)
	len2 := len(str2)

	// Create matrix
	matrix := make([][]int, len1+1)
	for i := range matrix {
		matrix[i] = make([]int, len2+1)
	}

	// Initialize first row and column
	for i := 0; i <= len1; i++ {
		matrix[i][0] = i
	}
	for j := 0; j <= len2; j++ {
		matrix[0][j] = j
	}

	// Fill matrix
	for i := 1; i <= len1; i++ {
		for j := 1; j <= len2; j++ {
			if str1[i-1] == str2[j-1] {
				matrix[i][j] = matrix[i-1][j-1]
			} else {
				matrix[i][j] = min(
					matrix[i-1][j-1]+1, // substitution
					matrix[i][j-1]+1,   // insertion
					matrix[i-1][j]+1,   // deletion
				)
			}
		}
	}

	return matrix[len1][len2]
}

// min returns the minimum of three integers
func min(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
		return c
	}
	if b < c {
		return b
	}
	return c
}

// calculateNameSimilarity calculates name similarity score (0-1)
func calculateNameSimilarity(name1, name2 string) float64 {
	normalized1 := normalizeString(name1)
	normalized2 := normalizeString(name2)

	maxLength := math.Max(float64(len(normalized1)), float64(len(normalized2)))
	if maxLength == 0 {
		return 1.0
	}

	distance := float64(levenshteinDistance(normalized1, normalized2))
	return 1 - (distance / maxLength)
}

// areTypesCompatible checks if data types are compatible
func areTypesCompatible(type1, type2 string) bool {
	normalizedType1 := normalizeString(type1)
	normalizedType2 := normalizeString(type2)

	// Check direct match
	if normalizedType1 == normalizedType2 {
		return true
	}

	// Check compatibility matrix
	compatibleTypes1, exists1 := dataTypeCompatibility[normalizedType1]
	compatibleTypes2, exists2 := dataTypeCompatibility[normalizedType2]

	if exists1 {
		for _, compatibleType := range compatibleTypes1 {
			if compatibleType == normalizedType2 {
				return true
			}
		}
	}

	if exists2 {
		for _, compatibleType := range compatibleTypes2 {
			if compatibleType == normalizedType1 {
				return true
			}
		}
	}

	return false
}

// calculateTypeSimilarity calculates type similarity score (0-1)
func calculateTypeSimilarity(type1, type2 string) float64 {
	if areTypesCompatible(type1, type2) {
		normalized1 := normalizeString(type1)
		normalized2 := normalizeString(type2)
		if normalized1 == normalized2 {
			return 1.0
		}
		return 0.8
	}
	return 0.0
}

// scoreEntry represents a scoring entry for sorting
type scoreEntry struct {
	sourceIndex     int
	targetIndex     int
	score           float64
	typesCompatible bool
}

// MatchColumns is the main matching function
func MatchColumns(sourceColumns, targetColumns []ColumnStructure, options *MatchOptions) []MatchResult {
	if options == nil {
		defaultOptions := DefaultMatchOptions()
		options = &defaultOptions
	}

	results := make([]MatchResult, 0)
	usedTargetColumns := make(map[int]bool)

	// Calculate scores for all possible pairs
	scores := make([]scoreEntry, 0)

	for sourceIndex, source := range sourceColumns {
		for targetIndex, target := range targetColumns {
			nameSimilarity := calculateNameSimilarity(source.Name, target.Name)
			typeSimilarity := calculateTypeSimilarity(source.DataType, target.DataType)

			score := (nameSimilarity * options.NameWeight) +
				(typeSimilarity * options.TypeWeight)

			scores = append(scores, scoreEntry{
				sourceIndex:     sourceIndex,
				targetIndex:     targetIndex,
				score:           score,
				typesCompatible: areTypesCompatible(source.DataType, target.DataType),
			})
		}
	}

	// Sort scores in descending order
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score > scores[j].score
	})

	// Match columns based on scores
	for _, score := range scores {
		if !usedTargetColumns[score.targetIndex] {
			source := sourceColumns[score.sourceIndex]
			target := targetColumns[score.targetIndex]

			results = append(results, MatchResult{
				SourceColumn:     source.Name,
				TargetColumn:     target.Name,
				Score:            score.score,
				IsTypeCompatible: score.typesCompatible,
				IsPoorMatch:      score.score < options.PoorMatchThreshold,
				IsUnmatched:      false,
			})

			usedTargetColumns[score.targetIndex] = true
		}
	}

	// Add unmatched source columns
	for _, source := range sourceColumns {
		isMatched := false
		for _, result := range results {
			if result.SourceColumn == source.Name {
				isMatched = true
				break
			}
		}
		if !isMatched {
			results = append(results, MatchResult{
				SourceColumn:     source.Name,
				TargetColumn:     "",
				Score:            0,
				IsTypeCompatible: false,
				IsPoorMatch:      true,
				IsUnmatched:      true,
			})
		}
	}

	// Add unmatched target columns
	for index, target := range targetColumns {
		if !usedTargetColumns[index] {
			results = append(results, MatchResult{
				SourceColumn:     "",
				TargetColumn:     target.Name,
				Score:            0,
				IsTypeCompatible: false,
				IsPoorMatch:      true,
				IsUnmatched:      true,
			})
		}
	}

	return results
}

// SchemaMatcher handles the matching of database schemas
type SchemaMatcher struct {
	adapters map[string]adapters.SchemaIngester
}

// MatchSchemasResult represents the result of matching two schemas
type MatchSchemasResult struct {
	Matches  []MatchResult `json:"matches"`
	Warnings []string      `json:"warnings"`
}

// NewSchemaMatcher creates a new SchemaMatcher with predefined adapters
func NewSchemaMatcher() *SchemaMatcher {
	matcher := &SchemaMatcher{
		adapters: make(map[string]adapters.SchemaIngester),
	}

	// Initialize adapters for different database types
	matcher.adapters["postgres"] = &adapters.PostgresIngester{}
	matcher.adapters["postgresql"] = &adapters.PostgresIngester{}
	matcher.adapters["mysql"] = &adapters.MySQLIngester{}
	matcher.adapters["mariadb"] = &adapters.MariaDBIngester{}
	matcher.adapters["mssql"] = &adapters.MSSQLIngester{}
	matcher.adapters["sqlserver"] = &adapters.MSSQLIngester{}
	matcher.adapters["oracle"] = &adapters.OracleIngester{}
	matcher.adapters["db2"] = &adapters.Db2Ingester{}
	matcher.adapters["cockroach"] = &adapters.CockroachIngester{}
	matcher.adapters["cockroachdb"] = &adapters.CockroachIngester{}
	matcher.adapters["clickhouse"] = &adapters.ClickhouseIngester{}
	matcher.adapters["cassandra"] = &adapters.CassandraIngester{}
	matcher.adapters["mongodb"] = &adapters.MongoDBIngester{}
	matcher.adapters["redis"] = &adapters.RedisIngester{}
	matcher.adapters["neo4j"] = &adapters.Neo4jIngester{}
	matcher.adapters["elasticsearch"] = &adapters.ElasticsearchIngester{}
	matcher.adapters["snowflake"] = &adapters.SnowflakeIngester{}
	matcher.adapters["pinecone"] = &adapters.PineconeIngester{}
	matcher.adapters["edgedb"] = &adapters.EdgeDBIngester{}

	return matcher
}

// MatchSchemas matches two schemas and returns the matching results
func (m *SchemaMatcher) MatchSchemas(schemaType string, previousSchema, currentSchema json.RawMessage) (*MatchSchemasResult, error) {
	// Convert schemas to unified models
	previousModel, previousWarnings, err := m.convertToUnifiedModel(schemaType, previousSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to convert previous schema: %w", err)
	}

	currentModel, currentWarnings, err := m.convertToUnifiedModel(schemaType, currentSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to convert current schema: %w", err)
	}

	// Combine warnings
	warnings := append(previousWarnings, currentWarnings...)

	// Convert unified models to column structures for matching
	previousColumns := m.convertToColumnStructures(previousModel)
	currentColumns := m.convertToColumnStructures(currentModel)

	// Perform column matching
	options := DefaultMatchOptions()
	matches := MatchColumns(previousColumns, currentColumns, &options)

	result := &MatchSchemasResult{
		Matches:  matches,
		Warnings: warnings,
	}

	return result, nil
}

// convertToUnifiedModel converts the input schema to a unified model
func (m *SchemaMatcher) convertToUnifiedModel(schemaType string, schema json.RawMessage) (*models.UnifiedModel, []string, error) {
	// Get the appropriate adapter for the schema type
	adapter, exists := m.adapters[schemaType]
	if !exists {
		return nil, nil, fmt.Errorf("unsupported schema type: %s", schemaType)
	}

	// Convert schema to unified model using the adapter
	model, warnings, err := adapter.IngestSchema(schema)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert schema: %w", err)
	}

	return model, warnings, nil
}

// convertToColumnStructures converts a unified model to column structures for matching
func (m *SchemaMatcher) convertToColumnStructures(model *models.UnifiedModel) []ColumnStructure {
	var columns []ColumnStructure

	for _, table := range model.Tables {
		for _, column := range table.Columns {
			colStruct := ColumnStructure{
				Name:            column.Name,
				DataType:        column.DataType.Name,
				IsNullable:      column.IsNullable,
				IsPrimaryKey:    column.IsPrimaryKey,
				IsArray:         column.DataType.IsArray,
				IsUnique:        column.IsUnique,
				IsAutoIncrement: column.IsAutoIncrement,
			}

			// Handle optional fields
			if column.DefaultValue != nil {
				colStruct.ColumnDefault = column.DefaultValue
			}

			if column.DataType.BaseType != "" {
				colStruct.ArrayElementType = &column.DataType.BaseType
			}

			if column.DataType.CustomTypeName != "" {
				colStruct.CustomTypeName = &column.DataType.CustomTypeName
			}

			if column.DataType.Length > 0 {
				colStruct.VarcharLength = &column.DataType.Length
			}

			columns = append(columns, colStruct)
		}
	}

	return columns
}
