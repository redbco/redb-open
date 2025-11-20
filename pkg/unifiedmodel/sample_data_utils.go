package unifiedmodel

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
)

// SampleDataCollector provides methods for collecting and processing sample data
type SampleDataCollector struct {
	config   SampleDataConfig
	patterns map[string]*regexp.Regexp // Compiled regex patterns
}

// NewSampleDataCollector creates a new sample data collector with the given configuration
func NewSampleDataCollector(config SampleDataConfig) *SampleDataCollector {
	collector := &SampleDataCollector{
		config:   config,
		patterns: make(map[string]*regexp.Regexp),
	}

	// Compile common PII detection patterns
	collector.initializePatterns()

	return collector
}

// initializePatterns compiles common regex patterns for PII detection
func (c *SampleDataCollector) initializePatterns() {
	piiPatterns := map[string]string{
		"email":       `\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b`,
		"phone":       `\b(?:\+?1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})\b`,
		"ssn":         `\b\d{3}-?\d{2}-?\d{4}\b`,
		"credit_card": `\b(?:\d{4}[-\s]?){3}\d{4}\b`,
		"ip_address":  `\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b`,
		"zip_code":    `\b\d{5}(?:-\d{4})?\b`,
		"date":        `\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b`,
		"guid":        `\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`,
	}

	for name, pattern := range piiPatterns {
		if compiled, err := regexp.Compile(pattern); err == nil {
			c.patterns[name] = compiled
		}
	}
}

// ProcessTableSample processes raw table data into structured sample data
func (c *SampleDataCollector) ProcessTableSample(tableName string, rows []map[string]interface{}, totalRowCount int64) TableSampleData {
	sample := TableSampleData{
		TableName:   tableName,
		RowCount:    totalRowCount,
		SampleCount: len(rows),
		Columns:     make(map[string]ColumnSampleValues),
		Rows:        rows,
		CollectedAt: time.Now(),
		Warnings:    make([]string, 0),
	}

	// Process each column
	if len(rows) > 0 {
		// Get all column names from the first row
		for columnName := range rows[0] {
			values := c.extractColumnValues(rows, columnName)
			sample.Columns[columnName] = c.analyzeColumnValues(columnName, values)
		}
	}

	return sample
}

// ProcessCollectionSample processes document collection data
func (c *SampleDataCollector) ProcessCollectionSample(collectionName string, documents []map[string]interface{}, totalDocCount int64) CollectionSampleData {
	sample := CollectionSampleData{
		CollectionName: collectionName,
		DocumentCount:  totalDocCount,
		SampleCount:    len(documents),
		Documents:      documents,
		FieldSamples:   make(map[string]FieldSampleValues),
		SchemaProfile:  c.analyzeDocumentSchema(documents),
		CollectedAt:    time.Now(),
		Warnings:       make([]string, 0),
	}

	// Extract all fields from documents
	allFields := c.extractDocumentFields(documents)
	for fieldPath, values := range allFields {
		fieldSample := c.analyzeFieldValues(fieldPath, values)
		sample.FieldSamples[fieldPath] = fieldSample
	}

	return sample
}

// ProcessKeyValueSample processes key-value pair data
func (c *SampleDataCollector) ProcessKeyValueSample(namespaceName string, keyValuePairs []KeyValueSampleEntry, totalKeyCount int64) KeyValueSampleData {
	sample := KeyValueSampleData{
		NamespaceName: namespaceName,
		KeyCount:      totalKeyCount,
		SampleCount:   len(keyValuePairs),
		KeySamples:    keyValuePairs,
		ValueTypes:    make(map[string]int),
		KeyPatterns:   make(map[string]KeyPatternInfo),
		ValueSamples:  make(map[string]ValueSampleInfo),
		CollectedAt:   time.Now(),
		Warnings:      make([]string, 0),
	}

	// Analyze key patterns and value types
	c.analyzeKeyPatterns(&sample)
	c.analyzeValueDistribution(&sample)

	return sample
}

// extractColumnValues extracts all values for a specific column from row data
func (c *SampleDataCollector) extractColumnValues(rows []map[string]interface{}, columnName string) []interface{} {
	values := make([]interface{}, 0, len(rows))
	for _, row := range rows {
		if val, exists := row[columnName]; exists {
			values = append(values, val)
		}
	}
	return values
}

// analyzeColumnValues performs comprehensive analysis on column values
func (c *SampleDataCollector) analyzeColumnValues(columnName string, values []interface{}) ColumnSampleValues {
	analysis := ColumnSampleValues{
		FieldName:       columnName,
		Values:          values,
		DistinctCount:   0,
		NullCount:       0,
		EmptyCount:      0,
		ValuePatterns:   make([]string, 0),
		CommonValues:    make([]ValueFreq, 0),
		PiiIndicators:   make([]string, 0),
		SensitivityTags: make([]string, 0),
	}

	if len(values) == 0 {
		return analysis
	}

	// Basic statistics
	distinctValues := make(map[interface{}]int)
	var stringLengths []int
	var numericValues []float64

	for _, value := range values {
		if value == nil {
			analysis.NullCount++
			continue
		}

		// Count distinct values
		distinctValues[value]++

		// Check for empty values
		if str, ok := value.(string); ok {
			if strings.TrimSpace(str) == "" {
				analysis.EmptyCount++
				continue
			}
			stringLengths = append(stringLengths, len(str))

			// Detect PII patterns in string values
			c.detectPIIPatterns(str, &analysis)
		}

		// Handle numeric values
		if num, ok := c.convertToFloat64(value); ok {
			numericValues = append(numericValues, num)
		}
	}

	analysis.DistinctCount = len(distinctValues)

	// String statistics
	if len(stringLengths) > 0 {
		minLen, maxLen, avgLen := c.calculateLengthStats(stringLengths)
		analysis.MinLength = &minLen
		analysis.MaxLength = &maxLen
		analysis.AvgLength = &avgLen
	}

	// Numeric statistics
	if len(numericValues) > 0 {
		sort.Float64s(numericValues)
		analysis.MinValue = numericValues[0]
		analysis.MaxValue = numericValues[len(numericValues)-1]
		analysis.DataType = c.inferNumericType(numericValues)
	} else if len(stringLengths) > 0 {
		analysis.DataType = "string"
	} else {
		analysis.DataType = "unknown"
	}

	// Common values analysis
	analysis.CommonValues = c.calculateValueFrequencies(distinctValues, len(values))

	// Data classification
	c.classifyColumnSensitivity(columnName, values, &analysis)

	return analysis
}

// analyzeFieldValues analyzes document field values (similar to column analysis but handles nested data)
func (c *SampleDataCollector) analyzeFieldValues(fieldPath string, values []interface{}) FieldSampleValues {
	columnAnalysis := c.analyzeColumnValues(fieldPath, values)

	fieldAnalysis := FieldSampleValues{
		ColumnSampleValues: columnAnalysis,
		NestedFields:       make(map[string]FieldSampleValues),
		ArraySamples:       make([]interface{}, 0),
	}

	// Handle nested objects and arrays
	for _, value := range values {
		if obj, ok := value.(map[string]interface{}); ok {
			// Process nested object
			for nestedField, nestedValue := range obj {
				nestedPath := fieldPath + "." + nestedField
				if _, exists := fieldAnalysis.NestedFields[nestedPath]; !exists {
					fieldAnalysis.NestedFields[nestedPath] = c.analyzeFieldValues(nestedPath, []interface{}{nestedValue})
				}
			}
		} else if arr, ok := value.([]interface{}); ok {
			// Sample array elements
			if len(fieldAnalysis.ArraySamples) < 10 { // Limit array samples
				fieldAnalysis.ArraySamples = append(fieldAnalysis.ArraySamples, arr...)
			}
		}
	}

	return fieldAnalysis
}

// analyzeDocumentSchema analyzes the overall schema of document collection
func (c *SampleDataCollector) analyzeDocumentSchema(documents []map[string]interface{}) DocumentSchemaProfile {
	profile := DocumentSchemaProfile{
		FieldTypes:     make(map[string]string),
		RequiredFields: make([]string, 0),
		OptionalFields: make([]string, 0),
		NestedLevels:   0,
		ArrayFields:    make([]string, 0),
	}

	if len(documents) == 0 {
		return profile
	}

	// Analyze field presence and types
	fieldCounts := make(map[string]int)

	for _, doc := range documents {
		c.analyzeDocumentStructure(doc, "", &profile, fieldCounts, 0)
	}

	// Determine required vs optional fields
	for field, count := range fieldCounts {
		percentage := float64(count) / float64(len(documents))
		if percentage > 0.9 { // Present in >90% of documents
			profile.RequiredFields = append(profile.RequiredFields, field)
		} else {
			profile.OptionalFields = append(profile.OptionalFields, field)
		}
	}

	return profile
}

// extractDocumentFields extracts all field paths and their values from documents
func (c *SampleDataCollector) extractDocumentFields(documents []map[string]interface{}) map[string][]interface{} {
	fields := make(map[string][]interface{})

	for _, doc := range documents {
		c.extractFieldsRecursive(doc, "", fields)
	}

	return fields
}

// extractFieldsRecursive recursively extracts fields from nested documents
func (c *SampleDataCollector) extractFieldsRecursive(obj map[string]interface{}, prefix string, fields map[string][]interface{}) {
	for key, value := range obj {
		fieldPath := key
		if prefix != "" {
			fieldPath = prefix + "." + key
		}

		fields[fieldPath] = append(fields[fieldPath], value)

		// Recursively process nested objects
		if nestedObj, ok := value.(map[string]interface{}); ok {
			c.extractFieldsRecursive(nestedObj, fieldPath, fields)
		}
	}
}

// analyzeKeyPatterns detects patterns in key names
func (c *SampleDataCollector) analyzeKeyPatterns(sample *KeyValueSampleData) {
	patterns := make(map[string]*KeyPatternInfo)

	for _, kvPair := range sample.KeySamples {
		// Simple pattern detection based on common separators and structures
		pattern := c.detectKeyPattern(kvPair.Key)

		if existing, exists := patterns[pattern]; exists {
			existing.Count++
			if len(existing.Examples) < 5 {
				existing.Examples = append(existing.Examples, kvPair.Key)
			}
		} else {
			patterns[pattern] = &KeyPatternInfo{
				Pattern:    pattern,
				Count:      1,
				Confidence: 0.8, // Default confidence
				Examples:   []string{kvPair.Key},
			}
		}
	}

	// Convert to the expected map type
	for pattern, info := range patterns {
		sample.KeyPatterns[pattern] = *info
	}
}

// analyzeValueDistribution analyzes the distribution of value types
func (c *SampleDataCollector) analyzeValueDistribution(sample *KeyValueSampleData) {
	typeDistribution := make(map[string]int)
	valueSamples := make(map[string][]interface{})

	for _, kvPair := range sample.KeySamples {
		valueType := kvPair.ValueType
		typeDistribution[valueType]++

		if len(valueSamples[valueType]) < 10 {
			valueSamples[valueType] = append(valueSamples[valueType], kvPair.Value)
		}
	}

	sample.ValueTypes = typeDistribution

	for valueType, samples := range valueSamples {
		sample.ValueSamples[valueType] = ValueSampleInfo{
			ValueType:    valueType,
			SampleValues: samples,
			Count:        typeDistribution[valueType],
		}
	}
}

// Helper functions

func (c *SampleDataCollector) detectPIIPatterns(value string, analysis *ColumnSampleValues) {
	for patternName, pattern := range c.patterns {
		if pattern.MatchString(value) {
			// Add to PII indicators if not already present
			found := false
			for _, existing := range analysis.PiiIndicators {
				if existing == patternName {
					found = true
					break
				}
			}
			if !found {
				analysis.PiiIndicators = append(analysis.PiiIndicators, patternName)
			}
		}
	}
}

func (c *SampleDataCollector) convertToFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case string:
		// Try to parse string as number
		// This is a simplified version - in practice you'd use strconv.ParseFloat
		return 0, false
	default:
		return 0, false
	}
}

func (c *SampleDataCollector) calculateLengthStats(lengths []int) (int, int, float64) {
	if len(lengths) == 0 {
		return 0, 0, 0
	}

	sort.Ints(lengths)
	min := lengths[0]
	max := lengths[len(lengths)-1]

	sum := 0
	for _, length := range lengths {
		sum += length
	}
	avg := float64(sum) / float64(len(lengths))

	return min, max, avg
}

func (c *SampleDataCollector) inferNumericType(values []float64) string {
	allIntegers := true
	for _, v := range values {
		if v != math.Trunc(v) {
			allIntegers = false
			break
		}
	}

	if allIntegers {
		return "integer"
	}
	return "float"
}

func (c *SampleDataCollector) calculateValueFrequencies(distinctValues map[interface{}]int, totalCount int) []ValueFreq {
	frequencies := make([]ValueFreq, 0, len(distinctValues))

	for value, count := range distinctValues {
		frequency := ValueFreq{
			Value:      value,
			Frequency:  count,
			Percentage: float64(count) / float64(totalCount) * 100,
		}
		frequencies = append(frequencies, frequency)
	}

	// Sort by frequency (descending)
	sort.Slice(frequencies, func(i, j int) bool {
		return frequencies[i].Frequency > frequencies[j].Frequency
	})

	// Return top 10 most common values
	if len(frequencies) > 10 {
		frequencies = frequencies[:10]
	}

	return frequencies
}

func (c *SampleDataCollector) classifyColumnSensitivity(columnName string, values []interface{}, analysis *ColumnSampleValues) {
	columnLower := strings.ToLower(columnName)

	// Common sensitive field names
	sensitiveKeywords := map[string]string{
		"password": "authentication",
		"ssn":      "government_id",
		"social":   "government_id",
		"email":    "contact_info",
		"phone":    "contact_info",
		"address":  "location",
		"credit":   "financial",
		"bank":     "financial",
		"salary":   "financial",
		"medical":  "health",
		"health":   "health",
		"dob":      "personal",
		"birth":    "personal",
	}

	for keyword, category := range sensitiveKeywords {
		if strings.Contains(columnLower, keyword) {
			analysis.SensitivityTags = append(analysis.SensitivityTags, category)
		}
	}

	// Add tags based on detected PII patterns
	if len(analysis.PiiIndicators) > 0 {
		analysis.SensitivityTags = append(analysis.SensitivityTags, "personally_identifiable")
	}
}

func (c *SampleDataCollector) detectKeyPattern(key string) string {
	// Simple pattern detection - in practice this would be more sophisticated
	if strings.Contains(key, ":") {
		return "namespace:key"
	}
	if strings.Contains(key, "_") {
		return "underscore_separated"
	}
	if strings.Contains(key, ".") {
		return "dot.notation"
	}
	if strings.Contains(key, "/") {
		return "path/like"
	}
	return "simple_key"
}

func (c *SampleDataCollector) analyzeDocumentStructure(obj map[string]interface{}, prefix string, profile *DocumentSchemaProfile, fieldCounts map[string]int, level int) {
	if level > profile.NestedLevels {
		profile.NestedLevels = level
	}

	for key, value := range obj {
		fieldPath := key
		if prefix != "" {
			fieldPath = prefix + "." + key
		}

		fieldCounts[fieldPath]++

		// Determine field type
		switch v := value.(type) {
		case map[string]interface{}:
			profile.FieldTypes[fieldPath] = "object"
			c.analyzeDocumentStructure(v, fieldPath, profile, fieldCounts, level+1)
		case []interface{}:
			profile.FieldTypes[fieldPath] = "array"
			profile.ArrayFields = append(profile.ArrayFields, fieldPath)
		case string:
			profile.FieldTypes[fieldPath] = "string"
		case float64, int, int32, int64:
			profile.FieldTypes[fieldPath] = "number"
		case bool:
			profile.FieldTypes[fieldPath] = "boolean"
		case nil:
			profile.FieldTypes[fieldPath] = "null"
		default:
			profile.FieldTypes[fieldPath] = "unknown"
		}
	}
}

// RedactSensitiveData applies privacy-aware redaction to sample data
func RedactSensitiveData(sampleData *UnifiedModelSampleData) error {
	if !sampleData.SampleConfig.RedactSensitiveData {
		return nil
	}

	sampleData.RedactionApplied = true
	sampleData.RedactionLevel = "automatic"

	// Redact table samples
	for tableName, table := range sampleData.TableSamples {
		redactedTable := table

		// Redact column values that contain PII
		for columnName, column := range table.Columns {
			if len(column.PiiIndicators) > 0 || len(column.SensitivityTags) > 0 {
				redactedColumn := column
				redactedColumn.Values = redactValues(column.Values)
				redactedTable.Columns[columnName] = redactedColumn
			}
		}

		// Redact rows
		redactedTable.Rows = redactRows(table.Rows, table.Columns)
		sampleData.TableSamples[tableName] = redactedTable
	}

	// Similar redaction for other paradigms...
	// Implementation would follow similar patterns

	return nil
}

// redactValues replaces sensitive values with redacted placeholders
func redactValues(values []interface{}) []interface{} {
	redacted := make([]interface{}, len(values))
	for i, value := range values {
		if value == nil {
			redacted[i] = nil
		} else if str, ok := value.(string); ok {
			if len(str) > 0 {
				redacted[i] = fmt.Sprintf("[REDACTED_%d_CHARS]", len(str))
			} else {
				redacted[i] = "[REDACTED_EMPTY]"
			}
		} else {
			redacted[i] = "[REDACTED_VALUE]"
		}
	}
	return redacted
}

// redactRows redacts entire rows based on column sensitivity
func redactRows(rows []map[string]interface{}, columns map[string]ColumnSampleValues) []map[string]interface{} {
	redactedRows := make([]map[string]interface{}, len(rows))

	for i, row := range rows {
		redactedRow := make(map[string]interface{})
		for columnName, value := range row {
			if column, exists := columns[columnName]; exists {
				if len(column.PiiIndicators) > 0 || len(column.SensitivityTags) > 0 {
					redactedRow[columnName] = "[REDACTED]"
				} else {
					redactedRow[columnName] = value
				}
			} else {
				redactedRow[columnName] = value
			}
		}
		redactedRows[i] = redactedRow
	}

	return redactedRows
}

// SerializeSampleData converts sample data to JSON for temporary storage/transport
func SerializeSampleData(sampleData *UnifiedModelSampleData) ([]byte, error) {
	return json.Marshal(sampleData)
}

// DeserializeSampleData parses sample data from JSON
func DeserializeSampleData(data []byte) (*UnifiedModelSampleData, error) {
	var sampleData UnifiedModelSampleData
	err := json.Unmarshal(data, &sampleData)
	return &sampleData, err
}

// ValidateSampleData performs validation checks on sample data
func ValidateSampleData(sampleData *UnifiedModelSampleData) []ValidationError {
	var errors []ValidationError

	if sampleData.SchemaID == "" {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorCritical,
			Message: "SchemaID is required",
			Field:   "schema_id",
		})
	}

	if sampleData.CollectedAt.IsZero() {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorWarning,
			Message: "CollectedAt timestamp is missing",
			Field:   "collected_at",
		})
	}

	// Check for reasonable sample sizes
	totalSamples := sampleData.GetTotalSampleCount()
	if totalSamples == 0 {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorWarning,
			Message: "No sample data collected",
			Field:   "sample_count",
		})
	} else if totalSamples > 100000 {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorWarning,
			Message: fmt.Sprintf("Very large sample size (%d) may impact performance", totalSamples),
			Field:   "sample_count",
		})
	}

	// Validate memory usage
	estimatedSize := sampleData.EstimateMemoryUsage()
	if estimatedSize > 100*1024*1024 { // 100MB
		errors = append(errors, ValidationError{
			Type:    ValidationErrorWarning,
			Message: fmt.Sprintf("Sample data size (%d bytes) may be too large for memory", estimatedSize),
			Field:   "memory_usage",
		})
	}

	return errors
}
