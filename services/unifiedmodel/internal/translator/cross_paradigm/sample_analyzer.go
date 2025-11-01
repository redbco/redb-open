package cross_paradigm

import (
	"fmt"
	"sort"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SampleAnalyzer provides sample data analysis for cross-paradigm conversions
type SampleAnalyzer struct{}

// NewSampleAnalyzer creates a new sample analyzer
func NewSampleAnalyzer() *SampleAnalyzer {
	return &SampleAnalyzer{}
}

// PropertyAnalysis represents the analysis of a property across samples
type PropertyAnalysis struct {
	PropertyName string
	Occurrences  int
	TotalSamples int
	Frequency    float64 // 0.0 to 1.0
	DataTypes    map[string]int
	PrimaryType  string
	IsNullable   bool
	SampleValues []interface{}
	IsCommon     bool // Frequency >= 0.7
	IsRare       bool // Frequency < 0.3
}

// NodePropertyAnalysis represents property analysis for a specific node label
type NodePropertyAnalysis struct {
	NodeLabel   string
	Properties  []PropertyAnalysis
	SampleCount int
}

// AnalyzeNodeProperties analyzes properties across node samples for a given label
func (sa *SampleAnalyzer) AnalyzeNodeProperties(samples []map[string]interface{}, threshold float64) []PropertyAnalysis {
	if len(samples) == 0 {
		return []PropertyAnalysis{}
	}

	propertyStats := make(map[string]*PropertyAnalysis)

	// Collect statistics
	for _, sample := range samples {
		for propName, propValue := range sample {
			if _, exists := propertyStats[propName]; !exists {
				propertyStats[propName] = &PropertyAnalysis{
					PropertyName: propName,
					DataTypes:    make(map[string]int),
					SampleValues: make([]interface{}, 0, 10),
				}
			}

			stat := propertyStats[propName]
			stat.Occurrences++

			// Track data type
			if propValue == nil {
				stat.IsNullable = true
			} else {
				dataType := inferDataType(propValue)
				stat.DataTypes[dataType]++

				// Store sample values (up to 10)
				if len(stat.SampleValues) < 10 {
					stat.SampleValues = append(stat.SampleValues, propValue)
				}
			}
		}
	}

	// Calculate frequencies and determine primary types
	totalSamples := len(samples)
	analyses := make([]PropertyAnalysis, 0, len(propertyStats))

	for _, stat := range propertyStats {
		stat.TotalSamples = totalSamples
		stat.Frequency = float64(stat.Occurrences) / float64(totalSamples)
		stat.IsCommon = stat.Frequency >= threshold
		stat.IsRare = stat.Frequency < 0.3

		// Determine primary type
		stat.PrimaryType = getPrimaryType(stat.DataTypes)

		analyses = append(analyses, *stat)
	}

	// Sort by frequency (descending)
	sort.Slice(analyses, func(i, j int) bool {
		return analyses[i].Frequency > analyses[j].Frequency
	})

	return analyses
}

// AnalyzeGraphSampleData analyzes sample data for graph databases
func (sa *SampleAnalyzer) AnalyzeGraphSampleData(sampleData *unifiedmodel.UnifiedModelSampleData, threshold float64) map[string]NodePropertyAnalysis {
	if sampleData == nil || len(sampleData.GraphSamples) == 0 {
		return nil
	}

	result := make(map[string]NodePropertyAnalysis)

	for graphName, graphSample := range sampleData.GraphSamples {
		for nodeLabel, nodeSample := range graphSample.NodeSamples {
			// Convert samples to []map[string]interface{}
			samples := nodeSample.Samples

			// Analyze properties
			properties := sa.AnalyzeNodeProperties(samples, threshold)

			result[fmt.Sprintf("%s.%s", graphName, nodeLabel)] = NodePropertyAnalysis{
				NodeLabel:   nodeLabel,
				Properties:  properties,
				SampleCount: len(samples),
			}
		}
	}

	return result
}

// CollectionFieldAnalysis represents field analysis for document collections
type CollectionFieldAnalysis struct {
	FieldName    string
	Occurrences  int
	TotalSamples int
	Frequency    float64
	DataTypes    map[string]int
	PrimaryType  string
	IsNullable   bool
	IsNested     bool
	IsArray      bool
	SampleValues []interface{}
	IsCommon     bool
	IsRare       bool
	NestedFields []CollectionFieldAnalysis
}

// AnalyzeCollectionFields analyzes fields in document collection samples
func (sa *SampleAnalyzer) AnalyzeCollectionFields(samples []map[string]interface{}, threshold float64) []CollectionFieldAnalysis {
	if len(samples) == 0 {
		return []CollectionFieldAnalysis{}
	}

	fieldStats := make(map[string]*CollectionFieldAnalysis)

	// Collect statistics
	for _, sample := range samples {
		for fieldName, fieldValue := range sample {
			if _, exists := fieldStats[fieldName]; !exists {
				fieldStats[fieldName] = &CollectionFieldAnalysis{
					FieldName:    fieldName,
					DataTypes:    make(map[string]int),
					SampleValues: make([]interface{}, 0, 10),
				}
			}

			stat := fieldStats[fieldName]
			stat.Occurrences++

			// Track data type and nested structures
			if fieldValue == nil {
				stat.IsNullable = true
			} else {
				dataType := inferDataType(fieldValue)
				stat.DataTypes[dataType]++

				// Check for nested structures
				switch fieldValue.(type) {
				case map[string]interface{}:
					stat.IsNested = true
					// Could recursively analyze nested fields here
				case []interface{}:
					stat.IsArray = true
				}

				// Store sample values (up to 10)
				if len(stat.SampleValues) < 10 && !stat.IsNested && !stat.IsArray {
					stat.SampleValues = append(stat.SampleValues, fieldValue)
				}
			}
		}
	}

	// Calculate frequencies
	totalSamples := len(samples)
	analyses := make([]CollectionFieldAnalysis, 0, len(fieldStats))

	for _, stat := range fieldStats {
		stat.TotalSamples = totalSamples
		stat.Frequency = float64(stat.Occurrences) / float64(totalSamples)
		stat.IsCommon = stat.Frequency >= threshold
		stat.IsRare = stat.Frequency < 0.3
		stat.PrimaryType = getPrimaryType(stat.DataTypes)

		analyses = append(analyses, *stat)
	}

	// Sort by frequency (descending)
	sort.Slice(analyses, func(i, j int) bool {
		return analyses[i].Frequency > analyses[j].Frequency
	})

	return analyses
}

// AnalyzeDocumentSampleData analyzes sample data for document databases
func (sa *SampleAnalyzer) AnalyzeDocumentSampleData(sampleData *unifiedmodel.UnifiedModelSampleData, threshold float64) map[string][]CollectionFieldAnalysis {
	if sampleData == nil || len(sampleData.CollectionSamples) == 0 {
		return nil
	}

	result := make(map[string][]CollectionFieldAnalysis)

	for collectionName, collectionSample := range sampleData.CollectionSamples {
		fields := sa.AnalyzeCollectionFields(collectionSample.Documents, threshold)
		result[collectionName] = fields
	}

	return result
}

// TableColumnAnalysis represents column analysis for relational tables
type TableColumnAnalysis struct {
	ColumnName   string
	Occurrences  int
	TotalSamples int
	Frequency    float64
	DataType     string
	IsNullable   bool
	IsCommon     bool
	IsRare       bool
	SampleValues []interface{}
}

// AnalyzeTableSampleData analyzes sample data for relational databases
func (sa *SampleAnalyzer) AnalyzeTableSampleData(sampleData *unifiedmodel.UnifiedModelSampleData) map[string][]TableColumnAnalysis {
	if sampleData == nil || len(sampleData.TableSamples) == 0 {
		return nil
	}

	result := make(map[string][]TableColumnAnalysis)

	for tableName, tableSample := range sampleData.TableSamples {
		analyses := make([]TableColumnAnalysis, 0)

		for columnName, columnSample := range tableSample.Columns {
			analysis := TableColumnAnalysis{
				ColumnName:   columnName,
				DataType:     columnSample.DataType,
				IsNullable:   columnSample.NullCount > 0,
				SampleValues: columnSample.Values,
				Occurrences:  len(columnSample.Values) - columnSample.NullCount,
				TotalSamples: len(columnSample.Values),
			}

			if analysis.TotalSamples > 0 {
				analysis.Frequency = float64(analysis.Occurrences) / float64(analysis.TotalSamples)
			}

			analysis.IsCommon = analysis.Frequency >= 0.7
			analysis.IsRare = analysis.Frequency < 0.3

			analyses = append(analyses, analysis)
		}

		result[tableName] = analyses
	}

	return result
}

// SuggestPropertyMappingStrategy suggests the best property mapping strategy based on analysis
func (sa *SampleAnalyzer) SuggestPropertyMappingStrategy(analyses []PropertyAnalysis) string {
	if len(analyses) == 0 {
		return "minimal_to_columns" // Default when no data
	}

	commonCount := 0
	rareCount := 0

	for _, analysis := range analyses {
		if analysis.IsCommon {
			commonCount++
		}
		if analysis.IsRare {
			rareCount++
		}
	}

	totalCount := len(analyses)

	// If most properties are common (>70%), map all to columns
	if float64(commonCount)/float64(totalCount) > 0.7 {
		return "all_to_columns"
	}

	// If there's a mix of common and rare properties, use core mapping
	if commonCount > 0 && rareCount > 0 {
		return "core_to_columns"
	}

	// If most properties are rare, use minimal mapping
	if float64(rareCount)/float64(totalCount) > 0.7 {
		return "minimal_to_columns"
	}

	// Default to core mapping as a balanced approach
	return "core_to_columns"
}

// GetCoreProperties returns properties that should be mapped to columns based on threshold
func (sa *SampleAnalyzer) GetCoreProperties(analyses []PropertyAnalysis, threshold float64) []string {
	coreProps := make([]string, 0)

	for _, analysis := range analyses {
		if analysis.Frequency >= threshold {
			coreProps = append(coreProps, analysis.PropertyName)
		}
	}

	return coreProps
}

// Helper functions

func inferDataType(value interface{}) string {
	if value == nil {
		return "null"
	}

	switch v := value.(type) {
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
		return fmt.Sprintf("unknown(%T)", v)
	}
}

func getPrimaryType(dataTypes map[string]int) string {
	if len(dataTypes) == 0 {
		return "unknown"
	}

	maxCount := 0
	primaryType := "unknown"

	for dataType, count := range dataTypes {
		if count > maxCount {
			maxCount = count
			primaryType = dataType
		}
	}

	return primaryType
}
