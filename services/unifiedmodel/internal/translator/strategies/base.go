package strategies

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/core"
)

// BaseStrategy provides common utilities for strategy implementations
type BaseStrategy struct {
	name           string
	sourceParadigm dbcapabilities.DataParadigm
	targetParadigm dbcapabilities.DataParadigm
	typeConverter  *unifiedmodel.TypeConverter
	config         StrategyConfig
}

// NewBaseStrategy creates a new base strategy
func NewBaseStrategy(name string, source, target dbcapabilities.DataParadigm, config StrategyConfig) *BaseStrategy {
	return &BaseStrategy{
		name:           name,
		sourceParadigm: source,
		targetParadigm: target,
		typeConverter:  unifiedmodel.NewTypeConverter(),
		config:         config,
	}
}

// Name returns the strategy name
func (bs *BaseStrategy) Name() string {
	return bs.name
}

// SourceParadigm returns the source paradigm
func (bs *BaseStrategy) SourceParadigm() dbcapabilities.DataParadigm {
	return bs.sourceParadigm
}

// TargetParadigm returns the target paradigm
func (bs *BaseStrategy) TargetParadigm() dbcapabilities.DataParadigm {
	return bs.targetParadigm
}

// GetConfig returns the strategy configuration
func (bs *BaseStrategy) GetConfig() StrategyConfig {
	return bs.config
}

// SetConfig updates the strategy configuration
func (bs *BaseStrategy) SetConfig(config StrategyConfig) {
	bs.config = config
}

// GetTypeConverter returns the type converter
func (bs *BaseStrategy) GetTypeConverter() *unifiedmodel.TypeConverter {
	return bs.typeConverter
}

// ConvertDataType converts a data type between databases
func (bs *BaseStrategy) ConvertDataType(dataType string, sourceDB, targetDB dbcapabilities.DatabaseType) (string, bool, error) {
	// Create a temporary column/field to use the type converter
	tempColumn := unifiedmodel.Column{
		Name:     "temp",
		DataType: dataType,
	}

	converted, err := bs.typeConverter.ConvertColumn(tempColumn, sourceDB, targetDB)
	if err != nil {
		return dataType, false, err
	}

	isLossy := false
	if converted.Options != nil {
		if lossy, exists := converted.Options["is_lossy_conversion"].(bool); exists {
			isLossy = lossy
		}
	}

	return converted.DataType, isLossy, nil
}

// GenerateMappingID generates a unique mapping ID
func (bs *BaseStrategy) GenerateMappingID(sourceIdentifier, targetIdentifier string) string {
	return fmt.Sprintf("map_%s_to_%s", sanitizeIdentifier(sourceIdentifier), sanitizeIdentifier(targetIdentifier))
}

// GenerateRuleID generates a unique rule ID
func (bs *BaseStrategy) GenerateRuleID(sourceField, targetField string) string {
	return fmt.Sprintf("rule_%s_to_%s", sanitizeIdentifier(sourceField), sanitizeIdentifier(targetField))
}

// CreateWarning creates a translation warning
func (bs *BaseStrategy) CreateWarning(warningType core.WarningType, objectType, objectName, message, severity, suggestion string) core.TranslationWarning {
	return core.TranslationWarning{
		WarningType: warningType,
		ObjectType:  objectType,
		ObjectName:  objectName,
		Message:     message,
		Severity:    severity,
		Suggestion:  suggestion,
	}
}

// SanitizeTableName sanitizes a name for use as a table name
func (bs *BaseStrategy) SanitizeTableName(name string) string {
	// Convert to lowercase and replace special characters
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, ".", "_")

	// Remove any characters that aren't alphanumeric or underscore
	var result strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			result.WriteRune(r)
		}
	}

	sanitized := result.String()

	// Ensure it doesn't start with a number
	if len(sanitized) > 0 && sanitized[0] >= '0' && sanitized[0] <= '9' {
		sanitized = "t_" + sanitized
	}

	// Ensure it's not empty
	if sanitized == "" {
		sanitized = "unnamed_table"
	}

	return sanitized
}

// SanitizeColumnName sanitizes a name for use as a column name
func (bs *BaseStrategy) SanitizeColumnName(name string) string {
	// Similar to table name but allows some variations
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, ".", "_")

	var result strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			result.WriteRune(r)
		}
	}

	sanitized := result.String()

	// Ensure it doesn't start with a number
	if len(sanitized) > 0 && sanitized[0] >= '0' && sanitized[0] <= '9' {
		sanitized = "col_" + sanitized
	}

	// Ensure it's not empty
	if sanitized == "" {
		sanitized = "unnamed_column"
	}

	return sanitized
}

// DetermineJSONBColumnType returns the appropriate JSONB column type for the target database
func (bs *BaseStrategy) DetermineJSONBColumnType(targetDB dbcapabilities.DatabaseType) string {
	switch targetDB {
	case dbcapabilities.PostgreSQL, dbcapabilities.CockroachDB:
		return "jsonb"
	case dbcapabilities.MySQL, dbcapabilities.MariaDB:
		return "json"
	case dbcapabilities.SQLServer:
		return "nvarchar(max)" // SQL Server uses nvarchar for JSON
	case dbcapabilities.Oracle:
		return "clob" // Oracle can use CLOB for JSON
	default:
		return "text" // Fallback to text
	}
}

// CreateHybridPropertyColumn creates a JSONB column for remaining properties
func (bs *BaseStrategy) CreateHybridPropertyColumn(targetDB dbcapabilities.DatabaseType) unifiedmodel.Column {
	return unifiedmodel.Column{
		Name:     "additional_properties",
		DataType: bs.DetermineJSONBColumnType(targetDB),
		Nullable: true,
		Options: map[string]any{
			"description":    "Additional properties stored as JSON",
			"hybrid_mapping": true,
		},
	}
}

// IsCommonProperty checks if a property is commonly found across samples
func (bs *BaseStrategy) IsCommonProperty(propertyName string, frequency float64, threshold float64) bool {
	return frequency >= threshold
}

// Helper functions

func sanitizeIdentifier(identifier string) string {
	// Replace special characters with underscores
	identifier = strings.ReplaceAll(identifier, ".", "_")
	identifier = strings.ReplaceAll(identifier, ":", "_")
	identifier = strings.ReplaceAll(identifier, "/", "_")
	identifier = strings.ReplaceAll(identifier, "-", "_")
	identifier = strings.ReplaceAll(identifier, " ", "_")

	// Remove any remaining non-alphanumeric characters
	var result strings.Builder
	for _, r := range identifier {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			result.WriteRune(r)
		}
	}

	sanitized := result.String()

	// Ensure it's not too long (max 50 chars)
	if len(sanitized) > 50 {
		sanitized = sanitized[:50]
	}

	return sanitized
}

// PropertyDistribution tracks property occurrence across samples
type PropertyDistribution struct {
	PropertyName string
	Occurrences  int
	TotalSamples int
	Frequency    float64
	DataTypes    map[string]int // Track data type variations
}

// AnalyzePropertyDistribution analyzes property distribution from sample data
func (bs *BaseStrategy) AnalyzePropertyDistribution(samples []map[string]interface{}) []PropertyDistribution {
	if len(samples) == 0 {
		return []PropertyDistribution{}
	}

	propertyStats := make(map[string]*PropertyDistribution)

	for _, sample := range samples {
		for propName, propValue := range sample {
			if _, exists := propertyStats[propName]; !exists {
				propertyStats[propName] = &PropertyDistribution{
					PropertyName: propName,
					DataTypes:    make(map[string]int),
				}
			}

			propertyStats[propName].Occurrences++

			// Track data type
			dataType := inferDataType(propValue)
			propertyStats[propName].DataTypes[dataType]++
		}
	}

	// Calculate frequencies
	totalSamples := len(samples)
	distributions := make([]PropertyDistribution, 0, len(propertyStats))

	for _, stat := range propertyStats {
		stat.TotalSamples = totalSamples
		stat.Frequency = float64(stat.Occurrences) / float64(totalSamples)
		distributions = append(distributions, *stat)
	}

	return distributions
}

// inferDataType infers the data type from a value
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
