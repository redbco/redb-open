package classifier

import (
	"context"
	"fmt"

	pb "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/classifier/classifier"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/classifier/config"
)

// TableMetadata represents metadata about a database table for classification
type TableMetadata struct {
	Name        string            `json:"name"`
	Columns     []ColumnMetadata  `json:"columns"`
	Indexes     []IndexMetadata   `json:"indexes"`
	Constraints []string          `json:"constraints"`
	Tags        map[string]string `json:"tags"`
}

// ColumnMetadata represents metadata about a database column
type ColumnMetadata struct {
	Name             string  `json:"name"`
	DataType         string  `json:"dataType"`
	IsNullable       bool    `json:"isNullable"`
	IsPrimaryKey     bool    `json:"isPrimaryKey"`
	IsUnique         bool    `json:"isUnique"`
	IsAutoIncrement  bool    `json:"isAutoIncrement"`
	ColumnDefault    *string `json:"columnDefault"`
	VarcharLength    *int    `json:"varcharLength"`
	IsArray          bool    `json:"isArray"`
	ArrayElementType *string `json:"arrayElementType"`
	CustomTypeName   *string `json:"customTypeName"`
}

// IndexMetadata represents metadata about a database index
type IndexMetadata struct {
	Name      string   `json:"name"`
	Columns   []string `json:"columns"`
	IsUnique  bool     `json:"isUnique"`
	IsPrimary bool     `json:"isPrimary"`
}

// CategoryScore represents a classification score for a category
type CategoryScore struct {
	Category string  `json:"category"`
	Score    float64 `json:"score"`
	Reason   string  `json:"reason"`
}

// ClassificationResult represents the result of classifying a table
type ClassificationResult struct {
	TableName       string          `json:"tableName"`
	PrimaryCategory string          `json:"primaryCategory"`
	Confidence      float64         `json:"confidence"`
	Scores          []CategoryScore `json:"scores"`
}

// ClassificationOptions configures the classification algorithm
type ClassificationOptions struct {
	TopN      int     `json:"topN"`
	Threshold float64 `json:"threshold"`
}

// DefaultClassificationOptions returns default classification options
func DefaultClassificationOptions() ClassificationOptions {
	return ClassificationOptions{
		TopN:      3,
		Threshold: 0.1,
	}
}

// TableClassifier handles table classification
type TableClassifier struct {
	service *classifier.Service
}

// NewTableClassifier creates a new TableClassifier with default configuration
func NewTableClassifier() *TableClassifier {
	return &TableClassifier{
		service: classifier.NewService(),
	}
}

// NewTableClassifierWithConfig creates a new TableClassifier with custom configuration
func NewTableClassifierWithConfig(cfg *config.Config) *TableClassifier {
	var service *classifier.Service
	if cfg != nil && cfg.Weights != nil {
		service = classifier.NewServiceWithWeights(cfg.Weights)
	} else {
		service = classifier.NewService()
	}

	return &TableClassifier{
		service: service,
	}
}

// ClassifyUnifiedModel classifies tables from a UnifiedModel directly
func (c *TableClassifier) ClassifyUnifiedModel(model *unifiedmodel.UnifiedModel, options *ClassificationOptions) ([]unifiedmodel.TableEnrichment, error) {
	if model == nil {
		return nil, fmt.Errorf("unified model cannot be nil")
	}

	if options == nil {
		defaultOptions := DefaultClassificationOptions()
		options = &defaultOptions
	}

	enrichments := make([]unifiedmodel.TableEnrichment, 0)

	// Classify each table in the unified model
	for _, table := range model.Tables {
		// Convert table to metadata format for the internal service
		metadata := c.convertTableToMetadata(table)

		// Get classification result
		result, err := c.classifyTableInternal(metadata, options)
		if err != nil {
			return nil, fmt.Errorf("failed to classify table %s: %w", table.Name, err)
		}

		// Convert result to enrichment format
		enrichment := c.convertResultToEnrichment(result)
		enrichments = append(enrichments, enrichment)
	}

	return enrichments, nil
}

// ClassifyTable classifies a single table based on its metadata
func (c *TableClassifier) ClassifyTable(metadata TableMetadata, options *ClassificationOptions) (*ClassificationResult, error) {
	if options == nil {
		defaultOptions := DefaultClassificationOptions()
		options = &defaultOptions
	}

	return c.classifyTableInternal(metadata, options)
}

// ClassifyTables classifies multiple tables
func (c *TableClassifier) ClassifyTables(tables []TableMetadata, options *ClassificationOptions) ([]ClassificationResult, error) {
	if options == nil {
		defaultOptions := DefaultClassificationOptions()
		options = &defaultOptions
	}

	var results []ClassificationResult

	for _, table := range tables {
		result, err := c.ClassifyTable(table, options)
		if err != nil {
			return nil, fmt.Errorf("failed to classify table %s: %w", table.Name, err)
		}
		results = append(results, *result)
	}

	return results, nil
}

// convertToProtoMetadata converts our TableMetadata to the internal proto format
func (c *TableClassifier) convertToProtoMetadata(metadata TableMetadata) *pb.TableMetadata {
	columns := make([]*pb.ColumnMetadata, len(metadata.Columns))
	for i, col := range metadata.Columns {
		columns[i] = &pb.ColumnMetadata{
			Name:            col.Name,
			Type:            col.DataType,
			IsNullable:      col.IsNullable,
			IsPrimaryKey:    col.IsPrimaryKey,
			IsAutoIncrement: col.IsAutoIncrement,
			IsArray:         col.IsArray,
		}

		// Handle optional fields
		if col.ColumnDefault != nil {
			columns[i].ColumnDefault = *col.ColumnDefault
		}
		if col.VarcharLength != nil {
			columns[i].VarcharLength = int32(*col.VarcharLength)
		}
	}

	return &pb.TableMetadata{
		Name:       metadata.Name,
		Columns:    columns,
		Properties: metadata.Tags,
	}
}

// classifyTableInternal performs the actual classification logic
func (c *TableClassifier) classifyTableInternal(metadata TableMetadata, options *ClassificationOptions) (*ClassificationResult, error) {
	// Convert TableMetadata to the internal format expected by the service
	protoMetadata := c.convertToProtoMetadata(metadata)

	// Create classification request
	req := &pb.ClassifyRequest{
		Metadata:  protoMetadata,
		TopN:      int32(options.TopN),
		Threshold: float64(options.Threshold),
	}

	// Classify using the internal service
	resp, err := c.service.Classify(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("failed to classify table %s: %w", metadata.Name, err)
	}

	// Convert response to our result format
	result := &ClassificationResult{
		TableName:       metadata.Name,
		PrimaryCategory: resp.PrimaryCategory,
		Confidence:      float64(resp.Confidence),
		Scores:          make([]CategoryScore, len(resp.Scores)),
	}

	for i, score := range resp.Scores {
		result.Scores[i] = CategoryScore{
			Category: score.Category,
			Score:    float64(score.Score),
			Reason:   score.Reason,
		}
	}

	return result, nil
}

// convertTableToMetadata converts a shared UnifiedModel table to our internal TableMetadata format
func (c *TableClassifier) convertTableToMetadata(table unifiedmodel.Table) TableMetadata {
	columns := make([]ColumnMetadata, 0, len(table.Columns))
	for _, col := range table.Columns {
		colMetadata := ColumnMetadata{
			Name:            col.Name,
			DataType:        col.DataType,
			IsNullable:      col.Nullable,
			IsPrimaryKey:    col.IsPrimaryKey,
			IsUnique:        false, // Not available in shared model
			IsAutoIncrement: col.AutoIncrement,
			IsArray:         false, // Not directly available in shared model
		}

		// Handle optional fields
		if col.Default != "" {
			colMetadata.ColumnDefault = &col.Default
		}

		columns = append(columns, colMetadata)
	}

	// Convert indexes
	indexes := make([]IndexMetadata, 0, len(table.Indexes))
	for _, idx := range table.Indexes {
		indexes = append(indexes, IndexMetadata{
			Name:      idx.Name,
			Columns:   idx.Columns,
			IsUnique:  idx.Unique,
			IsPrimary: false, // Determine from constraints if needed
		})
	}

	// Convert constraints
	constraints := make([]string, 0, len(table.Constraints))
	for _, constraint := range table.Constraints {
		constraints = append(constraints, string(constraint.Type)+" "+constraint.Name)
	}

	return TableMetadata{
		Name:        table.Name,
		Columns:     columns,
		Indexes:     indexes,
		Constraints: constraints,
		Tags:        make(map[string]string), // Could be populated from table options if needed
	}
}

// convertResultToEnrichment converts a ClassificationResult to a TableEnrichment
func (c *TableClassifier) convertResultToEnrichment(result *ClassificationResult) unifiedmodel.TableEnrichment {
	// Convert scores
	scores := make([]unifiedmodel.CategoryScore, len(result.Scores))
	for i, score := range result.Scores {
		scores[i] = unifiedmodel.CategoryScore{
			Category: score.Category,
			Score:    score.Score,
			Reason:   score.Reason,
		}
	}

	return unifiedmodel.TableEnrichment{
		PrimaryCategory:          unifiedmodel.TableCategory(result.PrimaryCategory),
		ClassificationConfidence: result.Confidence,
		ClassificationScores:     scores,
		AccessPattern:            unifiedmodel.AccessPatternReadWrite, // Default value
		HasPrivilegedData:        false,                               // Would need privileged data detection
		DataSensitivity:          0.0,                                 // Would need sensitivity analysis
		Tags:                     []string{},
		Context:                  make(map[string]string),
	}
}

// ClassifyTable is a convenience function that creates a classifier and classifies a single table
func ClassifyTable(metadata TableMetadata, options *ClassificationOptions) (*ClassificationResult, error) {
	classifier := NewTableClassifier()
	return classifier.ClassifyTable(metadata, options)
}

// ClassifyTables is a convenience function that creates a classifier and classifies multiple tables
func ClassifyTables(tables []TableMetadata, options *ClassificationOptions) ([]ClassificationResult, error) {
	classifier := NewTableClassifier()
	return classifier.ClassifyTables(tables, options)
}

// ClassifyUnifiedModel is a convenience function that creates a classifier and classifies from a UnifiedModel
func ClassifyUnifiedModel(model *unifiedmodel.UnifiedModel, options *ClassificationOptions) ([]unifiedmodel.TableEnrichment, error) {
	classifier := NewTableClassifier()
	return classifier.ClassifyUnifiedModel(model, options)
}
