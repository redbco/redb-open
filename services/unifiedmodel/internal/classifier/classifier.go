package classifier

import (
	"context"
	"encoding/json"
	"fmt"

	unifiedmodel "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/adapters"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/classifier/classifier"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/classifier/config"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
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
	service  *classifier.Service
	adapters map[string]adapters.SchemaIngester
}

// NewTableClassifier creates a new TableClassifier with default configuration
func NewTableClassifier() *TableClassifier {
	return &TableClassifier{
		service:  classifier.NewService(),
		adapters: initializeAdapters(),
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
		service:  service,
		adapters: initializeAdapters(),
	}
}

// initializeAdapters sets up the database adapters
func initializeAdapters() map[string]adapters.SchemaIngester {
	adapterMap := make(map[string]adapters.SchemaIngester)

	// Initialize adapters for different database types
	adapterMap["postgres"] = &adapters.PostgresIngester{}
	adapterMap["postgresql"] = &adapters.PostgresIngester{}
	adapterMap["mysql"] = &adapters.MySQLIngester{}
	adapterMap["mariadb"] = &adapters.MariaDBIngester{}
	adapterMap["mssql"] = &adapters.MSSQLIngester{}
	adapterMap["sqlserver"] = &adapters.MSSQLIngester{}
	adapterMap["oracle"] = &adapters.OracleIngester{}
	adapterMap["db2"] = &adapters.Db2Ingester{}
	adapterMap["cockroach"] = &adapters.CockroachIngester{}
	adapterMap["cockroachdb"] = &adapters.CockroachIngester{}
	adapterMap["clickhouse"] = &adapters.ClickhouseIngester{}
	adapterMap["cassandra"] = &adapters.CassandraIngester{}
	adapterMap["mongodb"] = &adapters.MongoDBIngester{}
	adapterMap["redis"] = &adapters.RedisIngester{}
	adapterMap["neo4j"] = &adapters.Neo4jIngester{}
	adapterMap["elasticsearch"] = &adapters.ElasticsearchIngester{}
	adapterMap["snowflake"] = &adapters.SnowflakeIngester{}
	adapterMap["pinecone"] = &adapters.PineconeIngester{}
	adapterMap["edgedb"] = &adapters.EdgeDBIngester{}

	return adapterMap
}

// ClassifyTable classifies a single table based on its metadata
func (c *TableClassifier) ClassifyTable(metadata TableMetadata, options *ClassificationOptions) (*ClassificationResult, error) {
	if options == nil {
		defaultOptions := DefaultClassificationOptions()
		options = &defaultOptions
	}

	// Convert TableMetadata to the internal format expected by the service
	protoMetadata := c.convertToProtoMetadata(metadata)

	// Create classification request
	req := &unifiedmodel.ClassifyRequest{
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

// ClassifyFromJSON classifies tables from JSON schema data
func (c *TableClassifier) ClassifyFromJSON(schemaType string, data json.RawMessage, options *ClassificationOptions) ([]ClassificationResult, error) {
	if options == nil {
		defaultOptions := DefaultClassificationOptions()
		options = &defaultOptions
	}

	// Get the appropriate adapter for the schema type
	adapter, exists := c.adapters[schemaType]
	if !exists {
		return nil, fmt.Errorf("unsupported schema type: %s", schemaType)
	}

	// Convert JSON schema to unified model
	model, _, err := adapter.IngestSchema(data)
	if err != nil {
		return nil, fmt.Errorf("failed to convert schema: %w", err)
	}

	// Convert unified model to our table metadata format
	tables := c.convertFromUnifiedModel(model)

	// Classify the tables
	return c.ClassifyTables(tables, options)
}

// convertToProtoMetadata converts our TableMetadata to the internal proto format
func (c *TableClassifier) convertToProtoMetadata(metadata TableMetadata) *unifiedmodel.TableMetadata {
	columns := make([]*unifiedmodel.ColumnMetadata, len(metadata.Columns))
	for i, col := range metadata.Columns {
		columns[i] = &unifiedmodel.ColumnMetadata{
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

	return &unifiedmodel.TableMetadata{
		Name:       metadata.Name,
		Columns:    columns,
		Properties: metadata.Tags,
	}
}

// convertFromUnifiedModel converts a unified model to our table metadata format
func (c *TableClassifier) convertFromUnifiedModel(model *models.UnifiedModel) []TableMetadata {
	var tables []TableMetadata

	for _, table := range model.Tables {
		columns := make([]ColumnMetadata, len(table.Columns))
		for i, col := range table.Columns {
			colMetadata := ColumnMetadata{
				Name:            col.Name,
				DataType:        col.DataType.Name,
				IsNullable:      col.IsNullable,
				IsPrimaryKey:    col.IsPrimaryKey,
				IsUnique:        col.IsUnique,
				IsAutoIncrement: col.IsAutoIncrement,
				IsArray:         col.DataType.IsArray,
			}

			// Handle optional fields
			if col.DefaultValue != nil {
				colMetadata.ColumnDefault = col.DefaultValue
			}

			if col.DataType.BaseType != "" {
				colMetadata.ArrayElementType = &col.DataType.BaseType
			}

			if col.DataType.CustomTypeName != "" {
				colMetadata.CustomTypeName = &col.DataType.CustomTypeName
			}

			if col.DataType.Length > 0 {
				colMetadata.VarcharLength = &col.DataType.Length
			}

			columns[i] = colMetadata
		}

		indexes := make([]IndexMetadata, len(table.Indexes))
		for i, idx := range table.Indexes {
			// Convert IndexColumn to string slice
			columnNames := make([]string, len(idx.Columns))
			for j, col := range idx.Columns {
				columnNames[j] = col.ColumnName
			}

			indexes[i] = IndexMetadata{
				Name:      idx.Name,
				Columns:   columnNames,
				IsUnique:  idx.IsUnique,
				IsPrimary: false, // Index doesn't have IsPrimary field, check if it's a primary key index
			}
		}

		// Convert constraints to string slice
		constraintStrings := make([]string, len(table.Constraints))
		for i, constraint := range table.Constraints {
			constraintStrings[i] = constraint.Type + " " + constraint.Name
		}

		tableMetadata := TableMetadata{
			Name:        table.Name,
			Columns:     columns,
			Indexes:     indexes,
			Constraints: constraintStrings,
			Tags:        make(map[string]string), // Table doesn't have Tags field, use empty map
		}

		tables = append(tables, tableMetadata)
	}

	return tables
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

// ClassifyFromJSON is a convenience function that creates a classifier and classifies from JSON schema
func ClassifyFromJSON(schemaType string, data json.RawMessage, options *ClassificationOptions) ([]ClassificationResult, error) {
	classifier := NewTableClassifier()
	return classifier.ClassifyFromJSON(schemaType, data, options)
}
