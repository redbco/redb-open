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

// LibraryTableMetadata represents metadata about a database table for classification in the library interface
type LibraryTableMetadata struct {
	Name        string                  `json:"name"`
	Columns     []LibraryColumnMetadata `json:"columns"`
	Indexes     []LibraryIndexMetadata  `json:"indexes"`
	Constraints []string                `json:"constraints"`
	Tags        map[string]string       `json:"tags"`
}

// LibraryColumnMetadata represents metadata about a database column in the library interface
type LibraryColumnMetadata struct {
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

// LibraryIndexMetadata represents metadata about a database index in the library interface
type LibraryIndexMetadata struct {
	Name      string   `json:"name"`
	Columns   []string `json:"columns"`
	IsUnique  bool     `json:"isUnique"`
	IsPrimary bool     `json:"isPrimary"`
}

// LibraryCategoryScore represents a classification score for a category in the library interface
type LibraryCategoryScore struct {
	Category string  `json:"category"`
	Score    float64 `json:"score"`
	Reason   string  `json:"reason"`
}

// LibraryClassificationResult represents the result of classifying a table in the library interface
type LibraryClassificationResult struct {
	TableName       string                 `json:"tableName"`
	PrimaryCategory string                 `json:"primaryCategory"`
	Confidence      float64                `json:"confidence"`
	Scores          []LibraryCategoryScore `json:"scores"`
}

// LibraryClassificationOptions configures the classification algorithm in the library interface
type LibraryClassificationOptions struct {
	TopN      int     `json:"topN"`
	Threshold float64 `json:"threshold"`
}

// DefaultLibraryClassificationOptions returns default classification options for the library interface
func DefaultLibraryClassificationOptions() LibraryClassificationOptions {
	return LibraryClassificationOptions{
		TopN:      3,
		Threshold: 0.1,
	}
}

// LibraryTableClassifier handles table classification in the library interface
type LibraryTableClassifier struct {
	service  *classifier.Service
	adapters map[string]adapters.SchemaIngester
}

// NewLibraryTableClassifier creates a new LibraryTableClassifier with default configuration
func NewLibraryTableClassifier() *LibraryTableClassifier {
	return &LibraryTableClassifier{
		service:  classifier.NewService(),
		adapters: initializeLibraryAdapters(),
	}
}

// NewLibraryTableClassifierWithConfig creates a new LibraryTableClassifier with custom configuration
func NewLibraryTableClassifierWithConfig(cfg *config.Config) *LibraryTableClassifier {
	var service *classifier.Service
	if cfg != nil && cfg.Weights != nil {
		service = classifier.NewServiceWithWeights(cfg.Weights)
	} else {
		service = classifier.NewService()
	}

	return &LibraryTableClassifier{
		service:  service,
		adapters: initializeLibraryAdapters(),
	}
}

// initializeLibraryAdapters sets up the database adapters for the library interface
func initializeLibraryAdapters() map[string]adapters.SchemaIngester {
	adaptersMap := make(map[string]adapters.SchemaIngester)

	// Initialize adapters for different database types
	adaptersMap["postgres"] = &adapters.PostgresIngester{}
	adaptersMap["postgresql"] = &adapters.PostgresIngester{}
	adaptersMap["mysql"] = &adapters.MySQLIngester{}
	adaptersMap["mariadb"] = &adapters.MariaDBIngester{}
	adaptersMap["mssql"] = &adapters.MSSQLIngester{}
	adaptersMap["sqlserver"] = &adapters.MSSQLIngester{}
	adaptersMap["oracle"] = &adapters.OracleIngester{}
	adaptersMap["db2"] = &adapters.Db2Ingester{}
	adaptersMap["cockroach"] = &adapters.CockroachIngester{}
	adaptersMap["cockroachdb"] = &adapters.CockroachIngester{}
	adaptersMap["clickhouse"] = &adapters.ClickhouseIngester{}
	adaptersMap["cassandra"] = &adapters.CassandraIngester{}
	adaptersMap["mongodb"] = &adapters.MongoDBIngester{}
	adaptersMap["redis"] = &adapters.RedisIngester{}
	adaptersMap["neo4j"] = &adapters.Neo4jIngester{}
	adaptersMap["elasticsearch"] = &adapters.ElasticsearchIngester{}
	adaptersMap["snowflake"] = &adapters.SnowflakeIngester{}
	adaptersMap["pinecone"] = &adapters.PineconeIngester{}
	adaptersMap["edgedb"] = &adapters.EdgeDBIngester{}

	return adaptersMap
}

// ClassifyTable classifies a single table based on its metadata
func (c *LibraryTableClassifier) ClassifyTable(metadata LibraryTableMetadata, options *LibraryClassificationOptions) (*LibraryClassificationResult, error) {
	if options == nil {
		defaultOptions := DefaultLibraryClassificationOptions()
		options = &defaultOptions
	}

	// Convert LibraryTableMetadata to the internal format expected by the service
	protoMetadata := c.convertToProtoMetadata(metadata)

	// Create classification request
	req := &unifiedmodel.ClassifyRequest{
		Metadata:  protoMetadata,
		TopN:      int32(options.TopN),
		Threshold: options.Threshold,
	}

	// Classify using the internal service
	resp, err := c.service.Classify(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("failed to classify table %s: %w", metadata.Name, err)
	}

	// Convert response to our result format
	result := &LibraryClassificationResult{
		TableName:       metadata.Name,
		PrimaryCategory: resp.PrimaryCategory,
		Confidence:      resp.Confidence,
		Scores:          make([]LibraryCategoryScore, len(resp.Scores)),
	}

	for i, score := range resp.Scores {
		result.Scores[i] = LibraryCategoryScore{
			Category: score.Category,
			Score:    score.Score,
			Reason:   score.Reason,
		}
	}

	return result, nil
}

// ClassifyTables classifies multiple tables
func (c *LibraryTableClassifier) ClassifyTables(tables []LibraryTableMetadata, options *LibraryClassificationOptions) ([]LibraryClassificationResult, error) {
	if options == nil {
		defaultOptions := DefaultLibraryClassificationOptions()
		options = &defaultOptions
	}

	var results []LibraryClassificationResult

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
func (c *LibraryTableClassifier) ClassifyFromJSON(schemaType string, data json.RawMessage, options *LibraryClassificationOptions) ([]LibraryClassificationResult, error) {
	if options == nil {
		defaultOptions := DefaultLibraryClassificationOptions()
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

// convertToProtoMetadata converts our LibraryTableMetadata to the internal proto format
func (c *LibraryTableClassifier) convertToProtoMetadata(metadata LibraryTableMetadata) *unifiedmodel.TableMetadata {
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

// convertFromUnifiedModel converts a unified model to our library table metadata format
func (c *LibraryTableClassifier) convertFromUnifiedModel(model *models.UnifiedModel) []LibraryTableMetadata {
	var tables []LibraryTableMetadata

	for _, table := range model.Tables {
		columns := make([]LibraryColumnMetadata, len(table.Columns))
		for i, col := range table.Columns {
			colMetadata := LibraryColumnMetadata{
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

		indexes := make([]LibraryIndexMetadata, len(table.Indexes))
		for i, idx := range table.Indexes {
			// Convert IndexColumn slice to string slice
			columnNames := make([]string, len(idx.Columns))
			for j, col := range idx.Columns {
				columnNames[j] = col.ColumnName
			}

			indexes[i] = LibraryIndexMetadata{
				Name:      idx.Name,
				Columns:   columnNames,
				IsUnique:  idx.IsUnique,
				IsPrimary: false, // models.Index doesn't have IsPrimary field
			}
		}

		// Convert constraints to string slice
		constraintStrings := make([]string, len(table.Constraints))
		for i, constraint := range table.Constraints {
			constraintStrings[i] = constraint.Type
		}

		tableMetadata := LibraryTableMetadata{
			Name:        table.Name,
			Columns:     columns,
			Indexes:     indexes,
			Constraints: constraintStrings,
			Tags:        make(map[string]string), // models.Table doesn't have Tags field
		}

		tables = append(tables, tableMetadata)
	}

	return tables
}

// LibraryClassifyTable is a convenience function that creates a classifier and classifies a single table
func LibraryClassifyTable(metadata LibraryTableMetadata, options *LibraryClassificationOptions) (*LibraryClassificationResult, error) {
	classifier := NewLibraryTableClassifier()
	return classifier.ClassifyTable(metadata, options)
}

// LibraryClassifyTables is a convenience function that creates a classifier and classifies multiple tables
func LibraryClassifyTables(tables []LibraryTableMetadata, options *LibraryClassificationOptions) ([]LibraryClassificationResult, error) {
	classifier := NewLibraryTableClassifier()
	return classifier.ClassifyTables(tables, options)
}

// LibraryClassifyFromJSON is a convenience function that creates a classifier and classifies from JSON schema
func LibraryClassifyFromJSON(schemaType string, data json.RawMessage, options *LibraryClassificationOptions) ([]LibraryClassificationResult, error) {
	classifier := NewLibraryTableClassifier()
	return classifier.ClassifyFromJSON(schemaType, data, options)
}
