package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	pb "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/adapters"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/classifier"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/comparison"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/detection"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/generators"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/matching"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator"
)

type Server struct {
	pb.UnimplementedUnifiedModelServiceServer
	engine *Engine
}

func NewServer(engine *Engine) *Server {
	return &Server{
		engine: engine,
	}
}

func (s *Server) Translate(ctx context.Context, req *pb.TranslationRequest) (*pb.TranslationResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	translator := translator.NewSchemaTranslator()

	// Register all adapters
	s.registerAdapters(translator)

	result, err := translator.Translate(req.SourceType, req.Target, req.SourceStructure)
	if err != nil {
		return nil, fmt.Errorf("translation failed: %w", err)
	}

	// Marshal the converted structure to JSON bytes
	convertedBytes, err := json.Marshal(result.ConvertedStructure)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal converted structure: %w", err)
	}

	return &pb.TranslationResponse{
		TargetStructure: convertedBytes,
		Warnings:        result.Warnings,
	}, nil
}

func (s *Server) Generate(ctx context.Context, req *pb.GenerationRequest) (*pb.GenerationResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	// First translate the schema
	translator := translator.NewSchemaTranslator()

	// Register all adapters
	s.registerAdapters(translator)

	translationResult, err := translator.Translate(req.SourceType, req.Target, req.Structure)
	if err != nil {
		return nil, fmt.Errorf("translation failed: %w", err)
	}

	// Create generator based on target
	generator, err := s.createGenerator(req.Target)
	if err != nil {
		return nil, err
	}

	statements, err := generator.GenerateCreateStatements(translationResult.ConvertedStructure)
	if err != nil {
		return nil, fmt.Errorf("statement generation failed: %w", err)
	}

	return &pb.GenerationResponse{
		Statements: statements,
		Warnings:   translationResult.Warnings,
	}, nil
}

func (s *Server) CompareSchemas(ctx context.Context, req *pb.CompareRequest) (*pb.CompareResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	comparator := comparison.NewSchemaComparator()

	result, err := comparator.CompareSchemas(req.SchemaType, req.PreviousSchema, req.CurrentSchema)
	if err != nil {
		return nil, fmt.Errorf("schema comparison failed: %w", err)
	}

	return &pb.CompareResponse{
		HasChanges: result.HasChanges,
		Changes:    result.Changes,
		Warnings:   result.Warnings,
	}, nil
}

func (s *Server) MatchSchemasEnriched(ctx context.Context, req *pb.MatchSchemasEnrichedRequest) (*pb.MatchSchemasEnrichedResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	matcher := matching.NewEnrichedSchemaMatcher()

	// Convert protobuf options to internal options
	var options *matching.EnrichedMatchOptions
	if req.Options != nil {
		options = &matching.EnrichedMatchOptions{
			NameSimilarityThreshold:  req.Options.NameSimilarityThreshold,
			PoorMatchThreshold:       req.Options.PoorMatchThreshold,
			NameWeight:               req.Options.NameWeight,
			TypeWeight:               req.Options.TypeWeight,
			ClassificationWeight:     req.Options.ClassificationWeight,
			PrivilegedDataWeight:     req.Options.PrivilegedDataWeight,
			TableStructureWeight:     req.Options.TableStructureWeight,
			EnableCrossTableMatching: req.Options.EnableCrossTableMatching,
		}
	}

	result, err := matcher.MatchSchemasEnriched(req.SourceSchemaType, req.SourceSchema, req.TargetSchemaType, req.TargetSchema, options)
	if err != nil {
		return nil, fmt.Errorf("enriched schema matching failed: %w", err)
	}

	// Convert table matches to proto format
	tableMatches := make([]*pb.EnrichedTableMatch, len(result.TableMatches))
	for i, match := range result.TableMatches {
		// Convert column matches
		columnMatches := make([]*pb.EnrichedColumnMatch, len(match.ColumnMatches))
		for j, colMatch := range match.ColumnMatches {
			columnMatches[j] = &pb.EnrichedColumnMatch{
				SourceTable:              colMatch.SourceTable,
				TargetTable:              colMatch.TargetTable,
				SourceColumn:             colMatch.SourceColumn,
				TargetColumn:             colMatch.TargetColumn,
				Score:                    colMatch.Score,
				IsTypeCompatible:         colMatch.IsTypeCompatible,
				IsPoorMatch:              colMatch.IsPoorMatch,
				IsUnmatched:              colMatch.IsUnmatched,
				PrivilegedDataMatch:      colMatch.PrivilegedDataMatch,
				DataCategoryMatch:        colMatch.DataCategoryMatch,
				PrivilegedConfidenceDiff: colMatch.PrivilegedConfidenceDiff,
			}
		}

		tableMatches[i] = &pb.EnrichedTableMatch{
			SourceTable:                  match.SourceTable,
			TargetTable:                  match.TargetTable,
			Score:                        match.Score,
			IsPoorMatch:                  match.IsPoorMatch,
			IsUnmatched:                  match.IsUnmatched,
			ClassificationMatch:          match.ClassificationMatch,
			ClassificationConfidenceDiff: match.ClassificationConfidenceDiff,
			MatchedColumns:               int32(match.MatchedColumns),
			TotalSourceColumns:           int32(match.TotalSourceColumns),
			TotalTargetColumns:           int32(match.TotalTargetColumns),
			ColumnMatches:                columnMatches,
		}
	}

	// Convert unmatched columns to proto format
	unmatchedColumns := make([]*pb.EnrichedColumnMatch, len(result.UnmatchedColumns))
	for i, match := range result.UnmatchedColumns {
		unmatchedColumns[i] = &pb.EnrichedColumnMatch{
			SourceTable:              match.SourceTable,
			TargetTable:              match.TargetTable,
			SourceColumn:             match.SourceColumn,
			TargetColumn:             match.TargetColumn,
			Score:                    match.Score,
			IsTypeCompatible:         match.IsTypeCompatible,
			IsPoorMatch:              match.IsPoorMatch,
			IsUnmatched:              match.IsUnmatched,
			PrivilegedDataMatch:      match.PrivilegedDataMatch,
			DataCategoryMatch:        match.DataCategoryMatch,
			PrivilegedConfidenceDiff: match.PrivilegedConfidenceDiff,
		}
	}

	return &pb.MatchSchemasEnrichedResponse{
		TableMatches:           tableMatches,
		UnmatchedColumns:       unmatchedColumns,
		Warnings:               result.Warnings,
		OverallSimilarityScore: result.OverallSimilarity,
	}, nil
}

func (s *Server) MatchTablesEnriched(ctx context.Context, req *pb.MatchTablesEnrichedRequest) (*pb.MatchTablesEnrichedResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	matcher := matching.NewEnrichedSchemaMatcher()

	// Convert protobuf options to internal options
	var options *matching.EnrichedMatchOptions
	if req.Options != nil {
		options = &matching.EnrichedMatchOptions{
			NameSimilarityThreshold:  req.Options.NameSimilarityThreshold,
			PoorMatchThreshold:       req.Options.PoorMatchThreshold,
			NameWeight:               req.Options.NameWeight,
			TypeWeight:               req.Options.TypeWeight,
			ClassificationWeight:     req.Options.ClassificationWeight,
			PrivilegedDataWeight:     req.Options.PrivilegedDataWeight,
			TableStructureWeight:     req.Options.TableStructureWeight,
			EnableCrossTableMatching: req.Options.EnableCrossTableMatching,
		}
	}

	// Convert protobuf tables to internal structures
	sourceTables := s.convertProtoToEnrichedTables(req.SourceTables)
	targetTables := s.convertProtoToEnrichedTables(req.TargetTables)

	results, err := matcher.MatchTablesEnriched(sourceTables, targetTables, options)
	if err != nil {
		return nil, fmt.Errorf("enriched table matching failed: %w", err)
	}

	// Convert matches to proto format
	matches := make([]*pb.EnrichedTableMatch, len(results))
	for i, result := range results {
		// Convert column matches
		columnMatches := make([]*pb.EnrichedColumnMatch, len(result.ColumnMatches))
		for j, colMatch := range result.ColumnMatches {
			columnMatches[j] = &pb.EnrichedColumnMatch{
				SourceTable:              colMatch.SourceTable,
				TargetTable:              colMatch.TargetTable,
				SourceColumn:             colMatch.SourceColumn,
				TargetColumn:             colMatch.TargetColumn,
				Score:                    colMatch.Score,
				IsTypeCompatible:         colMatch.IsTypeCompatible,
				IsPoorMatch:              colMatch.IsPoorMatch,
				IsUnmatched:              colMatch.IsUnmatched,
				PrivilegedDataMatch:      colMatch.PrivilegedDataMatch,
				DataCategoryMatch:        colMatch.DataCategoryMatch,
				PrivilegedConfidenceDiff: colMatch.PrivilegedConfidenceDiff,
			}
		}

		matches[i] = &pb.EnrichedTableMatch{
			SourceTable:                  result.SourceTable,
			TargetTable:                  result.TargetTable,
			Score:                        result.Score,
			IsPoorMatch:                  result.IsPoorMatch,
			IsUnmatched:                  result.IsUnmatched,
			ClassificationMatch:          result.ClassificationMatch,
			ClassificationConfidenceDiff: result.ClassificationConfidenceDiff,
			MatchedColumns:               int32(result.MatchedColumns),
			TotalSourceColumns:           int32(result.TotalSourceColumns),
			TotalTargetColumns:           int32(result.TotalTargetColumns),
			ColumnMatches:                columnMatches,
		}
	}

	// Calculate overall similarity score
	var overallScore float64
	if len(results) > 0 {
		var totalScore float64
		for _, result := range results {
			if !result.IsUnmatched {
				totalScore += result.Score
			}
		}
		overallScore = totalScore / float64(len(results))
	}

	return &pb.MatchTablesEnrichedResponse{
		Matches:                matches,
		Warnings:               []string{},
		OverallSimilarityScore: overallScore,
	}, nil
}

func (s *Server) Classify(ctx context.Context, req *pb.ClassifyRequest) (*pb.ClassifyResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	// Convert proto TableMetadata to classifier TableMetadata
	classifierMetadata := convertProtoToClassifierMetadata(req.Metadata)

	// Create classification options from request
	options := &classifier.ClassificationOptions{
		TopN:      int(req.TopN),
		Threshold: req.Threshold,
	}

	// Use default values if not specified
	if options.TopN <= 0 {
		options.TopN = 3
	}
	if options.Threshold <= 0 {
		options.Threshold = 0.1
	}

	classifierInstance := classifier.NewTableClassifier()

	result, err := classifierInstance.ClassifyTable(classifierMetadata, options)
	if err != nil {
		return nil, fmt.Errorf("table classification failed: %w", err)
	}

	// Convert classifier CategoryScore to proto CategoryScore
	scores := make([]*pb.CategoryScore, len(result.Scores))
	for i, score := range result.Scores {
		scores[i] = &pb.CategoryScore{
			Category: score.Category,
			Score:    score.Score,
			Reason:   score.Reason,
		}
	}

	return &pb.ClassifyResponse{
		PrimaryCategory: result.PrimaryCategory,
		Confidence:      result.Confidence,
		Scores:          scores,
	}, nil
}

func (s *Server) DetectPrivilegedData(ctx context.Context, req *pb.DetectRequest) (*pb.DetectResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	detector := detection.NewPrivilegedDataDetector()

	result, err := detector.DetectPrivilegedData(req.SchemaType, req.Schema)
	if err != nil {
		return nil, fmt.Errorf("privileged data detection failed: %w", err)
	}

	// Convert findings to proto format
	findings := make([]*pb.PrivilegedDataFinding, len(result.Findings))
	for i, finding := range result.Findings {
		findings[i] = &pb.PrivilegedDataFinding{
			TableName:    finding.TableName,
			ColumnName:   finding.ColumnName,
			DataType:     finding.DataType,
			DataCategory: finding.DataCategory,
			Confidence:   finding.Confidence,
			Description:  finding.Description,
			ExampleValue: finding.ExampleValue,
		}
	}

	return &pb.DetectResponse{
		Findings: findings,
		Warnings: result.Warnings,
	}, nil
}

func (s *Server) AnalyzeSchemaEnriched(ctx context.Context, req *pb.AnalyzeSchemaEnrichedRequest) (*pb.AnalyzeSchemaEnrichedResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	// Parse schema into unified model using the translator
	translator := translator.NewSchemaTranslator()
	s.registerAdapters(translator)

	// Convert schema to unified model
	translationResult, err := translator.Translate(req.SchemaType, "unified", req.Schema)
	if err != nil {
		return nil, fmt.Errorf("schema parsing failed: %w", err)
	}

	// Extract the unified model from the translation result
	unifiedModel, ok := translationResult.ConvertedStructure.(struct {
		SchemaType string `json:"schemaType"`
		*models.UnifiedModel
	})
	if !ok {
		return nil, fmt.Errorf("failed to extract unified model from translation result")
	}

	// Run privileged data detection on the schema
	detector := detection.NewPrivilegedDataDetector()
	detectionResult, err := detector.DetectPrivilegedData(req.SchemaType, req.Schema)
	if err != nil {
		return nil, fmt.Errorf("privileged data detection failed: %w", err)
	}

	// Create a map for quick lookup of privileged data findings
	privilegedDataMap := make(map[string]map[string]*detection.PrivilegedDataFinding)
	for _, finding := range detectionResult.Findings {
		if _, exists := privilegedDataMap[finding.TableName]; !exists {
			privilegedDataMap[finding.TableName] = make(map[string]*detection.PrivilegedDataFinding)
		}
		privilegedDataMap[finding.TableName][finding.ColumnName] = &finding
	}

	// Process each table
	enrichedTables := make([]*pb.EnrichedTableMetadata, 0, len(unifiedModel.Tables))
	allWarnings := append([]string{}, translationResult.Warnings...)
	allWarnings = append(allWarnings, detectionResult.Warnings...)

	for _, table := range unifiedModel.Tables {
		// Convert table to TableMetadata for classification
		tableMetadata := s.convertToTableMetadata(table, req.SchemaType)

		// Run classification on the table
		classifierInstance := classifier.NewTableClassifier()
		options := &classifier.ClassificationOptions{
			TopN:      3,
			Threshold: 0.1,
		}

		classificationResult, err := classifierInstance.ClassifyTable(tableMetadata, options)
		if err != nil {
			allWarnings = append(allWarnings, fmt.Sprintf("classification failed for table %s: %v", table.Name, err))
			// Continue with empty classification results
			classificationResult = &classifier.ClassificationResult{
				PrimaryCategory: "",
				Confidence:      0.0,
				Scores:          []classifier.CategoryScore{},
			}
		}

		// Build enriched columns
		enrichedColumns := make([]*pb.EnrichedColumnMetadata, 0, len(table.Columns))
		for _, column := range table.Columns {
			enrichedColumn := &pb.EnrichedColumnMetadata{
				Name:            column.Name,
				Type:            column.DataType.Name,
				IsPrimaryKey:    column.IsPrimaryKey,
				IsNullable:      column.IsNullable,
				IsArray:         column.DataType.IsArray,
				IsAutoIncrement: column.IsAutoIncrement,
			}

			// Add column default if available
			if column.DefaultValue != nil {
				enrichedColumn.ColumnDefault = *column.DefaultValue
			}

			// Add varchar length if available
			if column.DataType.Length > 0 {
				enrichedColumn.VarcharLength = int32(column.DataType.Length)
			}

			// Add vector properties (simplified - checking if the data type suggests vector data)
			if s.isVectorDataType(column.DataType.Name) {
				enrichedColumn.VectorDimension = int32(column.DataType.Precision) // Use precision as dimension approximation
				enrichedColumn.VectorDistanceMetric = "cosine"                    // Default metric
			}

			// Add index information (simplified - extract from constraints and indexes)
			enrichedColumn.Indexes = s.extractColumnIndexes(column, table)

			// Add foreign key information (simplified - check constraints)
			enrichedColumn.IsForeignKey = s.isColumnForeignKey(column, table)

			// Add privileged data detection results
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

		// Convert classification scores to proto format
		classificationScores := make([]*pb.CategoryScore, len(classificationResult.Scores))
		for i, score := range classificationResult.Scores {
			classificationScores[i] = &pb.CategoryScore{
				Category: score.Category,
				Score:    score.Score,
				Reason:   score.Reason,
			}
		}

		// Build enriched table metadata
		enrichedTable := &pb.EnrichedTableMetadata{
			Engine:                   req.SchemaType,
			Schema:                   table.Schema,
			Name:                     table.Name,
			Columns:                  enrichedColumns,
			Properties:               make(map[string]string),
			TableType:                table.TableType,
			PrimaryCategory:          classificationResult.PrimaryCategory,
			ClassificationConfidence: classificationResult.Confidence,
			ClassificationScores:     classificationScores,
		}

		// Add any additional table properties from the unified model
		// This is simplified - in a real implementation you might extract more metadata
		if table.Comment != "" {
			enrichedTable.Properties["comment"] = table.Comment
		}

		enrichedTables = append(enrichedTables, enrichedTable)
	}

	return &pb.AnalyzeSchemaEnrichedResponse{
		Tables:   enrichedTables,
		Warnings: allWarnings,
	}, nil
}

// convertToTableMetadata converts a unified model table to TableMetadata for classification
func (s *Server) convertToTableMetadata(table models.Table, engine string) classifier.TableMetadata {
	columns := make([]classifier.ColumnMetadata, len(table.Columns))
	for i, col := range table.Columns {
		columns[i] = classifier.ColumnMetadata{
			Name:            col.Name,
			DataType:        col.DataType.Name,
			IsNullable:      col.IsNullable,
			IsPrimaryKey:    col.IsPrimaryKey,
			IsUnique:        col.IsUnique,
			IsAutoIncrement: col.IsAutoIncrement,
			IsArray:         col.DataType.IsArray,
		}

		// Add column default if available
		if col.DefaultValue != nil {
			columns[i].ColumnDefault = col.DefaultValue
		}

		// Add varchar length if available
		if col.DataType.Length > 0 {
			length := int(col.DataType.Length)
			columns[i].VarcharLength = &length
		}
	}

	return classifier.TableMetadata{
		Name:        table.Name,
		Columns:     columns,
		Indexes:     []classifier.IndexMetadata{}, // Simplified - could be extracted from table.Indexes
		Constraints: []string{},                   // Simplified - could be extracted from table.Constraints
		Tags:        make(map[string]string),      // Could add properties if needed
	}
}

// extractColumnIndexes extracts index information for a column from table metadata
func (s *Server) extractColumnIndexes(column models.Column, table models.Table) []string {
	indexes := []string{}

	// Check table-level indexes that include this column
	for _, index := range table.Indexes {
		for _, indexCol := range index.Columns {
			if indexCol.ColumnName == column.Name {
				if index.IndexMethod != "" {
					indexes = append(indexes, index.IndexMethod)
				} else {
					indexes = append(indexes, "btree") // default
				}
				break
			}
		}
	}

	// Check constraints that imply indexes
	for _, constraint := range table.Constraints {
		for _, constraintCol := range constraint.Columns {
			if constraintCol == column.Name {
				switch constraint.Type {
				case "PRIMARY KEY":
					indexes = append(indexes, "primary")
				case "UNIQUE":
					indexes = append(indexes, "unique")
				case "FOREIGN KEY":
					indexes = append(indexes, "foreign")
				}
				break
			}
		}
	}

	return indexes
}

// isColumnForeignKey checks if a column is a foreign key
func (s *Server) isColumnForeignKey(column models.Column, table models.Table) bool {
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

// isVectorDataType checks if a data type suggests vector data
func (s *Server) isVectorDataType(dataType string) bool {
	vectorTypes := []string{"vector", "embedding", "float[]", "real[]", "numeric[]"}
	dataTypeLower := strings.ToLower(dataType)
	for _, vType := range vectorTypes {
		if strings.Contains(dataTypeLower, vType) {
			return true
		}
	}
	return false
}

// convertProtoToClassifierMetadata converts proto TableMetadata to classifier TableMetadata
func convertProtoToClassifierMetadata(protoMetadata *pb.TableMetadata) classifier.TableMetadata {
	columns := make([]classifier.ColumnMetadata, len(protoMetadata.Columns))
	for i, col := range protoMetadata.Columns {
		columns[i] = classifier.ColumnMetadata{
			Name:            col.Name,
			DataType:        col.Type,
			IsNullable:      col.IsNullable,
			IsPrimaryKey:    col.IsPrimaryKey,
			IsUnique:        false, // Not available in proto
			IsAutoIncrement: col.IsAutoIncrement,
			IsArray:         col.IsArray,
			ColumnDefault:   nil, // Not available in proto
			VarcharLength:   nil, // Not available in proto
		}

		// Handle optional fields
		if col.ColumnDefault != "" {
			columns[i].ColumnDefault = &col.ColumnDefault
		}
		if col.VarcharLength > 0 {
			length := int(col.VarcharLength)
			columns[i].VarcharLength = &length
		}
	}

	return classifier.TableMetadata{
		Name:        protoMetadata.Name,
		Columns:     columns,
		Indexes:     []classifier.IndexMetadata{}, // Not available in proto
		Constraints: []string{},                   // Not available in proto
		Tags:        protoMetadata.Properties,
	}
}

// registerAdapters registers all database adapters with the translator
func (s *Server) registerAdapters(translator *translator.SchemaTranslator) {
	// Cassandra
	cassandraAdapter := &adapters.CassandraIngester{}
	translator.RegisterIngester("cassandra", cassandraAdapter)
	translator.RegisterExporter("cassandra", &adapters.CassandraExporter{})

	// ClickHouse
	clickhouseAdapter := &adapters.ClickhouseIngester{}
	translator.RegisterIngester("clickhouse", clickhouseAdapter)
	translator.RegisterExporter("clickhouse", &adapters.ClickhouseExporter{})

	// CockroachDB
	cockroachdbAdapter := &adapters.CockroachIngester{}
	translator.RegisterIngester("cockroachdb", cockroachdbAdapter)
	translator.RegisterExporter("cockroachdb", &adapters.CockroachExporter{})

	// DB2
	db2Adapter := &adapters.Db2Ingester{}
	translator.RegisterIngester("db2", db2Adapter)
	translator.RegisterExporter("db2", &adapters.Db2Exporter{})

	// EdgeDB
	edgedbAdapter := &adapters.EdgeDBIngester{}
	translator.RegisterIngester("edgedb", edgedbAdapter)
	translator.RegisterExporter("edgedb", &adapters.EdgeDBExporter{})

	// Elasticsearch
	elasticsearchAdapter := &adapters.ElasticsearchIngester{}
	translator.RegisterIngester("elasticsearch", elasticsearchAdapter)
	translator.RegisterExporter("elasticsearch", &adapters.ElasticsearchExporter{})

	// MariaDB
	mariaAdapter := &adapters.MariaDBIngester{}
	translator.RegisterIngester("mariadb", mariaAdapter)
	translator.RegisterExporter("mariadb", &adapters.MariaDBExporter{})

	// MongoDB
	mongodbAdapter := &adapters.MongoDBIngester{}
	translator.RegisterIngester("mongodb", mongodbAdapter)
	translator.RegisterExporter("mongodb", &adapters.MongoDBExporter{})

	// MSSQL
	mssqlAdapter := &adapters.MSSQLIngester{}
	translator.RegisterIngester("mssql", mssqlAdapter)
	translator.RegisterExporter("mssql", &adapters.MSSQLExporter{})

	// MySQL
	mysqlAdapter := &adapters.MySQLIngester{}
	translator.RegisterIngester("mysql", mysqlAdapter)
	translator.RegisterExporter("mysql", &adapters.MySQLExporter{})

	// Neo4j
	neo4jAdapter := &adapters.Neo4jIngester{}
	translator.RegisterIngester("neo4j", neo4jAdapter)
	translator.RegisterExporter("neo4j", &adapters.Neo4jExporter{})

	// Oracle
	oracleAdapter := &adapters.OracleIngester{}
	translator.RegisterIngester("oracle", oracleAdapter)
	translator.RegisterExporter("oracle", &adapters.OracleExporter{})

	// Pinecone
	pineconeAdapter := &adapters.PineconeIngester{}
	translator.RegisterIngester("pinecone", pineconeAdapter)
	translator.RegisterExporter("pinecone", &adapters.PineconeExporter{})

	// PostgreSQL
	postgresAdapter := &adapters.PostgresIngester{}
	translator.RegisterIngester("postgres", postgresAdapter)
	translator.RegisterExporter("postgres", &adapters.PostgresExporter{})

	// Redis
	redisAdapter := &adapters.RedisIngester{}
	translator.RegisterIngester("redis", redisAdapter)
	translator.RegisterExporter("redis", &adapters.RedisExporter{})

	// Snowflake
	snowflakeAdapter := &adapters.SnowflakeIngester{}
	translator.RegisterIngester("snowflake", snowflakeAdapter)
	translator.RegisterExporter("snowflake", &adapters.SnowflakeExporter{})
}

// createGenerator creates the appropriate statement generator based on the target database
func (s *Server) createGenerator(target string) (generators.StatementGenerator, error) {
	switch target {
	case "cassandra":
		return &generators.CassandraGenerator{}, nil
	case "clickhouse":
		return &generators.ClickhouseGenerator{}, nil
	case "cockroachdb":
		return &generators.CockroachGenerator{}, nil
	case "db2":
		return &generators.Db2Generator{}, nil
	case "edgedb":
		return &generators.EdgeDBGenerator{}, nil
	case "elasticsearch":
		return &generators.ElasticsearchGenerator{}, nil
	case "mariadb":
		return &generators.MariaDBGenerator{}, nil
	case "mongodb":
		return &generators.MongoDBGenerator{}, nil
	case "mssql":
		return &generators.MSSQLGenerator{}, nil
	case "mysql":
		return &generators.MySQLGenerator{}, nil
	case "neo4j":
		return &generators.Neo4jGenerator{}, nil
	case "oracle":
		return &generators.OracleGenerator{}, nil
	case "pinecone":
		return &generators.PineconeGenerator{}, nil
	case "postgres":
		return &generators.PostgresGenerator{}, nil
	case "redis":
		return &generators.RedisGenerator{}, nil
	case "snowflake":
		return &generators.SnowflakeGenerator{}, nil
	default:
		return nil, fmt.Errorf("unsupported target database: %s", target)
	}
}

// convertProtoToEnrichedTables converts protobuf enriched table metadata to internal structures
func (s *Server) convertProtoToEnrichedTables(protoTables []*pb.EnrichedTableMetadata) []matching.EnrichedTableStructure {
	var tables []matching.EnrichedTableStructure

	for _, protoTable := range protoTables {
		// Convert columns
		var columns []matching.EnrichedColumnStructure
		for _, protoColumn := range protoTable.Columns {
			column := matching.EnrichedColumnStructure{
				TableName:             protoTable.Name,
				Name:                  protoColumn.Name,
				DataType:              protoColumn.Type,
				IsNullable:            protoColumn.IsNullable,
				IsPrimaryKey:          protoColumn.IsPrimaryKey,
				IsForeignKey:          protoColumn.IsForeignKey,
				IsArray:               protoColumn.IsArray,
				IsAutoIncrement:       protoColumn.IsAutoIncrement,
				Indexes:               protoColumn.Indexes,
				IsPrivilegedData:      protoColumn.IsPrivilegedData,
				DataCategory:          protoColumn.DataCategory,
				PrivilegedConfidence:  protoColumn.PrivilegedConfidence,
				PrivilegedDescription: protoColumn.PrivilegedDescription,
			}

			// Handle optional fields
			if protoColumn.ColumnDefault != "" {
				column.ColumnDefault = &protoColumn.ColumnDefault
			}

			if protoColumn.VarcharLength > 0 {
				length := int(protoColumn.VarcharLength)
				column.VarcharLength = &length
			}

			columns = append(columns, column)
		}

		// Convert classification scores
		var classificationScores []matching.CategoryScore
		for _, protoScore := range protoTable.ClassificationScores {
			classificationScores = append(classificationScores, matching.CategoryScore{
				Category: protoScore.Category,
				Score:    protoScore.Score,
				Reason:   protoScore.Reason,
			})
		}

		table := matching.EnrichedTableStructure{
			Engine:                   protoTable.Engine,
			Schema:                   protoTable.Schema,
			Name:                     protoTable.Name,
			TableType:                protoTable.TableType,
			Columns:                  columns,
			Properties:               protoTable.Properties,
			PrimaryCategory:          protoTable.PrimaryCategory,
			ClassificationConfidence: protoTable.ClassificationConfidence,
			ClassificationScores:     classificationScores,
		}

		tables = append(tables, table)
	}

	return tables
}
