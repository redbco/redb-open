package engine

import (
	"context"
	"fmt"
	"time"

	pb "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/classifier"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/comparison"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/detection"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/generators"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/matching"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/core"
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

	// Create unified translator
	translatorFactory := translator.NewTranslatorFactory()
	unifiedTranslator := translatorFactory.CreateUnifiedTranslator()

	// Parse source database type
	sourceDB, err := s.parseDBType(req.SourceType)
	if err != nil {
		return nil, fmt.Errorf("invalid source database type: %w", err)
	}

	// Parse target database type
	targetDB, err := s.parseDBType(req.TargetType)
	if err != nil {
		return nil, fmt.Errorf("invalid target database type: %w", err)
	}

	// Convert protobuf UnifiedModel to Go UnifiedModel
	sourceUnifiedModel := s.convertProtoToUnifiedModel(req.SourceStructure)
	if sourceUnifiedModel == nil {
		return nil, fmt.Errorf("failed to convert source structure")
	}

	// Create translation request (now using UnifiedModel directly)
	translationReq := &core.TranslationRequest{
		SourceDatabase: sourceDB,
		SourceSchema:   sourceUnifiedModel,
		TargetDatabase: targetDB,
		TargetFormat:   "unified", // Always return unified format
		Preferences: core.TranslationPreferences{
			AcceptDataLoss:         false,
			OptimizeForPerformance: true,
			PreserveRelationships:  true,
			IncludeMetadata:        true,
			GenerateComments:       true,
		},
		RequestID:   fmt.Sprintf("translate-%d", time.Now().UnixNano()),
		RequestedAt: time.Now(),
	}

	// Perform translation
	result, err := unifiedTranslator.Translate(ctx, translationReq)
	if err != nil {
		return nil, fmt.Errorf("translation failed: %w", err)
	}

	if !result.Success {
		return nil, fmt.Errorf("translation failed: %s", result.ErrorMessage)
	}

	// Convert result to protobuf UnifiedModel
	var targetUnifiedModel *pb.UnifiedModel
	if result.UnifiedSchema != nil {
		targetUnifiedModel = s.convertUnifiedModelToProto(result.UnifiedSchema)
	}

	// Convert warnings
	warnings := make([]string, len(result.Warnings))
	for i, warning := range result.Warnings {
		warnings[i] = warning.Message
	}

	return &pb.TranslationResponse{
		TargetStructure: targetUnifiedModel,
		Warnings:        warnings,
	}, nil
}

func (s *Server) Generate(ctx context.Context, req *pb.GenerationRequest) (*pb.GenerationResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	// Parse target database type (for validation)
	_, err := s.parseDBType(req.TargetType)
	if err != nil {
		return nil, fmt.Errorf("invalid target database type: %w", err)
	}

	// Convert protobuf UnifiedModel to Go UnifiedModel
	unifiedModel := s.convertProtoToUnifiedModel(req.Structure)
	if unifiedModel == nil {
		return nil, fmt.Errorf("unified model is required")
	}

	// Create generator factory and get the appropriate generator
	generatorFactory := generators.NewGeneratorFactory()
	generator, exists := generatorFactory.GetGenerator(req.TargetType)
	if !exists {
		return nil, fmt.Errorf("generator not available for target database: %s. Available generators: postgres, mysql, mongodb, mariadb, neo4j, edgedb, cassandra", req.TargetType)
	}

	// Generate statements using the unified model
	statements, err := generator.GenerateCreateStatements(unifiedModel)
	if err != nil {
		return nil, fmt.Errorf("statement generation failed: %w", err)
	}

	// No translation warnings since we're working directly with UnifiedModel
	var warnings []string

	return &pb.GenerationResponse{
		Statements: statements,
		Warnings:   warnings,
	}, nil
}

func (s *Server) CompareSchemas(ctx context.Context, req *pb.CompareRequest) (*pb.CompareResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	// Legacy method - now deprecated in favor of CompareUnifiedModels
	return nil, fmt.Errorf("legacy schema comparison is deprecated, please use CompareUnifiedModels instead")
}

func (s *Server) CompareUnifiedModels(ctx context.Context, req *pb.CompareUnifiedModelsRequest) (*pb.CompareResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	// Import the new comparison package
	unifiedComparator := comparison.NewUnifiedSchemaComparator()

	// Convert protobuf UnifiedModel objects to Go structs
	var previousModel, currentModel *unifiedmodel.UnifiedModel

	if req.PreviousUnifiedModel != nil {
		previousModel = s.convertProtoToUnifiedModel(req.PreviousUnifiedModel)
	}

	if req.CurrentUnifiedModel != nil {
		currentModel = s.convertProtoToUnifiedModel(req.CurrentUnifiedModel)
	}

	// Compare the unified models
	result, err := unifiedComparator.CompareUnifiedModels(previousModel, currentModel)
	if err != nil {
		return nil, fmt.Errorf("unified model comparison failed: %w", err)
	}

	return &pb.CompareResponse{
		HasChanges: result.HasChanges,
		Changes:    result.Changes,
		Warnings:   result.Warnings,
	}, nil
}

// ClassifyUnifiedModel classifies tables in a UnifiedModel and returns enrichment data
func (s *Server) ClassifyUnifiedModel(ctx context.Context, req *pb.ClassifyUnifiedModelRequest) (*pb.ClassifyUnifiedModelResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	// Convert protobuf UnifiedModel to Go struct
	unifiedModel := s.convertProtoToUnifiedModel(req.UnifiedModel)
	if unifiedModel == nil {
		return nil, fmt.Errorf("unified model is required")
	}

	// Create classifier and classify the model
	classifierInstance := classifier.NewTableClassifier()
	options := &classifier.ClassificationOptions{
		TopN:      3,
		Threshold: 0.1,
	}

	enrichments, err := classifierInstance.ClassifyUnifiedModel(unifiedModel, options)
	if err != nil {
		return nil, fmt.Errorf("unified model classification failed: %w", err)
	}

	// Create enrichment structure
	enrichmentData := &unifiedmodel.UnifiedModelEnrichment{
		SchemaID:          fmt.Sprintf("classify-%d", time.Now().UnixNano()),
		EnrichmentVersion: "1.0",
		GeneratedAt:       time.Now(),
		GeneratedBy:       "unifiedmodel-service",
		TableEnrichments:  make(map[string]unifiedmodel.TableEnrichment),
		ColumnEnrichments: make(map[string]unifiedmodel.ColumnEnrichment),
	}

	// Populate table enrichments
	for _, enrichment := range enrichments {
		// Find the corresponding table name from the enrichment
		for tableName := range unifiedModel.Tables {
			// Simple matching - in a real implementation you might need more sophisticated matching
			enrichmentData.TableEnrichments[tableName] = enrichment
			break // For now, just take the first table
		}
	}

	// Convert enrichment data to protobuf
	protoEnrichment := s.convertEnrichmentToProto(enrichmentData)

	return &pb.ClassifyUnifiedModelResponse{
		UnifiedModelEnrichment: protoEnrichment,
	}, nil
}

// AnalyzeSchema analyzes a schema and returns basic table information
func (s *Server) AnalyzeSchema(ctx context.Context, req *pb.AnalyzeSchemaRequest) (*pb.AnalyzeSchemaResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	// Convert protobuf UnifiedModel to Go UnifiedModel
	unifiedModel := s.convertProtoToUnifiedModel(req.UnifiedModel)
	if unifiedModel == nil {
		return nil, fmt.Errorf("unified model is required")
	}

	// Extract table information from the unified model
	tables := make([]*pb.Table, 0, len(unifiedModel.Tables))
	for _, table := range unifiedModel.Tables {
		protoTable := s.convertTableToProto(table)
		tables = append(tables, protoTable)
	}

	return &pb.AnalyzeSchemaResponse{
		Tables: tables,
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

	// Convert protobuf UnifiedModel to Go UnifiedModel
	unifiedModel := s.convertProtoToUnifiedModel(req.UnifiedModel)
	if unifiedModel == nil {
		return nil, fmt.Errorf("unified model is required")
	}

	// Run privileged data detection on the unified model
	detector := detection.NewPrivilegedDataDetector()
	result, err := detector.DetectPrivilegedData(unifiedModel)
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

	// Convert protobuf UnifiedModel to Go UnifiedModel
	unifiedModel := s.convertProtoToUnifiedModel(req.UnifiedModel)
	if unifiedModel == nil {
		return nil, fmt.Errorf("unified model is required")
	}

	// Run privileged data detection on the unified model
	detector := detection.NewPrivilegedDataDetector()
	detectionResult, err := detector.DetectPrivilegedData(unifiedModel)
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
	allWarnings := append([]string{}, detectionResult.Warnings...)

	for _, table := range unifiedModel.Tables {
		// Convert table to TableMetadata for classification
		tableMetadata := s.convertUnifiedTableToMetadata(table, req.SchemaType)

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
				Type:            column.DataType,
				IsPrimaryKey:    column.IsPrimaryKey,
				IsNullable:      column.Nullable,
				IsAutoIncrement: column.AutoIncrement,
			}

			// Add column default if available
			if column.Default != "" {
				enrichedColumn.ColumnDefault = column.Default
			}

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
			Name:                     table.Name,
			Columns:                  enrichedColumns,
			Properties:               make(map[string]string),
			PrimaryCategory:          classificationResult.PrimaryCategory,
			ClassificationConfidence: classificationResult.Confidence,
			ClassificationScores:     classificationScores,
		}

		enrichedTables = append(enrichedTables, enrichedTable)
	}

	return &pb.AnalyzeSchemaEnrichedResponse{
		Tables:   enrichedTables,
		Warnings: allWarnings,
	}, nil
}

// parseDBType parses a database type string into a DatabaseType enum
func (s *Server) parseDBType(dbTypeStr string) (dbcapabilities.DatabaseType, error) {
	switch dbTypeStr {
	case "postgres", "postgresql":
		return dbcapabilities.PostgreSQL, nil
	case "mysql":
		return dbcapabilities.MySQL, nil
	case "mongodb":
		return dbcapabilities.MongoDB, nil
	case "cassandra":
		return dbcapabilities.Cassandra, nil
	case "redis":
		return dbcapabilities.Redis, nil
	case "elasticsearch":
		return dbcapabilities.Elasticsearch, nil
	case "clickhouse":
		return dbcapabilities.ClickHouse, nil
	case "snowflake":
		return dbcapabilities.Snowflake, nil
	case "mssql", "sqlserver":
		return dbcapabilities.SQLServer, nil
	case "oracle":
		return dbcapabilities.Oracle, nil
	case "mariadb":
		return dbcapabilities.MariaDB, nil
	case "cockroachdb":
		return dbcapabilities.CockroachDB, nil
	case "db2":
		return dbcapabilities.DB2, nil
	case "neo4j":
		return dbcapabilities.Neo4j, nil
	case "pinecone":
		return dbcapabilities.Pinecone, nil
	case "edgedb":
		return dbcapabilities.EdgeDB, nil
	case "unified":
		// For unified model, we'll use a special constant or return a specific type
		return "unified", nil
	default:
		return "", fmt.Errorf("unsupported database type: %s", dbTypeStr)
	}
}

// convertUnifiedTableToMetadata converts a shared UnifiedModel table to TableMetadata for classification
func (s *Server) convertUnifiedTableToMetadata(table unifiedmodel.Table, engine string) classifier.TableMetadata {
	columns := make([]classifier.ColumnMetadata, 0, len(table.Columns))
	for _, col := range table.Columns {
		colMetadata := classifier.ColumnMetadata{
			Name:            col.Name,
			DataType:        col.DataType,
			IsNullable:      col.Nullable,
			IsPrimaryKey:    col.IsPrimaryKey,
			IsUnique:        false, // Not available in shared model
			IsAutoIncrement: col.AutoIncrement,
			IsArray:         false, // Not directly available in shared model
		}

		// Add column default if available
		if col.Default != "" {
			colMetadata.ColumnDefault = &col.Default
		}

		columns = append(columns, colMetadata)
	}

	// Convert indexes
	indexes := make([]classifier.IndexMetadata, 0, len(table.Indexes))
	for _, idx := range table.Indexes {
		indexes = append(indexes, classifier.IndexMetadata{
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

	return classifier.TableMetadata{
		Name:        table.Name,
		Columns:     columns,
		Indexes:     indexes,
		Constraints: constraints,
		Tags:        make(map[string]string), // Could be populated from table options if needed
	}
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

// MatchUnifiedModelsEnriched implements the gRPC MatchUnifiedModelsEnriched method
func (s *Server) MatchUnifiedModelsEnriched(ctx context.Context, req *pb.MatchUnifiedModelsEnrichedRequest) (*pb.MatchUnifiedModelsEnrichedResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	// Validate request
	if req.SourceUnifiedModel == nil {
		return nil, fmt.Errorf("source_unified_model is required")
	}
	if req.TargetUnifiedModel == nil {
		return nil, fmt.Errorf("target_unified_model is required")
	}

	// Convert protobuf unified models to Go structs
	sourceModel := s.convertProtoToUnifiedModel(req.SourceUnifiedModel)
	if sourceModel == nil {
		return nil, fmt.Errorf("failed to convert source unified model")
	}

	targetModel := s.convertProtoToUnifiedModel(req.TargetUnifiedModel)
	if targetModel == nil {
		return nil, fmt.Errorf("failed to convert target unified model")
	}

	// Convert enrichments (optional)
	var sourceEnrichment *unifiedmodel.UnifiedModelEnrichment
	if req.SourceEnrichment != nil {
		sourceEnrichment = s.convertProtoToEnrichment(req.SourceEnrichment)
	}

	var targetEnrichment *unifiedmodel.UnifiedModelEnrichment
	if req.TargetEnrichment != nil {
		targetEnrichment = s.convertProtoToEnrichment(req.TargetEnrichment)
	}

	// Convert protobuf options to internal options
	options := s.convertMatchOptions(req.Options)

	// Create unified matcher and perform matching
	matcher := matching.NewUnifiedModelMatcher()
	result, err := matcher.MatchUnifiedModels(sourceModel, sourceEnrichment, targetModel, targetEnrichment, options)
	if err != nil {
		return nil, fmt.Errorf("matching failed: %w", err)
	}

	// Convert result to protobuf format
	response := &pb.MatchUnifiedModelsEnrichedResponse{
		TableMatches:           s.convertTableMatchesToProto(result.TableMatches),
		UnmatchedColumns:       s.convertColumnMatchesToProto(result.UnmatchedColumns),
		Warnings:               result.Warnings,
		OverallSimilarityScore: result.OverallSimilarityScore,
	}

	return response, nil
}

// convertMatchOptions converts protobuf MatchOptions to internal UnifiedMatchOptions
func (s *Server) convertMatchOptions(protoOptions *pb.MatchOptions) *matching.UnifiedMatchOptions {
	if protoOptions == nil {
		defaultOptions := matching.DefaultUnifiedMatchOptions()
		return &defaultOptions
	}

	return &matching.UnifiedMatchOptions{
		NameSimilarityThreshold:  protoOptions.NameSimilarityThreshold,
		PoorMatchThreshold:       protoOptions.PoorMatchThreshold,
		NameWeight:               protoOptions.NameWeight,
		TypeWeight:               protoOptions.TypeWeight,
		ClassificationWeight:     protoOptions.ClassificationWeight,
		PrivilegedDataWeight:     protoOptions.PrivilegedDataWeight,
		TableStructureWeight:     protoOptions.TableStructureWeight,
		EnableCrossTableMatching: protoOptions.EnableCrossTableMatching,
	}
}

// convertTableMatchesToProto converts internal table matches to protobuf format
func (s *Server) convertTableMatchesToProto(matches []matching.UnifiedTableMatch) []*pb.EnrichedTableMatch {
	var protoMatches []*pb.EnrichedTableMatch

	for _, match := range matches {
		protoMatch := &pb.EnrichedTableMatch{
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
			ColumnMatches:                s.convertColumnMatchesToProto(match.ColumnMatches),
		}
		protoMatches = append(protoMatches, protoMatch)
	}

	return protoMatches
}

// convertColumnMatchesToProto converts internal column matches to protobuf format
func (s *Server) convertColumnMatchesToProto(matches []matching.UnifiedColumnMatch) []*pb.EnrichedColumnMatch {
	var protoMatches []*pb.EnrichedColumnMatch

	for _, match := range matches {
		protoMatch := &pb.EnrichedColumnMatch{
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
		protoMatches = append(protoMatches, protoMatch)
	}

	return protoMatches
}
