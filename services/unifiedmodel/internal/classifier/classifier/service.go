package classifier

import (
	"context"
	"fmt"
	"math"
	"time"

	pb "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/classifier/features"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/classifier/ingest"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/classifier/scoring"
)

// Service implements the table classification service
type Service struct {
	pb.UnimplementedUnifiedModelServiceServer
	extractor *features.Extractor
	scorer    *scoring.ScoringEngine
	adapter   *ingest.UniversalAdapter
}

// NewService creates a new classification service
func NewService() *Service {
	return &Service{
		extractor: features.NewExtractor(),
		scorer:    scoring.NewScoringEngine(),
		adapter:   ingest.NewUniversalAdapter(),
	}
}

// NewServiceWithWeights creates a service with custom scoring weights
func NewServiceWithWeights(weights scoring.WeightMatrix) *Service {
	return &Service{
		extractor: features.NewExtractor(),
		scorer:    scoring.NewScoringEngineWithWeights(weights),
		adapter:   ingest.NewUniversalAdapter(),
	}
}

// Classify implements the gRPC Classify method
func (s *Service) Classify(ctx context.Context, req *pb.ClassifyRequest) (*pb.ClassifyResponse, error) {
	if req.Metadata == nil {
		return nil, fmt.Errorf("metadata is required")
	}

	// Extract features
	fv := s.extractor.Extract(req.Metadata)

	// Score categories
	scores := s.scorer.Score(fv)

	// Apply filters
	topN := int(req.TopN)
	if topN <= 0 {
		topN = 3 // default
	}

	threshold := req.Threshold
	if threshold <= 0 {
		threshold = 0.1 // default
	}

	// Filter and limit results
	var filteredScores []*pb.CategoryScore
	for i, score := range scores {
		if i >= topN {
			break
		}
		if score.Score >= threshold {
			filteredScores = append(filteredScores, &pb.CategoryScore{
				Category: string(score.Category),
				Score:    score.Score,
				Reason:   score.Reason,
			})
		}
	}

	response := &pb.ClassifyResponse{
		Scores: filteredScores,
	}

	// Set primary category and confidence
	if len(filteredScores) > 0 {
		response.PrimaryCategory = filteredScores[0].Category
		response.Confidence = filteredScores[0].Score

		// Adjust confidence based on score gap
		if len(filteredScores) > 1 {
			gap := filteredScores[0].Score - filteredScores[1].Score
			// Clamp confidence to [0, 1] to prevent values exceeding 1.0
			response.Confidence = math.Max(0, math.Min(1, filteredScores[0].Score+gap*0.3))
		}
	}

	return response, nil
}

// ClassifyFromJSON classifies tables from JSON schema data
func (s *Service) ClassifyFromJSON(data []byte, engine string) ([]*pb.ClassifyResponse, error) {
	// Convert JSON to table metadata
	tables, err := s.adapter.ConvertToTableMetadata(data, engine)
	if err != nil {
		return nil, fmt.Errorf("failed to convert metadata: %w", err)
	}

	var responses []*pb.ClassifyResponse

	for _, table := range tables {
		req := &pb.ClassifyRequest{
			Metadata:  table,
			TopN:      3,
			Threshold: 0.1,
		}

		resp, err := s.Classify(context.Background(), req)
		if err != nil {
			return nil, fmt.Errorf("failed to classify table %s: %w", table.Name, err)
		}

		responses = append(responses, resp)
	}

	return responses, nil
}

// ClassifyUnifiedModel implements the gRPC ClassifyUnifiedModel method
func (s *Service) ClassifyUnifiedModel(ctx context.Context, req *pb.ClassifyUnifiedModelRequest) (*pb.ClassifyUnifiedModelResponse, error) {
	if req.UnifiedModel == nil {
		return nil, fmt.Errorf("unified_model is required")
	}

	// Convert protobuf UnifiedModel to Go UnifiedModel
	model := s.convertProtoToUnifiedModel(req.UnifiedModel)

	// Create enrichment with metadata
	enrichment := &unifiedmodel.UnifiedModelEnrichment{
		SchemaID:          generateSchemaID(&model),
		EnrichmentVersion: "1.0.0",
		GeneratedAt:       time.Now(),
		GeneratedBy:       "classifier-service",
		TableEnrichments:  make(map[string]unifiedmodel.TableEnrichment),
		ColumnEnrichments: make(map[string]unifiedmodel.ColumnEnrichment),
		IndexEnrichments:  make(map[string]unifiedmodel.IndexEnrichment),
		ViewEnrichments:   make(map[string]unifiedmodel.ViewEnrichment),
		ComplianceSummary: unifiedmodel.ComplianceSummary{},
		RiskAssessment:    unifiedmodel.RiskAssessment{},
		Recommendations:   make([]unifiedmodel.Recommendation, 0),
		PerformanceHints:  make([]unifiedmodel.PerformanceHint, 0),
	}

	// Classify each table in the unified model
	for tableName, table := range model.Tables {
		// Convert table to protobuf metadata format for classification
		metadata := s.convertTableToProtoMetadata(table, tableName)

		// Extract features and classify
		fv := s.extractor.Extract(metadata)
		scores := s.scorer.Score(fv)

		// Convert scores to enrichment format
		categoryScores := make([]unifiedmodel.CategoryScore, 0, len(scores))
		for i, score := range scores {
			if i >= 10 { // Limit to top 10 scores
				break
			}
			if score.Score >= 0.05 { // Minimum threshold
				categoryScores = append(categoryScores, unifiedmodel.CategoryScore{
					Category: string(score.Category),
					Score:    score.Score,
					Reason:   score.Reason,
				})
			}
		}

		// Create table enrichment
		tableEnrichment := unifiedmodel.TableEnrichment{
			PrimaryCategory:          unifiedmodel.TableCategory(string(scores[0].Category)),
			ClassificationConfidence: scores[0].Score,
			ClassificationScores:     categoryScores,
			AccessPattern:            s.inferAccessPattern(table),
			HasPrivilegedData:        false, // Would be set by privileged data detection
			DataSensitivity:          0.0,   // Would be calculated based on data analysis
			Tags:                     []string{},
			Context:                  make(map[string]string),
		}

		enrichment.TableEnrichments[tableName] = tableEnrichment

		// Create column enrichments for each column
		for columnName, column := range table.Columns {
			columnKey := fmt.Sprintf("%s.%s", tableName, columnName)
			qualityScore := 1.0
			columnEnrichment := unifiedmodel.ColumnEnrichment{
				IsPrivilegedData:     false, // Would be set by detection
				DataCategory:         unifiedmodel.DataCategoryBusiness,
				PrivilegedConfidence: 0.0,
				RiskLevel:            unifiedmodel.RiskLevelMinimal,
				ComplianceImpact:     []unifiedmodel.ComplianceFramework{},
				DataQualityScore:     &qualityScore,
				RecommendedIndexType: s.getRecommendedIndexType(column),
				ShouldEncrypt:        false,
				ShouldMask:           false,
				IsForeignKey:         false, // Would be determined by schema analysis
				Tags:                 []string{},
				Context:              make(map[string]string),
			}
			enrichment.ColumnEnrichments[columnKey] = columnEnrichment
		}

		// Create index enrichments
		for indexName := range table.Indexes {
			indexEnrichment := unifiedmodel.IndexEnrichment{
				IsRedundant:       false,
				ShouldDrop:        false,
				OptimizationHints: []string{},
				Context:           make(map[string]string),
			}
			enrichment.IndexEnrichments[indexName] = indexEnrichment
		}
	}

	// Convert Go enrichment to protobuf
	protoEnrichment := s.convertEnrichmentToProto(enrichment)

	return &pb.ClassifyUnifiedModelResponse{
		UnifiedModelEnrichment: protoEnrichment,
	}, nil
}

// Translate implements the gRPC Translate method
func (s *Service) Translate(ctx context.Context, req *pb.TranslationRequest) (*pb.TranslationResponse, error) {
	return nil, fmt.Errorf("translate method not implemented")
}

// Generate implements the gRPC Generate method
func (s *Service) Generate(ctx context.Context, req *pb.GenerationRequest) (*pb.GenerationResponse, error) {
	return nil, fmt.Errorf("generate method not implemented")
}

// CompareSchemas implements the gRPC CompareSchemas method
func (s *Service) CompareSchemas(ctx context.Context, req *pb.CompareRequest) (*pb.CompareResponse, error) {
	return nil, fmt.Errorf("compare schemas method not implemented")
}

// MatchUnifiedModelsEnriched implements the gRPC MatchUnifiedModelsEnriched method
func (s *Service) MatchUnifiedModelsEnriched(ctx context.Context, req *pb.MatchUnifiedModelsEnrichedRequest) (*pb.MatchUnifiedModelsEnrichedResponse, error) {
	return nil, fmt.Errorf("match unified models enriched method not implemented")
}

// DetectPrivilegedData implements the gRPC DetectPrivilegedData method
func (s *Service) DetectPrivilegedData(ctx context.Context, req *pb.DetectRequest) (*pb.DetectResponse, error) {
	return nil, fmt.Errorf("detect privileged data method not implemented")
}

// Helper methods for ClassifyUnifiedModel

// generateSchemaID generates a unique identifier for the schema
func generateSchemaID(model *unifiedmodel.UnifiedModel) string {
	// Simple hash based on table names and count
	tableNames := make([]string, 0, len(model.Tables))
	for name := range model.Tables {
		tableNames = append(tableNames, name)
	}
	return fmt.Sprintf("schema_%d_tables_%d", len(tableNames), time.Now().Unix())
}

// convertTableToProtoMetadata converts a shared UnifiedModel table to protobuf metadata
func (s *Service) convertTableToProtoMetadata(table unifiedmodel.Table, tableName string) *pb.TableMetadata {
	columns := make([]*pb.ColumnMetadata, 0, len(table.Columns))
	for _, col := range table.Columns {
		column := &pb.ColumnMetadata{
			Name:            col.Name,
			Type:            col.DataType,
			IsNullable:      col.Nullable,
			IsPrimaryKey:    col.IsPrimaryKey,
			IsAutoIncrement: col.AutoIncrement,
		}

		// Add default value if present
		if col.Default != "" {
			column.ColumnDefault = col.Default
		}

		columns = append(columns, column)
	}

	return &pb.TableMetadata{
		Name:       tableName,
		Columns:    columns,
		Properties: make(map[string]string), // Could be populated from table options
	}
}

// inferAccessPattern infers the access pattern based on table characteristics
func (s *Service) inferAccessPattern(table unifiedmodel.Table) unifiedmodel.AccessPattern {
	// Simple heuristics - could be enhanced with more sophisticated analysis
	columnCount := len(table.Columns)
	indexCount := len(table.Indexes)

	// Tables with many indexes might be read-heavy
	if indexCount > columnCount/2 {
		return unifiedmodel.AccessPatternReadHeavy
	}

	// Tables with timestamps might be append-only
	for _, col := range table.Columns {
		if col.DataType == "timestamp" || col.DataType == "datetime" {
			return unifiedmodel.AccessPatternAppendOnly
		}
	}

	// Default to read-write
	return unifiedmodel.AccessPatternReadWrite
}

// getRecommendedIndexType provides index type recommendations for a column
func (s *Service) getRecommendedIndexType(column unifiedmodel.Column) unifiedmodel.IndexType {
	// Simple recommendations based on column characteristics
	if column.IsPrimaryKey {
		return unifiedmodel.IndexTypeBTree
	}

	// Recommend indexes for common query columns
	if column.DataType == "varchar" || column.DataType == "text" {
		return unifiedmodel.IndexTypeBTree
	}

	// JSON columns might benefit from GIN indexes
	if column.DataType == "json" || column.DataType == "jsonb" {
		return unifiedmodel.IndexTypeGIN
	}

	// Default recommendation
	return unifiedmodel.IndexTypeBTree
}

// Conversion functions for protobuf <-> Go struct conversion

// convertProtoToUnifiedModel converts a protobuf UnifiedModel to Go UnifiedModel
func (s *Service) convertProtoToUnifiedModel(protoModel *pb.UnifiedModel) unifiedmodel.UnifiedModel {
	if protoModel == nil {
		return unifiedmodel.UnifiedModel{}
	}

	model := unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.DatabaseType(protoModel.DatabaseType),
		Tables:       make(map[string]unifiedmodel.Table),
		Collections:  make(map[string]unifiedmodel.Collection),
		Views:        make(map[string]unifiedmodel.View),
		Indexes:      make(map[string]unifiedmodel.Index),
	}

	// Convert tables
	for name, table := range protoModel.Tables {
		model.Tables[name] = s.convertProtoToTable(table)
	}

	// Convert collections
	for name, collection := range protoModel.Collections {
		model.Collections[name] = s.convertProtoToCollection(collection)
	}

	// Convert views
	for name, view := range protoModel.Views {
		model.Views[name] = s.convertProtoToView(view)
	}

	// Convert indexes
	for name, index := range protoModel.Indexes {
		model.Indexes[name] = s.convertProtoToIndex(index)
	}

	return model
}

// convertProtoToTable converts a protobuf Table to Go Table
func (s *Service) convertProtoToTable(protoTable *pb.Table) unifiedmodel.Table {
	table := unifiedmodel.Table{
		Name:    protoTable.Name,
		Owner:   protoTable.Owner,
		Comment: protoTable.Comment,
		Columns: make(map[string]unifiedmodel.Column),
		Indexes: make(map[string]unifiedmodel.Index),
	}

	for name, column := range protoTable.Columns {
		table.Columns[name] = s.convertProtoToColumn(column)
	}

	for name, index := range protoTable.Indexes {
		table.Indexes[name] = s.convertProtoToIndex(index)
	}

	return table
}

// convertProtoToColumn converts a protobuf Column to Go Column
func (s *Service) convertProtoToColumn(protoColumn *pb.Column) unifiedmodel.Column {
	return unifiedmodel.Column{
		Name:                protoColumn.Name,
		DataType:            protoColumn.DataType,
		Nullable:            protoColumn.Nullable,
		Default:             protoColumn.DefaultValue,
		GeneratedExpression: protoColumn.GeneratedExpression,
		IsPrimaryKey:        protoColumn.IsPrimaryKey,
		IsPartitionKey:      protoColumn.IsPartitionKey,
		IsClusteringKey:     protoColumn.IsClusteringKey,
		AutoIncrement:       protoColumn.AutoIncrement,
		Collation:           protoColumn.Collation,
	}
}

// convertProtoToCollection converts a protobuf Collection to Go Collection
func (s *Service) convertProtoToCollection(protoCollection *pb.Collection) unifiedmodel.Collection {
	collection := unifiedmodel.Collection{
		Name:     protoCollection.Name,
		Owner:    protoCollection.Owner,
		Comment:  protoCollection.Comment,
		Fields:   make(map[string]unifiedmodel.Field),
		Indexes:  make(map[string]unifiedmodel.Index),
		ShardKey: protoCollection.ShardKey,
	}

	for name, field := range protoCollection.Fields {
		collection.Fields[name] = s.convertProtoToField(field)
	}

	for name, index := range protoCollection.Indexes {
		collection.Indexes[name] = s.convertProtoToIndex(index)
	}

	return collection
}

// convertProtoToField converts a protobuf Field to Go Field
func (s *Service) convertProtoToField(protoField *pb.Field) unifiedmodel.Field {
	return unifiedmodel.Field{
		Name:     protoField.Name,
		Type:     protoField.Type,
		Required: protoField.Required,
	}
}

// convertProtoToView converts a protobuf View to Go View
func (s *Service) convertProtoToView(protoView *pb.View) unifiedmodel.View {
	view := unifiedmodel.View{
		Name:       protoView.Name,
		Definition: protoView.Definition,
		Comment:    protoView.Comment,
		Columns:    make(map[string]unifiedmodel.Column),
	}

	for name, column := range protoView.Columns {
		view.Columns[name] = s.convertProtoToColumn(column)
	}

	return view
}

// convertProtoToIndex converts a protobuf Index to Go Index
func (s *Service) convertProtoToIndex(protoIndex *pb.Index) unifiedmodel.Index {
	return unifiedmodel.Index{
		Name:       protoIndex.Name,
		Type:       unifiedmodel.IndexType(protoIndex.Type),
		Columns:    protoIndex.Columns,
		Fields:     protoIndex.Fields,
		Expression: protoIndex.Expression,
		Predicate:  protoIndex.Predicate,
		Unique:     protoIndex.Unique,
	}
}

// convertEnrichmentToProto converts Go UnifiedModelEnrichment to protobuf
func (s *Service) convertEnrichmentToProto(enrichment *unifiedmodel.UnifiedModelEnrichment) *pb.UnifiedModelEnrichment {
	if enrichment == nil {
		return nil
	}

	protoEnrichment := &pb.UnifiedModelEnrichment{
		SchemaId:          enrichment.SchemaID,
		EnrichmentVersion: enrichment.EnrichmentVersion,
		GeneratedAt:       enrichment.GeneratedAt.Unix(),
		GeneratedBy:       enrichment.GeneratedBy,
		TableEnrichments:  make(map[string]*pb.TableEnrichment),
		ColumnEnrichments: make(map[string]*pb.ColumnEnrichment),
		IndexEnrichments:  make(map[string]*pb.IndexEnrichment),
		ViewEnrichments:   make(map[string]*pb.ViewEnrichment),
	}

	// Convert table enrichments
	for name, tableEnrich := range enrichment.TableEnrichments {
		protoEnrichment.TableEnrichments[name] = s.convertTableEnrichmentToProto(tableEnrich)
	}

	// Convert column enrichments
	for name, columnEnrich := range enrichment.ColumnEnrichments {
		protoEnrichment.ColumnEnrichments[name] = s.convertColumnEnrichmentToProto(columnEnrich)
	}

	// Convert index enrichments
	for name, indexEnrich := range enrichment.IndexEnrichments {
		protoEnrichment.IndexEnrichments[name] = s.convertIndexEnrichmentToProto(indexEnrich)
	}

	// Convert view enrichments
	for name, viewEnrich := range enrichment.ViewEnrichments {
		protoEnrichment.ViewEnrichments[name] = s.convertViewEnrichmentToProto(viewEnrich)
	}

	return protoEnrichment
}

// convertTableEnrichmentToProto converts Go TableEnrichment to protobuf
func (s *Service) convertTableEnrichmentToProto(enrichment unifiedmodel.TableEnrichment) *pb.TableEnrichment {
	protoEnrichment := &pb.TableEnrichment{
		PrimaryCategory:          string(enrichment.PrimaryCategory),
		ClassificationConfidence: enrichment.ClassificationConfidence,
		AccessPattern:            string(enrichment.AccessPattern),
		HasPrivilegedData:        enrichment.HasPrivilegedData,
		DataSensitivity:          enrichment.DataSensitivity,
		Tags:                     enrichment.Tags,
		Context:                  enrichment.Context,
	}

	// Convert classification scores
	for _, score := range enrichment.ClassificationScores {
		protoEnrichment.ClassificationScores = append(protoEnrichment.ClassificationScores, &pb.CategoryScore{
			Category: score.Category,
			Score:    score.Score,
			Reason:   score.Reason,
		})
	}

	return protoEnrichment
}

// convertColumnEnrichmentToProto converts Go ColumnEnrichment to protobuf
func (s *Service) convertColumnEnrichmentToProto(enrichment unifiedmodel.ColumnEnrichment) *pb.ColumnEnrichment {
	protoEnrichment := &pb.ColumnEnrichment{
		IsPrivilegedData:     enrichment.IsPrivilegedData,
		DataCategory:         string(enrichment.DataCategory),
		PrivilegedConfidence: enrichment.PrivilegedConfidence,
		RiskLevel:            string(enrichment.RiskLevel),
		RecommendedIndexType: string(enrichment.RecommendedIndexType),
		ShouldEncrypt:        enrichment.ShouldEncrypt,
		ShouldMask:           enrichment.ShouldMask,
		IsForeignKey:         enrichment.IsForeignKey,
		Tags:                 enrichment.Tags,
		Context:              enrichment.Context,
	}

	if enrichment.DataQualityScore != nil {
		protoEnrichment.DataQualityScore = *enrichment.DataQualityScore
	}

	// Convert compliance impact
	for _, framework := range enrichment.ComplianceImpact {
		protoEnrichment.ComplianceImpact = append(protoEnrichment.ComplianceImpact, string(framework))
	}

	return protoEnrichment
}

// convertIndexEnrichmentToProto converts Go IndexEnrichment to protobuf
func (s *Service) convertIndexEnrichmentToProto(enrichment unifiedmodel.IndexEnrichment) *pb.IndexEnrichment {
	return &pb.IndexEnrichment{
		IsRedundant:       enrichment.IsRedundant,
		ShouldDrop:        enrichment.ShouldDrop,
		OptimizationHints: enrichment.OptimizationHints,
		Context:           enrichment.Context,
	}
}

// convertViewEnrichmentToProto converts Go ViewEnrichment to protobuf
func (s *Service) convertViewEnrichmentToProto(enrichment unifiedmodel.ViewEnrichment) *pb.ViewEnrichment {
	protoEnrichment := &pb.ViewEnrichment{
		ComplexityScore:   enrichment.ComplexityScore,
		QueryDepth:        int32(enrichment.QueryDepth),
		TableDependencies: enrichment.TableDependencies,
		ViewDependencies:  enrichment.ViewDependencies,
		IsOptimizable:     enrichment.IsOptimizable,
		IsMaterializable:  enrichment.IsMaterializable,
		BusinessPurpose:   enrichment.BusinessPurpose,
		Context:           enrichment.Context,
	}

	// Handle pointer fields
	if enrichment.EstimatedRowsReturned != nil {
		protoEnrichment.EstimatedRowsReturned = *enrichment.EstimatedRowsReturned
	}
	if enrichment.ExecutionCost != nil {
		protoEnrichment.ExecutionCost = *enrichment.ExecutionCost
	}
	if enrichment.AccessFrequency != nil {
		protoEnrichment.AccessFrequency = *enrichment.AccessFrequency
	}

	return protoEnrichment
}
