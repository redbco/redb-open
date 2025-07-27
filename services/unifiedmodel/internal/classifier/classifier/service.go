package classifier

import (
	"context"
	"fmt"

	unifiedmodel "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/classifier/features"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/classifier/ingest"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/classifier/scoring"
)

// Service implements the table classification service
type Service struct {
	unifiedmodel.UnimplementedUnifiedModelServiceServer
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
func (s *Service) Classify(ctx context.Context, req *unifiedmodel.ClassifyRequest) (*unifiedmodel.ClassifyResponse, error) {
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
	var filteredScores []*unifiedmodel.CategoryScore
	for i, score := range scores {
		if i >= topN {
			break
		}
		if score.Score >= threshold {
			filteredScores = append(filteredScores, &unifiedmodel.CategoryScore{
				Category: string(score.Category),
				Score:    score.Score,
				Reason:   score.Reason,
			})
		}
	}

	response := &unifiedmodel.ClassifyResponse{
		Scores: filteredScores,
	}

	// Set primary category and confidence
	if len(filteredScores) > 0 {
		response.PrimaryCategory = filteredScores[0].Category
		response.Confidence = filteredScores[0].Score

		// Adjust confidence based on score gap
		if len(filteredScores) > 1 {
			gap := filteredScores[0].Score - filteredScores[1].Score
			response.Confidence = filteredScores[0].Score + gap*0.3
		}
	}

	return response, nil
}

// ClassifyFromJSON classifies tables from JSON schema data
func (s *Service) ClassifyFromJSON(data []byte, engine string) ([]*unifiedmodel.ClassifyResponse, error) {
	// Convert JSON to table metadata
	tables, err := s.adapter.ConvertToTableMetadata(data, engine)
	if err != nil {
		return nil, fmt.Errorf("failed to convert metadata: %w", err)
	}

	var responses []*unifiedmodel.ClassifyResponse

	for _, table := range tables {
		req := &unifiedmodel.ClassifyRequest{
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

// Translate implements the gRPC Translate method
func (s *Service) Translate(ctx context.Context, req *unifiedmodel.TranslationRequest) (*unifiedmodel.TranslationResponse, error) {
	return nil, fmt.Errorf("translate method not implemented")
}

// Generate implements the gRPC Generate method
func (s *Service) Generate(ctx context.Context, req *unifiedmodel.GenerationRequest) (*unifiedmodel.GenerationResponse, error) {
	return nil, fmt.Errorf("generate method not implemented")
}

// CompareSchemas implements the gRPC CompareSchemas method
func (s *Service) CompareSchemas(ctx context.Context, req *unifiedmodel.CompareRequest) (*unifiedmodel.CompareResponse, error) {
	return nil, fmt.Errorf("compare schemas method not implemented")
}

// MatchSchemas implements the gRPC MatchSchemas method
func (s *Service) MatchSchemas(ctx context.Context, req *unifiedmodel.MatchSchemasEnrichedRequest) (*unifiedmodel.MatchSchemasEnrichedResponse, error) {
	return nil, fmt.Errorf("match schemas method not implemented")
}

// DetectPrivilegedData implements the gRPC DetectPrivilegedData method
func (s *Service) DetectPrivilegedData(ctx context.Context, req *unifiedmodel.DetectRequest) (*unifiedmodel.DetectResponse, error) {
	return nil, fmt.Errorf("detect privileged data method not implemented")
}
