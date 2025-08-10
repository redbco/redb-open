package engine

import (
	"context"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	integrationv1 "github.com/redbco/redb-open/api/proto/integration/v1"
	pkggrpc "github.com/redbco/redb-open/pkg/grpc"
	"github.com/redbco/redb-open/services/integration/internal/pipeline"
)

// RAGServer implements selected RAG endpoints as part of IntegrationService.
// Note: Integration proto already includes pipeline RPCs; use IntegrationService for registration.
type RAGServer struct {
	engine       *Engine
	orchestrator *pipeline.RAGPipelineOrchestrator
}

func NewRAGServer(engine *Engine) *RAGServer { return &RAGServer{engine: engine} }

func (s *RAGServer) initOrchestrator(ctx context.Context) error {
	if s.orchestrator != nil {
		return nil
	}
	// connect to anchor
	addr := s.engine.config.Get("services.anchor.grpc_address")
	if addr == "" {
		addr = "localhost:50055"
	}
	conn, err := pkggrpc.NewClient(ctx, addr, pkggrpc.DefaultClientOptions())
	if err != nil {
		return err
	}
	anchorClient := anchorv1.NewAnchorServiceClient(conn)
	// RAG manager and jobs are placeholders; wire real ones later
	var ragManager pipeline.RAGManagerInterface
	var jobTracker pipeline.JobTrackerInterface
	s.orchestrator = pipeline.NewOrchestrator(anchorClient, ragManager, jobTracker, &pipeline.PipelineConfig{DefaultBatch: 100, WorkerCount: 4})
	return nil
}

func (s *RAGServer) ExecuteRAGPipeline(ctx context.Context, req *integrationv1.ExecuteRAGPipelineRequest) (*integrationv1.ExecuteRAGPipelineResponse, error) {
	if err := s.initOrchestrator(ctx); err != nil {
		return &integrationv1.ExecuteRAGPipelineResponse{Status: commonv1.Status_STATUS_ERROR, StatusMessage: err.Error()}, nil
	}
	mode := req.GetMode()
	switch mode {
	case integrationv1.ExecutionMode_EXECUTION_MODE_ASYNC:
		jobID, err := s.orchestrator.ExecutePipelineAsync(ctx, req)
		if err != nil {
			return &integrationv1.ExecuteRAGPipelineResponse{Status: commonv1.Status_STATUS_ERROR, StatusMessage: err.Error()}, nil
		}
		return &integrationv1.ExecuteRAGPipelineResponse{JobId: jobID, Status: commonv1.Status_STATUS_SUCCESS, StatusMessage: "queued"}, nil
	default:
		res, err := s.orchestrator.ExecutePipeline(ctx, req)
		if err != nil {
			return &integrationv1.ExecuteRAGPipelineResponse{Status: commonv1.Status_STATUS_ERROR, StatusMessage: err.Error()}, nil
		}
		return &integrationv1.ExecuteRAGPipelineResponse{Result: res, Status: commonv1.Status_STATUS_SUCCESS, StatusMessage: "ok"}, nil
	}
}

// Additional RAG-specific RPCs not yet wired; left for future phases.
