package engine

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	pb "github.com/redbco/redb-open/api/proto/integration/v1"
	pkggrpc "github.com/redbco/redb-open/pkg/grpc"
	"github.com/redbco/redb-open/services/integration/internal/pipeline"
	"github.com/redbco/redb-open/services/integration/internal/pipeline/steps"
	"github.com/redbco/redb-open/services/integration/internal/rag"
	lightrag "github.com/redbco/redb-open/services/integration/internal/rag/lightrag"
	"google.golang.org/protobuf/types/known/structpb"
)

type IntegrationServer struct {
	pb.UnimplementedIntegrationServiceServer
	engine       *Engine
	orchestrator *pipeline.RAGPipelineOrchestrator
}

func NewIntegrationServer(engine *Engine) *IntegrationServer {
	return &IntegrationServer{engine: engine}
}

// Management
func (s *IntegrationServer) CreateIntegration(ctx context.Context, req *pb.CreateIntegrationRequest) (*pb.CreateIntegrationResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	atomic.AddInt64(&s.engine.metrics.requestsProcessed, 1)

	if req.Integration == nil || req.Integration.Name == "" {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.CreateIntegrationResponse{
			Status:        commonv1.Status_STATUS_FAILURE,
			StatusMessage: "integration.name is required",
		}, nil
	}

	// If no id is provided, generate one to satisfy DB schema
	if req.Integration.Id == "" {
		req.Integration.Id = fmt.Sprintf("integration_%d", time.Now().UnixNano())
	}
	// Persist to DB (best-effort) and cache in memory
	if _, err := s.engine.insertIntegration(ctx, req.Integration); err != nil {
		// log via metric; still proceed to in-memory store for now
		atomic.AddInt64(&s.engine.metrics.errors, 1)
	}
	integ, err := s.engine.store.Create(req.Integration)
	if err != nil {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.CreateIntegrationResponse{
			Status:        commonv1.Status_STATUS_ERROR,
			StatusMessage: err.Error(),
		}, nil
	}
	return &pb.CreateIntegrationResponse{Integration: integ, Status: commonv1.Status_STATUS_SUCCESS, StatusMessage: "created"}, nil
}

func (s *IntegrationServer) GetIntegration(ctx context.Context, req *pb.GetIntegrationRequest) (*pb.GetIntegrationResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	atomic.AddInt64(&s.engine.metrics.requestsProcessed, 1)
	integ, err := s.engine.selectIntegration(ctx, req.Id)
	if err != nil {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.GetIntegrationResponse{Status: commonv1.Status_STATUS_ERROR, StatusMessage: err.Error()}, nil
	}
	return &pb.GetIntegrationResponse{Integration: integ, Status: commonv1.Status_STATUS_SUCCESS, StatusMessage: "ok"}, nil
}

func (s *IntegrationServer) UpdateIntegration(ctx context.Context, req *pb.UpdateIntegrationRequest) (*pb.UpdateIntegrationResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	atomic.AddInt64(&s.engine.metrics.requestsProcessed, 1)
	integ, err := s.engine.store.Update(req.Integration)
	if err != nil {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.UpdateIntegrationResponse{Status: commonv1.Status_STATUS_ERROR, StatusMessage: err.Error()}, nil
	}
	return &pb.UpdateIntegrationResponse{Integration: integ, Status: commonv1.Status_STATUS_SUCCESS, StatusMessage: "updated"}, nil
}

func (s *IntegrationServer) DeleteIntegration(ctx context.Context, req *pb.DeleteIntegrationRequest) (*pb.DeleteIntegrationResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	atomic.AddInt64(&s.engine.metrics.requestsProcessed, 1)
	if err := s.engine.store.Delete(req.Id); err != nil {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.DeleteIntegrationResponse{Status: commonv1.Status_STATUS_ERROR, StatusMessage: err.Error()}, nil
	}
	return &pb.DeleteIntegrationResponse{Status: commonv1.Status_STATUS_SUCCESS, StatusMessage: "deleted"}, nil
}

func (s *IntegrationServer) ListIntegrations(ctx context.Context, req *pb.ListIntegrationsRequest) (*pb.ListIntegrationsResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	atomic.AddInt64(&s.engine.metrics.requestsProcessed, 1)
	list := s.engine.store.List(req.Type)
	return &pb.ListIntegrationsResponse{Integrations: list, Status: commonv1.Status_STATUS_SUCCESS, StatusMessage: "ok"}, nil
}

// Execution
func (s *IntegrationServer) ExecuteIntegration(ctx context.Context, req *pb.ExecuteIntegrationRequest) (*pb.ExecuteIntegrationResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	atomic.AddInt64(&s.engine.metrics.requestsProcessed, 1)

	// per-request timeout configuration
	timeoutStr := s.engine.config.Get("services.integration.timeout")
	timeout := 30 * time.Second
	if timeoutStr != "" {
		if parsed, err := time.ParseDuration(timeoutStr + "s"); err == nil {
			timeout = parsed
		}
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Validate
	if req.Id == "" || req.Operation == "" {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.ExecuteIntegrationResponse{Status: commonv1.Status_STATUS_FAILURE, StatusMessage: "id and operation are required"}, nil
	}

	// Route RAG pipeline operation
	if req.Operation == "RAG_PIPELINE" || req.Operation == "EXECUTE_RAG_PIPELINE" {
		if _, err := s.engine.store.Get(req.Id); err != nil {
			atomic.AddInt64(&s.engine.metrics.errors, 1)
			return &pb.ExecuteIntegrationResponse{Status: commonv1.Status_STATUS_ERROR, StatusMessage: fmt.Sprintf("integration not found: %v", err)}, nil
		}
		if err := s.ensureOrchestrator(ctx); err != nil {
			atomic.AddInt64(&s.engine.metrics.errors, 1)
			return &pb.ExecuteIntegrationResponse{Status: commonv1.Status_STATUS_ERROR, StatusMessage: err.Error()}, nil
		}
		execReq, err := buildRAGPipelineRequest(req)
		if err != nil {
			atomic.AddInt64(&s.engine.metrics.errors, 1)
			return &pb.ExecuteIntegrationResponse{Status: commonv1.Status_STATUS_FAILURE, StatusMessage: err.Error()}, nil
		}
		execReq.IntegrationId = req.Id
		switch req.GetMode() {
		case pb.ExecutionMode_EXECUTION_MODE_ASYNC:
			jobID, err := s.orchestrator.ExecutePipelineAsync(ctx, execReq)
			if err != nil {
				atomic.AddInt64(&s.engine.metrics.errors, 1)
				return &pb.ExecuteIntegrationResponse{Status: commonv1.Status_STATUS_ERROR, StatusMessage: err.Error()}, nil
			}
			return &pb.ExecuteIntegrationResponse{JobId: jobID, Status: commonv1.Status_STATUS_SUCCESS, StatusMessage: "queued"}, nil
		default:
			res, err := s.orchestrator.ExecutePipeline(ctx, execReq)
			if err != nil {
				atomic.AddInt64(&s.engine.metrics.errors, 1)
				return &pb.ExecuteIntegrationResponse{Status: commonv1.Status_STATUS_ERROR, StatusMessage: err.Error()}, nil
			}
			payload, _ := structpb.NewStruct(map[string]any{
				"processed_documents": res.GetProcessedDocuments(),
				"stored_embeddings":   res.GetStoredEmbeddings(),
				"failed_documents":    res.GetFailedDocuments(),
			})
			return &pb.ExecuteIntegrationResponse{Payload: payload, Status: commonv1.Status_STATUS_SUCCESS, StatusMessage: "ok"}, nil
		}
	}

	// Default behavior: echo payload and report success.
	if _, err := s.engine.store.Get(req.Id); err != nil {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.ExecuteIntegrationResponse{Status: commonv1.Status_STATUS_ERROR, StatusMessage: fmt.Sprintf("integration not found: %v", err)}, nil
	}

	return &pb.ExecuteIntegrationResponse{Payload: req.Payload, Status: commonv1.Status_STATUS_SUCCESS, StatusMessage: "executed"}, nil
}

// ensureOrchestrator initializes the pipeline orchestrator lazily
func (s *IntegrationServer) ensureOrchestrator(ctx context.Context) error {
	if s.orchestrator != nil {
		return nil
	}
	anchorAddr := s.engine.config.Get("services.anchor.grpc_address")
	if anchorAddr == "" {
		anchorAddr = "localhost:50055"
	}
	conn, err := pkggrpc.NewClient(ctx, anchorAddr, pkggrpc.DefaultClientOptions())
	if err != nil {
		return err
	}
	anchorClient := anchorv1.NewAnchorServiceClient(conn)
	ragManager := &simpleRAGManager{engine: s.engine}
	s.orchestrator = pipeline.NewOrchestrator(anchorClient, ragManager, nil, &pipeline.PipelineConfig{DefaultBatch: 100, WorkerCount: 4})
	return nil
}

// simpleRAGManager selects a provider based on integration config and adapts to steps.RAGProvider
type simpleRAGManager struct{ engine *Engine }

func (m *simpleRAGManager) GetProvider(ctx context.Context, integrationID string) (steps.RAGProvider, error) {
	integ, err := m.engine.store.Get(integrationID)
	if err != nil {
		return nil, err
	}
	cfg := map[string]any{}
	if integ.Config != nil {
		cfg = integ.Config.AsMap()
	}
	baseURL, _ := cfg["base_url"].(string)
	apiKey, _ := cfg["api_key"].(string)
	p := lightrag.New(lightrag.LightRAGConfig{BaseURL: baseURL, APIKey: apiKey})
	return &ragProviderAdapter{p: p}, nil
}

// ragProviderAdapter adapts rag.RAGProvider to steps.RAGProvider
type ragProviderAdapter struct{ p rag.RAGProvider }

func (a *ragProviderAdapter) Ingest(ctx context.Context, documents []*pb.Document) ([]*pb.IngestResult, error) {
	// Convert pb aliases are same as integrationv1; rag provider expects integrationv1 types which match pb
	return a.p.Ingest(ctx, documents)
}

func buildRAGPipelineRequest(req *pb.ExecuteIntegrationRequest) (*pb.ExecuteRAGPipelineRequest, error) {
	if req.Payload == nil {
		return nil, fmt.Errorf("payload is required")
	}
	m := req.Payload.AsMap()
	srcCfg := &pb.SourceConfiguration{}
	if v, ok := m["source"].(map[string]any); ok {
		if s, ok := v["database_id"].(string); ok {
			srcCfg.DatabaseId = s
		}
		if s, ok := v["database_type"].(string); ok {
			srcCfg.DatabaseType = s
		}
		if s, ok := v["query"].(string); ok {
			srcCfg.Query = s
		}
		if bs, ok := v["batch_size"].(float64); ok {
			srcCfg.BatchSize = int32(bs)
		}
	}
	tgtCfg := &pb.TargetConfiguration{}
	if v, ok := m["target"].(map[string]any); ok {
		if s, ok := v["database_id"].(string); ok {
			tgtCfg.DatabaseId = s
		}
		if s, ok := v["database_type"].(string); ok {
			tgtCfg.DatabaseType = s
		}
		if s, ok := v["collection_name"].(string); ok {
			tgtCfg.CollectionName = s
		}
	}
	return &pb.ExecuteRAGPipelineRequest{Source: srcCfg, Target: tgtCfg}, nil
}
