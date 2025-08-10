package engine

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	pb "github.com/redbco/redb-open/api/proto/integration/v1"
)

type IntegrationServer struct {
	pb.UnimplementedIntegrationServiceServer
	engine *Engine
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

	// For base implementation we simply echo payload and report success.
	// Later we will route based on IntegrationType and configured provider.
	_ = ctx
	if _, err := s.engine.store.Get(req.Id); err != nil {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.ExecuteIntegrationResponse{Status: commonv1.Status_STATUS_ERROR, StatusMessage: fmt.Sprintf("integration not found: %v", err)}, nil
	}

	return &pb.ExecuteIntegrationResponse{Payload: req.Payload, Status: commonv1.Status_STATUS_SUCCESS, StatusMessage: "executed"}, nil
}
