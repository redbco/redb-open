package engine

import (
	"context"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/services/core/internal/services/environment"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// EnvironmentService methods
func (s *Server) ListEnvironments(ctx context.Context, req *corev1.ListEnvironmentsRequest) (*corev1.ListEnvironmentsResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get environment service
	environmentService := environment.NewService(s.engine.db, s.engine.logger)

	// List environments for the tenant and workspace
	environments, err := environmentService.List(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to list environments: %v", err)
	}

	// Convert to protobuf format with counts
	protoEnvironments := make([]*corev1.Environment, len(environments))
	for i, env := range environments {
		protoEnvironment, err := s.environmentToProtoWithCounts(ctx, env, req.TenantId, req.WorkspaceName)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to convert environment: %v", err)
		}
		protoEnvironments[i] = protoEnvironment
	}

	return &corev1.ListEnvironmentsResponse{
		Environments: protoEnvironments,
	}, nil
}

func (s *Server) ShowEnvironment(ctx context.Context, req *corev1.ShowEnvironmentRequest) (*corev1.ShowEnvironmentResponse, error) {
	defer s.trackOperation()()

	// Get environment service
	environmentService := environment.NewService(s.engine.db, s.engine.logger)

	// Get the environment
	env, err := environmentService.Get(ctx, req.TenantId, req.WorkspaceName, req.EnvironmentName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "environment not found: %v", err)
	}

	// Convert to protobuf format with counts
	protoEnvironment, err := s.environmentToProtoWithCounts(ctx, env, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert environment: %v", err)
	}

	return &corev1.ShowEnvironmentResponse{
		Environment: protoEnvironment,
	}, nil
}

func (s *Server) AddEnvironment(ctx context.Context, req *corev1.AddEnvironmentRequest) (*corev1.AddEnvironmentResponse, error) {
	defer s.trackOperation()()

	// Get environment service
	environmentService := environment.NewService(s.engine.db, s.engine.logger)

	// Check if environment already exists
	exists, err := environmentService.Exists(ctx, req.TenantId, req.WorkspaceName, req.EnvironmentName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to check environment existence: %v", err)
	}
	if exists {
		return nil, status.Errorf(codes.AlreadyExists, "environment with name %s already exists", req.EnvironmentName)
	}

	// Set defaults for optional fields
	production := false
	if req.EnvironmentIsProduction != nil {
		production = *req.EnvironmentIsProduction
	}

	criticality := int32(0)
	if req.EnvironmentCriticality != nil {
		criticality = *req.EnvironmentCriticality
	}

	priority := int32(0)
	if req.EnvironmentPriority != nil {
		priority = *req.EnvironmentPriority
	}

	description := ""
	if req.EnvironmentDescription != nil {
		description = *req.EnvironmentDescription
	}

	// Create the environment
	env, err := environmentService.Create(ctx, req.TenantId, req.WorkspaceName, req.EnvironmentName, description, production, criticality, priority, req.OwnerId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create environment: %v", err)
	}

	// Convert to protobuf format with counts
	protoEnvironment, err := s.environmentToProtoWithCounts(ctx, env, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert environment: %v", err)
	}

	return &corev1.AddEnvironmentResponse{
		Message:     "Environment created successfully",
		Success:     true,
		Environment: protoEnvironment,
		Status:      commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) ModifyEnvironment(ctx context.Context, req *corev1.ModifyEnvironmentRequest) (*corev1.ModifyEnvironmentResponse, error) {
	defer s.trackOperation()()

	// Get environment service
	environmentService := environment.NewService(s.engine.db, s.engine.logger)

	// Verify environment exists
	_, err := environmentService.Get(ctx, req.TenantId, req.WorkspaceName, req.EnvironmentName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "environment not found: %v", err)
	}

	// Build updates map
	updates := make(map[string]interface{})
	if req.EnvironmentNameNew != nil {
		updates["environment_name"] = *req.EnvironmentNameNew
	}
	if req.EnvironmentDescription != nil {
		updates["environment_description"] = *req.EnvironmentDescription
	}
	if req.EnvironmentIsProduction != nil {
		updates["environment_is_production"] = *req.EnvironmentIsProduction
	}
	if req.EnvironmentCriticality != nil {
		updates["environment_criticality"] = *req.EnvironmentCriticality
	}
	if req.EnvironmentPriority != nil {
		updates["environment_priority"] = *req.EnvironmentPriority
	}

	// Update the environment
	updatedEnv, err := environmentService.Update(ctx, req.TenantId, req.WorkspaceName, req.EnvironmentName, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update environment: %v", err)
	}

	// Convert to protobuf format with counts
	protoEnvironment, err := s.environmentToProtoWithCounts(ctx, updatedEnv, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert environment: %v", err)
	}

	return &corev1.ModifyEnvironmentResponse{
		Message:     "Environment updated successfully",
		Success:     true,
		Environment: protoEnvironment,
		Status:      commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DeleteEnvironment(ctx context.Context, req *corev1.DeleteEnvironmentRequest) (*corev1.DeleteEnvironmentResponse, error) {
	defer s.trackOperation()()

	// Get environment service
	environmentService := environment.NewService(s.engine.db, s.engine.logger)

	// Verify environment exists
	_, err := environmentService.Get(ctx, req.TenantId, req.WorkspaceName, req.EnvironmentName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "environment not found: %v", err)
	}

	// Delete the environment
	err = environmentService.Delete(ctx, req.TenantId, req.WorkspaceName, req.EnvironmentName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to delete environment: %v", err)
	}

	return &corev1.DeleteEnvironmentResponse{
		Message: "Environment deleted successfully",
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}
