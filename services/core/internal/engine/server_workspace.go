package engine

import (
	"context"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/services/core/internal/services/workspace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WorkspaceService methods
func (s *Server) ListWorkspaces(ctx context.Context, req *corev1.ListWorkspacesRequest) (*corev1.ListWorkspacesResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// List workspaces for the tenant
	workspaces, err := workspaceService.List(ctx, req.TenantId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to list workspaces: %v", err)
	}

	// Convert to protobuf format with counts
	protoWorkspaces := make([]*corev1.Workspace, len(workspaces))
	for i, ws := range workspaces {
		protoWorkspace, err := s.workspaceToProtoWithCounts(ctx, ws, req.TenantId)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to convert workspace: %v", err)
		}
		protoWorkspaces[i] = protoWorkspace
	}

	return &corev1.ListWorkspacesResponse{
		Workspaces: protoWorkspaces,
	}, nil
}

func (s *Server) ShowWorkspace(ctx context.Context, req *corev1.ShowWorkspaceRequest) (*corev1.ShowWorkspaceResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get the workspace
	ws, err := workspaceService.Get(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Verify tenant access
	if ws.TenantID != req.TenantId {
		return nil, status.Errorf(codes.PermissionDenied, "workspace not found in tenant")
	}

	// Convert to protobuf format with counts
	protoWorkspace, err := s.workspaceToProtoWithCounts(ctx, ws, req.TenantId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert workspace: %v", err)
	}

	return &corev1.ShowWorkspaceResponse{
		Workspace: protoWorkspace,
	}, nil
}

func (s *Server) AddWorkspace(ctx context.Context, req *corev1.AddWorkspaceRequest) (*corev1.AddWorkspaceResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Check if workspace already exists
	exists, err := workspaceService.Exists(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to check workspace existence: %v", err)
	}
	if exists {
		return nil, status.Errorf(codes.AlreadyExists, "workspace with name %s already exists", req.WorkspaceName)
	}

	// Create the workspace
	description := ""
	if req.WorkspaceDescription != nil {
		description = *req.WorkspaceDescription
	}

	ws, err := workspaceService.Create(ctx, req.TenantId, req.WorkspaceName, description, req.OwnerId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create workspace: %v", err)
	}

	// Convert to protobuf format with counts
	protoWorkspace, err := s.workspaceToProtoWithCounts(ctx, ws, req.TenantId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert workspace: %v", err)
	}

	return &corev1.AddWorkspaceResponse{
		Message:   "Workspace created successfully",
		Success:   true,
		Workspace: protoWorkspace,
		Status:    commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) ModifyWorkspace(ctx context.Context, req *corev1.ModifyWorkspaceRequest) (*corev1.ModifyWorkspaceResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Verify workspace exists and belongs to tenant
	ws, err := workspaceService.Get(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}
	if ws.TenantID != req.TenantId {
		return nil, status.Errorf(codes.PermissionDenied, "workspace not found in tenant")
	}

	// Build updates map
	updates := make(map[string]interface{})
	if req.WorkspaceNameNew != nil {
		updates["workspace_name"] = *req.WorkspaceNameNew
	}
	if req.WorkspaceDescription != nil {
		updates["workspace_description"] = *req.WorkspaceDescription
	}

	// Update the workspace
	updatedWs, err := workspaceService.Update(ctx, req.TenantId, req.WorkspaceName, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update workspace: %v", err)
	}

	// Convert to protobuf format with counts
	protoWorkspace, err := s.workspaceToProtoWithCounts(ctx, updatedWs, req.TenantId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert workspace: %v", err)
	}

	return &corev1.ModifyWorkspaceResponse{
		Message:   "Workspace updated successfully",
		Success:   true,
		Workspace: protoWorkspace,
		Status:    commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DeleteWorkspace(ctx context.Context, req *corev1.DeleteWorkspaceRequest) (*corev1.DeleteWorkspaceResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Verify workspace exists and belongs to tenant
	ws, err := workspaceService.Get(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}
	if ws.TenantID != req.TenantId {
		return nil, status.Errorf(codes.PermissionDenied, "workspace not found in tenant")
	}

	// Delete the workspace
	err = workspaceService.Delete(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to delete workspace: %v", err)
	}

	return &corev1.DeleteWorkspaceResponse{
		Message: "Workspace deleted successfully",
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}
