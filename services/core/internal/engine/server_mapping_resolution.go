package engine

import (
	"context"
	"fmt"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/services/core/internal/services/mapping"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ResolveTemplateURIsInWorkspace resolves template URIs for all mappings in a workspace
func (s *Server) ResolveTemplateURIsInWorkspace(
	ctx context.Context,
	req *corev1.ResolveTemplateURIsRequest,
) (*corev1.ResolveTemplateURIsResponse, error) {
	// Validate request
	if req.WorkspaceId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "workspace_id is required")
	}

	// Create template resolution service
	resolutionSvc := mapping.NewTemplateResolutionService(s.engine.db)

	// Resolve all mappings in the workspace
	mappingsResolved, err := resolutionSvc.ResolveAllMappingsInWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &corev1.ResolveTemplateURIsResponse{
			MappingsResolved: 0,
			RulesResolved:    0,
			Errors:           []string{fmt.Sprintf("failed to resolve templates: %v", err)},
			Status:           commonv1.Status_STATUS_ERROR,
		}, nil // Return error in response, not gRPC error
	}

	return &corev1.ResolveTemplateURIsResponse{
		MappingsResolved: int32(mappingsResolved),
		RulesResolved:    int32(mappingsResolved * 2), // Approximate: assumes ~2 rules per mapping
		Errors:           []string{},
		Status:           commonv1.Status_STATUS_SUCCESS,
	}, nil
}
