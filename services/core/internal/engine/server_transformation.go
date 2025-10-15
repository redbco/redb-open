package engine

import (
	"context"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	transformationv1 "github.com/redbco/redb-open/api/proto/transformation/v1"
	"github.com/redbco/redb-open/services/core/internal/services/transformation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ============================================================================
// TransformationService gRPC handlers
// ============================================================================

func (s *Server) ListTransformations(ctx context.Context, req *corev1.ListTransformationsRequest) (*corev1.ListTransformationsResponse, error) {
	defer s.trackOperation()()

	// Check if requesting built-in transformations
	if req.BuiltinOnly != nil && *req.BuiltinOnly {
		// Get built-in transformations from transformation service
		transformationClient, err := s.getTransformationClient()
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Unavailable, "failed to connect to transformation service: %v", err)
		}

		listReq := &transformationv1.ListTransformationsRequest{}
		listResp, err := transformationClient.ListTransformations(ctx, listReq)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to list built-in transformations: %v", err)
		}

		// Convert transformation service metadata to core transformation format
		protoTransformations := make([]*corev1.Transformation, len(listResp.Transformations))
		for i, tm := range listResp.Transformations {
			protoTransformations[i] = &corev1.Transformation{
				TransformationId:          tm.Name, // Use name as ID for built-in transformations
				TransformationName:        tm.Name,
				TransformationDescription: tm.Description,
				TransformationType:        tm.Type,
				// Built-in transformations don't have tenant/workspace/owner
				TenantId:    "",
				WorkspaceId: "",
				OwnerId:     "",
				IsBuiltin:   true,
			}
		}

		return &corev1.ListTransformationsResponse{
			Transformations: protoTransformations,
		}, nil
	}

	// List user-created transformations from database
	transformationService := transformation.NewService(s.engine.db, s.engine.logger)
	transformations, err := transformationService.List(ctx, req.TenantId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to list transformations: %v", err)
	}

	// Convert to protobuf format
	protoTransformations := make([]*corev1.Transformation, len(transformations))
	for i, t := range transformations {
		protoTransformations[i] = s.transformationToProto(t)
	}

	return &corev1.ListTransformationsResponse{
		Transformations: protoTransformations,
	}, nil
}

func (s *Server) ShowTransformation(ctx context.Context, req *corev1.ShowTransformationRequest) (*corev1.ShowTransformationResponse, error) {
	defer s.trackOperation()()

	// Get transformation service
	transformationService := transformation.NewService(s.engine.db, s.engine.logger)

	// Get the transformation
	t, err := transformationService.Get(ctx, req.TenantId, req.TransformationId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "transformation not found: %v", err)
	}

	// Convert to protobuf format
	protoTransformation := s.transformationToProto(t)

	return &corev1.ShowTransformationResponse{
		Transformation: protoTransformation,
	}, nil
}

func (s *Server) AddTransformation(ctx context.Context, req *corev1.AddTransformationRequest) (*corev1.AddTransformationResponse, error) {
	defer s.trackOperation()()

	// Get transformation service
	transformationService := transformation.NewService(s.engine.db, s.engine.logger)

	// Create the transformation
	createdTransformation, err := transformationService.Create(ctx, req.TenantId, req.TransformationName, req.TransformationDescription, req.TransformationType, req.TransformationVersion, req.TransformationFunction, req.OwnerId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create transformation: %v", err)
	}

	// Convert to protobuf format
	protoTransformation := s.transformationToProto(createdTransformation)

	return &corev1.AddTransformationResponse{
		Message:        "Transformation created successfully",
		Success:        true,
		Transformation: protoTransformation,
		Status:         commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) ModifyTransformation(ctx context.Context, req *corev1.ModifyTransformationRequest) (*corev1.ModifyTransformationResponse, error) {
	defer s.trackOperation()()

	// Get transformation service
	transformationService := transformation.NewService(s.engine.db, s.engine.logger)

	// Build update map
	updates := make(map[string]interface{})
	if req.TransformationNameNew != nil {
		updates["transformation_name"] = *req.TransformationNameNew
	}
	if req.TransformationDescription != nil {
		updates["transformation_description"] = *req.TransformationDescription
	}
	if req.TransformationType != nil {
		updates["transformation_type"] = *req.TransformationType
	}
	if req.TransformationVersion != nil {
		updates["transformation_version"] = *req.TransformationVersion
	}
	if req.TransformationFunction != nil {
		updates["transformation_function"] = *req.TransformationFunction
	}

	// Update the transformation
	updatedTransformation, err := transformationService.Update(ctx, req.TenantId, req.TransformationId, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update transformation: %v", err)
	}

	// Convert to protobuf format
	protoTransformation := s.transformationToProto(updatedTransformation)

	return &corev1.ModifyTransformationResponse{
		Message:        "Transformation updated successfully",
		Success:        true,
		Transformation: protoTransformation,
		Status:         commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DeleteTransformation(ctx context.Context, req *corev1.DeleteTransformationRequest) (*corev1.DeleteTransformationResponse, error) {
	defer s.trackOperation()()

	// Get transformation service
	transformationService := transformation.NewService(s.engine.db, s.engine.logger)

	// Delete the transformation
	err := transformationService.Delete(ctx, req.TenantId, req.TransformationId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to delete transformation: %v", err)
	}

	return &corev1.DeleteTransformationResponse{
		Message: "Transformation deleted successfully",
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}
