package engine

import (
	"context"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/services/core/internal/services/transformation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ============================================================================
// TransformationService gRPC handlers
// ============================================================================

func (s *Server) ListTransformations(ctx context.Context, req *corev1.ListTransformationsRequest) (*corev1.ListTransformationsResponse, error) {
	defer s.trackOperation()()

	// Get transformation service
	transformationService := transformation.NewService(s.engine.db, s.engine.logger)

	// List transformations for the tenant
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
