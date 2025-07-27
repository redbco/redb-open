package engine

import (
	"context"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/services/core/internal/services/region"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RegionService methods
func (s *Server) ListRegions(ctx context.Context, req *corev1.ListRegionsRequest) (*corev1.ListRegionsResponse, error) {
	defer s.trackOperation()()

	// Get region service
	regionService := region.NewService(s.engine.db, s.engine.logger)

	// List regions for the tenant (includes global regions)
	regions, err := regionService.List(ctx, req.TenantId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to list regions: %v", err)
	}

	// Convert to protobuf format
	protoRegions := make([]*corev1.Region, len(regions))
	for i, reg := range regions {
		protoRegions[i] = s.regionToProto(reg)
	}

	return &corev1.ListRegionsResponse{
		Regions: protoRegions,
	}, nil
}

func (s *Server) ShowRegion(ctx context.Context, req *corev1.ShowRegionRequest) (*corev1.ShowRegionResponse, error) {
	defer s.trackOperation()()

	// Get region service
	regionService := region.NewService(s.engine.db, s.engine.logger)

	// Get the region
	reg, err := regionService.Get(ctx, req.RegionName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "region not found: %v", err)
	}

	// Convert to protobuf format
	protoRegion := s.regionToProto(reg)

	return &corev1.ShowRegionResponse{
		Region: protoRegion,
	}, nil
}

func (s *Server) AddRegion(ctx context.Context, req *corev1.AddRegionRequest) (*corev1.AddRegionResponse, error) {
	defer s.trackOperation()()

	// Get region service
	regionService := region.NewService(s.engine.db, s.engine.logger)

	// Check if region already exists
	exists, err := regionService.Exists(ctx, req.RegionName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to check region existence: %v", err)
	}
	if exists {
		return nil, status.Errorf(codes.AlreadyExists, "region with name %s already exists", req.RegionName)
	}

	// Set defaults for optional fields
	description := ""
	if req.RegionDescription != nil {
		description = *req.RegionDescription
	}

	location := ""
	if req.RegionLocation != nil {
		location = *req.RegionLocation
	}

	// Create the region
	reg, err := regionService.Create(ctx, req.TenantId, req.RegionName, req.RegionType, description, location, req.RegionLatitude, req.RegionLongitude)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create region: %v", err)
	}

	// Convert to protobuf format
	protoRegion := s.regionToProto(reg)

	return &corev1.AddRegionResponse{
		Message: "Region created successfully",
		Success: true,
		Region:  protoRegion,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) AddGlobalRegion(ctx context.Context, req *corev1.AddGlobalRegionRequest) (*corev1.AddGlobalRegionResponse, error) {
	defer s.trackOperation()()

	// Get region service
	regionService := region.NewService(s.engine.db, s.engine.logger)

	// Check if region already exists
	exists, err := regionService.Exists(ctx, req.RegionName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to check region existence: %v", err)
	}
	if exists {
		return nil, status.Errorf(codes.AlreadyExists, "region with name %s already exists", req.RegionName)
	}

	// Set defaults for optional fields
	description := ""
	if req.RegionDescription != nil {
		description = *req.RegionDescription
	}

	location := ""
	if req.RegionLocation != nil {
		location = *req.RegionLocation
	}

	// Create the global region
	reg, err := regionService.CreateGlobal(ctx, req.RegionName, req.RegionType, description, location, req.RegionLatitude, req.RegionLongitude)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create global region: %v", err)
	}

	// Convert to protobuf format
	protoRegion := s.regionToProto(reg)

	return &corev1.AddGlobalRegionResponse{
		Message: "Global region created successfully",
		Success: true,
		Region:  protoRegion,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) ModifyRegion(ctx context.Context, req *corev1.ModifyRegionRequest) (*corev1.ModifyRegionResponse, error) {
	defer s.trackOperation()()

	// Get region service
	regionService := region.NewService(s.engine.db, s.engine.logger)

	// Verify region exists
	_, err := regionService.Get(ctx, req.RegionName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "region not found: %v", err)
	}

	// Build updates map
	updates := make(map[string]interface{})
	if req.RegionNameNew != nil {
		updates["region_name"] = *req.RegionNameNew
	}
	if req.RegionDescription != nil {
		updates["region_description"] = *req.RegionDescription
	}
	if req.RegionLocation != nil {
		updates["region_location"] = *req.RegionLocation
	}
	if req.RegionLatitude != nil {
		updates["region_latitude"] = *req.RegionLatitude
	}
	if req.RegionLongitude != nil {
		updates["region_longitude"] = *req.RegionLongitude
	}

	// Update the region
	updatedReg, err := regionService.Update(ctx, req.RegionName, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update region: %v", err)
	}

	// Convert to protobuf format
	protoRegion := s.regionToProto(updatedReg)

	return &corev1.ModifyRegionResponse{
		Message: "Region updated successfully",
		Success: true,
		Region:  protoRegion,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) ModifyGlobalRegion(ctx context.Context, req *corev1.ModifyGlobalRegionRequest) (*corev1.ModifyGlobalRegionResponse, error) {
	defer s.trackOperation()()

	// Get region service
	regionService := region.NewService(s.engine.db, s.engine.logger)

	// Verify region exists and is global
	reg, err := regionService.Get(ctx, req.RegionName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "region not found: %v", err)
	}
	if !reg.GlobalRegion {
		return nil, status.Errorf(codes.PermissionDenied, "region is not a global region")
	}

	// Build updates map
	updates := make(map[string]interface{})
	if req.RegionNameNew != nil {
		updates["region_name"] = *req.RegionNameNew
	}
	if req.RegionDescription != nil {
		updates["region_description"] = *req.RegionDescription
	}
	if req.RegionLocation != nil {
		updates["region_location"] = *req.RegionLocation
	}
	if req.RegionLatitude != nil {
		updates["region_latitude"] = *req.RegionLatitude
	}
	if req.RegionLongitude != nil {
		updates["region_longitude"] = *req.RegionLongitude
	}

	// Update the region
	updatedReg, err := regionService.Update(ctx, req.RegionName, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update global region: %v", err)
	}

	// Convert to protobuf format
	protoRegion := s.regionToProto(updatedReg)

	return &corev1.ModifyGlobalRegionResponse{
		Message: "Global region updated successfully",
		Success: true,
		Region:  protoRegion,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DeleteRegion(ctx context.Context, req *corev1.DeleteRegionRequest) (*corev1.DeleteRegionResponse, error) {
	defer s.trackOperation()()

	// Get region service
	regionService := region.NewService(s.engine.db, s.engine.logger)

	// Verify region exists
	_, err := regionService.Get(ctx, req.RegionName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "region not found: %v", err)
	}

	// Delete the region
	err = regionService.Delete(ctx, req.RegionName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to delete region: %v", err)
	}

	return &corev1.DeleteRegionResponse{
		Message: "Region deleted successfully",
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DeleteGlobalRegion(ctx context.Context, req *corev1.DeleteGlobalRegionRequest) (*corev1.DeleteGlobalRegionResponse, error) {
	defer s.trackOperation()()

	// Get region service
	regionService := region.NewService(s.engine.db, s.engine.logger)

	// Verify region exists and is global
	reg, err := regionService.Get(ctx, req.RegionName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "region not found: %v", err)
	}
	if !reg.GlobalRegion {
		return nil, status.Errorf(codes.PermissionDenied, "region is not a global region")
	}

	// Delete the global region
	err = regionService.Delete(ctx, req.RegionName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to delete global region: %v", err)
	}

	return &corev1.DeleteGlobalRegionResponse{
		Message: "Global region deleted successfully",
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}
