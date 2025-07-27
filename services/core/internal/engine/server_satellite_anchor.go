package engine

import (
	"context"
	"fmt"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/services/core/internal/services/anchor"
	"github.com/redbco/redb-open/services/core/internal/services/satellite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SatelliteService methods
func (s *Server) ListSatellites(ctx context.Context, req *corev1.ListSatellitesRequest) (*corev1.ListSatellitesResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get satellite service
	satelliteService := satellite.NewService(s.engine.db, s.engine.logger)

	// List satellites for the tenant
	satellites, err := satelliteService.List(ctx, req.TenantId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to list satellites: %v", err)
	}

	// Convert to protobuf format
	protoSatellites := make([]*corev1.Satellite, len(satellites))
	for i, sat := range satellites {
		protoSatellites[i] = s.satelliteToProto(sat)
	}

	return &corev1.ListSatellitesResponse{
		Satellites: protoSatellites,
	}, nil
}

func (s *Server) ShowSatellite(ctx context.Context, req *corev1.ShowSatelliteRequest) (*corev1.ShowSatelliteResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get satellite service
	satelliteService := satellite.NewService(s.engine.db, s.engine.logger)

	// Get the satellite
	sat, err := satelliteService.Get(ctx, req.SatelliteId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "satellite not found: %v", err)
	}

	// Verify tenant access
	if sat.TenantID != req.TenantId {
		return nil, status.Errorf(codes.PermissionDenied, "satellite not found in tenant")
	}

	// Convert to protobuf format
	protoSatellite := s.satelliteToProto(sat)

	return &corev1.ShowSatelliteResponse{
		Satellite: protoSatellite,
	}, nil
}

func (s *Server) AddSatellite(ctx context.Context, req *corev1.AddSatelliteRequest) (*corev1.AddSatelliteResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get satellite service
	satelliteService := satellite.NewService(s.engine.db, s.engine.logger)

	// Create the satellite
	createdSatellite, err := satelliteService.Create(ctx, req.TenantId, req.SatelliteName, req.GetSatelliteDescription(), req.SatellitePlatform, req.SatelliteVersion, "", req.IpAddress, req.NodeId, req.PublicKey, req.PrivateKey, req.OwnerId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create satellite: %v", err)
	}

	// Convert to protobuf format
	protoSatellite := s.satelliteToProto(createdSatellite)

	return &corev1.AddSatelliteResponse{
		Message:   fmt.Sprintf("Satellite %s created successfully", createdSatellite.Name),
		Success:   true,
		Satellite: protoSatellite,
		Status:    commonv1.Status_STATUS_CREATED,
	}, nil
}

func (s *Server) ModifySatellite(ctx context.Context, req *corev1.ModifySatelliteRequest) (*corev1.ModifySatelliteResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get satellite service
	satelliteService := satellite.NewService(s.engine.db, s.engine.logger)

	// Build updates map
	updates := make(map[string]interface{})
	if req.SatelliteName != nil {
		updates["satellite_name"] = *req.SatelliteName
	}
	if req.SatelliteDescription != nil {
		updates["satellite_description"] = *req.SatelliteDescription
	}
	if req.SatellitePlatform != nil {
		updates["satellite_platform"] = *req.SatellitePlatform
	}
	if req.SatelliteVersion != nil {
		updates["satellite_version"] = *req.SatelliteVersion
	}
	if req.IpAddress != nil {
		updates["satellite_ip_address"] = *req.IpAddress
	}
	if req.NodeId != nil {
		updates["connected_to_node_id"] = *req.NodeId
	}

	// Update the satellite
	updatedSatellite, err := satelliteService.Update(ctx, req.SatelliteId, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update satellite: %v", err)
	}

	// Verify tenant access
	if updatedSatellite.TenantID != req.TenantId {
		return nil, status.Errorf(codes.PermissionDenied, "satellite not found in tenant")
	}

	// Convert to protobuf format
	protoSatellite := s.satelliteToProto(updatedSatellite)

	return &corev1.ModifySatelliteResponse{
		Message:   fmt.Sprintf("Satellite %s updated successfully", updatedSatellite.Name),
		Success:   true,
		Satellite: protoSatellite,
		Status:    commonv1.Status_STATUS_UPDATED,
	}, nil
}

func (s *Server) DeleteSatellite(ctx context.Context, req *corev1.DeleteSatelliteRequest) (*corev1.DeleteSatelliteResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get satellite service
	satelliteService := satellite.NewService(s.engine.db, s.engine.logger)

	// Verify the satellite exists and belongs to the tenant
	sat, err := satelliteService.Get(ctx, req.SatelliteId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "satellite not found: %v", err)
	}
	if sat.TenantID != req.TenantId {
		return nil, status.Errorf(codes.PermissionDenied, "satellite not found in tenant")
	}

	// Delete the satellite
	err = satelliteService.Delete(ctx, req.SatelliteId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to delete satellite: %v", err)
	}

	return &corev1.DeleteSatelliteResponse{
		Message: "Satellite deleted successfully",
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// AnchorService methods
func (s *Server) ListAnchors(ctx context.Context, req *corev1.ListAnchorsRequest) (*corev1.ListAnchorsResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get anchor service
	anchorService := anchor.NewService(s.engine.db, s.engine.logger)

	// List anchors for the tenant
	anchors, err := anchorService.List(ctx, req.TenantId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to list anchors: %v", err)
	}

	// Convert to protobuf format
	protoAnchors := make([]*corev1.Anchor, len(anchors))
	for i, anc := range anchors {
		protoAnchors[i] = s.anchorToProto(anc)
	}

	return &corev1.ListAnchorsResponse{
		Anchors: protoAnchors,
	}, nil
}

func (s *Server) ShowAnchor(ctx context.Context, req *corev1.ShowAnchorRequest) (*corev1.ShowAnchorResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get anchor service
	anchorService := anchor.NewService(s.engine.db, s.engine.logger)

	// Get the anchor
	anc, err := anchorService.Get(ctx, req.AnchorId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "anchor not found: %v", err)
	}

	// Verify tenant access
	if anc.TenantID != req.TenantId {
		return nil, status.Errorf(codes.PermissionDenied, "anchor not found in tenant")
	}

	// Convert to protobuf format
	protoAnchor := s.anchorToProto(anc)

	return &corev1.ShowAnchorResponse{
		Anchor: protoAnchor,
	}, nil
}

func (s *Server) AddAnchor(ctx context.Context, req *corev1.AddAnchorRequest) (*corev1.AddAnchorResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get anchor service
	anchorService := anchor.NewService(s.engine.db, s.engine.logger)

	// Create the anchor
	createdAnchor, err := anchorService.Create(ctx, req.TenantId, req.AnchorName, req.GetAnchorDescription(), req.AnchorPlatform, req.AnchorVersion, "", req.IpAddress, req.NodeId, req.PublicKey, req.PrivateKey, req.OwnerId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create anchor: %v", err)
	}

	// Convert to protobuf format
	protoAnchor := s.anchorToProto(createdAnchor)

	return &corev1.AddAnchorResponse{
		Message: fmt.Sprintf("Anchor %s created successfully", createdAnchor.Name),
		Success: true,
		Anchor:  protoAnchor,
		Status:  commonv1.Status_STATUS_CREATED,
	}, nil
}

func (s *Server) ModifyAnchor(ctx context.Context, req *corev1.ModifyAnchorRequest) (*corev1.ModifyAnchorResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get anchor service
	anchorService := anchor.NewService(s.engine.db, s.engine.logger)

	// Build updates map
	updates := make(map[string]interface{})
	if req.AnchorName != nil {
		updates["anchor_name"] = *req.AnchorName
	}
	if req.AnchorDescription != nil {
		updates["anchor_description"] = *req.AnchorDescription
	}
	if req.AnchorPlatform != nil {
		updates["anchor_platform"] = *req.AnchorPlatform
	}
	if req.AnchorVersion != nil {
		updates["anchor_version"] = *req.AnchorVersion
	}
	if req.IpAddress != nil {
		updates["anchor_ip_address"] = *req.IpAddress
	}
	if req.NodeId != nil {
		updates["connected_to_node_id"] = *req.NodeId
	}

	// Update the anchor
	updatedAnchor, err := anchorService.Update(ctx, req.AnchorId, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update anchor: %v", err)
	}

	// Verify tenant access
	if updatedAnchor.TenantID != req.TenantId {
		return nil, status.Errorf(codes.PermissionDenied, "anchor not found in tenant")
	}

	// Convert to protobuf format
	protoAnchor := s.anchorToProto(updatedAnchor)

	return &corev1.ModifyAnchorResponse{
		Message: fmt.Sprintf("Anchor %s updated successfully", updatedAnchor.Name),
		Success: true,
		Anchor:  protoAnchor,
		Status:  commonv1.Status_STATUS_UPDATED,
	}, nil
}

func (s *Server) DeleteAnchor(ctx context.Context, req *corev1.DeleteAnchorRequest) (*corev1.DeleteAnchorResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get anchor service
	anchorService := anchor.NewService(s.engine.db, s.engine.logger)

	// Verify the anchor exists and belongs to the tenant
	anc, err := anchorService.Get(ctx, req.AnchorId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "anchor not found: %v", err)
	}
	if anc.TenantID != req.TenantId {
		return nil, status.Errorf(codes.PermissionDenied, "anchor not found in tenant")
	}

	// Delete the anchor
	err = anchorService.Delete(ctx, req.AnchorId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to delete anchor: %v", err)
	}

	return &corev1.DeleteAnchorResponse{
		Message: "Anchor deleted successfully",
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}
