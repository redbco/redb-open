package engine

import (
	"context"
	"fmt"
	"time"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	"github.com/redbco/redb-open/services/core/internal/services/mesh"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MeshService methods
func (s *Server) SeedMesh(ctx context.Context, req *corev1.SeedMeshRequest) (*corev1.SeedMeshResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Validate required fields
	if req.MeshName == "" {
		return nil, status.Error(codes.InvalidArgument, "mesh_name is required")
	}
	if req.NodeName == "" {
		return nil, status.Error(codes.InvalidArgument, "node_name is required")
	}

	// Get mesh service
	meshService := mesh.NewService(s.engine.db, s.engine.logger)

	// Create the mesh
	createdMesh, err := meshService.Create(ctx, req.MeshName, req.GetMeshDescription(), req.GetAllowJoin())
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create mesh: %v", err)
	}

	// Create the seed node
	createdNode, err := meshService.CreateNode(ctx, createdMesh.ID, req.NodeName, req.GetNodeDescription(),
		"reDB", "1.0.0", "0.0.0.0", 8443, nil)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create seed node: %v", err)
	}

	// Call mesh service via gRPC to initialize runtime mesh
	meshClient := s.engine.GetMeshClient()
	if meshClient != nil {
		seedReq := &meshv1.SeedMeshRequest{
			MeshId: createdMesh.ID,
			NodeId: createdNode.ID,
		}

		seedResp, err := meshClient.SeedMesh(ctx, seedReq)
		if err != nil {
			s.engine.logger.Errorf("Failed to initialize mesh runtime: %v", err)
			// Don't fail the whole operation, just log the error
		} else if !seedResp.Success {
			s.engine.logger.Errorf("Mesh runtime initialization failed: %s", seedResp.Error)
		} else {
			s.engine.logger.Infof("Mesh runtime initialized successfully with token: %s", seedResp.Token)
		}
	} else {
		s.engine.logger.Warnf("Mesh client not available - runtime initialization skipped")
	}

	// Convert to protobuf format
	protoMesh := s.meshToProto(createdMesh)

	return &corev1.SeedMeshResponse{
		Message: fmt.Sprintf("Mesh %s created with seed node %s. Runtime initialization pending.", createdMesh.Name, createdNode.Name),
		Success: true,
		Mesh:    protoMesh,
		Status:  commonv1.Status_STATUS_CREATED,
	}, nil
}

func (s *Server) JoinMesh(ctx context.Context, req *corev1.JoinMeshRequest) (*corev1.JoinMeshResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Validate required fields
	if req.MeshId == "" {
		return nil, status.Error(codes.InvalidArgument, "mesh_id is required")
	}
	if req.NodeName == "" {
		return nil, status.Error(codes.InvalidArgument, "node_name is required")
	}

	// Get mesh service
	meshService := mesh.NewService(s.engine.db, s.engine.logger)

	// Verify mesh exists
	existingMesh, err := meshService.Get(ctx, req.MeshId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "mesh not found: %v", err)
	}

	// Create the node in the persistent tables
	createdNode, err := meshService.CreateNode(ctx, req.MeshId, req.NodeName, req.GetNodeDescription(), 
		"reDB", "1.0.0", "0.0.0.0", 8443, nil)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create node: %v", err)
	}

	// Call mesh service via gRPC to initialize runtime mesh
	meshClient := s.engine.GetMeshClient()
	if meshClient != nil {
		// TODO: Get proper token from mesh seed node
		// For now, generate a temporary token
		tempToken := fmt.Sprintf("mesh-token-%s-%d", req.MeshId, time.Now().Unix())

		joinReq := &meshv1.JoinMeshRequest{
			MeshId:        req.MeshId,
			NodeId:        createdNode.ID,
			Token:         tempToken,
			PeerAddresses: []string{"localhost:8443"}, // TODO: Get actual peer addresses
		}

		joinResp, err := meshClient.JoinMesh(ctx, joinReq)
		if err != nil {
			s.engine.logger.Errorf("Failed to join mesh runtime: %v", err)
			// Don't fail the whole operation, just log the error
		} else if !joinResp.Success {
			s.engine.logger.Errorf("Mesh runtime join failed: %s", joinResp.Error)
		} else {
			s.engine.logger.Infof("Successfully joined mesh runtime")
		}
	} else {
		s.engine.logger.Warnf("Mesh client not available - runtime join skipped")
	}
	
	return &corev1.JoinMeshResponse{
		Message: fmt.Sprintf("Node %s created for mesh %s. Runtime initialization pending.", createdNode.Name, existingMesh.Name),
		Success: true,
		Status:  commonv1.Status_STATUS_CREATED,
	}, nil
}

func (s *Server) LeaveMesh(ctx context.Context, req *corev1.LeaveMeshRequest) (*corev1.LeaveMeshResponse, error) {
	defer s.trackOperation()()
	return nil, s.notImplemented("LeaveMesh")
}

func (s *Server) ShowMesh(ctx context.Context, req *corev1.ShowMeshRequest) (*corev1.ShowMeshResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get mesh service
	meshService := mesh.NewService(s.engine.db, s.engine.logger)

	// Get the mesh
	meshObj, err := meshService.Get(ctx, req.MeshId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "mesh not found: %v", err)
	}

	// Convert to protobuf format
	protoMesh := s.meshToProto(meshObj)

	return &corev1.ShowMeshResponse{
		Mesh: protoMesh,
	}, nil
}

func (s *Server) ListNodes(ctx context.Context, req *corev1.ListNodesRequest) (*corev1.ListNodesResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get mesh service
	meshService := mesh.NewService(s.engine.db, s.engine.logger)

	// Get nodes in the mesh
	nodes, err := meshService.GetNodes(ctx, req.MeshId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to list nodes: %v", err)
	}

	// Convert to protobuf format
	protoNodes := make([]*corev1.Node, len(nodes))
	for i, node := range nodes {
		protoNodes[i] = s.nodeToProto(node)
	}

	return &corev1.ListNodesResponse{
		Nodes: protoNodes,
	}, nil
}

func (s *Server) ShowNode(ctx context.Context, req *corev1.ShowNodeRequest) (*corev1.ShowNodeResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get mesh service
	meshService := mesh.NewService(s.engine.db, s.engine.logger)

	// Get the node
	node, err := meshService.GetNode(ctx, req.MeshId, req.NodeId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "node not found: %v", err)
	}

	// Convert to protobuf format
	protoNode := s.nodeToProto(node)

	return &corev1.ShowNodeResponse{
		Node: protoNode,
	}, nil
}

func (s *Server) ShowTopology(ctx context.Context, req *corev1.ShowTopologyRequest) (*corev1.ShowTopologyResponse, error) {
	defer s.trackOperation()()
	return nil, s.notImplemented("ShowTopology")
}

func (s *Server) ModifyMesh(ctx context.Context, req *corev1.ModifyMeshRequest) (*corev1.ModifyMeshResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get mesh service
	meshService := mesh.NewService(s.engine.db, s.engine.logger)

	// Build updates map
	updates := make(map[string]interface{})
	if req.MeshName != nil {
		updates["mesh_name"] = *req.MeshName
	}
	if req.MeshDescription != nil {
		updates["mesh_description"] = *req.MeshDescription
	}
	if req.AllowJoin != nil {
		updates["allow_join"] = *req.AllowJoin
	}

	// Update the mesh
	updatedMesh, err := meshService.Update(ctx, req.MeshId, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update mesh: %v", err)
	}

	// Convert to protobuf format
	protoMesh := s.meshToProto(updatedMesh)

	return &corev1.ModifyMeshResponse{
		Message: fmt.Sprintf("Mesh %s updated successfully", updatedMesh.Name),
		Success: true,
		Mesh:    protoMesh,
		Status:  commonv1.Status_STATUS_UPDATED,
	}, nil
}

func (s *Server) ModifyNode(ctx context.Context, req *corev1.ModifyNodeRequest) (*corev1.ModifyNodeResponse, error) {
	defer s.trackOperation()()
	return nil, s.notImplemented("ModifyNode")
}

func (s *Server) EvictNode(ctx context.Context, req *corev1.EvictNodeRequest) (*corev1.EvictNodeResponse, error) {
	defer s.trackOperation()()
	return nil, s.notImplemented("EvictNode")
}

func (s *Server) AddMeshRoute(ctx context.Context, req *corev1.AddMeshRouteRequest) (*corev1.AddMeshRouteResponse, error) {
	defer s.trackOperation()()
	return nil, s.notImplemented("AddMeshRoute")
}

func (s *Server) ModifyMeshRoute(ctx context.Context, req *corev1.ModifyMeshRouteRequest) (*corev1.ModifyMeshRouteResponse, error) {
	defer s.trackOperation()()
	return nil, s.notImplemented("ModifyMeshRoute")
}

func (s *Server) DeleteMeshRoute(ctx context.Context, req *corev1.DeleteMeshRouteRequest) (*corev1.DeleteMeshRouteResponse, error) {
	defer s.trackOperation()()
	return nil, s.notImplemented("DeleteMeshRoute")
}
