package engine

import (
	"context"
	"fmt"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
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

	// Get mesh service
	meshService := mesh.NewService(s.engine.db, s.engine.logger)

	// Get the local node information (already exists from initialization)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	s.engine.logger.Infof("Using existing local node: %s (ID: %s) for mesh seeding", localNode.Name, localNode.ID)

	// Create the mesh
	createdMesh, err := meshService.Create(ctx, req.MeshName, req.GetMeshDescription(), req.GetAllowJoin())
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create mesh: %v", err)
	}

	// Set initial states: mesh stays PENDING, node becomes JOINING
	if err := meshService.UpdateMeshStatus(ctx, createdMesh.ID, "STATUS_PENDING"); err != nil {
		s.engine.logger.Warnf("Failed to set mesh status to PENDING: %v", err)
	}
	if err := meshService.SetNodeAsJoining(ctx, localNode.ID); err != nil {
		s.engine.logger.Warnf("Failed to set node status to JOINING: %v", err)
	}

	// Note: Mesh runtime starts automatically with the mesh service
	// No explicit StartMesh call needed - this is handled by the mesh service itself
	s.engine.logger.Infof("Mesh %s created with local node %s. Mesh runtime will start automatically.", createdMesh.Name, localNode.Name)

	// Convert to protobuf format
	protoMesh := s.meshToProto(createdMesh)

	return &corev1.SeedMeshResponse{
		Message: fmt.Sprintf("Mesh %s created with existing local node %s. Runtime initialization pending.", createdMesh.Name, localNode.Name),
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

	// Note: Mesh runtime starts automatically with the mesh service
	// For joining a mesh, we need to establish connections to existing nodes
	// TODO: Implement AddSession calls to connect to existing mesh nodes
	// This requires getting the list of existing nodes in the mesh and calling AddSession for each
	s.engine.logger.Infof("Node %s created for mesh %s. Mesh runtime will start automatically. Connection to existing nodes pending implementation.", createdNode.Name, req.MeshId)

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
