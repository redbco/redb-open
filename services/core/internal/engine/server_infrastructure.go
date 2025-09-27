package engine

import (
	"context"
	"fmt"
	"strconv"
	"time"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	meshsvc "github.com/redbco/redb-open/services/core/internal/services/mesh"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// === Core Mesh Operations ===

// SeedMesh creates a new mesh without connecting to any other nodes
func (s *Server) SeedMesh(ctx context.Context, req *corev1.SeedMeshRequest) (*corev1.SeedMeshResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Validate required fields
	if req.MeshName == "" {
		return nil, status.Error(codes.InvalidArgument, "mesh_name is required")
	}

	// Check if node is already in a mesh
	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	// Check if node is already part of a mesh
	if localNode.Status == "STATUS_ACTIVE" {
		// Check if node has mesh membership
		existingMeshes, err := meshService.GetMeshesByNodeID(ctx, localNode.ID)
		if err == nil && len(existingMeshes) > 0 {
			return nil, status.Error(codes.FailedPrecondition, "node is already part of a mesh")
		}
	}

	s.engine.logger.Infof("Seeding new mesh '%s' with local node: %s (ID: %s)", req.MeshName, localNode.Name, localNode.ID)

	// Create the mesh
	createdMesh, err := meshService.Create(ctx, req.MeshName, req.GetMeshDescription(), true)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create mesh: %v", err)
	}

	// Add local node to the mesh
	if err := meshService.AddNodeToMesh(ctx, createdMesh.ID, localNode.ID); err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to add node to mesh: %v", err)
	}

	// Update node status to ACTIVE
	if err := meshService.UpdateNodeStatus(ctx, localNode.ID, "STATUS_ACTIVE"); err != nil {
		s.engine.logger.Warnf("Failed to set node status to ACTIVE: %v", err)
	}

	s.engine.logger.Infof("Successfully seeded mesh '%s' with node '%s'", createdMesh.Name, localNode.Name)

	// Convert to protobuf format
	protoMesh := s.meshToProtoNew(createdMesh)

	return &corev1.SeedMeshResponse{
		Message: fmt.Sprintf("Successfully seeded mesh '%s' with local node '%s'", createdMesh.Name, localNode.Name),
		Success: true,
		Mesh:    protoMesh,
		Status:  commonv1.Status_STATUS_CREATED,
	}, nil
}

// JoinMesh joins an existing mesh by connecting to a node in that mesh
func (s *Server) JoinMesh(ctx context.Context, req *corev1.JoinMeshRequest) (*corev1.JoinMeshResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Validate required fields
	if req.TargetAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "target_address is required")
	}

	// Check if node is already in a mesh
	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	// Verify node is clean (not in any mesh)
	existingMeshes, err := meshService.GetMeshesByNodeID(ctx, localNode.ID)
	if err == nil && len(existingMeshes) > 0 {
		return nil, status.Error(codes.FailedPrecondition, "node is already part of a mesh")
	}

	// Set default strategy
	strategy := req.GetStrategy()
	if strategy == corev1.JoinStrategy_JOIN_STRATEGY_UNSPECIFIED {
		strategy = corev1.JoinStrategy_JOIN_STRATEGY_INHERIT
	}

	// Set default timeout
	timeout := req.GetTimeoutSeconds()
	if timeout == 0 {
		timeout = 30
	}

	s.engine.logger.Infof("Joining mesh via %s with strategy %s (timeout: %ds)", req.TargetAddress, strategy, timeout)

	// Update node status to JOINING
	if err := meshService.UpdateNodeStatus(ctx, localNode.ID, "STATUS_JOINING"); err != nil {
		s.engine.logger.Warnf("Failed to set node status to JOINING: %v", err)
	}

	// Attempt to connect to the target node
	addSessionResp, err := s.AddMeshSession(ctx, req.TargetAddress, timeout)
	if err != nil {
		// Reset node status to CLEAN on failure
		meshService.UpdateNodeStatus(ctx, localNode.ID, "STATUS_CLEAN")
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect to target node: %v", err)
	}

	if !addSessionResp.Success {
		// Reset node status to CLEAN on failure
		meshService.UpdateNodeStatus(ctx, localNode.ID, "STATUS_CLEAN")
		return nil, status.Errorf(codes.Internal, "failed to connect to target node: %s", addSessionResp.Message)
	}

	// TODO: Implement mesh discovery and configuration synchronization based on strategy
	// For now, we'll create a placeholder mesh entry

	// Get mesh information from the connected peer (this would be done via mesh protocol)
	// For now, create a temporary mesh entry
	meshName := fmt.Sprintf("mesh-via-%d", addSessionResp.PeerNodeId)
	createdMesh, err := meshService.Create(ctx, meshName, "Joined mesh", true)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create mesh entry: %v", err)
	}

	// Add local node to the mesh
	if err := meshService.AddNodeToMesh(ctx, createdMesh.ID, localNode.ID); err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to add node to mesh: %v", err)
	}

	// Update node status to ACTIVE
	if err := meshService.UpdateNodeStatus(ctx, localNode.ID, "STATUS_ACTIVE"); err != nil {
		s.engine.logger.Warnf("Failed to set node status to ACTIVE: %v", err)
	}

	s.engine.logger.Infof("Successfully joined mesh via peer node %d at %s", addSessionResp.PeerNodeId, addSessionResp.RemoteAddr)

	// Convert to protobuf format
	protoMesh := s.meshToProtoNew(createdMesh)

	return &corev1.JoinMeshResponse{
		Message:    fmt.Sprintf("Successfully joined mesh via peer node %d", addSessionResp.PeerNodeId),
		Success:    true,
		Mesh:       protoMesh,
		PeerNodeId: addSessionResp.PeerNodeId,
		RemoteAddr: addSessionResp.RemoteAddr,
		Status:     commonv1.Status_STATUS_CREATED,
	}, nil
}

// ExtendMesh extends the current mesh to a clean node
func (s *Server) ExtendMesh(ctx context.Context, req *corev1.ExtendMeshRequest) (*corev1.ExtendMeshResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Validate required fields
	if req.TargetAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "target_address is required")
	}

	// Check if node is in a mesh
	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	// Verify node is part of a mesh
	existingMeshes, err := meshService.GetMeshesByNodeID(ctx, localNode.ID)
	if err != nil || len(existingMeshes) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "node is not part of any mesh")
	}

	// Set default strategy
	strategy := req.GetStrategy()
	if strategy == corev1.JoinStrategy_JOIN_STRATEGY_UNSPECIFIED {
		strategy = corev1.JoinStrategy_JOIN_STRATEGY_INHERIT
	}

	// Set default timeout
	timeout := req.GetTimeoutSeconds()
	if timeout == 0 {
		timeout = 30
	}

	s.engine.logger.Infof("Extending mesh to %s with strategy %s (timeout: %ds)", req.TargetAddress, strategy, timeout)

	// Attempt to connect to the target node
	addSessionResp, err := s.AddMeshSession(ctx, req.TargetAddress, timeout)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect to target node: %v", err)
	}

	if !addSessionResp.Success {
		return nil, status.Errorf(codes.Internal, "failed to connect to target node: %s", addSessionResp.Message)
	}

	// TODO: Implement mesh extension logic based on strategy
	// This would involve synchronizing mesh configuration to the target node

	s.engine.logger.Infof("Successfully extended mesh to peer node %d at %s", addSessionResp.PeerNodeId, addSessionResp.RemoteAddr)

	return &corev1.ExtendMeshResponse{
		Message:    fmt.Sprintf("Successfully extended mesh to peer node %d", addSessionResp.PeerNodeId),
		Success:    true,
		PeerNodeId: addSessionResp.PeerNodeId,
		RemoteAddr: addSessionResp.RemoteAddr,
		Status:     commonv1.Status_STATUS_CREATED,
	}, nil
}

// LeaveMesh removes the current node from its mesh
func (s *Server) LeaveMesh(ctx context.Context, req *corev1.LeaveMeshRequest) (*corev1.LeaveMeshResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	// Verify node is part of a mesh
	existingMeshes, err := meshService.GetMeshesByNodeID(ctx, localNode.ID)
	if err != nil || len(existingMeshes) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "node is not part of any mesh")
	}

	s.engine.logger.Infof("Node %s leaving mesh", localNode.Name)

	// Update node status to LEAVING
	if err := meshService.UpdateNodeStatus(ctx, localNode.ID, "STATUS_LEAVING"); err != nil {
		s.engine.logger.Warnf("Failed to set node status to LEAVING: %v", err)
	}

	// Get all current connections
	connectionsDropped := 0
	if s.engine.meshControlClient != nil {
		// Get current sessions from mesh service
		sessionsResp, err := s.engine.meshControlClient.GetSessions(ctx, &meshv1.GetSessionsRequest{})
		if err == nil {
			for _, session := range sessionsResp.Sessions {
				// Drop each connection
				dropResp, err := s.DropMeshSession(ctx, session.PeerNodeId)
				if err != nil {
					s.engine.logger.Warnf("Failed to drop connection to node %d: %v", session.PeerNodeId, err)
					if !req.Force {
						return nil, status.Errorf(codes.Internal, "failed to drop connection to node %d: %v", session.PeerNodeId, err)
					}
				} else if dropResp.Success {
					connectionsDropped++
				}
			}
		}
	}

	// Remove node from mesh
	for _, mesh := range existingMeshes {
		if err := meshService.RemoveNodeFromMesh(ctx, mesh.ID, localNode.ID); err != nil {
			s.engine.logger.Warnf("Failed to remove node from mesh %s: %v", mesh.ID, err)
			if !req.Force {
				return nil, status.Errorf(codes.Internal, "failed to remove node from mesh: %v", err)
			}
		}
	}

	// Update node status to CLEAN
	if err := meshService.UpdateNodeStatus(ctx, localNode.ID, "STATUS_CLEAN"); err != nil {
		s.engine.logger.Warnf("Failed to set node status to CLEAN: %v", err)
	}

	s.engine.logger.Infof("Successfully left mesh, dropped %d connections", connectionsDropped)

	return &corev1.LeaveMeshResponse{
		Message:            fmt.Sprintf("Successfully left mesh, dropped %d connections", connectionsDropped),
		Success:            true,
		ConnectionsDropped: int32(connectionsDropped),
		Status:             commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// EvictNode removes another node from the mesh by force
func (s *Server) EvictNode(ctx context.Context, req *corev1.EvictNodeRequest) (*corev1.EvictNodeResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	if req.TargetNodeId == 0 {
		return nil, status.Error(codes.InvalidArgument, "target_node_id is required")
	}

	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	// Verify local node is part of a mesh
	existingMeshes, err := meshService.GetMeshesByNodeID(ctx, localNode.ID)
	if err != nil || len(existingMeshes) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "node is not part of any mesh")
	}

	s.engine.logger.Infof("Evicting node %d from mesh", req.TargetNodeId)

	// Drop connection to the target node
	targetCleaned := false
	if s.engine.meshControlClient != nil {
		dropResp, err := s.DropMeshSession(ctx, req.TargetNodeId)
		if err != nil {
			s.engine.logger.Warnf("Failed to drop connection to target node %d: %v", req.TargetNodeId, err)
		} else if dropResp.Success {
			s.engine.logger.Infof("Successfully dropped connection to node %d", req.TargetNodeId)
		}

		// TODO: If req.CleanTarget is true, send a command to clean the target node
		// This would require implementing a mesh protocol command
		targetCleaned = req.CleanTarget
	}

	// Remove target node from mesh (local database)
	targetNodeIDStr := strconv.FormatUint(req.TargetNodeId, 10)
	for _, mesh := range existingMeshes {
		if err := meshService.RemoveNodeFromMesh(ctx, mesh.ID, targetNodeIDStr); err != nil {
			s.engine.logger.Warnf("Failed to remove target node from mesh %s: %v", mesh.ID, err)
		}
	}

	s.engine.logger.Infof("Successfully evicted node %d from mesh", req.TargetNodeId)

	return &corev1.EvictNodeResponse{
		Message:       fmt.Sprintf("Successfully evicted node %d from mesh", req.TargetNodeId),
		Success:       true,
		TargetCleaned: targetCleaned,
		Status:        commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// === Connection Management ===

// AddConnection adds a connection to another node in the same mesh
func (s *Server) AddConnection(ctx context.Context, req *corev1.AddConnectionRequest) (*corev1.AddConnectionResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	if req.TargetNodeId == 0 {
		return nil, status.Error(codes.InvalidArgument, "target_node_id is required")
	}

	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	// Verify local node is part of a mesh
	existingMeshes, err := meshService.GetMeshesByNodeID(ctx, localNode.ID)
	if err != nil || len(existingMeshes) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "node is not part of any mesh")
	}

	// TODO: Get target node address from mesh database
	// For now, we'll return an error indicating this needs to be implemented
	return nil, status.Error(codes.Unimplemented, "AddConnection requires mesh node registry implementation")
}

// DropConnection drops a connection to another node
func (s *Server) DropConnection(ctx context.Context, req *corev1.DropConnectionRequest) (*corev1.DropConnectionResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	if req.PeerNodeId == 0 {
		return nil, status.Error(codes.InvalidArgument, "peer_node_id is required")
	}

	s.engine.logger.Infof("Dropping connection to node %d", req.PeerNodeId)

	// Use the existing DropMeshSession method
	dropResp, err := s.DropMeshSession(ctx, req.PeerNodeId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to drop connection: %v", err)
	}

	var statusCode commonv1.Status
	if dropResp.Success {
		statusCode = commonv1.Status_STATUS_SUCCESS
	} else {
		statusCode = commonv1.Status_STATUS_ERROR
	}

	return &corev1.DropConnectionResponse{
		Message: dropResp.Message,
		Success: dropResp.Success,
		Status:  statusCode,
	}, nil
}

// ListConnections lists all active connections
func (s *Server) ListConnections(ctx context.Context, req *corev1.ListConnectionsRequest) (*corev1.ListConnectionsResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	var connections []*corev1.Connection

	if s.engine.meshControlClient != nil {
		// Get current sessions from mesh service
		sessionsResp, err := s.engine.meshControlClient.GetSessions(ctx, &meshv1.GetSessionsRequest{})
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to get sessions: %v", err)
		}

		// Convert mesh sessions to connection objects
		for _, session := range sessionsResp.Sessions {
			connection := &corev1.Connection{
				PeerNodeId:      session.PeerNodeId,
				PeerNodeName:    fmt.Sprintf("node-%d", session.PeerNodeId), // TODO: Get actual name
				RemoteAddr:      session.RemoteAddr,
				Status:          corev1.ConnectionStatus_CONNECTION_STATUS_CONNECTED, // TODO: Map actual status
				RttMicroseconds: session.RttMicroseconds,
				BytesSent:       session.BytesSent,
				BytesReceived:   session.BytesReceived,
				IsTls:           session.IsTls,
				ConnectedAt:     time.Now().Unix(), // TODO: Get actual connection time
			}
			connections = append(connections, connection)
		}
	}

	return &corev1.ListConnectionsResponse{
		Connections: connections,
	}, nil
}

// === Information and Status ===

// ShowMesh shows the current mesh information
func (s *Server) ShowMesh(ctx context.Context, req *corev1.ShowMeshRequest) (*corev1.ShowMeshResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	// Get meshes for local node
	existingMeshes, err := meshService.GetMeshesByNodeID(ctx, localNode.ID)
	if err != nil || len(existingMeshes) == 0 {
		return nil, status.Error(codes.NotFound, "node is not part of any mesh")
	}

	// Return the first mesh (assuming single mesh membership for now)
	mesh := existingMeshes[0]
	protoMesh := s.meshToProto(mesh)

	return &corev1.ShowMeshResponse{
		Mesh: protoMesh,
	}, nil
}

// ListNodes lists all nodes in the current mesh
func (s *Server) ListNodes(ctx context.Context, req *corev1.ListNodesRequest) (*corev1.ListNodesResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	// Get meshes for local node
	existingMeshes, err := meshService.GetMeshesByNodeID(ctx, localNode.ID)
	if err != nil || len(existingMeshes) == 0 {
		return nil, status.Error(codes.NotFound, "node is not part of any mesh")
	}

	// Get nodes for the first mesh
	mesh := existingMeshes[0]
	nodes, err := meshService.GetNodes(ctx, mesh.ID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get nodes: %v", err)
	}

	// Convert to protobuf format
	var protoNodes []*corev1.Node
	for _, node := range nodes {
		protoNode := s.nodeToProtoNew(node)
		protoNodes = append(protoNodes, protoNode)
	}

	return &corev1.ListNodesResponse{
		Nodes: protoNodes,
	}, nil
}

// ShowNode shows information about a specific node
func (s *Server) ShowNode(ctx context.Context, req *corev1.ShowNodeRequest) (*corev1.ShowNodeResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)

	var node *meshsvc.Node
	var err error

	if req.NodeId != nil && *req.NodeId != 0 {
		// Show specific node
		nodeIDStr := strconv.FormatUint(*req.NodeId, 10)
		node, err = meshService.GetNodeByID(ctx, nodeIDStr)
	} else {
		// Show local node
		node, err = meshService.GetLocalNode(ctx)
	}

	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get node: %v", err)
	}

	protoNode := s.nodeToProtoNew(node)

	return &corev1.ShowNodeResponse{
		Node: protoNode,
	}, nil
}

// GetNodeStatus gets the status of the current node
func (s *Server) GetNodeStatus(ctx context.Context, req *corev1.GetNodeStatusRequest) (*corev1.GetNodeStatusResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	protoNode := s.nodeToProtoNew(localNode)

	// Get connections
	connectionsResp, err := s.ListConnections(ctx, &corev1.ListConnectionsRequest{})
	if err != nil {
		s.engine.logger.Warnf("Failed to get connections: %v", err)
		connectionsResp = &corev1.ListConnectionsResponse{Connections: []*corev1.Connection{}}
	}

	// Get mesh information if node is in a mesh
	var protoMesh *corev1.Mesh
	existingMeshes, err := meshService.GetMeshesByNodeID(ctx, localNode.ID)
	if err == nil && len(existingMeshes) > 0 {
		mesh := existingMeshes[0]
		protoMesh = s.meshToProtoNew(mesh)
	}

	return &corev1.GetNodeStatusResponse{
		Node:        protoNode,
		Connections: connectionsResp.Connections,
		Mesh:        protoMesh,
	}, nil
}

// === Helper Methods ===

// meshToProtoNew converts a mesh service object to new protobuf format
func (s *Server) meshToProtoNew(mesh *meshsvc.Mesh) *corev1.Mesh {
	return &corev1.Mesh{
		MeshId:          mesh.ID,
		MeshName:        mesh.Name,
		MeshDescription: mesh.Description,
		AllowJoin:       mesh.AllowJoin,
		NodeCount:       mesh.NodeCount,
		ConnectionCount: 0, // TODO: Calculate actual connection count
		Status:          s.convertStatus(mesh.Status),
		CreatedAt:       mesh.Created.Unix(),
		UpdatedAt:       mesh.Updated.Unix(),
	}
}

// nodeToProtoNew converts a node service object to new protobuf format
func (s *Server) nodeToProtoNew(node *meshsvc.Node) *corev1.Node {
	// Convert string node ID to uint64
	nodeID, _ := strconv.ParseUint(node.ID, 10, 64)

	// Map status to NodeStatus enum
	var nodeStatus corev1.NodeStatus
	switch node.Status {
	case "STATUS_CLEAN":
		nodeStatus = corev1.NodeStatus_NODE_STATUS_CLEAN
	case "STATUS_JOINING":
		nodeStatus = corev1.NodeStatus_NODE_STATUS_JOINING
	case "STATUS_ACTIVE":
		nodeStatus = corev1.NodeStatus_NODE_STATUS_ACTIVE
	case "STATUS_LEAVING":
		nodeStatus = corev1.NodeStatus_NODE_STATUS_LEAVING
	case "STATUS_OFFLINE":
		nodeStatus = corev1.NodeStatus_NODE_STATUS_OFFLINE
	default:
		nodeStatus = corev1.NodeStatus_NODE_STATUS_UNSPECIFIED
	}

	protoNode := &corev1.Node{
		NodeId:          strconv.FormatUint(nodeID, 10),
		NodeName:        node.Name,
		NodeDescription: node.Description,
		NodePlatform:    node.Platform,
		NodeVersion:     node.Version,
		RegionId:        node.RegionID,
		RegionName:      node.RegionName,
		IpAddress:       node.IPAddress,
		Port:            node.Port,
		NodeStatus:      nodeStatus,
		CreatedAt:       node.Created.Unix(),
		UpdatedAt:       node.Updated.Unix(),
	}

	// TODO: Set mesh_id if node is part of a mesh

	return protoNode
}

// convertStatus converts a string status to protobuf Status
func (s *Server) convertStatus(status string) commonv1.Status {
	switch status {
	case "STATUS_CREATED":
		return commonv1.Status_STATUS_CREATED
	case "STATUS_ACTIVE":
		return commonv1.Status_STATUS_STARTED
	case "STATUS_PENDING":
		return commonv1.Status_STATUS_PENDING
	case "STATUS_ERROR":
		return commonv1.Status_STATUS_ERROR
	default:
		return commonv1.Status_STATUS_UNKNOWN
	}
}
