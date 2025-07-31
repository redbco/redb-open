package engine

import (
	"context"
	"fmt"
	"time"

	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	"github.com/redbco/redb-open/services/mesh/internal/consensus"
	"github.com/redbco/redb-open/services/mesh/internal/mesh"
	"github.com/redbco/redb-open/services/mesh/internal/security"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	meshv1.UnimplementedMeshServiceServer
	meshv1.UnimplementedManagementServiceServer
	meshv1.UnimplementedConsensusServiceServer
	engine *Engine
}

func NewServer(engine *Engine) *Server {
	return &Server{
		engine: engine,
	}
}

// Helper method to track operations
func (s *Server) trackOperation() func() {
	s.engine.TrackOperation()
	return s.engine.UntrackOperation
}

// MeshService methods

func (s *Server) SendMessage(ctx context.Context, req *meshv1.SendMessageRequest) (*meshv1.SendMessageResponse, error) {
	defer s.trackOperation()()

	if req.ToNodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "to_node_id is required")
	}

	if len(req.Content) == 0 {
		return nil, status.Error(codes.InvalidArgument, "content is required")
	}

	// TODO: Implement message sending through the mesh node
	// For now, return a success response
	return &meshv1.SendMessageResponse{
		MessageId: "msg-001", // TODO: Generate unique message ID
		Success:   true,
	}, nil
}

func (s *Server) GetNodeStatus(ctx context.Context, req *meshv1.GetNodeStatusRequest) (*meshv1.GetNodeStatusResponse, error) {
	defer s.trackOperation()()

	node := s.engine.GetMeshNode()
	if node == nil {
		return nil, status.Error(codes.Internal, "mesh node not initialized")
	}

	conns := node.GetConnections()
	connectedNodes := make([]string, 0, len(conns))
	for nodeID := range conns {
		connectedNodes = append(connectedNodes, nodeID)
	}

	return &meshv1.GetNodeStatusResponse{
		NodeId:         node.GetID(),
		MeshId:         node.GetMeshID(),
		ConnectedNodes: connectedNodes,
		State:          meshv1.NodeState_NODE_STATE_RUNNING, // TODO: Implement proper state tracking
	}, nil
}

// ManagementService methods

func (s *Server) SeedMesh(ctx context.Context, req *meshv1.SeedMeshRequest) (*meshv1.SeedMeshResponse, error) {
	defer s.trackOperation()()

	if req.MeshId == "" {
		return nil, status.Error(codes.InvalidArgument, "mesh_id is required")
	}

	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	logger := s.engine.GetLogger()
	storage := s.engine.GetStorage()

	if storage == nil {
		return &meshv1.SeedMeshResponse{
			Success: false,
			Error:   "storage not available",
		}, nil
	}

	logger.Infof("Seeding mesh runtime for mesh %s with node %s", req.MeshId, req.NodeId)

	// Check if mesh already exists in persistent tables
	existingMesh, err := storage.GetMeshInfo(ctx)
	if err == nil && existingMesh != nil {
		return &meshv1.SeedMeshResponse{
			Success: false,
			Error:   "mesh already exists on this node",
		}, nil
	}

	// Verify the mesh and node exist in persistent tables (should be created by core service)
	meshInfo, err := storage.GetMeshInfo(ctx)
	if err != nil || meshInfo == nil {
		return &meshv1.SeedMeshResponse{
			Success: false,
			Error:   "mesh not found in persistent storage - must be created via core service first",
		}, nil
	}

	nodeInfo, err := storage.GetNodeInfo(ctx, req.NodeId)
	if err != nil || nodeInfo == nil {
		return &meshv1.SeedMeshResponse{
			Success: false,
			Error:   "node not found in persistent storage - must be created via core service first",
		}, nil
	}

	// Create credential manager
	credManager := security.NewCredentialManager(storage, logger)

	// Generate mesh credentials
	_, err = credManager.GenerateMeshCredentials(req.MeshId, req.NodeId)
	if err != nil {
		logger.Errorf("Failed to generate mesh credentials: %v", err)
		return &meshv1.SeedMeshResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to generate credentials: %v", err),
		}, nil
	}

	// Store runtime metadata only (not administrative data)
	runtimeMeta := map[string]interface{}{
		"initialization_time": time.Now(),
		"seed_node":          true,
		"runtime_state":      "initializing",
	}

	if err := storage.SaveConfig(ctx, "mesh_runtime_metadata", runtimeMeta); err != nil {
		logger.Errorf("Failed to store runtime metadata: %v", err)
		return &meshv1.SeedMeshResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to store runtime metadata: %v", err),
		}, nil
	}

	// Store local identity for this node
	localIdentity := map[string]interface{}{
		"identity_id": req.NodeId,
	}

	if err := storage.SaveConfig(ctx, "local_identity", localIdentity); err != nil {
		logger.Errorf("Failed to store local identity: %v", err)
		return &meshv1.SeedMeshResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to store local identity: %v", err),
		}, nil
	}

	// Generate mesh token for other nodes to join (simplified implementation)
	token := fmt.Sprintf("mesh-token-%s-%d", req.MeshId, time.Now().Unix())

	// Initialize mesh node with credentials
	meshConfig := mesh.Config{
		NodeID:        req.NodeId,
		MeshID:        req.MeshId,
		ListenAddress: ":8443", // Default WebSocket port
		Heartbeat:     30 * time.Second,
		Timeout:       60 * time.Second,
	}

	meshNode, err := mesh.NewNode(meshConfig, storage, logger)
	if err != nil {
		logger.Errorf("Failed to create mesh node: %v", err)
		return &meshv1.SeedMeshResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to create mesh node: %v", err),
		}, nil
	}

	// Start the mesh node
	if err := meshNode.Start(); err != nil {
		logger.Errorf("Failed to start mesh node: %v", err)
		return &meshv1.SeedMeshResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to start mesh node: %v", err),
		}, nil
	}

	// Update engine state
	s.engine.meshNode = meshNode
	s.engine.state.Lock()
	s.engine.state.initState = StateFullyConfigured
	s.engine.state.Unlock()

	logger.Infof("Successfully seeded mesh runtime for %s with node %s", req.MeshId, req.NodeId)

	return &meshv1.SeedMeshResponse{
		Success: true,
		Token:   token,
	}, nil
}

func (s *Server) JoinMesh(ctx context.Context, req *meshv1.JoinMeshRequest) (*meshv1.JoinMeshResponse, error) {
	defer s.trackOperation()()

	if req.MeshId == "" {
		return nil, status.Error(codes.InvalidArgument, "mesh_id is required")
	}

	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	if req.Token == "" {
		return nil, status.Error(codes.InvalidArgument, "token is required")
	}

	if len(req.PeerAddresses) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one peer address is required")
	}

	logger := s.engine.GetLogger()
	storage := s.engine.GetStorage()

	if storage == nil {
		return &meshv1.JoinMeshResponse{
			Success: false,
			Error:   "storage not available",
		}, nil
	}

	logger.Infof("Joining mesh runtime for mesh %s with node %s", req.MeshId, req.NodeId)

	// Check if already part of a mesh
	existingMesh, err := storage.GetMeshInfo(ctx)
	if err == nil && existingMesh != nil {
		return &meshv1.JoinMeshResponse{
			Success: false,
			Error:   "node is already part of a mesh",
		}, nil
	}

	// Verify the mesh and node exist in persistent tables (should be created by core service)
	meshInfo, err := storage.GetMeshInfo(ctx)
	if err != nil || meshInfo == nil {
		return &meshv1.JoinMeshResponse{
			Success: false,
			Error:   "mesh not found in persistent storage - must be created via core service first",
		}, nil
	}

	nodeInfo, err := storage.GetNodeInfo(ctx, req.NodeId)
	if err != nil || nodeInfo == nil {
		return &meshv1.JoinMeshResponse{
			Success: false,
			Error:   "node not found in persistent storage - must be created via core service first",
		}, nil
	}

	// Create credential manager
	credManager := security.NewCredentialManager(storage, logger)

	// Validate mesh token (simplified implementation)
	expectedTokenPrefix := fmt.Sprintf("mesh-token-%s", req.MeshId)
	if len(req.Token) < len(expectedTokenPrefix) || req.Token[:len(expectedTokenPrefix)] != expectedTokenPrefix {
		logger.Errorf("Invalid mesh token: %s", req.Token)
		return &meshv1.JoinMeshResponse{
			Success: false,
			Error:   "invalid mesh token",
		}, nil
	}

	// Generate join credentials
	_, err = credManager.GenerateJoinCredentials(req.MeshId, req.NodeId, req.Token)
	if err != nil {
		logger.Errorf("Failed to generate join credentials: %v", err)
		return &meshv1.JoinMeshResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to generate credentials: %v", err),
		}, nil
	}

	// Store runtime metadata only (not administrative data)
	runtimeMeta := map[string]interface{}{
		"initialization_time": time.Now(),
		"seed_node":          false,
		"runtime_state":      "joining",
		"peer_addresses":     req.PeerAddresses,
	}

	if err := storage.SaveConfig(ctx, "mesh_runtime_metadata", runtimeMeta); err != nil {
		logger.Errorf("Failed to store runtime metadata: %v", err)
		return &meshv1.JoinMeshResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to store runtime metadata: %v", err),
		}, nil
	}

	// Store local identity for this node
	localIdentity := map[string]interface{}{
		"identity_id": req.NodeId,
	}

	if err := storage.SaveConfig(ctx, "local_identity", localIdentity); err != nil {
		logger.Errorf("Failed to store local identity: %v", err)
		return &meshv1.JoinMeshResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to store local identity: %v", err),
		}, nil
	}

	// Initialize mesh node with credentials
	meshConfig := mesh.Config{
		NodeID:        req.NodeId,
		MeshID:        req.MeshId,
		ListenAddress: ":8443", // Default WebSocket port
		Heartbeat:     30 * time.Second,
		Timeout:       60 * time.Second,
	}

	meshNode, err := mesh.NewNode(meshConfig, storage, logger)
	if err != nil {
		logger.Errorf("Failed to create mesh node: %v", err)
		return &meshv1.JoinMeshResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to create mesh node: %v", err),
		}, nil
	}

	// Start the mesh node
	if err := meshNode.Start(); err != nil {
		logger.Errorf("Failed to start mesh node: %v", err)
		return &meshv1.JoinMeshResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to start mesh node: %v", err),
		}, nil
	}

	// Connect to peer nodes
	for _, peerAddr := range req.PeerAddresses {
		// Extract node ID from address (simplified - in production would be more robust)
		peerID := fmt.Sprintf("peer-%s", peerAddr)

		if err := meshNode.AddConnection(peerID); err != nil {
			logger.Warnf("Failed to add connection to peer %s: %v", peerAddr, err)
			// Continue with other peers
		} else {
			logger.Infof("Added connection to peer %s at %s", peerID, peerAddr)
		}
	}

	// Update engine state
	s.engine.meshNode = meshNode
	s.engine.state.Lock()
	s.engine.state.initState = StateFullyConfigured
	s.engine.state.Unlock()

	logger.Infof("Successfully joined mesh runtime for %s with node %s", req.MeshId, req.NodeId)

	return &meshv1.JoinMeshResponse{
		Success: true,
	}, nil
}

func (s *Server) AddConnection(ctx context.Context, req *meshv1.AddConnectionRequest) (*meshv1.AddConnectionResponse, error) {
	defer s.trackOperation()()

	if req.PeerId == "" {
		return nil, status.Error(codes.InvalidArgument, "peer_id is required")
	}

	if req.Address == "" {
		return nil, status.Error(codes.InvalidArgument, "address is required")
	}

	node := s.engine.GetMeshNode()
	if node == nil {
		return &meshv1.AddConnectionResponse{
			Success: false,
			Error:   "mesh node not initialized",
		}, nil
	}

	err := node.AddConnection(req.PeerId)
	if err != nil {
		return &meshv1.AddConnectionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &meshv1.AddConnectionResponse{
		Success: true,
	}, nil
}

func (s *Server) RemoveConnection(ctx context.Context, req *meshv1.RemoveConnectionRequest) (*meshv1.RemoveConnectionResponse, error) {
	defer s.trackOperation()()

	if req.PeerId == "" {
		return nil, status.Error(codes.InvalidArgument, "peer_id is required")
	}

	node := s.engine.GetMeshNode()
	if node == nil {
		return &meshv1.RemoveConnectionResponse{
			Success: false,
			Error:   "mesh node not initialized",
		}, nil
	}

	err := node.RemoveConnection(req.PeerId)
	if err != nil {
		return &meshv1.RemoveConnectionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &meshv1.RemoveConnectionResponse{
		Success: true,
	}, nil
}

func (s *Server) ListConnections(ctx context.Context, req *meshv1.ListConnectionsRequest) (*meshv1.ListConnectionsResponse, error) {
	defer s.trackOperation()()

	node := s.engine.GetMeshNode()
	if node == nil {
		return nil, status.Error(codes.Internal, "mesh node not initialized")
	}

	conns := node.GetConnections()
	connections := make([]*meshv1.Connection, 0, len(conns))

	for peerID, conn := range conns {
		connections = append(connections, &meshv1.Connection{
			PeerId:   peerID,
			Status:   conn.Status,
			LastSeen: 0, // TODO: Implement last seen tracking
		})
	}

	return &meshv1.ListConnectionsResponse{
		Connections: connections,
	}, nil
}

// ConsensusService methods

func (s *Server) CreateGroup(ctx context.Context, req *meshv1.CreateGroupRequest) (*meshv1.CreateGroupResponse, error) {
	defer s.trackOperation()()

	if req.GroupId == "" {
		return nil, status.Error(codes.InvalidArgument, "group ID is required")
	}

	// Check if group already exists
	if _, exists := s.engine.GetConsensusGroup(req.GroupId); exists {
		return nil, status.Error(codes.AlreadyExists, "group already exists")
	}

	// Create consensus group configuration
	cfg := consensus.Config{
		GroupID:      req.GroupId,
		DataDir:      fmt.Sprintf("/data/consensus/%s", req.GroupId),
		SnapshotPath: fmt.Sprintf("/data/snapshots/%s", req.GroupId),
	}

	logger := s.engine.GetLogger()
	if logger == nil {
		return nil, status.Error(codes.Internal, "logger not available")
	}

	// Create the consensus group
	group, err := consensus.NewGroup(cfg, logger)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create group: %v", err)
	}

	// Start the group
	if err := group.Start(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to start group: %v", err)
	}

	// Add to engine's group management
	s.engine.AddConsensusGroup(req.GroupId, group)

	return &meshv1.CreateGroupResponse{
		Success: true,
	}, nil
}

func (s *Server) JoinGroup(ctx context.Context, req *meshv1.JoinGroupRequest) (*meshv1.JoinGroupResponse, error) {
	defer s.trackOperation()()

	if req.GroupId == "" {
		return nil, status.Error(codes.InvalidArgument, "group ID is required")
	}

	group, exists := s.engine.GetConsensusGroup(req.GroupId)
	if !exists {
		return nil, status.Error(codes.NotFound, "group not found")
	}

	// Add the new peer to the group
	if err := group.AddPeer(req.NodeId, ""); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to add peer: %v", err)
	}

	return &meshv1.JoinGroupResponse{
		Success: true,
	}, nil
}

func (s *Server) LeaveGroup(ctx context.Context, req *meshv1.LeaveGroupRequest) (*meshv1.LeaveGroupResponse, error) {
	defer s.trackOperation()()

	if req.GroupId == "" {
		return nil, status.Error(codes.InvalidArgument, "group ID is required")
	}

	group, exists := s.engine.GetConsensusGroup(req.GroupId)
	if !exists {
		return nil, status.Error(codes.NotFound, "group not found")
	}

	// Remove the peer from the group
	if err := group.RemovePeer(req.NodeId); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to remove peer: %v", err)
	}

	return &meshv1.LeaveGroupResponse{
		Success: true,
	}, nil
}

func (s *Server) GetGroupStatus(ctx context.Context, req *meshv1.GetGroupStatusRequest) (*meshv1.GetGroupStatusResponse, error) {
	defer s.trackOperation()()

	if req.GroupId == "" {
		return nil, status.Error(codes.InvalidArgument, "group ID is required")
	}

	group, exists := s.engine.GetConsensusGroup(req.GroupId)
	if !exists {
		return nil, status.Error(codes.NotFound, "group not found")
	}

	state := group.GetState()
	leader := group.GetLeader()
	term := group.GetTerm()

	return &meshv1.GetGroupStatusResponse{
		GroupId:  req.GroupId,
		LeaderId: leader,
		State:    meshv1.GroupState(state),
		Term:     int64(term),
	}, nil
}
