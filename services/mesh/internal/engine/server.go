package engine

import (
	"context"
	"fmt"

	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	"github.com/redbco/redb-open/services/mesh/internal/consensus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	meshv1.UnimplementedMeshServiceServer
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

func (s *Server) StartMesh(ctx context.Context, req *meshv1.StartMeshRequest) (*meshv1.StartMeshResponse, error) {
	defer s.trackOperation()()

	logger := s.engine.GetLogger()
	storage := s.engine.GetStorage()

	if storage == nil {
		return &meshv1.StartMeshResponse{
			Success: false,
			Error:   "storage not available",
		}, nil
	}

	// Check if mesh is already running
	if s.engine.GetMeshNode() != nil {
		return &meshv1.StartMeshResponse{
			Success: false,
			Error:   "mesh runtime is already started",
		}, nil
	}

	// Get initialization state to determine what to start
	initInfo, err := s.engine.checkInitializationState(ctx)
	if err != nil {
		logger.Errorf("Failed to check initialization state: %v", err)
		return &meshv1.StartMeshResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to check initialization state: %v", err),
		}, nil
	}

	// Check if we have the required configuration
	if initInfo.State != StateFullyConfigured {
		return &meshv1.StartMeshResponse{
			Success: false,
			Error:   fmt.Sprintf("mesh not ready to start: %s", initInfo.ErrorMessage),
		}, nil
	}

	// Start the mesh runtime
	if err := s.engine.startMeshRuntime(ctx, initInfo); err != nil {
		logger.Errorf("Failed to start mesh runtime: %v", err)
		return &meshv1.StartMeshResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to start mesh runtime: %v", err),
		}, nil
	}

	logger.Infof("Successfully started mesh runtime for mesh %s with node %s",
		initInfo.MeshInfo.MeshID, initInfo.NodeInfo.NodeID)

	return &meshv1.StartMeshResponse{
		Success: true,
		MeshId:  initInfo.MeshInfo.MeshID,
		NodeId:  initInfo.NodeInfo.NodeID,
		Status:  "running",
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
