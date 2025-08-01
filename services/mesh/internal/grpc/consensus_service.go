package grpc

import (
	"context"
	"fmt"

	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/consensus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// consensusService implements the ConsensusServiceServer interface
type consensusService struct {
	meshv1.UnimplementedConsensusServiceServer
	groups map[string]*consensus.Group
	logger *logger.Logger
}

// NewConsensusService creates a new consensus service handler
func NewConsensusService(logger *logger.Logger) *consensusService {
	return &consensusService{
		groups: make(map[string]*consensus.Group),
		logger: logger,
	}
}

// CreateGroup creates a new consensus group
func (s *consensusService) CreateGroup(ctx context.Context, req *meshv1.CreateGroupRequest) (*meshv1.CreateGroupResponse, error) {
	if req.GroupId == "" {
		return nil, status.Error(codes.InvalidArgument, "group ID is required")
	}

	if _, exists := s.groups[req.GroupId]; exists {
		return nil, status.Error(codes.AlreadyExists, "group already exists")
	}

	// Create consensus group configuration
	cfg := consensus.Config{
		GroupID:      req.GroupId,
		DataDir:      fmt.Sprintf("/data/consensus/%s", req.GroupId),
		SnapshotPath: fmt.Sprintf("/data/snapshots/%s", req.GroupId),
	}

	// Create the consensus group
	group, err := consensus.NewGroup(cfg, s.logger)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create group: %v", err)
	}

	// Start the group
	if err := group.Start(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to start group: %v", err)
	}

	s.groups[req.GroupId] = group

	return &meshv1.CreateGroupResponse{
		Success: true,
	}, nil
}

// JoinGroup allows a node to join an existing consensus group
func (s *consensusService) JoinGroup(ctx context.Context, req *meshv1.JoinGroupRequest) (*meshv1.JoinGroupResponse, error) {
	if req.GroupId == "" {
		return nil, status.Error(codes.InvalidArgument, "group ID is required")
	}

	group, exists := s.groups[req.GroupId]
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

// LeaveGroup allows a node to leave a consensus group
func (s *consensusService) LeaveGroup(ctx context.Context, req *meshv1.LeaveGroupRequest) (*meshv1.LeaveGroupResponse, error) {
	if req.GroupId == "" {
		return nil, status.Error(codes.InvalidArgument, "group ID is required")
	}

	group, exists := s.groups[req.GroupId]
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

// GetGroupStatus returns the current status of a consensus group
func (s *consensusService) GetGroupStatus(ctx context.Context, req *meshv1.GetGroupStatusRequest) (*meshv1.GetGroupStatusResponse, error) {
	if req.GroupId == "" {
		return nil, status.Error(codes.InvalidArgument, "group ID is required")
	}

	group, exists := s.groups[req.GroupId]
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
