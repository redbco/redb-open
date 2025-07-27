package grpc

import (
	"context"
	"sync"

	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/mesh"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// managementService implements the ManagementService gRPC service
type managementService struct {
	meshv1.UnimplementedManagementServiceServer
	node   *mesh.Node
	logger *logger.Logger
	mu     sync.RWMutex
}

// NewManagementService creates a new management service handler
func NewManagementService(node *mesh.Node, logger *logger.Logger) *managementService {
	return &managementService{
		node:   node,
		logger: logger,
	}
}

// SeedMesh implements the SeedMesh RPC
func (s *managementService) SeedMesh(ctx context.Context, req *meshv1.SeedMeshRequest) (*meshv1.SeedMeshResponse, error) {
	if req.MeshId == "" {
		return nil, status.Error(codes.InvalidArgument, "mesh_id is required")
	}

	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	// TODO: Implement mesh seeding logic
	// For now, return a success response with a placeholder token
	return &meshv1.SeedMeshResponse{
		Success: true,
		Token:   "seed-token-001", // TODO: Generate proper token
	}, nil
}

// JoinMesh implements the JoinMesh RPC
func (s *managementService) JoinMesh(ctx context.Context, req *meshv1.JoinMeshRequest) (*meshv1.JoinMeshResponse, error) {
	if req.MeshId == "" {
		return nil, status.Error(codes.InvalidArgument, "mesh_id is required")
	}

	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	if req.Token == "" {
		return nil, status.Error(codes.InvalidArgument, "token is required")
	}

	// TODO: Implement mesh joining logic
	// For now, return a success response
	return &meshv1.JoinMeshResponse{
		Success: true,
	}, nil
}

// AddConnection implements the AddConnection RPC
func (s *managementService) AddConnection(ctx context.Context, req *meshv1.AddConnectionRequest) (*meshv1.AddConnectionResponse, error) {
	if req.PeerId == "" {
		return nil, status.Error(codes.InvalidArgument, "peer_id is required")
	}

	if req.Address == "" {
		return nil, status.Error(codes.InvalidArgument, "address is required")
	}

	err := s.node.AddConnection(req.PeerId)
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

// RemoveConnection implements the RemoveConnection RPC
func (s *managementService) RemoveConnection(ctx context.Context, req *meshv1.RemoveConnectionRequest) (*meshv1.RemoveConnectionResponse, error) {
	if req.PeerId == "" {
		return nil, status.Error(codes.InvalidArgument, "peer_id is required")
	}

	err := s.node.RemoveConnection(req.PeerId)
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

// ListConnections implements the ListConnections RPC
func (s *managementService) ListConnections(ctx context.Context, req *meshv1.ListConnectionsRequest) (*meshv1.ListConnectionsResponse, error) {
	conns := s.node.GetConnections()
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
