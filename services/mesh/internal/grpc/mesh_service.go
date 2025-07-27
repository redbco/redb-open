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

// meshService implements the MeshService gRPC service
type meshService struct {
	meshv1.UnimplementedMeshServiceServer
	node   *mesh.Node
	logger *logger.Logger
	mu     sync.RWMutex
}

// NewMeshService creates a new mesh service handler
func NewMeshService(node *mesh.Node, logger *logger.Logger) *meshService {
	return &meshService{
		node:   node,
		logger: logger,
	}
}

// SendMessage implements the SendMessage RPC
func (s *meshService) SendMessage(ctx context.Context, req *meshv1.SendMessageRequest) (*meshv1.SendMessageResponse, error) {
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

// ReceiveMessage implements the ReceiveMessage RPC
func (s *meshService) ReceiveMessage(req *meshv1.ReceiveMessageRequest, stream meshv1.MeshService_ReceiveMessageServer) error {
	// TODO: Implement message receiving through the mesh node
	// For now, just return an error
	return status.Error(codes.Unimplemented, "not implemented")
}

// GetNodeStatus implements the GetNodeStatus RPC
func (s *meshService) GetNodeStatus(ctx context.Context, req *meshv1.GetNodeStatusRequest) (*meshv1.GetNodeStatusResponse, error) {
	conns := s.node.GetConnections()
	connectedNodes := make([]string, 0, len(conns))
	for nodeID := range conns {
		connectedNodes = append(connectedNodes, nodeID)
	}

	return &meshv1.GetNodeStatusResponse{
		NodeId:         s.node.GetID(),
		MeshId:         s.node.GetMeshID(),
		ConnectedNodes: connectedNodes,
		State:          meshv1.NodeState_NODE_STATE_RUNNING, // TODO: Implement proper state tracking
	}, nil
}
