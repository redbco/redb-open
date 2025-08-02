package grpc

import (
	"fmt"
	"net"

	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/mesh"
	"google.golang.org/grpc"
)

// Config holds the gRPC server configuration
type Config struct {
	Address string
}

// Server represents the gRPC server
type Server struct {
	config Config
	node   *mesh.Node
	logger *logger.Logger
	server *grpc.Server
}

// NewServer creates a new gRPC server
func NewServer(cfg Config, node *mesh.Node, logger *logger.Logger) (*Server, error) {
	server := grpc.NewServer()

	// Register all services
	meshService := NewMeshService(node, logger)
	consensusService := NewConsensusService(logger)

	meshv1.RegisterMeshServiceServer(server, meshService)
	meshv1.RegisterConsensusServiceServer(server, consensusService)

	return &Server{
		config: cfg,
		node:   node,
		logger: logger,
		server: server,
	}, nil
}

// Start starts the gRPC server
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	s.logger.Infof("Starting gRPC server (address: %s)", s.config.Address)

	return s.server.Serve(lis)
}

// Shutdown gracefully shuts down the gRPC server
func (s *Server) Shutdown() error {
	s.logger.Info("Shutting down gRPC server")
	s.server.GracefulStop()
	return nil
}
