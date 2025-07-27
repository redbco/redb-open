package engine

import (
	"context"
	"fmt"
	"time"

	supervisorv1 "github.com/redbco/redb-open/api/proto/supervisor/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/health"
	"github.com/redbco/redb-open/pkg/logger"
	"google.golang.org/grpc"
)

type Service struct {
	engine     *Engine
	config     *config.Config
	grpcServer *grpc.Server // Store the gRPC server until engine is created
	standalone bool
	logger     *logger.Logger
}

func NewService(standalone bool) *Service {
	return &Service{
		standalone: standalone,
	}
}

// SetLogger implements the service.LoggerAware interface
func (s *Service) SetLogger(logger *logger.Logger) {
	s.logger = logger
	if s.engine != nil {
		s.engine.SetLogger(logger)
	}
}

// SetGRPCServer sets the gRPC server for the service
func (s *Service) SetGRPCServer(server *grpc.Server) {
	s.grpcServer = server

	// If engine already exists, set it immediately
	if s.engine != nil {
		s.engine.SetGRPCServer(server)
	}
}

func (s *Service) Initialize(ctx context.Context, cfg *config.Config) error {
	s.config = cfg

	// Set restart keys specific to Mesh service
	cfg.SetRestartKeys([]string{
		"services.mesh.grpc_port",
		"services.mesh.timeout",
		"services.mesh.mesh_id",
		"services.mesh.node_id",
		"services.mesh.storage.type",
		"services.mesh.storage.postgres_url",
		// Add other configuration keys that require service restart
	})

	// Initialize the Mesh engine
	s.engine = NewEngine(cfg, s.standalone)

	// Pass the logger to the engine if available
	if s.logger != nil {
		s.engine.SetLogger(s.logger)
	}

	// Set the gRPC server if it was provided earlier
	if s.grpcServer != nil {
		s.engine.SetGRPCServer(s.grpcServer)
	}

	return nil
}

func (s *Service) Start(ctx context.Context) error {
	return s.engine.Start(ctx)
}

func (s *Service) Stop(ctx context.Context, gracePeriod time.Duration) error {
	if s.engine != nil {
		return s.engine.Stop(ctx)
	}
	return nil
}

func (s *Service) GetCapabilities() *supervisorv1.ServiceCapabilities {
	return &supervisorv1.ServiceCapabilities{
		SupportsHotReload:        true,
		SupportsGracefulShutdown: true,
		Dependencies: []string{
			"supervisor",
			"database",
		},
		RequiredConfig: map[string]string{
			"services.mesh.grpc_port":            "gRPC port for the Mesh service",
			"services.mesh.timeout":              "Request timeout in seconds",
			"services.mesh.mesh_id":              "Mesh identifier",
			"services.mesh.node_id":              "Node identifier",
			"services.mesh.storage.type":         "Storage type (postgres, memory)",
			"services.mesh.storage.postgres_url": "PostgreSQL connection URL for storage",
		},
	}
}

func (s *Service) CollectMetrics() map[string]int64 {
	if s.engine == nil {
		return nil
	}
	return s.engine.GetMetrics()
}

func (s *Service) HealthChecks() map[string]health.CheckFunc {
	return map[string]health.CheckFunc{
		"grpc_server":      s.checkGRPCServer,
		"engine":           s.checkEngine,
		"storage":          s.checkStorage,
		"mesh_node":        s.checkMeshNode,
		"consensus_groups": s.checkConsensusGroups,
	}
}

func (s *Service) checkGRPCServer() error {
	if s.engine == nil {
		return fmt.Errorf("service not initialized")
	}
	return s.engine.CheckGRPCServer()
}

func (s *Service) checkEngine() error {
	if s.engine == nil {
		return fmt.Errorf("service not initialized")
	}
	return s.engine.CheckHealth()
}

func (s *Service) checkStorage() error {
	if s.engine == nil {
		return fmt.Errorf("service not initialized")
	}
	return s.engine.CheckStorage()
}

func (s *Service) checkMeshNode() error {
	if s.engine == nil {
		return fmt.Errorf("service not initialized")
	}
	return s.engine.CheckMeshNode()
}

func (s *Service) checkConsensusGroups() error {
	if s.engine == nil {
		return fmt.Errorf("service not initialized")
	}
	return s.engine.CheckConsensusGroups()
}
