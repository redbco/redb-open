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

	// Set restart keys specific to Anchor service
	cfg.SetRestartKeys([]string{
		"services.anchor.grpc_port",
		"services.anchor.timeout",
		"common.node_id",
		"services.supervisor.service_locations.core",
		"services.supervisor.service_locations.unifiedmodel",
		// Add other configuration keys that require service restart
	})

	// Initialize the Anchor engine
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
	if s.logger != nil {
		s.logger.Info("Received stop command")
	}

	if s.engine != nil {
		if s.logger != nil {
			s.logger.Info("Stopping anchor engine")
		}
		if err := s.engine.Stop(ctx); err != nil {
			if s.logger != nil {
				s.logger.Errorf("Failed to stop anchor engine: %v", err)
			}
			return err
		}
		if s.logger != nil {
			s.logger.Info("Anchor engine stopped successfully")
		}
	}

	if s.logger != nil {
		s.logger.Info("Stop command completed")
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
			"core",
			"unifiedmodel",
		},
		RequiredConfig: map[string]string{
			"services.anchor.grpc_port":                          "gRPC port for the Anchor service",
			"services.anchor.timeout":                            "Request timeout in seconds",
			"common.node_id":                                     "Node identifier UUID",
			"services.supervisor.service_locations.core":         "Core service location",
			"services.supervisor.service_locations.unifiedmodel": "UnifiedModel service location",
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
		"grpc_server":   s.checkGRPCServer,
		"engine":        s.checkEngine,
		"database":      s.checkDatabase,
		"core_service":  s.checkCoreService,
		"unified_model": s.checkUnifiedModelService,
		"watchers":      s.checkWatchers,
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

func (s *Service) checkDatabase() error {
	if s.engine == nil {
		return fmt.Errorf("service not initialized")
	}
	return s.engine.CheckDatabase()
}

func (s *Service) checkCoreService() error {
	if s.engine == nil {
		return fmt.Errorf("service not initialized")
	}
	return s.engine.CheckCoreService()
}

func (s *Service) checkUnifiedModelService() error {
	if s.engine == nil {
		return fmt.Errorf("service not initialized")
	}
	return s.engine.CheckUnifiedModelService()
}

func (s *Service) checkWatchers() error {
	if s.engine == nil {
		return fmt.Errorf("service not initialized")
	}
	return s.engine.CheckWatchers()
}

// SetLogger implements the service.LoggerAware interface
func (s *Service) SetLogger(logger *logger.Logger) {
	s.logger = logger
	if s.engine != nil {
		s.engine.SetLogger(logger)
	}
}
