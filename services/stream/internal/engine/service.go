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
	grpcServer *grpc.Server
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

	// Set restart keys specific to Stream service
	cfg.SetRestartKeys([]string{
		"services.stream.grpc_port",
		"services.stream.timeout",
		"common.node_id",
		"services.supervisor.service_locations.core",
		"services.supervisor.service_locations.unifiedmodel",
	})

	// Initialize the Stream engine
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
	return s.engine.Stop(ctx)
}

func (s *Service) CheckHealth(ctx context.Context) error {
	if err := s.engine.CheckHealth(); err != nil {
		return err
	}
	return nil
}

func (s *Service) SetLogger(l *logger.Logger) {
	s.logger = l
	if s.engine != nil {
		s.engine.SetLogger(l)
	}
}

func (s *Service) GetCapabilities() *supervisorv1.ServiceCapabilities {
	return &supervisorv1.ServiceCapabilities{}
}

func (s *Service) CollectMetrics() map[string]int64 {
	metrics := make(map[string]int64)

	if s.engine != nil {
		metrics["requests_processed"] = s.engine.metrics.requestsProcessed
		metrics["errors"] = s.engine.metrics.errors
		metrics["ongoing_operations"] = int64(s.engine.state.ongoingOperations)
	}

	return metrics
}

func (s *Service) HealthChecks() map[string]health.CheckFunc {
	checks := make(map[string]health.CheckFunc)

	checks["engine"] = func() error {
		if s.engine == nil {
			return fmt.Errorf("engine not initialized")
		}
		return s.engine.CheckHealth()
	}

	return checks
}
