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
	grpcServer *grpc.Server // Store the gRPC server for BaseService compatibility
	logger     *logger.Logger
}

func NewService() *Service {
	return &Service{}
}

// SetLogger implements the service.LoggerAware interface
func (s *Service) SetLogger(logger *logger.Logger) {
	s.logger = logger
	if s.engine != nil {
		s.engine.SetLogger(logger)
	}
}

// SetGRPCServer implements the GRPCServerAware interface
func (s *Service) SetGRPCServer(server *grpc.Server) {
	s.grpcServer = server
	// Client API doesn't need gRPC server registration (it's HTTP-only)
	// but we implement this for BaseService compatibility
}

func (s *Service) Initialize(ctx context.Context, cfg *config.Config) error {
	s.config = cfg

	// Set restart keys specific to Client API service
	cfg.SetRestartKeys([]string{
		"services.clientapi.rest_api_port", // REST API port configuration
		"services.clientapi.http_port",     // Legacy HTTP port configuration (for backward compatibility)
		"services.clientapi.timeout",       // Service timeout configuration
		"services.core.grpc_address",       // Core service gRPC address
		// Add other configuration keys that require service restart
	})

	// Initialize the Client API engine
	s.engine = NewEngine(cfg)

	// Pass the logger to the engine if available
	if s.logger != nil {
		s.engine.SetLogger(s.logger)
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
			"core",
		},
		RequiredConfig: map[string]string{
			"services.clientapi.http_port": "HTTP port for the Client API service (fallback)",
			"services.clientapi.timeout":   "Request timeout in seconds",
			"services.core.grpc_address":   "gRPC address of the Core service",
			"EXTERNAL_PORT":                "External HTTP port (environment variable)",
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
		"http_server": s.checkHTTPServer,
		"engine":      s.checkEngine,
	}
}

func (s *Service) checkHTTPServer() error {
	if s.engine == nil {
		return fmt.Errorf("service not initialized")
	}
	return s.engine.CheckGRPCServer() // This actually checks the HTTP server now
}

func (s *Service) checkEngine() error {
	if s.engine == nil {
		return fmt.Errorf("service not initialized")
	}
	return s.engine.CheckHealth()
}
