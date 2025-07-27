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
	if s.engine != nil {
		s.engine.SetGRPCServer(server)
	}
}

func (s *Service) Initialize(ctx context.Context, cfg *config.Config) error {
	s.config = cfg

	// Set restart keys specific to webhook service
	cfg.SetRestartKeys([]string{
		"services.webhook.grpc_port",
		"services.webhook.timeout",
		"services.webhook.max_concurrent_webhooks",
		"services.webhook.retry_config",
		// Add other configuration keys that require service restart
	})

	// Initialize the webhook engine
	s.engine = NewEngine(cfg)

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
		},
		RequiredConfig: map[string]string{
			"services.webhook.grpc_port":               "gRPC port for the webhook service",
			"services.webhook.timeout":                 "Request timeout in seconds",
			"services.webhook.max_concurrent_webhooks": "Maximum number of concurrent webhook deliveries",
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
		"grpc_server": s.checkGRPCServer,
		"engine":      s.checkEngine,
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
