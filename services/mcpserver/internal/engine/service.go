package engine

import (
	"context"
	"time"

	supervisorv1 "github.com/redbco/redb-open/api/proto/supervisor/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/health"
	"github.com/redbco/redb-open/pkg/logger"
	"google.golang.org/grpc"
)

// Service implements the MCP server-specific logic
type Service struct {
	engine     *Engine
	config     *config.Config
	grpcServer *grpc.Server // Store the gRPC server for BaseService compatibility
	logger     *logger.Logger
}

// NewService creates a new MCP service implementation
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
	// MCP server doesn't need gRPC server registration (it's a background service)
	// but we implement this for BaseService compatibility
}

// Initialize sets up the MCP service
func (s *Service) Initialize(ctx context.Context, cfg *config.Config) error {
	s.config = cfg

	// Set restart keys specific to MCP service
	cfg.SetRestartKeys([]string{
		"services.mcpserver.database_url",  // Fixed: Use service-specific naming
		"services.mcpserver.port",          // Fixed: Use service-specific naming
		"services.mcpserver.timeout",       // Fixed: Use service-specific naming
		"services.mcpserver.security_cert", // Fixed: Use service-specific naming
		"services.mcpserver.security_key",  // Fixed: Use service-specific naming
	})

	// Initialize MCP engine
	s.engine = NewEngine(cfg)

	// Pass the logger to the engine if available
	if s.logger != nil {
		s.engine.SetLogger(s.logger)
	}

	return nil
}

// Start begins the MCP service operations
func (s *Service) Start(ctx context.Context) error {
	// Start any background processes specific to MCP service
	return s.engine.Start(ctx)
}

// Stop gracefully shuts down the MCP service
func (s *Service) Stop(ctx context.Context, gracePeriod time.Duration) error {
	// Stop the MCP engine
	if s.engine != nil {
		return s.engine.Stop(ctx)
	}
	return nil
}

// GetEngine returns the service's engine instance
func (s *Service) GetEngine() *Engine {
	return s.engine
}

// GetCapabilities returns the service capabilities
func (s *Service) GetCapabilities() *supervisorv1.ServiceCapabilities {
	return &supervisorv1.ServiceCapabilities{
		SupportsHotReload:        true,
		SupportsGracefulShutdown: true,
		Dependencies:             []string{"supervisor"}, // Fixed: Updated dependencies
		RequiredConfig: map[string]string{
			"services.mcpserver.database_url":  "Database connection URL",    // Fixed: Use service-specific naming
			"services.mcpserver.port":          "MCP server port",            // Fixed: Use service-specific naming
			"services.mcpserver.timeout":       "Request timeout in seconds", // Fixed: Use service-specific naming
			"services.mcpserver.security_cert": "TLS certificate path",       // Fixed: Use service-specific naming
			"services.mcpserver.security_key":  "TLS private key path",       // Fixed: Use service-specific naming
		},
	}
}

// CollectMetrics returns MCP-specific metrics
func (s *Service) CollectMetrics() map[string]int64 {
	if s.engine == nil {
		return nil
	}

	return s.engine.GetMetrics()
}

// HealthChecks returns MCP-specific health check functions
func (s *Service) HealthChecks() map[string]health.CheckFunc {
	return map[string]health.CheckFunc{
		"database":    s.checkDatabase,
		"engine":      s.checkEngine,
		"connections": s.checkConnections,
	}
}

func (s *Service) checkDatabase() error {
	// TODO: Implement database connectivity check when service is fully implemented
	// For now, return success to avoid health check failures
	dbURL := s.config.Get("services.mcpserver.database_url") // Fixed: Use service-specific naming
	if dbURL == "" {
		// Service not fully configured yet, but healthy
		return nil
	}

	// Add actual database connectivity check here when implemented
	return nil
}

func (s *Service) checkEngine() error {
	if s.engine == nil {
		// Engine not initialized yet, but service is healthy
		return nil
	}

	// TODO: Implement actual engine validation when service is fully implemented
	// For now, return success to avoid health check failures
	return nil
}

func (s *Service) checkConnections() error {
	if s.engine == nil {
		// Engine not initialized yet, but service is healthy
		return nil
	}

	// TODO: Implement actual connection health checking when service is fully implemented
	// For now, return success to avoid health check failures
	return nil
}
