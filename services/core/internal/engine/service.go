package engine

import (
	"context"
	"fmt"
	"time"

	supervisorv1 "github.com/redbco/redb-open/api/proto/supervisor/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/health"
	"github.com/redbco/redb-open/pkg/logger"
	"google.golang.org/grpc"
)

type Service struct {
	engine     *Engine
	config     *config.Config
	grpcServer *grpc.Server // Store the gRPC server until engine is created
	logger     *logger.Logger
	db         *database.PostgreSQL
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

	// Set restart keys specific to Core service
	cfg.SetRestartKeys([]string{
		"services.core.grpc_port",
		"services.core.timeout",
		"services.core.database_url",
		// Add other configuration keys that require service restart
	})

	// Initialize database connection
	dbConfig := database.FromGlobalConfig(cfg)
	db, err := database.New(ctx, dbConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}
	s.db = db

	// Initialize the Core engine
	s.engine = NewEngine(cfg)

	// Set database connection on the engine
	s.engine.SetDatabase(s.db)

	// Pass the logger to the engine if available
	if s.logger != nil {
		s.engine.SetLogger(s.logger)
	}

	// Set the gRPC server on the engine if we have one stored
	if s.grpcServer != nil {
		s.engine.SetGRPCServer(s.grpcServer)
	}

	return nil
}

func (s *Service) Start(ctx context.Context) error {
	// Start the engine
	if s.engine != nil {
		if err := s.engine.Start(ctx); err != nil {
			return fmt.Errorf("failed to start core engine: %w", err)
		}
		if s.logger != nil {
			s.logger.Infof("Core engine started successfully")
		}
	}
	return nil
}

func (s *Service) Stop(ctx context.Context, gracePeriod time.Duration) error {
	if s.engine != nil {
		if err := s.engine.Stop(ctx); err != nil {
			return err
		}
	}

	// Close database connection
	if s.db != nil {
		s.db.Close()
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
			"services.core.grpc_port":    "gRPC port for the Core service",
			"services.core.timeout":      "Request timeout in seconds",
			"services.core.database_url": "Database connection URL",
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
		"database":    s.checkDatabase,
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
	if s.db == nil {
		return fmt.Errorf("database not initialized")
	}

	// Test database connection with a simple ping
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return s.db.Pool().Ping(ctx)
}
