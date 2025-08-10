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
	grpcServer *grpc.Server
	logger     *logger.Logger
	db         *database.PostgreSQL
}

func NewService() *Service { return &Service{} }

func (s *Service) SetLogger(logger *logger.Logger) {
	s.logger = logger
	if s.engine != nil {
		s.engine.SetLogger(logger)
	}
}

func (s *Service) SetGRPCServer(server *grpc.Server) {
	s.grpcServer = server
	if s.engine != nil {
		s.engine.SetGRPCServer(server)
	}
}

func (s *Service) Initialize(ctx context.Context, cfg *config.Config) error {
	s.config = cfg

	cfg.SetRestartKeys([]string{
		"services.integration.grpc_port",
		"services.integration.timeout",
	})

	// Initialize the database connection (similar to core)
	dbConfig := database.FromGlobalConfig(cfg)
	db, err := database.New(ctx, dbConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}
	s.db = db

	s.engine = NewEngine(cfg)
	s.engine.SetDatabase(db)

	if s.logger != nil {
		s.engine.SetLogger(s.logger)
	}
	if s.grpcServer != nil {
		s.engine.SetGRPCServer(s.grpcServer)
	}

	return nil
}

func (s *Service) Start(ctx context.Context) error { return s.engine.Start(ctx) }

func (s *Service) Stop(ctx context.Context, gracePeriod time.Duration) error {
	if s.engine != nil {
		if err := s.engine.Stop(ctx); err != nil {
			return err
		}
	}
	if s.db != nil {
		s.db.Close()
	}
	return nil
}

func (s *Service) GetCapabilities() *supervisorv1.ServiceCapabilities {
	return &supervisorv1.ServiceCapabilities{
		SupportsHotReload:        true,
		SupportsGracefulShutdown: true,
		Dependencies:             []string{"supervisor"},
		RequiredConfig: map[string]string{
			"services.integration.grpc_port": "gRPC port for the Integration service",
			"services.integration.timeout":   "Request timeout in seconds",
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.db.Pool().Ping(ctx)
}
