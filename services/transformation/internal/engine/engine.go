package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	pb "github.com/redbco/redb-open/api/proto/transformation/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
	"google.golang.org/grpc"
)

type Engine struct {
	config         *config.Config
	grpcServer     *grpc.Server
	logger         *logger.Logger
	db             *database.PostgreSQL
	registry       *TransformationRegistry
	workflowEngine *WorkflowEngine
	state          struct {
		sync.Mutex
		isRunning         bool
		ongoingOperations int32
	}
	metrics struct {
		requestsProcessed int64
		errors            int64
	}
}

func NewEngine(cfg *config.Config) *Engine {
	return &Engine{
		config: cfg,
	}
}

// SetLogger sets the logger for the engine
func (e *Engine) SetLogger(logger *logger.Logger) {
	e.logger = logger
}

// SetGRPCServer sets the shared gRPC server and registers the service immediately
func (e *Engine) SetGRPCServer(server *grpc.Server) {
	e.grpcServer = server

	// Register the service immediately when server is set (BEFORE serving starts)
	if e.grpcServer != nil {
		serviceServer := NewTransformationServer(e)
		pb.RegisterTransformationServiceServer(e.grpcServer, serviceServer)
	}
}

// InitializeDatabase initializes the database connection
func (e *Engine) InitializeDatabase(ctx context.Context) error {
	// Get database configuration
	dbConfig := database.FromGlobalConfig(e.config)

	// Create database connection
	db, err := database.New(ctx, dbConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}

	e.db = db
	e.logger.Info("Database connection initialized")
	return nil
}

// InitializeRegistry initializes the transformation registry
func (e *Engine) InitializeRegistry(ctx context.Context) error {
	if e.db == nil {
		return fmt.Errorf("database not initialized")
	}

	e.registry = NewTransformationRegistry(e.db, e.logger)

	// Register built-in functions
	e.registry.RegisterBuiltIn()

	e.logger.Info("Transformation registry initialized")
	return nil
}

// InitializeWorkflowEngine initializes the workflow engine
func (e *Engine) InitializeWorkflowEngine() error {
	if e.registry == nil {
		return fmt.Errorf("registry not initialized")
	}

	e.workflowEngine = NewWorkflowEngine(e.registry, e.logger)
	e.logger.Info("Workflow engine initialized")
	return nil
}

func (e *Engine) Start(ctx context.Context) error {
	e.state.Lock()
	defer e.state.Unlock()

	if e.state.isRunning {
		return fmt.Errorf("engine is already running")
	}

	if e.grpcServer == nil {
		return fmt.Errorf("gRPC server not set - call SetGRPCServer first")
	}

	// Initialize database connection
	if err := e.InitializeDatabase(ctx); err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}

	// Initialize registry
	if err := e.InitializeRegistry(ctx); err != nil {
		return fmt.Errorf("failed to initialize registry: %w", err)
	}

	// Initialize workflow engine
	if err := e.InitializeWorkflowEngine(); err != nil {
		return fmt.Errorf("failed to initialize workflow engine: %w", err)
	}

	// Service is already registered in SetGRPCServer, just mark as running
	e.state.isRunning = true
	e.logger.Info("Transformation engine started successfully")
	return nil
}

func (e *Engine) Stop(ctx context.Context) error {
	e.state.Lock()
	defer e.state.Unlock()

	if !e.state.isRunning {
		return nil
	}

	// Close database connection
	if e.db != nil {
		e.db.Close()
		e.logger.Info("Database connection closed")
	}

	e.state.isRunning = false
	e.logger.Info("Transformation engine stopped")
	return nil
}

func (e *Engine) GetMetrics() map[string]int64 {
	return map[string]int64{
		"requests_processed": atomic.LoadInt64(&e.metrics.requestsProcessed),
		"errors":             atomic.LoadInt64(&e.metrics.errors),
	}
}

func (e *Engine) CheckGRPCServer() error {
	e.state.Lock()
	defer e.state.Unlock()

	if !e.state.isRunning {
		return fmt.Errorf("service not initialized")
	}

	if e.grpcServer == nil {
		return fmt.Errorf("gRPC server not initialized")
	}

	return nil
}

func (e *Engine) CheckHealth() error {
	e.state.Lock()
	defer e.state.Unlock()

	if !e.state.isRunning {
		return fmt.Errorf("service not initialized")
	}

	return nil
}

func (e *Engine) TrackOperation() {
	atomic.AddInt32(&e.state.ongoingOperations, 1)
}

func (e *Engine) UntrackOperation() {
	atomic.AddInt32(&e.state.ongoingOperations, -1)
}
