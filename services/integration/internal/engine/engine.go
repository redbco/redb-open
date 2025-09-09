package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	pb "github.com/redbco/redb-open/api/proto/integration/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
	"google.golang.org/grpc"
)

type Engine struct {
	config     *config.Config
	grpcServer *grpc.Server
	logger     *logger.Logger
	db         *database.PostgreSQL
	state      struct {
		sync.Mutex
		isRunning         bool
		ongoingOperations int32
	}
	metrics struct {
		requestsProcessed int64
		errors            int64
	}
	// in-memory store for integrations for now
	store *MemoryStore
}

func NewEngine(cfg *config.Config) *Engine {
	return &Engine{
		config: cfg,
		store:  NewMemoryStore(),
	}
}

func (e *Engine) SetLogger(logger *logger.Logger) { e.logger = logger }

func (e *Engine) SetDatabase(db *database.PostgreSQL) { e.db = db }

// SetGRPCServer sets the shared gRPC server and registers the service immediately
func (e *Engine) SetGRPCServer(server *grpc.Server) {
	e.grpcServer = server
	if e.grpcServer != nil {
		serviceServer := NewIntegrationServer(e)
		pb.RegisterIntegrationServiceServer(e.grpcServer, serviceServer)
	}
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
	e.state.isRunning = true
	return nil
}

func (e *Engine) Stop(ctx context.Context) error {
	e.state.Lock()
	defer e.state.Unlock()
	if !e.state.isRunning {
		return nil
	}
	e.state.isRunning = false
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

func (e *Engine) TrackOperation()   { atomic.AddInt32(&e.state.ongoingOperations, 1) }
func (e *Engine) UntrackOperation() { atomic.AddInt32(&e.state.ongoingOperations, -1) }
