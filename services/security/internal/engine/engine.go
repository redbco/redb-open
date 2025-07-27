package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
	"google.golang.org/grpc"
)

type Engine struct {
	config      *config.Config
	grpcServer  *grpc.Server // Reference to the shared gRPC server
	securitySvc *SecurityServer
	logger      *logger.Logger
	db          *database.PostgreSQL
	state       struct {
		sync.Mutex
		isRunning         bool
		ongoingOperations int32
	}
	metrics struct {
		requestsProcessed      int64
		errors                 int64
		loginAttempts          int64
		authenticationRequests int64
		authorizationRequests  int64
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

// SetGRPCServer allows the BaseService to provide the shared gRPC server
func (e *Engine) SetGRPCServer(server *grpc.Server) {
	e.grpcServer = server
}

// SetDatabase sets the database connection for the engine
func (e *Engine) SetDatabase(db *database.PostgreSQL) {
	e.db = db
}

// GetDatabase returns the database connection
func (e *Engine) GetDatabase() *database.PostgreSQL {
	return e.db
}

// RegisterSecurityService registers the SecurityService with the gRPC server
func (e *Engine) RegisterSecurityService() error {
	if e.grpcServer == nil {
		return fmt.Errorf("gRPC server not provided to engine")
	}

	e.securitySvc = NewSecurityServer(e)
	securityv1.RegisterSecurityServiceServer(e.grpcServer, e.securitySvc)
	return nil
}

func (e *Engine) Start(ctx context.Context) error {
	e.state.Lock()
	if e.state.isRunning {
		e.state.Unlock()
		return fmt.Errorf("engine is already running")
	}
	e.state.isRunning = true
	e.state.Unlock()

	// SecurityService should already be registered by this point
	if e.grpcServer == nil {
		return fmt.Errorf("gRPC server not provided to engine")
	}

	return nil
}

func (e *Engine) Stop(ctx context.Context) error {
	e.state.Lock()
	if !e.state.isRunning {
		e.state.Unlock()
		return nil
	}
	e.state.isRunning = false
	e.state.Unlock()

	// Note: We don't stop the gRPC server here since it's shared
	// The BaseService will handle stopping the server

	return nil
}

func (e *Engine) GetMetrics() map[string]int64 {
	return map[string]int64{
		"requests_processed":      atomic.LoadInt64(&e.metrics.requestsProcessed),
		"errors":                  atomic.LoadInt64(&e.metrics.errors),
		"login_attempts":          atomic.LoadInt64(&e.metrics.loginAttempts),
		"authentication_requests": atomic.LoadInt64(&e.metrics.authenticationRequests),
		"authorization_requests":  atomic.LoadInt64(&e.metrics.authorizationRequests),
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

func (e *Engine) CheckDatabase() error {
	// TODO: Implement database health check
	return nil
}

func (e *Engine) TrackOperation() {
	atomic.AddInt32(&e.state.ongoingOperations, 1)
}

func (e *Engine) UntrackOperation() {
	atomic.AddInt32(&e.state.ongoingOperations, -1)
}

func (e *Engine) IncrementLoginAttempts() {
	atomic.AddInt64(&e.metrics.loginAttempts, 1)
}

func (e *Engine) IncrementAuthenticationRequests() {
	atomic.AddInt64(&e.metrics.authenticationRequests, 1)
}

func (e *Engine) IncrementAuthorizationRequests() {
	atomic.AddInt64(&e.metrics.authorizationRequests, 1)
}

func (e *Engine) IncrementRequestsProcessed() {
	atomic.AddInt64(&e.metrics.requestsProcessed, 1)
}

func (e *Engine) IncrementErrors() {
	atomic.AddInt64(&e.metrics.errors, 1)
}
