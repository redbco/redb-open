package engine

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"

	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Engine struct {
	config         *config.Config
	server         *http.Server
	securityClient securityv1.SecurityServiceClient
	tenantClient   corev1.TenantServiceClient
	meshClient     corev1.MeshServiceClient
	logger         *logger.Logger
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

func (e *Engine) Start(ctx context.Context) error {
	e.state.Lock()
	if e.state.isRunning {
		e.state.Unlock()
		return fmt.Errorf("engine is already running")
	}
	e.state.isRunning = true
	e.state.Unlock()

	if e.logger != nil {
		e.logger.Infof("Starting ServiceAPI engine...")
	}

	// Connect to core service
	coreAddr := e.config.Get("services.core.grpc_address")
	if coreAddr == "" {
		coreAddr = "localhost:50062"
	}

	if e.logger != nil {
		e.logger.Infof("Connecting to core service at: %s", coreAddr)
	}

	coreConn, err := grpc.Dial(coreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to connect to core service at %s: %v", coreAddr, err)
		}
		return fmt.Errorf("failed to connect to core service: %v", err)
	}

	if e.logger != nil {
		e.logger.Infof("Successfully connected to core service at: %s", coreAddr)
	}

	e.tenantClient = corev1.NewTenantServiceClient(coreConn)
	e.meshClient = corev1.NewMeshServiceClient(coreConn)

	// Connect to security service
	securityAddr := e.config.Get("services.security.grpc_address")
	if securityAddr == "" {
		securityAddr = "localhost:50051"
	}

	if e.logger != nil {
		e.logger.Infof("Connecting to security service at: %s", securityAddr)
	}

	securityConn, err := grpc.Dial(securityAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to connect to security service at %s: %v", securityAddr, err)
		}
		return fmt.Errorf("failed to connect to security service: %v", err)
	}

	if e.logger != nil {
		e.logger.Infof("Successfully connected to security service at: %s", securityAddr)
	}

	e.securityClient = securityv1.NewSecurityServiceClient(securityConn)

	if e.logger != nil {
		e.logger.Infof("Security client initialized successfully")
	}

	// Initialize HTTP server
	portStr := e.config.Get("services.serviceapi.http_port")
	if portStr == "" {
		portStr = "8081"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Invalid HTTP port configuration: %v", err)
		}
		return fmt.Errorf("invalid port configuration: %v", err)
	}

	e.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: NewServer(e),
	}

	if e.logger != nil {
		e.logger.Infof("Starting HTTP server on port: %d", port)
	}

	// Start HTTP server
	go func() {
		if err := e.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			if e.logger != nil {
				e.logger.Errorf("HTTP server error: %v", err)
			}
			atomic.AddInt64(&e.metrics.errors, 1)
		}
	}()

	if e.logger != nil {
		e.logger.Infof("ServiceAPI engine started successfully")
	}

	return nil
}

func (e *Engine) Stop(ctx context.Context) error {
	e.state.Lock()
	if !e.state.isRunning {
		e.state.Unlock()
		return nil
	}
	e.state.Unlock()

	if e.server != nil {
		return e.server.Shutdown(ctx)
	}

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

	if e.server == nil {
		return fmt.Errorf("HTTP server not initialized")
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

// Query forwards the request to the core service
func (e *Engine) Query(ctx context.Context, req interface{}) (interface{}, error) {
	// TODO: Implement query handling based on the request type
	return nil, fmt.Errorf("query not implemented")
}

// GetSecurityClient returns the security service client
func (e *Engine) GetSecurityClient() securityv1.SecurityServiceClient {
	return e.securityClient
}

// GetTenantClient returns the tenant service client
func (e *Engine) GetTenantClient() corev1.TenantServiceClient {
	return e.tenantClient
}
