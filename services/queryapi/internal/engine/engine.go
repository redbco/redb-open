package engine

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"

	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Engine struct {
	config     *config.Config
	server     *http.Server
	coreClient corev1.DatabaseServiceClient
	logger     *logger.Logger
	state      struct {
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

	// Connect to core service
	coreAddr := e.config.Get("services.core.grpc_address")
	if coreAddr == "" {
		// Default core service address to prevent empty target error
		coreAddr = "localhost:50062" // Default core service port (matches core service default)
	}

	conn, err := grpc.Dial(coreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to core service: %v", err)
	}
	e.coreClient = corev1.NewDatabaseServiceClient(conn)

	// Initialize HTTP server
	portStr := e.config.Get("services.queryapi.http_port") // Fixed: Use hyphenated name
	if portStr == "" {
		portStr = "8082" // Default HTTP port
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("invalid port configuration: %v", err)
	}

	e.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: NewServer(e),
	}

	// Start HTTP server
	go func() {
		if err := e.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			atomic.AddInt64(&e.metrics.errors, 1)
		}
	}()

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
