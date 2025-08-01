package service

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	supervisorv1 "github.com/redbco/redb-open/api/proto/supervisor/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/health"
	"github.com/redbco/redb-open/pkg/logger"
)

// Service interface that all microservices must implement
type Service interface {
	// Initialize is called after registration but before starting
	Initialize(ctx context.Context, config *config.Config) error

	// Start begins the service's main work
	Start(ctx context.Context) error

	// Stop gracefully shuts down the service
	Stop(ctx context.Context, gracePeriod time.Duration) error

	// GetCapabilities returns the service capabilities
	GetCapabilities() *supervisorv1.ServiceCapabilities

	// CollectMetrics returns current service metrics
	CollectMetrics() map[string]int64

	// HealthChecks returns service-specific health check functions
	HealthChecks() map[string]health.CheckFunc
}

// GRPCServerAware is an optional interface that services can implement
// if they need access to the shared gRPC server
type GRPCServerAware interface {
	SetGRPCServer(server *grpc.Server)
}

// LoggerAware is an optional interface that services can implement
// if they need access to the logger
type LoggerAware interface {
	SetLogger(logger *logger.Logger)
}

// BaseService provides common functionality for all microservices
type BaseService struct {
	// Service identification
	Name       string
	Version    string
	InstanceID string
	ServiceID  string

	// Network configuration
	Port           int
	SupervisorAddr string

	// Core components
	Logger        *logger.Logger
	Config        *config.Config
	HealthChecker *health.Checker

	// gRPC connections
	grpcServer       *grpc.Server
	supervisorConn   *grpc.ClientConn
	supervisorClient supervisorv1.SupervisorServiceClient

	// State management
	mu        sync.RWMutex
	state     commonv1.ServiceState
	stopCh    chan struct{}
	stoppedCh chan struct{}

	// Service implementation
	impl Service

	// gRPC server state
	listener net.Listener

	// Standalone mode flag
	standalone bool
}

// NewBaseService creates a new base service instance
func NewBaseService(name, version string, port int, supervisorAddr string, impl Service) *BaseService {
	instanceID := uuid.New().String()

	// Check if supervisor address indicates standalone mode
	standalone := supervisorAddr == "" || supervisorAddr == "standalone"

	return &BaseService{
		Name:           name,
		Version:        version,
		InstanceID:     instanceID,
		Port:           port,
		SupervisorAddr: supervisorAddr,
		Logger:         logger.New(name, version),
		Config:         config.New(),
		HealthChecker:  health.NewChecker(),
		stopCh:         make(chan struct{}),
		stoppedCh:      make(chan struct{}),
		impl:           impl,
		standalone:     standalone,
	}
}

// SetStandaloneMode sets the standalone mode flag
func (s *BaseService) SetStandaloneMode(standalone bool) {
	s.standalone = standalone
}

// Run starts the service and manages its lifecycle
func (s *BaseService) Run(ctx context.Context) error {
	// Set initial state
	s.setState(commonv1.ServiceState_SERVICE_STATE_STARTING)

	// Connect to supervisor (unless in standalone mode)
	if !s.standalone {
		if err := s.connectToSupervisor(ctx); err != nil {
			s.Logger.Warnf("Failed to connect to supervisor: %v", err)
			s.Logger.Infof("Switching to standalone mode - logs will be displayed directly")
			s.standalone = true
		} else {
			defer s.supervisorConn.Close()
		}
	}

	// Start gRPC server
	if err := s.startGRPCServer(); err != nil {
		return fmt.Errorf("failed to start gRPC server: %w", err)
	}

	// Provide gRPC server to service implementation
	if gRPCAware, ok := s.impl.(GRPCServerAware); ok {
		gRPCAware.SetGRPCServer(s.grpcServer)
	}

	// Provide logger to service implementation
	if loggerAware, ok := s.impl.(LoggerAware); ok {
		loggerAware.SetLogger(s.Logger)
	}

	// Initialize service implementation
	if err := s.impl.Initialize(ctx, s.Config); err != nil {
		return fmt.Errorf("failed to initialize service: %w", err)
	}
	s.Logger.Infof("Service implementation initialized successfully")

	// Now start serving gRPC requests after all services are registered
	s.StartServing()

	// Register with supervisor AFTER the server is serving (only if not standalone)
	if !s.standalone && s.supervisorConn != nil {
		if err := s.registerWithSupervisor(ctx); err != nil {
			s.Logger.Warnf("Failed to register with supervisor: %v", err)
			s.Logger.Infof("Continuing in standalone mode")
			s.standalone = true
		}
	}

	// Start background tasks
	s.Logger.Infof("Starting background tasks...")
	go s.heartbeatLoop(ctx)
	go s.logStreamLoop(ctx)
	go s.healthCheckLoop(ctx)
	s.Logger.Infof("Background tasks started")

	// Start service implementation
	s.Logger.Infof("Starting service implementation...")
	if err := s.impl.Start(ctx); err != nil {
		return fmt.Errorf("failed to start service: %w", err)
	}
	s.Logger.Infof("Service implementation started successfully")

	// Set running state
	s.setState(commonv1.ServiceState_SERVICE_STATE_RUNNING)
	s.Logger.Info("Service started successfully")

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigCh:
		s.Logger.Info("Received shutdown signal")
	case <-s.stopCh:
		s.Logger.Info("Received stop command")
	case <-ctx.Done():
		s.Logger.Info("Context cancelled")
	}

	// Graceful shutdown
	s.setState(commonv1.ServiceState_SERVICE_STATE_STOPPING)
	return s.shutdown(ctx)
}

func (s *BaseService) connectToSupervisor(ctx context.Context) error {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                10 * time.Second,
		Timeout:             3 * time.Second,
		PermitWithoutStream: true,
	}))

	// Add connection retry and backoff
	opts = append(opts, grpc.WithDefaultCallOptions(
		grpc.WaitForReady(true),
	))

	// Add a reasonable connection timeout
	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, s.SupervisorAddr, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to supervisor at %s: %w", s.SupervisorAddr, err)
	}

	s.supervisorConn = conn
	s.supervisorClient = supervisorv1.NewSupervisorServiceClient(conn)

	s.Logger.Infof("Connected to supervisor at %s", s.SupervisorAddr)

	return nil
}

func (s *BaseService) startGRPCServer() error {
	maxRetries := 3
	retryDelay := time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
		if err != nil {
			if attempt < maxRetries {
				s.Logger.Warnf("Failed to bind to port %d (attempt %d/%d): %v, retrying...", s.Port, attempt, maxRetries, err)
				time.Sleep(retryDelay)
				retryDelay *= 2
				continue
			}
			return fmt.Errorf("failed to listen on port %d after %d attempts: %w", s.Port, maxRetries, err)
		}

		var opts []grpc.ServerOption
		opts = append(opts, grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 15 * time.Second,
			MaxConnectionAge:  30 * time.Second,
			Time:              5 * time.Second,
			Timeout:           1 * time.Second,
		}))

		s.grpcServer = grpc.NewServer(opts...)

		// Register ServiceController server
		controllerServer := NewControllerServer(s)
		supervisorv1.RegisterServiceControllerServiceServer(s.grpcServer, controllerServer)

		s.Logger.Infof("gRPC server created on port %d", s.Port)

		// Store the listener for later serving
		s.listener = lis
		return nil
	}

	return fmt.Errorf("failed to start gRPC server after %d attempts", maxRetries)
}

// StartServing begins serving gRPC requests after all services are registered
func (s *BaseService) StartServing() {
	if s.grpcServer != nil && s.listener != nil {
		s.Logger.Infof("Starting gRPC server on port %d", s.Port)

		// Start server in background
		go func() {
			if err := s.grpcServer.Serve(s.listener); err != nil {
				s.Logger.Errorf("Failed to serve: %v", err)
			}
		}()

		// Give the server a moment to start
		time.Sleep(100 * time.Millisecond)
		s.Logger.Infof("gRPC server started successfully on port %d", s.Port)
	}
}

func (s *BaseService) registerWithSupervisor(ctx context.Context) error {
	s.Logger.Infof("Starting registration with supervisor...")

	req := &supervisorv1.RegisterServiceRequest{
		Service: &commonv1.ServiceInfo{
			Name:       s.Name,
			Version:    s.Version,
			InstanceId: s.InstanceID,
			Host:       "localhost",
			Port:       int32(s.Port),
			Metadata: map[string]string{
				"start_time": time.Now().Format(time.RFC3339),
			},
		},
		Capabilities: s.impl.GetCapabilities(),
	}

	s.Logger.Infof("Sending registration request...")
	resp, err := s.supervisorClient.RegisterService(ctx, req)
	if err != nil {
		return fmt.Errorf("registration failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("registration rejected: %s", resp.Message)
	}

	s.ServiceID = resp.ServiceId

	// Apply initial configuration
	if resp.InitialConfig != nil {
		s.Config.Update(resp.InitialConfig.Config)
	}

	s.Logger.Infof("Registered with supervisor, service ID: %s", s.ServiceID)
	return nil
}

func (s *BaseService) heartbeatLoop(ctx context.Context) {
	// If in standalone mode, don't send heartbeats
	if s.standalone {
		s.Logger.Infof("Running in standalone mode - heartbeat loop disabled")
		return
	}

	// Send initial heartbeat immediately
	if err := s.sendHeartbeat(ctx); err != nil {
		s.Logger.Errorf("Failed to send initial heartbeat: %v", err)
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.sendHeartbeat(ctx); err != nil {
				s.Logger.Errorf("Failed to send heartbeat: %v", err)
			}

		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		}
	}
}

func (s *BaseService) sendHeartbeat(ctx context.Context) error {
	// Check if we're shutting down before sending heartbeat
	select {
	case <-s.stopCh:
		return fmt.Errorf("service is shutting down")
	default:
	}

	metrics := s.collectMetrics()

	req := &supervisorv1.HeartbeatRequest{
		ServiceId:    s.ServiceID,
		HealthStatus: s.HealthChecker.GetOverallStatus(),
		Metrics:      metrics,
		Timestamp:    timestamppb.Now(),
	}

	// Use a shorter timeout for heartbeats to avoid blocking during shutdown
	heartbeatCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	resp, err := s.supervisorClient.SendHeartbeat(heartbeatCtx, req)
	if err != nil {
		// Check if this is due to shutdown
		select {
		case <-s.stopCh:
			return fmt.Errorf("service is shutting down")
		default:
		}
		return err
	}

	// Process configuration updates
	if resp.ConfigUpdate != nil {
		s.Logger.Info("Received configuration update")
		s.Config.Update(resp.ConfigUpdate.Config)
	}

	// Process commands
	for _, cmd := range resp.Commands {
		go s.processCommand(cmd)
	}

	return nil
}

func (s *BaseService) logStreamLoop(ctx context.Context) {
	// If in standalone mode, don't attempt to connect to supervisor
	if s.standalone {
		s.Logger.Infof("Running in standalone mode - logs will be displayed directly")
		return
	}

	// Add a small delay to ensure connection is stable after registration
	time.Sleep(500 * time.Millisecond)

	var stream supervisorv1.SupervisorService_StreamLogsClient
	var logCh <-chan logger.LogEntry

	// Retry logic for creating the log stream
	maxRetries := 5
	retryDelay := time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		var err error
		stream, err = s.supervisorClient.StreamLogs(ctx)
		if err != nil {
			s.Logger.Errorf("Failed to create log stream (attempt %d/%d): %v", attempt, maxRetries, err)
			if attempt < maxRetries {
				select {
				case <-time.After(retryDelay):
					retryDelay *= 2 // Exponential backoff
					continue
				case <-ctx.Done():
					return
				case <-s.stopCh:
					return
				}
			} else {
				s.Logger.Errorf("Failed to create log stream after %d attempts, keeping console output enabled", maxRetries)
				return
			}
		} else {
			s.Logger.Infof("Log stream established successfully")
			// Send an immediate heartbeat to trigger RUNNING state transition
			go func() {
				if err := s.sendHeartbeat(ctx); err != nil {
					s.Logger.Errorf("Failed to send immediate heartbeat: %v", err)
				}
			}()
			break
		}
	}

	// Disable console output from the shared logger to prevent duplicate logging
	// The supervisor will handle all console output
	s.Logger.DisableConsoleOutput()
	s.Logger.Infof("Console output disabled - logs will be displayed by supervisor")

	logCh = s.Logger.Subscribe()

	for {
		select {
		case entry := <-logCh:
			req := &supervisorv1.LogStreamRequest{
				Entry: &commonv1.LogEntry{
					Timestamp: timestamppb.New(entry.Time),
					Level:     logger.MapLogLevel(entry.Level),
					Message:   entry.Message,
					Service: &commonv1.ServiceInfo{
						Name:       s.Name,
						Version:    s.Version,
						InstanceId: s.InstanceID,
					},
					Fields:  entry.Fields,
					TraceId: entry.TraceID,
				},
			}

			if err := stream.Send(req); err != nil {
				// Re-enable console output temporarily for error logging
				s.Logger.EnableConsoleOutput()
				s.Logger.Errorf("Failed to send log: %v", err)
				s.Logger.DisableConsoleOutput()

				// Try to recreate the stream on error
				stream.CloseSend()

				// Retry creating the stream
				for retryAttempt := 1; retryAttempt <= 3; retryAttempt++ {
					time.Sleep(time.Second * time.Duration(retryAttempt))
					newStream, err := s.supervisorClient.StreamLogs(ctx)
					if err != nil {
						// Re-enable console output temporarily for error logging
						s.Logger.EnableConsoleOutput()
						s.Logger.Errorf("Failed to recreate log stream (attempt %d/3): %v", retryAttempt, err)
						s.Logger.DisableConsoleOutput()
						continue
					}
					stream = newStream
					// Re-enable console output temporarily for success logging
					s.Logger.EnableConsoleOutput()
					s.Logger.Infof("Log stream recreated successfully")
					s.Logger.DisableConsoleOutput()
					break
				}
			}

		case <-ctx.Done():
			if stream != nil {
				stream.CloseSend()
			}
			// Re-enable console output when shutting down
			s.Logger.EnableConsoleOutput()
			return
		case <-s.stopCh:
			if stream != nil {
				stream.CloseSend()
			}
			// Re-enable console output when shutting down
			s.Logger.EnableConsoleOutput()
			return
		}
	}
}

func (s *BaseService) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Get service-specific health checks
	checks := s.impl.HealthChecks()

	for {
		select {
		case <-ticker.C:
			// Run health checks
			for name, checkFunc := range checks {
				s.HealthChecker.RunCheck(name, checkFunc)
			}

		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		}
	}
}

func (s *BaseService) collectMetrics() *supervisorv1.ServiceMetrics {
	// Collect runtime metrics
	baseMetrics := &supervisorv1.ServiceMetrics{
		MemoryUsageBytes: getMemoryUsage(),
		CpuUsagePercent:  getCPUUsage(),
		Goroutines:       int64(runtime.NumGoroutine()),
		CustomMetrics:    s.impl.CollectMetrics(),
	}

	return baseMetrics
}

func (s *BaseService) processCommand(cmd *supervisorv1.ServiceCommand) {
	switch cmd.Type {
	case supervisorv1.ServiceCommand_COMMAND_TYPE_RELOAD_CONFIG:
		s.Logger.Info("Reloading configuration")
		// Configuration is already updated via heartbeat response

	case supervisorv1.ServiceCommand_COMMAND_TYPE_ROTATE_LOGS:
		s.Logger.Info("Rotating logs")
		s.Logger.Rotate()

	case supervisorv1.ServiceCommand_COMMAND_TYPE_COLLECT_METRICS:
		s.Logger.Info("Collecting detailed metrics")
		// Could implement detailed metrics collection

	case supervisorv1.ServiceCommand_COMMAND_TYPE_CUSTOM:
		s.Logger.Infof("Processing custom command: %v", cmd.Parameters)
		// Let service implementation handle custom commands
	}
}

func (s *BaseService) setState(state commonv1.ServiceState) {
	s.mu.Lock()
	s.state = state
	s.mu.Unlock()
}

func (s *BaseService) shutdown(ctx context.Context) error {
	s.Logger.Info("Starting graceful shutdown")

	// Stop service implementation
	gracePeriod := 30 * time.Second
	if err := s.impl.Stop(ctx, gracePeriod); err != nil {
		s.Logger.Errorf("Service implementation shutdown error: %v", err)
	}

	// Give supervisor time to send stop commands to all services before unregistering
	s.Logger.Info("Waiting before unregistering to allow supervisor to send stop commands...")
	time.Sleep(1 * time.Second)

	// Unregister from supervisor (only if not in standalone mode)
	if !s.standalone && s.ServiceID != "" && s.supervisorConn != nil {
		req := &supervisorv1.UnregisterServiceRequest{
			ServiceId: s.ServiceID,
			Reason:    "Graceful shutdown",
		}

		// Use a longer timeout for unregistration to avoid deadline exceeded errors
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		if _, err := s.supervisorClient.UnregisterService(ctx, req); err != nil {
			s.Logger.Errorf("Failed to unregister: %v", err)
		} else {
			s.Logger.Info("Successfully unregistered from supervisor")
		}
	}

	// Stop gRPC server
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	// Signal stopped
	close(s.stoppedCh)
	s.setState(commonv1.ServiceState_SERVICE_STATE_STOPPED)
	s.Logger.Info("Service stopped")

	return nil
}
