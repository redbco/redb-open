package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	supervisorv1 "github.com/redbco/redb-open/api/proto/supervisor/v1"
	server "github.com/redbco/redb-open/cmd/supervisor/internal/grpc"
	"github.com/redbco/redb-open/cmd/supervisor/internal/health"
	"github.com/redbco/redb-open/cmd/supervisor/internal/initialize"
	"github.com/redbco/redb-open/cmd/supervisor/internal/logger"
	"github.com/redbco/redb-open/cmd/supervisor/internal/manager"
	"github.com/redbco/redb-open/cmd/supervisor/internal/superconfig"
)

var (
	Version   = "dev"     // Default version for development
	GitCommit = "unknown" // Git commit hash
	BuildTime = "unknown" // Build timestamp
)

var (
	port           = flag.Int("port", 50000, "The supervisor port")
	configFile     = flag.String("config", "config.yaml", "Configuration file path")
	initializeFlag = flag.Bool("initialize", false, "Initialize the reDB node (database, keys, etc.)")
	versionFlag    = flag.Bool("version", false, "Show version information and exit")
)

func printVersionInfo() {
	fmt.Printf("reDB Node v0.0.1 - Open Source Version (build %s)\n", Version)
	fmt.Printf("Built: %s, from commit: %s\n", BuildTime, GitCommit)
	fmt.Printf("Go version: %s\n", runtime.Version())
	fmt.Printf("OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
}

func main() {
	flag.Parse()

	// Handle version flag
	if *versionFlag {
		printVersionInfo()
		os.Exit(0)
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize unified logger for initialization (using basic config since we may not have access to full config yet)
	log := logger.NewUnifiedLogger("supervisor", "1.0.0", "logs/redb-node-event.log", "info")

	// Handle initialization mode
	if *initializeFlag {
		log.Info("Starting reDB node initialization...")

		// Create a timeout context for initialization (10 minutes should be enough)
		initCtx, initCancel := context.WithTimeout(ctx, 10*time.Minute)
		defer initCancel()

		initializer := initialize.New(log)
		if err := initializer.Initialize(initCtx); err != nil {
			log.Fatalf("Node initialization failed: %v", err)
		}

		log.Info("Node initialization completed successfully!")
		os.Exit(0)
	}

	// Load configuration
	cfg, err := superconfig.Load(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Update logger with config-based settings
	log = logger.NewUnifiedLogger("supervisor", "1.0.0", "logs/redb-node-event.log", cfg.Logging.Level)

	// Initialize supervisor
	serviceManager := manager.New(log, cfg)
	supervisor := &Supervisor{
		port:             *port,
		config:           cfg,
		logger:           log,
		serviceManager:   serviceManager,
		healthMonitor:    health.NewMonitor(log),
		logStore:         logger.NewStore(cfg.Logging.RetentionDays),
		readinessManager: manager.NewReadinessManager(log, serviceManager),
		shutdownCh:       make(chan struct{}),
	}

	// Run supervisor
	if err := supervisor.Run(ctx); err != nil {
		log.Fatalf("Failed to run supervisor: %v", err)
	}
}

type Supervisor struct {
	port             int
	config           *superconfig.Config
	logger           logger.LoggerInterface
	serviceManager   *manager.ServiceManager
	healthMonitor    *health.Monitor
	logStore         *logger.Store
	readinessManager *manager.ReadinessManager
	grpcServer       *grpc.Server
	shutdownCh       chan struct{}
	wg               sync.WaitGroup
	backgroundCtx    context.Context
	backgroundCancel context.CancelFunc
}

func (s *Supervisor) Run(ctx context.Context) error {
	s.logger.Info("Starting reDB Node Supervisor")

	// Create a separate context for background routines that we can cancel during shutdown
	s.backgroundCtx, s.backgroundCancel = context.WithCancel(context.Background())

	// Start gRPC server
	if err := s.startGRPCServer(); err != nil {
		return fmt.Errorf("failed to start gRPC server: %w", err)
	}

	// Start health monitor with background context
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.logger.Info("Starting health monitor")
		s.healthMonitor.Start(s.backgroundCtx)
		s.logger.Info("Health monitor stopped")
	}()

	// Start log aggregator with background context
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.logger.Info("Starting log store")
		s.logStore.Start(s.backgroundCtx)
		s.logger.Info("Log store stopped")
	}()

	// Start readiness manager with background context
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.logger.Info("Starting system readiness monitor")
		s.readinessManager.Start(s.backgroundCtx)
		s.logger.Info("System readiness monitor stopped")
	}()

	// Add system ready callbacks (extensible for future functionality)
	s.addSystemReadyCallbacks()

	// Start configured services
	if err := s.startConfiguredServices(ctx); err != nil {
		s.logger.Errorf("Failed to start some services: %v", err)
	}

	s.logger.Info("Supervisor started successfully")

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigCh:
		s.logger.Info("Received shutdown signal")
	case <-ctx.Done():
		s.logger.Info("Context cancelled")
	}

	// Graceful shutdown
	return s.shutdown(ctx)
}

func (s *Supervisor) startGRPCServer() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	var opts []grpc.ServerOption
	opts = append(opts, grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionIdle: 15 * time.Second,
		MaxConnectionAge:  30 * time.Minute,
		Time:              5 * time.Second,
		Timeout:           1 * time.Second,
	}))
	opts = append(opts, grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}))

	s.grpcServer = grpc.NewServer(opts...)

	// Register supervisor service
	supervisorServer := server.NewSupervisorServer(
		s.serviceManager,
		s.healthMonitor,
		s.logStore,
		s.logger,
	)
	supervisorv1.RegisterSupervisorServiceServer(s.grpcServer, supervisorServer)

	// Start server in background
	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			s.logger.Errorf("Failed to serve: %v", err)
		}
	}()

	return nil
}

func (s *Supervisor) startConfiguredServices(ctx context.Context) error {
	// Get service startup order based on dependencies
	startupOrder := s.config.GetServiceStartupOrder()

	for _, serviceName := range startupOrder {
		svcConfig, exists := s.config.Services[serviceName]
		if !exists || !svcConfig.Enabled {
			continue
		}

		s.logger.Infof("Starting service: %s", serviceName)

		if err := s.serviceManager.StartService(ctx, serviceName, svcConfig); err != nil {
			s.logger.Errorf("Failed to start %s: %v", serviceName, err)
			if svcConfig.Required {
				return fmt.Errorf("required service %s failed to start: %w", serviceName, err)
			}
		}

		// Wait for service to be healthy before starting dependents
		if err := s.waitForServiceHealth(ctx, serviceName, 30*time.Second); err != nil {
			s.logger.Warnf("Service %s did not become healthy: %v", serviceName, err)
		}
	}

	return nil
}

func (s *Supervisor) waitForServiceHealth(ctx context.Context, serviceName string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for service health")
		case <-ticker.C:
			if s.serviceManager.IsServiceHealthy(serviceName) {
				return nil
			}
		}
	}
}

func (s *Supervisor) shutdown(ctx context.Context) error {
	s.logger.Info("Starting graceful shutdown")

	// Create a timeout context for the entire shutdown process
	shutdownCtx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()

	// Step 1: Stop all services first (but keep gRPC server running to accept unregister requests)
	s.logger.Info("Stopping all services...")
	if err := s.serviceManager.StopAllServices(shutdownCtx); err != nil {
		s.logger.Errorf("Error stopping services: %v", err)
	}

	// Step 2: Give services additional time to unregister themselves
	s.logger.Info("Waiting for services to unregister...")
	time.Sleep(2 * time.Second)

	// Step 3: Now stop accepting new connections and shutdown the gRPC server
	s.logger.Info("Stopping gRPC server...")
	if s.grpcServer != nil {
		// Use a separate timeout for gRPC server shutdown
		grpcShutdownDone := make(chan struct{})
		go func() {
			s.grpcServer.GracefulStop()
			close(grpcShutdownDone)
		}()

		// Wait for graceful stop with timeout
		select {
		case <-grpcShutdownDone:
			s.logger.Info("gRPC server stopped gracefully")
		case <-time.After(5 * time.Second):
			s.logger.Warn("gRPC server graceful stop timeout, forcing stop")
			s.grpcServer.Stop()
		}
	}

	// Step 4: Signal shutdown to background routines
	s.logger.Info("Stopping background routines...")
	s.backgroundCancel() // Cancel the background context first
	close(s.shutdownCh)

	// Step 5: Wait for background routines with more detailed logging
	s.logger.Info("Waiting for background routines to shutdown...")
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info("All background routines stopped successfully")
	case <-time.After(5 * time.Second):
		s.logger.Warn("Background routines shutdown timeout (5s) - forcing shutdown")
	}

	s.logger.Info("Supervisor shutdown complete")

	// Close logger if it supports it
	if unifiedLogger, ok := s.logger.(logger.UnifiedLoggerInterface); ok {
		unifiedLogger.Close()
	}

	return nil
}

// addSystemReadyCallbacks adds callbacks to be executed when the system becomes ready
// This method can be extended to add more functionality that should be triggered
// once all services are up, running, and healthy
func (s *Supervisor) addSystemReadyCallbacks() {
	// Example callback: Log additional system information
	s.readinessManager.AddSystemReadyCallback(func() {
		s.logger.Info("System ready callback: All services are operational")

		// You can extend this with additional functionality like:
		// - Sending notifications to external systems
		// - Starting additional background tasks
		// - Updating external monitoring systems
		// - Performing system health validations
		// - Enabling traffic routing
		// - etc.
	})

	// Example: Add a callback to perform system validation
	s.readinessManager.AddSystemReadyCallback(func() {
		s.logger.Info("Performing post-startup system validation...")
		// Add your validation logic here
	})
}
