package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	pb "github.com/redbco/redb-open/api/proto/anchor/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
	internalconfig "github.com/redbco/redb-open/services/anchor/internal/config"
	internaldatabase "github.com/redbco/redb-open/services/anchor/internal/database"
	"github.com/redbco/redb-open/services/anchor/internal/state"
	"github.com/redbco/redb-open/services/anchor/internal/watcher"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Engine struct {
	config             *config.Config
	grpcServer         *grpc.Server
	database           *database.PostgreSQL
	coreConn           *grpc.ClientConn
	umConn             *grpc.ClientConn
	configWatcher      *watcher.ConfigWatcher
	schemaWatcher      *watcher.SchemaWatcher
	replicationWatcher *watcher.ReplicationWatcher
	nodeID             string
	standalone         bool
	logger             *logger.Logger
	state              struct {
		sync.Mutex
		isRunning         bool
		ongoingOperations int32
	}
	metrics struct {
		requestsProcessed int64
		errors            int64
	}
	// Add context and cancel function for watcher shutdown
	watcherCtx    context.Context
	watcherCancel context.CancelFunc
}

func NewEngine(cfg *config.Config, standalone bool) *Engine {
	return &Engine{
		config:     cfg,
		standalone: standalone,
	}
}

// SetGRPCServer sets the shared gRPC server and registers the service immediately
func (e *Engine) SetGRPCServer(server *grpc.Server) {
	e.grpcServer = server

	// Register the service immediately when server is set (BEFORE serving starts)
	if e.grpcServer != nil {
		serviceServer := NewServer(e)
		pb.RegisterAnchorServiceServer(e.grpcServer, serviceServer)
	}
}

// SetLogger sets the logger for the engine
func (e *Engine) SetLogger(logger *logger.Logger) {
	e.logger = logger

	// Also set the logger on the DatabaseManager in GlobalState
	globalState := state.GetInstance()
	globalState.SetLogger(logger)
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

	// Initialize database connection using the config first
	dbConfig := database.FromGlobalConfig(e.config)

	// Create database logger for internal database logging
	var dbLogger *internaldatabase.DatabaseLogger
	if e.logger != nil {
		dbLogger = internaldatabase.NewDatabaseLogger(e.logger)
		// Log internal database connection attempt
		dbLogger.LogInternalConnectionAttempt(dbConfig.Host, dbConfig.Port)
	}

	db, err := database.New(ctx, dbConfig)
	if err != nil {
		// Log internal database connection failure as ERROR
		if dbLogger != nil {
			dbLogger.LogInternalConnectionFailure(dbConfig.Host, dbConfig.Port, err)
		}
		return fmt.Errorf("failed to initialize database: %w", err)
	}

	// Log successful internal database connection
	if dbLogger != nil {
		dbLogger.LogInternalConnectionSuccess(dbConfig.Host, dbConfig.Port)
	}

	e.database = db

	// Get NodeID from the database localidentity table
	nodeID, err := e.getNodeIDFromDatabase(ctx)
	if err != nil {
		return fmt.Errorf("failed to get node ID from database: %w", err)
	}
	e.nodeID = nodeID

	if e.logger != nil {
		e.logger.Info("Retrieved node ID from database: %s", e.nodeID)
	}

	// Initialize global state with database and nodeID
	globalState := state.GetInstance()

	// Initialize gRPC connections to other services (unless standalone)
	if !e.standalone {
		// Initialize gRPC connection to Core service
		coreAddr := e.config.Get("services.supervisor.service_locations.core")
		if coreAddr == "" {
			coreAddr = "localhost:50062" // default core service port
		}
		e.coreConn, err = grpc.Dial(coreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to Core service: %w", err)
		}

		// Initialize gRPC connection to UnifiedModel service
		umAddr := e.config.Get("services.supervisor.service_locations.unifiedmodel")
		if umAddr == "" {
			umAddr = "localhost:50053" // default unifiedmodel service port
		}
		e.umConn, err = grpc.Dial(umAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to UnifiedModel service: %w", err)
		}

		// Create config repository with Core service connection
		configRepository := internalconfig.NewRepository(e.database, e.coreConn)
		globalState.Initialize(configRepository, e.nodeID)
		globalState.SetDB(e.database)

		// Create watchers
		e.configWatcher = watcher.NewConfigWatcher(globalState.GetConfigRepository(), "", e.logger)
		e.schemaWatcher = watcher.NewSchemaWatcher(globalState.GetDB(), e.umConn, e.coreConn, "", e.logger)
		e.replicationWatcher = watcher.NewReplicationWatcher(globalState.GetConfigRepository(), e.logger)

		// Create context for watchers with cancellation
		e.watcherCtx, e.watcherCancel = context.WithCancel(ctx)

		// Perform initial database connections with retry logic
		maxRetries := 3
		retryDelay := time.Second * 2
		for i := 0; i < maxRetries; i++ {
			if err := e.configWatcher.InitialConnect(ctx); err != nil {
				if e.logger != nil {
					e.logger.Error("Initial connection attempt %d/%d failed: %v", i+1, maxRetries, err)
				}
				if i < maxRetries-1 {
					time.Sleep(retryDelay)
					retryDelay *= 2 // exponential backoff
				}
			} else {
				if e.logger != nil {
					e.logger.Info("Initial connections established successfully")
				}
				break
			}
		}

		// Start watchers with the cancellable context
		go e.configWatcher.Start(e.watcherCtx)
		go e.schemaWatcher.Start(e.watcherCtx)
		go e.replicationWatcher.Start(e.watcherCtx)
	} else {
		// In standalone mode, initialize state without external dependencies
		globalState.Initialize(nil, e.nodeID)
	}

	// Service is already registered in SetGRPCServer, just mark as running
	e.state.isRunning = true

	return nil
}

// getNodeIDFromDatabase retrieves the node ID from the localidentity table
func (e *Engine) getNodeIDFromDatabase(ctx context.Context) (string, error) {
	if e.database == nil {
		return "", fmt.Errorf("database not initialized")
	}

	var nodeID string
	query := "SELECT identity_id FROM localidentity LIMIT 1"

	row := e.database.Pool().QueryRow(ctx, query)
	err := row.Scan(&nodeID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// No node ID exists yet - this might be the first startup
			if e.logger != nil {
				e.logger.Warn("No node ID found in localidentity table - service may need initialization")
			}
			return "", fmt.Errorf("no node ID found in localidentity table")
		}
		return "", fmt.Errorf("failed to query node ID from database: %w", err)
	}

	if nodeID == "" {
		return "", fmt.Errorf("node ID is empty in database")
	}

	return nodeID, nil
}

func (e *Engine) Stop(ctx context.Context) error {
	e.state.Lock()
	defer e.state.Unlock()

	if !e.state.isRunning {
		return nil
	}

	e.state.isRunning = false

	// Cancel watcher context to signal shutdown
	if e.watcherCancel != nil {
		if e.logger != nil {
			e.logger.Info("Stopping watchers...")
		}
		e.watcherCancel()
		// Give watchers a moment to shutdown gracefully
		time.Sleep(100 * time.Millisecond)
	}

	// Update connection statuses before disconnecting (only in non-standalone mode)
	if !e.standalone {
		globalState := state.GetInstance()
		configRepo := globalState.GetConfigRepository()
		dbManager := globalState.GetDatabaseManager()

		if configRepo != nil && dbManager != nil {
			// Update all instance statuses to STATUS_DISCONNECTED
			for _, instanceID := range dbManager.GetAllInstanceClientIDs() {
				if err := configRepo.UpdateInstanceConnectionStatus(ctx, instanceID, false, "Service shutdown"); err != nil {
					if e.logger != nil {
						e.logger.Error("Failed to update instance %s status during shutdown: %v", instanceID, err)
					}
				}
			}

			// Update all database statuses to STATUS_DISCONNECTED
			for _, databaseID := range dbManager.GetAllDatabaseClientIDs() {
				if err := configRepo.UpdateDatabaseConnectionStatus(ctx, databaseID, false, "Service shutdown"); err != nil {
					if e.logger != nil {
						e.logger.Error("Failed to update database %s status during shutdown: %v", databaseID, err)
					}
				}
			}
		}

		// Disconnect all instances
		for _, id := range dbManager.GetAllInstanceClientIDs() {
			if err := dbManager.DisconnectInstance(id); err != nil {
				if e.logger != nil {
					e.logger.Error("Failed to disconnect instance %s during shutdown: %v", id, err)
				}
			}
		}

		// Disconnect all databases
		for _, id := range dbManager.GetAllDatabaseClientIDs() {
			if err := dbManager.DisconnectDatabase(id); err != nil {
				if e.logger != nil {
					e.logger.Error("Failed to disconnect database %s during shutdown: %v", id, err)
				}
			}
		}
	}

	// Close connections
	if e.coreConn != nil {
		e.coreConn.Close()
	}
	if e.umConn != nil {
		e.umConn.Close()
	}
	if e.database != nil {
		e.database.Close()
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
	e.state.Lock()
	defer e.state.Unlock()

	if !e.state.isRunning {
		return fmt.Errorf("service not initialized")
	}

	if e.database == nil {
		return fmt.Errorf("database not initialized")
	}

	// Test database connection with unified logging
	err := e.database.Pool().Ping(context.Background())

	// Log health check result for internal database
	if e.logger != nil {
		dbLogger := internaldatabase.NewDatabaseLogger(e.logger)
		dbLogger.LogHealthCheck(internaldatabase.DatabaseLogContext{
			DatabaseType: "postgres",
			DatabaseID:   "internal",
			IsInternal:   true,
		}, err == nil, err)
	}

	return err
}

func (e *Engine) CheckCoreService() error {
	if e.standalone {
		return nil // Skip check in standalone mode
	}

	e.state.Lock()
	defer e.state.Unlock()

	if !e.state.isRunning {
		return fmt.Errorf("service not initialized")
	}

	if e.coreConn == nil {
		return fmt.Errorf("core service connection not initialized")
	}

	// TODO: Add actual health check to core service
	return nil
}

func (e *Engine) CheckUnifiedModelService() error {
	if e.standalone {
		return nil // Skip check in standalone mode
	}

	e.state.Lock()
	defer e.state.Unlock()

	if !e.state.isRunning {
		return fmt.Errorf("service not initialized")
	}

	if e.umConn == nil {
		return fmt.Errorf("unified model service connection not initialized")
	}

	// TODO: Add actual health check to unified model service
	return nil
}

func (e *Engine) CheckWatchers() error {
	if e.standalone {
		return nil // Skip check in standalone mode
	}

	e.state.Lock()
	defer e.state.Unlock()

	if !e.state.isRunning {
		return fmt.Errorf("service not initialized")
	}

	// TODO: Add actual health check for watchers
	return nil
}

func (e *Engine) TrackOperation() {
	atomic.AddInt32(&e.state.ongoingOperations, 1)
	atomic.AddInt64(&e.metrics.requestsProcessed, 1)
}

func (e *Engine) UntrackOperation() {
	atomic.AddInt32(&e.state.ongoingOperations, -1)
}

func (e *Engine) GetState() *state.GlobalState {
	return state.GetInstance()
}
