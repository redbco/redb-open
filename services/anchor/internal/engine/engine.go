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
	"github.com/redbco/redb-open/pkg/grpcconfig"
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
		// Initialize gRPC connection to Core service using dynamic address resolution
		coreAddr := e.getServiceAddress("core")
		e.coreConn, err = grpc.Dial(coreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to Core service: %w", err)
		}

		// Initialize gRPC connection to UnifiedModel service using dynamic address resolution
		umAddr := e.getServiceAddress("unifiedmodel")
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
		// Give watchers time to shutdown gracefully (increased from 100ms to 2 seconds)
		if e.logger != nil {
			e.logger.Info("Waiting for watchers to shutdown...")
		}
		time.Sleep(2 * time.Second)
		if e.logger != nil {
			e.logger.Info("Watchers shutdown completed")
		}
	}

	// Gracefully stop and save CDC replication streams
	if e.logger != nil {
		e.logger.Info("Stopping active CDC replication streams...")
	}

	// Create a context with timeout for CDC shutdown
	cdcCtx, cdcCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cdcCancel()

	manager := getCDCManager()
	manager.mu.RLock()
	activeStreams := make(map[string]*CDCReplicationStream)
	for id, stream := range manager.activeReplications {
		activeStreams[id] = stream
	}
	manager.mu.RUnlock()

	if len(activeStreams) > 0 {
		if e.logger != nil {
			e.logger.Infof("Found %d active CDC streams to shutdown", len(activeStreams))
		}

		// Get config repository for updating statuses
		globalState := state.GetInstance()
		configRepo := globalState.GetConfigRepository()

		for id, stream := range activeStreams {
			// Save current stream state to database
			if err := e.saveCDCStreamState(cdcCtx, stream); err != nil {
				if e.logger != nil {
					e.logger.Errorf("Failed to save CDC stream state for %s: %v", id, err)
				}
			}

			// Mark the replication source as STOPPED in database
			// This ensures it won't auto-start on application restart
			if configRepo != nil {
				if err := configRepo.UpdateReplicationSourceStatus(cdcCtx, stream.ReplicationSourceID, "STATUS_STOPPED", "Stopped during application shutdown"); err != nil {
					if e.logger != nil {
						e.logger.Errorf("Failed to update replication source status for %s: %v", id, err)
					}
				} else if e.logger != nil {
					e.logger.Infof("Marked replication source %s as STOPPED in database", id)
				}

				// Also mark the relationship as STOPPED
				if stream.RelationshipID != "" {
					if err := configRepo.UpdateRelationshipStatus(cdcCtx, stream.RelationshipID, "STATUS_STOPPED", "Stopped during application shutdown"); err != nil {
						if e.logger != nil {
							e.logger.Errorf("Failed to update relationship status for %s: %v", stream.RelationshipID, err)
						}
					} else if e.logger != nil {
						e.logger.Infof("Marked relationship %s as STOPPED in database", stream.RelationshipID)
					}
				}
			}

			// Stop the replication source
			if stream.ReplicationSource != nil {
				if err := stream.ReplicationSource.Stop(); err != nil {
					if e.logger != nil {
						e.logger.Warnf("Error stopping replication source %s: %v", id, err)
					}
				} else if e.logger != nil {
					e.logger.Infof("Stopped replication source %s", id)
				}
			}

			// Close the replication connection
			if stream.ReplicationSource != nil {
				if err := stream.ReplicationSource.Close(); err != nil {
					if e.logger != nil {
						e.logger.Warnf("Error closing replication source %s: %v", id, err)
					}
				}
			}

			// Signal stop
			if stream.StopChan != nil {
				close(stream.StopChan)
			}
		}

		// Clear the active replications map
		manager.mu.Lock()
		manager.activeReplications = make(map[string]*CDCReplicationStream)
		manager.mu.Unlock()

		if e.logger != nil {
			e.logger.Info("CDC replication streams shutdown completed")
		}
	} else if e.logger != nil {
		e.logger.Info("No active CDC streams to shutdown")
	}

	// Update connection statuses before disconnecting (only in non-standalone mode)
	if !e.standalone {
		globalState := state.GetInstance()
		configRepo := globalState.GetConfigRepository()
		registry := globalState.GetConnectionRegistry()
		connManager := globalState.GetConnectionManager()

		if configRepo != nil && registry != nil {
			if e.logger != nil {
				e.logger.Info("Updating database and instance statuses during shutdown...")
			}

			// Use a separate context for database operations to avoid cancellation issues
			dbCtx := context.Background()
			shutdownCtx, cancel := context.WithTimeout(dbCtx, 5*time.Second)
			defer cancel()

			// Update all instance statuses to STATUS_DISCONNECTED (synchronous)
			for _, instanceID := range registry.GetAllInstanceClientIDs() {
				if err := configRepo.UpdateInstanceConnectionStatus(shutdownCtx, instanceID, false, "Service shutdown"); err != nil {
					if e.logger != nil {
						e.logger.Error("Failed to update instance %s status during shutdown: %v", instanceID, err)
					}
				} else if e.logger != nil {
					e.logger.Infof("Successfully updated instance %s status to DISCONNECTED", instanceID)
				}
			}

			// Update all database statuses to STATUS_DISCONNECTED (synchronous)
			for _, databaseID := range registry.GetAllDatabaseClientIDs() {
				if err := configRepo.UpdateDatabaseConnectionStatus(shutdownCtx, databaseID, false, "Service shutdown"); err != nil {
					if e.logger != nil {
						e.logger.Error("Failed to update database %s status during shutdown: %v", databaseID, err)
					}
				} else if e.logger != nil {
					e.logger.Infof("Successfully updated database %s status to DISCONNECTED", databaseID)
				}
			}

			// Disconnect all instances (synchronous)
			for _, id := range registry.GetAllInstanceClientIDs() {
				if err := registry.DisconnectInstance(id); err != nil {
					if e.logger != nil {
						e.logger.Error("Failed to disconnect instance %s during shutdown: %v", id, err)
					}
				} else if e.logger != nil {
					e.logger.Infof("Successfully disconnected instance %s", id)
				}
			}

			// Disconnect all databases (synchronous)
			for _, id := range registry.GetAllDatabaseClientIDs() {
				if err := registry.DisconnectDatabase(id); err != nil {
					if e.logger != nil {
						e.logger.Error("Failed to disconnect database %s during shutdown: %v", id, err)
					}
				} else if e.logger != nil {
					e.logger.Infof("Successfully disconnected database %s", id)
				}
			}

			// Also disconnect all ConnectionManager connections
			if connManager != nil {
				if e.logger != nil {
					e.logger.Info("Disconnecting all adapter-based connections...")
				}

				if err := connManager.DisconnectAll(shutdownCtx); err != nil {
					if e.logger != nil {
						e.logger.Error("Failed to disconnect all adapter connections: %v", err)
					}
				} else if e.logger != nil {
					e.logger.Info("All adapter connections disconnected successfully")
				}
			}

			if e.logger != nil {
				e.logger.Info("Database and instance disconnections completed")
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

	// Close database connection (synchronous since operations are now synchronous)
	if e.database != nil {
		if e.logger != nil {
			e.logger.Info("Closing database connection")
		}
		e.database.Close()
		if e.logger != nil {
			e.logger.Info("Database connection closed")
		}
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

// getServiceAddress returns the gRPC address for a service using dynamic resolution
func (e *Engine) getServiceAddress(serviceName string) string {
	return grpcconfig.GetServiceAddress(e.config, serviceName)
}
