package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	streamv1 "github.com/redbco/redb-open/api/proto/stream/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/grpcconfig"
	"github.com/redbco/redb-open/pkg/logger"
	internalconfig "github.com/redbco/redb-open/services/stream/internal/config"
	"github.com/redbco/redb-open/services/stream/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Engine struct {
	config     *config.Config
	grpcServer *grpc.Server
	database   *database.PostgreSQL
	coreConn   *grpc.ClientConn
	umConn     *grpc.ClientConn
	nodeID     string
	standalone bool
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

	// Register the service immediately when server is set
	if e.grpcServer != nil {
		serviceServer := NewServer(e)
		streamv1.RegisterStreamServiceServer(e.grpcServer, serviceServer)
	}
}

// SetLogger sets the logger for the engine
func (e *Engine) SetLogger(logger *logger.Logger) {
	e.logger = logger

	// Also set the logger on the GlobalState
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

	db, err := database.New(ctx, dbConfig)
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to initialize database: %v", err)
		}
		return fmt.Errorf("failed to initialize database: %w", err)
	}

	e.database = db

	// Get NodeID from the database localidentity table
	nodeID, err := e.getNodeIDFromDatabase(ctx)
	if err != nil {
		return fmt.Errorf("failed to get node ID from database: %w", err)
	}
	e.nodeID = nodeID

	if e.logger != nil {
		e.logger.Infof("Retrieved node ID from database: %s", e.nodeID)
	}

	// Initialize global state
	globalState := state.GetInstance()

	// Initialize gRPC connections to other services (unless standalone)
	if !e.standalone {
		// Initialize gRPC connection to Core service
		coreAddr := e.getServiceAddress("core")
		e.coreConn, err = grpc.Dial(coreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to Core service: %w", err)
		}

		// Initialize gRPC connection to UnifiedModel service
		umAddr := e.getServiceAddress("unifiedmodel")
		e.umConn, err = grpc.Dial(umAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to UnifiedModel service: %w", err)
		}

		// Create config repository with Core service connection
		configRepository := internalconfig.NewRepository(e.database.Pool(), e.coreConn)
		globalState.Initialize(configRepository, e.nodeID)
		globalState.SetDB(e.database)

		// Create context for watchers with cancellation
		e.watcherCtx, e.watcherCancel = context.WithCancel(ctx)

		// Perform initial stream connections
		if e.logger != nil {
			e.logger.Info("Loading existing stream configurations...")
		}

		streams, err := configRepository.ListStreams(e.watcherCtx)
		if err != nil {
			if e.logger != nil {
				e.logger.Warnf("Failed to list streams: %v", err)
			}
		} else {
			if e.logger != nil {
				e.logger.Infof("Found %d stream configurations", len(streams))
			}

			// Connect to each stream automatically
			for _, streamConfig := range streams {
				go e.connectToStream(e.watcherCtx, streamConfig)
			}
		}
	}

	e.state.isRunning = true

	if e.logger != nil {
		e.logger.Info("Stream engine started successfully")
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

	if e.logger != nil {
		e.logger.Info("Stopping stream engine...")
	}

	// Cancel watchers
	if e.watcherCancel != nil {
		e.watcherCancel()
	}

	// Disconnect all streams
	if !e.standalone {
		globalState := state.GetInstance()

		// Close all active connections
		streamIDs := globalState.GetAllStreamIDs()
		if e.logger != nil && len(streamIDs) > 0 {
			e.logger.Infof("Closing %d active stream connections...", len(streamIDs))
		}

		for _, streamID := range streamIDs {
			if conn, exists := globalState.GetConnection(streamID); exists {
				if err := conn.Close(); err != nil {
					if e.logger != nil {
						e.logger.Warnf("Error closing connection for stream %s: %v", streamID, err)
					}
				}
			}
		}

		// Update statuses in database
		configRepo := globalState.GetConfigRepository()
		if configRepo != nil {
			if e.logger != nil {
				e.logger.Info("Updating stream statuses during shutdown...")
			}

			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Update all stream statuses to DISCONNECTED
			for _, streamID := range globalState.GetAllStreamIDs() {
				if err := configRepo.UpdateStreamConnectionStatus(shutdownCtx, streamID, false, "Service shutdown"); err != nil {
					if e.logger != nil {
						e.logger.Errorf("Failed to update stream %s status during shutdown: %v", streamID, err)
					}
				}
				globalState.RemoveConnection(streamID)
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

	// Close database connection
	if e.database != nil {
		if e.logger != nil {
			e.logger.Info("Closing database connection")
		}
		e.database.Close()
	}

	e.state.Lock()
	e.state.isRunning = false
	e.state.Unlock()

	return nil
}

func (e *Engine) getNodeIDFromDatabase(ctx context.Context) (string, error) {
	var nodeID int64
	err := e.database.Pool().QueryRow(ctx, "SELECT identity_id FROM localidentity LIMIT 1").Scan(&nodeID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", fmt.Errorf("no node ID found in localidentity table")
		}
		return "", fmt.Errorf("failed to query node ID: %w", err)
	}
	return fmt.Sprintf("%d", nodeID), nil
}

func (e *Engine) getServiceAddress(serviceName string) string {
	return grpcconfig.GetServiceAddress(e.config, serviceName)
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

func (e *Engine) IncrementRequestsProcessed() {
	atomic.AddInt64(&e.metrics.requestsProcessed, 1)
}

func (e *Engine) IncrementErrors() {
	atomic.AddInt64(&e.metrics.errors, 1)
}

func (e *Engine) GetState() *state.State {
	return state.GetInstance()
}
