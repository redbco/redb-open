package engine

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/consensus"
	"github.com/redbco/redb-open/services/mesh/internal/mesh"
	"github.com/redbco/redb-open/services/mesh/internal/security"
	"github.com/redbco/redb-open/services/mesh/internal/storage"
	"google.golang.org/grpc"
)

// InitializationState represents the current initialization state of the mesh node
type InitializationState int

const (
	// StateUninitialized - no localidentity, nodes, or mesh defined in the database
	StateUninitialized InitializationState = iota
	// StateNodeOnly - localidentity and corresponding node exists but no mesh
	StateNodeOnly
	// StateFullyConfigured - localidentity, nodes and mesh all defined
	StateFullyConfigured
	// StateError - error state (e.g., localidentity exists but no matching node)
	StateError
)

// MeshInitInfo holds information about the mesh initialization state
type MeshInitInfo struct {
	State         InitializationState
	LocalIdentity *storage.LocalIdentity
	MeshInfo      *storage.MeshInfo
	NodeInfo      *storage.NodeInfo
	Routes        []*storage.RouteInfo
	ErrorMessage  string
}

type Engine struct {
	config     *config.Config
	grpcServer *grpc.Server
	meshNode   *mesh.Node
	storage    storage.Interface
	logger     *logger.Logger
	standalone bool
	db         *database.PostgreSQL
	initInfo   *MeshInitInfo // Store initialization info for shutdown operations

	// Consensus groups management
	consensusGroups map[string]*consensus.Group
	groupsMutex     sync.RWMutex

	state struct {
		sync.Mutex
		isRunning         bool
		ongoingOperations int32
		initState         InitializationState
	}
	metrics struct {
		requestsProcessed   int64
		errors              int64
		activeConnections   int64
		consensusOperations int64
	}
}

func NewEngine(cfg *config.Config, standalone bool) *Engine {
	return &Engine{
		config:          cfg,
		standalone:      standalone,
		consensusGroups: make(map[string]*consensus.Group),
	}
}

// SetLogger sets the unified logger for the engine
func (e *Engine) SetLogger(logger *logger.Logger) {
	e.logger = logger
}

// SetGRPCServer sets the shared gRPC server and registers the service immediately
func (e *Engine) SetGRPCServer(server *grpc.Server) {
	e.grpcServer = server

	// Register the service immediately when server is set (BEFORE serving starts)
	if e.grpcServer != nil {
		serviceServer := NewServer(e)
		meshv1.RegisterMeshServiceServer(e.grpcServer, serviceServer)
		meshv1.RegisterConsensusServiceServer(e.grpcServer, serviceServer)
	}
}

// SetDatabase sets the database connection for the engine
func (e *Engine) SetDatabase(db *database.PostgreSQL) {
	e.db = db
}

// GetDatabase returns the database connection
func (e *Engine) GetDatabase() *database.PostgreSQL {
	return e.db
}

// checkInitializationState examines the database to determine the current initialization state
func (e *Engine) checkInitializationState(ctx context.Context) (*MeshInitInfo, error) {
	initInfo := &MeshInitInfo{}

	// Check for local identity
	localIdentity, err := e.storage.GetLocalIdentity(ctx)
	if err != nil {
		initInfo.State = StateError
		initInfo.ErrorMessage = fmt.Sprintf("failed to query local identity: %v", err)
		return initInfo, nil
	}

	if localIdentity == nil {
		// No local identity - node is uninitialized
		initInfo.State = StateUninitialized
		return initInfo, nil
	}

	initInfo.LocalIdentity = localIdentity

	// Check for corresponding node information
	nodeInfo, err := e.storage.GetNodeInfo(ctx, localIdentity.IdentityID)
	if err != nil {
		initInfo.State = StateError
		initInfo.ErrorMessage = fmt.Sprintf("failed to query node info: %v", err)
		return initInfo, nil
	}

	if nodeInfo == nil {
		// Local identity exists but no matching node - error state
		initInfo.State = StateError
		initInfo.ErrorMessage = fmt.Sprintf("local identity %s exists but no matching node found", localIdentity.IdentityID)
		return initInfo, nil
	}

	initInfo.NodeInfo = nodeInfo

	// Check for mesh configuration
	meshInfo, err := e.storage.GetMeshInfo(ctx)
	if err != nil {
		initInfo.State = StateError
		initInfo.ErrorMessage = fmt.Sprintf("failed to query mesh info: %v", err)
		return initInfo, nil
	}

	if meshInfo == nil {
		// Node exists but no mesh - normal state for standalone nodes
		initInfo.State = StateNodeOnly
		return initInfo, nil
	}

	initInfo.MeshInfo = meshInfo

	// Check for routes
	routes, err := e.storage.GetRoutesForNode(ctx, nodeInfo.NodeID)
	if err != nil {
		initInfo.State = StateError
		initInfo.ErrorMessage = fmt.Sprintf("failed to query routes: %v", err)
		return initInfo, nil
	}

	initInfo.Routes = routes
	initInfo.State = StateFullyConfigured

	return initInfo, nil
}

// TODO: Move all SQL operations to a appropriate service

// startMeshRuntime starts the mesh runtime based on the initialization info
func (e *Engine) startMeshRuntime(ctx context.Context, initInfo *MeshInitInfo) error {
	if initInfo.MeshInfo == nil || initInfo.NodeInfo == nil {
		return fmt.Errorf("mesh info or node info is missing")
	}

	// Try to generate mesh credentials if storage is available
	if e.storage != nil {
		// Create credential manager
		credManager := security.NewCredentialManager(e.storage, e.logger)

		// Generate mesh credentials
		_, err := credManager.GenerateMeshCredentials(ctx, initInfo.MeshInfo.MeshID, initInfo.NodeInfo.NodeID)
		if err != nil {
			e.logger.Warnf("Failed to generate mesh credentials (continuing without credentials): %v", err)
			// Continue without credentials - mesh will work without TLS
		} else {
			e.logger.Infof("Generated mesh credentials successfully")
		}

		// Store runtime metadata
		runtimeMeta := map[string]interface{}{
			"initialization_time": time.Now(),
			"seed_node":           initInfo.NodeInfo.NodeID == initInfo.MeshInfo.MeshID, // Simplified logic
			"runtime_state":       "starting",
		}

		if err := e.storage.SaveConfig(ctx, "mesh_runtime_metadata", runtimeMeta); err != nil {
			e.logger.Warnf("Failed to store runtime metadata (continuing): %v", err)
		}
	} else {
		e.logger.Warnf("Storage not available - mesh will run without credentials and runtime metadata")
	}

	// Update mesh and node statuses to HEALTHY (mesh started successfully)
	if e.db != nil {
		// Update mesh status to HEALTHY
		_, err := e.db.Pool().Exec(ctx, `
			UPDATE mesh 
			SET status = $1, updated = CURRENT_TIMESTAMP
			WHERE mesh_id = $2
		`, "STATUS_HEALTHY", initInfo.MeshInfo.MeshID)
		if err != nil {
			e.logger.Warnf("Failed to update mesh status to HEALTHY: %v", err)
		}

		// Update local node status to HEALTHY
		_, err = e.db.Pool().Exec(ctx, `
			UPDATE nodes 
			SET status = $1, updated = CURRENT_TIMESTAMP
			WHERE node_id = $2
		`, "STATUS_HEALTHY", initInfo.NodeInfo.NodeID)
		if err != nil {
			e.logger.Warnf("Failed to update node status to HEALTHY: %v", err)
		}
	}

	// Note: Network layer is handled by the mesh node itself
	// The mesh node will initialize its own WebSocket network on the configured port

	// Get WebSocket port from configuration
	// Check for external_port from environment first, then fall back to config
	wsPortStr := os.Getenv("EXTERNAL_PORT")
	if wsPortStr == "" {
		wsPortStr = e.config.Get("services.mesh.external_port") // Fallback to config
	}
	if wsPortStr == "" {
		wsPortStr = "8443" // Default WebSocket port
	}
	wsPort, err := strconv.Atoi(wsPortStr)
	if err != nil {
		e.logger.Errorf("Invalid WebSocket port configuration: %v", err)
		return fmt.Errorf("invalid WebSocket port configuration: %v", err)
	}

	// Initialize mesh node (with or without credentials)
	meshConfig := mesh.Config{
		NodeID:        initInfo.NodeInfo.NodeID,
		MeshID:        initInfo.MeshInfo.MeshID,
		ListenAddress: fmt.Sprintf(":%d", wsPort),
		Heartbeat:     30 * time.Second,
		Timeout:       60 * time.Second,
	}

	meshNode, err := mesh.NewNode(meshConfig, e.storage, e.logger)
	if err != nil {
		// Update status to DEGRADED if mesh node creation fails
		if e.db != nil {
			_, updateErr := e.db.Pool().Exec(ctx, `
				UPDATE mesh SET status = $1, updated = CURRENT_TIMESTAMP WHERE mesh_id = $2
			`, "STATUS_DEGRADED", initInfo.MeshInfo.MeshID)
			if updateErr != nil {
				e.logger.Warnf("Failed to update mesh status to DEGRADED: %v", updateErr)
			}
			_, updateErr = e.db.Pool().Exec(ctx, `
				UPDATE nodes SET status = $1, updated = CURRENT_TIMESTAMP WHERE node_id = $2
			`, "STATUS_DEGRADED", initInfo.NodeInfo.NodeID)
			if updateErr != nil {
				e.logger.Warnf("Failed to update node status to DEGRADED: %v", updateErr)
			}
		}
		return fmt.Errorf("failed to create mesh node: %w", err)
	}

	// Start the mesh node
	if err := meshNode.Start(); err != nil {
		// Update status to DEGRADED if mesh node start fails
		if e.db != nil {
			_, updateErr := e.db.Pool().Exec(ctx, `
				UPDATE mesh SET status = $1, updated = CURRENT_TIMESTAMP WHERE mesh_id = $2
			`, "STATUS_DEGRADED", initInfo.MeshInfo.MeshID)
			if updateErr != nil {
				e.logger.Warnf("Failed to update mesh status to DEGRADED: %v", updateErr)
			}
			_, updateErr = e.db.Pool().Exec(ctx, `
				UPDATE nodes SET status = $1, updated = CURRENT_TIMESTAMP WHERE node_id = $2
			`, "STATUS_DEGRADED", initInfo.NodeInfo.NodeID)
			if updateErr != nil {
				e.logger.Warnf("Failed to update node status to DEGRADED: %v", updateErr)
			}
		}
		return fmt.Errorf("failed to start mesh node: %w", err)
	}

	// Update engine state
	e.meshNode = meshNode
	e.state.initState = StateFullyConfigured

	e.logger.Infof("Successfully started mesh runtime for mesh %s with node %s",
		initInfo.MeshInfo.MeshID, initInfo.NodeInfo.NodeID)

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

	// Use the logger provided by base service, don't create a new one
	if e.logger == nil {
		return fmt.Errorf("logger not set - call SetLogger first")
	}

	// RE-ENABLE: Storage initialization (testing component 1/3)
	// Initialize storage using the existing database connection (same as core service)
	if e.db != nil {
		var err error
		e.storage, err = storage.NewPostgresStorageWithDatabase(e.db, e.logger)
		if err != nil {
			e.logger.Errorf("Failed to initialize storage: %v", err)
			e.state.initState = StateError
			return fmt.Errorf("failed to initialize storage: %w", err)
		} else {
			e.logger.Infof("Storage initialized successfully with existing database connection")
		}
	} else {
		e.logger.Errorf("Database not available, storage is required for mesh service")
		e.state.initState = StateError
		return fmt.Errorf("database not available, storage is required for mesh service")
	}

	// Service is always marked as running so gRPC can receive requests
	e.state.isRunning = true

	// STAGE 1: Get initialization info and start basic mesh runtime (WebSocket connectivity only)
	initInfo, err := e.checkInitializationState(ctx)
	if err != nil {
		e.logger.Warnf("Failed to check initialization state (will retry later): %v", err)
		e.state.initState = StateError
		// Store empty initInfo to avoid nil pointer issues during shutdown
		e.initInfo = &MeshInitInfo{State: StateError, ErrorMessage: err.Error()}
	} else {
		e.state.initState = initInfo.State
		// Store initialization info for database operations during shutdown
		e.initInfo = initInfo
		e.logger.Infof("Mesh service started - initialization state: %v", initInfo.State)

		// STAGE 1: Start full mesh runtime for fully configured meshes
		if initInfo.State == StateFullyConfigured {
			e.logger.Infof("Mesh is fully configured, starting mesh runtime automatically")
			if err := e.startMeshRuntime(ctx, initInfo); err != nil {
				e.logger.Warnf("Failed to start mesh runtime (continuing without mesh): %v", err)
				// Don't fail startup completely - mesh service can still function without runtime
			}
		}
	}

	return nil
}

// GetInitializationState returns the current initialization state
func (e *Engine) GetInitializationState() InitializationState {
	e.state.Lock()
	defer e.state.Unlock()
	return e.state.initState
}

// IsFullyInitialized returns true if the mesh node is fully configured and running
func (e *Engine) IsFullyInitialized() bool {
	e.state.Lock()
	defer e.state.Unlock()
	return e.state.initState == StateFullyConfigured && e.meshNode != nil
}

func (e *Engine) Stop(ctx context.Context) error {
	e.state.Lock()
	defer e.state.Unlock()

	if !e.state.isRunning {
		return nil
	}

	e.state.isRunning = false

	if e.logger != nil {
		e.logger.Info("Stopping mesh engine")
	}

	// Update database statuses FIRST to ensure they complete before context cancellation
	if e.db != nil {
		if e.logger != nil {
			e.logger.Info("Updating database statuses during shutdown...")
		}

		// Use a separate context for database operations to avoid cancellation issues
		dbCtx := context.Background()
		shutdownCtx, cancel := context.WithTimeout(dbCtx, 1*time.Second)
		defer cancel()

		// Set mesh status to DISCONNECTED
		_, err := e.db.Pool().Exec(shutdownCtx, `
			UPDATE mesh 
			SET status = $1, updated = CURRENT_TIMESTAMP
		`, "STATUS_DISCONNECTED")
		if err != nil {
			if e.logger != nil {
				e.logger.Warnf("Failed to update mesh status to DISCONNECTED: %v", err)
			}
		} else if e.logger != nil {
			e.logger.Info("Successfully updated mesh status to DISCONNECTED")
		}

		// Set local node status to STOPPED (use initInfo.NodeInfo if available)
		var nodeID string
		if e.meshNode != nil {
			nodeID = e.meshNode.GetID()
		} else if e.initInfo != nil && e.initInfo.NodeInfo != nil {
			nodeID = e.initInfo.NodeInfo.NodeID
		}

		if nodeID != "" {
			_, err := e.db.Pool().Exec(shutdownCtx, `
				UPDATE nodes 
				SET status = $1, updated = CURRENT_TIMESTAMP
				WHERE node_id = $2
			`, "STATUS_STOPPED", nodeID)
			if err != nil {
				if e.logger != nil {
					e.logger.Warnf("Failed to update local node status to STOPPED: %v", err)
				}
			} else if e.logger != nil {
				e.logger.Info("Successfully updated local node status to STOPPED")
			}
		} else if e.logger != nil {
			e.logger.Warn("Cannot update local node status: no node ID available (node not initialized)")
		}

		// Set all other nodes to UNKNOWN (if we have a node ID)
		if nodeID != "" {
			_, err = e.db.Pool().Exec(shutdownCtx, `
				UPDATE nodes 
				SET status = $1, updated = CURRENT_TIMESTAMP
				WHERE node_id != $2
			`, "STATUS_UNKNOWN", nodeID)
			if err != nil {
				if e.logger != nil {
					e.logger.Warnf("Failed to update other nodes status to UNKNOWN: %v", err)
				}
			} else if e.logger != nil {
				e.logger.Info("Successfully updated other nodes status to UNKNOWN")
			}
		}

		if e.logger != nil {
			e.logger.Info("Database status updates completed")
		}
	}

	// Step 1: Stop all consensus groups (simplified handling)
	e.groupsMutex.Lock()
	for groupID := range e.consensusGroups {
		if e.logger != nil {
			e.logger.Infof("Stopping consensus group (group_id: %s)", groupID)
		}
		delete(e.consensusGroups, groupID)
	}
	e.groupsMutex.Unlock()

	// Step 2: Stop mesh node with timeout
	if e.meshNode != nil {
		if e.logger != nil {
			e.logger.Info("Stopping mesh node")
		}

		// Use timeout for mesh node shutdown to prevent hanging
		meshNodeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		// Create a channel to signal completion
		done := make(chan error, 1)
		go func() {
			done <- e.meshNode.Stop()
		}()

		select {
		case err := <-done:
			if err != nil && e.logger != nil {
				e.logger.Errorf("Failed to stop mesh node: %v", err)
			} else if e.logger != nil {
				e.logger.Info("Mesh node stopped successfully")
			}
		case <-meshNodeCtx.Done():
			if e.logger != nil {
				e.logger.Warn("Mesh node shutdown timed out, forcing stop")
			}
			// Force stop by canceling context
			e.meshNode.CancelContext()
		}
	}

	// Note: Network layer shutdown is handled by the mesh node itself

	if e.logger != nil {
		e.logger.Info("Mesh runtime components cleanup completed")
	}

	// Close storage
	if e.storage != nil {
		if err := e.storage.Close(); err != nil && e.logger != nil {
			e.logger.Errorf("Failed to close storage: %v", err)
		}
	}

	// Log successful stop
	if e.logger != nil {
		e.logger.Info("Mesh engine stopped successfully (with storage + database status updates)")
	}

	return nil
}

func (e *Engine) GetMetrics() map[string]int64 {
	e.groupsMutex.RLock()
	groupCount := int64(len(e.consensusGroups))
	e.groupsMutex.RUnlock()

	return map[string]int64{
		"requests_processed":     atomic.LoadInt64(&e.metrics.requestsProcessed),
		"errors":                 atomic.LoadInt64(&e.metrics.errors),
		"active_connections":     atomic.LoadInt64(&e.metrics.activeConnections),
		"consensus_operations":   atomic.LoadInt64(&e.metrics.consensusOperations),
		"consensus_groups_count": groupCount,
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

func (e *Engine) CheckStorage() error {
	e.state.Lock()
	defer e.state.Unlock()

	if !e.state.isRunning {
		return fmt.Errorf("service not initialized")
	}

	if e.storage == nil {
		return fmt.Errorf("storage not initialized")
	}

	// Test storage connectivity by trying to store and retrieve a test value
	testKey := "health_check"
	testValue := []byte("test")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := e.storage.StoreState(ctx, testKey, testValue); err != nil {
		return fmt.Errorf("storage write test failed: %w", err)
	}

	if _, err := e.storage.GetState(ctx, testKey); err != nil {
		return fmt.Errorf("storage read test failed: %w", err)
	}

	// Clean up test data
	if err := e.storage.DeleteState(ctx, testKey); err != nil {
		e.logger.Warnf("Failed to clean up test data: %v", err)
	}

	return nil
}

func (e *Engine) CheckMeshNode() error {
	e.state.Lock()
	defer e.state.Unlock()

	if !e.state.isRunning {
		return fmt.Errorf("service not initialized")
	}

	if e.meshNode == nil {
		return fmt.Errorf("mesh node not initialized")
	}

	return nil
}

func (e *Engine) CheckConsensusGroups() error {
	e.state.Lock()
	defer e.state.Unlock()

	if !e.state.isRunning {
		return fmt.Errorf("service not initialized")
	}

	return nil
}

func (e *Engine) TrackOperation() {
	atomic.AddInt32(&e.state.ongoingOperations, 1)
	atomic.AddInt64(&e.metrics.requestsProcessed, 1)
}

func (e *Engine) UntrackOperation() {
	atomic.AddInt32(&e.state.ongoingOperations, -1)
}

func (e *Engine) GetMeshNode() *mesh.Node {
	return e.meshNode
}

func (e *Engine) GetLogger() *logger.Logger {
	return e.logger
}

func (e *Engine) GetConsensusGroup(groupID string) (*consensus.Group, bool) {
	e.groupsMutex.RLock()
	defer e.groupsMutex.RUnlock()
	group, exists := e.consensusGroups[groupID]
	return group, exists
}

func (e *Engine) AddConsensusGroup(groupID string, group *consensus.Group) {
	e.groupsMutex.Lock()
	defer e.groupsMutex.Unlock()
	e.consensusGroups[groupID] = group
	atomic.AddInt64(&e.metrics.consensusOperations, 1)
}

// GetStorage returns the storage instance
func (e *Engine) GetStorage() storage.Interface {
	return e.storage
}

func (e *Engine) RemoveConsensusGroup(groupID string) {
	e.groupsMutex.Lock()
	defer e.groupsMutex.Unlock()
	delete(e.consensusGroups, groupID)
}
