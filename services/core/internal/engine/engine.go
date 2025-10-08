package engine

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	unifiedmodelv1 "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/grpcconfig"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/core/internal/mesh"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type Engine struct {
	config            *config.Config
	grpcServer        *grpc.Server
	coreSvc           *Server
	db                *database.PostgreSQL
	logger            *logger.Logger
	umClient          unifiedmodelv1.UnifiedModelServiceClient
	anchorClient      anchorv1.AnchorServiceClient
	meshControlClient meshv1.MeshControlClient
	meshDataClient    meshv1.MeshDataClient

	// Store gRPC connections for cleanup
	umConn     *grpc.ClientConn
	anchorConn *grpc.ClientConn
	meshConn   *grpc.ClientConn

	// Mesh components
	meshManager      *mesh.MeshCommunicationManager
	eventManager     *mesh.MeshEventManager
	consensusChecker *mesh.ConsensusChecker
	syncManager      *mesh.DatabaseSyncManager
	nodeID           uint64

	state struct {
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

// SetGRPCServer sets the shared gRPC server and registers all v2 services immediately
func (e *Engine) SetGRPCServer(server *grpc.Server) {
	e.grpcServer = server
	// Register services immediately when the gRPC server is available
	if server != nil {
		if err := e.RegisterCoreServices(); err != nil {
			if e.logger != nil {
				e.logger.Errorf("Failed to register core services: %v", err)
			}
		} else {
			if e.logger != nil {
				e.logger.Infof("Core services registered successfully")
			}
		}
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

// RegisterCoreServices registers all core services with the gRPC server
func (e *Engine) RegisterCoreServices() error {
	if e.grpcServer == nil {
		return fmt.Errorf("gRPC server not provided to engine")
	}

	e.coreSvc = NewServer(e)
	corev1.RegisterMeshServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterWorkspaceServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterSatelliteServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterAnchorServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterRegionServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterEnvironmentServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterInstanceServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterDatabaseServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterRepoServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterBranchServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterCommitServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterMappingServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterRelationshipServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterTransformationServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterPolicyServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterMCPServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterTenantServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterUserServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterTokenServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterGroupServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterRoleServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterPermissionServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterAssignmentServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterAuthorizationServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterTemplateServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterAuditServiceServer(e.grpcServer, e.coreSvc)
	corev1.RegisterImportExportServiceServer(e.grpcServer, e.coreSvc)

	return nil
}

func (e *Engine) Start(ctx context.Context) error {
	if e.logger != nil {
		e.logger.Info("Starting core engine")
	}

	e.state.Lock()
	if e.state.isRunning {
		e.state.Unlock()
		return fmt.Errorf("engine is already running")
	}
	e.state.isRunning = true
	e.state.Unlock()

	if e.logger != nil {
		e.logger.Debug("Setting engine state to running")
	}

	// Core services should already be registered by this point
	if e.grpcServer == nil {
		return fmt.Errorf("gRPC server not provided to engine")
	}

	if e.logger != nil {
		e.logger.Debug("gRPC server check passed")
	}

	// Initialize gRPC client connections with timeouts
	if e.logger != nil {
		e.logger.Debug("Initializing gRPC client connections")
	}

	// Initialize all gRPC service connections
	if err := e.initializeAllClients(ctx); err != nil {
		e.logger.Warnf("Failed to initialize some gRPC clients: %v", err)
		// Continue - individual client failures are handled gracefully
	}

	if e.logger != nil {
		e.logger.Debug("Getting node ID from database")
	}

	// Get node ID from database
	nodeID, err := e.getNodeIDFromDatabase(ctx)
	if err != nil {
		e.logger.Warnf("Failed to get node ID from database, using default: %v", err)
		nodeID = uint64(1) // Default fallback
	}
	e.nodeID = nodeID

	if e.logger != nil {
		e.logger.Infof("Using node ID: %d", nodeID)
	}

	// Initialize mesh communication manager
	e.meshManager = mesh.NewMeshCommunicationManager(
		e.meshDataClient,
		e.meshControlClient,
		e.logger,
		e.nodeID,
	)

	// Initialize consensus checker
	e.consensusChecker = mesh.NewConsensusChecker(
		e.db,
		e.meshManager,
		e.logger,
		e.nodeID,
	)

	// Initialize database sync manager
	e.syncManager = mesh.NewDatabaseSyncManager(
		e.db,
		e.meshManager,
		e.logger,
		e.nodeID,
	)

	// Initialize mesh event manager
	e.eventManager = mesh.NewMeshEventManager(
		e.db,
		e.meshManager,
		e.logger,
		e.nodeID,
	)

	// Set circular dependencies
	e.logger.Debug("Setting up circular dependencies")
	e.eventManager.SetConsensusChecker(e.consensusChecker)
	e.eventManager.SetSyncManager(e.syncManager)
	e.meshManager.SetEventManager(e.eventManager)
	e.logger.Debug("Circular dependencies configured")

	// Enable console logging for startup debugging
	e.logger.EnableConsoleOutput()

	// Start mesh communication manager (only if mesh clients are available)
	if e.meshDataClient != nil && e.meshControlClient != nil {
		e.logger.Debug("Starting mesh manager")

		// Start mesh manager with the main context (not a timeout context)
		// The mesh manager needs a long-lived context for its subscriptions
		if err := e.meshManager.Start(ctx); err != nil {
			e.logger.Warnf("Failed to start mesh communication manager: %v", err)
			// Continue without mesh communication - this is a degraded mode
		} else {
			e.logger.Info("Mesh communication manager started successfully")
		}
	} else {
		e.logger.Warnf("Mesh clients not available, running in degraded mode without mesh communication")
	}

	// Start mesh event manager (only if mesh manager is available)
	if e.meshManager != nil {
		if err := e.eventManager.Start(ctx); err != nil {
			e.logger.Warnf("Failed to start mesh event manager: %v", err)
			// Continue without mesh events - this is a degraded mode
		} else {
			e.logger.Infof("Mesh event manager started successfully")
		}
	} else {
		e.logger.Warnf("Mesh manager not available, running without mesh event processing")
	}

	// Update node and mesh status based on clean node status
	isCleanNode, err := e.isCleanNode(ctx)
	if err != nil {
		e.logger.Warnf("Failed to determine clean node status: %v", err)
		isCleanNode = true // Default to clean node if check fails
	}

	if isCleanNode {
		// Update node status to CLEAN
		if err := e.updateLocalNodeStatus(ctx, "STATUS_CLEAN"); err != nil {
			e.logger.Warnf("Failed to update node status to CLEAN: %v", err)
		}
	} else {
		// Update node status to ACTIVE
		if err := e.updateLocalNodeStatus(ctx, "STATUS_ACTIVE"); err != nil {
			e.logger.Warnf("Failed to update node status to ACTIVE: %v", err)
		}

		// Update mesh status to ACTIVE
		if err := e.updateLocalMeshStatus(ctx, "STATUS_ACTIVE"); err != nil {
			e.logger.Warnf("Failed to update mesh status to ACTIVE: %v", err)
		}
	}

	// Message handlers are automatically registered by the mesh manager

	if e.logger != nil {
		e.logger.Info("Core engine started successfully")
	}

	return nil
}

func (e *Engine) Stop(ctx context.Context) error {
	// Enable console logging to ensure we can see shutdown progress
	if e.logger != nil {
		e.logger.EnableConsoleOutput()
		e.logger.Info("Shutting down core engine")
		e.logger.Debug("Console logging enabled for engine shutdown")
	}

	e.state.Lock()
	if !e.state.isRunning {
		e.state.Unlock()
		if e.logger != nil {
			e.logger.Infof("Core engine already stopped")
		}
		return nil
	}
	e.state.isRunning = false
	e.state.Unlock()

	if e.logger != nil {
		e.logger.Debug("Setting engine state to stopped")
	}

	// Use a separate context for shutdown operations to avoid cancellation issues
	// (following the same pattern as anchor service)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	// Update node and mesh status during shutdown
	if e.logger != nil {
		e.logger.Debug("Determining node clean status")
	}
	isCleanNode, err := e.isCleanNode(shutdownCtx)
	if err != nil {
		e.logger.Warnf("Failed to determine clean node status during shutdown: %v", err)
		isCleanNode = true // Default to clean node if check fails
	}

	if e.logger != nil {
		e.logger.Debug("Node clean status determined: %t", isCleanNode)
	}

	if isCleanNode {
		if e.logger != nil {
			e.logger.Debug("Updating node status to clean")
		}
		// Update node status to CLEAN
		if err := e.updateLocalNodeStatus(shutdownCtx, "STATUS_CLEAN"); err != nil {
			e.logger.Warnf("Failed to update node status to CLEAN during shutdown: %v", err)
		} else {
			e.logger.Debug("Successfully updated node status to clean")
		}
	} else {
		if e.logger != nil {
			e.logger.Debug("Updating node status to stopped")
		}
		// Update node status to STOPPED
		if err := e.updateLocalNodeStatus(shutdownCtx, "STATUS_STOPPED"); err != nil {
			e.logger.Warnf("Failed to update node status to STOPPED during shutdown: %v", err)
		} else {
			e.logger.Debug("Successfully updated node status to stopped")
		}

		if e.logger != nil {
			e.logger.Debug("Updating mesh status to disconnected")
		}
		// Update mesh status to DISCONNECTED
		if err := e.updateLocalMeshStatus(shutdownCtx, "STATUS_DISCONNECTED"); err != nil {
			e.logger.Warnf("Failed to update mesh status to DISCONNECTED during shutdown: %v", err)
		} else {
			e.logger.Debug("Successfully updated mesh status to disconnected")
		}
	}

	// Stop mesh components in proper order with improved error handling
	// Use the shutdown context for all operations to ensure proper cancellation

	// Stop mesh event manager first (it depends on mesh manager)
	if e.eventManager != nil {
		if e.logger != nil {
			e.logger.Debug("Stopping mesh event manager")
		}

		if err := e.eventManager.Stop(); err != nil {
			if e.logger != nil {
				e.logger.Errorf("Failed to stop mesh event manager: %v", err)
			}
		} else if e.logger != nil {
			e.logger.Debug("Mesh event manager stopped successfully")
		}
	}

	// Stop mesh communication manager
	if e.meshManager != nil {
		if e.logger != nil {
			e.logger.Debug("Stopping mesh communication manager")
		}

		if err := e.meshManager.Stop(); err != nil {
			if e.logger != nil {
				e.logger.Errorf("Failed to stop mesh communication manager: %v", err)
			}
		} else if e.logger != nil {
			e.logger.Debug("Mesh communication manager stopped successfully")
		}
	}

	// Close all gRPC connections
	if e.logger != nil {
		e.logger.Debug("Closing all gRPC connections")
	}
	e.closeAllConnections()
	if e.logger != nil {
		e.logger.Debug("All gRPC connections closed")
	}

	// Note: ConsensusChecker and DatabaseSyncManager don't have background goroutines
	// that need explicit stopping, but we log their shutdown for completeness
	if e.consensusChecker != nil && e.logger != nil {
		e.logger.Debug("Consensus checker shutdown completed")
	}

	if e.syncManager != nil && e.logger != nil {
		e.logger.Debug("Database sync manager shutdown completed")
	}

	// Note: We don't stop the gRPC server here since it's shared
	// The BaseService will handle stopping the server

	if e.logger != nil {
		e.logger.Info("Core engine shutdown completed successfully")
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
		return fmt.Errorf("engine not running")
	}

	// Basic health check - engine is running and gRPC server is available
	if e.grpcServer == nil {
		return fmt.Errorf("gRPC server not available")
	}

	return nil
}

func (e *Engine) GetUnifiedModelClient() unifiedmodelv1.UnifiedModelServiceClient {
	return e.umClient
}

func (e *Engine) GetAnchorClient() anchorv1.AnchorServiceClient {
	return e.anchorClient
}

func (e *Engine) GetMeshControlClient() meshv1.MeshControlClient {
	return e.meshControlClient
}

func (e *Engine) GetMeshDataClient() meshv1.MeshDataClient {
	return e.meshDataClient
}

// TrackOperation increments the ongoing operations counter
func (e *Engine) TrackOperation() {
	atomic.AddInt32(&e.state.ongoingOperations, 1)
}

// UntrackOperation decrements the ongoing operations counter
func (e *Engine) UntrackOperation() {
	atomic.AddInt32(&e.state.ongoingOperations, -1)
}

// IncrementRequestsProcessed increments the requests processed counter
func (e *Engine) IncrementRequestsProcessed() {
	atomic.AddInt64(&e.metrics.requestsProcessed, 1)
}

// IncrementErrors increments the errors counter
func (e *Engine) IncrementErrors() {
	atomic.AddInt64(&e.metrics.errors, 1)
}

// Mesh component getters (return nil since components are disabled for testing)
func (e *Engine) GetMeshEventManager() interface{} {
	return e.eventManager
}

func (e *Engine) GetDatabaseSyncManager() interface{} {
	return e.syncManager
}

func (e *Engine) GetMeshManager() interface{} {
	return e.meshManager
}

func (e *Engine) GetConsensusChecker() interface{} {
	return e.consensusChecker
}

// Note: Message handlers are implemented in the MeshCommunicationManager

// Helper functions

// getNodeIDFromDatabase retrieves the node ID from the database
func (e *Engine) getNodeIDFromDatabase(ctx context.Context) (uint64, error) {
	if e.db == nil {
		return 0, fmt.Errorf("database not available")
	}

	// Query the node ID from nodes table via localidentity join
	// node_id now serves as both the identifier and routing ID
	var nodeID int64
	query := `
		SELECT n.node_id 
		FROM nodes n
		JOIN localidentity li ON n.node_id = li.identity_id
		LIMIT 1
	`

	err := e.db.Pool().QueryRow(ctx, query).Scan(&nodeID)
	if err != nil {
		return 0, fmt.Errorf("failed to query node ID from nodes via localidentity: %w", err)
	}

	return uint64(nodeID), nil
}

// parseNodeID converts a string node ID to uint64
func parseNodeID(nodeIDStr string) (uint64, error) {
	return strconv.ParseUint(nodeIDStr, 10, 64)
}

// updateLocalNodeStatus updates the status of the local node
func (e *Engine) updateLocalNodeStatus(ctx context.Context, status string) error {
	if e.db == nil {
		return fmt.Errorf("database not available")
	}

	// Use a separate context with timeout to avoid cancellation issues
	updateCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Get the local node ID from localidentity (BIGINT)
	var nodeID int64
	query := `SELECT identity_id FROM localidentity LIMIT 1`
	err := e.db.Pool().QueryRow(updateCtx, query).Scan(&nodeID)
	if err != nil {
		return fmt.Errorf("failed to get local node ID: %w", err)
	}

	// Update the node status
	updateQuery := `UPDATE nodes SET status = $1, updated = NOW() WHERE node_id = $2`
	_, err = e.db.Pool().Exec(updateCtx, updateQuery, status, nodeID)
	if err != nil {
		return fmt.Errorf("failed to update node status: %w", err)
	}

	if e.logger != nil {
		e.logger.Infof("Successfully updated node %d status to %s", nodeID, status)
	}

	return nil
}

// updateLocalMeshStatus updates the status of the mesh(es) associated with the local node
func (e *Engine) updateLocalMeshStatus(ctx context.Context, status string) error {
	if e.db == nil {
		return fmt.Errorf("database not available")
	}

	// Use a separate context with timeout to avoid cancellation issues
	updateCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Get the local node ID from localidentity (BIGINT)
	var nodeID int64
	query := `SELECT identity_id FROM localidentity LIMIT 1`
	err := e.db.Pool().QueryRow(updateCtx, query).Scan(&nodeID)
	if err != nil {
		return fmt.Errorf("failed to get local node ID: %w", err)
	}

	// Check if the node has a clean status (not part of any mesh)
	var nodeStatus string
	nodeQuery := `SELECT status FROM nodes WHERE node_id = $1`
	err = e.db.Pool().QueryRow(updateCtx, nodeQuery, nodeID).Scan(&nodeStatus)
	if err != nil {
		return fmt.Errorf("failed to get node status: %w", err)
	}

	if nodeStatus == "STATUS_CLEAN" {
		if e.logger != nil {
			e.logger.Infof("Node %d is clean (not part of any mesh), skipping mesh status update", nodeID)
		}
		return nil
	}

	// Update all meshes associated with this node
	updateQuery := `
		UPDATE mesh 
		SET status = $1, updated = NOW() 
		WHERE mesh_id IN (
			SELECT mesh_id FROM mesh_node_membership WHERE node_id = $2
		)`
	result, err := e.db.Pool().Exec(updateCtx, updateQuery, status, nodeID)
	if err != nil {
		return fmt.Errorf("failed to update mesh status: %w", err)
	}

	rowsAffected := result.RowsAffected()
	if e.logger != nil {
		e.logger.Infof("Successfully updated %d mesh(es) status to %s for node %d", rowsAffected, status, nodeID)
	}

	return nil
}

// isCleanNode checks if the node is part of any mesh
func (e *Engine) isCleanNode(ctx context.Context) (bool, error) {
	if e.db == nil {
		return true, fmt.Errorf("database not available")
	}

	// Use a separate context with timeout
	checkCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Check if there are any meshes in the database
	var meshCount int
	query := `SELECT COUNT(*) FROM mesh`
	err := e.db.Pool().QueryRow(checkCtx, query).Scan(&meshCount)
	if err != nil {
		return true, fmt.Errorf("failed to check mesh count: %w", err)
	}

	return meshCount == 0, nil
}

// initializeAllClients initializes gRPC clients for all service communications
func (e *Engine) initializeAllClients(ctx context.Context) error {
	var errors []string

	// Initialize UnifiedModel service connection
	if err := e.initializeUnifiedModelClient(ctx); err != nil {
		e.logger.Warnf("Failed to initialize UnifiedModel client: %v", err)
		errors = append(errors, fmt.Sprintf("UnifiedModel: %v", err))
	}

	// Initialize Anchor service connection
	if err := e.initializeAnchorClient(ctx); err != nil {
		e.logger.Warnf("Failed to initialize Anchor client: %v", err)
		errors = append(errors, fmt.Sprintf("Anchor: %v", err))
	}

	// Initialize Mesh service connection
	if err := e.initializeMeshClients(ctx); err != nil {
		e.logger.Warnf("Failed to initialize Mesh clients: %v", err)
		errors = append(errors, fmt.Sprintf("Mesh: %v", err))
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to initialize some clients: %v", errors)
	}

	e.logger.Infof("Successfully initialized all gRPC clients")
	return nil
}

// initializeUnifiedModelClient initializes the UnifiedModel service gRPC client
func (e *Engine) initializeUnifiedModelClient(ctx context.Context) error {
	address := e.getServiceAddress("unifiedmodel")
	e.logger.Infof("Connecting to UnifiedModel service at %s", address)

	conn, err := e.createGRPCConnection(ctx, address, "UnifiedModel")
	if err != nil {
		return err
	}

	e.umConn = conn
	e.umClient = unifiedmodelv1.NewUnifiedModelServiceClient(conn)
	e.logger.Infof("Successfully connected to UnifiedModel service at %s", address)
	return nil
}

// initializeAnchorClient initializes the Anchor service gRPC client
func (e *Engine) initializeAnchorClient(ctx context.Context) error {
	address := e.getServiceAddress("anchor")
	e.logger.Infof("Connecting to Anchor service at %s", address)

	conn, err := e.createGRPCConnection(ctx, address, "Anchor")
	if err != nil {
		return err
	}

	e.anchorConn = conn
	e.anchorClient = anchorv1.NewAnchorServiceClient(conn)
	e.logger.Infof("Successfully connected to Anchor service at %s", address)
	return nil
}

// initializeMeshClients initializes gRPC clients for mesh service communication
func (e *Engine) initializeMeshClients(ctx context.Context) error {
	address := e.getServiceAddress("mesh")
	e.logger.Infof("Connecting to Mesh service at %s", address)

	conn, err := e.createGRPCConnection(ctx, address, "Mesh")
	if err != nil {
		return err
	}

	e.meshConn = conn
	e.meshDataClient = meshv1.NewMeshDataClient(conn)
	e.meshControlClient = meshv1.NewMeshControlClient(conn)
	e.logger.Infof("Successfully connected to Mesh service at %s", address)
	return nil
}

// createGRPCConnection creates a gRPC connection with standard settings
func (e *Engine) createGRPCConnection(ctx context.Context, address, serviceName string) (*grpc.ClientConn, error) {
	e.logger.Infof("Attempting to connect to %s service at %s...", serviceName, address)

	// Create connection with timeout
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Dial service with timeout and keepalive
	conn, err := grpc.DialContext(dialCtx, address,
		grpc.WithInsecure(), // TODO: Add TLS support based on config
		grpc.WithBlock(),    // Wait for connection to be established
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		e.logger.Errorf("Failed to connect to %s service at %s: %v", serviceName, address, err)
		return nil, fmt.Errorf("failed to connect to %s service at %s: %w", serviceName, address, err)
	}

	e.logger.Infof("Successfully established gRPC connection to %s service at %s", serviceName, address)
	return conn, nil
}

// getServiceAddress returns the gRPC address for a service using dynamic resolution
func (e *Engine) getServiceAddress(serviceName string) string {
	return grpcconfig.GetServiceAddress(e.config, serviceName)
}

// closeAllConnections closes all gRPC connections gracefully
func (e *Engine) closeAllConnections() {
	if e.logger != nil {
		e.logger.Infof("Closing all gRPC connections...")
	}

	// Close UnifiedModel connection
	if e.umConn != nil {
		if e.logger != nil {
			e.logger.Infof("Closing UnifiedModel gRPC connection...")
		}
		if err := e.umConn.Close(); err != nil {
			e.logger.Warnf("Failed to close UnifiedModel gRPC connection: %v", err)
		} else {
			e.logger.Infof("UnifiedModel gRPC connection closed successfully")
		}
	}

	// Close Anchor connection
	if e.anchorConn != nil {
		if e.logger != nil {
			e.logger.Infof("Closing Anchor gRPC connection...")
		}
		if err := e.anchorConn.Close(); err != nil {
			e.logger.Warnf("Failed to close Anchor gRPC connection: %v", err)
		} else {
			e.logger.Infof("Anchor gRPC connection closed successfully")
		}
	}

	// Close Mesh connection
	if e.meshConn != nil {
		if e.logger != nil {
			e.logger.Infof("Closing Mesh gRPC connection...")
		}
		if err := e.meshConn.Close(); err != nil {
			e.logger.Warnf("Failed to close Mesh gRPC connection: %v", err)
		} else {
			e.logger.Infof("Mesh gRPC connection closed successfully")
		}
	}

	if e.logger != nil {
		e.logger.Infof("All gRPC connections closed")
	}
}
