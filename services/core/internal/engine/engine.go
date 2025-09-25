package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	unifiedmodelv1 "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/core/internal/mesh"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	meshManager       *mesh.MeshCommunicationManager
	nodeID            uint64
	state             struct {
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
	e.state.Lock()
	if e.state.isRunning {
		e.state.Unlock()
		return fmt.Errorf("engine is already running")
	}
	e.state.isRunning = true
	e.state.Unlock()

	// Core services should already be registered by this point
	if e.grpcServer == nil {
		return fmt.Errorf("gRPC server not provided to engine")
	}

	// Initialize unified model client
	umAddr := e.config.Get("services.unifiedmodel.grpc_address")
	if umAddr == "" {
		// TODO: make this dynamic
		umAddr = "localhost:50052" // default unifiedmodel service port
	}

	umConn, err := grpc.Dial(umAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to unified model service: %w", err)
	}

	e.umClient = unifiedmodelv1.NewUnifiedModelServiceClient(umConn)

	// Initialize anchor client
	anchorAddr := e.config.Get("services.anchor.grpc_address")
	if anchorAddr == "" {
		// TODO: make this dynamic
		anchorAddr = "localhost:50057" // default anchor service port
	}

	anchorConn, err := grpc.Dial(anchorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to anchor service: %w", err)
	}

	e.anchorClient = anchorv1.NewAnchorServiceClient(anchorConn)

	// Initialize mesh client
	meshAddr := e.config.Get("services.mesh.grpc_address")
	if meshAddr == "" {
		// TODO: make this dynamic
		meshAddr = "localhost:50056" // default mesh service port
	}

	meshConn, err := grpc.Dial(meshAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to mesh service: %w", err)
	}

	e.meshControlClient = meshv1.NewMeshControlClient(meshConn)
	e.meshDataClient = meshv1.NewMeshDataClient(meshConn)

	// Get node ID from database first, then fallback to config
	if nodeID, err := e.getNodeIDFromDatabase(ctx); err == nil {
		e.nodeID = nodeID
		e.logger.Infof("Using node ID from database: %d", nodeID)
	} else {
		e.logger.Warnf("Failed to get node ID from database, using config: %v", err)
		// Fallback to config
		nodeIDStr := e.config.Get("services.mesh.node_id")
		if nodeIDStr == "" {
			e.nodeID = 1 // Default node ID, should be configured properly
			e.logger.Warn("No node ID in config, using default: 1")
		} else {
			// Parse node ID from config
			if nodeID, err := parseNodeID(nodeIDStr); err == nil {
				e.nodeID = nodeID
				e.logger.Infof("Using node ID from config: %d", nodeID)
			} else {
				e.nodeID = 1 // Fallback
				e.logger.Warnf("Failed to parse node ID from config, using default: 1")
			}
		}
	}

	// Initialize mesh communication manager
	e.meshManager = mesh.NewMeshCommunicationManager(
		e.meshDataClient,
		e.meshControlClient,
		e.logger,
		e.nodeID,
	)

	// Start mesh communication manager
	if err := e.meshManager.Start(ctx); err != nil {
		return fmt.Errorf("failed to start mesh communication manager: %w", err)
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

	// Stop mesh communication manager
	if e.meshManager != nil {
		if err := e.meshManager.Stop(); err != nil {
			if e.logger != nil {
				e.logger.Errorf("Failed to stop mesh communication manager: %v", err)
			}
		}
	}

	// Note: We don't stop the gRPC server here since it's shared
	// The BaseService will handle stopping the server

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

	if e.db == nil {
		return fmt.Errorf("database not initialized")
	}

	// Test database connection
	return e.db.Pool().Ping(context.Background())
}

func (e *Engine) TrackOperation() {
	atomic.AddInt32(&e.state.ongoingOperations, 1)
	atomic.AddInt64(&e.metrics.requestsProcessed, 1)
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

// GetUnifiedModelClient returns the unified model client
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

// GetMeshManager returns the mesh communication manager
func (e *Engine) GetMeshManager() *mesh.MeshCommunicationManager {
	return e.meshManager
}

// parseNodeID parses a node ID from string to uint64
func parseNodeID(nodeIDStr string) (uint64, error) {
	// Simple implementation - in production, this should be more robust
	if nodeIDStr == "" {
		return 0, fmt.Errorf("empty node ID")
	}

	// For now, just use a simple conversion
	// In production, this could parse from various formats
	var nodeID uint64
	if _, err := fmt.Sscanf(nodeIDStr, "%d", &nodeID); err != nil {
		return 0, fmt.Errorf("invalid node ID format: %w", err)
	}

	return nodeID, nil
}

// getNodeIDFromDatabase retrieves the local node ID from the database
func (e *Engine) getNodeIDFromDatabase(ctx context.Context) (uint64, error) {
	if e.db == nil {
		return 0, fmt.Errorf("database connection not available")
	}

	// Query the routing ID directly from the database
	var routingID int64
	query := `
		SELECT n.routing_id 
		FROM nodes n
		JOIN localidentity li ON n.node_id = li.identity_id
		LIMIT 1
	`

	err := e.db.Pool().QueryRow(ctx, query).Scan(&routingID)
	if err != nil {
		return 0, fmt.Errorf("failed to query local routing ID: %w", err)
	}

	return uint64(routingID), nil
}
