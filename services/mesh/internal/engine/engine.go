package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/consensus"
	"github.com/redbco/redb-open/services/mesh/internal/mesh"
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
		meshv1.RegisterManagementServiceServer(e.grpcServer, serviceServer)
		meshv1.RegisterConsensusServiceServer(e.grpcServer, serviceServer)
	}
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

func (e *Engine) Start(ctx context.Context) error {
	e.state.Lock()
	defer e.state.Unlock()

	if e.state.isRunning {
		return fmt.Errorf("engine is already running")
	}

	if e.grpcServer == nil {
		return fmt.Errorf("gRPC server not set - call SetGRPCServer first")
	}

	// Initialize logger
	e.logger = logger.New("mesh", "1.0.0")

	// Initialize storage
	var err error
	storageConfig := storage.ConfigFromGlobal(e.config)
	e.storage, err = storage.NewStorage(ctx, storageConfig, e.logger)
	if err != nil {
		e.logger.Errorf("Failed to initialize storage, continuing without storage: %v", err)
		e.storage = nil
	} else {
		e.logger.Infof("Storage initialized successfully (type: %s)", storageConfig.Type)
	}

	// Service is always marked as running so gRPC can receive requests
	e.state.isRunning = true

	if e.storage == nil {
		e.logger.Warnf("Storage not available, mesh node will not be initialized")
		e.state.initState = StateError
		e.logger.Infof("Mesh engine started in error state - gRPC server available but no mesh functionality")
		return nil
	}

	// Check initialization state from database
	initInfo, err := e.checkInitializationState(ctx)
	if err != nil {
		e.logger.Errorf("Failed to check initialization state: %v", err)
		e.state.initState = StateError
		e.logger.Infof("Mesh engine started in error state - gRPC server available but no mesh functionality")
		return nil
	}

	e.state.initState = initInfo.State

	switch initInfo.State {
	case StateUninitialized:
		e.logger.Infof("Node has not been initialized (no localidentity, nodes, or mesh defined)")
		e.logger.Infof("Mesh engine started - gRPC server available for initialization requests")
		return nil

	case StateNodeOnly:
		e.logger.Infof("Node initialized but not added to a mesh (node_id: %s, node_name: %s)",
			initInfo.NodeInfo.NodeID, initInfo.NodeInfo.NodeName)
		e.logger.Infof("Mesh engine started - gRPC server available for mesh operations")
		return nil

	case StateFullyConfigured:
		e.logger.Infof("Node and mesh fully configured (node_id: %s, mesh_id: %s)",
			initInfo.NodeInfo.NodeID, initInfo.MeshInfo.MeshID)

		// Create mesh node configuration from database
		meshConfig := mesh.Config{
			NodeID:        initInfo.NodeInfo.NodeID,
			MeshID:        initInfo.MeshInfo.MeshID,
			ListenAddress: fmt.Sprintf("%s:%d", initInfo.NodeInfo.IPAddress, initInfo.NodeInfo.Port),
			Heartbeat:     30 * time.Second, // Default values
			Timeout:       60 * time.Second,
		}

		// Create and start mesh node
		e.meshNode, err = mesh.NewNode(meshConfig, e.storage, e.logger)
		if err != nil {
			e.logger.Errorf("Failed to create mesh node: %v", err)
			e.state.initState = StateError
			e.logger.Infof("Mesh engine started in error state - gRPC server available but mesh node failed to create")
			return nil
		}

		if err := e.meshNode.Start(); err != nil {
			e.logger.Errorf("Failed to start mesh node: %v", err)
			e.state.initState = StateError
			e.logger.Infof("Mesh engine started in error state - gRPC server available but mesh node failed to start")
			return nil
		}

		// Establish connections based on routes
		if len(initInfo.Routes) > 0 {
			e.logger.Infof("Attempting to establish %d route connections", len(initInfo.Routes))
			go e.establishRouteConnections(initInfo.Routes)
		} else {
			e.logger.Infof("No outbound routes defined - waiting for incoming connections")
		}

		e.logger.Infof("Mesh engine started successfully with full mesh configuration")
		return nil

	case StateError:
		e.logger.Errorf("Initialization error: %s", initInfo.ErrorMessage)
		e.logger.Infof("Mesh engine started in error state - gRPC server available but mesh functionality disabled")
		return nil
	}

	return nil
}

// establishRouteConnections attempts to establish connections to target nodes based on routes
func (e *Engine) establishRouteConnections(routes []*storage.RouteInfo) {
	for _, route := range routes {
		e.logger.Infof("Attempting to establish connection to node %s", route.TargetNodeID)

		// Get target node information
		targetNode, err := e.storage.GetNodeInfo(context.Background(), route.TargetNodeID)
		if err != nil {
			e.logger.Errorf("Failed to get target node info for %s: %v", route.TargetNodeID, err)
			continue
		}

		if targetNode == nil {
			e.logger.Errorf("Target node %s not found in database", route.TargetNodeID)
			continue
		}

		// Add connection to mesh node
		if err := e.meshNode.AddConnection(targetNode.NodeID); err != nil {
			e.logger.Errorf("Failed to add connection to %s: %v", targetNode.NodeID, err)
		} else {
			e.logger.Infof("Successfully added connection to node %s (%s:%d)",
				targetNode.NodeID, targetNode.IPAddress, targetNode.Port)
		}
	}
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

	// Stop all consensus groups (simplified handling)
	e.groupsMutex.Lock()
	for groupID := range e.consensusGroups {
		if e.logger != nil {
			e.logger.Infof("Stopping consensus group (group_id: %s)", groupID)
		}
		// We'll handle group cleanup in a simplified way
		delete(e.consensusGroups, groupID)
	}
	e.groupsMutex.Unlock()

	// Stop mesh node
	if e.meshNode != nil {
		if err := e.meshNode.Stop(); err != nil && e.logger != nil {
			e.logger.Errorf("Failed to stop mesh node: %v", err)
		}
	}

	// Close storage
	if e.storage != nil {
		if err := e.storage.Close(); err != nil && e.logger != nil {
			e.logger.Errorf("Failed to close storage: %v", err)
		}
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
