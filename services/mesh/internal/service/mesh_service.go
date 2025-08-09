package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/consensus"
	"github.com/redbco/redb-open/services/mesh/internal/topology"
	"github.com/redbco/redb-open/services/mesh/internal/transport"
)

// MeshService implements the protobuf MeshService interface
type MeshService struct {
	meshv1.UnimplementedMeshServiceServer

	config    *Config
	logger    *logger.Logger
	consensus consensus.ConsensusEngine
	transport transport.Transport
	topology  *topology.Manager
	meshState *MeshState
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
}

// Config holds configuration for the mesh service
type Config struct {
	NodeID            string
	MeshID            string
	ListenAddr        string
	TransportType     transport.TransportType
	TransportConfig   transport.TransportConfig
	ConsensusConfig   consensus.Config
	TopologyConfig    topology.Config
	HeartbeatInterval time.Duration
	JoinTimeout       time.Duration
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		NodeID:            "node-1",
		MeshID:            "mesh-1",
		ListenAddr:        ":8080",
		TransportType:     transport.TransportTypeWebSocket,
		TransportConfig:   transport.DefaultConfig(),
		ConsensusConfig:   consensus.DefaultConfig(),
		TopologyConfig:    topology.DefaultConfig(),
		HeartbeatInterval: 30 * time.Second,
		JoinTimeout:       60 * time.Second,
	}
}

// MeshState represents the current state of the mesh
type MeshState struct {
	MeshID    string
	Status    MeshStatus
	Nodes     map[string]*NodeInfo
	Topology  *topology.Topology
	CreatedAt time.Time
	UpdatedAt time.Time
	mu        sync.RWMutex
}

// MeshStatus represents the status of the mesh
type MeshStatus int

const (
	MeshStatusInitializing MeshStatus = iota
	MeshStatusSeeding
	MeshStatusJoining
	MeshStatusRunning
	MeshStatusStopping
	MeshStatusStopped
	MeshStatusFailed
)

// NodeInfo represents information about a node in the mesh
type NodeInfo struct {
	NodeID       string
	Status       NodeStatus
	Address      string
	Capabilities map[string]string
	JoinedAt     time.Time
	LastSeen     time.Time
}

// NodeStatus represents the status of a node
type NodeStatus int

const (
	NodeStatusJoining NodeStatus = iota
	NodeStatusActive
	NodeStatusInactive
	NodeStatusEvicted
)

// NewMeshService creates a new mesh service
func NewMeshService(config *Config, logger *logger.Logger) (*MeshService, error) {
	if config == nil {
		config = DefaultConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	ms := &MeshService{
		config: config,
		logger: logger,
		meshState: &MeshState{
			MeshID:    config.MeshID,
			Status:    MeshStatusInitializing,
			Nodes:     make(map[string]*NodeInfo),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		ctx:    ctx,
		cancel: cancel,
	}

	// Initialize topology manager
	ms.topology = topology.NewManager(config.TopologyConfig, logger)

	// Initialize consensus engine
	var err error
	ms.consensus, err = consensus.NewEngine(config.ConsensusConfig, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create consensus engine: %w", err)
	}

	return ms, nil
}

// Start starts the mesh service
func (ms *MeshService) Start() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.meshState.Status != MeshStatusInitializing {
		return fmt.Errorf("mesh service already started")
	}

	// Start transport
	if err := ms.transport.Start(); err != nil {
		return fmt.Errorf("failed to start transport: %w", err)
	}

	// Start consensus engine
	if err := ms.consensus.Start(); err != nil {
		ms.transport.Stop()
		return fmt.Errorf("failed to start consensus engine: %w", err)
	}

	// Start topology manager
	if err := ms.topology.Start(); err != nil {
		ms.consensus.Stop()
		ms.transport.Stop()
		return fmt.Errorf("failed to start topology manager: %w", err)
	}

	ms.meshState.Status = MeshStatusSeeding
	ms.meshState.UpdatedAt = time.Now()

	ms.logger.Info("Mesh service started", "mesh_id", ms.config.MeshID, "node_id", ms.config.NodeID)

	return nil
}

// Stop stops the mesh service
func (ms *MeshService) Stop() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.meshState.Status == MeshStatusStopped {
		return nil
	}

	ms.meshState.Status = MeshStatusStopping
	ms.meshState.UpdatedAt = time.Now()

	// Stop components in reverse order
	if ms.topology != nil {
		ms.topology.Stop()
	}

	if ms.consensus != nil {
		ms.consensus.Stop()
	}

	if ms.transport != nil {
		ms.transport.Stop()
	}

	ms.meshState.Status = MeshStatusStopped
	ms.meshState.UpdatedAt = time.Now()

	ms.cancel()

	ms.logger.Info("Mesh service stopped", "mesh_id", ms.config.MeshID, "node_id", ms.config.NodeID)

	return nil
}

// SeedMesh implements the SeedMesh RPC method
func (ms *MeshService) SeedMesh(ctx context.Context, req *meshv1.SeedMeshReq) (*meshv1.MeshStatus, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.meshState.Status != MeshStatusSeeding {
		return nil, fmt.Errorf("mesh not in seeding state")
	}

	// Initialize the mesh as the seed node
	ms.meshState.Status = MeshStatusRunning
	ms.meshState.UpdatedAt = time.Now()

	// Add self as the first node
	ms.meshState.Nodes[ms.config.NodeID] = &NodeInfo{
		NodeID:       ms.config.NodeID,
		Status:       NodeStatusActive,
		Address:      ms.config.ListenAddr,
		Capabilities: make(map[string]string),
		JoinedAt:     time.Now(),
		LastSeen:     time.Now(),
	}

	// Initialize topology
	if err := ms.topology.Initialize(ms.config.MeshID, ms.config.NodeID); err != nil {
		return nil, fmt.Errorf("failed to initialize topology: %w", err)
	}

	ms.logger.Info("Mesh seeded successfully", "mesh_id", ms.config.MeshID, "node_id", ms.config.NodeID)

	return ms.buildMeshStatus(), nil
}

// JoinMesh implements the JoinMesh RPC method
func (ms *MeshService) JoinMesh(ctx context.Context, req *meshv1.JoinMeshReq) (*meshv1.MeshStatus, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.meshState.Status != MeshStatusSeeding {
		return nil, fmt.Errorf("mesh not in seeding state")
	}

	// Connect to the seed node
	link, err := ms.transport.Connect(req.SeedNodeAddr, ms.config.NodeID, fmt.Sprintf("link-%s-%s", ms.config.NodeID, req.SeedNodeAddr))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to seed node: %w", err)
	}

	// Update mesh state
	ms.meshState.Status = MeshStatusJoining
	ms.meshState.UpdatedAt = time.Now()

	// Add self to nodes
	ms.meshState.Nodes[ms.config.NodeID] = &NodeInfo{
		NodeID:       ms.config.NodeID,
		Status:       NodeStatusJoining,
		Address:      ms.config.ListenAddr,
		Capabilities: make(map[string]string),
		JoinedAt:     time.Now(),
		LastSeen:     time.Now(),
	}

	// Join consensus cluster
	if err := ms.consensus.Join(req.SeedNodeAddr); err != nil {
		link.Close()
		return nil, fmt.Errorf("failed to join consensus cluster: %w", err)
	}

	// Update topology
	if err := ms.topology.Join(ms.config.MeshID, ms.config.NodeID, req.SeedNodeAddr); err != nil {
		link.Close()
		return nil, fmt.Errorf("failed to join topology: %w", err)
	}

	ms.meshState.Status = MeshStatusRunning
	ms.meshState.UpdatedAt = time.Now()

	ms.logger.Info("Joined mesh successfully", "mesh_id", ms.config.MeshID, "node_id", ms.config.NodeID, "seed_node", req.SeedNodeAddr)

	return ms.buildMeshStatus(), nil
}

// StartMesh implements the StartMesh RPC method
func (ms *MeshService) StartMesh(ctx context.Context, req *meshv1.StartMeshReq) (*meshv1.MeshStatus, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.meshState.Status != MeshStatusRunning {
		return nil, fmt.Errorf("mesh not in running state")
	}

	// Start mesh operations
	ms.meshState.Status = MeshStatusRunning
	ms.meshState.UpdatedAt = time.Now()

	ms.logger.Info("Mesh started", "mesh_id", ms.config.MeshID, "node_id", ms.config.NodeID)

	return ms.buildMeshStatus(), nil
}

// StopMesh implements the StopMesh RPC method
func (ms *MeshService) StopMesh(ctx context.Context, req *meshv1.StopMeshReq) (*meshv1.MeshStatus, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.meshState.Status != MeshStatusRunning {
		return nil, fmt.Errorf("mesh not in running state")
	}

	// Stop mesh operations
	ms.meshState.Status = MeshStatusStopping
	ms.meshState.UpdatedAt = time.Now()

	ms.logger.Info("Mesh stopped", "mesh_id", ms.config.MeshID, "node_id", ms.config.NodeID)

	return ms.buildMeshStatus(), nil
}

// LeaveMesh implements the LeaveMesh RPC method
func (ms *MeshService) LeaveMesh(ctx context.Context, req *meshv1.LeaveMeshReq) (*meshv1.SuccessStatus, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.meshState.Status == MeshStatusStopped {
		return &meshv1.SuccessStatus{Success: true}, nil
	}

	// Leave consensus cluster
	if ms.consensus != nil {
		if err := ms.consensus.Leave(); err != nil {
			ms.logger.Error("Failed to leave consensus cluster", "error", err)
		}
	}

	// Update topology
	if ms.topology != nil {
		if err := ms.topology.Leave(ms.config.MeshID, ms.config.NodeID); err != nil {
			ms.logger.Error("Failed to leave topology", "error", err)
		}
	}

	// Close transport connections
	if ms.transport != nil {
		links := ms.transport.ListLinks()
		for _, link := range links {
			link.Close()
		}
	}

	ms.meshState.Status = MeshStatusStopped
	ms.meshState.UpdatedAt = time.Now()

	ms.logger.Info("Left mesh", "mesh_id", ms.config.MeshID, "node_id", ms.config.NodeID)

	return &meshv1.SuccessStatus{Success: true}, nil
}

// EvictNode implements the EvictNode RPC method
func (ms *MeshService) EvictNode(ctx context.Context, req *meshv1.EvictNodeReq) (*meshv1.MeshStatus, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.meshState.Status != MeshStatusRunning {
		return nil, fmt.Errorf("mesh not in running state")
	}

	nodeID := req.NodeId
	node, exists := ms.meshState.Nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	// Evict the node
	node.Status = NodeStatusEvicted
	node.LastSeen = time.Now()

	// Update topology
	if err := ms.topology.EvictNode(ms.config.MeshID, nodeID); err != nil {
		return nil, fmt.Errorf("failed to evict node from topology: %w", err)
	}

	// Close transport connection if exists
	if ms.transport != nil {
		linkID := fmt.Sprintf("link-%s-%s", ms.config.NodeID, nodeID)
		if link, exists := ms.transport.GetLink(linkID); exists {
			link.Close()
		}
	}

	ms.meshState.UpdatedAt = time.Now()

	ms.logger.Info("Evicted node", "mesh_id", ms.config.MeshID, "node_id", nodeID)

	return ms.buildMeshStatus(), nil
}

// AddLink implements the AddLink RPC method
func (ms *MeshService) AddLink(ctx context.Context, req *meshv1.AddLinkReq) (*meshv1.TopologyStatus, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if ms.meshState.Status != MeshStatusRunning {
		return nil, fmt.Errorf("mesh not in running state")
	}

	// Add link to topology
	if err := ms.topology.AddLink(req.SourceNodeId, req.TargetNodeId, req.LinkType); err != nil {
		return nil, fmt.Errorf("failed to add link: %w", err)
	}

	ms.logger.Info("Added link", "source", req.SourceNodeId, "target", req.TargetNodeId, "type", req.LinkType)

	return &meshv1.TopologyStatus{
		Success: true,
		Message: "Link added successfully",
	}, nil
}

// DropLink implements the DropLink RPC method
func (ms *MeshService) DropLink(ctx context.Context, req *meshv1.DropLinkReq) (*meshv1.TopologyStatus, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if ms.meshState.Status != MeshStatusRunning {
		return nil, fmt.Errorf("mesh not in running state")
	}

	// Drop link from topology
	if err := ms.topology.DropLink(req.SourceNodeId, req.TargetNodeId); err != nil {
		return nil, fmt.Errorf("failed to drop link: %w", err)
	}

	ms.logger.Info("Dropped link", "source", req.SourceNodeId, "target", req.TargetNodeId)

	return &meshv1.TopologyStatus{
		Success: true,
		Message: "Link dropped successfully",
	}, nil
}

// EstablishFullLinks implements the EstablishFullLinks RPC method
func (ms *MeshService) EstablishFullLinks(ctx context.Context, req *meshv1.EstablishFullLinksReq) (*meshv1.TopologyStatus, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if ms.meshState.Status != MeshStatusRunning {
		return nil, fmt.Errorf("mesh not in running state")
	}

	// Establish full mesh topology
	if err := ms.topology.EstablishFullMesh(); err != nil {
		return nil, fmt.Errorf("failed to establish full mesh: %w", err)
	}

	ms.logger.Info("Established full mesh topology")

	return &meshv1.TopologyStatus{
		Success: true,
		Message: "Full mesh topology established successfully",
	}, nil
}

// GetTopology implements the GetTopology RPC method
func (ms *MeshService) GetTopology(ctx context.Context, req *meshv1.GetTopologyReq) (*meshv1.TopologyStatus, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if ms.meshState.Status != MeshStatusRunning {
		return nil, fmt.Errorf("mesh not in running state")
	}

	// Get topology from topology manager
	topology := ms.topology.GetTopology()
	if topology == nil {
		return nil, fmt.Errorf("topology not available")
	}

	// Convert topology to protobuf format
	topologyProto := &meshv1.Topology{
		MeshId: topology.MeshID,
		Nodes:  make([]*meshv1.Node, 0, len(topology.Nodes)),
		Links:  make([]*meshv1.Link, 0, len(topology.Links)),
	}

	for _, node := range topology.Nodes {
		topologyProto.Nodes = append(topologyProto.Nodes, &meshv1.Node{
			NodeId: node.NodeID,
			Status: meshv1.NodeStatus(node.Status),
		})
	}

	for _, link := range topology.Links {
		topologyProto.Links = append(topologyProto.Links, &meshv1.Link{
			SourceNodeId: link.SourceNodeID,
			TargetNodeId: link.TargetNodeID,
			LinkType:     link.LinkType,
		})
	}

	return &meshv1.TopologyStatus{
		Success:  true,
		Message:  "Topology retrieved successfully",
		Topology: topologyProto,
	}, nil
}

// buildMeshStatus builds a MeshStatus protobuf message from the current state
func (ms *MeshService) buildMeshStatus() *meshv1.MeshStatus {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	nodes := make([]*meshv1.Node, 0, len(ms.meshState.Nodes))
	for _, node := range ms.meshState.Nodes {
		nodes = append(nodes, &meshv1.Node{
			NodeId: node.NodeID,
			Status: meshv1.NodeStatus(node.Status),
		})
	}

	return &meshv1.MeshStatus{
		MeshId:    ms.meshState.MeshID,
		Status:    meshv1.MeshStatus(ms.meshState.Status),
		Nodes:     nodes,
		CreatedAt: ms.meshState.CreatedAt.Unix(),
		UpdatedAt: ms.meshState.UpdatedAt.Unix(),
	}
}

// SetTransport sets the transport layer for the mesh service
func (ms *MeshService) SetTransport(transport transport.Transport) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.transport = transport
}

// GetMeshState returns the current mesh state
func (ms *MeshService) GetMeshState() *MeshState {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.meshState
}
