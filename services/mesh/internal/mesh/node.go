package mesh

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/mesh/message"
	"github.com/redbco/redb-open/services/mesh/internal/mesh/router"
	"github.com/redbco/redb-open/services/mesh/internal/storage"
)

// MessageType represents the type of a network message
type MessageType string

const (
	HeartbeatMsg   MessageType = "heartbeat"
	ConsensusMsg   MessageType = "consensus"
	RouteUpdateMsg MessageType = "route_update"
	DataMsg        MessageType = "data"
)

// Config holds the configuration for a mesh node
type Config struct {
	NodeID        string        `yaml:"node_id"`
	MeshID        string        `yaml:"mesh_id"`
	ListenAddress string        `yaml:"listen_address"`
	Heartbeat     time.Duration `yaml:"heartbeat"`
	Timeout       time.Duration `yaml:"timeout"`
}

// NodeMessage represents a network message
type NodeMessage struct {
	Type    MessageType `json:"type"`
	Payload interface{} `json:"payload"`
}

// Node represents a mesh network node
type Node struct {
	config      Config
	store       storage.Interface
	logger      *logger.Logger
	network     *Network
	connections map[string]*Connection
	router      *router.Router
	consensus   *Consensus
	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
}

// ConsensusGroup represents a consensus group
type ConsensusGroup struct {
	ID       string
	Leader   string
	Members  []string
	Term     uint64
	LogIndex uint64
}

// NewNode creates a new mesh node
func NewNode(cfg Config, store storage.Interface, logger *logger.Logger) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("node ID is required")
	}
	if cfg.MeshID == "" {
		return nil, fmt.Errorf("mesh ID is required")
	}

	// Set default values for missing configuration
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = ":8443" // Default listen address
	}
	if cfg.Heartbeat == 0 {
		cfg.Heartbeat = 30 * time.Second
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 60 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create network layer
	networkCfg := NetworkConfig{
		ListenAddress: cfg.ListenAddress,
		BufferSize:    4096,
		ReadTimeout:   5 * time.Second,
		WriteTimeout:  5 * time.Second,
	}
	network, err := NewNetwork(networkCfg, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create network layer: %v", err)
	}

	// Create router
	router := router.NewRouter(network, cfg.NodeID, logger)

	// Create consensus (only if store is available)
	var consensus *Consensus
	if store != nil {
		consensusCfg := ConsensusConfig{
			ElectionTimeout:  cfg.Heartbeat * 2,
			HeartbeatTimeout: cfg.Heartbeat,
			MinVotes:         1, // Default for single node
		}
		consensus = NewConsensus(cfg.NodeID, consensusCfg, logger)
	} else {
		logger.Warnf("No storage available, consensus disabled for node %s", cfg.NodeID)
	}

	return &Node{
		config:      cfg,
		store:       store,
		logger:      logger,
		network:     network,
		connections: make(map[string]*Connection),
		router:      router,
		consensus:   consensus,
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

// Start starts the mesh node
func (n *Node) Start() error {
	if err := n.network.Start(); err != nil {
		return fmt.Errorf("failed to start network layer: %v", err)
	}

	n.router.Start()

	// Only start consensus if it's available
	if n.consensus != nil {
		n.consensus.Start()
	}

	go n.messageLoop()
	go n.heartbeatLoop()

	return nil
}

// Stop stops the mesh node
func (n *Node) Stop() error {
	n.cancel()
	n.router.Stop()

	// Only stop consensus if it's available
	if n.consensus != nil {
		n.consensus.Stop()
	}

	return n.network.Close()
}

// messageLoop processes incoming messages
func (n *Node) messageLoop() {
	for {
		select {
		case <-n.ctx.Done():
			return
		case msg := <-n.network.MessageChannel():
			n.handleMessage(msg)
		}
	}
}

// heartbeatLoop sends periodic heartbeats
func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(n.config.Heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			msg := message.Message{
				Type:    message.HeartbeatMsg,
				Payload: nil,
			}
			if err := n.network.Broadcast(string(msg.Type), msg.Payload); err != nil {
				n.logger.Error("Failed to broadcast heartbeat: %v", err)
			}
		}
	}
}

// handleMessage processes a received message
func (n *Node) handleMessage(msg message.Message) {
	switch msg.Type {
	case message.HeartbeatMsg:
		n.handleHeartbeat(msg)
	case message.ConsensusMsg:
		// TODO: Implement consensus message handling once the consensus protocol is fully implemented
		n.logger.Debug("Received consensus message - handling not yet implemented")
	case message.RouteUpdateMsg:
		var update router.RouteUpdate
		if err := json.Unmarshal(msg.Payload.([]byte), &update); err != nil {
			n.logger.Error("Failed to parse route update: %v", err)
			return
		}

		if err := n.router.HandleRouteUpdate(update); err != nil {
			n.logger.Error("Failed to handle route update: %v", err)
		}
	case message.DataMsg:
		n.handleData(msg)
	default:
		n.logger.Warn("Unknown message type: %s", string(msg.Type))
	}
}

// handleHeartbeat processes a heartbeat message
func (n *Node) handleHeartbeat(msg message.Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Update connection status
	if conn, exists := n.connections[msg.From]; exists {
		conn.LastSeen = time.Now()
		conn.Status = "connected"
	}
}

// handleData processes a data message
func (n *Node) handleData(msg message.Message) {
	// Forward message if not intended for this node
	if msg.To != n.config.NodeID {
		nextHop, err := n.router.GetNextHop(msg.To)
		if err != nil {
			n.logger.Error("Failed to get next hop: %v", err)
			return
		}

		if err := n.network.SendMessage(nextHop, string(message.DataMsg), msg.Payload); err != nil {
			n.logger.Error("Failed to forward message: %v", err)
		}
		return
	}

	// Process message locally
	// TODO: Implement message processing
}

// GetID returns the node's ID
func (n *Node) GetID() string {
	return n.config.NodeID
}

// GetMeshID returns the mesh network ID
func (n *Node) GetMeshID() string {
	return n.config.MeshID
}

// AddConnection adds a new connection to another node
func (n *Node) AddConnection(peerID string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, exists := n.connections[peerID]; exists {
		return nil
	}

	n.connections[peerID] = &Connection{
		ID:       peerID,
		Status:   "connecting",
		LastSeen: time.Now(),
	}

	n.logger.Info("Added new connection: (peer_id: %s)", peerID)
	return nil
}

// RemoveConnection removes a connection to another node
func (n *Node) RemoveConnection(peerID string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, exists := n.connections[peerID]; !exists {
		return nil
	}

	if err := n.closeConnection(peerID); err != nil {
		return fmt.Errorf("failed to close connection: %v", err)
	}

	delete(n.connections, peerID)
	n.logger.Info("Removed connection: (peer_id: %s)", peerID)
	return nil
}

// GetConnections returns all current connections
func (n *Node) GetConnections() map[string]*Connection {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Create a copy of the connections map
	conns := make(map[string]*Connection, len(n.connections))
	for k, v := range n.connections {
		conns[k] = v
	}
	return conns
}

// closeConnection closes a connection to another node
func (n *Node) closeConnection(peerID string) error {
	if err := n.network.RemoveConnection(peerID); err != nil {
		return fmt.Errorf("failed to remove connection: %v", err)
	}
	return nil
}
