package mesh

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/mesh/router"
	"github.com/redbco/redb-open/services/mesh/internal/messages"
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
	framer      *messages.MessageFramer
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

	// Create message framer for the new protocol
	framer := messages.NewMessageFramer(cfg.NodeID)

	return &Node{
		config:      cfg,
		store:       store,
		logger:      logger,
		network:     network,
		connections: make(map[string]*Connection),
		router:      router,
		consensus:   consensus,
		framer:      framer,
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
	if n.logger != nil {
		n.logger.Info("Stopping mesh node...")
	}

	// Cancel context to signal shutdown to all goroutines
	n.cancel()

	// Reduced wait time for faster shutdown
	if n.logger != nil {
		n.logger.Info("Waiting for mesh node goroutines to shutdown...")
	}
	time.Sleep(500 * time.Millisecond)

	// Stop router with reduced timeouts
	if n.logger != nil {
		n.logger.Info("Stopping router...")
	}
	n.router.Stop()

	// Only stop consensus if it's available
	if n.consensus != nil {
		if n.logger != nil {
			n.logger.Info("Stopping consensus...")
		}
		n.consensus.Stop()
	}

	// Close network
	if n.logger != nil {
		n.logger.Info("Closing network...")
	}
	if err := n.network.Close(); err != nil {
		if n.logger != nil {
			n.logger.Errorf("Failed to close network: %v", err)
		}
		return err
	}

	if n.logger != nil {
		n.logger.Info("Mesh node stopped successfully")
	}

	return nil
}

// messageLoop processes incoming messages
func (n *Node) messageLoop() {
	for {
		select {
		case <-n.ctx.Done():
			if n.logger != nil {
				n.logger.Info("Message loop shutting down due to context cancellation")
			}
			return
		case msg, ok := <-n.network.MessageChannel():
			if !ok {
				// Channel closed, exit gracefully
				if n.logger != nil {
					n.logger.Info("Message loop shutting down due to channel closure")
				}
				return
			}
			// Convert old message format to new format temporarily
			// TODO: Update Network layer to use new message protocol
			newMsg := &messages.Message{
				Header: messages.MessageHeader{
					Version:   messages.MessageVersionV1,
					Type:      string(msg.Type),
					From:      msg.From,
					To:        msg.To,
					Timestamp: msg.Timestamp.UnixNano(),
				},
			}
			n.handleMessage(newMsg)
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
			// Create heartbeat message using the new protocol
			_, err := n.framer.CreateHeartbeatMessage("")
			if err != nil {
				n.logger.Error("Failed to create heartbeat message: %v", err)
				continue
			}

			// Convert to old format temporarily until Network layer is updated
			if err := n.network.Broadcast("heartbeat", nil); err != nil {
				n.logger.Error("Failed to broadcast heartbeat: %v", err)
			}
		}
	}
}

// handleMessage processes a received message using the new unified protocol
func (n *Node) handleMessage(msg *messages.Message) {
	// Validate the message
	if err := messages.ValidateMessage(msg); err != nil {
		n.logger.Error("Invalid message received: %v", err)
		return
	}

	switch msg.Header.Type {
	case "heartbeat":
		n.handleHeartbeat(msg)
	case "consensus":
		// TODO: Implement consensus message handling once the consensus protocol is fully implemented
		n.logger.Debug("Received consensus message - handling not yet implemented")
	case "routing":
		n.handleRoutingMessage(msg)
	case "data":
		n.handleData(msg)
	case "management":
		n.handleManagementMessage(msg)
	default:
		n.logger.Warn("Unknown message type: %s", msg.Header.Type)
	}
}

// handleHeartbeat processes a heartbeat message
func (n *Node) handleHeartbeat(msg *messages.Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Update connection status
	if conn, exists := n.connections[msg.Header.From]; exists {
		conn.LastSeen = time.Now()
		conn.Status = "connected"
	}
}

// handleData processes a data message
func (n *Node) handleData(msg *messages.Message) {
	// Forward message if not intended for this node
	if msg.Header.To != "" && msg.Header.To != n.config.NodeID {
		nextHop, err := n.router.GetNextHop(msg.Header.To)
		if err != nil {
			n.logger.Error("Failed to get next hop: %v", err)
			return
		}

		// Use old network interface temporarily
		if err := n.network.SendMessage(nextHop, "data", msg.Payload); err != nil {
			n.logger.Error("Failed to forward message: %v", err)
		}
		return
	}

	// Process message locally
	// TODO: Implement message processing
}

// handleRoutingMessage processes routing messages
func (n *Node) handleRoutingMessage(msg *messages.Message) {
	var routingPayload messages.RoutingPayload
	if err := msg.UnmarshalPayload(&routingPayload); err != nil {
		n.logger.Error("Failed to unmarshal routing message: %v", err)
		return
	}

	n.logger.Debugf("Processing routing message: (sub_type: %s, from: %s)",
		routingPayload.SubType, msg.Header.From)

	switch routingPayload.SubType {
	case "route_update":
		// Handle route updates
		// TODO: Implement route update handling
	case "route_request":
		// Handle route requests
		// TODO: Implement route request handling
	case "route_response":
		// Handle route responses
		// TODO: Implement route response handling
	default:
		n.logger.Warn("Unknown routing sub-type: %s", routingPayload.SubType)
	}
}

// handleManagementMessage processes management messages
func (n *Node) handleManagementMessage(msg *messages.Message) {
	var managementPayload messages.ManagementPayload
	if err := msg.UnmarshalPayload(&managementPayload); err != nil {
		n.logger.Error("Failed to unmarshal management message: %v", err)
		return
	}

	n.logger.Debugf("Processing management message: (sub_type: %s, from: %s)",
		managementPayload.SubType, msg.Header.From)

	switch managementPayload.SubType {
	case "node_discovery":
		// Handle node discovery
		// TODO: Implement node discovery handling
	case "connection_management":
		// Handle connection management
		// TODO: Implement connection management handling
	case "topology_update":
		// Handle topology updates
		// TODO: Implement topology update handling
	case "health_status":
		// Handle health status updates
		// TODO: Implement health status handling
	default:
		n.logger.Warn("Unknown management sub-type: %s", managementPayload.SubType)
	}
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
