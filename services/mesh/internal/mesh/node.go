package mesh

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/mesh/router"
	"github.com/redbco/redb-open/services/mesh/internal/messages"
	"github.com/redbco/redb-open/services/mesh/internal/network"
	"github.com/redbco/redb-open/services/mesh/internal/storage"
)

// Config holds the configuration for a mesh node
type Config struct {
	NodeID        string        `yaml:"node_id"`
	MeshID        string        `yaml:"mesh_id"`
	ListenAddress string        `yaml:"listen_address"`
	Heartbeat     time.Duration `yaml:"heartbeat"`
	Timeout       time.Duration `yaml:"timeout"`
}

// Node represents a mesh network node
type Node struct {
	config      Config
	store       storage.Interface
	logger      *logger.Logger
	network     *WebSocketNetworkAdapter
	connections map[string]*Connection
	router      *router.Router
	consensus   *Consensus
	framer      *messages.MessageFramer
	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
}

type Connection struct {
	ID       string
	Status   string
	LastSeen time.Time
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

	// Create WebSocket network layer
	websocketCfg := network.Config{
		Address:        cfg.ListenAddress,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		MaxMessageSize: 65536,
		PingInterval:   30 * time.Second,
		PongTimeout:    60 * time.Second,
	}

	// Create the node first without network
	node := &Node{
		config:      cfg,
		store:       store,
		logger:      logger,
		connections: make(map[string]*Connection),
		ctx:         ctx,
		cancel:      cancel,
	}

	// Create WebSocket network adapter with the node as MessageHandler
	network, err := NewWebSocketNetworkAdapter(websocketCfg, node, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create WebSocket network layer: %v", err)
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

	// Set the network in the node
	node.network = network
	node.router = router
	node.consensus = consensus
	node.framer = framer

	return node, nil
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
	go n.presenceLoop()

	return nil
}

// CancelContext cancels the node's context to signal shutdown
func (n *Node) CancelContext() {
	n.cancel()
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

	// Stop network with timeout
	if n.logger != nil {
		n.logger.Info("Stopping network...")
	}

	// Use timeout for network shutdown to prevent hanging
	networkCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Create a channel to signal completion
	done := make(chan error, 1)
	go func() {
		done <- n.network.Stop()
	}()

	select {
	case err := <-done:
		if err != nil {
			if n.logger != nil {
				n.logger.Errorf("Failed to stop network: %v", err)
			}
			return err
		}
	case <-networkCtx.Done():
		if n.logger != nil {
			n.logger.Warn("Network shutdown timed out, forcing stop")
		}
		// Force stop by calling Stop again (it should be idempotent)
		n.network.Stop()
	}

	if n.logger != nil {
		n.logger.Info("Mesh node stopped successfully")
	}

	return nil
}

// messageLoop processes incoming messages
// Note: With WebSocket, messages are handled directly via HandleMessage callback
// This loop is kept for backward compatibility but is mostly idle
func (n *Node) messageLoop() {
	for {
		select {
		case <-n.ctx.Done():
			if n.logger != nil {
				n.logger.Info("Message loop shutting down due to context cancellation")
			}
			return
		default:
			// With WebSocket, messages are handled via HandleMessage callback
			// This loop just waits for shutdown signal
			time.Sleep(100 * time.Millisecond)
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
			heartbeatMsg, err := n.framer.CreateHeartbeatMessage("")
			if err != nil {
				n.logger.Error("Failed to create heartbeat message: %v", err)
				continue
			}

			// Broadcast heartbeat using WebSocket
			if err := n.network.Broadcast("heartbeat", heartbeatMsg); err != nil {
				n.logger.Error("Failed to broadcast heartbeat: %v", err)
			}
		}
	}
}

// presenceLoop broadcasts node presence periodically
func (n *Node) presenceLoop() {
	ticker := time.NewTicker(60 * time.Second) // Broadcast presence every minute
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			if err := n.BroadcastPresence(); err != nil {
				n.logger.Error("Failed to broadcast presence: %v", err)
			}
		}
	}
}

// HandleMessage implements the MessageHandler interface for WebSocket messages
func (n *Node) HandleMessage(msg *messages.Message) error {
	// Validate the message
	if err := messages.ValidateMessage(msg); err != nil {
		n.logger.Error("Invalid message received: %v", err)
		return err
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
	case "join_request":
		n.handleJoinRequest(msg)
	case "join_response":
		n.handleJoinResponse(msg)
	case "node_presence":
		n.handleNodePresence(msg)
	default:
		n.logger.Warn("Unknown message type: %s", msg.Header.Type)
	}

	return nil
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

// JoinMesh connects to an existing mesh node and joins the mesh
func (n *Node) JoinMesh(targetNodeAddress string) error {
	if n.logger != nil {
		n.logger.Infof("Joining mesh via node: %s", targetNodeAddress)
	}

	// For now, we'll use a simple approach where the target node ID
	// is derived from the mesh ID. In a more sophisticated implementation,
	// we'd implement node discovery to find the actual node ID.
	//
	// This works because in our current setup, the first node in a mesh
	// typically has a node ID that matches or is related to the mesh ID.
	targetNodeID := n.config.MeshID

	// Connect to the target node
	if err := n.network.ConnectToPeer(targetNodeID, targetNodeAddress); err != nil {
		return fmt.Errorf("failed to connect to target node: %v", err)
	}

	// Wait a moment for connection to stabilize
	time.Sleep(100 * time.Millisecond)

	// Verify connection is established
	connections := n.network.GetConnections()
	if _, exists := connections[targetNodeID]; !exists {
		return fmt.Errorf("failed to establish connection to target node")
	}

	// Send join request
	joinRequest := map[string]interface{}{
		"node_id": n.config.NodeID,
		"mesh_id": n.config.MeshID,
		"address": n.config.ListenAddress,
	}

	if err := n.network.SendMessage(targetNodeID, "join_request", joinRequest); err != nil {
		return fmt.Errorf("failed to send join request: %v", err)
	}

	if n.logger != nil {
		n.logger.Infof("Join request sent to node %s", targetNodeID)
	}

	return nil
}

// AcceptJoinRequest handles an incoming join request from another node
func (n *Node) AcceptJoinRequest(peerID string, joinData map[string]interface{}) error {
	if n.logger != nil {
		n.logger.Infof("Accepting join request from node: %s", peerID)
	}

	// Validate join request
	nodeID, ok := joinData["node_id"].(string)
	if !ok {
		return fmt.Errorf("invalid node_id in join request")
	}

	meshID, ok := joinData["mesh_id"].(string)
	if !ok {
		return fmt.Errorf("invalid mesh_id in join request")
	}

	// Verify mesh ID matches
	if meshID != n.config.MeshID {
		return fmt.Errorf("mesh ID mismatch: expected %s, got %s", n.config.MeshID, meshID)
	}

	// Add the new node to our connections
	if err := n.AddConnection(nodeID); err != nil {
		return fmt.Errorf("failed to add connection: %v", err)
	}

	// Send join response
	joinResponse := map[string]interface{}{
		"status":  "accepted",
		"mesh_id": n.config.MeshID,
		"node_id": n.config.NodeID,
	}

	if err := n.network.SendMessage(nodeID, "join_response", joinResponse); err != nil {
		return fmt.Errorf("failed to send join response: %v", err)
	}

	if n.logger != nil {
		n.logger.Infof("Join request accepted for node: %s", nodeID)
	}

	return nil
}

// BroadcastPresence broadcasts this node's presence to the mesh
func (n *Node) BroadcastPresence() error {
	presenceMsg := map[string]interface{}{
		"node_id":   n.config.NodeID,
		"mesh_id":   n.config.MeshID,
		"address":   n.config.ListenAddress,
		"timestamp": time.Now().Unix(),
	}

	if err := n.network.Broadcast("node_presence", presenceMsg); err != nil {
		return fmt.Errorf("failed to broadcast presence: %v", err)
	}

	if n.logger != nil {
		n.logger.Debug("Presence broadcasted to mesh")
	}

	return nil
}

// GetMeshStatus returns the current mesh status
func (n *Node) GetMeshStatus() map[string]interface{} {
	n.mu.RLock()
	defer n.mu.RUnlock()

	connections := make([]string, 0, len(n.connections))
	for peerID := range n.connections {
		connections = append(connections, peerID)
	}

	return map[string]interface{}{
		"node_id":          n.config.NodeID,
		"mesh_id":          n.config.MeshID,
		"connection_count": len(connections),
		"connections":      connections,
		"status":           "active",
	}
}

// GetMeshTopology returns the current mesh topology
func (n *Node) GetMeshTopology() map[string]interface{} {
	n.mu.RLock()
	defer n.mu.RUnlock()

	connections := make([]string, 0, len(n.connections))
	for peerID := range n.connections {
		connections = append(connections, peerID)
	}

	// Determine mesh type based on connections
	meshType := "single_node"
	if len(connections) > 0 {
		meshType = "multi_node"
	}

	return map[string]interface{}{
		"node_id":          n.config.NodeID,
		"mesh_id":          n.config.MeshID,
		"mesh_type":        meshType,
		"connection_count": len(connections),
		"connections":      connections,
		"status":           "active",
	}
}

// GetConnectedNodes returns a list of connected node IDs
func (n *Node) GetConnectedNodes() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	connectedNodes := make([]string, 0, len(n.connections))
	for peerID := range n.connections {
		connectedNodes = append(connectedNodes, peerID)
	}
	return connectedNodes
}

// IsConnectedTo returns true if this node is connected to the specified node
func (n *Node) IsConnectedTo(nodeID string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	_, exists := n.connections[nodeID]
	return exists
}

// handleJoinRequest processes a join request from another node
func (n *Node) handleJoinRequest(msg *messages.Message) {
	var joinData map[string]interface{}
	if err := msg.UnmarshalPayload(&joinData); err != nil {
		n.logger.Error("Failed to unmarshal join request: %v", err)
		return
	}

	if err := n.AcceptJoinRequest(msg.Header.From, joinData); err != nil {
		n.logger.Error("Failed to accept join request: %v", err)
	}
}

// handleJoinResponse processes a join response from a seed node
func (n *Node) handleJoinResponse(msg *messages.Message) {
	var responseData map[string]interface{}
	if err := msg.UnmarshalPayload(&responseData); err != nil {
		n.logger.Error("Failed to unmarshal join response: %v", err)
		return
	}

	status, ok := responseData["status"].(string)
	if !ok {
		n.logger.Error("Invalid status in join response")
		return
	}

	if status == "accepted" {
		n.logger.Infof("Successfully joined mesh via node: %s", msg.Header.From)
		// Add the target node to our connections
		if err := n.AddConnection(msg.Header.From); err != nil {
			n.logger.Error("Failed to add target node connection: %v", err)
		}
	} else {
		n.logger.Errorf("Join request rejected by node: %s", msg.Header.From)
	}
}

// handleNodePresence processes a node presence broadcast
func (n *Node) handleNodePresence(msg *messages.Message) {
	var presenceData map[string]interface{}
	if err := msg.UnmarshalPayload(&presenceData); err != nil {
		n.logger.Error("Failed to unmarshal node presence: %v", err)
		return
	}

	nodeID, ok := presenceData["node_id"].(string)
	if !ok {
		n.logger.Error("Invalid node_id in presence message")
		return
	}

	meshID, ok := presenceData["mesh_id"].(string)
	if !ok {
		n.logger.Error("Invalid mesh_id in presence message")
		return
	}

	// Only process presence from nodes in the same mesh
	if meshID != n.config.MeshID {
		return
	}

	// Update connection if we don't have this node yet
	if _, exists := n.connections[nodeID]; !exists && nodeID != n.config.NodeID {
		if err := n.AddConnection(nodeID); err != nil {
			n.logger.Error("Failed to add connection from presence: %v", err)
		} else {
			n.logger.Debugf("Added connection from presence broadcast: %s", nodeID)
		}
	}
}
