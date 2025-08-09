package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redbco/redb-open/pkg/logger"
)

// TransportManager manages WebSocket transport and VirtualLink connections
type TransportManager struct {
	config   TransportConfig
	logger   *logger.Logger
	upgrader websocket.Upgrader
	links    map[string]*VirtualLink
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
	server   *http.Server
}

// TransportConfig holds configuration for the transport manager
type TransportConfig struct {
	ListenAddr        string        `json:"listen_addr"`
	ReadBufferSize    int           `json:"read_buffer_size"`
	WriteBufferSize   int           `json:"write_buffer_size"`
	MaxMessageSize    int64         `json:"max_message_size"`
	HandshakeTimeout  time.Duration `json:"handshake_timeout"`
	WriteTimeout      time.Duration `json:"write_timeout"`
	PongWait          time.Duration `json:"pong_wait"`
	PingPeriod        time.Duration `json:"ping_period"`
	MaxConnections    int           `json:"max_connections"`
	EnableCompression bool          `json:"enable_compression"`
}

// DefaultTransportConfig returns default transport configuration
func DefaultTransportConfig() TransportConfig {
	return TransportConfig{
		ListenAddr:        ":8080",
		ReadBufferSize:    1024,
		WriteBufferSize:   1024,
		MaxMessageSize:    512 * 1024, // 512KB
		HandshakeTimeout:  10 * time.Second,
		WriteTimeout:      10 * time.Second,
		PongWait:          60 * time.Second,
		PingPeriod:        54 * time.Second,
		MaxConnections:    1000,
		EnableCompression: true,
	}
}

// NewTransportManager creates a new transport manager
func NewTransportManager(config TransportConfig, logger *logger.Logger) *TransportManager {
	if config.ReadBufferSize == 0 {
		config.ReadBufferSize = 1024
	}
	if config.WriteBufferSize == 0 {
		config.WriteBufferSize = 1024
	}
	if config.MaxMessageSize == 0 {
		config.MaxMessageSize = 512 * 1024
	}
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = 10 * time.Second
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = 10 * time.Second
	}
	if config.PongWait == 0 {
		config.PongWait = 60 * time.Second
	}
	if config.PingPeriod == 0 {
		config.PingPeriod = 54 * time.Second
	}
	if config.MaxConnections == 0 {
		config.MaxConnections = 1000
	}

	tm := &TransportManager{
		config: config,
		logger: logger,
		links:  make(map[string]*VirtualLink),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  config.ReadBufferSize,
			WriteBufferSize: config.WriteBufferSize,
			CheckOrigin: func(r *http.Request) bool {
				// TODO: Implement proper origin checking
				return true
			},
			EnableCompression: config.EnableCompression,
		},
	}

	tm.ctx, tm.cancel = context.WithCancel(context.Background())

	return tm
}

// Start starts the transport manager and begins listening for connections
func (tm *TransportManager) Start() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.server != nil {
		return fmt.Errorf("transport manager already started")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", tm.handleWebSocket)

	tm.server = &http.Server{
		Addr:         tm.config.ListenAddr,
		Handler:      mux,
		ReadTimeout:  tm.config.HandshakeTimeout,
		WriteTimeout: tm.config.WriteTimeout,
	}

	go func() {
		if err := tm.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			tm.logger.Error("Failed to start transport server", "error", err)
		}
	}()

	tm.logger.Info("Transport manager started", "listen_addr", tm.config.ListenAddr)
	return nil
}

// Stop stops the transport manager
func (tm *TransportManager) Stop() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.server == nil {
		return nil
	}

	// Close all VirtualLinks
	for _, link := range tm.links {
		link.Close()
	}
	tm.links = make(map[string]*VirtualLink)

	// Stop HTTP server
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := tm.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown server: %w", err)
	}

	tm.server = nil
	tm.cancel()

	tm.logger.Info("Transport manager stopped")
	return nil
}

// handleWebSocket handles incoming WebSocket connections
func (tm *TransportManager) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Check connection limit
	tm.mu.RLock()
	if len(tm.links) >= tm.config.MaxConnections {
		tm.mu.RUnlock()
		http.Error(w, "Too many connections", http.StatusServiceUnavailable)
		return
	}
	tm.mu.RUnlock()

	// Upgrade to WebSocket
	conn, err := tm.upgrader.Upgrade(w, r, nil)
	if err != nil {
		tm.logger.Error("Failed to upgrade connection", "error", err)
		return
	}

	// Set connection parameters
	conn.SetReadLimit(tm.config.MaxMessageSize)
	conn.SetReadDeadline(time.Now().Add(tm.config.PongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(tm.config.PongWait))
		return nil
	})

	// Start ping ticker
	go tm.pingTicker(conn)

	// Handle the connection
	go tm.handleConnection(conn)
}

// pingTicker sends periodic pings to keep the connection alive
func (tm *TransportManager) pingTicker(conn *websocket.Conn) {
	ticker := time.NewTicker(tm.config.PingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(tm.config.WriteTimeout)); err != nil {
				tm.logger.Error("Failed to send ping", "error", err)
				return
			}
		case <-tm.ctx.Done():
			return
		}
	}
}

// handleConnection handles an individual WebSocket connection
func (tm *TransportManager) handleConnection(conn *websocket.Conn) {
	defer conn.Close()

	// Read initial handshake message
	_, data, err := conn.ReadMessage()
	if err != nil {
		tm.logger.Error("Failed to read handshake", "error", err)
		return
	}

	// Parse handshake
	handshake, err := tm.parseHandshake(data)
	if err != nil {
		tm.logger.Error("Failed to parse handshake", "error", err)
		return
	}

	// Create or get VirtualLink
	link, err := tm.getOrCreateVirtualLink(handshake, conn)
	if err != nil {
		tm.logger.Error("Failed to create VirtualLink", "error", err)
		return
	}

	// Handle the VirtualLink
	tm.handleVirtualLink(link, conn)
}

// Handshake represents the initial connection handshake
type Handshake struct {
	NodeID       string            `json:"node_id"`
	LinkID       string            `json:"link_id"`
	Capabilities map[string]string `json:"capabilities"`
	Version      string            `json:"version"`
}

// parseHandshake parses the handshake message
func (tm *TransportManager) parseHandshake(data []byte) (*Handshake, error) {
	var handshake Handshake
	if err := json.Unmarshal(data, &handshake); err != nil {
		return nil, fmt.Errorf("failed to unmarshal handshake: %w", err)
	}

	if handshake.NodeID == "" {
		return nil, fmt.Errorf("missing node_id in handshake")
	}

	if handshake.LinkID == "" {
		return nil, fmt.Errorf("missing link_id in handshake")
	}

	return &handshake, nil
}

// getOrCreateVirtualLink gets or creates a VirtualLink for the connection
func (tm *TransportManager) getOrCreateVirtualLink(handshake *Handshake, conn *websocket.Conn) (*VirtualLink, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	linkID := handshake.LinkID
	link, exists := tm.links[linkID]

	if !exists {
		// Create new VirtualLink
		config := VirtualLinkConfig{
			MaxLanes:           4,
			CollapsibleMode:    false,
			HeartbeatInterval:  30 * time.Second,
			QueueBufferSize:    1000,
			AdaptiveScaling:    true,
			ScaleUpThreshold:   10 * time.Second,
			ScaleDownThreshold: 60 * time.Second,
			CooldownPeriod:     30 * time.Second,
		}

		link = NewVirtualLink(linkID, "local", handshake.NodeID, config, tm.logger)
		tm.links[linkID] = link

		tm.logger.Info("Created new VirtualLink", "link_id", linkID, "remote_node", handshake.NodeID)
	}

	// Assign the WebSocket connection to the appropriate lane
	// For now, assign to lane 0 (control lane)
	if lane, exists := link.Lanes[0]; exists {
		lane.Conn = conn
		lane.updateStatus(LaneStatusConnected)
		tm.logger.Info("Assigned connection to control lane", "link_id", linkID, "lane_id", 0)
	}

	return link, nil
}

// handleVirtualLink handles the VirtualLink connection
func (tm *TransportManager) handleVirtualLink(link *VirtualLink, conn *websocket.Conn) {
	// Keep the connection alive until it's closed
	<-tm.ctx.Done()
}

// Connect establishes a connection to a remote node
func (tm *TransportManager) Connect(remoteAddr, nodeID, linkID string) (*VirtualLink, error) {
	// Create WebSocket connection
	conn, _, err := websocket.DefaultDialer.Dial(fmt.Sprintf("ws://%s/ws", remoteAddr), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to dial WebSocket: %w", err)
	}

	// Send handshake
	handshake := &Handshake{
		NodeID: nodeID,
		LinkID: linkID,
		Capabilities: map[string]string{
			"compression": "gzip",
			"version":     "1.0",
		},
		Version: "1.0",
	}

	handshakeData, err := json.Marshal(handshake)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to marshal handshake: %w", err)
	}

	if err := conn.WriteMessage(websocket.TextMessage, handshakeData); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to send handshake: %w", err)
	}

	// Create VirtualLink
	config := VirtualLinkConfig{
		MaxLanes:           4,
		CollapsibleMode:    false,
		HeartbeatInterval:  30 * time.Second,
		QueueBufferSize:    1000,
		AdaptiveScaling:    true,
		ScaleUpThreshold:   10 * time.Second,
		ScaleDownThreshold: 60 * time.Second,
		CooldownPeriod:     30 * time.Second,
	}

	link := NewVirtualLink(linkID, nodeID, nodeID, config, tm.logger)

	// Assign connection to control lane
	if lane, exists := link.Lanes[0]; exists {
		lane.Conn = conn
		lane.updateStatus(LaneStatusConnected)
	}

	// Store the link
	tm.mu.Lock()
	tm.links[linkID] = link
	tm.mu.Unlock()

	tm.logger.Info("Connected to remote node", "remote_addr", remoteAddr, "link_id", linkID)

	return link, nil
}

// GetLink gets a VirtualLink by ID
func (tm *TransportManager) GetLink(linkID string) (*VirtualLink, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	link, exists := tm.links[linkID]
	return link, exists
}

// ListLinks returns all VirtualLinks
func (tm *TransportManager) ListLinks() []*VirtualLink {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	links := make([]*VirtualLink, 0, len(tm.links))
	for _, link := range tm.links {
		links = append(links, link)
	}

	return links
}

// CloseLink closes a specific VirtualLink
func (tm *TransportManager) CloseLink(linkID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	link, exists := tm.links[linkID]
	if !exists {
		return fmt.Errorf("link %s not found", linkID)
	}

	if err := link.Close(); err != nil {
		return fmt.Errorf("failed to close link: %w", err)
	}

	delete(tm.links, linkID)
	tm.logger.Info("Closed VirtualLink", "link_id", linkID)

	return nil
}

// Stats returns statistics for all VirtualLinks
func (tm *TransportManager) Stats() map[string]map[int]LaneStats {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	stats := make(map[string]map[int]LaneStats)
	for linkID, link := range tm.links {
		stats[linkID] = link.Stats()
	}

	return stats
}
