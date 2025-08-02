package network

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/messages"
)

// MessageHandler handles incoming WebSocket messages
type MessageHandler interface {
	HandleMessage(msg *messages.Message) error
}

// Config holds the WebSocket server configuration
type Config struct {
	Address          string
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	TLSConfig        *tls.Config
	MaxMessageSize   int64
	PingInterval     time.Duration
	PongTimeout      time.Duration
	CompressionLevel int
}

// Connection represents a WebSocket connection to a peer
type Connection struct {
	ID         string
	PeerID     string
	Address    string
	conn       *websocket.Conn
	isOutbound bool
	lastPing   time.Time
	lastSeen   time.Time
	status     string
	mu         sync.RWMutex
	writeChan  chan []byte
	stopChan   chan struct{}
	logger     *logger.Logger
}

// Server represents the WebSocket server
type Server struct {
	config      Config
	upgrader    websocket.Upgrader
	logger      *logger.Logger
	connections map[string]*Connection
	handler     MessageHandler
	mu          sync.RWMutex
	httpServer  *http.Server // Add HTTP server reference for proper shutdown
}

// NewServer creates a new WebSocket server
func NewServer(cfg Config, handler MessageHandler, logger *logger.Logger) *Server {
	return &Server{
		config: cfg,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				// TODO: Implement proper origin checking
				return true
			},
		},
		logger:      logger,
		connections: make(map[string]*Connection),
		handler:     handler,
	}
}

// Start starts the WebSocket server
func (s *Server) Start() error {
	http.HandleFunc("/ws", s.handleWebSocket)
	s.httpServer = &http.Server{
		Addr:         s.config.Address,
		TLSConfig:    s.config.TLSConfig,
		ReadTimeout:  s.config.ReadTimeout,
		WriteTimeout: s.config.WriteTimeout,
	}

	s.logger.Infof("Starting WebSocket server (address: %s)", s.config.Address)

	// Start HTTP server in a goroutine so it doesn't block startup
	started := make(chan error, 1)
	go func() {
		var err error
		if s.config.TLSConfig != nil {
			err = s.httpServer.ListenAndServeTLS("", "")
		} else {
			err = s.httpServer.ListenAndServe()
		}

		if err != nil && err != http.ErrServerClosed {
			s.logger.Errorf("HTTP server error: %v", err)
		}
		started <- err
	}()

	// Give the server a moment to start and check for immediate errors
	select {
	case err := <-started:
		if err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("failed to start HTTP server: %v", err)
		}
	case <-time.After(1 * time.Second):
		// Server started successfully (no immediate error)
		s.logger.Infof("WebSocket server started successfully on %s", s.config.Address)
	}

	return nil
}

// handleWebSocket handles incoming WebSocket connections
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	peerID := r.Header.Get("X-Peer-ID")
	if peerID == "" {
		http.Error(w, "peer ID is required", http.StatusBadRequest)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Errorf("Failed to upgrade connection (peer_id: %s): %v", peerID, err)
		return
	}

	connection := &Connection{
		ID:         fmt.Sprintf("in-%s-%d", peerID, time.Now().UnixNano()),
		PeerID:     peerID,
		Address:    r.RemoteAddr,
		conn:       conn,
		isOutbound: false,
		lastSeen:   time.Now(),
		status:     "connected",
		writeChan:  make(chan []byte, 100),
		stopChan:   make(chan struct{}),
		logger:     s.logger,
	}

	s.mu.Lock()
	s.connections[peerID] = connection
	s.mu.Unlock()

	s.logger.Infof("New WebSocket connection established (peer_id: %s)", peerID)

	go s.handleConnection(connection)
}

// handleConnection handles an individual WebSocket connection
func (s *Server) handleConnection(conn *Connection) {
	defer func() {
		conn.conn.Close()
		s.mu.Lock()
		delete(s.connections, conn.PeerID)
		s.mu.Unlock()
		close(conn.stopChan)
	}()

	// Start write pump
	go s.writePump(conn)

	for {
		select {
		case <-conn.stopChan:
			return
		default:
			_, messageData, err := conn.conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					s.logger.Errorf("Unexpected close error (peer_id: %s): %v", conn.PeerID, err)
				}
				return
			}

			var msg messages.Message
			if err := json.Unmarshal(messageData, &msg); err != nil {
				s.logger.Errorf("Failed to unmarshal message (peer_id: %s): %v", conn.PeerID, err)
				continue
			}

			// Set the sender ID if not already set
			if msg.Header.From == "" {
				msg.Header.From = conn.PeerID
			}

			// Update last seen
			conn.mu.Lock()
			conn.lastSeen = time.Now()
			conn.mu.Unlock()

			// Handle the message
			if err := s.handler.HandleMessage(&msg); err != nil {
				s.logger.Errorf("Failed to handle message (peer_id: %s): %v", conn.PeerID, err)
			}
		}
	}
}

// SendMessage sends a message to a specific peer
func (s *Server) SendMessage(peerID string, msg *messages.Message) error {
	s.mu.RLock()
	conn, exists := s.connections[peerID]
	s.mu.RUnlock()

	if !exists {
		return fmt.Errorf("peer %s not connected", peerID)
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	select {
	case conn.writeChan <- data:
		return nil
	default:
		return fmt.Errorf("write channel full for peer %s", peerID)
	}
}

// BroadcastMessage sends a message to all connected peers
func (s *Server) BroadcastMessage(msg *messages.Message) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var lastErr error
	for peerID := range s.connections {
		if err := s.SendMessage(peerID, msg); err != nil {
			s.logger.Errorf("Failed to send message to peer (peer_id: %s): %v", peerID, err)
			lastErr = err
		}
	}
	return lastErr
}

// CloseConnection closes the connection to a specific peer
func (s *Server) CloseConnection(peerID string) error {
	s.mu.Lock()
	conn, exists := s.connections[peerID]
	s.mu.Unlock()

	if !exists {
		return fmt.Errorf("peer %s not connected", peerID)
	}

	return conn.conn.Close()
}

// GetConnections returns all current connections
func (s *Server) GetConnections() map[string]*Connection {
	s.mu.RLock()
	defer s.mu.RUnlock()

	conns := make(map[string]*Connection, len(s.connections))
	for k, v := range s.connections {
		conns[k] = v
	}
	return conns
}

// writePump handles writing messages to a WebSocket connection
func (s *Server) writePump(conn *Connection) {
	ticker := time.NewTicker(s.config.PingInterval)
	defer func() {
		ticker.Stop()
		conn.conn.Close()
	}()

	for {
		select {
		case <-conn.stopChan:
			return
		case data := <-conn.writeChan:
			conn.conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
			if err := conn.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				s.logger.Errorf("Failed to write message to %s: %v", conn.PeerID, err)
				return
			}
		case <-ticker.C:
			conn.conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
			if err := conn.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				s.logger.Errorf("Failed to send ping to %s: %v", conn.PeerID, err)
				return
			}
			conn.mu.Lock()
			conn.lastPing = time.Now()
			conn.mu.Unlock()
		}
	}
}

// ConnectToPeer establishes an outbound WebSocket connection to a peer
func (s *Server) ConnectToPeer(peerID, address string) error {
	s.mu.RLock()
	if _, exists := s.connections[peerID]; exists {
		s.mu.RUnlock()
		return fmt.Errorf("connection to peer %s already exists", peerID)
	}
	s.mu.RUnlock()

	// Create WebSocket URL
	scheme := "ws"
	if s.config.TLSConfig != nil {
		scheme = "wss"
	}
	url := fmt.Sprintf("%s://%s/ws", scheme, address)

	// Create dialer with TLS config
	dialer := websocket.Dialer{
		TLSClientConfig:  s.config.TLSConfig,
		HandshakeTimeout: 45 * time.Second,
	}

	// Set headers
	headers := http.Header{}
	headers.Set("X-Peer-ID", peerID)

	// Connect to peer
	conn, _, err := dialer.Dial(url, headers)
	if err != nil {
		return fmt.Errorf("failed to connect to peer %s at %s: %v", peerID, address, err)
	}

	// Create connection wrapper
	connection := &Connection{
		ID:         fmt.Sprintf("out-%s-%d", peerID, time.Now().UnixNano()),
		PeerID:     peerID,
		Address:    address,
		conn:       conn,
		isOutbound: true,
		lastSeen:   time.Now(),
		status:     "connected",
		writeChan:  make(chan []byte, 100),
		stopChan:   make(chan struct{}),
		logger:     s.logger,
	}

	// Configure connection
	if s.config.MaxMessageSize > 0 {
		conn.SetReadLimit(s.config.MaxMessageSize)
	}
	conn.SetPongHandler(func(appData string) error {
		connection.mu.Lock()
		connection.lastSeen = time.Now()
		connection.mu.Unlock()
		return nil
	})

	// Add to connections map
	s.mu.Lock()
	s.connections[peerID] = connection
	s.mu.Unlock()

	// Start connection handlers
	go s.handleConnection(connection)

	s.logger.Infof("Connected to peer %s at %s", peerID, address)
	return nil
}

// Shutdown gracefully shuts down the WebSocket server
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// First, close all WebSocket connections to stop new messages
	s.logger.Infof("Closing %d WebSocket connections", len(s.connections))
	for _, conn := range s.connections {
		close(conn.stopChan)
		conn.conn.Close()
	}
	s.connections = make(map[string]*Connection) // Clear connections map

	// Then shutdown the HTTP server gracefully
	if s.httpServer != nil {
		s.logger.Infof("Shutting down HTTP server")
		if err := s.httpServer.Shutdown(ctx); err != nil {
			s.logger.Errorf("Failed to shutdown HTTP server: %v", err)
			return err
		}
		s.logger.Infof("HTTP server shutdown completed")
	}

	return nil
}
