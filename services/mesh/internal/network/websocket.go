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
)

// Message represents a message sent over WebSocket
type Message struct {
	Type    string          `json:"type"`
	From    string          `json:"from"`
	To      string          `json:"to"`
	Content json.RawMessage `json:"content"`
}

// MessageHandler handles incoming WebSocket messages
type MessageHandler interface {
	HandleMessage(msg *Message) error
}

// Config holds the WebSocket server configuration
type Config struct {
	Address      string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	TLSConfig    *tls.Config
}

// Connection represents a WebSocket connection to a peer
type Connection struct {
	ID        string
	conn      *websocket.Conn
	writeMu   sync.Mutex
	logger    *logger.Logger
	closeChan chan struct{}
}

// Server represents the WebSocket server
type Server struct {
	config      Config
	upgrader    websocket.Upgrader
	logger      *logger.Logger
	connections map[string]*Connection
	handler     MessageHandler
	mu          sync.RWMutex
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
	server := &http.Server{
		Addr:         s.config.Address,
		TLSConfig:    s.config.TLSConfig,
		ReadTimeout:  s.config.ReadTimeout,
		WriteTimeout: s.config.WriteTimeout,
	}

	s.logger.Infof("Starting WebSocket server (address: %s)", s.config.Address)

	if s.config.TLSConfig != nil {
		return server.ListenAndServeTLS("", "")
	}
	return server.ListenAndServe()
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
		ID:        peerID,
		conn:      conn,
		logger:    s.logger,
		closeChan: make(chan struct{}),
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
		delete(s.connections, conn.ID)
		s.mu.Unlock()
		close(conn.closeChan)
	}()

	for {
		_, message, err := conn.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				s.logger.Errorf("Unexpected close error (peer_id: %s): %v", conn.ID, err)
			}
			return
		}

		var msg Message
		if err := json.Unmarshal(message, &msg); err != nil {
			s.logger.Errorf("Failed to unmarshal message (peer_id: %s): %v", conn.ID, err)
			continue
		}

		// Set the sender ID
		msg.From = conn.ID

		// Handle the message
		if err := s.handler.HandleMessage(&msg); err != nil {
			s.logger.Errorf("Failed to handle message (peer_id: %s): %v", conn.ID, err)
		}
	}
}

// SendMessage sends a message to a specific peer
func (s *Server) SendMessage(peerID string, msg *Message) error {
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

	conn.writeMu.Lock()
	defer conn.writeMu.Unlock()

	return conn.conn.WriteMessage(websocket.TextMessage, data)
}

// BroadcastMessage sends a message to all connected peers
func (s *Server) BroadcastMessage(msg *Message) error {
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

// Shutdown gracefully shuts down the WebSocket server
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, conn := range s.connections {
		conn.conn.Close()
		<-conn.closeChan
	}

	return nil
}
