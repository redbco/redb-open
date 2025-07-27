package mesh

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/mesh/message"
)

// Message represents a message sent between nodes
type Message struct {
	Type      MessageType `json:"type"`
	From      string      `json:"from"`
	To        string      `json:"to"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

// NetworkConfig holds the configuration for the network layer
type NetworkConfig struct {
	ListenAddress string
	BufferSize    int
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
}

// Network represents the network layer of a mesh node
type Network struct {
	config      NetworkConfig
	logger      *logger.Logger
	listener    net.Listener
	connections map[string]net.Conn
	msgChan     chan message.Message
	stopChan    chan struct{}
	mu          sync.RWMutex
}

// NewNetwork creates a new network layer
func NewNetwork(cfg NetworkConfig, logger *logger.Logger) (*Network, error) {
	listener, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to create listener: %v", err)
	}

	return &Network{
		config:      cfg,
		logger:      logger,
		listener:    listener,
		connections: make(map[string]net.Conn),
		msgChan:     make(chan message.Message, 100),
		stopChan:    make(chan struct{}),
	}, nil
}

// SendMessage sends a message to a specific node
func (n *Network) SendMessage(target string, msgType string, payload interface{}) error {
	n.mu.RLock()
	conn, exists := n.connections[target]
	n.mu.RUnlock()

	if !exists {
		return fmt.Errorf("no connection to target node: %s", target)
	}

	msg := message.Message{
		Type:    message.Type(msgType),
		Payload: payload,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	if err := conn.SetWriteDeadline(time.Now().Add(n.config.WriteTimeout)); err != nil {
		return fmt.Errorf("failed to set write deadline: %v", err)
	}

	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("failed to write message: %v", err)
	}

	return nil
}

// Broadcast sends a message to all connected nodes
func (n *Network) Broadcast(msgType string, payload interface{}) error {
	n.mu.RLock()
	defer n.mu.RUnlock()

	var lastErr error
	for target := range n.connections {
		if err := n.SendMessage(target, msgType, payload); err != nil {
			lastErr = err
			n.logger.Error("Failed to send message: (target: %s, error: %v)", target, err)
		}
	}

	return lastErr
}

// Close closes the network layer
func (n *Network) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, conn := range n.connections {
		if err := conn.Close(); err != nil {
			n.logger.Error("Failed to close connection: (error: %v)", err)
		}
	}

	if err := n.listener.Close(); err != nil {
		return fmt.Errorf("failed to close listener: %v", err)
	}

	close(n.stopChan)
	return nil
}

// Start starts the network layer
func (n *Network) Start() error {
	n.logger.Info("Network layer started: (address: %s)", n.config.ListenAddress)

	go n.acceptLoop()
	return nil
}

// acceptLoop accepts incoming connections
func (n *Network) acceptLoop() {
	for {
		select {
		case <-n.stopChan:
			return
		default:
			conn, err := n.listener.Accept()
			if err != nil {
				n.logger.Error("Failed to accept connection: (error: %v)", err)
				continue
			}

			go n.handleConnection(conn)
		}
	}
}

// handleConnection handles an incoming connection
func (n *Network) handleConnection(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, n.config.BufferSize)
	for {
		select {
		case <-n.stopChan:
			return
		default:
			// Set read deadline
			if err := conn.SetReadDeadline(time.Now().Add(n.config.ReadTimeout)); err != nil {
				n.logger.Error("Failed to set read deadline: (error: %v)", err)
				return
			}

			// Read message
			bytesRead, err := conn.Read(buffer)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				n.logger.Error("Failed to read message: (error: %v)", err)
				return
			}

			// Parse message
			var msg message.Message
			if err := json.Unmarshal(buffer[:bytesRead], &msg); err != nil {
				n.logger.Error("Failed to parse message: (error: %v)", err)
				continue
			}

			// Send message to channel
			select {
			case n.msgChan <- msg:
			default:
				n.logger.Warn("Message channel full, dropping message: (type: %s)", string(msg.Type))
			}
		}
	}
}

// MessageChannel returns the channel for receiving messages
func (n *Network) MessageChannel() <-chan message.Message {
	return n.msgChan
}

// AddConnection adds a new connection
func (n *Network) AddConnection(peerID string, conn net.Conn) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, exists := n.connections[peerID]; exists {
		return nil
	}

	n.connections[peerID] = conn
	go n.handleConnection(conn)

	return nil
}

// RemoveConnection removes a connection
func (n *Network) RemoveConnection(peerID string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	conn, exists := n.connections[peerID]
	if !exists {
		return nil
	}

	if err := conn.Close(); err != nil {
		n.logger.Error("Failed to close connection: (peer_id: %s, error: %v)", peerID, err)
	}

	delete(n.connections, peerID)
	return nil
}
