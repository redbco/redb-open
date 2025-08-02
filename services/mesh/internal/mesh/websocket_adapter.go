package mesh

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/messages"
	"github.com/redbco/redb-open/services/mesh/internal/network"
)

// WebSocketNetworkAdapter implements the Network interface using WebSocket server
type WebSocketNetworkAdapter struct {
	server         *network.Server
	logger         *logger.Logger
	msgChan        chan messages.Message
	stopChan       chan struct{}
	messageHandler MessageHandler
}

// MessageHandler interface for handling incoming messages
type MessageHandler interface {
	HandleMessage(msg *messages.Message) error
}

// NewWebSocketNetworkAdapter creates a new WebSocket network adapter
func NewWebSocketNetworkAdapter(cfg network.Config, handler MessageHandler, logger *logger.Logger) (*WebSocketNetworkAdapter, error) {
	server := network.NewServer(cfg, handler, logger)

	adapter := &WebSocketNetworkAdapter{
		server:         server,
		logger:         logger,
		msgChan:        make(chan messages.Message, 100),
		stopChan:       make(chan struct{}),
		messageHandler: handler,
	}

	return adapter, nil
}

// Start starts the WebSocket network adapter
func (w *WebSocketNetworkAdapter) Start() error {
	return w.server.Start()
}

// Stop stops the WebSocket network adapter
func (w *WebSocketNetworkAdapter) Stop() error {
	close(w.stopChan)

	// Use timeout for server shutdown to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return w.server.Shutdown(ctx)
}

// SendMessage sends a message to a specific target using WebSocket
func (w *WebSocketNetworkAdapter) SendMessage(target string, msgType string, payload interface{}) error {
	// Convert payload to JSON
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	// Convert to new message format
	msg := &messages.Message{
		Header: messages.MessageHeader{
			Version:   messages.MessageVersionV1,
			Type:      msgType,
			To:        target,
			Timestamp: time.Now().UnixNano(),
		},
		Payload: payloadBytes,
	}

	return w.server.SendMessage(target, msg)
}

// Broadcast sends a message to all connected nodes using WebSocket
func (w *WebSocketNetworkAdapter) Broadcast(msgType string, payload interface{}) error {
	// Convert payload to JSON
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	// Convert to new message format
	msg := &messages.Message{
		Header: messages.MessageHeader{
			Version:   messages.MessageVersionV1,
			Type:      msgType,
			Timestamp: time.Now().UnixNano(),
		},
		Payload: payloadBytes,
	}

	return w.server.BroadcastMessage(msg)
}

// MessageChannel returns the channel for receiving messages (for backward compatibility)
func (w *WebSocketNetworkAdapter) MessageChannel() <-chan messages.Message {
	return w.msgChan
}

// AddConnection establishes a WebSocket connection to a peer
func (w *WebSocketNetworkAdapter) AddConnection(peerID string, conn interface{}) error {
	// For WebSocket, we use ConnectToPeer instead of AddConnection
	// This method is kept for interface compatibility but doesn't do anything
	// Actual connections are established via ConnectToPeer
	return nil
}

// RemoveConnection removes a WebSocket connection
func (w *WebSocketNetworkAdapter) RemoveConnection(peerID string) error {
	return w.server.CloseConnection(peerID)
}

// ConnectToPeer establishes a WebSocket connection to a peer
func (w *WebSocketNetworkAdapter) ConnectToPeer(peerID, address string) error {
	return w.server.ConnectToPeer(peerID, address)
}

// GetConnections returns all current WebSocket connections
func (w *WebSocketNetworkAdapter) GetConnections() map[string]*network.Connection {
	return w.server.GetConnections()
}
