package mesh

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	"github.com/redbco/redb-open/pkg/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Message types for core service communication
const (
	MessageTypeDBUpdate     = "db_update"
	MessageTypeAnchorQuery  = "anchor_query"
	MessageTypeAnchorResult = "anchor_result"
	MessageTypeCommand      = "command"
	MessageTypeResponse     = "response"
)

// CoreMessage represents a structured message between core services
type CoreMessage struct {
	Type      string                 `json:"type"`
	Operation string                 `json:"operation"`
	Data      map[string]interface{} `json:"data"`
	RequestID string                 `json:"request_id,omitempty"`
	Timestamp int64                  `json:"timestamp"`
}

// MessageHandler defines the interface for handling received messages
type MessageHandler func(ctx context.Context, msg *meshv1.Received) error

// MeshSubscription represents an active subscription to mesh messages
type MeshSubscription struct {
	stream  meshv1.MeshData_SubscribeClient
	cancel  context.CancelFunc
	filter  *meshv1.SubscribeRequest
	handler MessageHandler
}

// MeshCommunicationManager handles all mesh communication for the core service
type MeshCommunicationManager struct {
	meshDataClient    meshv1.MeshDataClient
	meshControlClient meshv1.MeshControlClient
	logger            *logger.Logger
	nodeID            uint64
	subscriptions     map[string]*MeshSubscription
	messageHandlers   map[string]MessageHandler
	pendingRequests   map[string]chan *meshv1.Received // For synchronous request-response
	mu                sync.RWMutex
	ctx               context.Context
	cancel            context.CancelFunc
}

// NewMeshCommunicationManager creates a new mesh communication manager
func NewMeshCommunicationManager(
	meshDataClient meshv1.MeshDataClient,
	meshControlClient meshv1.MeshControlClient,
	logger *logger.Logger,
	nodeID uint64,
) *MeshCommunicationManager {
	ctx, cancel := context.WithCancel(context.Background())

	mgr := &MeshCommunicationManager{
		meshDataClient:    meshDataClient,
		meshControlClient: meshControlClient,
		logger:            logger,
		nodeID:            nodeID,
		subscriptions:     make(map[string]*MeshSubscription),
		messageHandlers:   make(map[string]MessageHandler),
		pendingRequests:   make(map[string]chan *meshv1.Received),
		ctx:               ctx,
		cancel:            cancel,
	}

	// Register default message handlers
	mgr.registerDefaultHandlers()

	return mgr
}

// Start initializes the mesh communication manager
func (m *MeshCommunicationManager) Start(ctx context.Context) error {
	m.logger.Infof("Starting mesh communication manager for node %d", m.nodeID)

	// Subscribe to all messages for this node
	if err := m.SubscribeToMessages(ctx, nil); err != nil {
		return fmt.Errorf("failed to subscribe to mesh messages: %w", err)
	}

	m.logger.Infof("Mesh communication manager started successfully")
	return nil
}

// Stop gracefully shuts down the mesh communication manager
func (m *MeshCommunicationManager) Stop() error {
	m.logger.Infof("Stopping mesh communication manager")

	m.mu.Lock()
	defer m.mu.Unlock()

	// Cancel all subscriptions
	for subID, sub := range m.subscriptions {
		sub.cancel()
		delete(m.subscriptions, subID)
	}

	// Cancel pending requests
	for reqID, ch := range m.pendingRequests {
		close(ch)
		delete(m.pendingRequests, reqID)
	}

	m.cancel()
	m.logger.Infof("Mesh communication manager stopped")
	return nil
}

// SendMessage sends a message to another node in the mesh
func (m *MeshCommunicationManager) SendMessage(ctx context.Context, targetNodeID uint64, message *CoreMessage) (*meshv1.SendResponse, error) {
	// Serialize the message
	payload, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize message: %w", err)
	}

	// Create send request
	req := &meshv1.SendRequest{
		DstNode:    targetNodeID,
		Payload:    payload,
		CorrId:     uint64(time.Now().UnixNano()), // Use timestamp as correlation ID
		RequireAck: true,                          // Require acknowledgment for all messages
		Mode:       meshv1.SendMode_SEND_MODE_WAIT_FOR_DELIVERY,
		QosClass:   m.getQoSClass(message.Type),
		Partition:  m.getPartition(message.Type),
	}

	// Add headers
	req.Headers = []*meshv1.Header{
		{Key: "message_type", Value: []byte(message.Type)},
		{Key: "operation", Value: []byte(message.Operation)},
		{Key: "source_node", Value: []byte(fmt.Sprintf("%d", m.nodeID))},
	}

	// Send the message
	resp, err := m.meshDataClient.Send(ctx, req)
	if err != nil {
		m.logger.Errorf("Failed to send message to node %d: %v", targetNodeID, err)
		return nil, err
	}

	m.logger.Debugf("Message sent to node %d, msg_id: %d, status: %v",
		targetNodeID, resp.MsgId, resp.Status)

	return resp, nil
}

// SendMessageWithResponse sends a message and waits for a response
func (m *MeshCommunicationManager) SendMessageWithResponse(ctx context.Context, targetNodeID uint64, message *CoreMessage, timeout time.Duration) (*CoreMessage, error) {
	// Generate request ID for correlation
	requestID := fmt.Sprintf("%d_%d", m.nodeID, time.Now().UnixNano())
	message.RequestID = requestID

	// Create response channel
	respCh := make(chan *meshv1.Received, 1)

	m.mu.Lock()
	m.pendingRequests[requestID] = respCh
	m.mu.Unlock()

	// Cleanup on exit
	defer func() {
		m.mu.Lock()
		delete(m.pendingRequests, requestID)
		m.mu.Unlock()
		close(respCh)
	}()

	// Send the message
	_, err := m.SendMessage(ctx, targetNodeID, message)
	if err != nil {
		return nil, err
	}

	// Wait for response with timeout
	select {
	case resp := <-respCh:
		var coreMsg CoreMessage
		if err := json.Unmarshal(resp.Payload, &coreMsg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal response: %w", err)
		}
		return &coreMsg, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for response from node %d", targetNodeID)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// SubscribeToMessages subscribes to messages from the mesh
func (m *MeshCommunicationManager) SubscribeToMessages(ctx context.Context, filter *meshv1.SubscribeRequest) error {
	if filter == nil {
		filter = &meshv1.SubscribeRequest{
			// Subscribe to all messages for this node
			Partition: 0, // All partitions
			QosClass:  0, // All QoS classes
			SrcNode:   0, // All source nodes
		}
	}

	// Create subscription context
	subCtx, cancel := context.WithCancel(m.ctx)

	// Subscribe to mesh messages
	stream, err := m.meshDataClient.Subscribe(subCtx, filter)
	if err != nil {
		cancel()
		return fmt.Errorf("failed to subscribe to mesh: %w", err)
	}

	// Create subscription
	subID := fmt.Sprintf("main_%d", time.Now().UnixNano())
	subscription := &MeshSubscription{
		stream:  stream,
		cancel:  cancel,
		filter:  filter,
		handler: m.handleReceivedMessage,
	}

	m.mu.Lock()
	m.subscriptions[subID] = subscription
	m.mu.Unlock()

	// Start message processing goroutine
	go m.processMessages(subID, subscription)

	m.logger.Infof("Subscribed to mesh messages with subscription ID: %s", subID)
	return nil
}

// processMessages processes incoming messages from a subscription
func (m *MeshCommunicationManager) processMessages(subID string, sub *MeshSubscription) {
	defer func() {
		m.mu.Lock()
		delete(m.subscriptions, subID)
		m.mu.Unlock()
		m.logger.Infof("Subscription %s ended", subID)
	}()

	for {
		select {
		case <-m.ctx.Done():
			return
		default:
			msg, err := sub.stream.Recv()
			if err != nil {
				if status.Code(err) == codes.Canceled {
					return // Normal cancellation
				}
				m.logger.Errorf("Error receiving message from subscription %s: %v", subID, err)
				return
			}

			// Handle the message
			if err := sub.handler(m.ctx, msg); err != nil {
				m.logger.Errorf("Error handling message: %v", err)
			}

			// Send acknowledgment if required
			if msg.RequireAck {
				ack := &meshv1.Ack{
					SrcNode: msg.SrcNode,
					MsgId:   msg.MsgId,
					Success: true,
					Message: "Message processed successfully",
				}

				if _, err := m.meshDataClient.AckMessage(m.ctx, ack); err != nil {
					m.logger.Errorf("Failed to acknowledge message %d: %v", msg.MsgId, err)
				}
			}
		}
	}
}

// handleReceivedMessage handles incoming messages from other nodes
func (m *MeshCommunicationManager) handleReceivedMessage(ctx context.Context, msg *meshv1.Received) error {
	m.logger.Debugf("Received message from node %d, msg_id: %d, corr_id: %d",
		msg.SrcNode, msg.MsgId, msg.CorrId)

	// Parse the core message
	var coreMsg CoreMessage
	if err := json.Unmarshal(msg.Payload, &coreMsg); err != nil {
		m.logger.Errorf("Failed to unmarshal core message: %v", err)
		return err
	}

	// Check if this is a response to a pending request
	if coreMsg.RequestID != "" && coreMsg.Type == MessageTypeResponse {
		m.mu.RLock()
		if respCh, exists := m.pendingRequests[coreMsg.RequestID]; exists {
			select {
			case respCh <- msg:
			default:
				m.logger.Warnf("Response channel full for request %s", coreMsg.RequestID)
			}
		}
		m.mu.RUnlock()
		return nil
	}

	// Handle based on message type
	m.mu.RLock()
	handler, exists := m.messageHandlers[coreMsg.Type]
	m.mu.RUnlock()

	if !exists {
		m.logger.Warnf("No handler registered for message type: %s", coreMsg.Type)
		return nil
	}

	return handler(ctx, msg)
}

// RegisterMessageHandler registers a handler for a specific message type
func (m *MeshCommunicationManager) RegisterMessageHandler(messageType string, handler MessageHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messageHandlers[messageType] = handler
	m.logger.Infof("Registered handler for message type: %s", messageType)
}

// registerDefaultHandlers registers default message handlers
func (m *MeshCommunicationManager) registerDefaultHandlers() {
	m.RegisterMessageHandler(MessageTypeDBUpdate, m.handleDBUpdate)
	m.RegisterMessageHandler(MessageTypeAnchorQuery, m.handleAnchorQuery)
	m.RegisterMessageHandler(MessageTypeCommand, m.handleCommand)
}

// handleDBUpdate handles database update notifications
func (m *MeshCommunicationManager) handleDBUpdate(ctx context.Context, msg *meshv1.Received) error {
	var coreMsg CoreMessage
	if err := json.Unmarshal(msg.Payload, &coreMsg); err != nil {
		return err
	}

	m.logger.Infof("Received DB update from node %d: operation=%s", msg.SrcNode, coreMsg.Operation)

	// TODO: Implement database update handling
	// This could trigger local cache invalidation, replication, etc.

	return nil
}

// handleAnchorQuery handles anchor service queries from other nodes
func (m *MeshCommunicationManager) handleAnchorQuery(ctx context.Context, msg *meshv1.Received) error {
	var coreMsg CoreMessage
	if err := json.Unmarshal(msg.Payload, &coreMsg); err != nil {
		return err
	}

	m.logger.Infof("Received anchor query from node %d: operation=%s", msg.SrcNode, coreMsg.Operation)

	// TODO: Implement anchor query handling
	// This would call the local anchor service and send back results

	// Example response (implement actual logic)
	response := &CoreMessage{
		Type:      MessageTypeAnchorResult,
		Operation: coreMsg.Operation + "_result",
		Data:      map[string]interface{}{"status": "success"},
		RequestID: coreMsg.RequestID,
		Timestamp: time.Now().Unix(),
	}

	if coreMsg.RequestID != "" {
		response.Type = MessageTypeResponse
		_, err := m.SendMessage(ctx, msg.SrcNode, response)
		return err
	}

	return nil
}

// handleCommand handles command messages from other nodes
func (m *MeshCommunicationManager) handleCommand(ctx context.Context, msg *meshv1.Received) error {
	var coreMsg CoreMessage
	if err := json.Unmarshal(msg.Payload, &coreMsg); err != nil {
		return err
	}

	m.logger.Infof("Received command from node %d: operation=%s", msg.SrcNode, coreMsg.Operation)

	// TODO: Implement command handling based on operation
	// This could handle various administrative commands

	return nil
}

// getQoSClass returns the QoS class for a message type
func (m *MeshCommunicationManager) getQoSClass(messageType string) uint32 {
	switch messageType {
	case MessageTypeDBUpdate:
		return 2 // High priority
	case MessageTypeAnchorQuery:
		return 1 // Medium priority
	case MessageTypeCommand:
		return 1 // Medium priority
	default:
		return 1 // Default medium priority
	}
}

// getPartition returns the partition for a message type
func (m *MeshCommunicationManager) getPartition(messageType string) uint32 {
	switch messageType {
	case MessageTypeDBUpdate:
		return 1 // Data partition
	case MessageTypeAnchorQuery:
		return 2 // Query partition
	case MessageTypeCommand:
		return 0 // Control partition
	default:
		return 1 // Default data partition
	}
}

// BroadcastDBUpdate broadcasts a database update to all nodes in the mesh
func (m *MeshCommunicationManager) BroadcastDBUpdate(ctx context.Context, operation string, data map[string]interface{}) error {
	// Get topology to find all nodes
	topology, err := m.meshControlClient.GetTopology(ctx, &meshv1.GetTopologyRequest{})
	if err != nil {
		return fmt.Errorf("failed to get topology: %w", err)
	}

	message := &CoreMessage{
		Type:      MessageTypeDBUpdate,
		Operation: operation,
		Data:      data,
		Timestamp: time.Now().Unix(),
	}

	// Send to all neighbors (they will forward to other nodes)
	for _, neighbor := range topology.Topology.Neighbors {
		if neighbor.NodeId != m.nodeID {
			go func(nodeID uint64) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if _, err := m.SendMessage(ctx, nodeID, message); err != nil {
					m.logger.Errorf("Failed to send DB update to node %d: %v", nodeID, err)
				}
			}(neighbor.NodeId)
		}
	}

	return nil
}

// QueryAnchorService queries the anchor service on a specific node
func (m *MeshCommunicationManager) QueryAnchorService(ctx context.Context, targetNodeID uint64, query map[string]interface{}) (map[string]interface{}, error) {
	message := &CoreMessage{
		Type:      MessageTypeAnchorQuery,
		Operation: "query",
		Data:      query,
		Timestamp: time.Now().Unix(),
	}

	response, err := m.SendMessageWithResponse(ctx, targetNodeID, message, 30*time.Second)
	if err != nil {
		return nil, err
	}

	return response.Data, nil
}
