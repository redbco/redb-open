package mesh

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	"github.com/redbco/redb-open/pkg/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Message types for core service communication
const (
	MessageTypeDBUpdate            = "db_update"
	MessageTypeAnchorQuery         = "anchor_query"
	MessageTypeAnchorResult        = "anchor_result"
	MessageTypeCommand             = "command"
	MessageTypeResponse            = "response"
	MessageTypeMeshEvent           = "mesh_event"
	MessageTypeDatabaseSyncRequest = "database_sync_request"
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
	ctx     context.Context
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
	eventManager      *MeshEventManager                // Reference to event manager for handling mesh events
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
	// Create a context that can be cancelled for shutdown
	// We'll update this when Start() is called with the proper parent context
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
	// Enable console logging for debugging
	m.logger.EnableConsoleOutput()
	m.logger.Debug("Starting mesh manager for node %d", m.nodeID)

	// Update our internal context to be derived from the provided context
	// This ensures proper cancellation propagation during shutdown
	m.logger.Debug("Updating mesh manager context")
	m.mu.Lock()
	m.cancel() // Cancel the old context
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.mu.Unlock()
	m.logger.Debug("Mesh manager context updated")

	// Subscribe to all messages for this node
	m.logger.Debug("Subscribing to mesh messages")
	if err := m.SubscribeToMessages(ctx, nil); err != nil {
		m.logger.Errorf("Failed to subscribe to mesh messages: %v", err)
		return fmt.Errorf("failed to subscribe to mesh messages: %w", err)
	}
	m.logger.Debug("Mesh message subscription completed")

	m.logger.Info("Mesh communication manager started successfully")
	return nil
}

// Stop gracefully shuts down the mesh communication manager
func (m *MeshCommunicationManager) Stop() error {
	m.logger.Debug("Stopping mesh manager")

	m.mu.Lock()
	defer m.mu.Unlock()

	m.logger.Debug("Mesh manager acquired lock")

	// First, cancel the main context to signal all goroutines to stop
	m.logger.Debug("Cancelling main context")
	m.cancel()
	m.logger.Debug("Main context cancelled")

	// Close all gRPC streams before cancelling subscriptions
	m.logger.Debug("Closing %d subscriptions", len(m.subscriptions))
	for subID, sub := range m.subscriptions {
		m.logger.Debug("Closing subscription %s", subID)

		// Close the send side of the stream to signal completion with timeout
		if sub.stream != nil {
			m.logger.Debug("Closing stream for subscription %s", subID)

			// Use a timeout for CloseSend to prevent hanging
			done := make(chan error, 1)
			go func() {
				done <- sub.stream.CloseSend()
			}()

			select {
			case err := <-done:
				if err != nil {
					m.logger.Warnf("Failed to close stream for subscription %s: %v", subID, err)
				} else {
					m.logger.Debug("Stream closed for subscription %s", subID)
				}
			case <-time.After(1 * time.Second):
				m.logger.Warnf("Stream close timed out for subscription %s", subID)
			}
		}

		// Cancel the subscription context
		m.logger.Debug("Cancelling subscription context %s", subID)
		sub.cancel()
		delete(m.subscriptions, subID)
		m.logger.Debug("Subscription %s cleaned up", subID)
	}

	// Cancel pending requests with proper cleanup
	m.logger.Debug("Cleaning up %d pending requests", len(m.pendingRequests))
	for reqID, ch := range m.pendingRequests {
		select {
		case <-ch:
			// Channel already closed
			m.logger.Debug("Request %s channel already closed", reqID)
		default:
			close(ch)
			m.logger.Debug("Closed request %s channel", reqID)
		}
		delete(m.pendingRequests, reqID)
	}

	m.logger.Info("Mesh communication manager stopped")
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
	// Check if mesh data client is available
	if m.meshDataClient == nil {
		m.logger.Warnf("Mesh data client not available, skipping subscription")
		return fmt.Errorf("mesh data client not initialized")
	}

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

	// Subscribe to mesh messages with timeout
	m.logger.Infof("Attempting to subscribe to mesh messages for node %d", m.nodeID)

	// Add timeout to prevent hanging during subscription
	subscribeCtx, subscribeCancel := context.WithTimeout(subCtx, 10*time.Second)
	defer subscribeCancel()

	stream, err := m.meshDataClient.Subscribe(subscribeCtx, filter)
	if err != nil {
		cancel()
		m.logger.Errorf("Failed to subscribe to mesh messages: %v", err)
		return fmt.Errorf("failed to subscribe to mesh: %w", err)
	}
	m.logger.Infof("Successfully established mesh subscription stream")

	// Create subscription
	subID := fmt.Sprintf("main_%d", time.Now().UnixNano())
	subscription := &MeshSubscription{
		stream:  stream,
		ctx:     subCtx,
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

	// Use a goroutine to handle Recv() calls with proper context cancellation
	msgCh := make(chan *meshv1.Received, 1)
	errCh := make(chan error, 1)
	recvDone := make(chan struct{})

	go func() {
		defer close(msgCh)
		defer close(errCh)
		defer close(recvDone)

		for {
			select {
			case <-m.ctx.Done():
				m.logger.Debugf("Recv goroutine for subscription %s stopping due to context cancellation", subID)
				return
			case <-sub.ctx.Done():
				m.logger.Debugf("Recv goroutine for subscription %s stopping due to subscription cancellation", subID)
				return
			default:
				// Check if stream is still valid before attempting Recv
				if sub.stream == nil {
					m.logger.Debugf("Stream for subscription %s is nil, stopping recv goroutine", subID)
					return
				}

				// Create a separate goroutine for the blocking Recv() call
				recvResultCh := make(chan struct {
					msg *meshv1.Received
					err error
				}, 1)

				go func() {
					msg, err := sub.stream.Recv()
					select {
					case recvResultCh <- struct {
						msg *meshv1.Received
						err error
					}{msg, err}:
					case <-m.ctx.Done():
						// Context cancelled while Recv was blocking
					case <-sub.ctx.Done():
						// Subscription cancelled while Recv was blocking
					}
				}()

				// Wait for Recv result or cancellation with timeout
				select {
				case result := <-recvResultCh:
					if result.err != nil {
						select {
						case errCh <- result.err:
						case <-m.ctx.Done():
						case <-sub.ctx.Done():
						}
						return
					}

					select {
					case msgCh <- result.msg:
					case <-m.ctx.Done():
						return
					case <-sub.ctx.Done():
						return
					}

				case <-time.After(1 * time.Second):
					// Timeout - check if we should continue or exit
					select {
					case <-m.ctx.Done():
						return
					case <-sub.ctx.Done():
						return
					default:
						// Continue loop for next iteration
					}

				case <-m.ctx.Done():
					m.logger.Debugf("Recv loop for subscription %s stopping due to context cancellation", subID)
					return
				case <-sub.ctx.Done():
					m.logger.Debugf("Recv loop for subscription %s stopping due to subscription cancellation", subID)
					return
				}
			}
		}
	}()

	for {
		select {
		case <-m.ctx.Done():
			m.logger.Debugf("Message processing loop for subscription %s stopping due to context cancellation", subID)
			return
		case <-sub.ctx.Done():
			m.logger.Debugf("Message processing loop for subscription %s stopping due to subscription cancellation", subID)
			return
		case err := <-errCh:
			if err != nil {
				if status.Code(err) == codes.Canceled || status.Code(err) == codes.Unavailable {
					m.logger.Debugf("Subscription %s ended normally: %v", subID, err)
					return // Normal cancellation or service unavailable
				}
				m.logger.Errorf("Error receiving message from subscription %s: %v", subID, err)
				return
			}
		case msg := <-msgCh:
			if msg == nil {
				return // Channel closed
			}

			// Handle the message with context checking
			select {
			case <-m.ctx.Done():
				return
			case <-sub.ctx.Done():
				return
			default:
				if err := sub.handler(m.ctx, msg); err != nil {
					m.logger.Errorf("Error handling message: %v", err)
				}
			}

			// Send acknowledgment if required
			if msg.RequireAck {
				ack := &meshv1.Ack{
					SrcNode: msg.SrcNode,
					MsgId:   msg.MsgId,
					Success: true,
					Message: "Message processed successfully",
				}

				// Use a timeout for acknowledgment to prevent blocking during shutdown
				ackCtx, ackCancel := context.WithTimeout(context.Background(), 1*time.Second)
				if _, err := m.meshDataClient.AckMessage(ackCtx, ack); err != nil {
					// Don't log errors during shutdown
					select {
					case <-m.ctx.Done():
					case <-sub.ctx.Done():
					default:
						m.logger.Errorf("Failed to acknowledge message %d: %v", msg.MsgId, err)
					}
				}
				ackCancel()
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

// SetEventManager sets the event manager reference (for circular dependency resolution)
func (m *MeshCommunicationManager) SetEventManager(eventManager *MeshEventManager) {
	m.logger.Debug("Setting event manager on mesh manager")

	m.mu.Lock()
	m.logger.Debug("Setting event manager field")
	m.eventManager = eventManager

	// Register mesh event handler directly without calling RegisterMessageHandler
	// to avoid recursive mutex lock
	m.logger.Debug("Registering message handler directly")
	m.messageHandlers[MessageTypeMeshEvent] = m.handleMeshEvent
	m.logger.Infof("Registered handler for message type: %s", MessageTypeMeshEvent)
	m.mu.Unlock()

	m.logger.Debug("Event manager set on mesh manager completed")
}

// registerDefaultHandlers registers default message handlers
func (m *MeshCommunicationManager) registerDefaultHandlers() {
	m.RegisterMessageHandler(MessageTypeDBUpdate, m.handleDBUpdate)
	m.RegisterMessageHandler(MessageTypeAnchorQuery, m.handleAnchorQuery)
	m.RegisterMessageHandler(MessageTypeCommand, m.handleCommand)
	// Note: mesh_event handler is registered when SetEventManager is called
}

// BroadcastStateEvent broadcasts a state event to all nodes in the mesh
func (m *MeshCommunicationManager) BroadcastStateEvent(ctx context.Context, event *meshv1.MeshStateEvent) error {
	m.logger.Infof("Broadcasting state event %s (seq: %d) to mesh", event.EventType, event.SequenceNumber)

	// Use the mesh data client to broadcast the event
	_, err := m.meshDataClient.BroadcastStateEvent(ctx, event)
	if err != nil {
		return fmt.Errorf("failed to broadcast state event: %w", err)
	}

	m.logger.Debugf("Successfully broadcasted state event %s (seq: %d)", event.EventType, event.SequenceNumber)
	return nil
}

// RequestDatabaseSync requests database synchronization from other nodes
func (m *MeshCommunicationManager) RequestDatabaseSync(ctx context.Context, req *meshv1.DatabaseSyncRequest) (*meshv1.DatabaseSyncResponse, error) {
	m.logger.Infof("Requesting database sync for table %s", req.TableName)

	// Use the mesh data client to request sync
	resp, err := m.meshDataClient.RequestDatabaseSync(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to request database sync: %w", err)
	}

	m.logger.Debugf("Received database sync response for table %s: %d records", req.TableName, len(resp.Records))
	return resp, nil
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

// handleMeshEvent handles mesh state events received from other nodes
func (m *MeshCommunicationManager) handleMeshEvent(ctx context.Context, msg *meshv1.Received) error {
	m.logger.Debugf("Received mesh event from node %d", msg.SrcNode)

	// Check if we have an event manager
	m.mu.RLock()
	eventManager := m.eventManager
	m.mu.RUnlock()

	if eventManager == nil {
		m.logger.Warnf("Received mesh event but no event manager is set, ignoring")
		return nil
	}

	// Parse the mesh event from the payload
	var eventData map[string]interface{}
	if err := json.Unmarshal(msg.Payload, &eventData); err != nil {
		return fmt.Errorf("failed to parse mesh event payload: %w", err)
	}

	// Extract event information
	eventTypeRaw, ok := eventData["event_type"]
	if !ok {
		return fmt.Errorf("mesh event missing event_type field")
	}

	eventType, ok := eventTypeRaw.(float64) // JSON numbers are float64
	if !ok {
		return fmt.Errorf("invalid event_type format")
	}

	// Extract other fields with defaults
	originatorNode := uint64(0)
	if val, ok := eventData["originator_node"].(float64); ok {
		originatorNode = uint64(val)
	}

	affectedNode := uint64(0)
	if val, ok := eventData["affected_node"].(float64); ok {
		affectedNode = uint64(val)
	}

	sequenceNumber := uint64(0)
	if val, ok := eventData["sequence_number"].(float64); ok {
		sequenceNumber = uint64(val)
	}

	timestamp := time.Now()
	if val, ok := eventData["timestamp"].(float64); ok {
		timestamp = time.Unix(int64(val), 0)
	}

	metadata := make(map[string]string)
	if val, ok := eventData["metadata"].(map[string]interface{}); ok {
		for k, v := range val {
			if str, ok := v.(string); ok {
				metadata[k] = str
			}
		}
	}

	payload := []byte{}
	if val, ok := eventData["payload"].(string); ok {
		payload = []byte(val)
	}

	// Create internal mesh event
	event := &MeshEvent{
		Type:           corev1.MeshEventType(int32(eventType)),
		OriginatorNode: originatorNode,
		AffectedNode:   affectedNode,
		Sequence:       sequenceNumber,
		Timestamp:      timestamp,
		Metadata:       metadata,
		Payload:        payload,
	}

	// Forward to event manager
	if err := eventManager.HandleReceivedEvent(ctx, event, msg.SrcNode); err != nil {
		m.logger.Errorf("Failed to handle received mesh event: %v", err)
		return err
	}

	m.logger.Debugf("Successfully processed mesh event %d from node %d", int32(eventType), msg.SrcNode)
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
