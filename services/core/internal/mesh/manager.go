package mesh

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
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

	// System-level synchronization (mesh, nodes, routes)
	MessageTypeMeshSyncRequest   = "mesh_sync_request"   // Request mesh data from peer
	MessageTypeMeshSyncResponse  = "mesh_sync_response"  // Response with mesh data
	MessageTypeNodeJoinNotify    = "node_join_notify"    // Notify about joining node (direct)
	MessageTypeNodeJoinBroadcast = "node_join_broadcast" // Broadcast joining node to mesh

	// User-level synchronization (tenants, users, workspaces, etc.)
	MessageTypeUserDataSyncRequest  = "user_data_sync_request"  // Request user-level data from peer
	MessageTypeUserDataSyncResponse = "user_data_sync_response" // Response with user-level data
)

// CoreMessage represents a structured message between core services
type CoreMessage struct {
	Type      string                 `json:"type"`
	Operation string                 `json:"operation"`
	Data      map[string]interface{} `json:"data"`
	RequestID string                 `json:"request_id,omitempty"`
	Timestamp int64                  `json:"timestamp"`
}

// ResponseAck represents an application-level acknowledgment
type ResponseAck struct {
	MsgID    uint64
	Success  bool
	Message  string
	Response *meshv1.Received // Optional response message
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
	pendingAcks       map[uint64]chan *ResponseAck     // For waiting on application-level ACKs (keyed by correlation ID)
	processedMessages map[string]time.Time             // For message deduplication (key: "srcNode:msgId:corrId")
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
		pendingAcks:       make(map[uint64]chan *ResponseAck),
		processedMessages: make(map[string]time.Time),
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

	m.mu.Lock()

	// Clean up any existing subscriptions before starting new ones
	m.logger.Debug("Cleaning up existing subscriptions")
	for subID, sub := range m.subscriptions {
		m.logger.Debug("Cancelling existing subscription %s", subID)
		sub.cancel()
		delete(m.subscriptions, subID)
	}

	// Cancel the old context and create a new one
	m.logger.Debug("Updating mesh manager context")
	m.cancel() // Cancel the old context
	m.ctx, m.cancel = context.WithCancel(ctx)

	m.mu.Unlock()
	m.logger.Debug("Mesh manager context updated")

	// Subscribe to all messages for this node using the updated internal context
	// This ensures the subscription uses the same context as the manager
	m.logger.Debug("Subscribing to mesh messages")
	if err := m.SubscribeToMessages(m.ctx, nil); err != nil {
		m.logger.Errorf("Failed to subscribe to mesh messages: %v", err)
		return fmt.Errorf("failed to subscribe to mesh messages: %w", err)
	}
	m.logger.Debug("Mesh message subscription completed")

	// Start cleanup routine for processed messages
	go m.cleanupProcessedMessages()

	m.logger.Info("Mesh communication manager started successfully")
	return nil
}

// cleanupProcessedMessages periodically removes old processed message entries
func (m *MeshCommunicationManager) cleanupProcessedMessages() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.mu.Lock()
			now := time.Now()
			count := 0
			for key, timestamp := range m.processedMessages {
				// Remove entries older than 2 minutes
				if now.Sub(timestamp) > 2*time.Minute {
					delete(m.processedMessages, key)
					count++
				}
			}
			if count > 0 {
				m.logger.Debugf("Cleaned up %d old processed message entries", count)
			}
			m.mu.Unlock()
		}
	}
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
	return m.SendMessageWithCorrID(ctx, targetNodeID, message, uint64(time.Now().UnixNano()))
}

// SendMessageWithCorrID sends a message with a specific correlation ID
// This is useful for responses that need to preserve the correlation ID from the request
// Uses FIRE_AND_FORGET mode to avoid blocking the message handler goroutine
func (m *MeshCommunicationManager) SendMessageWithCorrID(ctx context.Context, targetNodeID uint64, message *CoreMessage, corrID uint64) (*meshv1.SendResponse, error) {
	// Serialize the message
	payload, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize message: %w", err)
	}

	// Create send request
	// CRITICAL: Use FIRE_AND_FORGET for response messages sent from handlers
	// to avoid blocking the message processing goroutine
	req := &meshv1.SendRequest{
		DstNode:    targetNodeID,
		Payload:    payload,
		CorrId:     corrID,                                    // Use provided correlation ID
		RequireAck: true,                                      // Require acknowledgment for reliability
		Mode:       meshv1.SendMode_SEND_MODE_FIRE_AND_FORGET, // Non-blocking send from handlers
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
	m.logger.Debugf("Sending message to node %d: type=%s, operation=%s, corr_id=%d, require_ack=%v, mode=%v",
		targetNodeID, message.Type, message.Operation, corrID, req.RequireAck, req.Mode)

	resp, err := m.meshDataClient.Send(ctx, req)
	if err != nil {
		m.logger.Errorf("Failed to send message to node %d: %v", targetNodeID, err)
		return nil, err
	}

	m.logger.Infof("Message sent to node %d: msg_id=%d, corr_id=%d, type=%s, operation=%s, status=%v, require_ack=%v",
		targetNodeID, resp.MsgId, corrID, message.Type, message.Operation, resp.Status, resp.RequireAck)

	return resp, nil
}

// SendMessageWithCallback sends a message with FIRE_AND_FORGET and registers a callback
// for application-level ACK. Returns immediately after sending.
func (m *MeshCommunicationManager) SendMessageWithCallback(ctx context.Context, targetNodeID uint64, message *CoreMessage, ackChan chan *ResponseAck) error {
	// Serialize the message
	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	// Generate a unique correlation ID for this request-response pair
	corrId := uint64(time.Now().UnixNano())

	// Create send request with FIRE_AND_FORGET for non-blocking send
	req := &meshv1.SendRequest{
		DstNode:    targetNodeID,
		Payload:    payload,
		CorrId:     corrId,                                    // Unique correlation ID
		RequireAck: true,                                      // We want application-level ACK
		Mode:       meshv1.SendMode_SEND_MODE_FIRE_AND_FORGET, // Non-blocking send
		QosClass:   m.getQoSClass(message.Type),
		Partition:  m.getPartition(message.Type),
	}

	// Add headers
	req.Headers = []*meshv1.Header{
		{Key: "message_type", Value: []byte(message.Type)},
		{Key: "operation", Value: []byte(message.Operation)},
		{Key: "source_node", Value: []byte(fmt.Sprintf("%d", m.nodeID))},
	}

	// Register the callback channel for this correlation ID BEFORE sending
	// This ensures we don't miss any fast responses
	m.mu.Lock()
	m.pendingAcks[corrId] = ackChan
	m.mu.Unlock()

	// Send the message
	m.logger.Debugf("Sending message with callback to node %d: type=%s, operation=%s, corr_id=%d",
		targetNodeID, message.Type, message.Operation, corrId)

	resp, err := m.meshDataClient.Send(ctx, req)
	if err != nil {
		m.logger.Errorf("Failed to send message to node %d: %v", targetNodeID, err)
		// Clean up the pending ACK registration on error
		m.mu.Lock()
		delete(m.pendingAcks, corrId)
		m.mu.Unlock()
		return err
	}

	m.logger.Infof("Message sent to node %d: msg_id=%d, corr_id=%d, type=%s, operation=%s, waiting for application ACK",
		targetNodeID, resp.MsgId, corrId, message.Type, message.Operation)

	return nil
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

	// Subscribe to mesh messages with timeout for the initial connection only
	m.logger.Infof("Attempting to subscribe to mesh messages for node %d", m.nodeID)

	// Use the subscription context directly for the Subscribe call
	// This ensures the stream is tied to the long-lived subscription context
	stream, err := m.meshDataClient.Subscribe(subCtx, filter)

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

	m.logger.Infof("Starting message processing for subscription %s", subID)

	// Use a goroutine to handle Recv() calls with proper context cancellation
	// Buffer size of 1024 to handle bursts of messages without blocking
	msgCh := make(chan *meshv1.Received, 1024)
	errCh := make(chan error, 1)
	recvDone := make(chan struct{})

	go func() {
		defer close(msgCh)
		defer close(errCh)
		defer close(recvDone)

		for {
			// Check if contexts are cancelled before attempting Recv
			select {
			case <-m.ctx.Done():
				m.logger.Debugf("Recv goroutine for subscription %s stopping due to context cancellation", subID)
				return
			case <-sub.ctx.Done():
				m.logger.Debugf("Recv goroutine for subscription %s stopping due to subscription cancellation", subID)
				return
			default:
			}

			// Check if stream is still valid before attempting Recv
			if sub.stream == nil {
				m.logger.Debugf("Stream for subscription %s is nil, stopping recv goroutine", subID)
				return
			}

			// Perform blocking Recv call
			msg, err := sub.stream.Recv()

			if err != nil {
				m.logger.Debugf("Recv error for subscription %s: %v", subID, err)
				select {
				case errCh <- err:
				case <-m.ctx.Done():
				case <-sub.ctx.Done():
				}
				return
			}

			m.logger.Debugf("Received message for subscription %s from node %d", subID, msg.SrcNode)

			// Send message to processing loop
			select {
			case msgCh <- msg:
			case <-m.ctx.Done():
				return
			case <-sub.ctx.Done():
				return
			}
		}
	}()

	// Add a keepalive ticker to ensure the subscription stays active
	keepaliveTicker := time.NewTicker(25 * time.Second) // Slightly less than the 30-second heartbeat
	defer keepaliveTicker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			m.logger.Debugf("Message processing loop for subscription %s stopping due to context cancellation", subID)
			return
		case <-sub.ctx.Done():
			m.logger.Debugf("Message processing loop for subscription %s stopping due to subscription cancellation", subID)
			return
		case <-keepaliveTicker.C:
			// Send a keepalive ping to ensure the connection stays active
			m.logger.Debugf("Sending keepalive for subscription %s", subID)
			// We don't need to send an actual message, just the fact that we're checking
			// the channels should be enough to keep the gRPC connection active
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
				m.logger.Debugf("Received nil message from channel, subscription %s ending", subID)
				return // Channel closed
			}

			m.logger.Infof("=== PROCESSING MESSAGE: src=%d, msg_id=%d, corr_id=%d, require_ack=%v, payload_size=%d ===",
				msg.SrcNode, msg.MsgId, msg.CorrId, msg.RequireAck, len(msg.Payload))

			// Check for duplicate messages
			msgKey := fmt.Sprintf("%d:%d:%d", msg.SrcNode, msg.MsgId, msg.CorrId)
			m.mu.Lock()
			if lastProcessed, exists := m.processedMessages[msgKey]; exists {
				// Skip if processed within last 60 seconds
				if time.Since(lastProcessed) < 60*time.Second {
					m.logger.Warnf("Skipping duplicate message: src=%d, msg_id=%d, corr_id=%d (last processed %v ago)",
						msg.SrcNode, msg.MsgId, msg.CorrId, time.Since(lastProcessed))
					m.mu.Unlock()

					// Still send ACK if required to avoid message retry
					if msg.RequireAck {
						ack := &meshv1.Ack{
							SrcNode: msg.SrcNode,
							MsgId:   msg.MsgId,
							Success: true,
							Message: "Duplicate message, already processed",
						}
						ackCtx, ackCancel := context.WithTimeout(context.Background(), 1*time.Second)
						m.meshDataClient.AckMessage(ackCtx, ack)
						ackCancel()
					}
					continue
				}
			}
			// Mark as processed
			m.processedMessages[msgKey] = time.Now()
			m.mu.Unlock()

			// Handle the message with context checking
			var handlerErr error
			select {
			case <-m.ctx.Done():
				return
			case <-sub.ctx.Done():
				return
			default:
				handlerErr = sub.handler(m.ctx, msg)
				if handlerErr != nil {
					m.logger.Errorf("Error handling message: %v", handlerErr)
				}
			}

			// Send acknowledgment if required
			if msg.RequireAck {
				m.logger.Debugf("Message %d requires ACK, sending acknowledgment to node %d", msg.MsgId, msg.SrcNode)

				ackSuccess := handlerErr == nil
				ackMsg := "Message processed successfully"
				if !ackSuccess {
					ackMsg = fmt.Sprintf("Handler error: %v", handlerErr)
				}

				ack := &meshv1.Ack{
					SrcNode: msg.SrcNode,
					MsgId:   msg.MsgId,
					Success: ackSuccess,
					Message: ackMsg,
				}

				// Use a timeout for acknowledgment to prevent blocking during shutdown
				ackCtx, ackCancel := context.WithTimeout(context.Background(), 1*time.Second)
				if _, err := m.meshDataClient.AckMessage(ackCtx, ack); err != nil {
					// Don't log errors during shutdown
					select {
					case <-m.ctx.Done():
					case <-sub.ctx.Done():
					default:
						m.logger.Errorf("Failed to send ACK for message %d from node %d: %v", msg.MsgId, msg.SrcNode, err)
					}
				} else {
					m.logger.Debugf("Successfully sent ACK for message %d from node %d (success=%v)", msg.MsgId, msg.SrcNode, ackSuccess)

					// Check if this is a response message for which we have a pending callback
					// Response messages have correlation IDs that match the original request
					if msg.CorrId > 0 {
						m.mu.Lock()
						if ackChan, exists := m.pendingAcks[msg.CorrId]; exists {
							m.logger.Infof("Notifying callback for correlation ID %d (success=%v)", msg.CorrId, ackSuccess)
							// Send the ACK notification in a non-blocking way
							select {
							case ackChan <- &ResponseAck{
								MsgID:    msg.MsgId,
								Success:  ackSuccess,
								Message:  ackMsg,
								Response: msg,
							}:
								m.logger.Debugf("ACK notification sent for correlation ID %d", msg.CorrId)
							default:
								m.logger.Warnf("Could not send ACK notification for correlation ID %d (channel full or closed)", msg.CorrId)
							}
							// Clean up the pending ACK registration
							delete(m.pendingAcks, msg.CorrId)
						}
						m.mu.Unlock()
					}
				}
				ackCancel()
			} else {
				m.logger.Debugf("Message %d does not require ACK", msg.MsgId)
			}
		}
	}
}

// handleReceivedMessage handles incoming messages from other nodes
func (m *MeshCommunicationManager) handleReceivedMessage(ctx context.Context, msg *meshv1.Received) error {
	m.logger.Debugf("Received message from node %d, msg_id: %d, corr_id: %d, payload size: %d",
		msg.SrcNode, msg.MsgId, msg.CorrId, len(msg.Payload))

	// Parse the core message
	var coreMsg CoreMessage
	if err := json.Unmarshal(msg.Payload, &coreMsg); err != nil {
		m.logger.Errorf("Failed to unmarshal core message: %v", err)
		return err
	}

	m.logger.Debugf("Parsed message type: '%s', operation: '%s'", coreMsg.Type, coreMsg.Operation)

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
		m.logger.Warnf("No handler registered for message type: '%s' (operation: '%s')", coreMsg.Type, coreMsg.Operation)
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

	// System-level sync handlers
	m.RegisterMessageHandler(MessageTypeMeshSyncRequest, m.handleMeshSyncRequest)
	m.RegisterMessageHandler(MessageTypeMeshSyncResponse, m.handleMeshSyncResponse)
	m.RegisterMessageHandler(MessageTypeNodeJoinNotify, m.handleNodeJoinNotify)
	m.RegisterMessageHandler(MessageTypeNodeJoinBroadcast, m.handleNodeJoinBroadcast)
	m.RegisterMessageHandler("node_join_ack", m.handleNodeJoinAck)

	// User-level sync handlers
	m.RegisterMessageHandler(MessageTypeUserDataSyncRequest, m.handleUserDataSyncRequest)
	m.RegisterMessageHandler(MessageTypeUserDataSyncResponse, m.handleUserDataSyncResponse)

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

// === Mesh Synchronization Handlers ===

// handleMeshSyncRequest handles requests for mesh data from joining nodes
func (m *MeshCommunicationManager) handleMeshSyncRequest(ctx context.Context, msg *meshv1.Received) error {
	m.logger.Infof("Handling mesh sync request from node %d", msg.SrcNode)

	// Parse request message
	var reqMsg CoreMessage
	if err := json.Unmarshal(msg.Payload, &reqMsg); err != nil {
		m.logger.Errorf("Failed to unmarshal mesh sync request: %v", err)
		return fmt.Errorf("failed to unmarshal request: %w", err)
	}

	// Get sync manager
	var syncManager *DatabaseSyncManager
	if m.eventManager != nil {
		syncManager = m.eventManager.GetSyncManager()
	}

	if syncManager == nil {
		m.logger.Warn("Database sync manager not available, cannot provide mesh data")
		return fmt.Errorf("sync manager not available")
	}

	// Get mesh data for synchronization
	meshData, nodesData, routesData, err := syncManager.GetMeshDataForSync(ctx)
	if err != nil {
		m.logger.Errorf("Failed to get mesh data for sync: %v", err)
		return err
	}

	// Build response
	response := &CoreMessage{
		Type:      MessageTypeMeshSyncResponse,
		Operation: "sync_response",
		RequestID: reqMsg.RequestID, // Echo back request ID for correlation
		Data: map[string]interface{}{
			"mesh":   meshData,
			"nodes":  nodesData,
			"routes": routesData,
		},
		Timestamp: time.Now().Unix(),
	}

	// Send response back to requesting node using the same correlation ID from the request
	// This is critical for the callback mechanism to match the response to the request
	// Uses FIRE_AND_FORGET to avoid blocking the message handler goroutine
	_, err = m.SendMessageWithCorrID(ctx, msg.SrcNode, response, msg.CorrId)
	if err != nil {
		m.logger.Errorf("Failed to send mesh sync response to node %d: %v", msg.SrcNode, err)
		return err
	}

	m.logger.Infof("Successfully sent mesh sync response to node %d with corr_id=%d (non-blocking)", msg.SrcNode, msg.CorrId)
	return nil
}

// handleMeshSyncResponse handles mesh data responses from seed nodes
func (m *MeshCommunicationManager) handleMeshSyncResponse(ctx context.Context, msg *meshv1.Received) error {
	m.logger.Infof("Handling mesh sync response from node %d", msg.SrcNode)

	// Parse response message
	var respMsg CoreMessage
	if err := json.Unmarshal(msg.Payload, &respMsg); err != nil {
		m.logger.Errorf("Failed to unmarshal mesh sync response: %v", err)
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	// Get sync manager
	var syncManager *DatabaseSyncManager
	if m.eventManager != nil {
		syncManager = m.eventManager.GetSyncManager()
	}

	if syncManager == nil {
		m.logger.Warn("Database sync manager not available, cannot apply synced data")
		return fmt.Errorf("sync manager not available")
	}

	// Apply synced mesh data to local database
	err := syncManager.ApplySyncedMeshData(ctx, respMsg.Data)
	if err != nil {
		m.logger.Errorf("Failed to apply synced mesh data from node %d: %v", msg.SrcNode, err)
		return err
	}

	m.logger.Infof("Successfully applied mesh sync from node %d", msg.SrcNode)
	return nil
}

// broadcastJoinNotificationToMesh broadcasts a join notification to all nodes in the mesh
// Uses routing_id = 0 to reach all nodes through the mesh routing infrastructure
func (m *MeshCommunicationManager) broadcastJoinNotificationToMesh(ctx context.Context, joiningNodeID uint64, nodeData map[string]interface{}) error {
	m.logger.Infof("Broadcasting join notification for node %d to all mesh nodes", joiningNodeID)

	// Create a copy of nodeData and ensure node_id is properly formatted as string
	// This prevents precision loss when large int64 values go through JSON float64 conversion
	broadcastData := make(map[string]interface{})
	for k, v := range nodeData {
		broadcastData[k] = v
	}
	// Override node_id with string representation to preserve precision
	broadcastData["node_id"] = fmt.Sprintf("%d", joiningNodeID)

	// Create broadcast message using a different message type to distinguish from direct notifications
	broadcastMsg := &CoreMessage{
		Type:      MessageTypeNodeJoinBroadcast,
		Operation: "node_joined_broadcast",
		Data:      broadcastData,
		Timestamp: time.Now().Unix(),
	}

	// Serialize the message
	payload, err := json.Marshal(broadcastMsg)
	if err != nil {
		return fmt.Errorf("failed to serialize broadcast message: %w", err)
	}

	// Create broadcast request using routing_id = 0 to reach all nodes
	req := &meshv1.SendRequest{
		DstNode:    0, // routing_id = 0 for broadcast
		Payload:    payload,
		CorrId:     uint64(time.Now().UnixNano()),
		RequireAck: false,                                     // Broadcast doesn't require individual ACKs
		Mode:       meshv1.SendMode_SEND_MODE_FIRE_AND_FORGET, // Non-blocking
		QosClass:   2,                                         // High priority for topology updates
		Partition:  0,                                         // Control partition
	}

	// Add headers
	req.Headers = []*meshv1.Header{
		{Key: "message_type", Value: []byte(broadcastMsg.Type)},
		{Key: "operation", Value: []byte(broadcastMsg.Operation)},
		{Key: "source_node", Value: []byte(fmt.Sprintf("%d", m.nodeID))},
		{Key: "broadcast", Value: []byte("true")},
	}

	// Send broadcast
	resp, err := m.meshDataClient.Send(ctx, req)
	if err != nil {
		m.logger.Errorf("Failed to broadcast join notification: %v", err)
		return err
	}

	m.logger.Infof("Successfully broadcasted join notification for node %d (msg_id=%d)", joiningNodeID, resp.MsgId)
	return nil
}

// handleNodeJoinNotify handles direct notifications about joining nodes
func (m *MeshCommunicationManager) handleNodeJoinNotify(ctx context.Context, msg *meshv1.Received) error {
	m.logger.Infof("Handling node join notification from node %d (payload size: %d)", msg.SrcNode, len(msg.Payload))

	// Parse notification message
	var notifyMsg CoreMessage
	if err := json.Unmarshal(msg.Payload, &notifyMsg); err != nil {
		m.logger.Errorf("Failed to unmarshal node join notification: %v", err)
		return fmt.Errorf("failed to unmarshal notification: %w", err)
	}

	m.logger.Infof("Join notification parsed: type=%s, operation=%s, has %d data fields",
		notifyMsg.Type, notifyMsg.Operation, len(notifyMsg.Data))

	// Get sync manager
	var syncManager *DatabaseSyncManager
	if m.eventManager != nil {
		syncManager = m.eventManager.GetSyncManager()
	}

	if syncManager == nil {
		m.logger.Warn("Database sync manager not available, cannot add joining node")
		return fmt.Errorf("sync manager not available")
	}

	// Add joining node to local database
	err := syncManager.AddJoiningNode(ctx, msg.SrcNode, notifyMsg.Data)
	if err != nil {
		m.logger.Errorf("Failed to add joining node %d: %v", msg.SrcNode, err)
		return err
	}

	m.logger.Infof("Successfully processed join notification from node %d", msg.SrcNode)

	// Broadcast the join notification to all other nodes in the mesh asynchronously
	// This ensures global topology consistency without blocking the ACK response
	// Run in goroutine to avoid delaying the join handshake
	go func(nodeID uint64, data map[string]interface{}) {
		// Use a separate context with timeout for the broadcast
		broadcastCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := m.broadcastJoinNotificationToMesh(broadcastCtx, nodeID, data); err != nil {
			m.logger.Warnf("Failed to broadcast join notification to mesh: %v", err)
			// Don't fail the operation - the joining node was successfully added locally
		}
	}(msg.SrcNode, notifyMsg.Data)

	// Send acknowledgment response to complete the handshake
	// This allows the joining node to know the notification was processed successfully
	// Uses FIRE_AND_FORGET to avoid blocking the message handler goroutine
	ackResponse := &CoreMessage{
		Type:      "node_join_ack",
		Operation: "join_acknowledged",
		Data: map[string]interface{}{
			"success": true,
			"message": "Node join notification processed successfully",
		},
		Timestamp: time.Now().Unix(),
	}

	// Send response using the same correlation ID from the notification
	_, err = m.SendMessageWithCorrID(ctx, msg.SrcNode, ackResponse, msg.CorrId)
	if err != nil {
		m.logger.Errorf("Failed to send join acknowledgment to node %d: %v", msg.SrcNode, err)
		// Don't fail the whole operation if we can't send the ack response
		// The mesh layer ACK is sufficient for reliability
		return nil
	}

	m.logger.Infof("Sent join acknowledgment response to node %d with corr_id=%d (non-blocking)", msg.SrcNode, msg.CorrId)
	return nil
}

// handleNodeJoinBroadcast handles broadcast notifications about joining nodes
// This is separate from handleNodeJoinNotify to avoid interfering with direct join notifications
func (m *MeshCommunicationManager) handleNodeJoinBroadcast(ctx context.Context, msg *meshv1.Received) error {
	m.logger.Infof("Handling broadcast join notification from node %d (payload size: %d)", msg.SrcNode, len(msg.Payload))

	// Parse broadcast message
	var broadcastMsg CoreMessage
	if err := json.Unmarshal(msg.Payload, &broadcastMsg); err != nil {
		m.logger.Errorf("Failed to unmarshal broadcast join notification: %v", err)
		return fmt.Errorf("failed to unmarshal broadcast: %w", err)
	}

	m.logger.Infof("Broadcast join notification parsed: type=%s, operation=%s, has %d data fields",
		broadcastMsg.Type, broadcastMsg.Operation, len(broadcastMsg.Data))

	// Get sync manager
	var syncManager *DatabaseSyncManager
	if m.eventManager != nil {
		syncManager = m.eventManager.GetSyncManager()
	}

	if syncManager == nil {
		m.logger.Warn("Database sync manager not available, cannot add joining node from broadcast")
		return fmt.Errorf("sync manager not available")
	}

	// Extract the joining node ID from the broadcast data
	var joiningNodeID uint64
	if nodeIDData, ok := broadcastMsg.Data["node_id"]; ok {
		switch v := nodeIDData.(type) {
		case string:
			// Parse string representation (prevents precision loss from float64)
			parsed, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				m.logger.Errorf("Failed to parse node_id string '%s': %v", v, err)
				return fmt.Errorf("invalid node_id string in broadcast: %w", err)
			}
			joiningNodeID = parsed
		case float64:
			joiningNodeID = uint64(v)
		case int64:
			joiningNodeID = uint64(v)
		case uint64:
			joiningNodeID = v
		default:
			m.logger.Warnf("Unexpected type for node_id in broadcast: %T", nodeIDData)
			return fmt.Errorf("invalid node_id type in broadcast")
		}
	} else {
		m.logger.Warn("No node_id in broadcast data")
		return fmt.Errorf("missing node_id in broadcast")
	}

	m.logger.Infof("Processing broadcast for joining node %d (forwarded by node %d)", joiningNodeID, msg.SrcNode)

	// Check if we already know about this node (deduplication)
	// This prevents duplicate processing when receiving broadcast notifications
	if nodeExists, err := syncManager.NodeExists(ctx, joiningNodeID); err == nil && nodeExists {
		m.logger.Debugf("Node %d already exists locally, skipping broadcast notification (deduplication)", joiningNodeID)
		return nil
	}

	// Add joining node to local database
	err := syncManager.AddJoiningNode(ctx, joiningNodeID, broadcastMsg.Data)
	if err != nil {
		m.logger.Errorf("Failed to add joining node %d from broadcast: %v", joiningNodeID, err)
		return err
	}

	m.logger.Infof("Successfully processed broadcast join notification for node %d", joiningNodeID)

	// Note: Broadcasts do NOT trigger re-broadcasts (prevents loops)
	// Note: Broadcasts do NOT send ACK responses (not expected by broadcaster)

	return nil
}

// handleNodeJoinAck handles acknowledgment responses to join notifications
func (m *MeshCommunicationManager) handleNodeJoinAck(ctx context.Context, msg *meshv1.Received) error {
	m.logger.Infof("Received join acknowledgment from node %d", msg.SrcNode)

	// Parse acknowledgment message
	var ackMsg CoreMessage
	if err := json.Unmarshal(msg.Payload, &ackMsg); err != nil {
		m.logger.Errorf("Failed to unmarshal join ack: %v", err)
		return fmt.Errorf("failed to unmarshal ack: %w", err)
	}

	m.logger.Debugf("Join ack parsed: type=%s, operation=%s, data=%+v",
		ackMsg.Type, ackMsg.Operation, ackMsg.Data)

	// Check if the join was successful
	if success, ok := ackMsg.Data["success"].(bool); ok && success {
		m.logger.Infof("Join notification to node %d was successfully acknowledged", msg.SrcNode)
	} else {
		m.logger.Warnf("Join notification to node %d was acknowledged with failure", msg.SrcNode)
	}

	return nil
}

// === User-Level Data Synchronization Handlers ===

// handleUserDataSyncRequest handles requests for user-level data from joining nodes
func (m *MeshCommunicationManager) handleUserDataSyncRequest(ctx context.Context, msg *meshv1.Received) error {
	m.logger.Infof("=== ENTERING handleUserDataSyncRequest from node %d (payload size: %d bytes) ===", msg.SrcNode, len(msg.Payload))

	// Parse request message
	var reqMsg CoreMessage
	if err := json.Unmarshal(msg.Payload, &reqMsg); err != nil {
		m.logger.Errorf("Failed to unmarshal user data sync request: %v", err)
		return fmt.Errorf("failed to unmarshal request: %w", err)
	}

	m.logger.Debugf("Parsed user data sync request: type=%s, operation=%s", reqMsg.Type, reqMsg.Operation)

	// Get sync manager
	var syncManager *DatabaseSyncManager
	if m.eventManager != nil {
		syncManager = m.eventManager.GetSyncManager()
	}

	if syncManager == nil {
		m.logger.Warn("Database sync manager not available")
		return fmt.Errorf("sync manager not available")
	}

	// Gather user-level data
	userData, err := syncManager.GetUserDataForSync(ctx)
	if err != nil {
		m.logger.Errorf("Failed to get user-level data: %v", err)
		return err
	}

	// Send response
	respMsg := &CoreMessage{
		Type:      MessageTypeUserDataSyncResponse,
		Operation: "sync_response",
		Data: map[string]interface{}{
			"user_data": userData,
		},
		Timestamp: time.Now().Unix(),
	}

	respPayload, err := json.Marshal(respMsg)
	if err != nil {
		m.logger.Errorf("Failed to marshal user data sync response: %v", err)
		return err
	}

	m.logger.Infof("Sending user-level data sync response to node %d (payload size: %d bytes)", msg.SrcNode, len(respPayload))
	// Send response using the same correlation ID from the request for callback matching
	// Uses FIRE_AND_FORGET to avoid blocking the message handler goroutine
	_, err = m.SendMessageWithCorrID(ctx, msg.SrcNode, respMsg, msg.CorrId)
	if err != nil {
		m.logger.Errorf("Failed to send user data sync response: %v", err)
		return err
	}

	m.logger.Infof("Successfully sent user-level data to node %d with corr_id=%d (non-blocking)", msg.SrcNode, msg.CorrId)
	return nil
}

// handleUserDataSyncResponse handles user-level data responses from seed nodes
func (m *MeshCommunicationManager) handleUserDataSyncResponse(ctx context.Context, msg *meshv1.Received) error {
	m.logger.Infof("Handling user-level data sync response from node %d", msg.SrcNode)

	// Parse response message
	var respMsg CoreMessage
	if err := json.Unmarshal(msg.Payload, &respMsg); err != nil {
		m.logger.Errorf("Failed to unmarshal user data sync response: %v", err)
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	// Get sync manager
	var syncManager *DatabaseSyncManager
	if m.eventManager != nil {
		syncManager = m.eventManager.GetSyncManager()
	}

	if syncManager == nil {
		m.logger.Warn("Database sync manager not available")
		return fmt.Errorf("sync manager not available")
	}

	// Extract user data
	userDataInterface, ok := respMsg.Data["user_data"]
	if !ok {
		m.logger.Warn("No user_data in response")
		return fmt.Errorf("no user_data in response")
	}

	userData, ok := userDataInterface.(map[string]interface{})
	if !ok {
		m.logger.Errorf("Invalid user_data format (type: %T)", userDataInterface)
		return fmt.Errorf("invalid user_data format")
	}

	// Apply user-level data
	err := syncManager.ApplyUserDataSync(ctx, userData)
	if err != nil {
		m.logger.Errorf("Failed to apply user-level data: %v", err)
		return err
	}

	m.logger.Infof("Successfully applied user-level data from node %d", msg.SrcNode)
	return nil
}
