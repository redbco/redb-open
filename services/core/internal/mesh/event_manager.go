package mesh

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// MeshEventManager handles mesh state synchronization events
type MeshEventManager struct {
	db               *database.PostgreSQL
	meshManager      *MeshCommunicationManager
	logger           *logger.Logger
	nodeID           uint64
	eventSequence    uint64
	consensusChecker *ConsensusChecker
	syncManager      *DatabaseSyncManager

	// Event processing
	eventQueue      chan *MeshEvent
	processingQueue chan *MeshEvent
	shutdown        chan struct{}
	wg              sync.WaitGroup

	// State
	mu        sync.RWMutex
	isRunning bool
}

// MeshEvent represents a mesh state event
type MeshEvent struct {
	Type           corev1.MeshEventType
	OriginatorNode uint64
	AffectedNode   uint64
	Sequence       uint64
	Timestamp      time.Time
	Metadata       map[string]string
	Payload        []byte
}

// NewMeshEventManager creates a new mesh event manager
func NewMeshEventManager(
	db *database.PostgreSQL,
	meshManager *MeshCommunicationManager,
	logger *logger.Logger,
	nodeID uint64,
) *MeshEventManager {
	return &MeshEventManager{
		db:              db,
		meshManager:     meshManager,
		logger:          logger,
		nodeID:          nodeID,
		eventQueue:      make(chan *MeshEvent, 1000),
		processingQueue: make(chan *MeshEvent, 1000),
		shutdown:        make(chan struct{}),
	}
}

// SetConsensusChecker sets the consensus checker (circular dependency resolution)
func (m *MeshEventManager) SetConsensusChecker(checker *ConsensusChecker) {
	m.consensusChecker = checker
}

// SetSyncManager sets the database sync manager (circular dependency resolution)
func (m *MeshEventManager) SetSyncManager(syncManager *DatabaseSyncManager) {
	m.syncManager = syncManager
}

// Start starts the event manager
func (m *MeshEventManager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isRunning {
		return fmt.Errorf("event manager is already running")
	}

	m.isRunning = true

	// Start event processing goroutines
	m.wg.Add(3)
	go m.eventPublisher(ctx)
	go m.eventProcessor(ctx)
	go m.periodicTasks(ctx)

	m.logger.Infof("Mesh event manager started for node %d", m.nodeID)
	return nil
}

// Stop stops the event manager
func (m *MeshEventManager) Stop() error {
	m.logger.Debug("Stopping event manager")

	m.mu.Lock()
	defer m.mu.Unlock()

	m.logger.Debug("Event manager acquired lock")

	if !m.isRunning {
		m.logger.Debug("Event manager not running, returning")
		return nil
	}

	m.logger.Debug("Setting event manager to not running")
	m.isRunning = false

	m.logger.Debug("Closing shutdown channel")
	close(m.shutdown)

	m.logger.Debug("Waiting for goroutines to finish")

	// Use a timeout to prevent hanging indefinitely
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		m.logger.Debug("All goroutines finished")
	case <-time.After(3 * time.Second):
		m.logger.Warnf("Goroutines did not finish within timeout, forcing shutdown")
	}

	m.logger.Info("Mesh event manager stopped")
	return nil
}

// PublishEvent publishes a mesh state event
func (m *MeshEventManager) PublishEvent(ctx context.Context, event *MeshEvent) error {
	if !m.isRunning {
		return fmt.Errorf("event manager is not running")
	}

	// Set sequence number and timestamp
	event.Sequence = atomic.AddUint64(&m.eventSequence, 1)
	event.Timestamp = time.Now()
	event.OriginatorNode = m.nodeID

	// Store event in database
	if err := m.storeEvent(ctx, event); err != nil {
		m.logger.Errorf("Failed to store event in database: %v", err)
		return fmt.Errorf("failed to store event: %w", err)
	}

	// Queue for broadcasting
	select {
	case m.eventQueue <- event:
		m.logger.Debugf("Queued event %s (seq: %d) for broadcasting", event.Type, event.Sequence)
		return nil
	default:
		return fmt.Errorf("event queue is full")
	}
}

// HandleReceivedEvent handles an event received from another node
func (m *MeshEventManager) HandleReceivedEvent(ctx context.Context, event *MeshEvent, sourceNode uint64) error {
	m.logger.Infof("Received event %s from node %d (originator: %d, seq: %d)",
		event.Type, sourceNode, event.OriginatorNode, event.Sequence)

	// Check if we've already processed this event
	exists, err := m.eventExists(ctx, event.OriginatorNode, event.Sequence)
	if err != nil {
		return fmt.Errorf("failed to check event existence: %w", err)
	}

	if exists {
		m.logger.Debugf("Event %d from node %d already processed, skipping", event.Sequence, event.OriginatorNode)
		return nil
	}

	// Store the received event
	if err := m.storeEvent(ctx, event); err != nil {
		return fmt.Errorf("failed to store received event: %w", err)
	}

	// Queue for processing
	select {
	case m.processingQueue <- event:
		m.logger.Debugf("Queued received event %s (seq: %d) for processing", event.Type, event.Sequence)
		return nil
	default:
		return fmt.Errorf("processing queue is full")
	}
}

// eventPublisher handles broadcasting events to other nodes
func (m *MeshEventManager) eventPublisher(ctx context.Context) {
	defer m.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.shutdown:
			return
		case event := <-m.eventQueue:
			if err := m.broadcastEvent(ctx, event); err != nil {
				m.logger.Errorf("Failed to broadcast event %s (seq: %d): %v", event.Type, event.Sequence, err)
			}
		}
	}
}

// eventProcessor handles processing received events
func (m *MeshEventManager) eventProcessor(ctx context.Context) {
	defer m.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.shutdown:
			return
		case event := <-m.processingQueue:
			if err := m.processEvent(ctx, event); err != nil {
				m.logger.Errorf("Failed to process event %s (seq: %d): %v", event.Type, event.Sequence, err)
			}
		}
	}
}

// periodicTasks handles periodic maintenance tasks
func (m *MeshEventManager) periodicTasks(ctx context.Context) {
	defer m.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.shutdown:
			return
		case <-ticker.C:
			// Process any unprocessed events
			if err := m.processUnprocessedEvents(ctx); err != nil {
				m.logger.Errorf("Failed to process unprocessed events: %v", err)
			}

			// Trigger consensus check if we have a consensus checker
			if m.consensusChecker != nil {
				if err := m.consensusChecker.PeriodicCheck(ctx); err != nil {
					m.logger.Errorf("Consensus check failed: %v", err)
				}
			}
		}
	}
}

// broadcastEvent broadcasts an event to all nodes via mesh service
func (m *MeshEventManager) broadcastEvent(ctx context.Context, event *MeshEvent) error {
	// Convert to mesh protobuf format
	meshEvent := &meshv1.MeshStateEvent{
		EventType:      meshv1.MeshEventType(event.Type),
		OriginatorNode: event.OriginatorNode,
		AffectedNode:   event.AffectedNode,
		SequenceNumber: event.Sequence,
		Timestamp:      uint64(event.Timestamp.Unix()),
		Metadata:       event.Metadata,
		Payload:        event.Payload,
	}

	// Call mesh service to broadcast with timeout
	broadcastCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := m.meshManager.BroadcastStateEvent(broadcastCtx, meshEvent); err != nil {
		// Don't return error during shutdown to prevent hanging
		select {
		case <-m.shutdown:
			m.logger.Debugf("Ignoring broadcast error during shutdown: %v", err)
			return nil
		default:
			return fmt.Errorf("failed to broadcast via mesh service: %w", err)
		}
	}

	m.logger.Infof("Broadcasted event %s (seq: %d) to mesh", event.Type, event.Sequence)
	return nil
}

// processEvent processes a received event and updates local state
func (m *MeshEventManager) processEvent(ctx context.Context, event *MeshEvent) error {
	m.logger.Infof("Processing event %s from node %d (seq: %d)", event.Type, event.OriginatorNode, event.Sequence)

	switch event.Type {
	case corev1.MeshEventType_MESH_EVENT_NODE_JOINED:
		return m.handleNodeJoined(ctx, event)
	case corev1.MeshEventType_MESH_EVENT_NODE_LEFT:
		return m.handleNodeLeft(ctx, event)
	case corev1.MeshEventType_MESH_EVENT_NODE_EVICTED:
		return m.handleNodeEvicted(ctx, event)
	case corev1.MeshEventType_MESH_EVENT_SESSION_ADDED:
		return m.handleSessionAdded(ctx, event)
	case corev1.MeshEventType_MESH_EVENT_SESSION_REMOVED:
		return m.handleSessionRemoved(ctx, event)
	case corev1.MeshEventType_MESH_EVENT_NODE_SHUTDOWN:
		return m.handleNodeShutdown(ctx, event)
	case corev1.MeshEventType_MESH_EVENT_NODE_STARTED:
		return m.handleNodeStarted(ctx, event)
	case corev1.MeshEventType_MESH_EVENT_SESSION_INTERRUPTED:
		return m.handleSessionInterrupted(ctx, event)
	case corev1.MeshEventType_MESH_EVENT_SESSION_RECOVERED:
		return m.handleSessionRecovered(ctx, event)
	case corev1.MeshEventType_MESH_EVENT_NODE_OFFLINE:
		return m.handleNodeOffline(ctx, event)
	case corev1.MeshEventType_MESH_EVENT_NODE_RECOVERED:
		return m.handleNodeRecovered(ctx, event)
	case corev1.MeshEventType_MESH_EVENT_SPLIT_DETECTED:
		return m.handleSplitDetected(ctx, event)
	case corev1.MeshEventType_MESH_EVENT_SPLIT_RESOLVED:
		return m.handleSplitResolved(ctx, event)
	default:
		m.logger.Warnf("Unknown event type: %s", event.Type)
		return nil
	}
}

// Database operations
func (m *MeshEventManager) storeEvent(ctx context.Context, event *MeshEvent) error {
	eventData, err := json.Marshal(map[string]interface{}{
		"metadata": event.Metadata,
		"payload":  event.Payload,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	// Check if originator_node exists in our local database
	if event.OriginatorNode != 0 {
		var exists bool
		checkQuery := `SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id = $1)`
		if err := m.db.Pool().QueryRow(ctx, checkQuery, event.OriginatorNode).Scan(&exists); err != nil {
			return fmt.Errorf("failed to check if originator node exists: %w", err)
		}
		if !exists {
			// Node doesn't exist locally yet - skip storing this event
			m.logger.Debugf("Skipping storage of event from unknown originator node %d - not in local database yet", event.OriginatorNode)
			return nil
		}
	}

	// Check if affected_node exists in our local database (if specified)
	if event.AffectedNode != 0 {
		var exists bool
		checkQuery := `SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id = $1)`
		if err := m.db.Pool().QueryRow(ctx, checkQuery, event.AffectedNode).Scan(&exists); err != nil {
			return fmt.Errorf("failed to check if affected node exists: %w", err)
		}
		if !exists {
			// Node doesn't exist locally yet - skip storing this event
			m.logger.Debugf("Skipping storage of event for unknown affected node %d - not in local database yet", event.AffectedNode)
			return nil
		}
	}

	query := `
		INSERT INTO mesh_event_log (event_type, originator_node, affected_node, sequence_number, event_data)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (originator_node, sequence_number) DO NOTHING
	`

	_, err = m.db.Pool().Exec(ctx, query,
		event.Type.String(),
		event.OriginatorNode,
		event.AffectedNode,
		event.Sequence,
		eventData,
	)

	return err
}

func (m *MeshEventManager) eventExists(ctx context.Context, originatorNode, sequence uint64) (bool, error) {
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM mesh_event_log WHERE originator_node = $1 AND sequence_number = $2)`

	err := m.db.Pool().QueryRow(ctx, query, originatorNode, sequence).Scan(&exists)
	return exists, err
}

func (m *MeshEventManager) processUnprocessedEvents(ctx context.Context) error {
	query := `
		SELECT event_type, originator_node, affected_node, sequence_number, event_data
		FROM mesh_event_log 
		WHERE processed = FALSE 
		ORDER BY created ASC 
		LIMIT 100
	`

	rows, err := m.db.Pool().Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var processedCount int
	for rows.Next() {
		var eventTypeStr string
		var originatorNode, affectedNode int64
		var sequence uint64
		var eventDataJSON []byte

		if err := rows.Scan(&eventTypeStr, &originatorNode, &affectedNode, &sequence, &eventDataJSON); err != nil {
			m.logger.Errorf("Failed to scan event row: %v", err)
			continue
		}

		// Parse event data
		var eventData map[string]interface{}
		if err := json.Unmarshal(eventDataJSON, &eventData); err != nil {
			m.logger.Errorf("Failed to unmarshal event data: %v", err)
			continue
		}

		// Convert to MeshEvent
		event := &MeshEvent{
			Type:           m.parseEventType(eventTypeStr),
			OriginatorNode: uint64(originatorNode),
			AffectedNode:   uint64(affectedNode),
			Sequence:       sequence,
		}

		if metadata, ok := eventData["metadata"].(map[string]interface{}); ok {
			event.Metadata = make(map[string]string)
			for k, v := range metadata {
				if str, ok := v.(string); ok {
					event.Metadata[k] = str
				}
			}
		}

		if payload, ok := eventData["payload"].([]byte); ok {
			event.Payload = payload
		}

		// Process the event
		if err := m.processEvent(ctx, event); err != nil {
			m.logger.Errorf("Failed to process unprocessed event: %v", err)
			continue
		}

		// Mark as processed
		if err := m.markEventProcessed(ctx, event.OriginatorNode, event.Sequence); err != nil {
			m.logger.Errorf("Failed to mark event as processed: %v", err)
		}

		processedCount++
	}

	if processedCount > 0 {
		m.logger.Infof("Processed %d unprocessed events", processedCount)
	}

	return rows.Err()
}

func (m *MeshEventManager) markEventProcessed(ctx context.Context, originatorNode, sequence uint64) error {
	query := `UPDATE mesh_event_log SET processed = TRUE WHERE originator_node = $1 AND sequence_number = $2`
	_, err := m.db.Pool().Exec(ctx, query, int64(originatorNode), sequence)
	return err
}

// Helper functions
func (m *MeshEventManager) parseEventType(eventTypeStr string) corev1.MeshEventType {
	switch eventTypeStr {
	case "MESH_EVENT_NODE_JOINED":
		return corev1.MeshEventType_MESH_EVENT_NODE_JOINED
	case "MESH_EVENT_NODE_LEFT":
		return corev1.MeshEventType_MESH_EVENT_NODE_LEFT
	case "MESH_EVENT_NODE_EVICTED":
		return corev1.MeshEventType_MESH_EVENT_NODE_EVICTED
	case "MESH_EVENT_SESSION_ADDED":
		return corev1.MeshEventType_MESH_EVENT_SESSION_ADDED
	case "MESH_EVENT_SESSION_REMOVED":
		return corev1.MeshEventType_MESH_EVENT_SESSION_REMOVED
	case "MESH_EVENT_NODE_SHUTDOWN":
		return corev1.MeshEventType_MESH_EVENT_NODE_SHUTDOWN
	case "MESH_EVENT_NODE_STARTED":
		return corev1.MeshEventType_MESH_EVENT_NODE_STARTED
	case "MESH_EVENT_SESSION_INTERRUPTED":
		return corev1.MeshEventType_MESH_EVENT_SESSION_INTERRUPTED
	case "MESH_EVENT_SESSION_RECOVERED":
		return corev1.MeshEventType_MESH_EVENT_SESSION_RECOVERED
	case "MESH_EVENT_NODE_OFFLINE":
		return corev1.MeshEventType_MESH_EVENT_NODE_OFFLINE
	case "MESH_EVENT_NODE_RECOVERED":
		return corev1.MeshEventType_MESH_EVENT_NODE_RECOVERED
	case "MESH_EVENT_SPLIT_DETECTED":
		return corev1.MeshEventType_MESH_EVENT_SPLIT_DETECTED
	case "MESH_EVENT_SPLIT_RESOLVED":
		return corev1.MeshEventType_MESH_EVENT_SPLIT_RESOLVED
	default:
		return corev1.MeshEventType_MESH_EVENT_UNSPECIFIED
	}
}

// Event handlers (to be implemented in next steps)
func (m *MeshEventManager) handleNodeJoined(ctx context.Context, event *MeshEvent) error {
	m.logger.Infof("Handling node joined event for node %d", event.AffectedNode)
	// TODO: Update local database with new node information
	return nil
}

func (m *MeshEventManager) handleNodeLeft(ctx context.Context, event *MeshEvent) error {
	m.logger.Infof("Handling node left event for node %d", event.AffectedNode)
	// TODO: Update local database to reflect node departure
	return nil
}

func (m *MeshEventManager) handleNodeEvicted(ctx context.Context, event *MeshEvent) error {
	m.logger.Infof("Handling node evicted event for node %d", event.AffectedNode)
	// TODO: Remove node from local database
	return nil
}

func (m *MeshEventManager) handleSessionAdded(ctx context.Context, event *MeshEvent) error {
	m.logger.Infof("Handling session added event")
	// TODO: Update routing/topology information
	return nil
}

func (m *MeshEventManager) handleSessionRemoved(ctx context.Context, event *MeshEvent) error {
	m.logger.Infof("Handling session removed event")
	// TODO: Update routing/topology information
	return nil
}

func (m *MeshEventManager) handleNodeShutdown(ctx context.Context, event *MeshEvent) error {
	m.logger.Infof("Handling node shutdown event for node %d", event.AffectedNode)
	// TODO: Mark node as offline in database
	return nil
}

func (m *MeshEventManager) handleNodeStarted(ctx context.Context, event *MeshEvent) error {
	m.logger.Infof("Handling node started event for node %d", event.AffectedNode)
	// TODO: Mark node as online in database
	return nil
}

func (m *MeshEventManager) handleSessionInterrupted(ctx context.Context, event *MeshEvent) error {
	m.logger.Infof("Handling session interrupted event")
	// TODO: Update connection status
	return nil
}

func (m *MeshEventManager) handleSessionRecovered(ctx context.Context, event *MeshEvent) error {
	m.logger.Infof("Handling session recovered event")
	// TODO: Update connection status
	return nil
}

func (m *MeshEventManager) handleNodeOffline(ctx context.Context, event *MeshEvent) error {
	m.logger.Infof("Handling node offline event for node %d", event.AffectedNode)
	// TODO: Mark node as offline, trigger consensus check
	if m.consensusChecker != nil {
		return m.consensusChecker.CheckSplitBrain(ctx)
	}
	return nil
}

func (m *MeshEventManager) handleNodeRecovered(ctx context.Context, event *MeshEvent) error {
	m.logger.Infof("Handling node recovered event for node %d", event.AffectedNode)
	// TODO: Mark node as online, trigger database sync
	if m.syncManager != nil {
		return m.syncManager.SyncWithNode(ctx, event.AffectedNode)
	}
	return nil
}

func (m *MeshEventManager) handleSplitDetected(ctx context.Context, event *MeshEvent) error {
	m.logger.Warnf("Handling split-brain detected event")
	// TODO: Enter split-brain mode, disable writes if in minority
	if m.consensusChecker != nil {
		return m.consensusChecker.HandleSplitBrain(ctx)
	}
	return nil
}

func (m *MeshEventManager) handleSplitResolved(ctx context.Context, event *MeshEvent) error {
	m.logger.Infof("Handling split-brain resolved event")
	// TODO: Exit split-brain mode, re-enable writes, sync databases
	if m.syncManager != nil {
		return m.syncManager.FullSync(ctx)
	}
	return nil
}

// GetSyncManager returns the database sync manager
func (m *MeshEventManager) GetSyncManager() *DatabaseSyncManager {
	return m.syncManager
}
