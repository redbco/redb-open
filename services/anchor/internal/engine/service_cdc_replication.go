package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// CDCReplicationManager manages active CDC replication streams (database-agnostic version)
type CDCReplicationManager struct {
	mu                 sync.RWMutex
	activeReplications map[string]*CDCReplicationStream
}

// CDCReplicationStream represents an active CDC replication stream (database-agnostic version)
type CDCReplicationStream struct {
	ReplicationSourceID string
	RelationshipID      string
	SourceDatabaseID    string
	TargetDatabaseID    string
	TableNames          []string
	MappingRules        []byte
	EventRouter         *CDCEventRouter
	ReplicationSource   adapter.ReplicationSource
	StopChan            chan struct{}
	Status              string
	EventsProcessed     int64
	LastEventTimestamp  time.Time
	mu                  sync.RWMutex
}

var (
	cdcManager *CDCReplicationManager
	cdcOnce    sync.Once
)

// getCDCManager returns the singleton CDC replication manager
func getCDCManager() *CDCReplicationManager {
	cdcOnce.Do(func() {
		cdcManager = &CDCReplicationManager{
			activeReplications: make(map[string]*CDCReplicationStream),
		}
	})
	return cdcManager
}

// StartCDCReplication starts CDC replication for a relationship (database-agnostic version)
func (e *Engine) StartCDCReplication(ctx context.Context, req *anchorv1.StartCDCReplicationRequest) (*anchorv1.StartCDCReplicationResponse, error) {
	e.logger.Info("Starting CDC replication for relationship %s", req.RelationshipId)

	registry := e.GetState().GetConnectionRegistry()
	if registry == nil {
		return nil, status.Errorf(codes.Internal, "connection registry not available")
	}

	// Step 1: Get source and target adapter connections
	sourceConn, err := registry.GetAdapterConnection(req.SourceDatabaseId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "source database not found: %v", err)
	}

	targetConn, err := registry.GetAdapterConnection(req.TargetDatabaseId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "target database not found: %v", err)
	}

	// Step 2: Check if source database supports CDC
	sourceRepOps := sourceConn.ReplicationOperations()
	if !sourceRepOps.IsSupported() {
		return nil, status.Errorf(codes.InvalidArgument,
			"source database type %s does not support CDC replication",
			sourceConn.Type())
	}

	// Step 3: Check if target database can receive CDC events
	targetRepOps := targetConn.ReplicationOperations()
	if !targetRepOps.IsSupported() {
		return nil, status.Errorf(codes.InvalidArgument,
			"target database type %s does not support CDC replication",
			targetConn.Type())
	}

	e.logger.Info("CDC support verified: source=%s, target=%s",
		sourceConn.Type(), targetConn.Type())

	// Step 4: Create CDC event router for transforming and routing events
	// Get transformation service endpoint for custom transformations
	transformationServiceEndpoint := e.getServiceAddress("transformation")
	eventRouter, err := NewCDCEventRouter(sourceConn, targetConn, req.MappingRules, transformationServiceEndpoint, e.logger)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create event router: %v", err)
	}

	// Step 5: Build replication configuration
	replicationConfig := adapter.ReplicationConfig{
		ReplicationID:   req.ReplicationSourceId,
		DatabaseID:      req.SourceDatabaseId,
		WorkspaceID:     req.WorkspaceId,
		TenantID:        req.TenantId,
		ReplicationName: fmt.Sprintf("replication_%s", req.RelationshipId),
		ConnectionType:  string(sourceConn.Type()),
		DatabaseVendor:  string(sourceConn.Type()),
		Host:            sourceConn.Config().Host,
		Port:            sourceConn.Config().Port,
		Username:        sourceConn.Config().Username,
		Password:        sourceConn.Config().Password,
		DatabaseName:    sourceConn.Config().DatabaseName,
		SSL:             sourceConn.Config().SSL,
		SSLMode:         sourceConn.Config().SSLMode,
		SSLCert:         getStringValue(sourceConn.Config().SSLCert),
		SSLKey:          getStringValue(sourceConn.Config().SSLKey),
		SSLRootCert:     getStringValue(sourceConn.Config().SSLRootCert),
		TableNames:      req.TableNames,
		EventHandler:    wrapEventHandler(eventRouter.CreateEventHandler()),
	}

	// Step 6: Extract database-specific parameters from node_id if provided
	if req.NodeId != nil && *req.NodeId != "" {
		e.parseReplicationParameters(req.NodeId, &replicationConfig)
	}

	// Step 7: Set default database-specific parameters if not provided
	e.setDefaultReplicationParameters(&replicationConfig, req.RelationshipId)

	// Step 7.5: Load saved replication position for resume (if available)
	if savedPosition, savedEvents, err := e.loadCDCStreamState(ctx, req.ReplicationSourceId); err == nil {
		if savedPosition != "" {
			e.logger.Infof("Resuming CDC replication from saved position: %s (events processed: %d)", savedPosition, savedEvents)
			replicationConfig.StartPosition = savedPosition
		}
	} else {
		// If loading fails, log warning but continue (will start from beginning)
		e.logger.Warnf("Could not load saved CDC position for %s, starting from beginning: %v", req.ReplicationSourceId, err)
	}

	// Step 8: Connect replication using source adapter
	replicationSource, err := sourceRepOps.Connect(ctx, replicationConfig)
	if err != nil {
		e.logger.Errorf("Failed to connect replication: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to connect replication: %v", err)
	}

	// Step 8.5: Set up checkpoint function for periodic position saving
	// This is implemented differently per database type
	// For PostgreSQL, we need to access the underlying PostgresReplicationSourceDetails
	// For MySQL, we need to access the underlying MySQLReplicationSourceDetails
	// The checkpoint function will be called automatically by the replication source
	checkpointFunc := e.createCheckpointFunc(req.ReplicationSourceId)

	// Try to set checkpoint function on the replication source
	// This is done via a type assertion since different databases have different implementations
	if pgSource, ok := replicationSource.(interface {
		SetCheckpointFunc(func(context.Context, string) error)
	}); ok {
		pgSource.SetCheckpointFunc(checkpointFunc)
		e.logger.Infof("Checkpoint function configured for replication source %s", req.ReplicationSourceId)
	} else {
		e.logger.Warnf("Could not set checkpoint function for replication source %s (not supported by this database type)", req.ReplicationSourceId)
	}

	// Step 9: Start the replication stream
	if err := replicationSource.Start(); err != nil {
		e.logger.Errorf("Failed to start replication stream: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to start replication stream: %v", err)
	}

	e.logger.Infof("Replication stream started successfully for relationship %s", req.RelationshipId)

	// Step 10: Store CDC details
	cdcDetails := map[string]string{
		"source_database_id": req.SourceDatabaseId,
		"target_database_id": req.TargetDatabaseId,
		"relationship_id":    req.RelationshipId,
		"started_at":         time.Now().Format(time.RFC3339),
		"table_names":        fmt.Sprintf("%v", req.TableNames),
		"source_type":        string(sourceConn.Type()),
		"target_type":        string(targetConn.Type()),
	}

	// Add database-specific metadata
	if metadata := replicationSource.GetMetadata(); metadata != nil {
		for k, v := range metadata {
			cdcDetails[k] = fmt.Sprintf("%v", v)
		}
	}

	// Step 11: Add to CDC manager for tracking
	manager := getCDCManager()
	stream := &CDCReplicationStream{
		ReplicationSourceID: req.ReplicationSourceId,
		RelationshipID:      req.RelationshipId,
		SourceDatabaseID:    req.SourceDatabaseId,
		TargetDatabaseID:    req.TargetDatabaseId,
		TableNames:          req.TableNames,
		MappingRules:        req.MappingRules,
		EventRouter:         eventRouter,
		ReplicationSource:   replicationSource,
		StopChan:            make(chan struct{}),
		Status:              "active",
		EventsProcessed:     0,
		LastEventTimestamp:  time.Now(),
	}

	manager.mu.Lock()
	manager.activeReplications[req.ReplicationSourceId] = stream
	manager.mu.Unlock()

	e.logger.Info("CDC replication started successfully for relationship %s (source: %s -> target: %s)",
		req.RelationshipId, sourceConn.Type(), targetConn.Type())

	return &anchorv1.StartCDCReplicationResponse{
		Message:             "CDC replication started successfully",
		Success:             true,
		Status:              commonv1.Status_STATUS_SUCCESS,
		ReplicationSourceId: req.ReplicationSourceId,
		CdcConnectionId:     req.ReplicationSourceId,
		CdcDetails:          cdcDetails,
	}, nil
}

// StopCDCReplication stops CDC replication (database-agnostic version)
func (e *Engine) StopCDCReplication(ctx context.Context, req *anchorv1.StopCDCReplicationRequest) (*anchorv1.StopCDCReplicationResponse, error) {
	e.logger.Info("Stopping CDC replication for source %s", req.ReplicationSourceId)

	manager := getCDCManager()
	manager.mu.Lock()
	stream, exists := manager.activeReplications[req.ReplicationSourceId]
	if exists {
		delete(manager.activeReplications, req.ReplicationSourceId)
	}
	manager.mu.Unlock()

	if !exists {
		return &anchorv1.StopCDCReplicationResponse{
			Message:             "CDC replication not found or already stopped",
			Success:             true,
			Status:              commonv1.Status_STATUS_SUCCESS,
			ReplicationSourceId: req.ReplicationSourceId,
		}, nil
	}

	// Stop the replication source
	if stream.ReplicationSource != nil {
		if err := stream.ReplicationSource.Stop(); err != nil {
			e.logger.Warnf("Error stopping replication source: %v", err)
		}
	}

	// Signal stop
	close(stream.StopChan)

	// Preserve state if requested
	preservedState := make(map[string]string)
	if req.PreserveState != nil && *req.PreserveState {
		stream.mu.RLock()
		preservedState["status"] = stream.Status
		preservedState["events_processed"] = fmt.Sprintf("%d", stream.EventsProcessed)
		preservedState["last_event_timestamp"] = stream.LastEventTimestamp.Format(time.RFC3339)

		// Add statistics from event router
		if stream.EventRouter != nil {
			stats := stream.EventRouter.GetStatistics()
			preservedState["events_failed"] = fmt.Sprintf("%d", stats.EventsFailed)
			preservedState["average_latency"] = stats.AverageLatency.String()
		}
		stream.mu.RUnlock()
	}

	e.logger.Info("CDC replication stopped for source %s", req.ReplicationSourceId)

	return &anchorv1.StopCDCReplicationResponse{
		Message:             "CDC replication stopped successfully",
		Success:             true,
		Status:              commonv1.Status_STATUS_SUCCESS,
		ReplicationSourceId: req.ReplicationSourceId,
		PreservedState:      preservedState,
	}, nil
}

// ResumeCDCReplication resumes a stopped CDC replication (database-agnostic version)
func (e *Engine) ResumeCDCReplication(ctx context.Context, req *anchorv1.ResumeCDCReplicationRequest) (*anchorv1.ResumeCDCReplicationResponse, error) {
	e.logger.Info("Resuming CDC replication for source %s", req.ReplicationSourceId)

	manager := getCDCManager()
	manager.mu.RLock()
	stream, exists := manager.activeReplications[req.ReplicationSourceId]
	manager.mu.RUnlock()

	if exists {
		// Already active - try to start if it's not running
		if stream.ReplicationSource != nil {
			if err := stream.ReplicationSource.Start(); err != nil {
				e.logger.Warnf("Replication source may already be running: %v", err)
			}
		}

		return &anchorv1.ResumeCDCReplicationResponse{
			Message:             "CDC replication is active",
			Success:             true,
			Status:              commonv1.Status_STATUS_SUCCESS,
			ReplicationSourceId: req.ReplicationSourceId,
			CdcConnectionId:     req.ReplicationSourceId,
		}, nil
	}

	// For full resume implementation, would need to:
	// 1. Fetch relationship details from database
	// 2. Restore preserved state (LSN, position, etc.)
	// 3. Call StartCDCReplication with restored configuration

	e.logger.Info("CDC replication resume requested for source %s - would need to restore from saved state", req.ReplicationSourceId)

	return &anchorv1.ResumeCDCReplicationResponse{
		Message:             "CDC replication resume not yet fully implemented",
		Success:             false,
		Status:              commonv1.Status_STATUS_ERROR,
		ReplicationSourceId: req.ReplicationSourceId,
	}, nil
}

// GetCDCReplicationStatus gets the status of a CDC replication (database-agnostic version)
func (e *Engine) GetCDCReplicationStatus(ctx context.Context, req *anchorv1.GetCDCReplicationStatusRequest) (*anchorv1.GetCDCReplicationStatusResponse, error) {
	manager := getCDCManager()
	manager.mu.RLock()
	stream, exists := manager.activeReplications[req.ReplicationSourceId]
	manager.mu.RUnlock()

	if !exists {
		return &anchorv1.GetCDCReplicationStatusResponse{
			Message:             "CDC replication not found",
			Success:             false,
			Status:              commonv1.Status_STATUS_ERROR,
			ReplicationSourceId: req.ReplicationSourceId,
			CdcStatus:           "stopped",
		}, nil
	}

	stream.mu.RLock()
	defer stream.mu.RUnlock()

	// Get status from replication source
	cdcStatus := "active"
	if stream.ReplicationSource != nil {
		if !stream.ReplicationSource.IsActive() {
			cdcStatus = "inactive"
		}
	}

	// Get statistics from event router
	var eventsProcessed int64
	var eventsFailed int64
	cdcPosition := make(map[string]string)

	if stream.EventRouter != nil {
		stats := stream.EventRouter.GetStatistics()
		eventsProcessed = stats.EventsProcessed
		eventsFailed = stats.EventsFailed
		cdcPosition["last_event_timestamp"] = stats.LastEventTimestamp.Format(time.RFC3339)
		cdcPosition["last_event_lsn"] = stats.LastEventLSN
		cdcPosition["average_latency"] = stats.AverageLatency.String()
	}

	// Add metadata from replication source
	if stream.ReplicationSource != nil {
		if metadata := stream.ReplicationSource.GetStatus(); metadata != nil {
			for k, v := range metadata {
				cdcPosition[k] = fmt.Sprintf("%v", v)
			}
		}
	}

	return &anchorv1.GetCDCReplicationStatusResponse{
		Message:             "CDC replication status retrieved",
		Success:             true,
		Status:              commonv1.Status_STATUS_SUCCESS,
		ReplicationSourceId: req.ReplicationSourceId,
		CdcStatus:           cdcStatus,
		EventsProcessed:     eventsProcessed,
		EventsPending:       eventsFailed, // Use failed events as pending for now
		LastEventTimestamp:  stream.LastEventTimestamp.Format(time.RFC3339),
		CdcPosition:         cdcPosition,
	}, nil
}

// StreamCDCEvents streams CDC events (for monitoring/debugging)
func (e *Engine) StreamCDCEvents(req *anchorv1.StreamCDCEventsRequest, stream anchorv1.AnchorService_StreamCDCEventsServer) error {
	// This would stream CDC events for monitoring purposes
	// For now, return a simple message
	return stream.Send(&anchorv1.StreamCDCEventsResponse{
		Message:             "CDC event streaming not yet implemented",
		Success:             false,
		Status:              commonv1.Status_STATUS_ERROR,
		ReplicationSourceId: req.ReplicationSourceId,
	})
}

// Helper functions

// getStringValue safely extracts string value from pointer
func getStringValue(ptr *string) string {
	if ptr != nil {
		return *ptr
	}
	return ""
}

// wrapEventHandler wraps an error-returning handler to match the expected signature
func wrapEventHandler(handler func(map[string]interface{}) error) func(map[string]interface{}) {
	return func(event map[string]interface{}) {
		// Call the handler and ignore the error
		// The error is already logged by the handler
		_ = handler(event)
	}
}

// parseReplicationParameters extracts database-specific parameters from node_id
func (e *Engine) parseReplicationParameters(nodeID *string, config *adapter.ReplicationConfig) {
	if nodeID == nil || *nodeID == "" {
		return
	}

	// NodeId format examples:
	// - PostgreSQL: "slot:<slotname>:pub:<pubname>"
	// - MySQL: "server_id:<id>:log_file:<file>:log_pos:<pos>"
	parts := strings.Split(*nodeID, ":")

	for i := 0; i < len(parts)-1; i += 2 {
		key := parts[i]
		value := parts[i+1]

		switch strings.ToLower(key) {
		case "slot":
			config.SlotName = value
		case "pub", "publication":
			config.PublicationName = value
		case "server_id":
			if config.Options == nil {
				config.Options = make(map[string]interface{})
			}
			config.Options["server_id"] = value
		case "log_file":
			if config.Options == nil {
				config.Options = make(map[string]interface{})
			}
			config.Options["log_file"] = value
		case "log_pos", "log_position":
			if config.Options == nil {
				config.Options = make(map[string]interface{})
			}
			config.Options["log_position"] = value
		}
	}
}

// setDefaultReplicationParameters sets default database-specific parameters
func (e *Engine) setDefaultReplicationParameters(config *adapter.ReplicationConfig, relationshipID string) {
	// Generate default names based on relationship ID
	shortID := relationshipID
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}

	// PostgreSQL-specific defaults
	if config.SlotName == "" {
		config.SlotName = fmt.Sprintf("redb_rel_%s", shortID)
	}
	if config.PublicationName == "" {
		config.PublicationName = fmt.Sprintf("redb_pub_%s", shortID)
	}

	// MySQL-specific defaults would go here if needed
	// For example: server_id, binlog position tracking, etc.
}

// saveCDCStreamState saves the current state of a CDC replication stream to the database
func (e *Engine) saveCDCStreamState(ctx context.Context, stream *CDCReplicationStream) error {
	if stream == nil {
		return fmt.Errorf("stream is nil")
	}

	// Get the current replication position
	position, err := stream.ReplicationSource.GetPosition()
	if err != nil {
		// If we can't get position, log warning but don't fail
		if e.logger != nil {
			e.logger.Warnf("Could not get position for replication source %s: %v", stream.ReplicationSourceID, err)
		}
		position = "" // Continue with empty position
	}

	// Get event statistics
	stream.mu.RLock()
	eventsProcessed := stream.EventsProcessed
	stream.mu.RUnlock()

	// Update the replication source in the database
	globalState := e.GetState()
	configRepo := globalState.GetConfigRepository()

	if configRepo == nil {
		return fmt.Errorf("configuration repository not available")
	}

	// Save the position and event count
	if err := configRepo.UpdateReplicationSourcePosition(ctx, stream.ReplicationSourceID, position, eventsProcessed); err != nil {
		return fmt.Errorf("failed to update replication source position: %w", err)
	}

	if e.logger != nil {
		e.logger.Infof("Saved CDC stream state for %s: position=%s, events=%d",
			stream.ReplicationSourceID, position, eventsProcessed)
	}

	return nil
}

// loadCDCStreamState loads the saved state of a CDC replication stream from the database
func (e *Engine) loadCDCStreamState(ctx context.Context, replicationSourceID string) (position string, eventsProcessed int64, err error) {
	globalState := e.GetState()
	configRepo := globalState.GetConfigRepository()

	if configRepo == nil {
		return "", 0, fmt.Errorf("configuration repository not available")
	}

	// Get the replication source from database
	source, err := configRepo.GetReplicationSource(ctx, replicationSourceID)
	if err != nil {
		return "", 0, fmt.Errorf("failed to get replication source: %w", err)
	}

	position = source.CDCPosition
	eventsProcessed = source.EventsProcessed

	if e.logger != nil {
		e.logger.Infof("Loaded CDC stream state for %s: position=%s, events=%d",
			replicationSourceID, position, eventsProcessed)
	}

	return position, eventsProcessed, nil
}

// createCheckpointFunc creates a checkpoint function for a replication source
// This function will be called periodically by the replication source to save its position
func (e *Engine) createCheckpointFunc(replicationSourceID string) func(context.Context, string) error {
	return func(ctx context.Context, position string) error {
		globalState := e.GetState()
		configRepo := globalState.GetConfigRepository()

		if configRepo == nil {
			return fmt.Errorf("configuration repository not available")
		}

		// Get current event count from the CDC manager
		manager := getCDCManager()
		manager.mu.RLock()
		stream, exists := manager.activeReplications[replicationSourceID]
		manager.mu.RUnlock()

		var eventsProcessed int64
		if exists {
			stream.mu.RLock()
			eventsProcessed = stream.EventsProcessed
			stream.mu.RUnlock()
		}

		// Update the replication source position
		if err := configRepo.UpdateReplicationSourcePosition(ctx, replicationSourceID, position, eventsProcessed); err != nil {
			if e.logger != nil {
				e.logger.Errorf("Failed to save checkpoint for %s: %v", replicationSourceID, err)
			}
			return err
		}

		if e.logger != nil {
			e.logger.Debugf("Saved checkpoint for %s: position=%s, events=%d",
				replicationSourceID, position, eventsProcessed)
		}

		return nil
	}
}
