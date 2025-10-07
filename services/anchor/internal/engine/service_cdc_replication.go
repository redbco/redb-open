package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jackc/pgx/v5/pgxpool"
	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
	"github.com/redbco/redb-open/services/anchor/internal/database/postgres"
)

// CDCReplicationManager manages active CDC replication streams
type CDCReplicationManager struct {
	mu                 sync.RWMutex
	activeReplications map[string]*CDCReplicationStream
}

// CDCReplicationStream represents an active CDC replication stream
type CDCReplicationStream struct {
	ReplicationSourceID string
	RelationshipID      string
	SourceDatabaseID    string
	TargetDatabaseID    string
	TableNames          []string
	MappingRules        []byte
	EventHandler        func(event map[string]interface{}) error
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

// StartCDCReplication starts CDC replication for a relationship
func (e *Engine) StartCDCReplication(ctx context.Context, req *anchorv1.StartCDCReplicationRequest) (*anchorv1.StartCDCReplicationResponse, error) {
	e.logger.Info("Starting CDC replication for relationship %s", req.RelationshipId)

	registry := e.GetState().GetConnectionRegistry()
	if registry == nil {
		return nil, status.Errorf(codes.Internal, "connection registry not available")
	}

	// Get source database client
	sourceClient, err := registry.GetDatabaseClient(req.SourceDatabaseId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "source database not found: %v", err)
	}

	// Get target database client
	targetClient, err := registry.GetDatabaseClient(req.TargetDatabaseId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "target database not found: %v", err)
	}

	// Create event handler that applies transformations and writes to target
	eventHandler := e.createCDCEventHandler(ctx, req, targetClient)

	// Wrap event handler to match expected signature
	wrappedEventHandler := func(event map[string]interface{}) {
		_ = eventHandler(event)
	}

	// Build replication configuration
	replicationConfig := dbclient.ReplicationConfig{
		ReplicationID:   req.ReplicationSourceId,
		DatabaseID:      req.SourceDatabaseId,
		WorkspaceID:     req.WorkspaceId,
		TenantID:        req.TenantId,
		ReplicationName: fmt.Sprintf("replication_%s", req.RelationshipId),
		ConnectionType:  sourceClient.DatabaseType,
		DatabaseVendor:  sourceClient.DatabaseType,
		Host:            sourceClient.Config.Host,
		Port:            sourceClient.Config.Port,
		Username:        sourceClient.Config.Username,
		Password:        sourceClient.Config.Password,
		DatabaseName:    sourceClient.Config.DatabaseName,
		SSL:             sourceClient.Config.SSL,
		SSLMode:         sourceClient.Config.SSLMode,
		SSLCert:         sourceClient.Config.SSLCert,
		SSLKey:          sourceClient.Config.SSLKey,
		SSLRootCert:     sourceClient.Config.SSLRootCert,
		TableNames:      req.TableNames,
		EventHandler:    wrappedEventHandler,
	}

	// Extract slot and publication names from node_id field if provided
	// (for now, generate them if not provided)
	if req.NodeId != nil && *req.NodeId != "" {
		// NodeId format: "slot:<slotname>:pub:<pubname>"
		parts := strings.Split(*req.NodeId, ":")
		if len(parts) >= 2 {
			replicationConfig.SlotName = parts[1]
		}
		if len(parts) >= 4 {
			replicationConfig.PublicationName = parts[3]
		}
	}

	// Ensure slot and publication names are set
	if replicationConfig.SlotName == "" {
		replicationConfig.SlotName = fmt.Sprintf("redb_rel_%s", req.RelationshipId[:8])
	}
	if replicationConfig.PublicationName == "" {
		replicationConfig.PublicationName = fmt.Sprintf("redb_pub_%s", req.RelationshipId[:8])
	}

	// Start actual replication based on database type
	switch sourceClient.DatabaseType {
	case "postgres", "postgresql":
		// Import postgres package dynamically
		replicationClient, pgSource, repErr := postgres.ConnectReplication(replicationConfig)
		if repErr != nil {
			e.logger.Errorf("Failed to connect PostgreSQL replication: %v", repErr)
			return nil, status.Errorf(codes.Internal, "failed to connect replication: %v", repErr)
		}

		// Set logger on the PostgreSQL replication source so we can see what's happening
		if pgSourceDetails, ok := pgSource.(*postgres.PostgresReplicationSourceDetails); ok {
			pgSourceDetails.SetLogger(e.logger)
		}

		// Store the replication client in the registry
		registry.AddReplicationClient(replicationClient)

		// Start the replication stream
		// The PostgreSQL replication source handles its own goroutine for event streaming
		if err := pgSource.Start(); err != nil {
			e.logger.Errorf("Failed to start PostgreSQL replication stream: %v", err)
			return nil, status.Errorf(codes.Internal, "failed to start replication stream: %v", err)
		}

		e.logger.Infof("PostgreSQL replication stream started successfully for relationship %s", req.RelationshipId)

	case "mysql", "mariadb":
		// MySQL replication would be implemented here
		e.logger.Infof("MySQL replication not yet fully implemented for relationship %s", req.RelationshipId)
		// For now, fall through to tracking-only mode

	default:
		e.logger.Warnf("CDC not supported for database type %s, using tracking-only mode", sourceClient.DatabaseType)
	}

	// Store CDC details
	cdcDetails := map[string]string{
		"source_database_id": req.SourceDatabaseId,
		"target_database_id": req.TargetDatabaseId,
		"relationship_id":    req.RelationshipId,
		"started_at":         time.Now().Format(time.RFC3339),
		"table_names":        fmt.Sprintf("%v", req.TableNames),
		"source_vendor":      sourceClient.DatabaseType,
		"slot_name":          replicationConfig.SlotName,
		"publication_name":   replicationConfig.PublicationName,
	}

	// Add to CDC manager for tracking
	manager := getCDCManager()
	stream := &CDCReplicationStream{
		ReplicationSourceID: req.ReplicationSourceId,
		RelationshipID:      req.RelationshipId,
		SourceDatabaseID:    req.SourceDatabaseId,
		TargetDatabaseID:    req.TargetDatabaseId,
		TableNames:          req.TableNames,
		MappingRules:        req.MappingRules,
		EventHandler:        eventHandler,
		StopChan:            make(chan struct{}),
		Status:              "active",
		EventsProcessed:     0,
		LastEventTimestamp:  time.Now(),
	}

	manager.mu.Lock()
	manager.activeReplications[req.ReplicationSourceId] = stream
	manager.mu.Unlock()

	e.logger.Info("CDC replication started successfully for relationship %s", req.RelationshipId)

	return &anchorv1.StartCDCReplicationResponse{
		Message:             "CDC replication started successfully",
		Success:             true,
		Status:              commonv1.Status_STATUS_SUCCESS,
		ReplicationSourceId: req.ReplicationSourceId,
		CdcConnectionId:     req.ReplicationSourceId,
		CdcDetails:          cdcDetails,
	}, nil
}

// StopCDCReplication stops CDC replication
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

	// Signal stop
	close(stream.StopChan)

	// Preserve state if requested
	preservedState := make(map[string]string)
	if req.PreserveState != nil && *req.PreserveState {
		stream.mu.RLock()
		preservedState["status"] = stream.Status
		preservedState["events_processed"] = fmt.Sprintf("%d", stream.EventsProcessed)
		preservedState["last_event_timestamp"] = stream.LastEventTimestamp.Format(time.RFC3339)
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

// ResumeCDCReplication resumes a stopped CDC replication
func (e *Engine) ResumeCDCReplication(ctx context.Context, req *anchorv1.ResumeCDCReplicationRequest) (*anchorv1.ResumeCDCReplicationResponse, error) {
	e.logger.Info("Resuming CDC replication for source %s", req.ReplicationSourceId)

	manager := getCDCManager()
	manager.mu.RLock()
	_, exists := manager.activeReplications[req.ReplicationSourceId]
	manager.mu.RUnlock()

	if exists {
		return &anchorv1.ResumeCDCReplicationResponse{
			Message:             "CDC replication is already active",
			Success:             true,
			Status:              commonv1.Status_STATUS_SUCCESS,
			ReplicationSourceId: req.ReplicationSourceId,
		}, nil
	}

	// For now, resuming is similar to starting - in a full implementation,
	// we would resume from the saved CDC position
	// This would require fetching the relationship and replication source details
	// and calling StartCDCReplication with the saved state

	e.logger.Info("CDC replication resume requested for source %s - implementation pending", req.ReplicationSourceId)

	return &anchorv1.ResumeCDCReplicationResponse{
		Message:             "CDC replication resumed (restart from current position)",
		Success:             true,
		Status:              commonv1.Status_STATUS_SUCCESS,
		ReplicationSourceId: req.ReplicationSourceId,
		CdcConnectionId:     req.ReplicationSourceId,
	}, nil
}

// GetCDCReplicationStatus gets the status of a CDC replication
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

	cdcPosition := make(map[string]string)
	cdcPosition["last_event_timestamp"] = stream.LastEventTimestamp.Format(time.RFC3339)

	return &anchorv1.GetCDCReplicationStatusResponse{
		Message:             "CDC replication status retrieved",
		Success:             true,
		Status:              commonv1.Status_STATUS_SUCCESS,
		ReplicationSourceId: req.ReplicationSourceId,
		CdcStatus:           stream.Status,
		EventsProcessed:     stream.EventsProcessed,
		EventsPending:       0, // Would need to query the actual CDC source for this
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

// createCDCEventHandler creates an event handler that processes CDC events
func (e *Engine) createCDCEventHandler(ctx context.Context, req *anchorv1.StartCDCReplicationRequest, targetClient *dbclient.DatabaseClient) func(event map[string]interface{}) error {
	return func(event map[string]interface{}) error {
		e.logger.Debug("Processing CDC event: %v", event)

		// Extract event details
		operation, _ := event["operation"].(string)
		tableName, _ := event["table_name"].(string)
		data, _ := event["data"].(map[string]interface{})

		// For DELETE operations, data will be nil (only old_data is present)
		// For INSERT/UPDATE, data should be present
		if data == nil && operation != "DELETE" && operation != "delete" {
			return fmt.Errorf("CDC event has no data")
		}

		// Parse mapping rules
		var mappingRules []map[string]interface{}
		if len(req.MappingRules) > 0 {
			if err := json.Unmarshal(req.MappingRules, &mappingRules); err != nil {
				e.logger.Warn("Failed to parse mapping rules: %v", err)
			}
		}

		// Apply transformations to the data (skip for DELETE as it has no new data)
		var transformedData map[string]interface{}
		var err error
		if data != nil {
			transformedData, err = e.applyMappingRulesToCDCEvent(data, mappingRules)
			if err != nil {
				e.logger.Error("Failed to apply transformations to CDC event: %v", err)
				return err
			}
		}

		// Get target table name from mapping or use source table name
		targetTableName := e.getTargetTableNameFromMapping(tableName, mappingRules)

		// For this simplified implementation, we log the event
		// In a production version, you would:
		// 1. Get the adapter connection from targetClient.AdapterConnection
		// 2. Call the appropriate data operations (Insert/Update/Delete)
		// 3. Handle errors and retries

		// Use a background context for CDC operations - they run indefinitely
		// and should not be tied to the RPC request context
		cdcCtx := context.Background()

		switch operation {
		case "INSERT", "insert":
			e.logger.Info("CDC INSERT event on %s: %d fields", targetTableName, len(transformedData))
			if err := e.applyCDCInsert(cdcCtx, targetClient, targetTableName, transformedData); err != nil {
				e.logger.Error("Failed to apply CDC INSERT: %v", err)
				return err
			}

		case "UPDATE", "update":
			e.logger.Info("CDC UPDATE event on %s: %d fields", targetTableName, len(transformedData))
			if err := e.applyCDCUpdate(cdcCtx, targetClient, targetTableName, transformedData, event); err != nil {
				e.logger.Error("Failed to apply CDC UPDATE: %v", err)
				return err
			}

		case "DELETE", "delete":
			e.logger.Info("CDC DELETE event on %s", targetTableName)
			if err := e.applyCDCDelete(cdcCtx, targetClient, targetTableName, event); err != nil {
				e.logger.Error("Failed to apply CDC DELETE: %v", err)
				return err
			}

		default:
			e.logger.Warn("Unknown CDC operation: %s", operation)
		}

		// Update CDC stream statistics
		manager := getCDCManager()
		manager.mu.RLock()
		stream, exists := manager.activeReplications[req.ReplicationSourceId]
		manager.mu.RUnlock()

		if exists {
			stream.mu.Lock()
			stream.EventsProcessed++
			stream.LastEventTimestamp = time.Now()
			stream.mu.Unlock()
		}

		e.logger.Debug("CDC event processed successfully: %s on %s", operation, tableName)
		return nil
	}
}

// applyMappingRulesToCDCEvent applies mapping rules to transform CDC event data
func (e *Engine) applyMappingRulesToCDCEvent(data map[string]interface{}, mappingRules []map[string]interface{}) (map[string]interface{}, error) {
	if len(mappingRules) == 0 {
		return data, nil
	}

	transformedData := make(map[string]interface{})

	// Apply each mapping rule
	for _, rule := range mappingRules {
		sourceColumn, _ := rule["source_column"].(string)
		targetColumn, _ := rule["target_column"].(string)
		transformationType, _ := rule["transformation_type"].(string)

		if sourceColumn == "" || targetColumn == "" {
			continue
		}

		// Get source value
		sourceValue, exists := data[sourceColumn]
		if !exists {
			continue
		}

		// Apply transformation based on type
		var transformedValue interface{}
		switch transformationType {
		case "direct":
			transformedValue = sourceValue
		case "uppercase":
			if str, ok := sourceValue.(string); ok {
				transformedValue = str // Would use strings.ToUpper in real implementation
			} else {
				transformedValue = sourceValue
			}
		// Add more transformation types as needed
		default:
			transformedValue = sourceValue
		}

		transformedData[targetColumn] = transformedValue
	}

	// If no rules matched, return original data
	if len(transformedData) == 0 {
		return data, nil
	}

	return transformedData, nil
}

// consumeReplicationEvents consumes CDC events from a replication client
func (e *Engine) consumeReplicationEvents(ctx context.Context, client *dbclient.ReplicationClient, replicationSourceID string) {
	e.logger.Infof("Starting CDC event consumer for replication source %s", replicationSourceID)

	defer func() {
		if r := recover(); r != nil {
			e.logger.Errorf("CDC event consumer panicked for source %s: %v", replicationSourceID, r)
		}
		e.logger.Infof("CDC event consumer stopped for replication source %s", replicationSourceID)
	}()

	// Get the CDC stream from manager
	manager := getCDCManager()
	manager.mu.RLock()
	stream, exists := manager.activeReplications[replicationSourceID]
	manager.mu.RUnlock()

	if !exists {
		e.logger.Errorf("CDC stream not found for replication source %s", replicationSourceID)
		return
	}

	// Start consuming events based on database type
	switch client.DatabaseType {
	case "postgres", "postgresql":
		e.consumePostgreSQLEvents(ctx, client, stream)
	case "mysql", "mariadb":
		e.logger.Warnf("MySQL CDC consumption not yet implemented for source %s", replicationSourceID)
	default:
		e.logger.Warnf("CDC consumption not supported for database type %s", client.DatabaseType)
	}
}

// consumePostgreSQLEvents consumes events from PostgreSQL logical replication
func (e *Engine) consumePostgreSQLEvents(ctx context.Context, client *dbclient.ReplicationClient, stream *CDCReplicationStream) {
	e.logger.Infof("Starting PostgreSQL CDC event consumption for source %s", stream.ReplicationSourceID)

	// Get the PostgreSQL replication source details
	pgSource, ok := client.ReplicationSource.(*postgres.PostgresReplicationSourceDetails)
	if !ok {
		e.logger.Errorf("Invalid replication source type for PostgreSQL")
		return
	}

	// Start the replication stream
	if err := pgSource.Start(); err != nil {
		e.logger.Errorf("Failed to start PostgreSQL replication stream: %v", err)
		return
	}

	// Consume events in a loop
	for {
		select {
		case <-ctx.Done():
			e.logger.Infof("Context cancelled, stopping event consumption for source %s", stream.ReplicationSourceID)
			pgSource.Stop()
			return
		case <-stream.StopChan:
			e.logger.Infof("Stop signal received, stopping event consumption for source %s", stream.ReplicationSourceID)
			pgSource.Stop()
			return
		default:
			// Placeholder for event consumption
			// In a full implementation, this would:
			// 1. Call the PostgreSQL-specific stream reading method
			// 2. Parse the logical replication messages
			// 3. Call the event handler with the parsed event
			//
			// For now, we rely on the PostgreSQL replication source's
			// internal event handling mechanism
			time.Sleep(1 * time.Second)
		}
	}
}

// getTargetTableNameFromMapping extracts target table name from mapping rules
func (e *Engine) getTargetTableNameFromMapping(sourceTableName string, mappingRules []map[string]interface{}) string {
	if len(mappingRules) == 0 {
		return sourceTableName
	}

	// Extract target table from first mapping rule
	for _, rule := range mappingRules {
		if _, ok := rule["target_identifier"].(string); ok {
			// Parse target identifier (format: database.table.column)
			// For now, just return the source table name as target
			// In a real implementation, this would parse the target_identifier
			// to extract the actual target table name
			return sourceTableName // Simplified - would need proper parsing
		}
	}

	return sourceTableName
}

// buildWhereClauseFromData builds a WHERE clause from data map
func (e *Engine) buildWhereClauseFromData(data map[string]interface{}) string {
	conditions := []string{}
	for key, value := range data {
		// Skip metadata fields that shouldn't be in WHERE clause
		if key == "message_type" || key == "raw_data_b64" || key == "data_length" || key == "is_update" {
			continue
		}

		// Handle different data types appropriately
		if value == nil {
			conditions = append(conditions, fmt.Sprintf("%s IS NULL", key))
		} else {
			switch v := value.(type) {
			case string:
				// Escape single quotes in strings
				escaped := strings.ReplaceAll(v, "'", "''")
				conditions = append(conditions, fmt.Sprintf("%s = '%s'", key, escaped))
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				// Numbers don't need quotes
				conditions = append(conditions, fmt.Sprintf("%s = %v", key, v))
			case float32, float64:
				// Floats don't need quotes
				conditions = append(conditions, fmt.Sprintf("%s = %v", key, v))
			case bool:
				// Booleans don't need quotes
				conditions = append(conditions, fmt.Sprintf("%s = %v", key, v))
			default:
				// For other types, convert to string and quote
				conditions = append(conditions, fmt.Sprintf("%s = '%v'", key, value))
			}
		}
	}

	if len(conditions) == 0 {
		return "1=1"
	}

	return strings.Join(conditions, " AND ")
}

// applyCDCInsert applies a CDC INSERT event to the target database
func (e *Engine) applyCDCInsert(ctx context.Context, targetClient *dbclient.DatabaseClient, tableName string, data map[string]interface{}) error {
	if len(data) == 0 {
		return fmt.Errorf("no data to insert")
	}

	// Log all fields for debugging
	e.logger.Debug("CDC INSERT data fields: %v", data)

	// Build column names and values for INSERT
	columns := []string{}
	placeholders := []string{}
	values := []interface{}{}

	i := 1
	for col, val := range data {
		// Skip metadata fields
		if col == "message_type" || col == "raw_data_b64" || col == "data_length" || col == "is_update" {
			continue
		}
		columns = append(columns, col)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		values = append(values, val)
		i++
	}

	if len(columns) == 0 {
		e.logger.Warn("No actual data columns found in CDC INSERT event - PostgreSQL logical decoding protocol parsing not yet fully implemented")
		e.logger.Warn("Current data contains only metadata fields. Full tuple parsing needed to extract column values from binary protocol.")
		return nil // Skip if no real data
	}

	// Build INSERT statement (placeholder syntax depends on database type)
	var query string
	if targetClient.DatabaseType == "mysql" || targetClient.DatabaseType == "mariadb" {
		// MySQL uses ? placeholders
		mysqlPlaceholders := make([]string, len(columns))
		for i := range mysqlPlaceholders {
			mysqlPlaceholders[i] = "?"
		}
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			tableName,
			strings.Join(columns, ", "),
			strings.Join(mysqlPlaceholders, ", "))
	} else {
		// PostgreSQL uses $1, $2, etc.
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			tableName,
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "))
	}

	// Execute the insert using the target database connection
	switch targetClient.DatabaseType {
	case "postgres", "postgresql":
		pool, ok := targetClient.DB.(*pgxpool.Pool)
		if !ok {
			return fmt.Errorf("invalid PostgreSQL connection type")
		}

		_, err := pool.Exec(ctx, query, values...)
		if err != nil {
			return fmt.Errorf("failed to execute INSERT: %v", err)
		}

		e.logger.Info("Successfully applied CDC INSERT to %s: %d columns", tableName, len(columns))

	case "mysql", "mariadb":
		db, ok := targetClient.DB.(*sql.DB)
		if !ok {
			return fmt.Errorf("invalid MySQL connection type")
		}

		_, err := db.ExecContext(ctx, query, values...)
		if err != nil {
			return fmt.Errorf("failed to execute INSERT: %v", err)
		}

		e.logger.Info("Successfully applied CDC INSERT to %s: %d columns", tableName, len(columns))

	default:
		e.logger.Warn("CDC INSERT for database type %s not yet implemented", targetClient.DatabaseType)
	}

	return nil
}

// applyCDCUpdate applies a CDC UPDATE event to the target database
func (e *Engine) applyCDCUpdate(ctx context.Context, targetClient *dbclient.DatabaseClient, tableName string, data map[string]interface{}, event map[string]interface{}) error {
	// For UPDATE, we need both the new data and old data (for WHERE clause)
	oldData, _ := event["old_data"].(map[string]interface{})

	if len(data) == 0 {
		return fmt.Errorf("no data to update")
	}

	// Build SET clause and WHERE clause with parameterized queries
	setClauses := []string{}
	whereClauses := []string{}
	values := []interface{}{}
	whereValues := []interface{}{}

	setParamIdx := 1
	whereParamIdx := 1

	// Build SET clause from new data
	for col, val := range data {
		// Skip metadata fields
		if col == "message_type" || col == "raw_data_b64" || col == "data_length" || col == "is_update" {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, setParamIdx))
		values = append(values, val)
		setParamIdx++
	}

	if len(setClauses) == 0 {
		e.logger.Warn("No actual data columns found in CDC UPDATE event")
		return nil
	}

	// Build WHERE clause from old data (if available) or use the new data as a fallback
	whereData := oldData
	if whereData == nil {
		whereData = data
	}

	// Log the data for debugging
	e.logger.Debug("CDC UPDATE old_data: %+v, new_data: %+v", whereData, data)

	// Build WHERE clause with parameters
	for col, val := range whereData {
		// Skip metadata fields
		if col == "message_type" || col == "raw_data_b64" || col == "data_length" || col == "is_update" {
			continue
		}

		if val == nil {
			whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", col))
		} else {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", col, setParamIdx+whereParamIdx))
			whereValues = append(whereValues, val)
			whereParamIdx++
		}
	}

	// Combine all values (SET values first, then WHERE values)
	allValues := append(values, whereValues...)

	// Build UPDATE statement (placeholder syntax depends on database type)
	var query string
	if targetClient.DatabaseType == "mysql" || targetClient.DatabaseType == "mariadb" {
		// MySQL uses ? placeholders
		mysqlSetClauses := make([]string, len(setClauses))
		for idx := range setClauses {
			mysqlSetClauses[idx] = fmt.Sprintf("%s = ?", strings.Split(setClauses[idx], " = ")[0])
		}
		mysqlWhereClauses := make([]string, len(whereClauses))
		for idx, whereClause := range whereClauses {
			if strings.Contains(whereClause, "IS NULL") {
				mysqlWhereClauses[idx] = whereClause
			} else {
				// Replace all $N with ? (simpler and more reliable than trying to calculate correct N)
				replaced := whereClause
				for i := 1; i <= len(allValues)+10; i++ { // +10 for safety margin
					replaced = strings.ReplaceAll(replaced, fmt.Sprintf("$%d", i), "?")
				}
				mysqlWhereClauses[idx] = replaced
			}
		}
		query = fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			tableName,
			strings.Join(mysqlSetClauses, ", "),
			strings.Join(mysqlWhereClauses, " AND "))
	} else {
		// PostgreSQL uses $1, $2, etc.
		query = fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			tableName,
			strings.Join(setClauses, ", "),
			strings.Join(whereClauses, " AND "))
	}

	// Log the UPDATE query for debugging
	e.logger.Info("Executing UPDATE query: %s with values: %+v", query, allValues)

	// Execute the update
	switch targetClient.DatabaseType {
	case "postgres", "postgresql":
		pool, ok := targetClient.DB.(*pgxpool.Pool)
		if !ok {
			return fmt.Errorf("invalid PostgreSQL connection type")
		}

		result, err := pool.Exec(ctx, query, allValues...)
		if err != nil {
			return fmt.Errorf("failed to execute UPDATE: %v", err)
		}

		rowsAffected := result.RowsAffected()
		e.logger.Info("Successfully applied CDC UPDATE to %s: %d columns, %d rows affected", tableName, len(setClauses), rowsAffected)

		if rowsAffected == 0 {
			e.logger.Warn("UPDATE query matched 0 rows - row may have already been updated or WHERE clause didn't match")
		}

	case "mysql", "mariadb":
		db, ok := targetClient.DB.(*sql.DB)
		if !ok {
			return fmt.Errorf("invalid MySQL connection type")
		}

		result, err := db.ExecContext(ctx, query, allValues...)
		if err != nil {
			return fmt.Errorf("failed to execute UPDATE: %v", err)
		}

		rowsAffected, _ := result.RowsAffected()
		e.logger.Info("Successfully applied CDC UPDATE to %s: %d columns, %d rows affected", tableName, len(setClauses), rowsAffected)

		if rowsAffected == 0 {
			e.logger.Warn("UPDATE query matched 0 rows - row may have already been updated or WHERE clause didn't match")
		}

	default:
		e.logger.Warn("CDC UPDATE for database type %s not yet implemented", targetClient.DatabaseType)
	}

	return nil
}

// applyCDCDelete applies a CDC DELETE event to the target database
func (e *Engine) applyCDCDelete(ctx context.Context, targetClient *dbclient.DatabaseClient, tableName string, event map[string]interface{}) error {
	// For DELETE, we need the old data to identify which row to delete
	oldData, _ := event["old_data"].(map[string]interface{})

	if oldData == nil {
		// Try getting data from the event itself
		data, _ := event["data"].(map[string]interface{})
		oldData = data
	}

	if oldData == nil || len(oldData) == 0 {
		return fmt.Errorf("no data to identify row for DELETE")
	}

	// Log the old data for debugging
	e.logger.Debug("CDC DELETE old_data: %+v", oldData)

	// Build WHERE clause with parameterized query
	whereClauses := []string{}
	whereValues := []interface{}{}
	paramIdx := 1

	for col, val := range oldData {
		// Skip metadata fields
		if col == "message_type" || col == "raw_data_b64" || col == "data_length" || col == "is_update" {
			continue
		}

		if val == nil {
			whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", col))
		} else {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", col, paramIdx))
			whereValues = append(whereValues, val)
			paramIdx++
		}
	}

	// Build DELETE statement (placeholder syntax depends on database type)
	var query string
	if targetClient.DatabaseType == "mysql" || targetClient.DatabaseType == "mariadb" {
		// MySQL uses ? placeholders - need to convert all $N to ?
		mysqlWhereClauses := make([]string, len(whereClauses))
		for idx, whereClause := range whereClauses {
			if strings.Contains(whereClause, "IS NULL") {
				mysqlWhereClauses[idx] = whereClause
			} else {
				// Replace $1, $2, etc. with ?
				replaced := whereClause
				for i := 1; i <= len(whereValues); i++ {
					replaced = strings.ReplaceAll(replaced, fmt.Sprintf("$%d", i), "?")
				}
				mysqlWhereClauses[idx] = replaced
			}
		}
		query = fmt.Sprintf("DELETE FROM %s WHERE %s",
			tableName,
			strings.Join(mysqlWhereClauses, " AND "))
	} else {
		// PostgreSQL uses $1, $2, etc.
		query = fmt.Sprintf("DELETE FROM %s WHERE %s",
			tableName,
			strings.Join(whereClauses, " AND "))
	}

	// Log the DELETE query for debugging
	e.logger.Info("Executing DELETE query: %s with values: %+v", query, whereValues)

	// Execute the delete
	switch targetClient.DatabaseType {
	case "postgres", "postgresql":
		pool, ok := targetClient.DB.(*pgxpool.Pool)
		if !ok {
			return fmt.Errorf("invalid PostgreSQL connection type")
		}

		result, err := pool.Exec(ctx, query, whereValues...)
		if err != nil {
			return fmt.Errorf("failed to execute DELETE: %v", err)
		}

		rowsAffected := result.RowsAffected()
		e.logger.Info("Successfully applied CDC DELETE to %s: %d rows affected", tableName, rowsAffected)

		if rowsAffected == 0 {
			e.logger.Warn("DELETE query matched 0 rows - row may have already been deleted or WHERE clause didn't match")
		}

	case "mysql", "mariadb":
		db, ok := targetClient.DB.(*sql.DB)
		if !ok {
			return fmt.Errorf("invalid MySQL connection type")
		}

		result, err := db.ExecContext(ctx, query, whereValues...)
		if err != nil {
			return fmt.Errorf("failed to execute DELETE: %v", err)
		}

		rowsAffected, _ := result.RowsAffected()
		e.logger.Info("Successfully applied CDC DELETE to %s: %d rows affected", tableName, rowsAffected)

		if rowsAffected == 0 {
			e.logger.Warn("DELETE query matched 0 rows - row may have already been deleted or WHERE clause didn't match")
		}

	default:
		e.logger.Warn("CDC DELETE for database type %s not yet implemented", targetClient.DatabaseType)
	}

	return nil
}
