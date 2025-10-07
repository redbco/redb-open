package watcher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/anchor/internal/config"
	"github.com/redbco/redb-open/services/anchor/internal/database"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
	"github.com/redbco/redb-open/services/anchor/internal/database/postgres"
	"github.com/redbco/redb-open/services/anchor/internal/state"
)

type ReplicationWatcher struct {
	state      *state.GlobalState
	repository *config.Repository
	logger     *logger.Logger
	stopChan   chan struct{}
	isRunning  int32
}

func NewReplicationWatcher(repository *config.Repository, logger *logger.Logger) *ReplicationWatcher {
	return &ReplicationWatcher{
		state:      state.GetInstance(),
		repository: repository,
		logger:     logger,
		stopChan:   make(chan struct{}),
	}
}

func (w *ReplicationWatcher) Start(ctx context.Context) {
	if !atomic.CompareAndSwapInt32(&w.isRunning, 0, 1) {
		w.logger.Warn("Replication watcher is already running")
		return
	}

	// Check for replication sources every 5 minutes (less frequent than before)
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	w.logger.Info("Replication watcher starting with event-driven approach...")
	defer func() {
		atomic.StoreInt32(&w.isRunning, 0)
		w.logger.Info("Replication watcher shutdown complete")
	}()

	// Perform initial setup of replication clients
	w.logger.Info("Performing initial replication client setup...")
	if err := w.setupInitialReplicationClients(ctx); err != nil {
		w.logger.Error("Failed initial replication client setup: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Replication watcher received shutdown signal")
			return
		case <-w.stopChan:
			w.logger.Info("Replication watcher received stop signal")
			return
		case <-ticker.C:
			// Periodic health check and setup of missing replication clients
			if ctx.Err() != nil {
				w.logger.Info("Replication watcher shutting down, skipping periodic check")
				return
			}

			if err := w.periodicReplicationHealthCheck(ctx); err != nil {
				if ctx.Err() == nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					w.logger.Error("Failed periodic replication health check: %v", err)
				}
			}
		}
	}
}

func (w *ReplicationWatcher) Stop() {
	if atomic.LoadInt32(&w.isRunning) == 1 {
		close(w.stopChan)
	}
}

func (w *ReplicationWatcher) setupInitialReplicationClients(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	nodeID := w.state.GetNodeID()
	registry := w.state.GetConnectionRegistry()

	// Get all workspace IDs for this node
	workspaceIDs, err := w.getWorkspaceIDsForNode(ctx, nodeID)
	if err != nil {
		w.logger.Error("Failed to get workspace IDs for node: %v", err)
		return err
	}

	w.logger.Info("Setting up replication clients for %d workspaces for node %s", len(workspaceIDs), nodeID)

	// Setup replication clients for each workspace
	for _, workspaceID := range workspaceIDs {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := w.setupWorkspaceReplicationClients(ctx, workspaceID, registry)
		if err != nil {
			w.logger.Error("Failed to setup replication clients for workspace %s: %v", workspaceID, err)
			// Continue with other workspaces even if one fails
		}
	}

	return nil
}

func (w *ReplicationWatcher) getWorkspaceIDsForNode(ctx context.Context, nodeID string) ([]string, error) {
	// Get all database configs for this node to extract workspace IDs
	dbConfigs, err := w.repository.GetAllDatabaseConfigs(ctx, nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database configs: %w", err)
	}

	// Extract unique workspace IDs
	workspaceIDSet := make(map[string]bool)
	for _, config := range dbConfigs {
		workspaceIDSet[config.WorkspaceID] = true
	}

	var workspaceIDs []string
	for workspaceID := range workspaceIDSet {
		workspaceIDs = append(workspaceIDs, workspaceID)
	}

	return workspaceIDs, nil
}

func (w *ReplicationWatcher) setupWorkspaceReplicationClients(ctx context.Context, workspaceID string, registry *database.ConnectionRegistry) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	w.logger.Info("Setting up replication clients for workspace %s", workspaceID)

	// Get all replication sources for this workspace
	replicationSources, err := w.repository.GetAllReplicationSources(ctx, workspaceID)
	if err != nil {
		return fmt.Errorf("failed to get replication sources for workspace %s: %w", workspaceID, err)
	}

	w.logger.Info("Found %d replication sources for workspace %s", len(replicationSources), workspaceID)

	// Setup replication client for each source
	for _, source := range replicationSources {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Skip sources that are stopped - they require manual restart
		if source.Status == "STATUS_STOPPED" {
			w.logger.Info("Skipping replication source %s (status: STOPPED - requires manual start)", source.ReplicationSourceID)
			continue
		}

		// Only auto-start sources that were previously active
		if source.Status != "STATUS_ACTIVE" && source.Status != "STATUS_PENDING" {
			w.logger.Info("Skipping replication source %s (status: %s - not eligible for auto-start)", source.ReplicationSourceID, source.Status)
			continue
		}

		err := w.setupReplicationClient(ctx, source, registry)
		if err != nil {
			w.logger.Error("Failed to setup replication client for source %s: %v", source.ReplicationSourceID, err)
			// Update status to indicate failure
			w.repository.UpdateReplicationSourceStatus(ctx, source.ReplicationSourceID, "STATUS_ERROR", fmt.Sprintf("Setup failed: %v", err))
		} else {
			w.logger.Info("Successfully setup replication client for source %s", source.ReplicationSourceID)
			// Update status to indicate success
			w.repository.UpdateReplicationSourceStatus(ctx, source.ReplicationSourceID, "STATUS_ACTIVE", "Replication client active")
		}
	}

	return nil
}

func (w *ReplicationWatcher) setupReplicationClient(ctx context.Context, source *config.ReplicationSource, registry *database.ConnectionRegistry) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	w.logger.Info("Setting up replication client for source %s (database %s, table %s)",
		source.ReplicationSourceID, source.DatabaseID, source.TableName)

	// Check if the main database is connected
	dbClient, err := registry.GetDatabaseClient(source.DatabaseID)
	if err != nil {
		w.logger.Info("Database %s is not connected, skipping replication client setup for source %s",
			source.DatabaseID, source.ReplicationSourceID)
		return nil // Don't treat this as an error - database might be intentionally disconnected
	}

	// Check if replication client already exists
	existingClient, err := registry.GetReplicationClient(source.ReplicationSourceID)
	if err == nil && atomic.LoadInt32(&existingClient.IsConnected) == 1 {
		w.logger.Info("Replication client for source %s is already connected", source.ReplicationSourceID)
		return nil
	}

	// Create replication configuration
	replicationConfig := dbclient.ReplicationConfig{
		ReplicationID:     source.ReplicationSourceID,
		DatabaseID:        source.DatabaseID,
		WorkspaceID:       source.WorkspaceID,
		TenantID:          dbClient.Config.TenantID,
		ReplicationName:   fmt.Sprintf("replication_%s_%s", source.DatabaseID, source.TableName),
		ConnectionType:    dbClient.DatabaseType,
		DatabaseVendor:    dbClient.Config.DatabaseVendor,
		Host:              dbClient.Config.Host,
		Port:              dbClient.Config.Port,
		Username:          dbClient.Config.Username,
		Password:          dbClient.Config.Password,
		DatabaseName:      dbClient.Config.DatabaseName,
		SSL:               dbClient.Config.SSL,
		SSLMode:           dbClient.Config.SSLMode,
		SSLCert:           dbClient.Config.SSLCert,
		SSLKey:            dbClient.Config.SSLKey,
		SSLRootCert:       dbClient.Config.SSLRootCert,
		ConnectedToNodeID: dbClient.Config.ConnectedToNodeID,
		OwnerID:           dbClient.Config.OwnerID,

		// Replication-specific configuration
		TableNames:      strings.Split(source.TableName, ","),
		SlotName:        source.SlotName,
		PublicationName: source.PublicationName,
		StartPosition:   source.CDCPosition, // Resume from saved position
		EventHandler:    w.createEventHandler(source),
	}

	// Log if we're resuming from a saved position
	if source.CDCPosition != "" {
		w.logger.Info("Resuming replication for %s from saved position: %s (events processed: %d)",
			source.ReplicationSourceID, source.CDCPosition, source.EventsProcessed)
	}

	// Actually create the replication connection using the PostgreSQL-specific code
	// This will create the replication slot, publication, and establish the connection
	var client *dbclient.ReplicationClient
	var replicationSourceObj dbclient.ReplicationSourceInterface

	if dbClient.DatabaseType == "postgres" {
		// Import postgres package dynamically to avoid import cycles
		// Call ConnectReplication to create the actual replication source
		pgClient, pgSource, err := postgres.ConnectReplication(replicationConfig)
		if err != nil {
			return fmt.Errorf("failed to connect PostgreSQL replication: %w", err)
		}
		client = pgClient
		replicationSourceObj = pgSource
	} else {
		// For non-PostgreSQL databases, create a placeholder client
		w.logger.Warn("Database type %s does not have replication support in ReplicationWatcher yet", dbClient.DatabaseType)
		client = &dbclient.ReplicationClient{
			ReplicationID:     source.ReplicationSourceID,
			DatabaseID:        source.DatabaseID,
			ReplicationSource: source,
			Config:            replicationConfig,
			IsConnected:       1,
		}
	}

	// Track the client in registry
	registry.AddReplicationClient(client)

	// Start the replication source
	if replicationSourceObj != nil {
		if err := replicationSourceObj.Start(); err != nil {
			w.logger.Error("Failed to start replication source for %s: %v", source.ReplicationSourceID, err)
			// Don't return error, just log it
		} else {
			w.logger.Info("Successfully started replication source for %s", source.ReplicationSourceID)
		}
	}

	return nil
}

func (w *ReplicationWatcher) createEventHandler(source *config.ReplicationSource) func(map[string]interface{}) {
	return func(event map[string]interface{}) {
		// Enhanced event handler with better logging and potential forwarding
		w.logger.Info("Replication Event [%s.%s]: %s",
			source.DatabaseID, source.TableName, event["operation"])

		// Add metadata to the event
		enrichedEvent := make(map[string]interface{})
		for k, v := range event {
			enrichedEvent[k] = v
		}
		enrichedEvent["source_id"] = source.ReplicationSourceID
		enrichedEvent["workspace_id"] = source.WorkspaceID
		enrichedEvent["received_at"] = time.Now().UTC().Format(time.RFC3339)

		// TODO: Forward events to event processing system
		// This could be extended to send events to a message queue, webhook, etc.

		// For now, just log the enriched event
		w.logger.Debug("Enriched replication event: %+v", enrichedEvent)
	}
}

func (w *ReplicationWatcher) periodicReplicationHealthCheck(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	registry := w.state.GetConnectionRegistry()

	// Get all active replication clients
	activeClients, err := registry.GetActiveReplicationClients()
	if err != nil {
		return fmt.Errorf("failed to get active replication clients: %w", err)
	}

	w.logger.Info("Performing health check on %d active replication clients", len(activeClients))

	// Check health of each replication client
	for _, client := range activeClients {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		w.checkReplicationClientHealth(client, registry)
	}

	// Also check for any missing replication clients
	return w.setupInitialReplicationClients(ctx)
}

func (w *ReplicationWatcher) checkReplicationClientHealth(client *dbclient.ReplicationClient, registry *database.ConnectionRegistry) {
	// Check if the underlying database is still connected
	_, err := registry.GetDatabaseClient(client.DatabaseID)
	if err != nil {
		w.logger.Info("Database %s is disconnected, stopping replication client %s",
			client.DatabaseID, client.ReplicationID)

		// Stop and disconnect the replication client
		if err := registry.DisconnectReplication(client.ReplicationID); err != nil {
			w.logger.Error("Failed to disconnect orphaned replication client %s: %v",
				client.ReplicationID, err)
		}
		return
	}

	// Check if the replication source is still active
	if replicationSource, ok := client.ReplicationSource.(dbclient.ReplicationSourceInterface); ok {
		if !replicationSource.IsActive() {
			w.logger.Warn("Replication source for client %s is not active, attempting to restart",
				client.ReplicationID)

			if err := replicationSource.Start(); err != nil {
				w.logger.Error("Failed to restart replication source for client %s: %v",
					client.ReplicationID, err)
				atomic.StoreInt32(&client.ErrorCount, atomic.LoadInt32(&client.ErrorCount)+1)
			} else {
				w.logger.Info("Successfully restarted replication source for client %s",
					client.ReplicationID)
				atomic.StoreInt32(&client.ErrorCount, 0) // Reset error count on success
			}
		}
	}

	// Update last activity
	client.LastActivity = time.Now()
}
