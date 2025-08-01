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
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
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
	dbManager := w.state.GetDatabaseManager()

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

		err := w.setupWorkspaceReplicationClients(ctx, workspaceID, dbManager)
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

func (w *ReplicationWatcher) setupWorkspaceReplicationClients(ctx context.Context, workspaceID string, dbManager *database.DatabaseManager) error {
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

		err := w.setupReplicationClient(ctx, source, dbManager)
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

func (w *ReplicationWatcher) setupReplicationClient(ctx context.Context, source *config.ReplicationSource, dbManager *database.DatabaseManager) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	w.logger.Info("Setting up replication client for source %s (database %s, table %s)",
		source.ReplicationSourceID, source.DatabaseID, source.TableName)

	// Check if the main database is connected
	dbClient, err := dbManager.GetDatabaseClient(source.DatabaseID)
	if err != nil {
		w.logger.Info("Database %s is not connected, skipping replication client setup for source %s",
			source.DatabaseID, source.ReplicationSourceID)
		return nil // Don't treat this as an error - database might be intentionally disconnected
	}

	// Check if replication client already exists
	existingClient, err := dbManager.GetReplicationClient(source.ReplicationSourceID)
	if err == nil && atomic.LoadInt32(&existingClient.IsConnected) == 1 {
		w.logger.Info("Replication client for source %s is already connected", source.ReplicationSourceID)
		return nil
	}

	// Create replication configuration
	replicationConfig := common.ReplicationConfig{
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
		EventHandler:    w.createEventHandler(source),
	}

	// Create the replication client
	client, err := dbManager.ConnectReplication(replicationConfig)
	if err != nil {
		return fmt.Errorf("failed to connect replication client: %w", err)
	}

	// Start the replication source
	if replicationSource, ok := client.ReplicationSource.(common.ReplicationSourceInterface); ok {
		if err := replicationSource.Start(); err != nil {
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

	dbManager := w.state.GetDatabaseManager()

	// Get all active replication clients
	activeClients, err := dbManager.GetActiveReplicationClients()
	if err != nil {
		return fmt.Errorf("failed to get active replication clients: %w", err)
	}

	w.logger.Info("Performing health check on %d active replication clients", len(activeClients))

	// Check health of each replication client
	for _, client := range activeClients {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		w.checkReplicationClientHealth(client, dbManager)
	}

	// Also check for any missing replication clients
	return w.setupInitialReplicationClients(ctx)
}

func (w *ReplicationWatcher) checkReplicationClientHealth(client *common.ReplicationClient, dbManager *database.DatabaseManager) {
	// Check if the underlying database is still connected
	_, err := dbManager.GetDatabaseClient(client.DatabaseID)
	if err != nil {
		w.logger.Info("Database %s is disconnected, stopping replication client %s",
			client.DatabaseID, client.ReplicationID)

		// Stop and disconnect the replication client
		if err := dbManager.DisconnectReplication(client.ReplicationID); err != nil {
			w.logger.Error("Failed to disconnect orphaned replication client %s: %v",
				client.ReplicationID, err)
		}
		return
	}

	// Check if the replication source is still active
	if replicationSource, ok := client.ReplicationSource.(common.ReplicationSourceInterface); ok {
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
