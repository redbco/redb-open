package watcher

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/anchor/internal/config"
	"github.com/redbco/redb-open/services/anchor/internal/database"
	"github.com/redbco/redb-open/services/anchor/internal/state"
)

type ConfigWatcher struct {
	state      *state.GlobalState
	repository *config.Repository
	logger     *logger.Logger
}

func NewConfigWatcher(repository *config.Repository, supervisorAddr string, logger *logger.Logger) *ConfigWatcher {
	return &ConfigWatcher{
		state:      state.GetInstance(),
		repository: repository,
		logger:     logger,
	}
}

func (w *ConfigWatcher) Start(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	w.logger.Info("Config watcher starting...")
	defer w.logger.Info("Config watcher shutdown complete")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Check if context is cancelled before starting work
			if ctx.Err() != nil {
				w.logger.Info("Config watcher shutting down, skipping work")
				return
			}

			if err := w.checkConnectionHealth(ctx); err != nil {
				// Don't log context cancellation errors as they're expected during shutdown
				if ctx.Err() == nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					w.logger.Error("Failed to check connection health: %v", err)
				}
			}
		}
	}
}

func (w *ConfigWatcher) checkConnectionHealth(ctx context.Context) error {
	// Check if context is cancelled before starting
	if ctx.Err() != nil {
		return ctx.Err()
	}

	nodeID := w.state.GetNodeID()
	dbManager := w.state.GetDatabaseManager()

	// Process database configurations
	if err := w.processDatabaseConfigs(ctx, nodeID, dbManager); err != nil {
		w.logger.Error("Failed to process database configs: %v", err)
		return err
	}

	// Check if context is cancelled before continuing
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Process instance configurations
	if err := w.processInstanceConfigs(ctx, nodeID, dbManager); err != nil {
		w.logger.Error("Failed to process instance configs: %v", err)
		return err
	}

	// Check if context is cancelled before continuing
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Check health of all connected databases and update metadata
	return w.checkAllConnectionsHealth(ctx, dbManager)
}

func (w *ConfigWatcher) processDatabaseConfigs(ctx context.Context, nodeID string, dbManager *database.DatabaseManager) error {
	// Check if context is cancelled before starting
	if ctx.Err() != nil {
		return ctx.Err()
	}

	w.logger.Info("Processing database configs")
	// Load all enabled database configurations
	dbConfigs, err := w.repository.GetAllDatabaseConfigs(ctx, nodeID)
	if err != nil {
		w.logger.Error("Failed to load database configurations: %v", err)
		return err
	}

	w.logger.Info("Found %d enabled database configurations to process", len(dbConfigs))

	connectionErrors := 0
	// Establish connections for new database configurations
	for _, dbConfig := range dbConfigs {
		// Check if context is cancelled before processing each config
		if ctx.Err() != nil {
			return ctx.Err()
		}

		clientID := dbConfig.DatabaseID

		// Skip if already connected
		if _, err := dbManager.GetDatabaseClient(clientID); err == nil {
			w.logger.Debug("Database %s already connected, skipping", clientID)
			continue
		}

		// Convert unified config to connection config
		dbConnConfig := dbConfig.ToConnectionConfig()

		// Attempt to establish new connection
		w.logger.Info("Establishing new connection for database: %s", clientID)
		client, err := dbManager.ConnectDatabase(dbConnConfig)
		if err != nil {
			// Client database connection failures are warnings, not errors
			// (detailed logging is already handled by DatabaseManager's unified logging)
			w.logger.Warn("Client database connection failed: %s", clientID)
			w.repository.UpdateDatabaseConnectionStatus(ctx, clientID, false, fmt.Sprintf("Connection failed: %v", err))
			connectionErrors++
			continue
		}

		// Mark as connected if successful
		atomic.StoreInt32(&client.IsConnected, 1)
		w.repository.UpdateDatabaseConnectionStatus(ctx, clientID, true, "Connected successfully")
		w.logger.Info("Connected to database %s", clientID)

		collector := database.NewDatabaseMetadataCollector(client)
		metadata, err := collector.CollectMetadata(ctx, clientID)
		if err != nil {
			w.logger.Error("Failed to collect database metadata for %s: %v", clientID, err)
		} else {
			// Store metadata in repository
			//w.logger.Info("Updating database metadata: %v", metadata)
			w.repository.UpdateDatabaseMetadata(ctx, &config.DatabaseMetadata{
				DatabaseID:  clientID,
				Version:     metadata.Version,
				SizeBytes:   metadata.SizeBytes,
				TablesCount: metadata.TablesCount,
			})
		}
	}

	if connectionErrors > 0 {
		w.logger.Warn("Unable to connect to %d out of %d client databases (expected for unreachable databases)", connectionErrors, len(dbConfigs))
	} else {
		w.logger.Info("Successfully processed all %d database configurations", len(dbConfigs))
	}

	return nil
}

func (w *ConfigWatcher) processInstanceConfigs(ctx context.Context, nodeID string, dbManager *database.DatabaseManager) error {
	// Check if context is cancelled before starting
	if ctx.Err() != nil {
		return ctx.Err()
	}

	w.logger.Info("Processing instance configs")
	// Load all enabled instance configurations
	instanceConfigs, err := w.repository.GetAllInstanceConfigs(ctx, nodeID)
	if err != nil {
		w.logger.Error("Failed to load instance configurations: %v", err)
		return err
	}

	w.logger.Info("Found %d enabled instance configurations to process", len(instanceConfigs))

	connectionErrors := 0
	// Establish connections for new instance configurations
	for _, instConfig := range instanceConfigs {
		// Check if context is cancelled before processing each config
		if ctx.Err() != nil {
			return ctx.Err()
		}

		clientID := instConfig.InstanceID

		// Skip if already connected
		if _, err := dbManager.GetInstanceClient(clientID); err == nil {
			w.logger.Debug("Instance %s already connected, skipping", clientID)
			continue
		}

		// Convert unified config to connection config
		dbConnConfig := instConfig.ToConnectionConfig()

		// Attempt to establish new connection
		w.logger.Info("Establishing new connection for instance: %s", clientID)
		client, err := dbManager.ConnectInstance(dbConnConfig)
		if err != nil {
			// Client instance connection failures are warnings, not errors
			// (detailed logging is already handled by DatabaseManager's unified logging)
			w.logger.Warn("Client instance connection failed: %s", clientID)
			w.repository.UpdateInstanceConnectionStatus(ctx, clientID, false, fmt.Sprintf("Connection failed: %v", err))
			connectionErrors++
			continue
		}

		// Mark as connected if successful
		atomic.StoreInt32(&client.IsConnected, 1)
		w.repository.UpdateInstanceConnectionStatus(ctx, clientID, true, "Connected successfully")
		w.logger.Info("Connected to instance %s", clientID)

		collector := database.NewInstanceMetadataCollector(client)
		metadata, err := collector.CollectMetadata(ctx, clientID)
		if err != nil {
			w.logger.Error("Failed to collect instance metadata for %s: %v", clientID, err)
		} else {
			// Store metadata in repository
			//w.logger.Info("Updating instance metadata: %v", metadata)
			w.repository.UpdateInstanceMetadata(ctx, &config.InstanceMetadata{
				InstanceID:       clientID,
				Version:          metadata.Version,
				UptimeSeconds:    metadata.UptimeSeconds,
				TotalDatabases:   metadata.TotalDatabases,
				TotalConnections: metadata.TotalConnections,
				MaxConnections:   metadata.MaxConnections,
			})
		}
	}

	if connectionErrors > 0 {
		w.logger.Warn("Unable to connect to %d out of %d client instances (expected for unreachable instances)", connectionErrors, len(instanceConfigs))
	} else {
		w.logger.Info("Successfully processed all %d instance configurations", len(instanceConfigs))
	}

	return nil
}

func (w *ConfigWatcher) checkAllConnectionsHealth(ctx context.Context, dbManager *database.DatabaseManager) error {
	// Check if context is cancelled before starting
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Check health of all connected databases
	for _, clientID := range dbManager.GetAllDatabaseClientIDs() {
		// Check if context is cancelled before processing each database
		if ctx.Err() != nil {
			return ctx.Err()
		}

		w.logger.Info("Checking health for database client: %s", clientID)

		client, err := dbManager.GetDatabaseClient(clientID)
		if err != nil {
			w.logger.Error("Failed to get database client %s: %v", clientID, err)
			continue
		}

		// Check if client is still connected
		if atomic.LoadInt32(&client.IsConnected) == 0 {
			w.logger.Error("Database client %s is not connected", clientID)
			// Clean up disconnected client from DatabaseManager
			w.logger.Info("Removing disconnected database client %s from DatabaseManager", clientID)
			dbManager.DisconnectDatabase(clientID)
			continue
		}

		// Check if database still exists in repository before updating status
		_, err = w.repository.GetDatabaseConfigByID(ctx, clientID)
		if err != nil {
			w.logger.Info("Database %s no longer exists in repository, cleaning up from DatabaseManager", clientID)
			// Database was removed from repository but still exists in DatabaseManager
			// This can happen when database is disconnected but not properly cleaned up
			err = dbManager.DisconnectDatabase(clientID)
			if err != nil {
				w.logger.Error("Failed to cleanup orphaned database %s from DatabaseManager: %v", clientID, err)
			} else {
				w.logger.Info("Successfully cleaned up orphaned database %s from DatabaseManager", clientID)
			}
			continue
		}

		// Update connection status
		if err := w.repository.UpdateDatabaseConnectionStatus(ctx, clientID, true, "Connection healthy"); err != nil {
			w.logger.Error("Failed to update database connection status: %v", err)
			continue
		}

		// Collect and update metadata
		collector := database.NewDatabaseMetadataCollector(client)
		metadata, err := collector.CollectMetadata(ctx, client.DatabaseID)
		if err != nil {
			w.logger.Error("Failed to collect database metadata: %v", err)
		} else {
			// Store metadata in repository
			//w.logger.Info("Updating database metadata: %v", metadata)
			w.repository.UpdateDatabaseMetadata(ctx, &config.DatabaseMetadata{
				DatabaseID:  client.DatabaseID,
				Version:     metadata.Version,
				SizeBytes:   metadata.SizeBytes,
				TablesCount: metadata.TablesCount,
			})
		}
	}

	// Check if context is cancelled before processing instances
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Check health of all connected instances
	for _, clientID := range dbManager.GetAllInstanceClientIDs() {
		// Check if context is cancelled before processing each instance
		if ctx.Err() != nil {
			return ctx.Err()
		}

		w.logger.Info("Checking health for instance client: %s", clientID)

		client, err := dbManager.GetInstanceClient(clientID)
		if err != nil {
			w.logger.Error("Failed to get instance client %s: %v", clientID, err)
			continue
		}

		// Check if client is still connected
		if atomic.LoadInt32(&client.IsConnected) == 0 {
			w.logger.Error("Instance client %s is not connected", clientID)
			// Clean up disconnected client from DatabaseManager
			w.logger.Info("Removing disconnected instance client %s from DatabaseManager", clientID)
			dbManager.DisconnectInstance(clientID)
			continue
		}

		// Check if instance still exists in repository before updating status
		_, err = w.repository.GetInstanceConfigByID(ctx, clientID)
		if err != nil {
			w.logger.Info("Instance %s no longer exists in repository, cleaning up from DatabaseManager", clientID)
			// Instance was removed from repository but still exists in DatabaseManager
			// This can happen when instance is disconnected but not properly cleaned up
			err = dbManager.DisconnectInstance(clientID)
			if err != nil {
				w.logger.Error("Failed to cleanup orphaned instance %s from DatabaseManager: %v", clientID, err)
			} else {
				w.logger.Info("Successfully cleaned up orphaned instance %s from DatabaseManager", clientID)
			}
			continue
		}

		// Update connection status
		if err := w.repository.UpdateInstanceConnectionStatus(ctx, clientID, true, "Connection healthy"); err != nil {
			w.logger.Error("Failed to update instance connection status: %v", err)
			continue
		}

		// Collect and update metadata
		collector := database.NewInstanceMetadataCollector(client)
		metadata, err := collector.CollectMetadata(ctx, client.InstanceID)
		if err != nil {
			w.logger.Error("Failed to collect instance metadata: %v", err)
		} else {
			// Store metadata in repository
			//w.logger.Info("Updating instance metadata: %v", metadata)
			w.repository.UpdateInstanceMetadata(ctx, &config.InstanceMetadata{
				InstanceID:       client.InstanceID,
				Version:          metadata.Version,
				UptimeSeconds:    metadata.UptimeSeconds,
				TotalDatabases:   metadata.TotalDatabases,
				TotalConnections: metadata.TotalConnections,
				MaxConnections:   metadata.MaxConnections,
			})
		}
	}

	return nil
}

func (w *ConfigWatcher) InitialConnect(ctx context.Context) error {
	w.logger.Info("Performing initial connections...")

	// Ensure we have a valid repository before proceeding
	if w.repository == nil {
		w.logger.Error("Repository is not initialized")
		return fmt.Errorf("repository is not initialized")
	}

	nodeID := w.state.GetNodeID()
	if nodeID == "" {
		w.logger.Error("Node ID is not set")
		return fmt.Errorf("node ID is not set")
	}

	w.logger.Info("Using node ID: %s", nodeID)

	// Try to perform initial connection health check with retries
	var lastErr error
	maxAttempts := 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		w.logger.Info("Initial connection attempt %d/%d", attempt, maxAttempts)

		err := w.checkConnectionHealth(ctx)
		if err == nil {
			w.logger.Info("Initial connections completed successfully")
			return nil
		}

		lastErr = err
		w.logger.Error("Initial connection attempt %d failed: %v", attempt, err)

		if attempt < maxAttempts {
			w.logger.Info("Retrying in 2 seconds...")
			time.Sleep(2 * time.Second)
		}
	}

	w.logger.Error("All initial connection attempts failed. Last error: %v", lastErr)
	// Don't return error to prevent service startup failure
	// The periodic watcher will continue to attempt connections
	return nil
}
