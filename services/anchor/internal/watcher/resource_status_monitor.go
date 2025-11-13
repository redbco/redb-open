package watcher

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/anchor/internal/resources"
)

// ResourceStatusMonitor monitors database connections and updates resource status
type ResourceStatusMonitor struct {
	pool          *pgxpool.Pool
	resourceRepo  *resources.Repository
	logger        *logger.Logger
	checkInterval time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
	doneCh        chan struct{}
}

// NewResourceStatusMonitor creates a new resource status monitor
func NewResourceStatusMonitor(pool *pgxpool.Pool, resourceRepo *resources.Repository, logger *logger.Logger) *ResourceStatusMonitor {
	return &ResourceStatusMonitor{
		pool:          pool,
		resourceRepo:  resourceRepo,
		logger:        logger,
		checkInterval: 30 * time.Second, // Check every 30 seconds
		doneCh:        make(chan struct{}),
	}
}

// Start begins monitoring resource status
func (m *ResourceStatusMonitor) Start(ctx context.Context) error {
	m.ctx, m.cancel = context.WithCancel(ctx)

	if m.logger != nil {
		m.logger.Info("Starting resource status monitor")
	}

	go m.monitorLoop()

	return nil
}

// Stop stops the resource status monitor
func (m *ResourceStatusMonitor) Stop(ctx context.Context) error {
	if m.logger != nil {
		m.logger.Info("Stopping resource status monitor")
	}

	if m.cancel != nil {
		m.cancel()
	}

	// Wait for monitor loop to finish or timeout
	select {
	case <-m.doneCh:
		if m.logger != nil {
			m.logger.Info("Resource status monitor stopped gracefully")
		}
	case <-ctx.Done():
		if m.logger != nil {
			m.logger.Warn("Resource status monitor stop timed out")
		}
	}

	return nil
}

// monitorLoop continuously checks database status and updates resources
func (m *ResourceStatusMonitor) monitorLoop() {
	defer close(m.doneCh)

	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	// Do an initial check immediately
	if err := m.checkAndUpdateResources(); err != nil {
		if m.logger != nil {
			m.logger.Errorf("Failed to check resource status: %v", err)
		}
	}

	for {
		select {
		case <-m.ctx.Done():
			if m.logger != nil {
				m.logger.Info("Resource status monitor loop exiting")
			}
			return
		case <-ticker.C:
			if err := m.checkAndUpdateResources(); err != nil {
				if m.logger != nil {
					m.logger.Errorf("Failed to check resource status: %v", err)
				}
			}
		}
	}
}

// checkAndUpdateResources checks all databases and updates resource online status
func (m *ResourceStatusMonitor) checkAndUpdateResources() error {
	if m.logger != nil {
		m.logger.Debug("Checking resource status")
	}

	// Query all databases with their connection status
	query := `
		SELECT d.database_id, d.database_enabled, d.status,
		       CASE 
		           WHEN d.database_enabled = true AND d.status = 'STATUS_CONNECTED' THEN true
		           ELSE false
		       END as is_online
		FROM databases d
	`

	rows, err := m.pool.Query(m.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query databases: %w", err)
	}
	defer rows.Close()

	var databaseStatuses []struct {
		DatabaseID string
		IsOnline   bool
	}

	for rows.Next() {
		var databaseID string
		var enabled bool
		var status string
		var isOnline bool

		if err := rows.Scan(&databaseID, &enabled, &status, &isOnline); err != nil {
			if m.logger != nil {
				m.logger.Warnf("Failed to scan database row: %v", err)
			}
			continue
		}

		databaseStatuses = append(databaseStatuses, struct {
			DatabaseID string
			IsOnline   bool
		}{
			DatabaseID: databaseID,
			IsOnline:   isOnline,
		})
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating database rows: %w", err)
	}

	// Update resource status for each database
	for _, dbStatus := range databaseStatuses {
		if err := m.updateDatabaseResources(dbStatus.DatabaseID, dbStatus.IsOnline); err != nil {
			if m.logger != nil {
				m.logger.Warnf("Failed to update resources for database %s: %v", dbStatus.DatabaseID, err)
			}
		}
	}

	// Also check integration status (webhooks, streams, MCP servers)
	if err := m.updateIntegrationResources(); err != nil {
		if m.logger != nil {
			m.logger.Warnf("Failed to update integration resources: %v", err)
		}
	}

	if m.logger != nil {
		m.logger.Debugf("Resource status check completed for %d databases", len(databaseStatuses))
	}

	return nil
}

// updateDatabaseResources updates the online status of all resources for a database
func (m *ResourceStatusMonitor) updateDatabaseResources(databaseID string, online bool) error {
	// Update all containers for this database
	query := `
		UPDATE resource_containers
		SET online = $1, 
		    last_seen = CURRENT_TIMESTAMP,
		    updated = CURRENT_TIMESTAMP
		WHERE database_id = $2
		  AND online != $1
	`

	result, err := m.pool.Exec(m.ctx, query, online, databaseID)
	if err != nil {
		return fmt.Errorf("failed to update container status: %w", err)
	}

	rowsAffected := result.RowsAffected()

	// Update all items for containers of this database
	itemQuery := `
		UPDATE resource_items
		SET online = $1,
		    updated = CURRENT_TIMESTAMP
		WHERE container_id IN (
		    SELECT container_id 
		    FROM resource_containers 
		    WHERE database_id = $2
		)
		AND online != $1
	`

	itemResult, err := m.pool.Exec(m.ctx, itemQuery, online, databaseID)
	if err != nil {
		return fmt.Errorf("failed to update item status: %w", err)
	}

	itemsAffected := itemResult.RowsAffected()

	if rowsAffected > 0 || itemsAffected > 0 {
		status := "online"
		if !online {
			status = "offline"
		}
		if m.logger != nil {
			m.logger.Infof("Updated database %s resources to %s: %d containers, %d items",
				databaseID, status, rowsAffected, itemsAffected)
		}
	}

	return nil
}

// updateIntegrationResources updates the status of integration resources (webhooks, MCP, streams)
func (m *ResourceStatusMonitor) updateIntegrationResources() error {
	// Check webhook integrations
	webhookQuery := `
		UPDATE resource_containers rc
		SET online = CASE 
		        WHEN i.status = 'STATUS_ACTIVE' THEN true
		        ELSE false
		    END,
		    last_seen = CURRENT_TIMESTAMP,
		    updated = CURRENT_TIMESTAMP
		FROM integrations i
		WHERE rc.integration_id = i.integration_id
		  AND rc.protocol = 'webhook'
		  AND rc.online != CASE 
		        WHEN i.status = 'STATUS_ACTIVE' THEN true
		        ELSE false
		    END
	`

	if _, err := m.pool.Exec(m.ctx, webhookQuery); err != nil {
		return fmt.Errorf("failed to update webhook resources: %w", err)
	}

	// Check MCP server resources
	mcpQuery := `
		UPDATE resource_containers rc
		SET online = CASE 
		        WHEN ms.mcpserver_enabled = true AND ms.status = 'STATUS_ACTIVE' THEN true
		        ELSE false
		    END,
		    last_seen = CURRENT_TIMESTAMP,
		    updated = CURRENT_TIMESTAMP
		FROM mcpservers ms
		WHERE rc.mcpserver_id = ms.mcpserver_id
		  AND rc.protocol = 'mcp'
		  AND rc.online != CASE 
		        WHEN ms.mcpserver_enabled = true AND ms.status = 'STATUS_ACTIVE' THEN true
		        ELSE false
		    END
	`

	if _, err := m.pool.Exec(m.ctx, mcpQuery); err != nil {
		return fmt.Errorf("failed to update MCP resources: %w", err)
	}

	// Check stream integrations
	streamQuery := `
		UPDATE resource_containers rc
		SET online = CASE 
		        WHEN i.status = 'STATUS_ACTIVE' THEN true
		        ELSE false
		    END,
		    last_seen = CURRENT_TIMESTAMP,
		    updated = CURRENT_TIMESTAMP
		FROM integrations i
		WHERE rc.integration_id = i.integration_id
		  AND rc.protocol = 'stream'
		  AND rc.online != CASE 
		        WHEN i.status = 'STATUS_ACTIVE' THEN true
		        ELSE false
		    END
	`

	if _, err := m.pool.Exec(m.ctx, streamQuery); err != nil {
		return fmt.Errorf("failed to update stream resources: %w", err)
	}

	// Update items for affected containers
	itemQuery := `
		UPDATE resource_items ri
		SET online = rc.online,
		    updated = CURRENT_TIMESTAMP
		FROM resource_containers rc
		WHERE ri.container_id = rc.container_id
		  AND ri.online != rc.online
		  AND rc.protocol IN ('webhook', 'mcp', 'stream')
	`

	if _, err := m.pool.Exec(m.ctx, itemQuery); err != nil {
		return fmt.Errorf("failed to update integration item status: %w", err)
	}

	return nil
}

// SetCheckInterval sets the check interval for the monitor
func (m *ResourceStatusMonitor) SetCheckInterval(interval time.Duration) {
	m.checkInterval = interval
}

// GetCheckInterval returns the current check interval
func (m *ResourceStatusMonitor) GetCheckInterval() time.Duration {
	return m.checkInterval
}
