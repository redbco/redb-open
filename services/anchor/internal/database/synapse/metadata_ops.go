package synapse

import (
	"context"
	"database/sql"
	"fmt"
)

// MetadataOps implements metadata operations for Synapse.
type MetadataOps struct {
	conn         *Connection
	instanceConn *InstanceConnection
}

// CollectDatabaseMetadata collects metadata about the Synapse database.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	if m.conn == nil {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})
	metadata["database_name"] = m.conn.client.GetDatabaseName()
	metadata["database_type"] = "synapse"

	// Get database size
	size, err := m.GetDatabaseSize(ctx)
	if err == nil {
		metadata["database_size"] = size
	}

	// Get table count
	count, err := m.GetTableCount(ctx)
	if err == nil {
		metadata["table_count"] = count
	}

	return metadata, nil
}

// CollectInstanceMetadata collects metadata about the Synapse workspace.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	var client *SynapseClient

	if m.conn != nil {
		client = m.conn.client
	} else if m.instanceConn != nil {
		client = m.instanceConn.client
	} else {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})
	metadata["database_type"] = "synapse"

	// List databases
	databases, err := client.ListDatabases(ctx)
	if err == nil {
		metadata["database_count"] = len(databases)
		metadata["databases"] = databases
	}

	// Get version
	version, err := m.GetVersion(ctx)
	if err == nil {
		metadata["version"] = version
	}

	return metadata, nil
}

// GetVersion returns the Synapse version.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	var client *SynapseClient

	if m.conn != nil {
		client = m.conn.client
	} else if m.instanceConn != nil {
		client = m.instanceConn.client
	} else {
		return "", fmt.Errorf("no connection available")
	}

	var version string
	query := "SELECT @@VERSION"
	err := client.DB().QueryRowContext(ctx, query).Scan(&version)
	if err != nil {
		return "", fmt.Errorf("failed to get version: %w", err)
	}

	return version, nil
}

// GetUniqueIdentifier returns the workspace identifier.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	if m.conn != nil {
		return fmt.Sprintf("synapse::%s", m.conn.client.GetDatabaseName()), nil
	}

	if m.instanceConn != nil {
		return "synapse::workspace", nil
	}

	return "synapse::unknown", nil
}

// GetDatabaseSize returns the size of the database in bytes.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	if m.conn == nil {
		return 0, fmt.Errorf("no connection available")
	}

	query := `
		SELECT SUM(reserved_page_count) * 8 * 1024 as total_size
		FROM sys.dm_db_partition_stats
	`

	var size sql.NullInt64
	err := m.conn.client.DB().QueryRowContext(ctx, query).Scan(&size)
	if err != nil {
		return 0, fmt.Errorf("failed to get database size: %w", err)
	}

	if size.Valid {
		return size.Int64, nil
	}

	return 0, nil
}

// GetTableCount returns the number of tables in the database.
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	if m.conn == nil {
		return 0, fmt.Errorf("no connection available")
	}

	query := `
		SELECT COUNT(*)
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE'
	`

	var count int
	err := m.conn.client.DB().QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count tables: %w", err)
	}

	return count, nil
}

// ExecuteCommand executes a Synapse SQL command.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	if m.conn == nil {
		return nil, fmt.Errorf("no connection available")
	}

	result, err := m.conn.client.DB().ExecContext(ctx, command)
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %w", err)
	}

	rows, _ := result.RowsAffected()
	return []byte(fmt.Sprintf("Command executed successfully. Rows affected: %d", rows)), nil
}
