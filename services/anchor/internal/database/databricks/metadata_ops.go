package databricks

import (
	"context"
	"database/sql"
	"fmt"
)

// MetadataOps implements metadata operations for Databricks.
type MetadataOps struct {
	conn         *Connection
	instanceConn *InstanceConnection
}

// CollectDatabaseMetadata collects metadata about the Databricks database.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	if m.conn == nil {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})
	metadata["database_name"] = m.conn.client.GetDatabaseName()
	metadata["database_type"] = "databricks"

	// Get table count
	count, err := m.GetTableCount(ctx)
	if err == nil {
		metadata["table_count"] = count
	}

	return metadata, nil
}

// CollectInstanceMetadata collects metadata about the Databricks workspace.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	var client *DatabricksClient

	if m.conn != nil {
		client = m.conn.client
	} else if m.instanceConn != nil {
		client = m.instanceConn.client
	} else {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})
	metadata["database_type"] = "databricks"

	// List databases
	databases, err := client.ListDatabases(ctx)
	if err == nil {
		metadata["database_count"] = len(databases)
		metadata["databases"] = databases
	}

	return metadata, nil
}

// GetVersion returns the Databricks runtime version.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	var client *DatabricksClient

	if m.conn != nil {
		client = m.conn.client
	} else if m.instanceConn != nil {
		client = m.instanceConn.client
	} else {
		return "", fmt.Errorf("no connection available")
	}

	var version string
	query := "SELECT version()"
	err := client.DB().QueryRowContext(ctx, query).Scan(&version)
	if err != nil {
		return "Databricks Runtime", nil // Fallback if version query fails
	}

	return version, nil
}

// GetUniqueIdentifier returns the workspace identifier.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	if m.conn != nil {
		return fmt.Sprintf("databricks::%s", m.conn.client.GetDatabaseName()), nil
	}

	if m.instanceConn != nil {
		return "databricks::workspace", nil
	}

	return "databricks::unknown", nil
}

// GetDatabaseSize returns the size of the database in bytes.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	if m.conn == nil {
		return 0, fmt.Errorf("no connection available")
	}

	// Databricks doesn't provide easy size queries, return 0
	return 0, fmt.Errorf("GetDatabaseSize not directly supported for Databricks")
}

// GetTableCount returns the number of tables in the database.
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	if m.conn == nil {
		return 0, fmt.Errorf("no connection available")
	}

	query := "SHOW TABLES"

	rows, err := m.conn.client.DB().QueryContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to count tables: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var database, tableName, isTemporary sql.NullString
		if err := rows.Scan(&database, &tableName, &isTemporary); err != nil {
			continue
		}
		count++
	}

	return count, nil
}

// ExecuteCommand executes a Databricks SQL command.
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
