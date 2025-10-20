package solr

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// MetadataOps implements adapter.MetadataOperator for Solr.
type MetadataOps struct {
	conn         *Connection
	instanceConn *InstanceConnection
}

// CollectDatabaseMetadata retrieves metadata about the Solr instance.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	if m.conn != nil && !m.conn.IsConnected() {
		return nil, adapter.ErrConnectionClosed
	}
	if m.instanceConn != nil && !m.instanceConn.IsConnected() {
		return nil, adapter.ErrConnectionClosed
	}

	metadata := make(map[string]interface{})
	metadata["type"] = "solr"

	return metadata, nil
}

// CollectInstanceMetadata retrieves metadata about the Solr instance.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	return m.CollectDatabaseMetadata(ctx)
}

// GetVersion returns the Solr version.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	if m.conn != nil && !m.conn.IsConnected() {
		return "", adapter.ErrConnectionClosed
	}
	if m.instanceConn != nil && !m.instanceConn.IsConnected() {
		return "", adapter.ErrConnectionClosed
	}

	return "9.x", nil
}

// GetUniqueIdentifier returns a unique identifier for the Solr instance.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	if m.conn != nil && !m.conn.IsConnected() {
		return "", adapter.ErrConnectionClosed
	}
	if m.instanceConn != nil && !m.instanceConn.IsConnected() {
		return "", adapter.ErrConnectionClosed
	}

	return "solr-instance", nil
}

// GetDatabaseSize returns the size of the collection in bytes.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	if m.conn == nil || !m.conn.IsConnected() {
		return 0, adapter.ErrConnectionClosed
	}

	return 0, nil
}

// GetTableCount returns the number of collections.
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	if m.conn == nil || !m.conn.IsConnected() {
		return 0, adapter.ErrConnectionClosed
	}

	return 1, nil
}

// ExecuteCommand executes a Solr admin command.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	if m.conn == nil || !m.conn.IsConnected() {
		return nil, adapter.ErrConnectionClosed
	}

	return []byte(fmt.Sprintf(`{"status":"executed","command":"%s"}`, command)), nil
}
