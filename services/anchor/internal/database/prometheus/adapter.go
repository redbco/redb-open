package prometheus

import (
	"context"
	"sync/atomic"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// Adapter implements adapter.DatabaseAdapter for Prometheus.
type Adapter struct{}

// NewAdapter creates a new Prometheus adapter instance.
func NewAdapter() adapter.DatabaseAdapter {
	return &Adapter{}
}

// Type returns the database type identifier.
func (a *Adapter) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.Prometheus
}

// Capabilities returns the capability metadata.
func (a *Adapter) Capabilities() dbcapabilities.Capability {
	return dbcapabilities.MustGet(dbcapabilities.Prometheus)
}

// Connect establishes a connection to Prometheus.
func (a *Adapter) Connect(ctx context.Context, config adapter.ConnectionConfig) (adapter.Connection, error) {
	// Create Prometheus client
	client, err := NewPrometheusClient(ctx, config)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.Prometheus,
			config.Host,
			config.Port,
			err,
		)
	}

	conn := &Connection{
		id:        config.DatabaseID,
		client:    client,
		config:    config,
		adapter:   a,
		connected: 1,
	}

	return conn, nil
}

// ConnectInstance establishes an instance-level connection to Prometheus.
func (a *Adapter) ConnectInstance(ctx context.Context, config adapter.InstanceConfig) (adapter.InstanceConnection, error) {
	// For Prometheus, instance connection represents access to the server
	client, err := NewPrometheusClientFromInstance(ctx, config)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.Prometheus,
			config.Host,
			config.Port,
			err,
		)
	}

	conn := &InstanceConnection{
		id:        config.InstanceID,
		client:    client,
		config:    config,
		adapter:   a,
		connected: 1,
	}

	return conn, nil
}

// Connection implements adapter.Connection for Prometheus.
type Connection struct {
	id        string
	client    *PrometheusClient
	config    adapter.ConnectionConfig
	adapter   *Adapter
	connected int32
}

// ID returns the connection identifier.
func (c *Connection) ID() string {
	return c.id
}

// Type returns the database type.
func (c *Connection) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.Prometheus
}

// IsConnected returns whether the connection is active.
func (c *Connection) IsConnected() bool {
	return atomic.LoadInt32(&c.connected) == 1
}

// Ping tests the connection.
func (c *Connection) Ping(ctx context.Context) error {
	if !c.IsConnected() {
		return adapter.ErrConnectionClosed
	}
	return c.client.Ping(ctx)
}

// Close closes the connection.
func (c *Connection) Close() error {
	if !atomic.CompareAndSwapInt32(&c.connected, 1, 0) {
		return adapter.ErrConnectionClosed
	}
	// Prometheus HTTP client doesn't need explicit closing
	return nil
}

// SchemaOperations returns the schema operator.
func (c *Connection) SchemaOperations() adapter.SchemaOperator {
	return &SchemaOps{conn: c}
}

// DataOperations returns the data operator.
func (c *Connection) DataOperations() adapter.DataOperator {
	return &DataOps{conn: c}
}

// ReplicationOperations returns the replication operator.
func (c *Connection) ReplicationOperations() adapter.ReplicationOperator {
	return &ReplicationOps{conn: c}
}

// MetadataOperations returns the metadata operator.
func (c *Connection) MetadataOperations() adapter.MetadataOperator {
	return &MetadataOps{conn: c}
}

// Raw returns the underlying Prometheus client.
func (c *Connection) Raw() interface{} {
	return c.client
}

// Config returns the connection configuration.
func (c *Connection) Config() adapter.ConnectionConfig {
	return c.config
}

// Adapter returns the database adapter.
func (c *Connection) Adapter() adapter.DatabaseAdapter {
	return c.adapter
}

// InstanceConnection implements adapter.InstanceConnection for Prometheus.
type InstanceConnection struct {
	id        string
	client    *PrometheusClient
	config    adapter.InstanceConfig
	adapter   *Adapter
	connected int32
}

// ID returns the instance connection identifier.
func (ic *InstanceConnection) ID() string {
	return ic.id
}

// Type returns the database type.
func (ic *InstanceConnection) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.Prometheus
}

// IsConnected returns whether the connection is active.
func (ic *InstanceConnection) IsConnected() bool {
	return atomic.LoadInt32(&ic.connected) == 1
}

// Ping tests the connection.
func (ic *InstanceConnection) Ping(ctx context.Context) error {
	if !ic.IsConnected() {
		return adapter.ErrConnectionClosed
	}
	return ic.client.Ping(ctx)
}

// Close closes the connection.
func (ic *InstanceConnection) Close() error {
	if !atomic.CompareAndSwapInt32(&ic.connected, 1, 0) {
		return adapter.ErrConnectionClosed
	}
	return nil
}

// ListDatabases lists all "databases" (Prometheus doesn't have databases, returns metrics namespaces).
func (ic *InstanceConnection) ListDatabases(ctx context.Context) ([]string, error) {
	if !ic.IsConnected() {
		return nil, adapter.ErrConnectionClosed
	}
	// Prometheus doesn't have databases; return a default
	return []string{"metrics"}, nil
}

// CreateDatabase is not supported for Prometheus.
func (ic *InstanceConnection) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	return adapter.NewUnsupportedOperationError(
		dbcapabilities.Prometheus,
		"create_database",
		"Prometheus does not support database creation",
	)
}

// DropDatabase is not supported for Prometheus.
func (ic *InstanceConnection) DropDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	return adapter.NewUnsupportedOperationError(
		dbcapabilities.Prometheus,
		"drop_database",
		"Prometheus does not support database deletion",
	)
}

// MetadataOperations returns the metadata operator.
func (ic *InstanceConnection) MetadataOperations() adapter.MetadataOperator {
	return &MetadataOps{instanceConn: ic}
}

// Raw returns the underlying Prometheus client.
func (ic *InstanceConnection) Raw() interface{} {
	return ic.client
}

// Config returns the instance configuration.
func (ic *InstanceConnection) Config() adapter.InstanceConfig {
	return ic.config
}

// Adapter returns the database adapter.
func (ic *InstanceConnection) Adapter() adapter.DatabaseAdapter {
	return ic.adapter
}
