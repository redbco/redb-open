package minio

import (
	"context"
	"sync/atomic"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// Adapter implements adapter.DatabaseAdapter for MinIO.
type Adapter struct{}

// NewAdapter creates a new MinIO adapter instance.
func NewAdapter() adapter.DatabaseAdapter {
	return &Adapter{}
}

// Type returns the database type identifier.
func (a *Adapter) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.MinIO
}

// Capabilities returns the capability metadata.
func (a *Adapter) Capabilities() dbcapabilities.Capability {
	return dbcapabilities.MustGet(dbcapabilities.MinIO)
}

// Connect establishes a connection to MinIO.
func (a *Adapter) Connect(ctx context.Context, config adapter.ConnectionConfig) (adapter.Connection, error) {
	// Create MinIO client
	client, err := NewMinIOClient(ctx, config)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.MinIO,
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

// ConnectInstance establishes an instance-level connection to MinIO.
func (a *Adapter) ConnectInstance(ctx context.Context, config adapter.InstanceConfig) (adapter.InstanceConnection, error) {
	// For MinIO, instance connection represents access to all buckets
	client, err := NewMinIOClientFromInstance(ctx, config)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.MinIO,
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

// Connection implements adapter.Connection for MinIO.
type Connection struct {
	id        string
	client    *MinIOClient
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
	return dbcapabilities.MinIO
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
	// MinIO client doesn't need explicit closing
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

// Raw returns the underlying MinIO client.
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

// InstanceConnection implements adapter.InstanceConnection for MinIO.
type InstanceConnection struct {
	id        string
	client    *MinIOClient
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
	return dbcapabilities.MinIO
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

// ListDatabases lists all MinIO buckets (buckets are treated as databases).
func (ic *InstanceConnection) ListDatabases(ctx context.Context) ([]string, error) {
	if !ic.IsConnected() {
		return nil, adapter.ErrConnectionClosed
	}
	return ic.client.ListBuckets(ctx)
}

// CreateDatabase creates a new MinIO bucket.
func (ic *InstanceConnection) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if !ic.IsConnected() {
		return adapter.ErrConnectionClosed
	}
	return ic.client.CreateBucket(ctx, name, options)
}

// DropDatabase deletes a MinIO bucket.
func (ic *InstanceConnection) DropDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if !ic.IsConnected() {
		return adapter.ErrConnectionClosed
	}
	return ic.client.DeleteBucket(ctx, name)
}

// MetadataOperations returns the metadata operator.
func (ic *InstanceConnection) MetadataOperations() adapter.MetadataOperator {
	return &MetadataOps{instanceConn: ic}
}

// Raw returns the underlying MinIO client.
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
