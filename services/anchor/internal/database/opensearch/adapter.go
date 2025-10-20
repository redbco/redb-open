package opensearch

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// Adapter implements adapter.DatabaseAdapter for OpenSearch.
type Adapter struct{}

// NewAdapter creates a new OpenSearch adapter instance.
func NewAdapter() adapter.DatabaseAdapter {
	return &Adapter{}
}

// Type returns the database type identifier.
func (a *Adapter) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.OpenSearch
}

// Capabilities returns the capability metadata.
func (a *Adapter) Capabilities() dbcapabilities.Capability {
	return dbcapabilities.MustGet(dbcapabilities.OpenSearch)
}

// Connect establishes a connection to OpenSearch.
func (a *Adapter) Connect(ctx context.Context, config adapter.ConnectionConfig) (adapter.Connection, error) {
	// Build OpenSearch configuration
	cfg := opensearch.Config{
		Addresses: []string{
			fmt.Sprintf("https://%s:%d", config.Host, config.Port),
		},
		Username: config.Username,
		Password: config.Password,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // In production, properly validate certificates
			},
		},
	}

	// Create client
	client, err := opensearch.NewClient(cfg)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.OpenSearch,
			config.Host,
			config.Port,
			err,
		)
	}

	// Test connection
	res, err := client.Info()
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.OpenSearch,
			config.Host,
			config.Port,
			err,
		)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, adapter.NewConnectionError(
			dbcapabilities.OpenSearch,
			config.Host,
			config.Port,
			fmt.Errorf("connection test failed: %s", res.Status()),
		)
	}

	conn := &Connection{
		id:        config.DatabaseID,
		client:    client,
		config:    config,
		indexName: config.DatabaseName, // In OpenSearch, database name maps to index name
		adapter:   a,
		connected: 1,
	}

	return conn, nil
}

// ConnectInstance establishes an instance-level connection to OpenSearch.
func (a *Adapter) ConnectInstance(ctx context.Context, config adapter.InstanceConfig) (adapter.InstanceConnection, error) {
	// Build OpenSearch configuration
	cfg := opensearch.Config{
		Addresses: []string{
			fmt.Sprintf("https://%s:%d", config.Host, config.Port),
		},
		Username: config.Username,
		Password: config.Password,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	// Create client
	client, err := opensearch.NewClient(cfg)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.OpenSearch,
			config.Host,
			config.Port,
			err,
		)
	}

	// Test connection
	res, err := client.Info()
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.OpenSearch,
			config.Host,
			config.Port,
			err,
		)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, adapter.NewConnectionError(
			dbcapabilities.OpenSearch,
			config.Host,
			config.Port,
			fmt.Errorf("connection test failed: %s", res.Status()),
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

// Connection implements adapter.Connection for OpenSearch.
type Connection struct {
	id        string
	client    *opensearch.Client
	config    adapter.ConnectionConfig
	indexName string
	adapter   *Adapter
	connected int32
}

// ID returns the connection identifier.
func (c *Connection) ID() string {
	return c.id
}

// Type returns the database type.
func (c *Connection) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.OpenSearch
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

	res, err := c.client.Ping()
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("ping failed: %s", res.Status())
	}

	return nil
}

// Close closes the connection.
func (c *Connection) Close() error {
	atomic.StoreInt32(&c.connected, 0)
	// OpenSearch client doesn't require explicit close
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

// Raw returns the underlying OpenSearch client.
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

// InstanceConnection implements adapter.InstanceConnection for OpenSearch.
type InstanceConnection struct {
	id        string
	client    *opensearch.Client
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
	return dbcapabilities.OpenSearch
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

	res, err := ic.client.Ping()
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("ping failed: %s", res.Status())
	}

	return nil
}

// Close closes the connection.
func (ic *InstanceConnection) Close() error {
	atomic.StoreInt32(&ic.connected, 0)
	return nil
}

// ListDatabases lists all indexes in the OpenSearch cluster (indexes act as "databases").
func (ic *InstanceConnection) ListDatabases(ctx context.Context) ([]string, error) {
	if !ic.IsConnected() {
		return nil, adapter.ErrConnectionClosed
	}

	res, err := ic.client.Cat.Indices(
		ic.client.Cat.Indices.WithContext(ctx),
		ic.client.Cat.Indices.WithFormat("json"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list indices: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("failed to list indices: %s", res.Status())
	}

	var indices []struct {
		Index string `json:"index"`
	}

	if err := json.NewDecoder(res.Body).Decode(&indices); err != nil {
		return nil, err
	}

	var databases []string
	for _, idx := range indices {
		// Skip system indices (starting with .)
		if len(idx.Index) > 0 && idx.Index[0] != '.' {
			databases = append(databases, idx.Index)
		}
	}

	return databases, nil
}

// CreateDatabase creates a new index in OpenSearch.
func (ic *InstanceConnection) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if !ic.IsConnected() {
		return adapter.ErrConnectionClosed
	}

	res, err := ic.client.Indices.Create(name,
		ic.client.Indices.Create.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to create index: %s", res.Status())
	}

	return nil
}

// DropDatabase deletes an index from OpenSearch.
func (ic *InstanceConnection) DropDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if !ic.IsConnected() {
		return adapter.ErrConnectionClosed
	}

	res, err := ic.client.Indices.Delete([]string{name},
		ic.client.Indices.Delete.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to delete index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to delete index: %s", res.Status())
	}

	return nil
}

// MetadataOperations returns the metadata operator.
func (ic *InstanceConnection) MetadataOperations() adapter.MetadataOperator {
	return &MetadataOps{instanceConn: ic}
}

// Raw returns the underlying OpenSearch client.
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
