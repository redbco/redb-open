package solr

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// Adapter implements adapter.DatabaseAdapter for Apache Solr.
type Adapter struct{}

// NewAdapter creates a new Solr adapter instance.
func NewAdapter() adapter.DatabaseAdapter {
	return &Adapter{}
}

// Type returns the database type identifier.
func (a *Adapter) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.Solr
}

// Capabilities returns the capability metadata.
func (a *Adapter) Capabilities() dbcapabilities.Capability {
	return dbcapabilities.MustGet(dbcapabilities.Solr)
}

// Connect establishes a connection to Solr.
func (a *Adapter) Connect(ctx context.Context, config adapter.ConnectionConfig) (adapter.Connection, error) {
	// Build Solr base URL
	baseURL := fmt.Sprintf("http://%s:%d/solr", config.Host, config.Port)

	// Create HTTP client
	client := &http.Client{}

	// Test connection
	testURL := fmt.Sprintf("%s/%s/admin/ping", baseURL, config.DatabaseName)
	req, err := http.NewRequestWithContext(ctx, "GET", testURL, nil)
	if err != nil {
		return nil, err
	}

	// Add basic auth if provided
	if config.Username != "" {
		req.SetBasicAuth(config.Username, config.Password)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.Solr,
			config.Host,
			config.Port,
			err,
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, adapter.NewConnectionError(
			dbcapabilities.Solr,
			config.Host,
			config.Port,
			fmt.Errorf("ping failed with status: %s", resp.Status),
		)
	}

	conn := &Connection{
		id:         config.DatabaseID,
		client:     client,
		baseURL:    baseURL,
		collection: config.DatabaseName,
		username:   config.Username,
		password:   config.Password,
		config:     config,
		adapter:    a,
		connected:  1,
	}

	return conn, nil
}

// ConnectInstance establishes an instance-level connection to Solr.
func (a *Adapter) ConnectInstance(ctx context.Context, config adapter.InstanceConfig) (adapter.InstanceConnection, error) {
	// Build Solr base URL
	baseURL := fmt.Sprintf("http://%s:%d/solr", config.Host, config.Port)

	// Create HTTP client
	client := &http.Client{}

	// Test connection
	testURL := fmt.Sprintf("%s/admin/info/system", baseURL)
	req, err := http.NewRequestWithContext(ctx, "GET", testURL, nil)
	if err != nil {
		return nil, err
	}

	// Add basic auth if provided
	if config.Username != "" {
		req.SetBasicAuth(config.Username, config.Password)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.Solr,
			config.Host,
			config.Port,
			err,
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, adapter.NewConnectionError(
			dbcapabilities.Solr,
			config.Host,
			config.Port,
			fmt.Errorf("connection test failed with status: %s", resp.Status),
		)
	}

	conn := &InstanceConnection{
		id:        config.InstanceID,
		client:    client,
		baseURL:   baseURL,
		username:  config.Username,
		password:  config.Password,
		config:    config,
		adapter:   a,
		connected: 1,
	}

	return conn, nil
}

// Connection implements adapter.Connection for Solr.
type Connection struct {
	id         string
	client     *http.Client
	baseURL    string
	collection string
	username   string
	password   string
	config     adapter.ConnectionConfig
	adapter    *Adapter
	connected  int32
}

// ID returns the connection identifier.
func (c *Connection) ID() string {
	return c.id
}

// Type returns the database type.
func (c *Connection) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.Solr
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

	url := fmt.Sprintf("%s/%s/admin/ping", c.baseURL, c.collection)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ping failed: %s", resp.Status)
	}

	return nil
}

// Close closes the connection.
func (c *Connection) Close() error {
	atomic.StoreInt32(&c.connected, 0)
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

// Raw returns the underlying HTTP client.
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

// InstanceConnection implements adapter.InstanceConnection for Solr.
type InstanceConnection struct {
	id        string
	client    *http.Client
	baseURL   string
	username  string
	password  string
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
	return dbcapabilities.Solr
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

	url := fmt.Sprintf("%s/admin/info/system", ic.baseURL)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	if ic.username != "" {
		req.SetBasicAuth(ic.username, ic.password)
	}

	resp, err := ic.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ping failed: %s", resp.Status)
	}

	return nil
}

// Close closes the connection.
func (ic *InstanceConnection) Close() error {
	atomic.StoreInt32(&ic.connected, 0)
	return nil
}

// ListDatabases lists all collections in Solr (collections act as "databases").
func (ic *InstanceConnection) ListDatabases(ctx context.Context) ([]string, error) {
	if !ic.IsConnected() {
		return nil, adapter.ErrConnectionClosed
	}

	url := fmt.Sprintf("%s/admin/collections?action=LIST&wt=json", ic.baseURL)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	if ic.username != "" {
		req.SetBasicAuth(ic.username, ic.password)
	}

	resp, err := ic.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to list collections: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to list collections: %s", resp.Status)
	}

	var result struct {
		Collections []string `json:"collections"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result.Collections, nil
}

// CreateDatabase creates a new collection in Solr.
func (ic *InstanceConnection) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if !ic.IsConnected() {
		return adapter.ErrConnectionClosed
	}

	// Build create collection URL
	params := url.Values{}
	params.Set("action", "CREATE")
	params.Set("name", name)
	params.Set("numShards", "1")
	params.Set("replicationFactor", "1")
	params.Set("wt", "json")

	url := fmt.Sprintf("%s/admin/collections?%s", ic.baseURL, params.Encode())
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	if ic.username != "" {
		req.SetBasicAuth(ic.username, ic.password)
	}

	resp, err := ic.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create collection: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to create collection: %s", resp.Status)
	}

	return nil
}

// DropDatabase deletes a collection from Solr.
func (ic *InstanceConnection) DropDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if !ic.IsConnected() {
		return adapter.ErrConnectionClosed
	}

	params := url.Values{}
	params.Set("action", "DELETE")
	params.Set("name", name)
	params.Set("wt", "json")

	url := fmt.Sprintf("%s/admin/collections?%s", ic.baseURL, params.Encode())
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	if ic.username != "" {
		req.SetBasicAuth(ic.username, ic.password)
	}

	resp, err := ic.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete collection: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to delete collection: %s", resp.Status)
	}

	return nil
}

// MetadataOperations returns the metadata operator.
func (ic *InstanceConnection) MetadataOperations() adapter.MetadataOperator {
	return &MetadataOps{instanceConn: ic}
}

// Raw returns the underlying HTTP client.
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

// Helper function to create request with auth
func (c *Connection) newRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	url := fmt.Sprintf("%s/%s%s", c.baseURL, c.collection, path)
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}

	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	req.Header.Set("Content-Type", "application/json")

	return req, nil
}
