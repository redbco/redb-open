package cassandra

import (
	"context"
	"sync/atomic"

	"github.com/gocql/gocql"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

type Adapter struct{}

func NewAdapter() adapter.DatabaseAdapter {
	return &Adapter{}
}

func (a *Adapter) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.Cassandra
}

func (a *Adapter) Capabilities() dbcapabilities.Capability {
	return dbcapabilities.MustGet(dbcapabilities.Cassandra)
}

func (a *Adapter) Connect(ctx context.Context, config adapter.ConnectionConfig) (adapter.Connection, error) {
	legacyConfig := dbclient.DatabaseConfig{
		DatabaseID:            config.DatabaseID,
		WorkspaceID:           config.WorkspaceID,
		TenantID:              config.TenantID,
		EnvironmentID:         adapter.GetString(config.EnvironmentID),
		InstanceID:            config.InstanceID,
		Name:                  config.Name,
		Description:           config.Description,
		DatabaseVendor:        config.DatabaseVendor,
		ConnectionType:        config.ConnectionType,
		Host:                  config.Host,
		Port:                  config.Port,
		Username:              config.Username,
		Password:              config.Password,
		DatabaseName:          config.DatabaseName,
		Enabled:               config.Enabled,
		SSL:                   config.SSL,
		SSLMode:               config.SSLMode,
		SSLRejectUnauthorized: config.SSLRejectUnauthorized,
		SSLCert:               adapter.GetString(config.SSLCert),
		SSLKey:                adapter.GetString(config.SSLKey),
		SSLRootCert:           adapter.GetString(config.SSLRootCert),
		Role:                  config.Role,
		ConnectedToNodeID:     config.ConnectedToNodeID,
		OwnerID:               config.OwnerID,
	}

	client, err := Connect(legacyConfig)
	if err != nil {
		return nil, adapter.NewConnectionError(dbcapabilities.Cassandra, config.Host, config.Port, err)
	}

	session, ok := client.DB.(*gocql.Session)
	if !ok {
		return nil, adapter.NewConfigurationError(dbcapabilities.Cassandra, "connection", "invalid cassandra connection type")
	}

	return &Connection{
		id:        config.DatabaseID,
		session:   session,
		config:    config,
		adapter:   a,
		connected: 1,
	}, nil
}

func (a *Adapter) ConnectInstance(ctx context.Context, config adapter.InstanceConfig) (adapter.InstanceConnection, error) {
	legacyConfig := dbclient.InstanceConfig{
		InstanceID:            config.InstanceID,
		WorkspaceID:           config.WorkspaceID,
		TenantID:              config.TenantID,
		EnvironmentID:         adapter.GetString(config.EnvironmentID),
		Name:                  config.Name,
		Description:           config.Description,
		DatabaseVendor:        config.DatabaseVendor,
		ConnectionType:        config.ConnectionType,
		Host:                  config.Host,
		Port:                  config.Port,
		Username:              config.Username,
		Password:              config.Password,
		DatabaseName:          config.DatabaseName,
		Enabled:               config.Enabled,
		SSL:                   config.SSL,
		SSLMode:               config.SSLMode,
		SSLRejectUnauthorized: config.SSLRejectUnauthorized,
		SSLCert:               adapter.GetString(config.SSLCert),
		SSLKey:                adapter.GetString(config.SSLKey),
		SSLRootCert:           adapter.GetString(config.SSLRootCert),
		Role:                  config.Role,
		ConnectedToNodeID:     config.ConnectedToNodeID,
		OwnerID:               config.OwnerID,
		UniqueIdentifier:      config.UniqueIdentifier,
		Version:               config.Version,
	}

	client, err := ConnectInstance(legacyConfig)
	if err != nil {
		return nil, adapter.NewConnectionError(dbcapabilities.Cassandra, config.Host, config.Port, err)
	}

	session, ok := client.DB.(*gocql.Session)
	if !ok {
		return nil, adapter.NewConfigurationError(dbcapabilities.Cassandra, "connection", "invalid cassandra connection type")
	}

	return &InstanceConnection{
		id:        config.InstanceID,
		session:   session,
		config:    config,
		adapter:   a,
		connected: 1,
	}, nil
}

type Connection struct {
	id        string
	session   *gocql.Session
	config    adapter.ConnectionConfig
	adapter   *Adapter
	connected int32
}

func (c *Connection) ID() string                        { return c.id }
func (c *Connection) Type() dbcapabilities.DatabaseType { return dbcapabilities.Cassandra }
func (c *Connection) IsConnected() bool                 { return atomic.LoadInt32(&c.connected) == 1 }
func (c *Connection) Ping(ctx context.Context) error {
	return c.session.Query("SELECT now() FROM system.local").WithContext(ctx).Exec()
}
func (c *Connection) Close() error {
	atomic.StoreInt32(&c.connected, 0)
	c.session.Close()
	return nil
}
func (c *Connection) SchemaOperations() adapter.SchemaOperator     { return &SchemaOps{conn: c} }
func (c *Connection) DataOperations() adapter.DataOperator         { return &DataOps{conn: c} }
func (c *Connection) MetadataOperations() adapter.MetadataOperator { return &MetadataOps{conn: c} }
func (c *Connection) Raw() interface{}                             { return c.session }
func (c *Connection) Config() adapter.ConnectionConfig             { return c.config }
func (c *Connection) Adapter() adapter.DatabaseAdapter             { return c.adapter }

func (c *Connection) ReplicationOperations() adapter.ReplicationOperator {
	return adapter.NewUnsupportedReplicationOperator(dbcapabilities.Cassandra)
}

type InstanceConnection struct {
	id        string
	session   *gocql.Session
	config    adapter.InstanceConfig
	adapter   *Adapter
	connected int32
}

func (i *InstanceConnection) ID() string                        { return i.id }
func (i *InstanceConnection) Type() dbcapabilities.DatabaseType { return dbcapabilities.Cassandra }
func (i *InstanceConnection) IsConnected() bool                 { return atomic.LoadInt32(&i.connected) == 1 }
func (i *InstanceConnection) Ping(ctx context.Context) error {
	return i.session.Query("SELECT now() FROM system.local").WithContext(ctx).Exec()
}
func (i *InstanceConnection) Close() error {
	atomic.StoreInt32(&i.connected, 0)
	i.session.Close()
	return nil
}
func (i *InstanceConnection) MetadataOperations() adapter.MetadataOperator {
	return &InstanceMetadataOps{conn: i}
}
func (i *InstanceConnection) Raw() interface{}                 { return i.session }
func (i *InstanceConnection) Config() adapter.InstanceConfig   { return i.config }
func (i *InstanceConnection) Adapter() adapter.DatabaseAdapter { return i.adapter }

func (i *InstanceConnection) ListDatabases(ctx context.Context) ([]string, error) {
	// In Cassandra, databases are called keyspaces
	var keyspaces []string
	iter := i.session.Query("SELECT keyspace_name FROM system_schema.keyspaces").WithContext(ctx).Iter()

	var keyspace string
	for iter.Scan(&keyspace) {
		// Skip system keyspaces
		if keyspace != "system" && keyspace != "system_schema" && keyspace != "system_auth" && keyspace != "system_traces" {
			keyspaces = append(keyspaces, keyspace)
		}
	}

	if err := iter.Close(); err != nil {
		return nil, adapter.WrapError(dbcapabilities.Cassandra, "list_databases", err)
	}
	return keyspaces, nil
}

func (i *InstanceConnection) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	return CreateDatabase(ctx, i.session, name, options)
}

func (i *InstanceConnection) DropDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	return DropDatabase(ctx, i.session, name, options)
}
