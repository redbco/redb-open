package neo4j

import (
	"context"
	"sync/atomic"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

type Adapter struct{}

func NewAdapter() adapter.DatabaseAdapter {
	return &Adapter{}
}

func (a *Adapter) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.Neo4j
}

func (a *Adapter) Capabilities() dbcapabilities.Capability {
	return dbcapabilities.MustGet(dbcapabilities.Neo4j)
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
		return nil, adapter.NewConnectionError(dbcapabilities.Neo4j, config.Host, config.Port, err)
	}

	driver, ok := client.DB.(neo4jdriver.DriverWithContext)
	if !ok {
		return nil, adapter.NewConfigurationError(dbcapabilities.Neo4j, "connection", "invalid neo4j connection type")
	}

	return &Connection{
		id:        config.DatabaseID,
		driver:    driver,
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
		return nil, adapter.NewConnectionError(dbcapabilities.Neo4j, config.Host, config.Port, err)
	}

	driver, ok := client.DB.(neo4jdriver.DriverWithContext)
	if !ok {
		return nil, adapter.NewConfigurationError(dbcapabilities.Neo4j, "connection", "invalid neo4j connection type")
	}

	return &InstanceConnection{
		id:        config.InstanceID,
		driver:    driver,
		config:    config,
		adapter:   a,
		connected: 1,
	}, nil
}

type Connection struct {
	id        string
	driver    neo4jdriver.DriverWithContext
	config    adapter.ConnectionConfig
	adapter   *Adapter
	connected int32
}

func (c *Connection) ID() string                        { return c.id }
func (c *Connection) Type() dbcapabilities.DatabaseType { return dbcapabilities.Neo4j }
func (c *Connection) IsConnected() bool                 { return atomic.LoadInt32(&c.connected) == 1 }
func (c *Connection) Ping(ctx context.Context) error    { return c.driver.VerifyConnectivity(ctx) }
func (c *Connection) Close() error {
	atomic.StoreInt32(&c.connected, 0)
	return c.driver.Close(context.Background())
}
func (c *Connection) SchemaOperations() adapter.SchemaOperator     { return &SchemaOps{conn: c} }
func (c *Connection) DataOperations() adapter.DataOperator         { return &DataOps{conn: c} }
func (c *Connection) MetadataOperations() adapter.MetadataOperator { return &MetadataOps{conn: c} }
func (c *Connection) Raw() interface{}                             { return c.driver }
func (c *Connection) Config() adapter.ConnectionConfig             { return c.config }
func (c *Connection) Adapter() adapter.DatabaseAdapter             { return c.adapter }

func (c *Connection) ReplicationOperations() adapter.ReplicationOperator {
	return &ReplicationOps{conn: c}
}

type InstanceConnection struct {
	id        string
	driver    neo4jdriver.DriverWithContext
	config    adapter.InstanceConfig
	adapter   *Adapter
	connected int32
}

func (i *InstanceConnection) ID() string                        { return i.id }
func (i *InstanceConnection) Type() dbcapabilities.DatabaseType { return dbcapabilities.Neo4j }
func (i *InstanceConnection) IsConnected() bool                 { return atomic.LoadInt32(&i.connected) == 1 }
func (i *InstanceConnection) Ping(ctx context.Context) error    { return i.driver.VerifyConnectivity(ctx) }
func (i *InstanceConnection) Close() error {
	atomic.StoreInt32(&i.connected, 0)
	return i.driver.Close(context.Background())
}
func (i *InstanceConnection) MetadataOperations() adapter.MetadataOperator {
	return &InstanceMetadataOps{conn: i}
}
func (i *InstanceConnection) Raw() interface{}                 { return i.driver }
func (i *InstanceConnection) Config() adapter.InstanceConfig   { return i.config }
func (i *InstanceConnection) Adapter() adapter.DatabaseAdapter { return i.adapter }

func (i *InstanceConnection) ListDatabases(ctx context.Context) ([]string, error) {
	session := i.driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "SHOW DATABASES", nil)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Neo4j, "list_databases", err)
	}

	var databases []string
	for result.Next(ctx) {
		record := result.Record()
		if name, ok := record.Get("name"); ok {
			databases = append(databases, name.(string))
		}
	}

	if err := result.Err(); err != nil {
		return nil, adapter.WrapError(dbcapabilities.Neo4j, "list_databases", err)
	}
	return databases, nil
}

func (i *InstanceConnection) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	return CreateDatabase(ctx, i.driver, name, options)
}

func (i *InstanceConnection) DropDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	return DropDatabase(ctx, i.driver, name, options)
}
