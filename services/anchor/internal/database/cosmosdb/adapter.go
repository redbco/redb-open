package cosmosdb

import (
	"context"
	"sync/atomic"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

type Adapter struct{}

func NewAdapter() adapter.DatabaseAdapter {
	return &Adapter{}
}

func (a *Adapter) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.CosmosDB
}

func (a *Adapter) Capabilities() dbcapabilities.Capability {
	return dbcapabilities.MustGet(dbcapabilities.CosmosDB)
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
		return nil, adapter.NewConnectionError(dbcapabilities.CosmosDB, config.Host, config.Port, err)
	}

	cosmosClient, ok := client.DB.(*azcosmos.Client)
	if !ok {
		return nil, adapter.NewConfigurationError(dbcapabilities.CosmosDB, "connection", "invalid cosmosdb connection type")
	}

	return &Connection{
		id:        config.DatabaseID,
		client:    cosmosClient,
		dbName:    config.DatabaseName,
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
		return nil, adapter.NewConnectionError(dbcapabilities.CosmosDB, config.Host, config.Port, err)
	}

	cosmosClient, ok := client.DB.(*azcosmos.Client)
	if !ok {
		return nil, adapter.NewConfigurationError(dbcapabilities.CosmosDB, "connection", "invalid cosmosdb connection type")
	}

	return &InstanceConnection{
		id:        config.InstanceID,
		client:    cosmosClient,
		config:    config,
		adapter:   a,
		connected: 1,
	}, nil
}

type Connection struct {
	id        string
	client    *azcosmos.Client
	dbName    string
	config    adapter.ConnectionConfig
	adapter   *Adapter
	connected int32
}

func (c *Connection) ID() string                        { return c.id }
func (c *Connection) Type() dbcapabilities.DatabaseType { return dbcapabilities.CosmosDB }
func (c *Connection) IsConnected() bool                 { return atomic.LoadInt32(&c.connected) == 1 }
func (c *Connection) Ping(ctx context.Context) error {
	if c.dbName == "" {
		return nil
	}
	dbClient, err := c.client.NewDatabase(c.dbName)
	if err != nil {
		return err
	}
	_, err = dbClient.Read(ctx, nil)
	return err
}
func (c *Connection) Close() error {
	atomic.StoreInt32(&c.connected, 0)
	return nil // CosmosDB client doesn't need explicit close
}
func (c *Connection) SchemaOperations() adapter.SchemaOperator     { return &SchemaOps{conn: c} }
func (c *Connection) DataOperations() adapter.DataOperator         { return &DataOps{conn: c} }
func (c *Connection) MetadataOperations() adapter.MetadataOperator { return &MetadataOps{conn: c} }
func (c *Connection) Raw() interface{}                             { return c.client }
func (c *Connection) Config() adapter.ConnectionConfig             { return c.config }
func (c *Connection) Adapter() adapter.DatabaseAdapter             { return c.adapter }

func (c *Connection) ReplicationOperations() adapter.ReplicationOperator {
	return &ReplicationOps{conn: c}
}

type InstanceConnection struct {
	id        string
	client    *azcosmos.Client
	config    adapter.InstanceConfig
	adapter   *Adapter
	connected int32
}

func (i *InstanceConnection) ID() string                        { return i.id }
func (i *InstanceConnection) Type() dbcapabilities.DatabaseType { return dbcapabilities.CosmosDB }
func (i *InstanceConnection) IsConnected() bool                 { return atomic.LoadInt32(&i.connected) == 1 }
func (i *InstanceConnection) Ping(ctx context.Context) error {
	// Ping by listing databases
	pager := i.client.NewQueryDatabasesPager("SELECT * FROM c", nil)
	_, err := pager.NextPage(ctx)
	return err
}
func (i *InstanceConnection) Close() error {
	atomic.StoreInt32(&i.connected, 0)
	return nil
}
func (i *InstanceConnection) MetadataOperations() adapter.MetadataOperator {
	return &InstanceMetadataOps{conn: i}
}
func (i *InstanceConnection) Raw() interface{}                 { return i.client }
func (i *InstanceConnection) Config() adapter.InstanceConfig   { return i.config }
func (i *InstanceConnection) Adapter() adapter.DatabaseAdapter { return i.adapter }

func (i *InstanceConnection) ListDatabases(ctx context.Context) ([]string, error) {
	pager := i.client.NewQueryDatabasesPager("SELECT * FROM c", nil)

	var databases []string
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, adapter.WrapError(dbcapabilities.CosmosDB, "list_databases", err)
		}
		for _, db := range resp.Databases {
			databases = append(databases, db.ID)
		}
	}
	return databases, nil
}

func (i *InstanceConnection) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	return CreateDatabase(ctx, i.client, name, options)
}

func (i *InstanceConnection) DropDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	return DropDatabase(ctx, i.client, name, options)
}
