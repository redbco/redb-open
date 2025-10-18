package snowflake

import (
	"context"
	"database/sql"
	"sync/atomic"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

type Adapter struct{}

func NewAdapter() adapter.DatabaseAdapter {
	return &Adapter{}
}

func (a *Adapter) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.Snowflake
}

func (a *Adapter) Capabilities() dbcapabilities.Capability {
	return dbcapabilities.MustGet(dbcapabilities.Snowflake)
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
		return nil, adapter.NewConnectionError(dbcapabilities.Snowflake, config.Host, config.Port, err)
	}

	db, ok := client.DB.(*sql.DB)
	if !ok {
		return nil, adapter.NewConfigurationError(dbcapabilities.Snowflake, "connection", "invalid snowflake connection type")
	}

	return &Connection{
		id:        config.DatabaseID,
		db:        db,
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
		return nil, adapter.NewConnectionError(dbcapabilities.Snowflake, config.Host, config.Port, err)
	}

	db, ok := client.DB.(*sql.DB)
	if !ok {
		return nil, adapter.NewConfigurationError(dbcapabilities.Snowflake, "connection", "invalid snowflake connection type")
	}

	return &InstanceConnection{
		id:        config.InstanceID,
		db:        db,
		config:    config,
		adapter:   a,
		connected: 1,
	}, nil
}

type Connection struct {
	id        string
	db        *sql.DB
	config    adapter.ConnectionConfig
	adapter   *Adapter
	connected int32
}

func (c *Connection) ID() string                        { return c.id }
func (c *Connection) Type() dbcapabilities.DatabaseType { return dbcapabilities.Snowflake }
func (c *Connection) IsConnected() bool                 { return atomic.LoadInt32(&c.connected) == 1 }
func (c *Connection) Ping(ctx context.Context) error    { return c.db.PingContext(ctx) }
func (c *Connection) Close() error {
	atomic.StoreInt32(&c.connected, 0)
	return c.db.Close()
}
func (c *Connection) SchemaOperations() adapter.SchemaOperator     { return &SchemaOps{conn: c} }
func (c *Connection) DataOperations() adapter.DataOperator         { return &DataOps{conn: c} }
func (c *Connection) MetadataOperations() adapter.MetadataOperator { return &MetadataOps{conn: c} }
func (c *Connection) Raw() interface{}                             { return c.db }
func (c *Connection) Config() adapter.ConnectionConfig             { return c.config }
func (c *Connection) Adapter() adapter.DatabaseAdapter             { return c.adapter }

func (c *Connection) ReplicationOperations() adapter.ReplicationOperator {
	return &ReplicationOps{conn: c}
}

type InstanceConnection struct {
	id        string
	db        *sql.DB
	config    adapter.InstanceConfig
	adapter   *Adapter
	connected int32
}

func (i *InstanceConnection) ID() string                        { return i.id }
func (i *InstanceConnection) Type() dbcapabilities.DatabaseType { return dbcapabilities.Snowflake }
func (i *InstanceConnection) IsConnected() bool                 { return atomic.LoadInt32(&i.connected) == 1 }
func (i *InstanceConnection) Ping(ctx context.Context) error    { return i.db.PingContext(ctx) }
func (i *InstanceConnection) Close() error {
	atomic.StoreInt32(&i.connected, 0)
	return i.db.Close()
}
func (i *InstanceConnection) MetadataOperations() adapter.MetadataOperator {
	return &InstanceMetadataOps{conn: i}
}
func (i *InstanceConnection) Raw() interface{}                 { return i.db }
func (i *InstanceConnection) Config() adapter.InstanceConfig   { return i.config }
func (i *InstanceConnection) Adapter() adapter.DatabaseAdapter { return i.adapter }

func (i *InstanceConnection) ListDatabases(ctx context.Context) ([]string, error) {
	rows, err := i.db.QueryContext(ctx, "SHOW DATABASES")
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Snowflake, "list_databases", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var name, createdOn, isDefault, isCurrent, origin, owner, comment, options, retentionTime string
		if err := rows.Scan(&createdOn, &name, &isDefault, &isCurrent, &origin, &owner, &comment, &options, &retentionTime); err == nil {
			databases = append(databases, name)
		}
	}
	return databases, nil
}

func (i *InstanceConnection) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	return CreateDatabase(ctx, i.db, name, options)
}

func (i *InstanceConnection) DropDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	return DropDatabase(ctx, i.db, name, options)
}
