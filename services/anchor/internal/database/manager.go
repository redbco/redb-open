package database

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/anchor/internal/database/cassandra"
	"github.com/redbco/redb-open/services/anchor/internal/database/clickhouse"
	"github.com/redbco/redb-open/services/anchor/internal/database/cockroach"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
	"github.com/redbco/redb-open/services/anchor/internal/database/edgedb"
	"github.com/redbco/redb-open/services/anchor/internal/database/elasticsearch"
	"github.com/redbco/redb-open/services/anchor/internal/database/iceberg"
	"github.com/redbco/redb-open/services/anchor/internal/database/mariadb"
	"github.com/redbco/redb-open/services/anchor/internal/database/mongodb"
	"github.com/redbco/redb-open/services/anchor/internal/database/mssql"
	"github.com/redbco/redb-open/services/anchor/internal/database/mysql"
	"github.com/redbco/redb-open/services/anchor/internal/database/neo4j"
	"github.com/redbco/redb-open/services/anchor/internal/database/pinecone"
	"github.com/redbco/redb-open/services/anchor/internal/database/postgres"
	"github.com/redbco/redb-open/services/anchor/internal/database/redis"
	"github.com/redbco/redb-open/services/anchor/internal/database/snowflake"
)

// DatabaseManager manages database connections
type DatabaseManager struct {
	databaseClients    map[string]*common.DatabaseClient
	instanceClients    map[string]*common.InstanceClient
	replicationClients map[string]*common.ReplicationClient
	replicationManager *ReplicationManager
	mu                 sync.RWMutex
	logger             *logger.Logger
	dbLogger           *DatabaseLogger
}

// NewDatabaseManager creates a new DatabaseManager instance
func NewDatabaseManager() *DatabaseManager {
	dm := &DatabaseManager{
		databaseClients:    make(map[string]*common.DatabaseClient),
		instanceClients:    make(map[string]*common.InstanceClient),
		replicationClients: make(map[string]*common.ReplicationClient),
	}
	dm.replicationManager = NewReplicationManager(dm)
	return dm
}

// DatabaseMetadataCollector collects metadata about a database
type DatabaseMetadataCollector struct {
	client *common.DatabaseClient
}

// NewDatabaseMetadataCollector creates a new metadata collector
func NewDatabaseMetadataCollector(client *common.DatabaseClient) *DatabaseMetadataCollector {
	return &DatabaseMetadataCollector{
		client: client,
	}
}

// InstanceMetadataCollector collects metadata about a database instance
type InstanceMetadataCollector struct {
	client *common.InstanceClient
}

// NewInstanceMetadataCollector creates a new instance metadata collector
func NewInstanceMetadataCollector(client *common.InstanceClient) *InstanceMetadataCollector {
	return &InstanceMetadataCollector{
		client: client,
	}
}

func (dm *DatabaseManager) SetLogger(logger *logger.Logger) {
	dm.logger = logger
	dm.dbLogger = NewDatabaseLogger(logger)
	if dm.replicationManager != nil {
		dm.replicationManager.SetLogger(logger)
	}
}

func (dm *DatabaseManager) GetLogger() *logger.Logger {
	return dm.logger
}

// GetReplicationManager returns the replication manager instance
func (dm *DatabaseManager) GetReplicationManager() *ReplicationManager {
	return dm.replicationManager
}

// safeLog safely logs a message if logger is available
func (dm *DatabaseManager) safeLog(level string, format string, args ...interface{}) {
	if dm.logger != nil {
		switch level {
		case "info":
			dm.logger.Info(format, args...)
		case "error":
			dm.logger.Error(format, args...)
		case "warn":
			dm.logger.Warn(format, args...)
		case "debug":
			dm.logger.Debug(format, args...)
		}
	}
}

// GetClient returns a database client by ID
func (dm *DatabaseManager) GetDatabaseClient(id string) (*common.DatabaseClient, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	client, exists := dm.databaseClients[id]
	if !exists {
		return nil, fmt.Errorf("database with ID %s not found", id)
	}

	return client, nil
}

// GetAllClientIDs returns a slice of all connected client IDs
func (dm *DatabaseManager) GetAllDatabaseClientIDs() []string {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	ids := make([]string, 0, len(dm.databaseClients))
	for id := range dm.databaseClients {
		ids = append(ids, id)
	}
	return ids
}

// UpdateDatabaseID updates a database ID in the manager's map
func (dm *DatabaseManager) UpdateDatabaseID(id string, databaseID string) {
	dm.safeLog("info", "Updating database ID %s for client %s", databaseID, id)
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Update both the Config.DatabaseID and DatabaseID fields
	if client, exists := dm.databaseClients[id]; exists {
		client.Config.DatabaseID = databaseID
		client.DatabaseID = databaseID
		dm.safeLog("info", "Updated database ID %s for client %s", databaseID, id)
	} else {
		dm.safeLog("error", "Client %s not found when updating database ID", id)
	}
}

// Helper function to convert interface{} to int64
func convertToInt64(value interface{}) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	case int:
		return int64(v)
	default:
		return 0 // Default value if type is unknown
	}
}

// Helper function to convert interface{} to int
func convertToInt(value interface{}) int {
	if value == nil {
		return 0
	}

	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0 // Default value if type is unknown
	}
}

// GetInstanceClient returns an instance client by ID
func (dm *DatabaseManager) GetInstanceClient(id string) (*common.InstanceClient, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	client, exists := dm.instanceClients[id]
	if !exists {
		return nil, fmt.Errorf("instance with ID %s not found", id)
	}

	return client, nil
}

// GetAllInstanceClientIDs returns a slice of all connected instance client IDs
func (dm *DatabaseManager) GetAllInstanceClientIDs() []string {
	dm.safeLog("info", "Getting all instance client IDs")
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	ids := make([]string, 0, len(dm.instanceClients))
	for id := range dm.instanceClients {
		ids = append(ids, id)
	}
	return ids
}

// UpdateInstanceID updates an instance ID in the manager's map
func (dm *DatabaseManager) UpdateInstanceID(id string, instanceID string) {
	dm.safeLog("info", "Updating instance ID %s for client %s", instanceID, id)
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Update both the Config.InstanceID and InstanceID fields
	if client, exists := dm.instanceClients[id]; exists {
		client.Config.InstanceID = instanceID
		client.InstanceID = instanceID
		dm.safeLog("info", "Updated instance ID %s for client %s", instanceID, id)
	} else {
		dm.safeLog("error", "Client %s not found when updating instance ID", id)
	}
}

// GetReplicationClient returns a replication client by ID
func (dm *DatabaseManager) GetReplicationClient(id string) (*common.ReplicationClient, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	client, exists := dm.replicationClients[id]
	if !exists {
		return nil, fmt.Errorf("replication client with ID %s not found", id)
	}

	return client, nil
}

// GetAllReplicationClientIDs returns a slice of all connected replication client IDs
func (dm *DatabaseManager) GetAllReplicationClientIDs() []string {
	dm.safeLog("info", "Getting all replication client IDs")
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	ids := make([]string, 0, len(dm.replicationClients))
	for id := range dm.replicationClients {
		ids = append(ids, id)
	}
	return ids
}

// UpdateReplicationID updates a replication ID in the manager's map
func (dm *DatabaseManager) UpdateReplicationID(id string, replicationID string) {
	dm.safeLog("info", "Updating replication ID %s for client %s", replicationID, id)
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Update both the Config.ReplicationID and ReplicationID fields
	if client, exists := dm.replicationClients[id]; exists {
		client.Config.ReplicationID = replicationID
		client.ReplicationID = replicationID
		dm.safeLog("info", "Updated replication ID %s for client %s", replicationID, id)
	} else {
		dm.safeLog("error", "Replication client %s not found when updating replication ID", id)
	}
}

// GetReplicationClientsByDatabaseID returns all replication clients for a specific database
func (dm *DatabaseManager) GetReplicationClientsByDatabaseID(databaseID string) ([]*common.ReplicationClient, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	var clients []*common.ReplicationClient
	for _, client := range dm.replicationClients {
		if client.DatabaseID == databaseID {
			clients = append(clients, client)
		}
	}

	return clients, nil
}

// GetActiveReplicationClients returns all active replication clients
func (dm *DatabaseManager) GetActiveReplicationClients() ([]*common.ReplicationClient, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	var activeClients []*common.ReplicationClient
	for _, client := range dm.replicationClients {
		if atomic.LoadInt32(&client.IsConnected) == 1 {
			activeClients = append(activeClients, client)
		}
	}

	return activeClients, nil
}

// ExecuteCommand executes a command on a database
func (dm *DatabaseManager) ExecuteCommand(databaseID string, command string) ([]byte, error) {
	dm.safeLog("info", "Executing command %s on database %s", command, databaseID)

	client, err := dm.GetDatabaseClient(databaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database client: %w", err)
	}

	if atomic.LoadInt32(&client.IsConnected) == 0 {
		return nil, fmt.Errorf("database %s is disconnected", databaseID)
	}

	switch client.DatabaseType {
	case string(dbcapabilities.PostgreSQL):
		return postgres.ExecuteCommand(context.Background(), client.DB, command)
	case string(dbcapabilities.MySQL):
		return mysql.ExecuteCommand(context.Background(), client.DB, command)
	case string(dbcapabilities.MariaDB):
		return mariadb.ExecuteCommand(context.Background(), client.DB, command)
	case string(dbcapabilities.CockroachDB):
		return cockroach.ExecuteCommand(context.Background(), client.DB, command)
	case string(dbcapabilities.Redis):
		return redis.ExecuteCommand(context.Background(), client.DB, command)
	case string(dbcapabilities.MongoDB):
		return mongodb.ExecuteCommand(context.Background(), client.DB, command)
	case string(dbcapabilities.SQLServer):
		return mssql.ExecuteCommand(context.Background(), client.DB, command)
	case string(dbcapabilities.Cassandra):
		return cassandra.ExecuteCommand(context.Background(), client.DB, command)
	case string(dbcapabilities.EdgeDB):
		return edgedb.ExecuteCommand(context.Background(), client.DB, command)
	case string(dbcapabilities.Snowflake):
		return snowflake.ExecuteCommand(context.Background(), client.DB, command)
	case string(dbcapabilities.ClickHouse):
		return clickhouse.ExecuteCommand(context.Background(), client.DB, command)
	case string(dbcapabilities.Pinecone):
		return pinecone.ExecuteCommand(context.Background(), client.DB, command)
	case string(dbcapabilities.Elasticsearch):
		return elasticsearch.ExecuteCommand(context.Background(), client.DB, command)
	case string(dbcapabilities.Neo4j):
		return neo4j.ExecuteCommand(context.Background(), client.DB, command)
	case string(dbcapabilities.Iceberg):
		return iceberg.ExecuteCommand(context.Background(), client.DB, command)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", client.DatabaseType)
	}
}
