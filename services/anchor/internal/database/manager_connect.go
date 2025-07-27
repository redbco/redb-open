package database

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"

	"github.com/geldata/gel-go"
	"github.com/gocql/gocql"
	"github.com/jackc/pgx/v5/pgxpool"
	neo4jgo "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redbco/redb-open/services/anchor/internal/database/cassandra"
	"github.com/redbco/redb-open/services/anchor/internal/database/chroma"
	"github.com/redbco/redb-open/services/anchor/internal/database/clickhouse"
	"github.com/redbco/redb-open/services/anchor/internal/database/cockroach"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
	"github.com/redbco/redb-open/services/anchor/internal/database/cosmosdb"
	"github.com/redbco/redb-open/services/anchor/internal/database/dynamodb"
	"github.com/redbco/redb-open/services/anchor/internal/database/edgedb"
	"github.com/redbco/redb-open/services/anchor/internal/database/elasticsearch"
	"github.com/redbco/redb-open/services/anchor/internal/database/mariadb"
	"github.com/redbco/redb-open/services/anchor/internal/database/milvus"
	"github.com/redbco/redb-open/services/anchor/internal/database/mongodb"
	"github.com/redbco/redb-open/services/anchor/internal/database/mssql"
	"github.com/redbco/redb-open/services/anchor/internal/database/mysql"
	"github.com/redbco/redb-open/services/anchor/internal/database/neo4j"
	"github.com/redbco/redb-open/services/anchor/internal/database/pinecone"
	"github.com/redbco/redb-open/services/anchor/internal/database/postgres"
	"github.com/redbco/redb-open/services/anchor/internal/database/redis"
	"github.com/redbco/redb-open/services/anchor/internal/database/snowflake"
	"github.com/redbco/redb-open/services/anchor/internal/database/weaviate"
	goredis "github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// ConnectDatabase establishes a connection to a database
func (dm *DatabaseManager) ConnectDatabase(config common.DatabaseConfig) (*common.DatabaseClient, error) {
	// Log connection attempt with unified logging
	if dm.dbLogger != nil {
		dm.dbLogger.LogClientConnectionAttempt(config.ConnectionType, config.DatabaseID, config.Host, config.Port)
	}

	var client *common.DatabaseClient
	var err error

	switch config.ConnectionType {
	case "postgres":
		client, err = postgres.Connect(config)
	case "mysql":
		client, err = mysql.Connect(config)
	case "mariadb":
		client, err = mariadb.Connect(config)
	case "cockroach":
		client, err = cockroach.Connect(config)
	case "redis":
		client, err = redis.Connect(config)
	case "mongodb":
		client, err = mongodb.Connect(config)
	case "mssql":
		client, err = mssql.Connect(config)
	case "cassandra":
		client, err = cassandra.Connect(config)
	case "edgedb":
		client, err = edgedb.Connect(config)
	case "snowflake":
		client, err = snowflake.Connect(config)
	case "clickhouse":
		client, err = clickhouse.Connect(config)
	case "pinecone":
		client, err = pinecone.Connect(config)
	case "chroma":
		client, err = chroma.Connect(config)
	case "milvus":
		client, err = milvus.Connect(config)
	case "weaviate":
		client, err = weaviate.Connect(config)
	case "elasticsearch":
		client, err = elasticsearch.Connect(config)
	case "neo4j":
		client, err = neo4j.Connect(config)
	case "dynamodb":
		client, err = dynamodb.Connect(config)
	case "cosmosdb":
		client, err = cosmosdb.Connect(config)
	//case "db2":
	//	client, err = db2.Connect(config)
	//case "oracle":
	//	client, err = oracle.Connect(config)
	default:
		// Log unsupported database type as a warning (not an error)
		if dm.dbLogger != nil {
			dm.dbLogger.LogClientConnectionFailure(config.ConnectionType, config.DatabaseID, config.Host, config.Port,
				fmt.Errorf("unsupported database type: %s", config.ConnectionType))
		}
		return nil, fmt.Errorf("unsupported database type: %s", config.ConnectionType)
	}

	if err != nil {
		// Log connection failure as warning for client databases
		if dm.dbLogger != nil {
			dm.dbLogger.LogClientConnectionFailure(config.ConnectionType, config.DatabaseID, config.Host, config.Port, err)
		}
		return nil, err
	}

	// Log successful connection
	if dm.dbLogger != nil {
		dm.dbLogger.LogClientConnectionSuccess(config.ConnectionType, config.DatabaseID, config.Host, config.Port)
	}

	dm.mu.Lock()
	dm.databaseClients[config.DatabaseID] = client
	dm.mu.Unlock()

	return client, nil
}

// DisconnectDatabase closes a database connection
func (dm *DatabaseManager) DisconnectDatabase(id string) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	client, exists := dm.databaseClients[id]
	if !exists {
		// Log that database is already disconnected (info level)
		if dm.dbLogger != nil {
			dm.dbLogger.LogDisconnectionSuccess(DatabaseLogContext{
				DatabaseType: "unknown",
				DatabaseID:   id,
				IsInternal:   false,
			})
		}
		return nil // Don't return error - database is already disconnected
	}

	// Log disconnection attempt
	if dm.dbLogger != nil {
		dm.dbLogger.LogDisconnectionAttempt(DatabaseLogContext{
			DatabaseType: client.DatabaseType,
			DatabaseID:   id,
			IsInternal:   false,
		})
	}

	if err := closeDatabase(client); err != nil {
		// Log disconnection failure as warning
		if dm.dbLogger != nil {
			dm.dbLogger.LogDisconnectionFailure(DatabaseLogContext{
				DatabaseType: client.DatabaseType,
				DatabaseID:   id,
				IsInternal:   false,
			}, err)
		}
		// Still remove from map even if close failed to prevent orphaned entries
		delete(dm.databaseClients, id)
		return err
	}

	delete(dm.databaseClients, id)

	// Log successful disconnection
	if dm.dbLogger != nil {
		dm.dbLogger.LogDisconnectionSuccess(DatabaseLogContext{
			DatabaseType: client.DatabaseType,
			DatabaseID:   id,
			IsInternal:   false,
		})
	}

	return nil
}

// closeDatabase closes the database connection properly based on the type
func closeDatabase(client *common.DatabaseClient) error {
	atomic.StoreInt32(&client.IsConnected, 0)

	switch client.DatabaseType {
	case "postgres":
		if pool, ok := client.DB.(*pgxpool.Pool); ok {
			pool.Close()
			return nil
		}
		return fmt.Errorf("invalid postgres connection type")
	case "mysql":
		if db, ok := client.DB.(*sql.DB); ok {
			return db.Close()
		}
		return fmt.Errorf("invalid mysql connection type")
	case "mariadb":
		if db, ok := client.DB.(*sql.DB); ok {
			return db.Close()
		}
		return fmt.Errorf("invalid mariadb connection type")
	case "cockroach":
		if pool, ok := client.DB.(*pgxpool.Pool); ok {
			pool.Close()
			return nil
		}
		return fmt.Errorf("invalid cockroach connection type")
	case "redis":
		if client, ok := client.DB.(*goredis.Client); ok {
			client.Close()
			return nil
		}
		return fmt.Errorf("invalid redis connection type")
	case "mongodb":
		if database, ok := client.DB.(*mongo.Database); ok {
			return database.Client().Disconnect(context.Background())
		}
		return fmt.Errorf("invalid mongodb connection type")
	case "mssql":
		if db, ok := client.DB.(*sql.DB); ok {
			return db.Close()
		}
		return fmt.Errorf("invalid mssql connection type")
	case "cassandra":
		if session, ok := client.DB.(*gocql.Session); ok {
			session.Close()
			return nil
		}
		return fmt.Errorf("invalid cassandra connection type")
	case "edgedb":
		if client, ok := client.DB.(*gel.Client); ok {
			client.Close()
			return nil
		}
		return fmt.Errorf("invalid edgedb connection type")
	case "snowflake":
		if db, ok := client.DB.(*sql.DB); ok {
			return db.Close()
		}
		return fmt.Errorf("invalid snowflake connection type")
	case "clickhouse":
		if conn, ok := client.DB.(clickhouse.ClickhouseConn); ok {
			conn.Close()
			return nil
		}
		return fmt.Errorf("invalid clickhouse connection type")
	case "pinecone":
		if client, ok := client.DB.(*pinecone.PineconeClient); ok {
			client.Close()
			return nil
		}
		return fmt.Errorf("invalid pinecone connection type")
	case "chroma":
		if client, ok := client.DB.(*chroma.ChromaClient); ok {
			client.Close()
			return nil
		}
		return fmt.Errorf("invalid chroma connection type")
	case "milvus":
		if client, ok := client.DB.(*milvus.MilvusClient); ok {
			client.Close()
			return nil
		}
		return fmt.Errorf("invalid milvus connection type")
	case "weaviate":
		if client, ok := client.DB.(*weaviate.WeaviateClient); ok {
			client.Close()
			return nil
		}
		return fmt.Errorf("invalid weaviate connection type")
	case "elasticsearch":
		if client, ok := client.DB.(*elasticsearch.ElasticsearchClient); ok {
			client.Close()
			return nil
		}
		return fmt.Errorf("invalid elasticsearch connection type")
	case "neo4j":
		if driver, ok := client.DB.(neo4jgo.DriverWithContext); ok {
			driver.Close(context.Background())
			return nil
		}
		return fmt.Errorf("invalid neo4j connection type")
	case "dynamodb":
		// DynamoDB client doesn't need explicit close
		return nil
	case "cosmosdb":
		// CosmosDB client doesn't need explicit close
		return nil
	//case "db2":
	//	if db, ok := client.DB.(*sql.DB); ok {
	//		return db.Close()
	//	}
	//	return fmt.Errorf("invalid db2 connection type")
	//case "oracle":
	//	if db, ok := client.DB.(*sql.DB); ok {
	//		return db.Close()
	//	}
	//	return fmt.Errorf("invalid oracle connection type")
	default:
		return fmt.Errorf("unsupported database type: %s", client.DatabaseType)
	}
}

// ConnectInstance establishes a connection to a database instance
func (dm *DatabaseManager) ConnectInstance(config common.InstanceConfig) (*common.InstanceClient, error) {
	// Log instance connection attempt with unified logging
	if dm.dbLogger != nil {
		dm.dbLogger.LogConnectionAttempt(DatabaseLogContext{
			DatabaseType: config.ConnectionType,
			InstanceID:   config.InstanceID,
			Host:         config.Host,
			Port:         config.Port,
			IsInternal:   false,
		})
	}

	var client *common.InstanceClient
	var err error

	switch config.ConnectionType {
	case "postgres":
		client, err = postgres.ConnectInstance(config)
	case "mysql":
		client, err = mysql.ConnectInstance(config)
	case "mariadb":
		client, err = mariadb.ConnectInstance(config)
	case "cockroach":
		client, err = cockroach.ConnectInstance(config)
	case "redis":
		client, err = redis.ConnectInstance(config)
	case "mongodb":
		client, err = mongodb.ConnectInstance(config)
	case "mssql":
		client, err = mssql.ConnectInstance(config)
	case "cassandra":
		client, err = cassandra.ConnectInstance(config)
	case "edgedb":
		client, err = edgedb.ConnectInstance(config)
	case "snowflake":
		client, err = snowflake.ConnectInstance(config)
	case "clickhouse":
		client, err = clickhouse.ConnectInstance(config)
	case "pinecone":
		client, err = pinecone.ConnectInstance(config)
	case "chroma":
		client, err = chroma.ConnectInstance(config)
	case "milvus":
		client, err = milvus.ConnectInstance(config)
	case "weaviate":
		client, err = weaviate.ConnectInstance(config)
	case "elasticsearch":
		client, err = elasticsearch.ConnectInstance(config)
	case "neo4j":
		client, err = neo4j.ConnectInstance(config)
	case "dynamodb":
		client, err = dynamodb.ConnectInstance(config)
	case "cosmosdb":
		client, err = cosmosdb.ConnectInstance(config)
	//case "db2":
	//	client, err = db2.ConnectInstance(config)
	//case "oracle":
	//	client, err = oracle.ConnectInstance(config)
	default:
		// Log unsupported instance type as a warning
		if dm.dbLogger != nil {
			dm.dbLogger.LogConnectionFailure(DatabaseLogContext{
				DatabaseType: config.ConnectionType,
				InstanceID:   config.InstanceID,
				Host:         config.Host,
				Port:         config.Port,
				IsInternal:   false,
			}, fmt.Errorf("unsupported instance type: %s", config.ConnectionType))
		}
		return nil, fmt.Errorf("unsupported instance type: %s", config.ConnectionType)
	}

	if err != nil {
		// Log instance connection failure as warning for client instances
		if dm.dbLogger != nil {
			dm.dbLogger.LogConnectionFailure(DatabaseLogContext{
				DatabaseType: config.ConnectionType,
				InstanceID:   config.InstanceID,
				Host:         config.Host,
				Port:         config.Port,
				IsInternal:   false,
			}, err)
		}
		return nil, err
	}

	// Log successful instance connection
	if dm.dbLogger != nil {
		dm.dbLogger.LogConnectionSuccess(DatabaseLogContext{
			DatabaseType: config.ConnectionType,
			InstanceID:   config.InstanceID,
			Host:         config.Host,
			Port:         config.Port,
			IsInternal:   false,
		})
	}

	dm.mu.Lock()
	dm.instanceClients[config.InstanceID] = client
	dm.mu.Unlock()

	return client, nil
}

// DisconnectInstance closes an instance connection
func (dm *DatabaseManager) DisconnectInstance(id string) error {
	dm.safeLog("info", "Disconnecting instance %s", id)
	dm.mu.Lock()
	defer dm.mu.Unlock()

	client, exists := dm.instanceClients[id]
	if !exists {
		dm.safeLog("info", "Instance %s already disconnected or not found", id)
		return nil // Don't return error - instance is already disconnected
	}

	if err := closeInstance(client); err != nil {
		dm.safeLog("error", "Failed to close instance %s: %v", id, err)
		// Still remove from map even if close failed to prevent orphaned entries
		delete(dm.instanceClients, id)
		return err
	}

	delete(dm.instanceClients, id)
	dm.safeLog("info", "Successfully disconnected instance %s", id)
	return nil
}

// closeInstance closes the database connection properly based on the type
func closeInstance(client *common.InstanceClient) error {
	atomic.StoreInt32(&client.IsConnected, 0)

	switch client.InstanceType {
	case "postgres":
		if pool, ok := client.DB.(*pgxpool.Pool); ok {
			pool.Close()
			return nil
		}
		return fmt.Errorf("invalid postgres connection type")
	case "mysql":
		if db, ok := client.DB.(*sql.DB); ok {
			return db.Close()
		}
		return fmt.Errorf("invalid mysql connection type")
	case "mariadb":
		if db, ok := client.DB.(*sql.DB); ok {
			return db.Close()
		}
		return fmt.Errorf("invalid mariadb connection type")
	case "cockroach":
		if pool, ok := client.DB.(*pgxpool.Pool); ok {
			pool.Close()
			return nil
		}
		return fmt.Errorf("invalid cockroach connection type")
	case "redis":
		if client, ok := client.DB.(*goredis.Client); ok {
			client.Close()
			return nil
		}
		return fmt.Errorf("invalid redis connection type")
	case "mongodb":
		if database, ok := client.DB.(*mongo.Database); ok {
			return database.Client().Disconnect(context.Background())
		}
		return fmt.Errorf("invalid mongodb connection type")
	case "mssql":
		if db, ok := client.DB.(*sql.DB); ok {
			return db.Close()
		}
		return fmt.Errorf("invalid mssql connection type")
	case "cassandra":
		if session, ok := client.DB.(*gocql.Session); ok {
			session.Close()
			return nil
		}
		return fmt.Errorf("invalid cassandra connection type")
	case "edgedb":
		if client, ok := client.DB.(*gel.Client); ok {
			client.Close()
			return nil
		}
		return fmt.Errorf("invalid edgedb connection type")
	case "snowflake":
		if db, ok := client.DB.(*sql.DB); ok {
			return db.Close()
		}
		return fmt.Errorf("invalid snowflake connection type")
	case "clickhouse":
		if conn, ok := client.DB.(clickhouse.ClickhouseConn); ok {
			conn.Close()
			return nil
		}
		return fmt.Errorf("invalid clickhouse connection type")
	case "pinecone":
		if client, ok := client.DB.(*pinecone.PineconeClient); ok {
			client.Close()
			return nil
		}
		return fmt.Errorf("invalid pinecone connection type")
	case "chroma":
		if client, ok := client.DB.(*chroma.ChromaClient); ok {
			client.Close()
			return nil
		}
		return fmt.Errorf("invalid chroma connection type")
	case "milvus":
		if client, ok := client.DB.(*milvus.MilvusClient); ok {
			client.Close()
			return nil
		}
		return fmt.Errorf("invalid milvus connection type")
	case "weaviate":
		if client, ok := client.DB.(*weaviate.WeaviateClient); ok {
			client.Close()
			return nil
		}
		return fmt.Errorf("invalid weaviate connection type")
	case "elasticsearch":
		if client, ok := client.DB.(*elasticsearch.ElasticsearchClient); ok {
			client.Close()
			return nil
		}
		return fmt.Errorf("invalid elasticsearch connection type")
	case "neo4j":
		if driver, ok := client.DB.(neo4jgo.DriverWithContext); ok {
			driver.Close(context.Background())
			return nil
		}
		return fmt.Errorf("invalid neo4j connection type")
	case "dynamodb":
		// No explicit close method for aws dynamodb client
		return fmt.Errorf("invalid dynamodb connection type")
	case "cosmosdb":
		// No explicit close method for azure cosmos db client
		return fmt.Errorf("invalid cosmosdb connection type")
	//case "db2":
	//	if db, ok := client.DB.(*sql.DB); ok {
	//		return db.Close()
	//	}
	//	return fmt.Errorf("invalid db2 connection type")
	//case "oracle":
	//	if db, ok := client.DB.(*sql.DB); ok {
	//		return db.Close()
	//	}
	//	return fmt.Errorf("invalid oracle connection type")
	default:
		return fmt.Errorf("unsupported instance type: %s", client.InstanceType)
	}
}

// Refactor ConnectReplication for multi-table support
func (dm *DatabaseManager) ConnectReplication(config common.ReplicationConfig) (*common.ReplicationClient, error) {
	// Log replication connection attempt
	if dm.dbLogger != nil {
		dm.dbLogger.LogReplicationEvent(DatabaseLogContext{
			DatabaseType:  config.ConnectionType,
			DatabaseID:    config.DatabaseID,
			ReplicationID: config.ReplicationID,
			IsInternal:    false,
		}, "Connecting replication client", fmt.Sprintf("tables=%v", config.TableNames))
	}

	// Validate that the main database connection exists
	_, err := dm.GetDatabaseClient(config.DatabaseID)
	if err != nil {
		// Log replication validation failure
		if dm.dbLogger != nil {
			dm.dbLogger.LogReplicationError(DatabaseLogContext{
				DatabaseType:  config.ConnectionType,
				DatabaseID:    config.DatabaseID,
				ReplicationID: config.ReplicationID,
				IsInternal:    false,
			}, fmt.Errorf("database client %s must be connected before creating replication connection: %w", config.DatabaseID, err))
		}
		return nil, fmt.Errorf("database client %s must be connected before creating replication connection: %w", config.DatabaseID, err)
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Check if a replication client already exists for this database
	client, exists := dm.replicationClients[config.ReplicationID]
	if exists {
		// Add new tables to the existing client
		for _, t := range config.TableNames {
			client.AddTable(t)
		}
		// Log table addition to existing replication client
		if dm.dbLogger != nil {
			dm.dbLogger.LogReplicationEvent(DatabaseLogContext{
				DatabaseType:  config.ConnectionType,
				DatabaseID:    config.DatabaseID,
				ReplicationID: config.ReplicationID,
				IsInternal:    false,
			}, "Added tables to existing replication client", config.TableNames)
		}
		return client, nil
	}

	// No client exists, create a new one
	if config.ConnectionType == "postgres" {
		var err error
		client, _, err = postgres.ConnectReplication(config)
		if err != nil {
			// Log replication connection failure
			if dm.dbLogger != nil {
				dm.dbLogger.LogReplicationError(DatabaseLogContext{
					DatabaseType:  config.ConnectionType,
					DatabaseID:    config.DatabaseID,
					ReplicationID: config.ReplicationID,
					IsInternal:    false,
				}, fmt.Errorf("failed to connect replication client: %w", err))
			}
			return nil, fmt.Errorf("failed to connect replication client: %w", err)
		}
	} else {
		err := fmt.Errorf("replication not yet implemented for type: %s", config.ConnectionType)
		// Log unsupported replication type
		if dm.dbLogger != nil {
			dm.dbLogger.LogReplicationError(DatabaseLogContext{
				DatabaseType:  config.ConnectionType,
				DatabaseID:    config.DatabaseID,
				ReplicationID: config.ReplicationID,
				IsInternal:    false,
			}, err)
		}
		return nil, err
	}

	// Add all tables to the client
	for _, t := range config.TableNames {
		client.AddTable(t)
	}

	dm.replicationClients[config.ReplicationID] = client

	// Log successful replication connection
	if dm.dbLogger != nil {
		dm.dbLogger.LogReplicationEvent(DatabaseLogContext{
			DatabaseType:  config.ConnectionType,
			DatabaseID:    config.DatabaseID,
			ReplicationID: config.ReplicationID,
			IsInternal:    false,
		}, "Successfully connected replication client", fmt.Sprintf("tables=%v", config.TableNames))
	}

	return client, nil
}

// Refactor DisconnectReplication for multi-table support
func (dm *DatabaseManager) DisconnectReplication(id string) error {
	dm.safeLog("info", "Disconnecting replication client %s", id)
	dm.mu.Lock()
	defer dm.mu.Unlock()

	client, exists := dm.replicationClients[id]
	if !exists {
		dm.safeLog("info", "Replication client %s already disconnected or not found", id)
		return nil // Don't return error - replication client is already disconnected
	}

	// Only disconnect if no tables remain
	if len(client.TableNames) > 0 {
		dm.safeLog("info", "Replication client %s still has tables: %v, not disconnecting", id, client.GetTables())
		return nil
	}

	if err := closeReplication(client); err != nil {
		dm.safeLog("error", "Failed to close replication client %s: %v", id, err)
		delete(dm.replicationClients, id)
		return err
	}

	delete(dm.replicationClients, id)
	dm.safeLog("info", "Successfully disconnected replication client %s", id)
	return nil
}

// closeReplication closes the replication connection properly based on the type
func closeReplication(client *common.ReplicationClient) error {
	atomic.StoreInt32(&client.IsConnected, 0)

	// Close the replication source if it implements the interface
	if source, ok := client.ReplicationSource.(common.ReplicationSourceInterface); ok {
		if err := source.Close(); err != nil {
			return fmt.Errorf("failed to close replication source: %w", err)
		}
	}

	// Close the underlying connection based on database type
	switch client.DatabaseType {
	case "postgres":
		if replicationSource, ok := client.ReplicationSource.(*postgres.PostgresReplicationSourceDetails); ok {
			if replicationSource.ReplicationConn != nil {
				replicationSource.ReplicationConn.Close(context.Background())
			}
			if replicationSource.StopChan != nil {
				close(replicationSource.StopChan)
			}
		}
		return nil
	case "mysql", "mariadb", "mssql", "snowflake":
		// SQL-based databases might use regular database connections for replication
		if db, ok := client.Connection.(*sql.DB); ok {
			return db.Close()
		}
		return nil
	case "mongodb":
		if database, ok := client.Connection.(*mongo.Database); ok {
			return database.Client().Disconnect(context.Background())
		}
		return nil
	case "redis":
		if redisClient, ok := client.Connection.(*goredis.Client); ok {
			redisClient.Close()
		}
		return nil
	case "cassandra":
		if session, ok := client.Connection.(*gocql.Session); ok {
			session.Close()
		}
		return nil
	case "elasticsearch":
		if esClient, ok := client.Connection.(*elasticsearch.ElasticsearchClient); ok {
			esClient.Close()
		}
		return nil
	case "neo4j":
		if driver, ok := client.Connection.(neo4jgo.DriverWithContext); ok {
			driver.Close(context.Background())
		}
		return nil
	case "edgedb":
		if edgeClient, ok := client.Connection.(*gel.Client); ok {
			edgeClient.Close()
		}
		return nil
	case "clickhouse":
		if conn, ok := client.Connection.(clickhouse.ClickhouseConn); ok {
			conn.Close()
		}
		return nil
	case "pinecone":
		if pineconeClient, ok := client.Connection.(*pinecone.PineconeClient); ok {
			pineconeClient.Close()
		}
		return nil
	case "dynamodb":
		// No explicit close method for aws dynamodb client
		return nil
	case "cosmosdb":
		// No explicit close method for azure cosmos db client
		return nil
	//case "db2":
	//	if db, ok := client.Connection.(*sql.DB); ok {
	//		return db.Close()
	//	}
	//	return nil
	//case "oracle":
	//	if db, ok := client.Connection.(*sql.DB); ok {
	//		return db.Close()
	//	}
	//	return nil
	default:
		return fmt.Errorf("unsupported replication type: %s", client.DatabaseType)
	}
}
