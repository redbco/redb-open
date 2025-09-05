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
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/cassandra"
	"github.com/redbco/redb-open/services/anchor/internal/database/chroma"
	"github.com/redbco/redb-open/services/anchor/internal/database/clickhouse"
	"github.com/redbco/redb-open/services/anchor/internal/database/cockroach"

	"github.com/redbco/redb-open/services/anchor/internal/database/cosmosdb"
	"github.com/redbco/redb-open/services/anchor/internal/database/dynamodb"
	"github.com/redbco/redb-open/services/anchor/internal/database/edgedb"
	"github.com/redbco/redb-open/services/anchor/internal/database/elasticsearch"
	"github.com/redbco/redb-open/services/anchor/internal/database/iceberg"
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

// GetDatabaseStructure returns the structure of a database as a UnifiedModel or legacy format
func (dm *DatabaseManager) GetDatabaseStructure(id string) (interface{}, error) {
	dm.safeLog("info", "Getting database structure for %s", id)
	client, err := dm.GetDatabaseClient(id)
	if err != nil {
		return nil, err
	}

	if atomic.LoadInt32(&client.IsConnected) == 0 {
		return nil, fmt.Errorf("database %s is disconnected", id)
	}

	switch client.DatabaseType {
	case string(dbcapabilities.PostgreSQL):
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return nil, fmt.Errorf("invalid postgres connection type")
		}
		// PostgreSQL now returns UnifiedModel
		return postgres.DiscoverSchema(pool)
	case string(dbcapabilities.MySQL):
		// MySQL now returns UnifiedModel
		return mysql.DiscoverSchema(client.DB)
	case string(dbcapabilities.MariaDB):
		// MariaDB now returns UnifiedModel
		return mariadb.DiscoverSchema(client.DB)
	case string(dbcapabilities.CockroachDB):
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return nil, fmt.Errorf("invalid cockroach connection type")
		}
		// CockroachDB now returns UnifiedModel
		return cockroach.DiscoverSchema(pool)
	case string(dbcapabilities.Redis):
		client, ok := client.DB.(*goredis.Client)
		if !ok {
			return nil, fmt.Errorf("invalid redis connection type")
		}
		// Redis now returns UnifiedModel
		return redis.DiscoverSchema(client)
	case string(dbcapabilities.MongoDB):
		db, ok := client.DB.(*mongo.Database)
		if !ok {
			return nil, fmt.Errorf("invalid mongodb connection type")
		}
		// MongoDB now returns UnifiedModel
		return mongodb.DiscoverSchema(db)
	case string(dbcapabilities.SQLServer):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return nil, fmt.Errorf("invalid mssql connection type")
		}
		// Microsoft SQL Server now returns UnifiedModel
		return mssql.DiscoverSchema(db)
	case string(dbcapabilities.Cassandra):
		session, ok := client.DB.(*gocql.Session)
		if !ok {
			return nil, fmt.Errorf("invalid cassandra connection type")
		}
		// Cassandra now returns UnifiedModel
		return cassandra.DiscoverSchema(session)
	case string(dbcapabilities.EdgeDB):
		gelClient, ok := client.DB.(*gel.Client)
		if !ok {
			return nil, fmt.Errorf("invalid edgedb connection type")
		}
		// EdgeDB now returns UnifiedModel
		return edgedb.DiscoverSchema(gelClient)
	case string(dbcapabilities.Snowflake):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return nil, fmt.Errorf("invalid snowflake connection type")
		}
		// Snowflake now returns UnifiedModel
		return snowflake.DiscoverSchema(db)
	case string(dbcapabilities.ClickHouse):
		conn, ok := client.DB.(clickhouse.ClickhouseConn)
		if !ok {
			return nil, fmt.Errorf("invalid clickhouse connection type")
		}
		// ClickHouse now returns UnifiedModel
		return clickhouse.DiscoverSchema(conn)
	case string(dbcapabilities.Pinecone):
		client, ok := client.DB.(*pinecone.PineconeClient)
		if !ok {
			return nil, fmt.Errorf("invalid pinecone connection type")
		}
		return pinecone.DiscoverSchema(client)
	case string(dbcapabilities.Chroma):
		client, ok := client.DB.(*chroma.ChromaClient)
		if !ok {
			return nil, fmt.Errorf("invalid chroma connection type")
		}
		return chroma.DiscoverSchema(client)
	case string(dbcapabilities.Milvus):
		client, ok := client.DB.(*milvus.MilvusClient)
		if !ok {
			return nil, fmt.Errorf("invalid milvus connection type")
		}
		return milvus.DiscoverSchema(client)
	case string(dbcapabilities.Weaviate):
		client, ok := client.DB.(*weaviate.WeaviateClient)
		if !ok {
			return nil, fmt.Errorf("invalid weaviate connection type")
		}
		// Weaviate now returns UnifiedModel
		return weaviate.DiscoverSchema(client)
	case string(dbcapabilities.Elasticsearch):
		client, ok := client.DB.(*elasticsearch.ElasticsearchClient)
		if !ok {
			return nil, fmt.Errorf("invalid elasticsearch connection type")
		}
		// Elasticsearch now returns UnifiedModel
		return elasticsearch.DiscoverSchema(client)
	case string(dbcapabilities.Neo4j):
		driver, ok := client.DB.(neo4jgo.DriverWithContext)
		if !ok {
			return nil, fmt.Errorf("invalid neo4j connection type")
		}
		// Neo4j now returns UnifiedModel
		return neo4j.DiscoverSchema(driver)
	case string(dbcapabilities.Iceberg):
		return iceberg.DiscoverSchema(client.DB)
	case string(dbcapabilities.CosmosDB):
		// CosmosDB now returns UnifiedModel
		return cosmosdb.DiscoverSchema(client.DB)
	case string(dbcapabilities.DynamoDB):
		// DynamoDB now returns UnifiedModel
		return dynamodb.DiscoverSchema(client.DB)

	//case string(dbcapabilities.DB2):
	//	db, ok := client.DB.(*sql.DB)
	//	if !ok {
	//		return nil, fmt.Errorf("invalid db2 connection type")
	//	}
	//	return db2.DiscoverSchema(db)
	//case string(dbcapabilities.Oracle):
	//	db, ok := client.DB.(*sql.DB)
	//	if !ok {
	//		return nil, fmt.Errorf("invalid oracle connection type")
	//	}
	//	return oracle.DiscoverSchema(db)
	default:
		return nil, fmt.Errorf("schema discovery not supported for database type: %s", client.DatabaseType)
	}
}

// DeployDatabaseStructure deploys a database structure from a UnifiedModel
func (dm *DatabaseManager) DeployDatabaseStructure(databaseID string, um *unifiedmodel.UnifiedModel) error {
	dm.safeLog("info", "Deploying database structure for %s", databaseID)

	client, err := dm.GetDatabaseClient(databaseID)
	if err != nil {
		return fmt.Errorf("failed to get database client: %w", err)
	}

	if atomic.LoadInt32(&client.IsConnected) == 0 {
		return fmt.Errorf("database %s is disconnected", databaseID)
	}

	switch client.DatabaseType {
	case string(dbcapabilities.PostgreSQL):
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return fmt.Errorf("invalid postgres connection type")
		}
		return postgres.CreateStructure(pool, um)
	//case string(dbcapabilities.MySQL):
	//	db, ok := client.DB.(*sql.DB)
	//	if !ok {
	//		return fmt.Errorf("invalid mysql connection type")
	//	}
	//	return mysql.CreateStructure(db, um)
	//case string(dbcapabilities.MariaDB):
	//	db, ok := client.DB.(*sql.DB)
	//	if !ok {
	//		return fmt.Errorf("invalid mariadb connection type")
	//	}
	//	return mariadb.CreateStructure(db, um)
	//case string(dbcapabilities.CockroachDB):
	//	pool, ok := client.DB.(*pgxpool.Pool)
	//	if !ok {
	//		return fmt.Errorf("invalid cockroach connection type")
	//	}
	//	return cockroach.CreateStructure(pool, um)
	// Redis does not have a schema deployment feature
	//case string(dbcapabilities.Redis):
	//	client, ok := client.DB.(*goredis.Client)
	//	if !ok {
	//		return fmt.Errorf("invalid redis connection type")
	//	}
	//	return redis.CreateStructure(client, structure)
	//case string(dbcapabilities.MongoDB):
	//	db, ok := client.DB.(*mongo.Database)
	//	if !ok {
	//		return fmt.Errorf("invalid mongodb connection type")
	//	}
	//	return mongodb.CreateStructure(db, um)
	//case string(dbcapabilities.SQLServer):
	//	db, ok := client.DB.(*sql.DB)
	//	if !ok {
	//		return fmt.Errorf("invalid mssql connection type")
	//	}
	//	return mssql.CreateStructure(db, um)
	//case string(dbcapabilities.Cassandra):
	//	session, ok := client.DB.(*gocql.Session)
	//	if !ok {
	//		return fmt.Errorf("invalid cassandra connection type")
	//	}
	//	return cassandra.CreateStructure(session, um)
	//case string(dbcapabilities.EdgeDB):
	//	gelClient, ok := client.DB.(*gel.Client)
	//	if !ok {
	//		return fmt.Errorf("invalid edgedb connection type")
	//	}
	//	return edgedb.CreateStructure(gelClient, um)
	//case string(dbcapabilities.Snowflake):
	//	db, ok := client.DB.(*sql.DB)
	//	if !ok {
	//		return fmt.Errorf("invalid snowflake connection type")
	//	}
	//	return snowflake.CreateStructure(db, um)
	//case string(dbcapabilities.ClickHouse):
	//	conn, ok := client.DB.(clickhouse.ClickhouseConn)
	//	if !ok {
	//		return fmt.Errorf("invalid clickhouse connection type")
	//	}
	//	return clickhouse.CreateStructure(conn, um)
	//case string(dbcapabilities.Pinecone):
	//	client, ok := client.DB.(*pinecone.PineconeClient)
	//	if !ok {
	//		return fmt.Errorf("invalid pinecone connection type")
	//	}
	//	return pinecone.CreateStructure(client, um)
	//case string(dbcapabilities.Chroma):
	//	client, ok := client.DB.(*chroma.ChromaClient)
	//	if !ok {
	//		return fmt.Errorf("invalid chroma connection type")
	//	}
	//	return chroma.CreateStructure(client, um)
	//case string(dbcapabilities.Milvus):
	//	client, ok := client.DB.(*milvus.MilvusClient)
	//	if !ok {
	//		return fmt.Errorf("invalid milvus connection type")
	//	}
	//	return milvus.CreateStructure(client, um)
	//case string(dbcapabilities.Weaviate):
	//	client, ok := client.DB.(*weaviate.WeaviateClient)
	//	if !ok {
	//		return fmt.Errorf("invalid weaviate connection type")
	//	}
	//	return weaviate.CreateStructure(client, um)
	//case string(dbcapabilities.Elasticsearch):
	//	client, ok := client.DB.(*elasticsearch.ElasticsearchClient)
	//	if !ok {
	//		return fmt.Errorf("invalid elasticsearch connection type")
	//	}
	//	return elasticsearch.CreateStructure(client, um)
	//case string(dbcapabilities.Neo4j):
	//	driver, ok := client.DB.(neo4jgo.DriverWithContext)
	//	if !ok {
	//		return fmt.Errorf("invalid neo4j connection type")
	//	}
	//	return neo4j.CreateStructure(driver, um)
	//case string(dbcapabilities.Iceberg):
	//	return iceberg.CreateStructure(client.DB, um)
	default:
		// For databases not yet refactored to use UnifiedModel natively,
		// we'll need to implement them one by one following the PostgreSQL pattern
		return fmt.Errorf("schema deployment from UnifiedModel not yet implemented for database type: %s. Please refactor this adapter to work with UnifiedModel directly", client.DatabaseType)
	}
}

// CreateDatabase creates a new database
func (dm *DatabaseManager) CreateDatabase(databaseID string, options map[string]interface{}) error {
	dm.safeLog("info", "Creating database %s with options %v", databaseID, options)

	client, err := dm.GetDatabaseClient(databaseID)
	if err != nil {
		return fmt.Errorf("failed to get database client: %w", err)
	}

	if atomic.LoadInt32(&client.IsConnected) == 0 {
		return fmt.Errorf("database %s is disconnected", databaseID)
	}

	switch client.DatabaseType {
	case string(dbcapabilities.PostgreSQL):
		return postgres.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.MySQL):
		return mysql.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.MariaDB):
		return mariadb.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.CockroachDB):
		return cockroach.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.Redis):
		return redis.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.MongoDB):
		return mongodb.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.SQLServer):
		return mssql.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.Cassandra):
		return cassandra.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.EdgeDB):
		return edgedb.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.Snowflake):
		return snowflake.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.ClickHouse):
		return clickhouse.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.Pinecone):
		return pinecone.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.Elasticsearch):
		return elasticsearch.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.Neo4j):
		return neo4j.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.Iceberg):
		return iceberg.CreateDatabase(context.Background(), client.DB, databaseID, options)
	default:
		return fmt.Errorf("unsupported database type: %s", client.DatabaseType)
	}
}

// DropDatabase drops a database
func (dm *DatabaseManager) DropDatabase(databaseID string, options map[string]interface{}) error {
	dm.safeLog("info", "Dropping database %s with options %v", databaseID, options)

	client, err := dm.GetDatabaseClient(databaseID)
	if err != nil {
		return fmt.Errorf("failed to get database client: %w", err)
	}

	if atomic.LoadInt32(&client.IsConnected) == 0 {
		return fmt.Errorf("database %s is disconnected", databaseID)
	}

	switch client.DatabaseType {
	case string(dbcapabilities.PostgreSQL):
		return postgres.DropDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.MySQL):
		return mysql.DropDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.MariaDB):
		return mariadb.DropDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.CockroachDB):
		return cockroach.DropDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.Redis):
		return redis.DropDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.MongoDB):
		return mongodb.DropDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.SQLServer):
		return mssql.DropDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.Cassandra):
		return cassandra.DropDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.EdgeDB):
		return edgedb.DropDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.Snowflake):
		return snowflake.DropDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.ClickHouse):
		return clickhouse.DropDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.Pinecone):
		return pinecone.DropDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.Elasticsearch):
		return elasticsearch.DropDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.Neo4j):
		return neo4j.DropDatabase(context.Background(), client.DB, databaseID, options)
	case string(dbcapabilities.Iceberg):
		return iceberg.DropDatabase(context.Background(), client.DB, databaseID, options)
	default:
		return fmt.Errorf("unsupported database type: %s", client.DatabaseType)
	}
}
