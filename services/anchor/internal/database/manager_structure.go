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

// GetDatabaseStructure returns the structure of a database
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
	case "postgres":
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return nil, fmt.Errorf("invalid postgres connection type")
		}
		return postgres.DiscoverSchema(pool)
	case "mysql":
		return mysql.DiscoverSchema(client.DB)
	case "mariadb":
		return mariadb.DiscoverSchema(client.DB)
	case "cockroach":
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return nil, fmt.Errorf("invalid cockroach connection type")
		}
		return cockroach.DiscoverSchema(pool)
	case "redis":
		client, ok := client.DB.(*goredis.Client)
		if !ok {
			return nil, fmt.Errorf("invalid redis connection type")
		}
		return redis.DiscoverSchema(client)
	case "mongodb":
		db, ok := client.DB.(*mongo.Database)
		if !ok {
			return nil, fmt.Errorf("invalid mongodb connection type")
		}
		return mongodb.DiscoverSchema(db)
	case "mssql":
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return nil, fmt.Errorf("invalid mssql connection type")
		}
		return mssql.DiscoverSchema(db)
	case "cassandra":
		session, ok := client.DB.(*gocql.Session)
		if !ok {
			return nil, fmt.Errorf("invalid cassandra connection type")
		}
		return cassandra.DiscoverSchema(session)
	case "edgedb":
		gelClient, ok := client.DB.(*gel.Client)
		if !ok {
			return nil, fmt.Errorf("invalid edgedb connection type")
		}
		return edgedb.DiscoverSchema(gelClient)
	case "snowflake":
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return nil, fmt.Errorf("invalid snowflake connection type")
		}
		return snowflake.DiscoverSchema(db)
	case "clickhouse":
		conn, ok := client.DB.(clickhouse.ClickhouseConn)
		if !ok {
			return nil, fmt.Errorf("invalid clickhouse connection type")
		}
		return clickhouse.DiscoverSchema(conn)
	case "pinecone":
		client, ok := client.DB.(*pinecone.PineconeClient)
		if !ok {
			return nil, fmt.Errorf("invalid pinecone connection type")
		}
		return pinecone.DiscoverSchema(client)
	case "chroma":
		client, ok := client.DB.(*chroma.ChromaClient)
		if !ok {
			return nil, fmt.Errorf("invalid chroma connection type")
		}
		return chroma.DiscoverSchema(client)
	case "milvus":
		client, ok := client.DB.(*milvus.MilvusClient)
		if !ok {
			return nil, fmt.Errorf("invalid milvus connection type")
		}
		return milvus.DiscoverSchema(client)
	case "weaviate":
		client, ok := client.DB.(*weaviate.WeaviateClient)
		if !ok {
			return nil, fmt.Errorf("invalid weaviate connection type")
		}
		return weaviate.DiscoverSchema(client)
	case "elasticsearch":
		client, ok := client.DB.(*elasticsearch.ElasticsearchClient)
		if !ok {
			return nil, fmt.Errorf("invalid elasticsearch connection type")
		}
		return elasticsearch.DiscoverSchema(client)
	case "neo4j":
		driver, ok := client.DB.(neo4jgo.DriverWithContext)
		if !ok {
			return nil, fmt.Errorf("invalid neo4j connection type")
		}
		return neo4j.DiscoverSchema(driver)
	//case "db2":
	//	db, ok := client.DB.(*sql.DB)
	//	if !ok {
	//		return nil, fmt.Errorf("invalid db2 connection type")
	//	}
	//	return db2.DiscoverSchema(db)
	//case "oracle":
	//	db, ok := client.DB.(*sql.DB)
	//	if !ok {
	//		return nil, fmt.Errorf("invalid oracle connection type")
	//	}
	//	return oracle.DiscoverSchema(db)
	default:
		return nil, fmt.Errorf("schema discovery not supported for database type: %s", client.DatabaseType)
	}
}

// DeployDatabaseStructure deploys a database structure to a database
func (dm *DatabaseManager) DeployDatabaseStructure(databaseID string, structure common.StructureParams) error {
	dm.safeLog("info", "Deploying database structure for %s", databaseID)

	client, err := dm.GetDatabaseClient(databaseID)
	if err != nil {
		return fmt.Errorf("failed to get database client: %w", err)
	}

	if atomic.LoadInt32(&client.IsConnected) == 0 {
		return fmt.Errorf("database %s is disconnected", databaseID)
	}

	switch client.DatabaseType {
	case "postgres":
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return fmt.Errorf("invalid postgres connection type")
		}
		return postgres.CreateStructure(pool, structure)
	case "mysql":
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return fmt.Errorf("invalid mysql connection type")
		}
		return mysql.CreateStructure(db, structure)
	case "mariadb":
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return fmt.Errorf("invalid mariadb connection type")
		}
		return mariadb.CreateStructure(db, structure)
	case "cockroach":
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return fmt.Errorf("invalid cockroach connection type")
		}
		return cockroach.CreateStructure(pool, structure)
	// Redis does not have a schema deployment feature
	//case "redis":
	//	client, ok := client.DB.(*goredis.Client)
	//	if !ok {
	//		return fmt.Errorf("invalid redis connection type")
	//	}
	//	return redis.CreateStructure(client, structure)
	case "mongodb":
		db, ok := client.DB.(*mongo.Database)
		if !ok {
			return fmt.Errorf("invalid mongodb connection type")
		}
		return mongodb.CreateStructure(db, structure)
	case "mssql":
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return fmt.Errorf("invalid mssql connection type")
		}
		return mssql.CreateStructure(db, structure)
	case "cassandra":
		session, ok := client.DB.(*gocql.Session)
		if !ok {
			return fmt.Errorf("invalid cassandra connection type")
		}
		return cassandra.CreateStructure(session, structure)
	case "edgedb":
		gelClient, ok := client.DB.(*gel.Client)
		if !ok {
			return fmt.Errorf("invalid edgedb connection type")
		}
		return edgedb.CreateStructure(gelClient, structure)
	case "snowflake":
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return fmt.Errorf("invalid snowflake connection type")
		}
		return snowflake.CreateStructure(db, structure)
	case "clickhouse":
		conn, ok := client.DB.(clickhouse.ClickhouseConn)
		if !ok {
			return fmt.Errorf("invalid clickhouse connection type")
		}
		return clickhouse.CreateStructure(conn, structure)
	case "pinecone":
		client, ok := client.DB.(*pinecone.PineconeClient)
		if !ok {
			return fmt.Errorf("invalid pinecone connection type")
		}
		return pinecone.CreateStructure(client, structure)
	case "chroma":
		client, ok := client.DB.(*chroma.ChromaClient)
		if !ok {
			return fmt.Errorf("invalid chroma connection type")
		}
		return chroma.CreateStructure(client, structure)
	case "milvus":
		client, ok := client.DB.(*milvus.MilvusClient)
		if !ok {
			return fmt.Errorf("invalid milvus connection type")
		}
		return milvus.CreateStructure(client, structure)
	case "weaviate":
		client, ok := client.DB.(*weaviate.WeaviateClient)
		if !ok {
			return fmt.Errorf("invalid weaviate connection type")
		}
		return weaviate.CreateStructure(client, structure)
	case "elasticsearch":
		client, ok := client.DB.(*elasticsearch.ElasticsearchClient)
		if !ok {
			return fmt.Errorf("invalid elasticsearch connection type")
		}
		return elasticsearch.CreateStructure(client, structure)
	case "neo4j":
		driver, ok := client.DB.(neo4jgo.DriverWithContext)
		if !ok {
			return fmt.Errorf("invalid neo4j connection type")
		}
		return neo4j.CreateStructure(driver, structure)
	default:
		return fmt.Errorf("schema deployment not supported for database type: %s", client.DatabaseType)
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
	case "postgres":
		return postgres.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case "mysql":
		return mysql.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case "mariadb":
		return mariadb.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case "cockroach":
		return cockroach.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case "redis":
		return redis.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case "mongodb":
		return mongodb.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case "mssql":
		return mssql.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case "cassandra":
		return cassandra.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case "edgedb":
		return edgedb.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case "snowflake":
		return snowflake.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case "clickhouse":
		return clickhouse.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case "pinecone":
		return pinecone.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case "elasticsearch":
		return elasticsearch.CreateDatabase(context.Background(), client.DB, databaseID, options)
	case "neo4j":
		return neo4j.CreateDatabase(context.Background(), client.DB, databaseID, options)
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
	case "postgres":
		return postgres.DropDatabase(context.Background(), client.DB, databaseID, options)
	case "mysql":
		return mysql.DropDatabase(context.Background(), client.DB, databaseID, options)
	case "mariadb":
		return mariadb.DropDatabase(context.Background(), client.DB, databaseID, options)
	case "cockroach":
		return cockroach.DropDatabase(context.Background(), client.DB, databaseID, options)
	case "redis":
		return redis.DropDatabase(context.Background(), client.DB, databaseID, options)
	case "mongodb":
		return mongodb.DropDatabase(context.Background(), client.DB, databaseID, options)
	case "mssql":
		return mssql.DropDatabase(context.Background(), client.DB, databaseID, options)
	case "cassandra":
		return cassandra.DropDatabase(context.Background(), client.DB, databaseID, options)
	case "edgedb":
		return edgedb.DropDatabase(context.Background(), client.DB, databaseID, options)
	case "snowflake":
		return snowflake.DropDatabase(context.Background(), client.DB, databaseID, options)
	case "clickhouse":
		return clickhouse.DropDatabase(context.Background(), client.DB, databaseID, options)
	case "pinecone":
		return pinecone.DropDatabase(context.Background(), client.DB, databaseID, options)
	case "elasticsearch":
		return elasticsearch.DropDatabase(context.Background(), client.DB, databaseID, options)
	case "neo4j":
		return neo4j.DropDatabase(context.Background(), client.DB, databaseID, options)
	default:
		return fmt.Errorf("unsupported database type: %s", client.DatabaseType)
	}
}
