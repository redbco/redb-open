package database

import (
	"database/sql"
	"fmt"
	"sync/atomic"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/geldata/gel-go"
	"github.com/gocql/gocql"
	"github.com/jackc/pgx/v5/pgxpool"
	neo4jgo "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
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

// GetDataFromDatabase retrieves data from a specific table in a database
func (dm *DatabaseManager) GetDataFromDatabase(databaseID string, tableName string, limit int) ([]map[string]interface{}, error) {
	dm.safeLog("info", "Getting data from database %s, table %s", databaseID, tableName)

	client, err := dm.GetDatabaseClient(databaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database client: %w", err)
	}

	if atomic.LoadInt32(&client.IsConnected) == 0 {
		return nil, fmt.Errorf("database %s is disconnected", databaseID)
	}

	switch client.DatabaseType {
	case string(dbcapabilities.PostgreSQL):
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return nil, fmt.Errorf("invalid postgres connection type")
		}
		return postgres.FetchData(pool, tableName, limit)
	case string(dbcapabilities.MySQL):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return nil, fmt.Errorf("invalid mysql connection type")
		}
		return mysql.FetchData(db, tableName, limit, dm.logger)
	case string(dbcapabilities.MariaDB):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return nil, fmt.Errorf("invalid mariadb connection type")
		}
		return mariadb.FetchData(db, tableName, limit)
	case string(dbcapabilities.CockroachDB):
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return nil, fmt.Errorf("invalid cockroach connection type")
		}
		return cockroach.FetchData(pool, tableName, limit)
	case string(dbcapabilities.Redis):
		client, ok := client.DB.(*goredis.Client)
		if !ok {
			return nil, fmt.Errorf("invalid redis connection type")
		}
		return redis.FetchData(client, tableName, limit)
	case string(dbcapabilities.MongoDB):
		db, ok := client.DB.(*mongo.Database)
		if !ok {
			return nil, fmt.Errorf("invalid mongodb connection type")
		}
		return mongodb.FetchData(db, tableName, limit)
	case string(dbcapabilities.SQLServer):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return nil, fmt.Errorf("invalid mssql connection type")
		}
		return mssql.FetchData(db, tableName, limit)
	case string(dbcapabilities.Cassandra):
		session, ok := client.DB.(*gocql.Session)
		if !ok {
			return nil, fmt.Errorf("invalid cassandra connection type")
		}
		return cassandra.FetchData(session, tableName, limit)
	case string(dbcapabilities.EdgeDB):
		gelClient, ok := client.DB.(*gel.Client)
		if !ok {
			return nil, fmt.Errorf("invalid edgedb connection type")
		}
		return edgedb.FetchData(gelClient, tableName, limit)
	case string(dbcapabilities.Snowflake):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return nil, fmt.Errorf("invalid snowflake connection type")
		}
		return snowflake.FetchData(db, tableName, limit)
	case string(dbcapabilities.ClickHouse):
		conn, ok := client.DB.(clickhouse.ClickhouseConn)
		if !ok {
			return nil, fmt.Errorf("invalid clickhouse connection type")
		}
		return clickhouse.FetchData(conn, tableName, limit)
	case string(dbcapabilities.Pinecone):
		client, ok := client.DB.(*pinecone.PineconeClient)
		if !ok {
			return nil, fmt.Errorf("invalid pinecone connection type")
		}
		return pinecone.FetchData(client, tableName, "", limit)
	case string(dbcapabilities.Chroma):
		client, ok := client.DB.(*chroma.ChromaClient)
		if !ok {
			return nil, fmt.Errorf("invalid chroma connection type")
		}
		return chroma.FetchData(client, tableName, limit)
	case string(dbcapabilities.Milvus):
		client, ok := client.DB.(*milvus.MilvusClient)
		if !ok {
			return nil, fmt.Errorf("invalid milvus connection type")
		}
		return milvus.FetchData(client, tableName, limit)
	case string(dbcapabilities.Weaviate):
		client, ok := client.DB.(*weaviate.WeaviateClient)
		if !ok {
			return nil, fmt.Errorf("invalid weaviate connection type")
		}
		return weaviate.FetchData(client, tableName, limit)
	case string(dbcapabilities.Elasticsearch):
		client, ok := client.DB.(*elasticsearch.ElasticsearchClient)
		if !ok {
			return nil, fmt.Errorf("invalid elasticsearch connection type")
		}
		return elasticsearch.FetchData(client, tableName, limit)
	case string(dbcapabilities.Neo4j):
		driver, ok := client.DB.(neo4jgo.DriverWithContext)
		if !ok {
			return nil, fmt.Errorf("invalid neo4j connection type")
		}
		return neo4j.FetchData(driver, tableName, false, limit)
	case string(dbcapabilities.Iceberg):
		return iceberg.FetchData(client.DB, tableName, limit)
	case string(dbcapabilities.DynamoDB):
		dynamoClient, ok := client.DB.(*awsdynamodb.Client)
		if !ok {
			return nil, fmt.Errorf("invalid DynamoDB connection type")
		}
		return dynamodb.FetchData(dynamoClient, tableName, limit)
	case string(dbcapabilities.CosmosDB):
		cosmosClient, ok := client.DB.(*azcosmos.Client)
		if !ok {
			return nil, fmt.Errorf("invalid CosmosDB connection type")
		}
		return cosmosdb.FetchData(cosmosClient, tableName, limit)
	default:
		return nil, fmt.Errorf("data fetching not supported for database type: %s", client.DatabaseType)
	}
}

// InsertDataToDatabase inserts data into a specific table in a database
func (dm *DatabaseManager) InsertDataToDatabase(databaseID string, tableName string, data []map[string]interface{}) (int64, error) {
	dm.safeLog("info", "Inserting data into database %s, table %s", databaseID, tableName)

	client, err := dm.GetDatabaseClient(databaseID)
	if err != nil {
		return 0, fmt.Errorf("failed to get database client: %w", err)
	}

	if atomic.LoadInt32(&client.IsConnected) == 0 {
		return 0, fmt.Errorf("database %s is disconnected", databaseID)
	}

	switch client.DatabaseType {
	case string(dbcapabilities.PostgreSQL):
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return 0, fmt.Errorf("invalid postgres connection type")
		}
		return postgres.InsertData(pool, tableName, data)
	case string(dbcapabilities.MySQL):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return 0, fmt.Errorf("invalid mysql connection type")
		}
		return mysql.InsertData(db, tableName, data, dm.logger)
	case string(dbcapabilities.MariaDB):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return 0, fmt.Errorf("invalid mariadb connection type")
		}
		return mariadb.InsertData(db, tableName, data)
	case string(dbcapabilities.CockroachDB):
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return 0, fmt.Errorf("invalid cockroach connection type")
		}
		return cockroach.InsertData(pool, tableName, data)
	case string(dbcapabilities.Redis):
		client, ok := client.DB.(*goredis.Client)
		if !ok {
			return 0, fmt.Errorf("invalid redis connection type")
		}
		return redis.InsertData(client, tableName, data)
	case string(dbcapabilities.MongoDB):
		db, ok := client.DB.(*mongo.Database)
		if !ok {
			return 0, fmt.Errorf("invalid mongodb connection type")
		}
		return mongodb.InsertData(db, tableName, data)
	case string(dbcapabilities.SQLServer):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return 0, fmt.Errorf("invalid mssql connection type")
		}
		return mssql.InsertData(db, tableName, data)
	case string(dbcapabilities.Cassandra):
		session, ok := client.DB.(*gocql.Session)
		if !ok {
			return 0, fmt.Errorf("invalid cassandra connection type")
		}
		return cassandra.InsertData(session, tableName, data)
	case string(dbcapabilities.EdgeDB):
		gelClient, ok := client.DB.(*gel.Client)
		if !ok {
			return 0, fmt.Errorf("invalid edgedb connection type")
		}
		return edgedb.InsertData(gelClient, tableName, data)
	case string(dbcapabilities.Snowflake):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return 0, fmt.Errorf("invalid snowflake connection type")
		}
		return snowflake.InsertData(db, tableName, data)
	case string(dbcapabilities.ClickHouse):
		conn, ok := client.DB.(clickhouse.ClickhouseConn)
		if !ok {
			return 0, fmt.Errorf("invalid clickhouse connection type")
		}
		return clickhouse.InsertData(conn, tableName, data)
	case string(dbcapabilities.Pinecone):
		client, ok := client.DB.(*pinecone.PineconeClient)
		if !ok {
			return 0, fmt.Errorf("invalid pinecone connection type")
		}
		return pinecone.InsertData(client, tableName, "", data)
	case string(dbcapabilities.Chroma):
		client, ok := client.DB.(*chroma.ChromaClient)
		if !ok {
			return 0, fmt.Errorf("invalid chroma connection type")
		}
		return chroma.InsertData(client, tableName, data)
	case string(dbcapabilities.Milvus):
		client, ok := client.DB.(*milvus.MilvusClient)
		if !ok {
			return 0, fmt.Errorf("invalid milvus connection type")
		}
		return milvus.InsertData(client, tableName, data)
	case string(dbcapabilities.Weaviate):
		client, ok := client.DB.(*weaviate.WeaviateClient)
		if !ok {
			return 0, fmt.Errorf("invalid weaviate connection type")
		}
		return weaviate.InsertData(client, tableName, data)
	case string(dbcapabilities.Elasticsearch):
		client, ok := client.DB.(*elasticsearch.ElasticsearchClient)
		if !ok {
			return 0, fmt.Errorf("invalid elasticsearch connection type")
		}
		return elasticsearch.InsertData(client, tableName, data)
	case string(dbcapabilities.Neo4j):
		driver, ok := client.DB.(neo4jgo.DriverWithContext)
		if !ok {
			return 0, fmt.Errorf("invalid neo4j connection type")
		}
		return neo4j.InsertData(driver, tableName, false, data)
	case string(dbcapabilities.Iceberg):
		return iceberg.InsertData(client.DB, tableName, data)
	case string(dbcapabilities.DynamoDB):
		dynamoClient, ok := client.DB.(*awsdynamodb.Client)
		if !ok {
			return 0, fmt.Errorf("invalid DynamoDB connection type")
		}
		return dynamodb.InsertData(dynamoClient, tableName, data)
	case string(dbcapabilities.CosmosDB):
		cosmosClient, ok := client.DB.(*azcosmos.Client)
		if !ok {
			return 0, fmt.Errorf("invalid CosmosDB connection type")
		}
		return cosmosdb.InsertData(cosmosClient, tableName, data)
	default:
		return 0, fmt.Errorf("data insertion not supported for database type: %s", client.DatabaseType)
	}
}

// UpsertDataToDatabase upserts data into a specific table in a database
func (dm *DatabaseManager) UpsertDataToDatabase(databaseID string, tableName string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	dm.safeLog("info", "Upserting data into database %s, table %s", databaseID, tableName)

	client, err := dm.GetDatabaseClient(databaseID)
	if err != nil {
		return 0, fmt.Errorf("failed to get database client: %w", err)
	}

	if atomic.LoadInt32(&client.IsConnected) == 0 {
		return 0, fmt.Errorf("database %s is disconnected", databaseID)
	}

	switch client.DatabaseType {
	case string(dbcapabilities.PostgreSQL):
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return 0, fmt.Errorf("invalid postgres connection type")
		}
		return postgres.UpsertData(pool, tableName, data, uniqueColumns)
	case string(dbcapabilities.MySQL):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return 0, fmt.Errorf("invalid mysql connection type")
		}
		return mysql.UpsertData(db, tableName, data, uniqueColumns, dm.logger)
	case string(dbcapabilities.MariaDB):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return 0, fmt.Errorf("invalid mariadb connection type")
		}
		return mariadb.UpsertData(db, tableName, data, uniqueColumns)
	//case string(dbcapabilities.CockroachDB):
	//	pool, ok := client.DB.(*pgxpool.Pool)
	//	if !ok {
	//		return 0, fmt.Errorf("invalid cockroach connection type")
	//	}
	//	return cockroach.UpsertData(pool, tableName, data, uniqueColumns)
	//case string(dbcapabilities.Redis):
	//	client, ok := client.DB.(*goredis.Client)
	//	if !ok {
	//		return 0, fmt.Errorf("invalid redis connection type")
	//	}
	//	return redis.UpsertData(client, tableName, data, uniqueColumns)
	case string(dbcapabilities.MongoDB):
		db, ok := client.DB.(*mongo.Database)
		if !ok {
			return 0, fmt.Errorf("invalid mongodb connection type")
		}
		return mongodb.UpsertData(db, tableName, data, uniqueColumns)
	//case string(dbcapabilities.SQLServer):
	//	db, ok := client.DB.(*sql.DB)
	//	if !ok {
	//		return 0, fmt.Errorf("invalid mssql connection type")
	//	}
	//	return mssql.UpsertData(db, tableName, data, uniqueColumns)
	case string(dbcapabilities.Cassandra):
		session, ok := client.DB.(*gocql.Session)
		if !ok {
			return 0, fmt.Errorf("invalid cassandra connection type")
		}
		return cassandra.UpsertData(session, tableName, data, uniqueColumns)
	//case string(dbcapabilities.EdgeDB):
	//	gelClient, ok := client.DB.(*gel.Client)
	//	if !ok {
	//		return 0, fmt.Errorf("invalid edgedb connection type")
	//	}
	//	return edgedb.UpsertData(gelClient, tableName, data, uniqueColumns)
	//case string(dbcapabilities.Snowflake):
	//	db, ok := client.DB.(*sql.DB)
	//	if !ok {
	//		return 0, fmt.Errorf("invalid snowflake connection type")
	//	}
	//	return snowflake.UpsertData(db, tableName, data, uniqueColumns)
	//case string(dbcapabilities.ClickHouse):
	//	conn, ok := client.DB.(clickhouse.ClickhouseConn)
	//	if !ok {
	//		return 0, fmt.Errorf("invalid clickhouse connection type")
	//	}
	//	return clickhouse.UpsertData(conn, tableName, data, uniqueColumns)
	//case string(dbcapabilities.Pinecone):
	//	client, ok := client.DB.(*pinecone.PineconeClient)
	//	if !ok {
	//		return 0, fmt.Errorf("invalid pinecone connection type")
	//	}
	//	return pinecone.UpsertData(client, tableName, "", data, uniqueColumns)
	//case string(dbcapabilities.Elasticsearch):
	//	client, ok := client.DB.(*elasticsearch.ElasticsearchClient)
	//	if !ok {
	//		return 0, fmt.Errorf("invalid elasticsearch connection type")
	//	}
	//	return elasticsearch.UpsertData(client, tableName, data, uniqueColumns)
	//case string(dbcapabilities.Neo4j):
	//	driver, ok := client.DB.(neo4jgo.DriverWithContext)
	//	if !ok {
	//		return 0, fmt.Errorf("invalid neo4j connection type")
	//	}
	//	return neo4j.UpsertData(driver, tableName, false, data, uniqueColumns)
	default:
		return 0, fmt.Errorf("data upserting not supported for database type: %s", client.DatabaseType)
	}
}

// UpdateDataInDatabase updates existing data in a specific table in a database
func (dm *DatabaseManager) UpdateDataInDatabase(databaseID string, tableName string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	dm.safeLog("info", "Updating data in database %s, table %s", databaseID, tableName)

	client, err := dm.GetDatabaseClient(databaseID)
	if err != nil {
		return 0, fmt.Errorf("failed to get database client: %w", err)
	}

	if atomic.LoadInt32(&client.IsConnected) == 0 {
		return 0, fmt.Errorf("database %s is disconnected", databaseID)
	}

	switch client.DatabaseType {
	case string(dbcapabilities.PostgreSQL):
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return 0, fmt.Errorf("invalid postgres connection type")
		}
		return postgres.UpdateData(pool, tableName, data, whereColumns)
	case string(dbcapabilities.MySQL):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return 0, fmt.Errorf("invalid mysql connection type")
		}
		return mysql.UpdateData(db, tableName, data, whereColumns, dm.logger)
	case string(dbcapabilities.MariaDB):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return 0, fmt.Errorf("invalid mariadb connection type")
		}
		return mariadb.UpdateData(db, tableName, data, whereColumns)
	//case string(dbcapabilities.CockroachDB):
	//	pool, ok := client.DB.(*pgxpool.Pool)
	//	if !ok {
	//		return 0, fmt.Errorf("invalid cockroach connection type")
	//	}
	//	return cockroach.UpdateData(pool, tableName, data, whereColumns)
	//case string(dbcapabilities.Redis):
	//	client, ok := client.DB.(*goredis.Client)
	//	if !ok {
	//		return 0, fmt.Errorf("invalid redis connection type")
	//	}
	//	return redis.UpdateData(client, tableName, data, whereColumns)
	case string(dbcapabilities.MongoDB):
		db, ok := client.DB.(*mongo.Database)
		if !ok {
			return 0, fmt.Errorf("invalid mongodb connection type")
		}
		return mongodb.UpdateData(db, tableName, data, whereColumns)
	//case string(dbcapabilities.SQLServer):
	//	db, ok := client.DB.(*sql.DB)
	//	if !ok {
	//		return 0, fmt.Errorf("invalid mssql connection type")
	//	}
	//	return mssql.UpdateData(db, tableName, data, whereColumns)
	case string(dbcapabilities.Cassandra):
		session, ok := client.DB.(*gocql.Session)
		if !ok {
			return 0, fmt.Errorf("invalid cassandra connection type")
		}
		return cassandra.UpdateData(session, tableName, data, whereColumns)
	//case string(dbcapabilities.EdgeDB):
	//	gelClient, ok := client.DB.(*gel.Client)
	//	if !ok {
	//		return 0, fmt.Errorf("invalid edgedb connection type")
	//	}
	//	return edgedb.UpdateData(gelClient, tableName, data, whereColumns)
	//case string(dbcapabilities.Snowflake):
	//	db, ok := client.DB.(*sql.DB)
	//	if !ok {
	//		return 0, fmt.Errorf("invalid snowflake connection type")
	//	}
	//	return snowflake.UpdateData(db, tableName, data, whereColumns)
	//case string(dbcapabilities.ClickHouse):
	//	conn, ok := client.DB.(clickhouse.ClickhouseConn)
	//	if !ok {
	//		return 0, fmt.Errorf("invalid clickhouse connection type")
	//	}
	//	return clickhouse.UpdateData(conn, tableName, data, whereColumns)
	//case string(dbcapabilities.Pinecone):
	//	client, ok := client.DB.(*pinecone.PineconeClient)
	//	if !ok {
	//		return 0, fmt.Errorf("invalid pinecone connection type")
	//	}
	//	return pinecone.UpdateData(client, tableName, "", data, whereColumns)
	//case string(dbcapabilities.Elasticsearch):
	//	client, ok := client.DB.(*elasticsearch.ElasticsearchClient)
	//	if !ok {
	//		return 0, fmt.Errorf("invalid elasticsearch connection type")
	//	}
	//	return elasticsearch.UpdateData(client, tableName, data, whereColumns)
	//case string(dbcapabilities.Neo4j):
	//	driver, ok := client.DB.(neo4jgo.DriverWithContext)
	//	if !ok {
	//		return 0, fmt.Errorf("invalid neo4j connection type")
	//	}
	//	return neo4j.UpdateData(driver, tableName, false, data, whereColumns)
	default:
		return 0, fmt.Errorf("data updating not supported for database type: %s", client.DatabaseType)
	}
}

// WipeDatabase wipes a specific database
func (dm *DatabaseManager) WipeDatabase(databaseID string) error {
	dm.safeLog("info", "Wiping database %s", databaseID)

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
		return postgres.WipeDatabase(pool)
	case string(dbcapabilities.MySQL):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return fmt.Errorf("invalid mysql connection type")
		}
		return mysql.WipeDatabase(db, dm.logger)
	case string(dbcapabilities.MariaDB):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return fmt.Errorf("invalid mariadb connection type")
		}
		return mariadb.WipeDatabase(db)
	case string(dbcapabilities.CockroachDB):
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return fmt.Errorf("invalid cockroach connection type")
		}
		return cockroach.WipeDatabase(pool)
	case string(dbcapabilities.Redis):
		client, ok := client.DB.(*goredis.Client)
		if !ok {
			return fmt.Errorf("invalid redis connection type")
		}
		return redis.WipeDatabase(client)
	case string(dbcapabilities.MongoDB):
		db, ok := client.DB.(*mongo.Database)
		if !ok {
			return fmt.Errorf("invalid mongodb connection type")
		}
		return mongodb.WipeDatabase(db)
	case string(dbcapabilities.SQLServer):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return fmt.Errorf("invalid mssql connection type")
		}
		return mssql.WipeDatabase(db)
	case string(dbcapabilities.Cassandra):
		session, ok := client.DB.(*gocql.Session)
		if !ok {
			return fmt.Errorf("invalid cassandra connection type")
		}
		return cassandra.WipeDatabase(session)
	case string(dbcapabilities.EdgeDB):
		gelClient, ok := client.DB.(*gel.Client)
		if !ok {
			return fmt.Errorf("invalid edgedb connection type")
		}
		return edgedb.WipeDatabase(gelClient)
	case string(dbcapabilities.Snowflake):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return fmt.Errorf("invalid snowflake connection type")
		}
		return snowflake.WipeDatabase(db)
	case string(dbcapabilities.ClickHouse):
		conn, ok := client.DB.(clickhouse.ClickhouseConn)
		if !ok {
			return fmt.Errorf("invalid clickhouse connection type")
		}
		return clickhouse.WipeDatabase(conn)
	case string(dbcapabilities.Pinecone):
		client, ok := client.DB.(*pinecone.PineconeClient)
		if !ok {
			return fmt.Errorf("invalid pinecone connection type")
		}
		return pinecone.WipeDatabase(client)
	case string(dbcapabilities.Chroma):
		client, ok := client.DB.(*chroma.ChromaClient)
		if !ok {
			return fmt.Errorf("invalid chroma connection type")
		}
		return chroma.WipeDatabase(client)
	case string(dbcapabilities.Milvus):
		client, ok := client.DB.(*milvus.MilvusClient)
		if !ok {
			return fmt.Errorf("invalid milvus connection type")
		}
		return milvus.WipeDatabase(client)
	case string(dbcapabilities.Weaviate):
		client, ok := client.DB.(*weaviate.WeaviateClient)
		if !ok {
			return fmt.Errorf("invalid weaviate connection type")
		}
		return weaviate.WipeDatabase(client)
	case string(dbcapabilities.Elasticsearch):
		client, ok := client.DB.(*elasticsearch.ElasticsearchClient)
		if !ok {
			return fmt.Errorf("invalid elasticsearch connection type")
		}
		return elasticsearch.WipeDatabase(client)
	case string(dbcapabilities.Neo4j):
		driver, ok := client.DB.(neo4jgo.DriverWithContext)
		if !ok {
			return fmt.Errorf("invalid neo4j connection type")
		}
		return neo4j.WipeDatabase(driver)
	case string(dbcapabilities.Iceberg):
		// Iceberg doesn't support wiping entire databases
		return fmt.Errorf("database wiping not supported for Iceberg - use DROP DATABASE instead")
	default:
		return fmt.Errorf("wiping not supported for database type: %s", client.DatabaseType)
	}
}

// ExecuteQuery executes a generic SQL query and returns results
func (dm *DatabaseManager) ExecuteQuery(databaseID string, query string, args ...interface{}) ([]interface{}, error) {
	dm.safeLog("info", "Executing query on database %s: %s", databaseID, query)

	client, err := dm.GetDatabaseClient(databaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database client: %w", err)
	}

	if atomic.LoadInt32(&client.IsConnected) == 0 {
		return nil, fmt.Errorf("database %s is disconnected", databaseID)
	}

	switch client.DatabaseType {
	case string(dbcapabilities.PostgreSQL):
		return postgres.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.MySQL):
		return mysql.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.MariaDB):
		return mariadb.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.CockroachDB):
		return cockroach.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.SQLServer):
		return mssql.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.MongoDB):
		return mongodb.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.Cassandra):
		return cassandra.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.EdgeDB):
		return edgedb.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.Snowflake):
		return snowflake.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.ClickHouse):
		return clickhouse.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.Neo4j):
		return neo4j.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.Pinecone):
		return pinecone.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.Chroma):
		return chroma.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.Milvus):
		return milvus.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.Weaviate):
		return weaviate.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.CosmosDB):
		return cosmosdb.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.DynamoDB):
		return dynamodb.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.Iceberg):
		return iceberg.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.Elasticsearch):
		return elasticsearch.ExecuteQuery(client.DB, query, args...)
	case string(dbcapabilities.Redis):
		return redis.ExecuteQuery(client.DB, query, args...)
	default:
		return nil, fmt.Errorf("query execution not supported for database type: %s", client.DatabaseType)
	}
}

// ExecuteCountQuery executes a count query and returns the result
func (dm *DatabaseManager) ExecuteCountQuery(databaseID string, query string) (int64, error) {
	dm.safeLog("info", "Executing count query on database %s: %s", databaseID, query)

	client, err := dm.GetDatabaseClient(databaseID)
	if err != nil {
		return 0, fmt.Errorf("failed to get database client: %w", err)
	}

	if atomic.LoadInt32(&client.IsConnected) == 0 {
		return 0, fmt.Errorf("database %s is disconnected", databaseID)
	}

	switch client.DatabaseType {
	case string(dbcapabilities.PostgreSQL):
		return postgres.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.MySQL):
		return mysql.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.MariaDB):
		return mariadb.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.CockroachDB):
		return cockroach.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.SQLServer):
		return mssql.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.MongoDB):
		return mongodb.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.Cassandra):
		return cassandra.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.EdgeDB):
		return edgedb.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.Snowflake):
		return snowflake.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.ClickHouse):
		return clickhouse.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.Neo4j):
		return neo4j.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.Pinecone):
		return pinecone.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.Chroma):
		return chroma.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.Milvus):
		return milvus.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.Weaviate):
		return weaviate.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.CosmosDB):
		return cosmosdb.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.DynamoDB):
		return dynamodb.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.Iceberg):
		return iceberg.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.Elasticsearch):
		return elasticsearch.ExecuteCountQuery(client.DB, query)
	case string(dbcapabilities.Redis):
		return redis.ExecuteCountQuery(client.DB, query)
	default:
		return 0, fmt.Errorf("count query execution not supported for database type: %s", client.DatabaseType)
	}
}

// StreamTableData streams data from a table in batches for efficient data copying
func (dm *DatabaseManager) StreamTableData(databaseID string, tableName string, batchSize int32, offset int64, columns []string) ([]map[string]interface{}, bool, string, error) {
	dm.safeLog("info", "Streaming data from database %s, table %s, batch_size %d, offset %d", databaseID, tableName, batchSize, offset)

	client, err := dm.GetDatabaseClient(databaseID)
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to get database client: %w", err)
	}

	if atomic.LoadInt32(&client.IsConnected) == 0 {
		return nil, false, "", fmt.Errorf("database %s is disconnected", databaseID)
	}

	switch client.DatabaseType {
	case string(dbcapabilities.PostgreSQL):
		return postgres.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.MySQL):
		return mysql.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.MariaDB):
		return mariadb.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.CockroachDB):
		return cockroach.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.SQLServer):
		return mssql.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.MongoDB):
		return mongodb.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.Cassandra):
		return cassandra.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.EdgeDB):
		return edgedb.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.Snowflake):
		return snowflake.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.ClickHouse):
		return clickhouse.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.Neo4j):
		return neo4j.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.Pinecone):
		return pinecone.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.Chroma):
		return chroma.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.Milvus):
		return milvus.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.Weaviate):
		return weaviate.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.CosmosDB):
		return cosmosdb.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.DynamoDB):
		return dynamodb.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.Iceberg):
		return iceberg.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.Elasticsearch):
		return elasticsearch.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	case string(dbcapabilities.Redis):
		return redis.StreamTableData(client.DB, tableName, batchSize, offset, columns)
	default:
		return nil, false, "", fmt.Errorf("table data streaming not supported for database type: %s", client.DatabaseType)
	}
}

// GetTableRowCount returns the number of rows in a table, optionally with a WHERE clause
func (dm *DatabaseManager) GetTableRowCount(databaseID string, tableName string, whereClause string) (int64, bool, error) {
	dm.safeLog("info", "Getting row count from database %s, table %s", databaseID, tableName)

	client, err := dm.GetDatabaseClient(databaseID)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get database client: %w", err)
	}

	if atomic.LoadInt32(&client.IsConnected) == 0 {
		return 0, false, fmt.Errorf("database %s is disconnected", databaseID)
	}

	switch client.DatabaseType {
	case string(dbcapabilities.PostgreSQL):
		return postgres.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.MySQL):
		return mysql.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.MariaDB):
		return mariadb.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.CockroachDB):
		return cockroach.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.SQLServer):
		return mssql.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.MongoDB):
		return mongodb.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.Cassandra):
		return cassandra.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.EdgeDB):
		return edgedb.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.Snowflake):
		return snowflake.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.ClickHouse):
		return clickhouse.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.Neo4j):
		return neo4j.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.Pinecone):
		return pinecone.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.Chroma):
		return chroma.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.Milvus):
		return milvus.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.Weaviate):
		return weaviate.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.CosmosDB):
		return cosmosdb.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.DynamoDB):
		return dynamodb.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.Iceberg):
		return iceberg.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.Elasticsearch):
		return elasticsearch.GetTableRowCount(client.DB, tableName, whereClause)
	case string(dbcapabilities.Redis):
		return redis.GetTableRowCount(client.DB, tableName, whereClause)
	default:
		return 0, false, fmt.Errorf("row count retrieval not supported for database type: %s", client.DatabaseType)
	}
}
