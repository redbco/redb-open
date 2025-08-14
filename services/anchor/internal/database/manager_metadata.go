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
	"github.com/redbco/redb-open/services/anchor/internal/config"
	"github.com/redbco/redb-open/services/anchor/internal/database/cassandra"
	"github.com/redbco/redb-open/services/anchor/internal/database/chroma"
	"github.com/redbco/redb-open/services/anchor/internal/database/clickhouse"
	"github.com/redbco/redb-open/services/anchor/internal/database/cockroach"
	"github.com/redbco/redb-open/services/anchor/internal/database/edgedb"
	"github.com/redbco/redb-open/services/anchor/internal/database/elasticsearch"
	"github.com/redbco/redb-open/services/anchor/internal/database/mariadb"
	"github.com/redbco/redb-open/services/anchor/internal/database/mongodb"
	"github.com/redbco/redb-open/services/anchor/internal/database/mssql"
	"github.com/redbco/redb-open/services/anchor/internal/database/mysql"
	"github.com/redbco/redb-open/services/anchor/internal/database/neo4j"
	"github.com/redbco/redb-open/services/anchor/internal/database/pinecone"
	"github.com/redbco/redb-open/services/anchor/internal/database/postgres"
	"github.com/redbco/redb-open/services/anchor/internal/database/redis"
	"github.com/redbco/redb-open/services/anchor/internal/database/snowflake"
	goredis "github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// CollectMetadata collects metadata about the database
func (c *DatabaseMetadataCollector) CollectMetadata(ctx context.Context, databaseID string) (config.DatabaseMetadata, error) {
	metadata := make(map[string]interface{})
	var err error
	switch c.client.DatabaseType {
	case string(dbcapabilities.PostgreSQL):
		metadata, err = postgres.CollectDatabaseMetadata(ctx, c.client.DB)
	case string(dbcapabilities.MySQL):
		metadata, err = mysql.CollectDatabaseMetadata(ctx, c.client.DB)
	case string(dbcapabilities.MariaDB):
		metadata, err = mariadb.CollectDatabaseMetadata(ctx, c.client.DB)
	case string(dbcapabilities.CockroachDB):
		metadata, err = cockroach.CollectDatabaseMetadata(ctx, c.client.DB)
	case string(dbcapabilities.Redis):
		metadata, err = redis.CollectDatabaseMetadata(ctx, c.client.DB)
	case string(dbcapabilities.MongoDB):
		metadata, err = mongodb.CollectDatabaseMetadata(ctx, c.client.DB)
	case string(dbcapabilities.SQLServer):
		metadata, err = mssql.CollectDatabaseMetadata(ctx, c.client.DB)
	case string(dbcapabilities.Cassandra):
		metadata, err = cassandra.CollectDatabaseMetadata(ctx, c.client.DB)
	case string(dbcapabilities.EdgeDB):
		metadata, err = edgedb.CollectDatabaseMetadata(ctx, c.client.DB)
	case string(dbcapabilities.Snowflake):
		metadata, err = snowflake.CollectDatabaseMetadata(ctx, c.client.DB)
	case string(dbcapabilities.ClickHouse):
		metadata, err = clickhouse.CollectDatabaseMetadata(ctx, c.client.DB)
	case string(dbcapabilities.Pinecone):
		metadata, err = pinecone.CollectDatabaseMetadata(ctx, c.client.DB)
	case string(dbcapabilities.Chroma):
		if cc, ok := c.client.DB.(*chroma.ChromaClient); ok {
			metadata, err = chroma.CollectDatabaseMetadata(ctx, cc)
		} else {
			return config.DatabaseMetadata{}, fmt.Errorf("invalid chroma connection type")
		}
	case string(dbcapabilities.Elasticsearch):
		metadata, err = elasticsearch.CollectDatabaseMetadata(ctx, c.client.DB)
	case string(dbcapabilities.Neo4j):
		metadata, err = neo4j.CollectDatabaseMetadata(ctx, c.client.DB)
	//case string(dbcapabilities.DB2):
	//	return db2.CollectDatabaseMetadata(ctx, c.client.DB)
	//case string(dbcapabilities.Oracle):
	//	return oracle.CollectDatabaseMetadata(ctx, c.client.DB)
	default:
		return config.DatabaseMetadata{}, fmt.Errorf("unsupported database type: %s", c.client.DatabaseType)
	}

	if err != nil {
		return config.DatabaseMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
	}

	// Convert metadata into DatabaseMetadata type
	dbMetadata := config.DatabaseMetadata{DatabaseID: databaseID}
	if v, ok := metadata["version"].(string); ok {
		dbMetadata.Version = v
	}
	if v, ok := metadata["size_bytes"]; ok {
		dbMetadata.SizeBytes = convertToInt64(v)
	}
	if v, ok := metadata["tables_count"]; ok {
		dbMetadata.TablesCount = convertToInt(v)
	}

	return dbMetadata, nil
}

// CollectMetadata collects metadata about the database instance
func (c *InstanceMetadataCollector) CollectMetadata(ctx context.Context, instanceID string) (config.InstanceMetadata, error) {
	metadata := make(map[string]interface{})
	var err error
	switch c.client.InstanceType {
	case string(dbcapabilities.PostgreSQL):
		metadata, err = postgres.CollectInstanceMetadata(ctx, c.client.DB)
	case string(dbcapabilities.MySQL):
		metadata, err = mysql.CollectInstanceMetadata(ctx, c.client.DB)
	case string(dbcapabilities.MariaDB):
		metadata, err = mariadb.CollectInstanceMetadata(ctx, c.client.DB)
	case string(dbcapabilities.CockroachDB):
		metadata, err = cockroach.CollectInstanceMetadata(ctx, c.client.DB)
	case string(dbcapabilities.Redis):
		metadata, err = redis.CollectInstanceMetadata(ctx, c.client.DB)
	case string(dbcapabilities.MongoDB):
		metadata, err = mongodb.CollectInstanceMetadata(ctx, c.client.DB)
	case string(dbcapabilities.SQLServer):
		metadata, err = mssql.CollectInstanceMetadata(ctx, c.client.DB)
	case string(dbcapabilities.Cassandra):
		metadata, err = cassandra.CollectInstanceMetadata(ctx, c.client.DB)
	case string(dbcapabilities.EdgeDB):
		metadata, err = edgedb.CollectInstanceMetadata(ctx, c.client.DB)
	case string(dbcapabilities.Snowflake):
		metadata, err = snowflake.CollectInstanceMetadata(ctx, c.client.DB)
	case string(dbcapabilities.ClickHouse):
		metadata, err = clickhouse.CollectInstanceMetadata(ctx, c.client.DB)
	case string(dbcapabilities.Pinecone):
		metadata, err = pinecone.CollectInstanceMetadata(ctx, c.client.DB)
	case string(dbcapabilities.Elasticsearch):
		metadata, err = elasticsearch.CollectInstanceMetadata(ctx, c.client.DB)
	case string(dbcapabilities.Neo4j):
		metadata, err = neo4j.CollectInstanceMetadata(ctx, c.client.DB)
	case string(dbcapabilities.Chroma):
		if cc, ok := c.client.DB.(*chroma.ChromaClient); ok {
			metadata, err = chroma.CollectInstanceMetadata(ctx, cc)
		} else {
			return config.InstanceMetadata{}, fmt.Errorf("invalid chroma connection type")
		}
	//case string(dbcapabilities.DB2):
	//	return db2.CollectInstanceMetadata(ctx, c.client.DB)
	//case string(dbcapabilities.Oracle):
	//	return oracle.CollectInstanceMetadata(ctx, c.client.DB)
	default:
		return config.InstanceMetadata{}, fmt.Errorf("unsupported database type: %s", c.client.InstanceType)
	}

	if err != nil {
		return config.InstanceMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
	}

	im := config.InstanceMetadata{InstanceID: instanceID}
	if v, ok := metadata["version"].(string); ok {
		im.Version = v
	}
	if v, ok := metadata["uptime_seconds"]; ok {
		im.UptimeSeconds = convertToInt64(v)
	}
	if v, ok := metadata["total_databases"]; ok {
		im.TotalDatabases = convertToInt(v)
	}
	if v, ok := metadata["total_connections"]; ok {
		im.TotalConnections = convertToInt(v)
	}
	if v, ok := metadata["max_connections"]; ok {
		im.MaxConnections = convertToInt(v)
	}
	if im.Version == "" {
		if details, ok := metadata["details"].(map[string]interface{}); ok {
			im.Version = fmt.Sprintf("%v", details["version"])
			im.TotalDatabases = convertToInt(details["collectionCount"])
		}
	}
	return im, nil
}

// GetDatabaseMetadata returns the metadata of a database
func (dm *DatabaseManager) GetDatabaseMetadata(id string) (config.DatabaseMetadata, error) {
	dm.safeLog("info", "Getting database metadata for %s", id)
	client, err := dm.GetDatabaseClient(id)
	if err != nil {
		return config.DatabaseMetadata{}, err
	}

	if atomic.LoadInt32(&client.IsConnected) == 0 {
		return config.DatabaseMetadata{}, fmt.Errorf("database %s is disconnected", id)
	}

	switch client.DatabaseType {
	case string(dbcapabilities.PostgreSQL):
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return config.DatabaseMetadata{}, fmt.Errorf("invalid postgres connection type")
		}
		metadata, err := postgres.CollectDatabaseMetadata(context.Background(), pool)
		if err != nil {
			return config.DatabaseMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.DatabaseMetadata{
			DatabaseID:  id,
			Version:     metadata["version"].(string),
			SizeBytes:   convertToInt64(metadata["size_bytes"]),
			TablesCount: convertToInt(metadata["tables_count"]),
		}, nil
	case string(dbcapabilities.MySQL):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return config.DatabaseMetadata{}, fmt.Errorf("invalid mysql connection type")
		}
		metadata, err := mysql.CollectDatabaseMetadata(context.Background(), db)
		if err != nil {
			return config.DatabaseMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.DatabaseMetadata{
			DatabaseID:  id,
			Version:     metadata["version"].(string),
			SizeBytes:   convertToInt64(metadata["size_bytes"]),
			TablesCount: convertToInt(metadata["tables_count"]),
		}, nil
	case string(dbcapabilities.MariaDB):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return config.DatabaseMetadata{}, fmt.Errorf("invalid mariadb connection type")
		}
		metadata, err := mariadb.CollectDatabaseMetadata(context.Background(), db)
		if err != nil {
			return config.DatabaseMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.DatabaseMetadata{
			DatabaseID:  id,
			Version:     metadata["version"].(string),
			SizeBytes:   convertToInt64(metadata["size_bytes"]),
			TablesCount: convertToInt(metadata["tables_count"]),
		}, nil
	case string(dbcapabilities.CockroachDB):
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return config.DatabaseMetadata{}, fmt.Errorf("invalid cockroach connection type")
		}
		metadata, err := cockroach.CollectDatabaseMetadata(context.Background(), pool)
		if err != nil {
			return config.DatabaseMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.DatabaseMetadata{
			DatabaseID:  id,
			Version:     metadata["version"].(string),
			SizeBytes:   convertToInt64(metadata["size_bytes"]),
			TablesCount: convertToInt(metadata["tables_count"]),
		}, nil
	case string(dbcapabilities.Redis):
		client, ok := client.DB.(*goredis.Client)
		if !ok {
			return config.DatabaseMetadata{}, fmt.Errorf("invalid redis connection type")
		}
		metadata, err := redis.CollectDatabaseMetadata(context.Background(), client)
		if err != nil {
			return config.DatabaseMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.DatabaseMetadata{
			DatabaseID:  id,
			Version:     metadata["version"].(string),
			SizeBytes:   convertToInt64(metadata["size_bytes"]),
			TablesCount: convertToInt(metadata["tables_count"]),
		}, nil
	case string(dbcapabilities.MongoDB):
		db, ok := client.DB.(*mongo.Database)
		if !ok {
			return config.DatabaseMetadata{}, fmt.Errorf("invalid mongodb connection type")
		}
		metadata, err := mongodb.CollectDatabaseMetadata(context.Background(), db)
		if err != nil {
			return config.DatabaseMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.DatabaseMetadata{
			DatabaseID:  id,
			Version:     metadata["version"].(string),
			SizeBytes:   convertToInt64(metadata["size_bytes"]),
			TablesCount: convertToInt(metadata["tables_count"]),
		}, nil
	case string(dbcapabilities.SQLServer):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return config.DatabaseMetadata{}, fmt.Errorf("invalid mssql connection type")
		}
		metadata, err := mssql.CollectDatabaseMetadata(context.Background(), db)
		if err != nil {
			return config.DatabaseMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.DatabaseMetadata{
			DatabaseID:  id,
			Version:     metadata["version"].(string),
			SizeBytes:   convertToInt64(metadata["size_bytes"]),
			TablesCount: convertToInt(metadata["tables_count"]),
		}, nil
	case string(dbcapabilities.Cassandra):
		session, ok := client.DB.(*gocql.Session)
		if !ok {
			return config.DatabaseMetadata{}, fmt.Errorf("invalid cassandra connection type")
		}
		metadata, err := cassandra.CollectDatabaseMetadata(context.Background(), session)
		if err != nil {
			return config.DatabaseMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.DatabaseMetadata{
			DatabaseID:  id,
			Version:     metadata["version"].(string),
			SizeBytes:   convertToInt64(metadata["size_bytes"]),
			TablesCount: convertToInt(metadata["tables_count"]),
		}, nil
	case string(dbcapabilities.Elasticsearch):
		client, ok := client.DB.(*elasticsearch.ElasticsearchClient)
		if !ok {
			return config.DatabaseMetadata{}, fmt.Errorf("invalid elasticsearch connection type")
		}
		metadata, err := elasticsearch.CollectDatabaseMetadata(context.Background(), client)
		if err != nil {
			return config.DatabaseMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.DatabaseMetadata{
			DatabaseID:  id,
			Version:     metadata["version"].(string),
			SizeBytes:   convertToInt64(metadata["size_bytes"]),
			TablesCount: convertToInt(metadata["tables_count"]),
		}, nil
	case string(dbcapabilities.Neo4j):
		driver, ok := client.DB.(neo4jgo.DriverWithContext)
		if !ok {
			return config.DatabaseMetadata{}, fmt.Errorf("invalid neo4j connection type")
		}
		metadata, err := neo4j.CollectDatabaseMetadata(context.Background(), driver)
		if err != nil {
			return config.DatabaseMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.DatabaseMetadata{
			DatabaseID:  id,
			Version:     metadata["version"].(string),
			SizeBytes:   convertToInt64(metadata["size_bytes"]),
			TablesCount: convertToInt(metadata["tables_count"]),
		}, nil
	case string(dbcapabilities.Chroma):
		cc, ok := client.DB.(*chroma.ChromaClient)
		if !ok {
			return config.DatabaseMetadata{}, fmt.Errorf("invalid chroma connection type")
		}
		metadata, err := chroma.CollectDatabaseMetadata(context.Background(), cc)
		if err != nil {
			return config.DatabaseMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		// Pull fields defensively from the nested details map
		var versionStr string
		var sizeBytes int64
		var tables int
		if details, ok := metadata["details"].(map[string]interface{}); ok {
			versionStr = fmt.Sprintf("%v", details["version"])
			sizeBytes = convertToInt64(details["databaseSize"])
			tables = convertToInt(details["collectionCount"])
		}
		return config.DatabaseMetadata{
			DatabaseID:  id,
			Version:     versionStr,
			SizeBytes:   sizeBytes,
			TablesCount: tables,
		}, nil
	default:
		return config.DatabaseMetadata{}, fmt.Errorf("metadata collection not supported for database type: %s", client.DatabaseType)
	}
}

// GetInstanceMetadata returns the metadata of a database instance
func (dm *DatabaseManager) GetInstanceMetadata(id string) (config.InstanceMetadata, error) {
	dm.safeLog("info", "Getting instance metadata for %s", id)
	client, err := dm.GetInstanceClient(id)
	if err != nil {
		return config.InstanceMetadata{}, err
	}

	if atomic.LoadInt32(&client.IsConnected) == 0 {
		return config.InstanceMetadata{}, fmt.Errorf("instance %s is disconnected", id)
	}

	switch client.InstanceType {
	case string(dbcapabilities.PostgreSQL):
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return config.InstanceMetadata{}, fmt.Errorf("invalid postgres connection type")
		}
		metadata, err := postgres.CollectInstanceMetadata(context.Background(), pool)
		if err != nil {
			return config.InstanceMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.InstanceMetadata{
			InstanceID:       id,
			Version:          metadata["version"].(string),
			UptimeSeconds:    convertToInt64(metadata["uptime_seconds"]),
			TotalDatabases:   convertToInt(metadata["total_databases"]),
			TotalConnections: convertToInt(metadata["total_connections"]),
			MaxConnections:   convertToInt(metadata["max_connections"]),
		}, nil
	case string(dbcapabilities.MySQL):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return config.InstanceMetadata{}, fmt.Errorf("invalid mysql connection type")
		}
		metadata, err := mysql.CollectInstanceMetadata(context.Background(), db)
		if err != nil {
			return config.InstanceMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.InstanceMetadata{
			InstanceID:       id,
			Version:          metadata["version"].(string),
			UptimeSeconds:    convertToInt64(metadata["uptime_seconds"]),
			TotalDatabases:   convertToInt(metadata["total_databases"]),
			TotalConnections: convertToInt(metadata["total_connections"]),
			MaxConnections:   convertToInt(metadata["max_connections"]),
		}, nil
	case string(dbcapabilities.CockroachDB):
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return config.InstanceMetadata{}, fmt.Errorf("invalid cockroach connection type")
		}
		metadata, err := cockroach.CollectInstanceMetadata(context.Background(), pool)
		if err != nil {
			return config.InstanceMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.InstanceMetadata{
			InstanceID:       id,
			Version:          metadata["version"].(string),
			UptimeSeconds:    convertToInt64(metadata["uptime_seconds"]),
			TotalDatabases:   convertToInt(metadata["total_databases"]),
			TotalConnections: convertToInt(metadata["total_connections"]),
			MaxConnections:   convertToInt(metadata["max_connections"]),
		}, nil
	case string(dbcapabilities.Redis):
		client, ok := client.DB.(*goredis.Client)
		if !ok {
			return config.InstanceMetadata{}, fmt.Errorf("invalid redis connection type")
		}
		metadata, err := redis.CollectInstanceMetadata(context.Background(), client)
		if err != nil {
			return config.InstanceMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.InstanceMetadata{
			InstanceID:       id,
			Version:          metadata["version"].(string),
			UptimeSeconds:    convertToInt64(metadata["uptime_seconds"]),
			TotalDatabases:   convertToInt(metadata["total_databases"]),
			TotalConnections: convertToInt(metadata["total_connections"]),
			MaxConnections:   convertToInt(metadata["max_connections"]),
		}, nil
	case string(dbcapabilities.MongoDB):
		db, ok := client.DB.(*mongo.Database)
		if !ok {
			return config.InstanceMetadata{}, fmt.Errorf("invalid mongodb connection type")
		}
		metadata, err := mongodb.CollectInstanceMetadata(context.Background(), db)
		if err != nil {
			return config.InstanceMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.InstanceMetadata{
			InstanceID:       id,
			Version:          metadata["version"].(string),
			UptimeSeconds:    convertToInt64(metadata["uptime_seconds"]),
			TotalDatabases:   convertToInt(metadata["total_databases"]),
			TotalConnections: convertToInt(metadata["total_connections"]),
			MaxConnections:   convertToInt(metadata["max_connections"]),
		}, nil
	case string(dbcapabilities.SQLServer):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return config.InstanceMetadata{}, fmt.Errorf("invalid mssql connection type")
		}
		metadata, err := mssql.CollectInstanceMetadata(context.Background(), db)
		if err != nil {
			return config.InstanceMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.InstanceMetadata{
			InstanceID:       id,
			Version:          metadata["version"].(string),
			UptimeSeconds:    convertToInt64(metadata["uptime_seconds"]),
			TotalDatabases:   convertToInt(metadata["total_databases"]),
			TotalConnections: convertToInt(metadata["total_connections"]),
			MaxConnections:   convertToInt(metadata["max_connections"]),
		}, nil
	case string(dbcapabilities.Cassandra):
		session, ok := client.DB.(*gocql.Session)
		if !ok {
			return config.InstanceMetadata{}, fmt.Errorf("invalid cassandra connection type")
		}
		metadata, err := cassandra.CollectInstanceMetadata(context.Background(), session)
		if err != nil {
			return config.InstanceMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.InstanceMetadata{
			InstanceID:       id,
			Version:          metadata["version"].(string),
			UptimeSeconds:    convertToInt64(metadata["uptime_seconds"]),
			TotalDatabases:   convertToInt(metadata["total_databases"]),
			TotalConnections: convertToInt(metadata["total_connections"]),
			MaxConnections:   convertToInt(metadata["max_connections"]),
		}, nil
	case string(dbcapabilities.EdgeDB):
		gelClient, ok := client.DB.(*gel.Client)
		if !ok {
			return config.InstanceMetadata{}, fmt.Errorf("invalid edgedb connection type")
		}
		metadata, err := edgedb.CollectInstanceMetadata(context.Background(), gelClient)
		if err != nil {
			return config.InstanceMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.InstanceMetadata{
			InstanceID:       id,
			Version:          metadata["version"].(string),
			UptimeSeconds:    convertToInt64(metadata["uptime_seconds"]),
			TotalDatabases:   convertToInt(metadata["total_databases"]),
			TotalConnections: convertToInt(metadata["total_connections"]),
			MaxConnections:   convertToInt(metadata["max_connections"]),
		}, nil
	case string(dbcapabilities.Snowflake):
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return config.InstanceMetadata{}, fmt.Errorf("invalid snowflake connection type")
		}
		metadata, err := snowflake.CollectInstanceMetadata(context.Background(), db)
		if err != nil {
			return config.InstanceMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.InstanceMetadata{
			InstanceID:       id,
			Version:          metadata["version"].(string),
			UptimeSeconds:    convertToInt64(metadata["uptime_seconds"]),
			TotalDatabases:   convertToInt(metadata["total_databases"]),
			TotalConnections: convertToInt(metadata["total_connections"]),
			MaxConnections:   convertToInt(metadata["max_connections"]),
		}, nil
	case string(dbcapabilities.ClickHouse):
		conn, ok := client.DB.(clickhouse.ClickhouseConn)
		if !ok {
			return config.InstanceMetadata{}, fmt.Errorf("invalid clickhouse connection type")
		}
		metadata, err := clickhouse.CollectInstanceMetadata(context.Background(), conn)
		if err != nil {
			return config.InstanceMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.InstanceMetadata{
			InstanceID:       id,
			Version:          metadata["version"].(string),
			UptimeSeconds:    convertToInt64(metadata["uptime_seconds"]),
			TotalDatabases:   convertToInt(metadata["total_databases"]),
			TotalConnections: convertToInt(metadata["total_connections"]),
			MaxConnections:   convertToInt(metadata["max_connections"]),
		}, nil
	case string(dbcapabilities.Pinecone):
		client, ok := client.DB.(*pinecone.PineconeClient)
		if !ok {
			return config.InstanceMetadata{}, fmt.Errorf("invalid pinecone connection type")
		}
		metadata, err := pinecone.CollectInstanceMetadata(context.Background(), client)
		if err != nil {
			return config.InstanceMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.InstanceMetadata{
			InstanceID:       id,
			Version:          metadata["version"].(string),
			UptimeSeconds:    convertToInt64(metadata["uptime_seconds"]),
			TotalDatabases:   convertToInt(metadata["total_databases"]),
			TotalConnections: convertToInt(metadata["total_connections"]),
			MaxConnections:   convertToInt(metadata["max_connections"]),
		}, nil
	case string(dbcapabilities.Elasticsearch):
		client, ok := client.DB.(*elasticsearch.ElasticsearchClient)
		if !ok {
			return config.InstanceMetadata{}, fmt.Errorf("invalid elasticsearch connection type")
		}
		metadata, err := elasticsearch.CollectInstanceMetadata(context.Background(), client)
		if err != nil {
			return config.InstanceMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.InstanceMetadata{
			InstanceID:       id,
			Version:          metadata["version"].(string),
			UptimeSeconds:    convertToInt64(metadata["uptime_seconds"]),
			TotalDatabases:   convertToInt(metadata["total_databases"]),
			TotalConnections: convertToInt(metadata["total_connections"]),
			MaxConnections:   convertToInt(metadata["max_connections"]),
		}, nil
	case string(dbcapabilities.Neo4j):
		driver, ok := client.DB.(neo4jgo.DriverWithContext)
		if !ok {
			return config.InstanceMetadata{}, fmt.Errorf("invalid neo4j connection type")
		}
		metadata, err := neo4j.CollectInstanceMetadata(context.Background(), driver)
		if err != nil {
			return config.InstanceMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		return config.InstanceMetadata{
			InstanceID:       id,
			Version:          metadata["version"].(string),
			UptimeSeconds:    convertToInt64(metadata["uptime_seconds"]),
			TotalDatabases:   convertToInt(metadata["total_databases"]),
			TotalConnections: convertToInt(metadata["total_connections"]),
			MaxConnections:   convertToInt(metadata["max_connections"]),
		}, nil
	case string(dbcapabilities.Chroma):
		cc, ok := client.DB.(*chroma.ChromaClient)
		if !ok {
			return config.InstanceMetadata{}, fmt.Errorf("invalid chroma connection type")
		}
		metadata, err := chroma.CollectInstanceMetadata(context.Background(), cc)
		if err != nil {
			return config.InstanceMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
		}
		var versionStr string
		var totalCollections int
		if details, ok := metadata["details"].(map[string]interface{}); ok {
			versionStr = fmt.Sprintf("%v", details["version"])
			totalCollections = convertToInt(details["collectionCount"])
		}
		return config.InstanceMetadata{
			InstanceID:       id,
			Version:          versionStr,
			UptimeSeconds:    0,
			TotalDatabases:   totalCollections,
			TotalConnections: 0,
			MaxConnections:   0,
		}, nil
	default:
		return config.InstanceMetadata{}, fmt.Errorf("schema discovery not supported for database type: %s", client.InstanceType)
	}
}
