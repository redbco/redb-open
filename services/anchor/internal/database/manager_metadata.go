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
	"github.com/redbco/redb-open/services/anchor/internal/config"
	"github.com/redbco/redb-open/services/anchor/internal/database/cassandra"
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
	case "postgres":
		metadata, err = postgres.CollectDatabaseMetadata(ctx, c.client.DB)
	case "mysql":
		metadata, err = mysql.CollectDatabaseMetadata(ctx, c.client.DB)
	case "mariadb":
		metadata, err = mariadb.CollectDatabaseMetadata(ctx, c.client.DB)
	case "cockroach":
		metadata, err = cockroach.CollectDatabaseMetadata(ctx, c.client.DB)
	case "redis":
		metadata, err = redis.CollectDatabaseMetadata(ctx, c.client.DB)
	case "mongodb":
		metadata, err = mongodb.CollectDatabaseMetadata(ctx, c.client.DB)
	case "mssql":
		metadata, err = mssql.CollectDatabaseMetadata(ctx, c.client.DB)
	case "cassandra":
		metadata, err = cassandra.CollectDatabaseMetadata(ctx, c.client.DB)
	case "edgedb":
		metadata, err = edgedb.CollectDatabaseMetadata(ctx, c.client.DB)
	case "snowflake":
		metadata, err = snowflake.CollectDatabaseMetadata(ctx, c.client.DB)
	case "clickhouse":
		metadata, err = clickhouse.CollectDatabaseMetadata(ctx, c.client.DB)
	case "pinecone":
		metadata, err = pinecone.CollectDatabaseMetadata(ctx, c.client.DB)
	case "elasticsearch":
		metadata, err = elasticsearch.CollectDatabaseMetadata(ctx, c.client.DB)
	case "neo4j":
		metadata, err = neo4j.CollectDatabaseMetadata(ctx, c.client.DB)
	//case "db2":
	//	return db2.CollectDatabaseMetadata(ctx, c.client.DB)
	//case "oracle":
	//	return oracle.CollectDatabaseMetadata(ctx, c.client.DB)
	default:
		return config.DatabaseMetadata{}, fmt.Errorf("unsupported database type: %s", c.client.DatabaseType)
	}

	if err != nil {
		return config.DatabaseMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
	}

	// Convert metadata into DatabaseMetadata type
	dbMetadata := config.DatabaseMetadata{
		DatabaseID:  databaseID,
		Version:     metadata["version"].(string),
		SizeBytes:   convertToInt64(metadata["size_bytes"]), // Handle both int64 and float64 types
		TablesCount: convertToInt(metadata["tables_count"]), // Handle nil and different numeric types
	}

	return dbMetadata, nil
}

// CollectMetadata collects metadata about the database instance
func (c *InstanceMetadataCollector) CollectMetadata(ctx context.Context, instanceID string) (config.InstanceMetadata, error) {
	metadata := make(map[string]interface{})
	var err error
	switch c.client.InstanceType {
	case "postgres":
		metadata, err = postgres.CollectInstanceMetadata(ctx, c.client.DB)
	case "mysql":
		metadata, err = mysql.CollectInstanceMetadata(ctx, c.client.DB)
	case "mariadb":
		metadata, err = mariadb.CollectInstanceMetadata(ctx, c.client.DB)
	case "cockroach":
		metadata, err = cockroach.CollectInstanceMetadata(ctx, c.client.DB)
	case "redis":
		metadata, err = redis.CollectInstanceMetadata(ctx, c.client.DB)
	case "mongodb":
		metadata, err = mongodb.CollectInstanceMetadata(ctx, c.client.DB)
	case "mssql":
		metadata, err = mssql.CollectInstanceMetadata(ctx, c.client.DB)
	case "cassandra":
		metadata, err = cassandra.CollectInstanceMetadata(ctx, c.client.DB)
	case "edgedb":
		metadata, err = edgedb.CollectInstanceMetadata(ctx, c.client.DB)
	case "snowflake":
		metadata, err = snowflake.CollectInstanceMetadata(ctx, c.client.DB)
	case "clickhouse":
		metadata, err = clickhouse.CollectInstanceMetadata(ctx, c.client.DB)
	case "pinecone":
		metadata, err = pinecone.CollectInstanceMetadata(ctx, c.client.DB)
	case "elasticsearch":
		metadata, err = elasticsearch.CollectInstanceMetadata(ctx, c.client.DB)
	case "neo4j":
		metadata, err = neo4j.CollectInstanceMetadata(ctx, c.client.DB)
	//case "db2":
	//	return db2.CollectInstanceMetadata(ctx, c.client.DB)
	//case "oracle":
	//	return oracle.CollectInstanceMetadata(ctx, c.client.DB)
	default:
		return config.InstanceMetadata{}, fmt.Errorf("unsupported database type: %s", c.client.InstanceType)
	}

	if err != nil {
		return config.InstanceMetadata{}, fmt.Errorf("failed to collect metadata: %w", err)
	}

	// Convert metadata into DatabaseMetadata type
	instanceMetadata := config.InstanceMetadata{
		InstanceID:       instanceID,
		Version:          metadata["version"].(string),
		UptimeSeconds:    convertToInt64(metadata["uptime_seconds"]),
		TotalDatabases:   convertToInt(metadata["total_databases"]),
		TotalConnections: convertToInt(metadata["total_connections"]),
		MaxConnections:   convertToInt(metadata["max_connections"]),
	}

	return instanceMetadata, nil
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
	case "postgres":
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
	case "mysql":
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
	case "mariadb":
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
	case "cockroach":
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
	case "redis":
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
	case "mongodb":
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
	case "mssql":
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
	case "cassandra":
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
	case "elasticsearch":
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
	case "neo4j":
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
	case "postgres":
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
	case "mysql":
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
	case "cockroach":
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
	case "redis":
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
	case "mongodb":
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
	case "mssql":
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
	case "cassandra":
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
	case "edgedb":
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
	case "snowflake":
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
	case "clickhouse":
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
	case "pinecone":
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
	case "elasticsearch":
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
	case "neo4j":
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
	default:
		return config.InstanceMetadata{}, fmt.Errorf("schema discovery not supported for database type: %s", client.InstanceType)
	}
}
