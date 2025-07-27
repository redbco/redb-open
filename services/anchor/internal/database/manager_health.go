package database

import (
	"context"
	"database/sql"
	"sync/atomic"
	"time"

	"github.com/geldata/gel-go"
	"github.com/gocql/gocql"
	"github.com/jackc/pgx/v5/pgxpool"
	neo4jgo "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redbco/redb-open/services/anchor/internal/database/clickhouse"
	"github.com/redbco/redb-open/services/anchor/internal/database/elasticsearch"
	"github.com/redbco/redb-open/services/anchor/internal/database/pinecone"
	goredis "github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// CheckHealth verifies the connection health for a specific client
func (dm *DatabaseManager) CheckDatabaseHealth(clientID string) bool {
	dm.safeLog("info", "Checking health for client %s", clientID)
	dm.mu.RLock()
	client, exists := dm.databaseClients[clientID]
	dm.mu.RUnlock()

	if !exists {
		return false
	}

	// Check if client is marked as connected
	if atomic.LoadInt32(&client.IsConnected) != 1 {
		return false
	}

	// Perform database-specific health check
	switch client.DatabaseType {
	case "postgres":
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return pool.Ping(ctx) == nil

	case "mysql":
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return db.PingContext(ctx) == nil

	case "mariadb":
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return db.PingContext(ctx) == nil

	case "cockroach":
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return pool.Ping(ctx) == nil

	case "redis":
		client, ok := client.DB.(*goredis.Client)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return client.Ping(ctx).Err() == nil

	case "mongodb":
		db, ok := client.DB.(*mongo.Client)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return db.Ping(ctx, readpref.Primary()) == nil

	case "mssql":
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return db.PingContext(ctx) == nil

	case "cassandra":
		session, ok := client.DB.(*gocql.Session)
		if !ok {
			return false
		}
		return !session.Closed()

	case "edgedb":
		gelClient, ok := client.DB.(*gel.Client)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Simple query to check if the connection is alive
		var result string
		err := gelClient.QuerySingle(ctx, "SELECT 'ping'", &result)
		return err == nil && result == "ping"

	case "snowflake":
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return db.PingContext(ctx) == nil

	case "clickhouse":
		conn, ok := client.DB.(clickhouse.ClickhouseConn)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rows, err := conn.Query(ctx, "SELECT 1")
		return err == nil && rows.Next()

	case "pinecone":
		client, ok := client.DB.(*pinecone.PineconeClient)
		if !ok {
			return false
		}
		return client.IsConnected == 1

	case "elasticsearch":
		client, ok := client.DB.(*elasticsearch.ElasticsearchClient)
		if !ok {
			return false
		}
		return client.IsConnected == 1

	case "neo4j":
		driver, ok := client.DB.(neo4jgo.DriverWithContext)
		if !ok {
			return false
		}
		return driver.VerifyConnectivity(context.Background()) == nil

	//case "db2":
	//	db, ok := client.DB.(*sql.DB)
	//	if !ok {
	//		return false
	//	}
	//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//	defer cancel()
	//	return db.PingContext(ctx) == nil

	//case "oracle":
	//	db, ok := client.DB.(*sql.DB)
	//	if !ok {
	//		return false
	//	}
	//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//	defer cancel()
	//	return db.PingContext(ctx) == nil

	default:
		return false
	}
}

// CheckInstanceHealth verifies the connection health for a specific instance client
func (dm *DatabaseManager) CheckInstanceHealth(clientID string) bool {
	dm.safeLog("info", "Checking health for instance client %s", clientID)
	dm.mu.RLock()
	client, exists := dm.instanceClients[clientID]
	dm.mu.RUnlock()

	if !exists {
		return false
	}

	// Check if client is marked as connected
	if atomic.LoadInt32(&client.IsConnected) != 1 {
		return false
	}

	// Perform database-specific health check
	switch client.InstanceType {
	case "postgres":
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return pool.Ping(ctx) == nil

	case "mysql":
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return db.PingContext(ctx) == nil

	case "mariadb":
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return db.PingContext(ctx) == nil

	case "cockroach":
		pool, ok := client.DB.(*pgxpool.Pool)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return pool.Ping(ctx) == nil

	case "redis":
		client, ok := client.DB.(*goredis.Client)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return client.Ping(ctx).Err() == nil

	case "mongodb":
		db, ok := client.DB.(*mongo.Client)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return db.Ping(ctx, readpref.Primary()) == nil

	case "mssql":
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return db.PingContext(ctx) == nil

	case "cassandra":
		session, ok := client.DB.(*gocql.Session)
		if !ok {
			return false
		}
		return !session.Closed()

	case "edgedb":
		gelClient, ok := client.DB.(*gel.Client)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Simple query to check if the connection is alive
		var result string
		err := gelClient.QuerySingle(ctx, "SELECT 'ping'", &result)
		return err == nil && result == "ping"

	case "snowflake":
		db, ok := client.DB.(*sql.DB)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return db.PingContext(ctx) == nil

	case "clickhouse":
		conn, ok := client.DB.(clickhouse.ClickhouseConn)
		if !ok {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rows, err := conn.Query(ctx, "SELECT 1")
		return err == nil && rows.Next()

	case "pinecone":
		client, ok := client.DB.(*pinecone.PineconeClient)
		if !ok {
			return false
		}
		return client.IsConnected == 1

	case "elasticsearch":
		client, ok := client.DB.(*elasticsearch.ElasticsearchClient)
		if !ok {
			return false
		}
		return client.IsConnected == 1

	case "neo4j":
		driver, ok := client.DB.(neo4jgo.DriverWithContext)
		if !ok {
			return false
		}
		return driver.VerifyConnectivity(context.Background()) == nil

	default:
		return false
	}
}
