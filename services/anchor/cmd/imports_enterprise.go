//go:build enterprise
// +build enterprise

package main

import (
	// Import all database adapters (community + enterprise) to trigger their init() registration

	// Community database adapters
	_ "github.com/redbco/redb-open/services/anchor/internal/database/cassandra"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/chroma"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/clickhouse"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/cockroach"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/cosmosdb"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/dynamodb"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/edgedb"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/elasticsearch"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/iceberg"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/mariadb"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/milvus"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/mongodb"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/mssql"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/mysql"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/neo4j"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/pinecone"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/postgres"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/redis"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/snowflake"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/weaviate"

	// Enterprise database adapters (require native dependencies)
	_ "github.com/redbco/redb-open/services/anchor/internal/database/db2"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/hana"
	_ "github.com/redbco/redb-open/services/anchor/internal/database/oracle"
)
