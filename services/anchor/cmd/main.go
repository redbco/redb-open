package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/redbco/redb-open/pkg/service"
	"github.com/redbco/redb-open/services/anchor/internal/engine"

	// Import all database adapters to trigger their init() registration
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
)

var (
	port           = flag.Int("port", 50057, "The server port")
	supervisorAddr = flag.String("supervisor", "localhost:50000", "Supervisor address")
	standalone     = flag.Bool("standalone", false, "Run in standalone mode without supervisor connection")
	serviceVersion = "1.0.0"
)

func main() {
	flag.Parse()

	// Create service implementation
	impl := engine.NewService(*standalone)

	// Create base service with implementation
	svc := service.NewBaseService(
		"anchor",
		serviceVersion,
		*port,
		*supervisorAddr,
		impl,
	)

	// Set standalone mode if requested
	if *standalone {
		svc.SetStandaloneMode(true)
	}

	// Create context with signal handling
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Run the service
	if err := svc.Run(ctx); err != nil {
		stop()
		log.Fatalf("Failed to run service: %v", err)
	}
}
