package main

import (
	"context"
	"flag"
	"log"

	"github.com/redbco/redb-open/pkg/service"
	"github.com/redbco/redb-open/services/mesh/internal/engine"
)

var (
	port           = flag.Int("port", 50056, "The server port")
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
		"mesh",
		serviceVersion,
		*port,
		*supervisorAddr,
		impl,
	)

	// Set standalone mode if requested
	if *standalone {
		svc.SetStandaloneMode(true)
	}

	// Run the service
	ctx := context.Background()
	if err := svc.Run(ctx); err != nil {
		log.Fatalf("Failed to run service: %v", err)
	}
}
