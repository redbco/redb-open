package main

import (
	"context"
	"flag"
	"log"

	"github.com/redbco/redb-open/pkg/service"
	"github.com/redbco/redb-open/services/core/internal/engine"
)

var (
	port           = flag.Int("port", 50062, "The server port")
	supervisorAddr = flag.String("supervisor", "localhost:50000", "Supervisor address")
	serviceVersion = "1.0.0"
)

func main() {
	flag.Parse()

	// Create service implementation
	impl := engine.NewService()

	// Create base service with implementation
	svc := service.NewBaseService(
		"core",
		serviceVersion,
		*port,
		*supervisorAddr,
		impl,
	)

	// Run the service
	ctx := context.Background()
	if err := svc.Run(ctx); err != nil {
		log.Fatalf("Failed to run service: %v", err)
	}
}
