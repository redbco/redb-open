package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/redbco/redb-open/pkg/service"
	"github.com/redbco/redb-open/services/clientapi/internal/engine"
)

var (
	port           = flag.Int("port", 50059, "The server port")
	supervisorAddr = flag.String("supervisor", "localhost:50000", "Supervisor address")
	serviceVersion = "1.0.0"
)

func main() {
	flag.Parse()

	// Create service implementation
	impl := engine.NewService()

	// Create base service with implementation
	svc := service.NewBaseService(
		"clientapi",
		serviceVersion,
		*port,
		*supervisorAddr,
		impl,
	)

	// Create context with signal handling
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Run the service
	if err := svc.Run(ctx); err != nil {
		stop()
		log.Fatalf("Failed to run service: %v", err)
	}
}
