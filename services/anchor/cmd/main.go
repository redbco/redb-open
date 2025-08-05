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
)

var (
	port           = flag.Int("port", 50055, "The server port")
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
