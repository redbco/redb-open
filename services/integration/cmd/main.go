package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/redbco/redb-open/pkg/service"
	"github.com/redbco/redb-open/services/integration/internal/engine"
)

var (
	port           = flag.Int("port", 50063, "The server port")
	supervisorAddr = flag.String("supervisor", "localhost:50000", "Supervisor address")
	serviceVersion = "1.0.0"
)

func main() {
	flag.Parse()

	impl := engine.NewService()

	svc := service.NewBaseService(
		"integration",
		serviceVersion,
		*port,
		*supervisorAddr,
		impl,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := svc.Run(ctx); err != nil {
		stop()
		log.Fatalf("Failed to run service: %v", err)
	}
}
