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

// @title           RedB Client API
// @version         1.0
// @description     This is the client API for RedB.
// @termsOfService  http://swagger.io/terms/

// @contact.name   API Support
// @contact.url    http://www.swagger.io/support
// @contact.email  support@swagger.io

// @license.name  Apache 2.0
// @license.url   http://www.apache.org/licenses/LICENSE-2.0.html

// @host      localhost:50059
// @BasePath  /api/v1

// @securityDefinitions.apikey BearerAuth
// @in header
// @name Authorization

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
