package mqtt

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/redbco/redb-open/pkg/stream/adapter"
	"github.com/redbco/redb-open/pkg/streamcapabilities"
)

// ServerAdapter implements the MQTT server mode (acts as broker)
type ServerAdapter struct{}

func NewServerAdapter() *ServerAdapter {
	return &ServerAdapter{}
}

func (a *ServerAdapter) Type() streamcapabilities.StreamPlatform {
	return streamcapabilities.MQTTServer
}

func (a *ServerAdapter) Capabilities() streamcapabilities.Capability {
	cap, _ := streamcapabilities.Get(streamcapabilities.MQTTServer)
	return cap
}

func (a *ServerAdapter) Connect(ctx context.Context, cfg adapter.ConnectionConfig) (adapter.Connection, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Create a no-op logger to suppress mochi-mqtt's default logging
	// The logging will be handled by the reDB logger
	noopLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Create MQTT server with custom logger
	mqttServer := mqtt.New(&mqtt.Options{
		InlineClient: true, // Enable inline client for programmatic access
		Logger:       noopLogger,
	})

	// Setup authentication - simplified for now
	// In production, would use proper authentication
	allowHook := new(auth.AllowHook)
	if err := mqttServer.AddHook(allowHook, nil); err != nil {
		return nil, fmt.Errorf("failed to add allow hook: %w", err)
	}

	// Determine bind address and port
	bindAddr := cfg.Configuration["bind_address"]
	if bindAddr == "" {
		bindAddr = "0.0.0.0"
	}

	port := 1883 // Default MQTT port
	if cfg.Configuration["port"] != "" {
		p, err := strconv.Atoi(cfg.Configuration["port"])
		if err == nil {
			port = p
		}
	}

	// Create TCP listener
	tcp := listeners.NewTCP(listeners.Config{
		ID:      "t1",
		Address: fmt.Sprintf("%s:%d", bindAddr, port),
	})

	if err := mqttServer.AddListener(tcp); err != nil {
		return nil, fmt.Errorf("failed to add TCP listener: %w", err)
	}

	// Setup TLS listener if enabled
	if cfg.TLSEnabled {
		// Note: Building TLS config would require cert files
		// For now, skip TLS listener if no helper method available
		tlsPort := 8883 // Default MQTT TLS port
		if cfg.Configuration["tls_port"] != "" {
			p, err := strconv.Atoi(cfg.Configuration["tls_port"])
			if err == nil {
				tlsPort = p
			}
		}

		// TLS would be configured here if cert paths are available
		_ = tlsPort // Use variable to avoid unused error
	}

	// Setup WebSocket listener if enabled
	if cfg.Configuration["enable_websocket"] == "true" {
		wsPort := 8080
		if cfg.Configuration["websocket_port"] != "" {
			p, err := strconv.Atoi(cfg.Configuration["websocket_port"])
			if err == nil {
				wsPort = p
			}
		}

		ws := listeners.NewWebsocket(listeners.Config{
			ID:      "ws1",
			Address: fmt.Sprintf("%s:%d", bindAddr, wsPort),
		})

		if err := mqttServer.AddListener(ws); err != nil {
			return nil, fmt.Errorf("failed to add WebSocket listener: %w", err)
		}
	}

	// Start server in background
	go func() {
		if err := mqttServer.Serve(); err != nil {
			// Log error
		}
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	conn := &ServerConnection{
		id:            cfg.ID,
		config:        cfg,
		server:        mqttServer,
		bindAddr:      bindAddr,
		port:          port,
		messageBuffer: make(chan *adapter.Message, 1000),
	}

	return conn, nil
}

// ServerConnection represents an MQTT server instance
type ServerConnection struct {
	id            string
	config        adapter.ConnectionConfig
	server        *mqtt.Server
	bindAddr      string
	port          int
	messageBuffer chan *adapter.Message
	mu            sync.RWMutex
}

func (c *ServerConnection) ID() string {
	return c.id
}

func (c *ServerConnection) Type() streamcapabilities.StreamPlatform {
	return streamcapabilities.MQTTServer
}

func (c *ServerConnection) IsConnected() bool {
	// Check if server is listening
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", c.bindAddr, c.port), 1*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func (c *ServerConnection) Ping(ctx context.Context) error {
	if !c.IsConnected() {
		return fmt.Errorf("server not listening")
	}
	return nil
}

func (c *ServerConnection) Close() error {
	return c.server.Close()
}

func (c *ServerConnection) ProducerOperations() adapter.ProducerOperator {
	return &ServerProducer{conn: c}
}

func (c *ServerConnection) ConsumerOperations() adapter.ConsumerOperator {
	return &ServerConsumer{conn: c}
}

func (c *ServerConnection) AdminOperations() adapter.AdminOperator {
	return &ServerAdmin{conn: c}
}

func (c *ServerConnection) Raw() interface{} {
	return c.server
}

func (c *ServerConnection) Config() adapter.ConnectionConfig {
	return c.config
}

func (c *ServerConnection) Adapter() adapter.StreamAdapter {
	return &ServerAdapter{}
}
