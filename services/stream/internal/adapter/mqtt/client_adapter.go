package mqtt

import (
	"context"
	"fmt"
	"sync"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/redbco/redb-open/pkg/stream/adapter"
	"github.com/redbco/redb-open/pkg/streamcapabilities"
)

// ClientAdapter implements the MQTT client mode (connects to external broker)
type ClientAdapter struct{}

func NewClientAdapter() *ClientAdapter {
	return &ClientAdapter{}
}

func (a *ClientAdapter) Type() streamcapabilities.StreamPlatform {
	return streamcapabilities.MQTT
}

func (a *ClientAdapter) Capabilities() streamcapabilities.Capability {
	cap, _ := streamcapabilities.Get(streamcapabilities.MQTT)
	return cap
}

func (a *ClientAdapter) Connect(ctx context.Context, cfg adapter.ConnectionConfig) (adapter.Connection, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Build broker URL
	brokerURL := cfg.Endpoint
	if brokerURL == "" && len(cfg.Brokers) > 0 {
		brokerURL = cfg.Brokers[0]
	}
	if brokerURL == "" {
		return nil, fmt.Errorf("broker URL is required")
	}

	// Create client options
	opts := pahomqtt.NewClientOptions()
	opts.AddBroker(brokerURL)

	// Set client ID
	clientID := cfg.Configuration["client_id"]
	if clientID == "" {
		clientID = fmt.Sprintf("redb-stream-%d", time.Now().Unix())
	}
	opts.SetClientID(clientID)

	// Set credentials if provided
	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
		opts.SetPassword(cfg.Password)
	}

	// Set TLS if enabled
	if cfg.TLSEnabled {
		// Note: Would need to build TLS config from cert files
		// For now, just note that TLS is requested
		// In production, would load certificates here
	}

	// Set connection timeouts
	if cfg.ConnectTimeout > 0 {
		opts.SetConnectTimeout(cfg.ConnectTimeout)
	} else {
		opts.SetConnectTimeout(30 * time.Second)
	}

	// Set keep alive
	keepAlive := 60
	if cfg.Configuration["keep_alive"] != "" {
		fmt.Sscanf(cfg.Configuration["keep_alive"], "%d", &keepAlive)
	}
	opts.SetKeepAlive(time.Duration(keepAlive) * time.Second)

	// Set clean session
	cleanSession := true
	if cfg.Configuration["clean_session"] == "false" {
		cleanSession = false
	}
	opts.SetCleanSession(cleanSession)

	// Set QoS
	qos := byte(1)
	if cfg.Configuration["qos"] != "" {
		var q int
		fmt.Sscanf(cfg.Configuration["qos"], "%d", &q)
		qos = byte(q)
	}

	// Set auto reconnect
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(60 * time.Second)

	// Connection status handlers
	opts.SetConnectionLostHandler(func(client pahomqtt.Client, err error) {
		// Log connection loss
	})

	opts.SetOnConnectHandler(func(client pahomqtt.Client) {
		// Log connection establishment
	})

	// Create client
	client := pahomqtt.NewClient(opts)

	// Connect
	token := client.Connect()
	if !token.WaitTimeout(opts.ConnectTimeout) {
		return nil, fmt.Errorf("connection timeout")
	}
	if err := token.Error(); err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	conn := &ClientConnection{
		id:            cfg.ID,
		config:        cfg,
		client:        client,
		qos:           qos,
		subscriptions: make(map[string]pahomqtt.Token),
	}

	return conn, nil
}

// ClientConnection represents an MQTT client connection to a broker
type ClientConnection struct {
	id            string
	config        adapter.ConnectionConfig
	client        pahomqtt.Client
	qos           byte
	subscriptions map[string]pahomqtt.Token
	mu            sync.RWMutex
}

func (c *ClientConnection) ID() string {
	return c.id
}

func (c *ClientConnection) Type() streamcapabilities.StreamPlatform {
	return streamcapabilities.MQTT
}

func (c *ClientConnection) IsConnected() bool {
	return c.client.IsConnected()
}

func (c *ClientConnection) Ping(ctx context.Context) error {
	if !c.client.IsConnected() {
		return fmt.Errorf("client not connected")
	}
	return nil
}

func (c *ClientConnection) Close() error {
	c.client.Disconnect(250) // 250ms graceful disconnect
	return nil
}

func (c *ClientConnection) ProducerOperations() adapter.ProducerOperator {
	return &ClientProducer{conn: c}
}

func (c *ClientConnection) ConsumerOperations() adapter.ConsumerOperator {
	return &ClientConsumer{conn: c}
}

func (c *ClientConnection) AdminOperations() adapter.AdminOperator {
	// MQTT doesn't have admin operations in client mode
	return nil
}

func (c *ClientConnection) Raw() interface{} {
	return c.client
}

func (c *ClientConnection) Config() adapter.ConnectionConfig {
	return c.config
}

func (c *ClientConnection) Adapter() adapter.StreamAdapter {
	return &ClientAdapter{}
}
