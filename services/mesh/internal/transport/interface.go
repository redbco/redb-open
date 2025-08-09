package transport

import (
	"time"
)

// Transport represents the transport layer interface
type Transport interface {
	// Lifecycle
	Start() error
	Stop() error

	// Connection management
	Connect(remoteAddr, nodeID, linkID string) (Link, error)
	GetLink(linkID string) (Link, bool)
	ListLinks() []Link
	CloseLink(linkID string) error

	// Statistics
	Stats() map[string]map[int]LaneStats
}

// Link represents a logical connection between two nodes
type Link interface {
	// Basic info
	ID() string
	LocalNodeID() string
	RemoteNodeID() string
	Status() LinkStatus

	// Stream management
	OpenStream(params StreamParams) (Stream, error)

	// Data transfer
	Send(data []byte, laneID int) error

	// Statistics
	Stats() map[int]LaneStats
	Backpressure() map[int]BackpressureState

	// Lifecycle
	Close() error
}

// Stream represents a logical stream within a link
type Stream interface {
	// Basic info
	ID() string
	LaneID() int
	Class() LaneClass
	Priority() int
	Status() StreamStatus

	// Data transfer
	Send(data []byte) error
	Receive() ([]byte, error)

	// Lifecycle
	Close() error
}

// StreamParams defines parameters for opening a stream
type StreamParams struct {
	StreamID string
	Class    LaneClass
	Priority int
	Size     int64
}

// LaneClass represents the class/priority of a lane
type LaneClass int

const (
	LaneClassControl LaneClass = iota
	LaneClassPriority
	LaneClassBulk
)

// LinkStatus represents the status of a link
type LinkStatus int

const (
	LinkStatusConnecting LinkStatus = iota
	LinkStatusConnected
	LinkStatusDegraded
	LinkStatusFailed
	LinkStatusClosed
)

// StreamStatus represents the status of a stream
type StreamStatus int

const (
	StreamStatusOpening StreamStatus = iota
	StreamStatusOpen
	StreamStatusClosed
	StreamStatusFailed
)

// LaneStats tracks performance metrics for a lane
type LaneStats struct {
	MessagesSent     int64
	MessagesReceived int64
	BytesSent        int64
	BytesReceived    int64
	Latency          time.Duration
	LastHeartbeat    time.Time
	LastActivity     time.Time
	SendQueueDepth   int
	RecvQueueDepth   int
}

// BackpressureState represents the backpressure state of a lane
type BackpressureState struct {
	QueueDepth int
	QueueCap   int
	Status     LaneStatus
}

// LaneStatus represents the status of a lane
type LaneStatus int

const (
	LaneStatusConnecting LaneStatus = iota
	LaneStatusConnected
	LaneStatusDegraded
	LaneStatusFailed
	LaneStatusClosed
)

// Message represents a message sent over the transport
type Message struct {
	Type      MessageType
	StreamID  string
	LaneID    int
	Payload   []byte
	Headers   map[string]string
	Timestamp time.Time
}

// MessageType represents the type of message
type MessageType int

const (
	MessageTypeData MessageType = iota
	MessageTypeControl
	MessageTypeHeartbeat
	MessageTypeAck
)

// TransportConfig holds configuration for transport implementations
type TransportConfig struct {
	ListenAddr        string
	ReadBufferSize    int
	WriteBufferSize   int
	MaxMessageSize    int64
	HandshakeTimeout  time.Duration
	WriteTimeout      time.Duration
	PongWait          time.Duration
	PingPeriod        time.Duration
	MaxConnections    int
	EnableCompression bool
}

// DefaultConfig returns default transport configuration
func DefaultConfig() TransportConfig {
	return TransportConfig{
		ListenAddr:        ":8080",
		ReadBufferSize:    1024,
		WriteBufferSize:   1024,
		MaxMessageSize:    512 * 1024,
		HandshakeTimeout:  10 * time.Second,
		WriteTimeout:      10 * time.Second,
		PongWait:          60 * time.Second,
		PingPeriod:        54 * time.Second,
		MaxConnections:    1000,
		EnableCompression: true,
	}
}

// TransportFactory creates transport instances
type TransportFactory interface {
	Create(config TransportConfig) (Transport, error)
}

// TransportType represents the type of transport
type TransportType string

const (
	TransportTypeWebSocket TransportType = "websocket"
	TransportTypeTCP       TransportType = "tcp"
	TransportTypeUDP       TransportType = "udp"
)

// FactoryRegistry manages transport factories
type FactoryRegistry struct {
	factories map[TransportType]TransportFactory
}

// NewFactoryRegistry creates a new factory registry
func NewFactoryRegistry() *FactoryRegistry {
	return &FactoryRegistry{
		factories: make(map[TransportType]TransportFactory),
	}
}

// Register registers a transport factory
func (r *FactoryRegistry) Register(transportType TransportType, factory TransportFactory) {
	r.factories[transportType] = factory
}

// Create creates a transport instance
func (r *FactoryRegistry) Create(transportType TransportType, config TransportConfig) (Transport, error) {
	factory, exists := r.factories[transportType]
	if !exists {
		return nil, ErrTransportTypeNotSupported
	}

	return factory.Create(config)
}

// ErrTransportTypeNotSupported is returned when a transport type is not supported
var ErrTransportTypeNotSupported = &TransportError{
	Type:    "transport_type_not_supported",
	Message: "Transport type not supported",
}

// TransportError represents a transport-related error
type TransportError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

func (e *TransportError) Error() string {
	return e.Message
}
