package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
)

// Config holds the storage configuration
type Config struct {
	Type string `yaml:"type" json:"type"`
}

// LocalIdentity represents the local node identity
type LocalIdentity struct {
	IdentityID string `json:"identity_id"`
}

// MeshInfo represents mesh configuration
type MeshInfo struct {
	MeshID          string    `json:"mesh_id"`
	MeshName        string    `json:"mesh_name"`
	MeshDescription string    `json:"mesh_description"`
	AllowJoin       bool      `json:"allow_join"`
	Status          string    `json:"status"`
	Created         time.Time `json:"created"`
	Updated         time.Time `json:"updated"`
}

// NodeInfo represents node configuration
type NodeInfo struct {
	NodeID          string    `json:"node_id"`
	NodeName        string    `json:"node_name"`
	NodeDescription string    `json:"node_description"`
	NodePlatform    string    `json:"node_platform"`
	NodeVersion     string    `json:"node_version"`
	RegionID        *string   `json:"region_id"`
	IPAddress       string    `json:"ip_address"`
	Port            int       `json:"port"`
	Status          string    `json:"status"`
	Created         time.Time `json:"created"`
	Updated         time.Time `json:"updated"`
}

// RouteInfo represents route configuration
type RouteInfo struct {
	RouteID            string    `json:"route_id"`
	SourceNodeID       string    `json:"source_node_id"`
	TargetNodeID       string    `json:"target_node_id"`
	RouteBidirectional bool      `json:"route_bidirectional"`
	RouteLatency       float64   `json:"route_latency"`
	RouteBandwidth     float64   `json:"route_bandwidth"`
	RouteCost          int       `json:"route_cost"`
	Status             string    `json:"status"`
	Created            time.Time `json:"created"`
	Updated            time.Time `json:"updated"`
}

// Interface defines the storage interface for mesh operations
type Interface interface {
	// Message operations
	StoreMessage(ctx context.Context, msg *Message) error
	GetMessage(ctx context.Context, id string) (*Message, error)
	DeleteMessage(ctx context.Context, id string) error

	// State operations
	StoreState(ctx context.Context, key string, value []byte) error
	GetState(ctx context.Context, key string) ([]byte, error)
	DeleteState(ctx context.Context, key string) error

	// Node state operations
	SaveNodeState(ctx context.Context, nodeID string, state interface{}) error
	GetNodeState(ctx context.Context, nodeID string) (interface{}, error)
	DeleteNodeState(ctx context.Context, nodeID string) error

	// Consensus log operations
	AppendLog(ctx context.Context, term uint64, index uint64, entry interface{}) error
	GetLog(ctx context.Context, index uint64) (interface{}, error)
	GetLogs(ctx context.Context, startIndex, endIndex uint64) ([]interface{}, error)
	DeleteLogs(ctx context.Context, startIndex, endIndex uint64) error

	// Route operations
	SaveRoute(ctx context.Context, destination string, route interface{}) error
	GetRoute(ctx context.Context, destination string) (interface{}, error)
	GetRoutes(ctx context.Context) (map[string]interface{}, error)
	DeleteRoute(ctx context.Context, destination string) error

	// Configuration operations
	SaveConfig(ctx context.Context, key string, value interface{}) error
	GetConfig(ctx context.Context, key string) (interface{}, error)
	DeleteConfig(ctx context.Context, key string) error

	// Mesh initialization operations
	GetLocalIdentity(ctx context.Context) (*LocalIdentity, error)
	GetMeshInfo(ctx context.Context) (*MeshInfo, error)
	GetNodeInfo(ctx context.Context, nodeID string) (*NodeInfo, error)
	GetRoutesForNode(ctx context.Context, nodeID string) ([]*RouteInfo, error)

	// Transaction operations
	CreateTransaction(ctx context.Context) (Transaction, error)

	// Administrative operations
	CreateBackup(ctx context.Context, path string) error
	RestoreFromBackup(ctx context.Context, path string) error

	// Connection management
	Close() error
}

// Transaction represents a storage transaction
type Transaction interface {
	// Message operations within transaction
	StoreMessage(ctx context.Context, msg *Message) error
	GetMessage(ctx context.Context, id string) (*Message, error)

	// State operations within transaction
	StoreState(ctx context.Context, key string, value []byte) error
	GetState(ctx context.Context, key string) ([]byte, error)

	// Transaction control
	Commit() error
	Rollback() error
}

// Message represents a message in the storage
type Message struct {
	ID        string `json:"id"`
	From      string `json:"from"`
	To        string `json:"to"`
	Content   []byte `json:"content"`
	Timestamp int64  `json:"timestamp"`
}

// NewStorage creates a new storage instance based on the configuration
func NewStorage(ctx context.Context, config Config, logger *logger.Logger) (Interface, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	switch config.Type {
	case "postgres", "postgresql":
		return NewPostgresStorage(ctx, config, logger)
	case "memory":
		// TODO: Implement memory storage for testing
		return nil, fmt.Errorf("memory storage not implemented")
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", config.Type)
	}
}

// ConfigFromGlobal creates a storage config from global configuration
func ConfigFromGlobal(globalConfig interface{}) Config {
	// Default configuration
	return Config{
		Type: "postgres",
	}
}

// ConfigFromConnectionString creates a storage config from a connection string
func ConfigFromConnectionString(connStr string) (Config, error) {
	return Config{
		Type: "postgres",
	}, nil
}
