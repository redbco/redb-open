package consensus

// Package consensus provides Raft-based distributed consensus functionality.
// This implementation uses HashiCorp Raft for leader election, log replication,
// and state machine replication across a cluster of nodes.
//
// The consensus group manages:
// - Leader election and failover
// - Log replication and consistency
// - State machine operations
// - Membership changes
// - Snapshot and recovery

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/consensus/fsm"
	"github.com/redbco/redb-open/services/mesh/internal/consensus/stores"
)

// Config holds the consensus group configuration
type Config struct {
	GroupID      string
	NodeID       string
	DataDir      string
	BindAddr     string
	Peers        []string
	SnapshotPath string
	PostgreSQL   *database.PostgreSQL
	Redis        *database.Redis
}

// Group represents a consensus group
type Group struct {
	config    Config
	raft      *raft.Raft
	fsm       *fsm.StateMachine
	logger    *logger.Logger
	mu        sync.RWMutex
	state     GroupState
	leaderID  string
	term      uint64
	closeChan chan struct{}
}

// NewGroup creates a new consensus group
func NewGroup(cfg Config, logger *logger.Logger) (*Group, error) {
	// Validate configuration
	if cfg.GroupID == "" {
		return nil, fmt.Errorf("group ID is required")
	}
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("node ID is required")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	if cfg.SnapshotPath == "" {
		return nil, fmt.Errorf("snapshot path is required")
	}

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	// Create FSM
	fsm := fsm.NewStateMachine(logger)

	// Create Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)

	// Validate database connections
	if cfg.PostgreSQL == nil {
		return nil, fmt.Errorf("PostgreSQL connection is required")
	}
	if cfg.Redis == nil {
		return nil, fmt.Errorf("Redis connection is required")
	}

	// Create log store using PostgreSQL
	logStore, err := stores.NewPostgresLogStore(cfg.PostgreSQL, logger, cfg.GroupID)
	if err != nil {
		return nil, fmt.Errorf("failed to create log store: %v", err)
	}

	// Create stable store using PostgreSQL
	stableStore, err := stores.NewPostgresStableStore(cfg.PostgreSQL, logger, cfg.GroupID)
	if err != nil {
		return nil, fmt.Errorf("failed to create stable store: %v", err)
	}

	// Create snapshot store using Redis
	snapshotStore, err := stores.NewRedisSnapshotStore(cfg.Redis, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %v", err)
	}

	// Create transport
	transport, err := raft.NewTCPTransport(cfg.BindAddr, nil, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %v", err)
	}

	// Create Raft instance
	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create Raft instance: %v", err)
	}

	// Bootstrap the cluster if this is the first node
	if len(cfg.Peers) == 0 {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(cfg.NodeID),
					Address: transport.LocalAddr(),
				},
			},
		}
		if err := r.BootstrapCluster(configuration).Error(); err != nil {
			return nil, fmt.Errorf("failed to bootstrap cluster: %v", err)
		}
	}

	return &Group{
		config:    cfg,
		raft:      r,
		fsm:       fsm,
		logger:    logger,
		state:     GroupStateForming,
		closeChan: make(chan struct{}),
	}, nil
}

// Start starts the consensus group
func (g *Group) Start() error {
	go g.monitorState()
	return nil
}

// monitorState monitors the Raft state and updates the group state accordingly
func (g *Group) monitorState() {
	for {
		select {
		case <-g.closeChan:
			return
		default:
			state := g.raft.State()
			g.mu.Lock()
			switch state {
			case raft.Leader:
				g.state = GroupStateActive
				g.leaderID = g.config.NodeID
			case raft.Follower:
				g.state = GroupStateActive
				g.leaderID = string(g.raft.Leader())
			case raft.Candidate:
				g.state = GroupStateForming
				g.leaderID = ""
			case raft.Shutdown:
				g.state = GroupStateDisbanded
				g.leaderID = ""
			}
			stats := g.raft.Stats()
			if term, ok := stats["last_log_term"]; ok {
				if termUint, err := strconv.ParseUint(term, 10, 64); err == nil {
					g.term = termUint
				}
			}
			g.mu.Unlock()

			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Apply applies a command to the state machine
func (g *Group) Apply(cmd []byte) error {
	future := g.raft.Apply(cmd, 5*time.Second)
	return future.Error()
}

// ApplyCommand applies a structured command to the state machine
func (g *Group) ApplyCommand(cmdType string, payload interface{}) error {
	// Marshal payload to JSON
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %v", err)
	}

	// Create command structure
	cmd := fsm.Command{
		Type:    cmdType,
		Payload: json.RawMessage(payloadBytes),
	}

	// Marshal command to JSON
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %v", err)
	}

	return g.Apply(cmdBytes)
}

// GetState returns the current state of the group
func (g *Group) GetState() GroupState {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state
}

// GetLeader returns the current leader ID
func (g *Group) GetLeader() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.leaderID
}

// GetTerm returns the current term
func (g *Group) GetTerm() uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.term
}

// GetMembers returns the current members of the consensus group
func (g *Group) GetMembers() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	state := g.fsm.GetState()
	members := make([]string, 0, len(state.Members))
	for memberID := range state.Members {
		members = append(members, memberID)
	}
	return members
}

// GetConfig returns the current configuration of the consensus group
func (g *Group) GetConfig() map[string]interface{} {
	g.mu.RLock()
	defer g.mu.RUnlock()

	state := g.fsm.GetState()
	return state.Config
}

// AddMember adds a member to the consensus group
func (g *Group) AddMember(memberID string) error {
	return g.ApplyCommand("add_member", memberID)
}

// RemoveMember removes a member from the consensus group
func (g *Group) RemoveMember(memberID string) error {
	return g.ApplyCommand("remove_member", memberID)
}

// UpdateConfig updates the configuration of the consensus group
func (g *Group) UpdateConfig(config map[string]interface{}) error {
	return g.ApplyCommand("update_config", config)
}

// AddPeer adds a new peer to the cluster
func (g *Group) AddPeer(peerID string, peerAddr string) error {
	future := g.raft.AddVoter(raft.ServerID(peerID), raft.ServerAddress(peerAddr), 0, 0)
	return future.Error()
}

// RemovePeer removes a peer from the cluster
func (g *Group) RemovePeer(peerID string) error {
	future := g.raft.RemoveServer(raft.ServerID(peerID), 0, 0)
	return future.Error()
}

// Shutdown gracefully shuts down the consensus group
func (g *Group) Shutdown() error {
	close(g.closeChan)
	return g.raft.Shutdown().Error()
}

// GroupState represents the current state of a consensus group
type GroupState int

const (
	GroupStateUnspecified GroupState = iota
	GroupStateForming
	GroupStateActive
	GroupStateDegraded
	GroupStateDisbanded
)
