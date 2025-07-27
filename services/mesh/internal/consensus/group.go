package consensus

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/consensus/fsm"
)

// Config holds the consensus group configuration
type Config struct {
	GroupID      string
	NodeID       string
	DataDir      string
	BindAddr     string
	Peers        []string
	SnapshotPath string
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
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	// Create FSM
	fsm := fsm.NewStateMachine(logger)

	// Create Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)

	// Create log store
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create log store: %v", err)
	}

	// Create stable store
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "stable.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create stable store: %v", err)
	}

	// Create snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.SnapshotPath, 3, os.Stderr)
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

// StateMachine implements the Raft FSM interface
type StateMachine struct {
	logger *logger.Logger
	mu     sync.RWMutex
	state  map[string]interface{}
}

// NewStateMachine creates a new state machine
func NewStateMachine(logger *logger.Logger) *StateMachine {
	return &StateMachine{
		logger: logger,
		state:  make(map[string]interface{}),
	}
}

// Apply applies a log entry to the state machine
func (s *StateMachine) Apply(log *raft.Log) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	// TODO: Implement state machine logic
	s.logger.Debug("Applying log entry: (index: %d, term: %d, type: %s)", log.Index, log.Term, log.Type.String())

	return nil
}

// Snapshot returns a snapshot of the state machine
func (s *StateMachine) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// TODO: Implement snapshot logic
	return &Snapshot{state: s.state}, nil
}

// Restore restores the state machine from a snapshot
func (s *StateMachine) Restore(rc io.ReadCloser) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// TODO: Implement restore logic
	return nil
}

// Snapshot represents a snapshot of the state machine
type Snapshot struct {
	state map[string]interface{}
}

// Persist saves the snapshot to the given sink
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	// TODO: Implement persist logic
	return nil
}

// Release releases any resources associated with the snapshot
func (s *Snapshot) Release() {}
