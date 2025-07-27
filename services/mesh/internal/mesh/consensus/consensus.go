package consensus

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/mesh/message"
	"github.com/redbco/redb-open/services/mesh/internal/storage"
)

// Role represents the role of a node in the consensus protocol
type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

// LogEntry represents an entry in the consensus log
type LogEntry struct {
	Term    uint64      `json:"term"`
	Index   uint64      `json:"index"`
	Command interface{} `json:"command"`
}

// ConsensusState represents the state of a node in the consensus protocol
type ConsensusState struct {
	CurrentTerm uint64
	VotedFor    string
	Log         []LogEntry
	CommitIndex uint64
	LastApplied uint64
	Role        Role
	mu          sync.RWMutex
}

// ConsensusConfig holds the configuration for the consensus protocol
type ConsensusConfig struct {
	NodeID             string
	ElectionTimeout    time.Duration
	HeartbeatInterval  time.Duration
	MinElectionTimeout time.Duration
	MaxElectionTimeout time.Duration
}

// Consensus handles the consensus protocol
type Consensus struct {
	config     ConsensusConfig
	state      *ConsensusState
	store      storage.Interface
	logger     *logger.Logger
	msgChan    chan message.Message
	stopChan   chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
}

// NewConsensus creates a new consensus instance
func NewConsensus(cfg ConsensusConfig, store storage.Interface, logger *logger.Logger) *Consensus {
	ctx, cancel := context.WithCancel(context.Background())
	return &Consensus{
		config: cfg,
		state: &ConsensusState{
			CurrentTerm: 0,
			VotedFor:    "",
			Log:         make([]LogEntry, 0),
			CommitIndex: 0,
			LastApplied: 0,
			Role:        Follower,
		},
		store:      store,
		logger:     logger,
		msgChan:    make(chan message.Message, 100),
		stopChan:   make(chan struct{}),
		ctx:        ctx,
		cancel:     cancel,
		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),
	}
}

// Start begins the consensus protocol
func (c *Consensus) Start() {
	go c.runElectionTimer()
	go c.runHeartbeatTimer()
	go c.applyCommittedEntries()
}

// Stop gracefully shuts down the consensus protocol
func (c *Consensus) Stop() {
	c.cancel()
	close(c.stopChan)
}

// runElectionTimer manages the election timeout
func (c *Consensus) runElectionTimer() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			timeout := c.getRandomElectionTimeout()
			select {
			case <-time.After(timeout):
				c.startElection()
			case <-c.stopChan:
				return
			}
		}
	}
}

// runHeartbeatTimer sends periodic heartbeats if leader
func (c *Consensus) runHeartbeatTimer() {
	ticker := time.NewTicker(c.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if c.isLeader() {
				c.sendHeartbeat()
			}
		}
	}
}

// startElection initiates a new election
func (c *Consensus) startElection() {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	c.state.CurrentTerm++
	c.state.Role = Candidate
	c.state.VotedFor = c.config.NodeID

	// Request votes from all peers
	// TODO: Implement vote request logic
}

// sendHeartbeat sends heartbeat messages to all followers
func (c *Consensus) sendHeartbeat() {
	c.state.mu.RLock()
	defer c.state.mu.RUnlock()

	// TODO: Implement heartbeat sending logic
}

// applyCommittedEntries applies committed log entries to the state machine
func (c *Consensus) applyCommittedEntries() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			c.state.mu.Lock()
			for c.state.LastApplied < c.state.CommitIndex {
				c.state.LastApplied++
				_ = c.state.Log[c.state.LastApplied-1]
				// TODO: Apply entry to state machine
			}
			c.state.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// isLeader checks if the node is currently the leader
func (c *Consensus) isLeader() bool {
	c.state.mu.RLock()
	defer c.state.mu.RUnlock()
	return c.state.Role == Leader
}

// getRandomElectionTimeout returns a random election timeout
func (c *Consensus) getRandomElectionTimeout() time.Duration {
	return c.config.MinElectionTimeout + time.Duration(rand.Int63n(int64(c.config.MaxElectionTimeout-c.config.MinElectionTimeout)))
}

// HandleMessage processes incoming consensus messages
func (c *Consensus) HandleMessage(msg message.Message) error {
	switch msg.Type {
	case message.ConsensusMsg:
		return c.handleConsensusMessage(msg)
	default:
		return fmt.Errorf("unknown message type: %s", msg.Type)
	}
}

// handleConsensusMessage processes consensus-specific messages
func (c *Consensus) handleConsensusMessage(msg message.Message) error {
	// TODO: Implement consensus message handling
	return nil
}

// ProposeCommand proposes a new command to the consensus protocol
func (c *Consensus) ProposeCommand(command interface{}) error {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	if !c.isLeader() {
		return fmt.Errorf("only leader can propose commands")
	}

	entry := LogEntry{
		Term:    c.state.CurrentTerm,
		Index:   uint64(len(c.state.Log) + 1),
		Command: command,
	}

	c.state.Log = append(c.state.Log, entry)
	// TODO: Replicate log to followers

	return nil
}
