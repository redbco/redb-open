package mesh

import (
	"context"
	"sync"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
)

// NOTE: This file contains a custom consensus implementation that is separate from
// the Raft-based consensus implementation in services/mesh/internal/consensus/.
// This custom implementation is for mesh-level consensus operations,
// while the Raft implementation is for service level consensus groups.
// TODO: These might be merged in the future.

// ConsensusState represents the state of a node in the consensus protocol
type ConsensusState string

const (
	Follower  ConsensusState = "follower"
	Candidate ConsensusState = "candidate"
	Leader    ConsensusState = "leader"
)

// ConsensusConfig holds the configuration for the consensus protocol
type ConsensusConfig struct {
	ElectionTimeout  time.Duration `yaml:"election_timeout"`
	HeartbeatTimeout time.Duration `yaml:"heartbeat_timeout"`
	MinVotes         int           `yaml:"min_votes"`
}

// ConsensusLog represents a log entry in the consensus protocol
type ConsensusLog struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// ConsensusVote represents a vote in the consensus protocol
type ConsensusVote struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// ConsensusResponse represents a response to a consensus request
type ConsensusResponse struct {
	Term    uint64
	Success bool
	Error   error
}

// ConsensusRequest represents a request in the consensus protocol
type ConsensusRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []ConsensusLog
	LeaderCommit uint64
}

// Consensus implements the consensus protocol
type Consensus struct {
	nodeID      string
	config      ConsensusConfig
	logger      *logger.Logger
	state       ConsensusState
	term        uint64
	votedFor    string
	logs        []ConsensusLog
	commitIndex uint64
	lastApplied uint64
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewConsensus creates a new consensus instance
func NewConsensus(nodeID string, cfg ConsensusConfig, logger *logger.Logger) *Consensus {
	ctx, cancel := context.WithCancel(context.Background())
	return &Consensus{
		nodeID:      nodeID,
		config:      cfg,
		logger:      logger,
		state:       Follower,
		term:        0,
		votedFor:    "",
		logs:        make([]ConsensusLog, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start starts the consensus protocol
func (c *Consensus) Start() {
	c.logger.Info("Starting consensus protocol: (node_id: %s, state: %s)", c.nodeID, c.state)

	go c.runElectionTimer()
}

// Stop stops the consensus protocol
func (c *Consensus) Stop() {
	c.logger.Info("Stopping consensus protocol: (node_id: %s)", c.nodeID)
	c.cancel()
}

// RequestVote handles a vote request from a candidate
func (c *Consensus) RequestVote(req ConsensusVote) ConsensusResponse {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If request term is less than current term, reject
	if req.Term < c.term {
		return ConsensusResponse{
			Term:    c.term,
			Success: false,
		}
	}

	// If request term is greater than current term, become follower
	if req.Term > c.term {
		c.term = req.Term
		c.state = Follower
		c.votedFor = ""
	}

	// Check if we can vote for this candidate
	canVote := c.votedFor == "" || c.votedFor == req.CandidateID
	lastLogIndex := uint64(len(c.logs) - 1)
	lastLogTerm := c.logs[lastLogIndex].Term

	if canVote && (req.LastLogTerm > lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)) {
		c.votedFor = req.CandidateID
		return ConsensusResponse{
			Term:    c.term,
			Success: true,
		}
	}

	return ConsensusResponse{
		Term:    c.term,
		Success: false,
	}
}

// AppendEntries handles an append entries request from a leader
func (c *Consensus) AppendEntries(req ConsensusRequest) ConsensusResponse {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If request term is less than current term, reject
	if req.Term < c.term {
		return ConsensusResponse{
			Term:    c.term,
			Success: false,
		}
	}

	// If request term is greater than or equal to current term, become follower
	if req.Term >= c.term {
		c.term = req.Term
		c.state = Follower
		c.votedFor = ""
	}

	// Check if log consistency is maintained
	if req.PrevLogIndex >= uint64(len(c.logs)) {
		return ConsensusResponse{
			Term:    c.term,
			Success: false,
		}
	}

	if req.PrevLogIndex > 0 {
		prevLog := c.logs[req.PrevLogIndex-1]
		if prevLog.Term != req.PrevLogTerm {
			return ConsensusResponse{
				Term:    c.term,
				Success: false,
			}
		}
	}

	// Append new entries
	if len(req.Entries) > 0 {
		c.logs = c.logs[:req.PrevLogIndex]
		c.logs = append(c.logs, req.Entries...)
	}

	// Update commit index
	if req.LeaderCommit > c.commitIndex {
		c.commitIndex = min(req.LeaderCommit, uint64(len(c.logs)-1))
	}

	return ConsensusResponse{
		Term:    c.term,
		Success: true,
	}
}

// runElectionTimer runs the election timer
func (c *Consensus) runElectionTimer() {
	ticker := time.NewTicker(c.config.ElectionTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			if c.state != Leader {
				c.startElection()
			}
			c.mu.Unlock()
		}
	}
}

// startElection starts a new election
func (c *Consensus) startElection() {
	c.state = Candidate
	c.term++
	c.votedFor = c.nodeID
	c.logger.Info("Starting election: (node_id: %s, term: %d)", c.nodeID, c.term)

	// TODO: Send RequestVote RPCs to all other nodes
	// This would involve:
	// 1. Creating a vote request
	// 2. Sending it to all peers
	// 3. Collecting responses
	// 4. Becoming leader if majority votes received
}

// min returns the minimum of two uint64 values
func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
