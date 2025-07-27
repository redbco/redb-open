package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/redbco/redb-open/pkg/logger"
)

// Command represents a command to be applied to the state machine
type Command struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// State represents the current state of the consensus group
type State struct {
	Members   map[string]bool        `json:"members"`
	Config    map[string]interface{} `json:"config"`
	LastIndex uint64                 `json:"last_index"`
	LastTerm  uint64                 `json:"last_term"`
}

// StateMachine implements the Raft FSM interface
type StateMachine struct {
	logger *logger.Logger
	mu     sync.RWMutex
	state  *State
}

// NewStateMachine creates a new state machine
func NewStateMachine(logger *logger.Logger) *StateMachine {
	return &StateMachine{
		logger: logger,
		state: &State{
			Members: make(map[string]bool),
			Config:  make(map[string]interface{}),
		},
	}
}

// Apply applies a log entry to the state machine
func (s *StateMachine) Apply(log *raft.Log) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		s.logger.Error("Failed to unmarshal command: (error: %v, index: %d, term: %d)", err, log.Index, log.Term)
		return err
	}

	s.logger.Debug("Applying command: (type: %s, index: %d, term: %d)", cmd.Type, log.Index, log.Term)

	var err error
	switch cmd.Type {
	case "add_member":
		err = s.handleAddMember(cmd.Payload)
	case "remove_member":
		err = s.handleRemoveMember(cmd.Payload)
	case "update_config":
		err = s.handleUpdateConfig(cmd.Payload)
	default:
		err = fmt.Errorf("unknown command type: %s", cmd.Type)
	}

	if err != nil {
		s.logger.Error("Failed to apply command: (error: %v, type: %s)", err, cmd.Type)
		return err
	}

	s.state.LastIndex = log.Index
	s.state.LastTerm = log.Term
	return nil
}

// handleAddMember handles the add_member command
func (s *StateMachine) handleAddMember(payload json.RawMessage) error {
	var memberID string
	if err := json.Unmarshal(payload, &memberID); err != nil {
		return fmt.Errorf("failed to unmarshal member ID: %v", err)
	}

	s.state.Members[memberID] = true
	s.logger.Info("Added member to consensus group: (member_id: %s)", memberID)
	return nil
}

// handleRemoveMember handles the remove_member command
func (s *StateMachine) handleRemoveMember(payload json.RawMessage) error {
	var memberID string
	if err := json.Unmarshal(payload, &memberID); err != nil {
		return fmt.Errorf("failed to unmarshal member ID: %v", err)
	}

	delete(s.state.Members, memberID)
	s.logger.Info("Removed member from consensus group: (member_id: %s)", memberID)
	return nil
}

// handleUpdateConfig handles the update_config command
func (s *StateMachine) handleUpdateConfig(payload json.RawMessage) error {
	var config map[string]interface{}
	if err := json.Unmarshal(payload, &config); err != nil {
		return fmt.Errorf("failed to unmarshal config: %v", err)
	}

	for k, v := range config {
		s.state.Config[k] = v
	}
	s.logger.Info("Updated consensus group config")
	return nil
}

// Snapshot returns a snapshot of the state machine
func (s *StateMachine) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Create a deep copy of the state
	stateCopy := &State{
		Members:   make(map[string]bool),
		Config:    make(map[string]interface{}),
		LastIndex: s.state.LastIndex,
		LastTerm:  s.state.LastTerm,
	}

	for k, v := range s.state.Members {
		stateCopy.Members[k] = v
	}
	for k, v := range s.state.Config {
		stateCopy.Config[k] = v
	}

	return &Snapshot{state: stateCopy}, nil
}

// Restore restores the state machine from a snapshot
func (s *StateMachine) Restore(rc io.ReadCloser) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var state State
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return fmt.Errorf("failed to decode snapshot: %v", err)
	}

	s.state = &state
	s.logger.Info("Restored state from snapshot: (last_index: %d, last_term: %d)", state.LastIndex, state.LastTerm)
	return nil
}

// GetState returns a copy of the current state
func (s *StateMachine) GetState() *State {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stateCopy := &State{
		Members:   make(map[string]bool),
		Config:    make(map[string]interface{}),
		LastIndex: s.state.LastIndex,
		LastTerm:  s.state.LastTerm,
	}

	for k, v := range s.state.Members {
		stateCopy.Members[k] = v
	}
	for k, v := range s.state.Config {
		stateCopy.Config[k] = v
	}

	return stateCopy
}

// Snapshot represents a snapshot of the state machine
type Snapshot struct {
	state *State
}

// Persist saves the snapshot to the given sink
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.state); err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to encode snapshot: %v", err)
	}
	return sink.Close()
}

// Release releases any resources associated with the snapshot
func (s *Snapshot) Release() {}
