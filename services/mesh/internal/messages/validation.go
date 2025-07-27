package messages

import (
	"fmt"
	"sync"
	"time"
)

// MessageValidator validates mesh network messages
type MessageValidator struct {
	rateLimiter *RateLimiter
}

// NewMessageValidator creates a new message validator
func NewMessageValidator() *MessageValidator {
	return &MessageValidator{
		rateLimiter: NewRateLimiter(100, 1000), // 100 messages per second, burst of 1000
	}
}

// ValidateMessage validates a mesh network message
func (v *MessageValidator) ValidateMessage(msg *Message) error {
	// Check rate limiting
	if !v.rateLimiter.Allow(msg.From) {
		return fmt.Errorf("rate limit exceeded for node %s", msg.From)
	}

	// Validate message fields
	if msg.Type == "" {
		return fmt.Errorf("message type is required")
	}
	if msg.From == "" {
		return fmt.Errorf("message sender is required")
	}

	// Validate message type-specific fields
	switch msg.Type {
	case "routing":
		return v.validateRoutingMessage(msg)
	case "consensus":
		return v.validateConsensusMessage(msg)
	case "management":
		return v.validateManagementMessage(msg)
	default:
		return fmt.Errorf("unknown message type: %s", msg.Type)
	}
}

// validateRoutingMessage validates routing messages
func (v *MessageValidator) validateRoutingMessage(msg *Message) error {
	// TODO: Implement routing message validation
	return nil
}

// validateConsensusMessage validates consensus messages
func (v *MessageValidator) validateConsensusMessage(msg *Message) error {
	var consensusMsg ConsensusMessage
	if err := msg.UnmarshalContent(&consensusMsg); err != nil {
		return fmt.Errorf("invalid consensus message format: %v", err)
	}

	if consensusMsg.Term == 0 {
		return fmt.Errorf("consensus term is required")
	}

	switch consensusMsg.Type {
	case "request_vote":
		return v.validateRequestVote(consensusMsg)
	case "append_entries":
		return v.validateAppendEntries(consensusMsg)
	case "heartbeat":
		return v.validateHeartbeat(consensusMsg)
	case "config_change":
		return v.validateConfigChange(consensusMsg)
	default:
		return fmt.Errorf("unknown consensus message type: %s", consensusMsg.Type)
	}
}

// validateRequestVote validates vote request messages
func (v *MessageValidator) validateRequestVote(msg ConsensusMessage) error {
	var voteReq RequestVoteMessage
	if err := msg.UnmarshalContent(&voteReq); err != nil {
		return fmt.Errorf("invalid vote request format: %v", err)
	}

	if voteReq.CandidateID == "" {
		return fmt.Errorf("candidate ID is required")
	}

	return nil
}

// validateAppendEntries validates log replication messages
func (v *MessageValidator) validateAppendEntries(msg ConsensusMessage) error {
	var appendReq AppendEntriesMessage
	if err := msg.UnmarshalContent(&appendReq); err != nil {
		return fmt.Errorf("invalid append entries format: %v", err)
	}

	if appendReq.LeaderID == "" {
		return fmt.Errorf("leader ID is required")
	}

	return nil
}

// validateHeartbeat validates heartbeat messages
func (v *MessageValidator) validateHeartbeat(msg ConsensusMessage) error {
	var heartbeat AppendEntriesMessage
	if err := msg.UnmarshalContent(&heartbeat); err != nil {
		return fmt.Errorf("invalid heartbeat format: %v", err)
	}

	if heartbeat.LeaderID == "" {
		return fmt.Errorf("leader ID is required")
	}

	return nil
}

// validateConfigChange validates configuration change messages
func (v *MessageValidator) validateConfigChange(msg ConsensusMessage) error {
	var configChange ConfigChangeMessage
	if err := msg.UnmarshalContent(&configChange); err != nil {
		return fmt.Errorf("invalid config change format: %v", err)
	}

	if configChange.Type == "" {
		return fmt.Errorf("config change type is required")
	}
	if configChange.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}

	return nil
}

// validateManagementMessage validates management messages
func (v *MessageValidator) validateManagementMessage(msg *Message) error {
	var managementMsg ManagementMessage
	if err := msg.UnmarshalContent(&managementMsg); err != nil {
		return fmt.Errorf("invalid management message format: %v", err)
	}

	switch managementMsg.Type {
	case "node_discovery":
		return v.validateNodeDiscovery(managementMsg)
	case "connection_management":
		return v.validateConnectionManagement(managementMsg)
	case "topology_update":
		return v.validateTopologyUpdate(managementMsg)
	case "health_status":
		return v.validateHealthStatus(managementMsg)
	default:
		return fmt.Errorf("unknown management message type: %s", managementMsg.Type)
	}
}

// validateNodeDiscovery validates node discovery messages
func (v *MessageValidator) validateNodeDiscovery(msg ManagementMessage) error {
	var discoveryMsg NodeDiscoveryMessage
	if err := msg.UnmarshalContent(&discoveryMsg); err != nil {
		return fmt.Errorf("invalid node discovery format: %v", err)
	}

	if discoveryMsg.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}
	if discoveryMsg.Address == "" {
		return fmt.Errorf("node address is required")
	}

	return nil
}

// validateConnectionManagement validates connection management messages
func (v *MessageValidator) validateConnectionManagement(msg ManagementMessage) error {
	var connMsg ConnectionManagementMessage
	if err := msg.UnmarshalContent(&connMsg); err != nil {
		return fmt.Errorf("invalid connection management format: %v", err)
	}

	if connMsg.PeerID == "" {
		return fmt.Errorf("peer ID is required")
	}
	if connMsg.Address == "" {
		return fmt.Errorf("peer address is required")
	}

	return nil
}

// validateTopologyUpdate validates network topology update messages
func (v *MessageValidator) validateTopologyUpdate(msg ManagementMessage) error {
	var topologyMsg TopologyUpdateMessage
	if err := msg.UnmarshalContent(&topologyMsg); err != nil {
		return fmt.Errorf("invalid topology update format: %v", err)
	}

	if topologyMsg.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}
	if topologyMsg.Address == "" {
		return fmt.Errorf("node address is required")
	}

	return nil
}

// validateHealthStatus validates health status update messages
func (v *MessageValidator) validateHealthStatus(msg ManagementMessage) error {
	var healthMsg HealthStatusMessage
	if err := msg.UnmarshalContent(&healthMsg); err != nil {
		return fmt.Errorf("invalid health status format: %v", err)
	}

	if healthMsg.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}
	if healthMsg.Status == "" {
		return fmt.Errorf("health status is required")
	}

	return nil
}

// RateLimiter implements a token bucket rate limiter
type RateLimiter struct {
	rate       float64
	bucketSize float64
	tokens     map[string]float64
	lastUpdate map[string]time.Time
	mu         sync.RWMutex
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(rate, bucketSize float64) *RateLimiter {
	return &RateLimiter{
		rate:       rate,
		bucketSize: bucketSize,
		tokens:     make(map[string]float64),
		lastUpdate: make(map[string]time.Time),
	}
}

// Allow checks if a request from the given key is allowed
func (r *RateLimiter) Allow(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	lastUpdate, exists := r.lastUpdate[key]
	if !exists {
		r.tokens[key] = r.bucketSize
		r.lastUpdate[key] = now
		return true
	}

	elapsed := now.Sub(lastUpdate).Seconds()
	tokens := r.tokens[key] + elapsed*r.rate
	tokens = min(tokens, r.bucketSize)

	if tokens < 1 {
		return false
	}

	r.tokens[key] = tokens - 1
	r.lastUpdate[key] = now
	return true
}

// min returns the minimum of two float64 values
func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
