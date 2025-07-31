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
	// First validate the message structure
	if err := ValidateMessage(msg); err != nil {
		return fmt.Errorf("message structure validation failed: %w", err)
	}

	// Check rate limiting
	if !v.rateLimiter.Allow(msg.Header.From) {
		return fmt.Errorf("rate limit exceeded for node %s", msg.Header.From)
	}

	// Validate message type-specific fields
	switch msg.Header.Type {
	case "routing":
		return v.validateRoutingMessage(msg)
	case "consensus":
		return v.validateConsensusMessage(msg)
	case "management":
		return v.validateManagementMessage(msg)
	case "heartbeat":
		return v.validateHeartbeatMessage(msg)
	case "data":
		return v.validateDataMessage(msg)
	default:
		return fmt.Errorf("unknown message type: %s", msg.Header.Type)
	}
}

// validateRoutingMessage validates routing messages
func (v *MessageValidator) validateRoutingMessage(msg *Message) error {
	var routingPayload RoutingPayload
	if err := msg.UnmarshalPayload(&routingPayload); err != nil {
		return fmt.Errorf("invalid routing message format: %w", err)
	}

	if routingPayload.SubType == "" {
		return fmt.Errorf("routing sub-type is required")
	}

	switch routingPayload.SubType {
	case "route_update":
		return v.validateRouteUpdate(routingPayload)
	case "route_request":
		return v.validateRouteRequest(routingPayload)
	case "route_response":
		return v.validateRouteResponse(routingPayload)
	default:
		return fmt.Errorf("unknown routing sub-type: %s", routingPayload.SubType)
	}
}

// validateConsensusMessage validates consensus messages
func (v *MessageValidator) validateConsensusMessage(msg *Message) error {
	var consensusPayload ConsensusPayload
	if err := msg.UnmarshalPayload(&consensusPayload); err != nil {
		return fmt.Errorf("invalid consensus message format: %w", err)
	}

	if consensusPayload.Term == 0 {
		return fmt.Errorf("consensus term is required")
	}

	if consensusPayload.SubType == "" {
		return fmt.Errorf("consensus sub-type is required")
	}

	switch consensusPayload.SubType {
	case "request_vote":
		return v.validateRequestVote(consensusPayload)
	case "append_entries":
		return v.validateAppendEntries(consensusPayload)
	case "heartbeat":
		return v.validateConsensusHeartbeat(consensusPayload)
	case "config_change":
		return v.validateConfigChange(consensusPayload)
	default:
		return fmt.Errorf("unknown consensus sub-type: %s", consensusPayload.SubType)
	}
}

// validateRequestVote validates vote request messages
func (v *MessageValidator) validateRequestVote(payload ConsensusPayload) error {
	var voteReq RequestVoteMessage
	if err := payload.UnmarshalData(&voteReq); err != nil {
		return fmt.Errorf("invalid vote request format: %w", err)
	}

	if voteReq.CandidateID == "" {
		return fmt.Errorf("candidate ID is required")
	}

	return nil
}

// validateAppendEntries validates log replication messages
func (v *MessageValidator) validateAppendEntries(payload ConsensusPayload) error {
	var appendReq AppendEntriesMessage
	if err := payload.UnmarshalData(&appendReq); err != nil {
		return fmt.Errorf("invalid append entries format: %w", err)
	}

	if appendReq.LeaderID == "" {
		return fmt.Errorf("leader ID is required")
	}

	return nil
}

// validateConsensusHeartbeat validates consensus heartbeat messages
func (v *MessageValidator) validateConsensusHeartbeat(payload ConsensusPayload) error {
	var heartbeat AppendEntriesMessage
	if err := payload.UnmarshalData(&heartbeat); err != nil {
		return fmt.Errorf("invalid consensus heartbeat format: %w", err)
	}

	if heartbeat.LeaderID == "" {
		return fmt.Errorf("leader ID is required")
	}

	return nil
}

// validateConfigChange validates configuration change messages
func (v *MessageValidator) validateConfigChange(payload ConsensusPayload) error {
	var configChange ConfigChangeMessage
	if err := payload.UnmarshalData(&configChange); err != nil {
		return fmt.Errorf("invalid config change format: %w", err)
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
	var managementPayload ManagementPayload
	if err := msg.UnmarshalPayload(&managementPayload); err != nil {
		return fmt.Errorf("invalid management message format: %w", err)
	}

	if managementPayload.SubType == "" {
		return fmt.Errorf("management sub-type is required")
	}

	switch managementPayload.SubType {
	case "node_discovery":
		return v.validateNodeDiscovery(managementPayload)
	case "connection_management":
		return v.validateConnectionManagement(managementPayload)
	case "topology_update":
		return v.validateTopologyUpdate(managementPayload)
	case "health_status":
		return v.validateHealthStatus(managementPayload)
	default:
		return fmt.Errorf("unknown management sub-type: %s", managementPayload.SubType)
	}
}

// validateNodeDiscovery validates node discovery messages
func (v *MessageValidator) validateNodeDiscovery(payload ManagementPayload) error {
	var discoveryMsg NodeDiscoveryMessage
	if err := payload.UnmarshalData(&discoveryMsg); err != nil {
		return fmt.Errorf("invalid node discovery format: %w", err)
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
func (v *MessageValidator) validateConnectionManagement(payload ManagementPayload) error {
	var connMsg ConnectionManagementMessage
	if err := payload.UnmarshalData(&connMsg); err != nil {
		return fmt.Errorf("invalid connection management format: %w", err)
	}

	if connMsg.PeerID == "" {
		return fmt.Errorf("peer ID is required")
	}
	if connMsg.Type != "status" && connMsg.Address == "" {
		return fmt.Errorf("peer address is required for %s operations", connMsg.Type)
	}

	return nil
}

// validateTopologyUpdate validates network topology update messages
func (v *MessageValidator) validateTopologyUpdate(payload ManagementPayload) error {
	var topologyMsg TopologyUpdateMessage
	if err := payload.UnmarshalData(&topologyMsg); err != nil {
		return fmt.Errorf("invalid topology update format: %w", err)
	}

	if topologyMsg.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}
	if topologyMsg.Action != "remove" && topologyMsg.Address == "" {
		return fmt.Errorf("node address is required for %s operations", topologyMsg.Action)
	}

	return nil
}

// validateHealthStatus validates health status update messages
func (v *MessageValidator) validateHealthStatus(payload ManagementPayload) error {
	var healthMsg HealthStatusMessage
	if err := payload.UnmarshalData(&healthMsg); err != nil {
		return fmt.Errorf("invalid health status format: %w", err)
	}

	if healthMsg.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}
	if healthMsg.Status == "" {
		return fmt.Errorf("health status is required")
	}

	return nil
}

// validateHeartbeatMessage validates heartbeat messages
func (v *MessageValidator) validateHeartbeatMessage(msg *Message) error {
	// Heartbeat messages typically have no payload or minimal payload
	// Just validate that it's properly formed
	return nil
}

// validateDataMessage validates data messages
func (v *MessageValidator) validateDataMessage(msg *Message) error {
	// Data messages should have a payload
	if len(msg.Payload) == 0 {
		return fmt.Errorf("data message payload cannot be empty")
	}
	return nil
}

// validateRouteUpdate validates route update messages
func (v *MessageValidator) validateRouteUpdate(payload RoutingPayload) error {
	// TODO: Implement route update validation based on routing protocol
	return nil
}

// validateRouteRequest validates route request messages
func (v *MessageValidator) validateRouteRequest(payload RoutingPayload) error {
	// TODO: Implement route request validation based on routing protocol
	return nil
}

// validateRouteResponse validates route response messages
func (v *MessageValidator) validateRouteResponse(payload RoutingPayload) error {
	// TODO: Implement route response validation based on routing protocol
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
