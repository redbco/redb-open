package messages

import (
	"encoding/json"
	"time"
)

// MessageVersion defines the protocol version
const (
	MessageVersionV1 = "1.0"
)

// MessagePriority defines message priority levels
type MessagePriority uint8

const (
	PriorityLow    MessagePriority = 1
	PriorityNormal MessagePriority = 2
	PriorityHigh   MessagePriority = 3
	PriorityUrgent MessagePriority = 4
)

// MessageHeader contains common message metadata
type MessageHeader struct {
	Version   string          `json:"version"`   // Protocol version
	ID        string          `json:"id"`        // Unique message ID
	Type      string          `json:"type"`      // Message type
	From      string          `json:"from"`      // Sender node ID
	To        string          `json:"to"`        // Target node ID (empty for broadcast)
	Priority  MessagePriority `json:"priority"`  // Message priority
	Timestamp int64           `json:"timestamp"` // Unix timestamp in nanoseconds
	TTL       uint32          `json:"ttl"`       // Time to live in seconds
	Sequence  uint64          `json:"sequence"`  // Message sequence number
}

// Message represents a mesh network message with unified framing
type Message struct {
	Header  MessageHeader   `json:"header"`
	Payload json.RawMessage `json:"payload"`
}

// UnmarshalPayload unmarshals the message payload into the provided value
func (m *Message) UnmarshalPayload(v interface{}) error {
	return json.Unmarshal(m.Payload, v)
}

// IsExpired checks if the message has exceeded its TTL
func (m *Message) IsExpired() bool {
	if m.Header.TTL == 0 {
		return false // No expiration
	}
	return time.Now().Unix() > time.Unix(0, m.Header.Timestamp).Unix()+int64(m.Header.TTL)
}

// Age returns the age of the message in seconds
func (m *Message) Age() float64 {
	return time.Since(time.Unix(0, m.Header.Timestamp)).Seconds()
}

// ConsensusPayload represents the payload for consensus-related messages
type ConsensusPayload struct {
	SubType string          `json:"sub_type"` // request_vote, append_entries, heartbeat, config_change
	Term    uint64          `json:"term"`
	Data    json.RawMessage `json:"data"`
}

// UnmarshalData unmarshals the consensus data into the provided value
func (c *ConsensusPayload) UnmarshalData(v interface{}) error {
	return json.Unmarshal(c.Data, v)
}

// RequestVoteMessage represents a vote request message
type RequestVoteMessage struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// AppendEntriesMessage represents a log replication message
type AppendEntriesMessage struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leader_commit"`
}

// LogEntry represents a single log entry
type LogEntry struct {
	Term    uint64          `json:"term"`
	Index   uint64          `json:"index"`
	Command json.RawMessage `json:"command"`
}

// ConfigChangeMessage represents a configuration change message
type ConfigChangeMessage struct {
	Term    uint64 `json:"term"`
	Type    string `json:"type"` // "add" or "remove"
	NodeID  string `json:"node_id"`
	Address string `json:"address,omitempty"`
}

// ManagementPayload represents the payload for management-related messages
type ManagementPayload struct {
	SubType string          `json:"sub_type"` // node_discovery, connection_management, topology_update, health_status
	Data    json.RawMessage `json:"data"`
}

// UnmarshalData unmarshals the management data into the provided value
func (m *ManagementPayload) UnmarshalData(v interface{}) error {
	return json.Unmarshal(m.Data, v)
}

// NodeDiscoveryMessage represents a node discovery message
type NodeDiscoveryMessage struct {
	Type         string            `json:"type"` // "announce" or "request"
	NodeID       string            `json:"node_id"`
	MeshID       string            `json:"mesh_id"`
	Address      string            `json:"address"`
	Capabilities []string          `json:"capabilities"`
	Metadata     map[string]string `json:"metadata"`
}

// ConnectionManagementMessage represents a connection management message
type ConnectionManagementMessage struct {
	Type    string `json:"type"` // "connect", "disconnect", "status"
	PeerID  string `json:"peer_id"`
	Address string `json:"address,omitempty"`
	Status  string `json:"status,omitempty"`
}

// RoutingPayload represents the payload for routing-related messages
type RoutingPayload struct {
	SubType string          `json:"sub_type"` // route_update, route_request, route_response
	Data    json.RawMessage `json:"data"`
}

// UnmarshalData unmarshals the routing data into the provided value
func (r *RoutingPayload) UnmarshalData(v interface{}) error {
	return json.Unmarshal(r.Data, v)
}

// TopologyUpdateMessage represents a network topology update message
type TopologyUpdateMessage struct {
	Action    string            `json:"action"` // "add", "remove", "update"
	NodeID    string            `json:"node_id"`
	Address   string            `json:"address"`
	Neighbors []string          `json:"neighbors"`
	Metadata  map[string]string `json:"metadata"`
}

// HealthStatusMessage represents a health status update message
type HealthStatusMessage struct {
	NodeID    string             `json:"node_id"`
	Status    string             `json:"status"` // "healthy", "degraded", "unhealthy"
	Metrics   map[string]float64 `json:"metrics"`
	Timestamp int64              `json:"timestamp"`
}
