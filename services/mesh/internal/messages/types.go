package messages

import (
	"encoding/json"
)

// Message represents a mesh network message
type Message struct {
	Type    string          `json:"type"`
	From    string          `json:"from"`
	To      string          `json:"to"`
	Content json.RawMessage `json:"content"`
}

// UnmarshalContent unmarshals the message content into the provided value
func (m *Message) UnmarshalContent(v interface{}) error {
	return json.Unmarshal(m.Content, v)
}

// ConsensusMessage represents a consensus-related message
type ConsensusMessage struct {
	Type    string          `json:"type"`
	Term    uint64          `json:"term"`
	From    string          `json:"from"`
	To      string          `json:"to"`
	Content json.RawMessage `json:"content"`
}

// UnmarshalContent unmarshals the message content into the provided value
func (m *ConsensusMessage) UnmarshalContent(v interface{}) error {
	return json.Unmarshal(m.Content, v)
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

// ManagementMessage represents a management-related message
type ManagementMessage struct {
	Type    string          `json:"type"`
	From    string          `json:"from"`
	To      string          `json:"to"`
	Content json.RawMessage `json:"content"`
}

// UnmarshalContent unmarshals the message content into the provided value
func (m *ManagementMessage) UnmarshalContent(v interface{}) error {
	return json.Unmarshal(m.Content, v)
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

// TopologyUpdateMessage represents a network topology update message
type TopologyUpdateMessage struct {
	Type      string            `json:"type"` // "add", "remove", "update"
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
