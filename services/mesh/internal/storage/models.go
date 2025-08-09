package storage

import (
	"encoding/json"
	"time"
)

// MeshNode represents a node in the mesh network
type MeshNode struct {
	NodeID      string          `db:"node_id" json:"node_id"`
	Name        string          `db:"name" json:"name"`
	Description string          `db:"description" json:"description"`
	PubKey      []byte          `db:"pubkey" json:"pubkey"`
	LastSeen    time.Time       `db:"last_seen" json:"last_seen"`
	Status      string          `db:"status" json:"status"`
	Incarnation int64           `db:"incarnation" json:"incarnation"`
	Meta        json.RawMessage `db:"meta" json:"meta"`
	Platform    string          `db:"platform" json:"platform"`
	Version     string          `db:"version" json:"version"`
	RegionID    *string         `db:"region_id" json:"region_id"`
	IPAddress   *string         `db:"ip_address" json:"ip_address"`
	Port        *int            `db:"port" json:"port"`
	Created     time.Time       `db:"created" json:"created"`
	Updated     time.Time       `db:"updated" json:"updated"`
}

// MeshLink represents a link between two nodes in the mesh
type MeshLink struct {
	ID            string          `db:"id" json:"id"`
	ANode         string          `db:"a_node" json:"a_node"`
	BNode         string          `db:"b_node" json:"b_node"`
	LatencyMs     int             `db:"latency_ms" json:"latency_ms"`
	BandwidthMbps int             `db:"bandwidth_mbps" json:"bandwidth_mbps"`
	Loss          float64         `db:"loss" json:"loss"`
	Utilization   float64         `db:"utilization" json:"utilization"`
	Status        string          `db:"status" json:"status"`
	Meta          json.RawMessage `db:"meta" json:"meta"`
	Created       time.Time       `db:"created" json:"created"`
	Updated       time.Time       `db:"updated" json:"updated"`
}

// MeshLSAVersion represents a link-state advertisement version for a node
type MeshLSAVersion struct {
	NodeID  string    `db:"node_id" json:"node_id"`
	Version int64     `db:"version" json:"version"`
	Hash    string    `db:"hash" json:"hash"`
	Created time.Time `db:"created" json:"created"`
}

// MeshRaftGroup represents a Raft consensus group (MCG or DSG)
type MeshRaftGroup struct {
	ID       string          `db:"id" json:"id"`
	Type     string          `db:"type" json:"type"`
	Members  []string        `db:"members" json:"members"`
	Term     int64           `db:"term" json:"term"`
	LeaderID *string         `db:"leader_id" json:"leader_id"`
	Meta     json.RawMessage `db:"meta" json:"meta"`
	Created  time.Time       `db:"created" json:"created"`
	Updated  time.Time       `db:"updated" json:"updated"`
}

// MeshRaftLog represents a Raft log entry
type MeshRaftLog struct {
	GroupID  string    `db:"group_id" json:"group_id"`
	LogIndex int64     `db:"log_index" json:"log_index"`
	Term     int64     `db:"term" json:"term"`
	Payload  []byte    `db:"payload" json:"payload"`
	Created  time.Time `db:"created" json:"created"`
}

// MeshStream represents a data stream in the mesh
type MeshStream struct {
	ID       string          `db:"id" json:"id"`
	TenantID string          `db:"tenant_id" json:"tenant_id"`
	SrcNode  string          `db:"src_node" json:"src_node"`
	DstNodes []string        `db:"dst_nodes" json:"dst_nodes"`
	QoS      string          `db:"qos" json:"qos"`
	Priority int             `db:"priority" json:"priority"`
	Meta     json.RawMessage `db:"meta" json:"meta"`
	Created  time.Time       `db:"created" json:"created"`
	Updated  time.Time       `db:"updated" json:"updated"`
}

// MeshStreamOffset represents the committed sequence number for a stream at a node
type MeshStreamOffset struct {
	StreamID     string    `db:"stream_id" json:"stream_id"`
	NodeID       string    `db:"node_id" json:"node_id"`
	CommittedSeq int64     `db:"committed_seq" json:"committed_seq"`
	Updated      time.Time `db:"updated" json:"updated"`
}

// MeshDeliveryLog represents the delivery status of a message
type MeshDeliveryLog struct {
	StreamID  string    `db:"stream_id" json:"stream_id"`
	MessageID string    `db:"message_id" json:"message_id"`
	SrcNode   string    `db:"src_node" json:"src_node"`
	DstNode   string    `db:"dst_node" json:"dst_node"`
	State     string    `db:"state" json:"state"`
	Err       string    `db:"err" json:"err"`
	Updated   time.Time `db:"updated" json:"updated"`
}

// MeshOutbox represents an outbound message in the outbox pattern
type MeshOutbox struct {
	StreamID    string          `db:"stream_id" json:"stream_id"`
	MessageID   string          `db:"message_id" json:"message_id"`
	Payload     []byte          `db:"payload" json:"payload"`
	Headers     json.RawMessage `db:"headers" json:"headers"`
	NextAttempt time.Time       `db:"next_attempt" json:"next_attempt"`
	Attempts    int             `db:"attempts" json:"attempts"`
	Status      string          `db:"status" json:"status"`
	Created     time.Time       `db:"created" json:"created"`
	Updated     time.Time       `db:"updated" json:"updated"`
}

// MeshInbox represents an inbound message in the inbox pattern
type MeshInbox struct {
	StreamID  string          `db:"stream_id" json:"stream_id"`
	MessageID string          `db:"message_id" json:"message_id"`
	Payload   []byte          `db:"payload" json:"payload"`
	Headers   json.RawMessage `db:"headers" json:"headers"`
	Received  time.Time       `db:"received" json:"received"`
	Processed *time.Time      `db:"processed" json:"processed"`
}

// MeshTopologySnapshot represents a snapshot of the mesh topology
type MeshTopologySnapshot struct {
	Version int64           `db:"version" json:"version"`
	Graph   json.RawMessage `db:"graph" json:"graph"`
	Created time.Time       `db:"created" json:"created"`
}

// MeshConfigKV represents a key-value configuration item
type MeshConfigKV struct {
	Key     string          `db:"key" json:"key"`
	Value   json.RawMessage `db:"value" json:"value"`
	Updated time.Time       `db:"updated" json:"updated"`
}

// Constants for status values
const (
	// Node statuses
	NodeStatusActive     = "STATUS_ACTIVE"
	NodeStatusInactive   = "STATUS_INACTIVE"
	NodeStatusSuspicious = "STATUS_SUSPICIOUS"
	NodeStatusFailed     = "STATUS_FAILED"

	// Link statuses
	LinkStatusActive   = "STATUS_ACTIVE"
	LinkStatusInactive = "STATUS_INACTIVE"
	LinkStatusFailed   = "STATUS_FAILED"

	// Raft group types
	RaftGroupTypeMCG = "MCG" // Mesh Control Group
	RaftGroupTypeDSG = "DSG" // Data Stream Group

	// Stream QoS levels
	StreamQoSCritical = "QOS_CRITICAL"
	StreamQoSHigh     = "QOS_HIGH"
	StreamQoSNormal   = "QOS_NORMAL"
	StreamQoSLow      = "QOS_LOW"

	// Delivery states
	DeliveryStateReceived   = "received"
	DeliveryStateProcessing = "processing"
	DeliveryStateDone       = "done"
	DeliveryStateFailed     = "failed"

	// Outbox statuses
	OutboxStatusPending   = "pending"
	OutboxStatusSent      = "sent"
	OutboxStatusFailed    = "failed"
	OutboxStatusCancelled = "cancelled"
)

// IsValidNodeStatus checks if a node status is valid
func IsValidNodeStatus(status string) bool {
	switch status {
	case NodeStatusActive, NodeStatusInactive, NodeStatusSuspicious, NodeStatusFailed:
		return true
	default:
		return false
	}
}

// IsValidLinkStatus checks if a link status is valid
func IsValidLinkStatus(status string) bool {
	switch status {
	case LinkStatusActive, LinkStatusInactive, LinkStatusFailed:
		return true
	default:
		return false
	}
}

// IsValidRaftGroupType checks if a Raft group type is valid
func IsValidRaftGroupType(groupType string) bool {
	switch groupType {
	case RaftGroupTypeMCG, RaftGroupTypeDSG:
		return true
	default:
		return false
	}
}

// IsValidStreamQoS checks if a stream QoS level is valid
func IsValidStreamQoS(qos string) bool {
	switch qos {
	case StreamQoSCritical, StreamQoSHigh, StreamQoSNormal, StreamQoSLow:
		return true
	default:
		return false
	}
}

// IsValidDeliveryState checks if a delivery state is valid
func IsValidDeliveryState(state string) bool {
	switch state {
	case DeliveryStateReceived, DeliveryStateProcessing, DeliveryStateDone, DeliveryStateFailed:
		return true
	default:
		return false
	}
}

// IsValidOutboxStatus checks if an outbox status is valid
func IsValidOutboxStatus(status string) bool {
	switch status {
	case OutboxStatusPending, OutboxStatusSent, OutboxStatusFailed, OutboxStatusCancelled:
		return true
	default:
		return false
	}
}
