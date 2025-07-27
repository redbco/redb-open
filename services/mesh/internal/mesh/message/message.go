package message

import "time"

// Type represents the type of a network message
type Type string

const (
	HeartbeatMsg   Type = "heartbeat"
	ConsensusMsg   Type = "consensus"
	RouteUpdateMsg Type = "route_update"
	DataMsg        Type = "data"
)

// Message represents a network message
type Message struct {
	Type      Type        `json:"type"`
	From      string      `json:"from"`
	To        string      `json:"to"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}
