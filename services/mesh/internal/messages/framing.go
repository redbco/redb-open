package messages

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"
)

// MessageFramer handles message framing and creation
type MessageFramer struct {
	nodeID   string
	sequence uint64
}

// NewMessageFramer creates a new message framer
func NewMessageFramer(nodeID string) *MessageFramer {
	return &MessageFramer{
		nodeID:   nodeID,
		sequence: 0,
	}
}

// CreateMessage creates a new message with proper framing
func (f *MessageFramer) CreateMessage(msgType, targetID string, priority MessagePriority, ttlSeconds uint32, payload interface{}) (*Message, error) {
	// Generate unique message ID
	msgID, err := f.generateMessageID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate message ID: %w", err)
	}

	// Serialize payload
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Create message header
	header := MessageHeader{
		Version:   MessageVersionV1,
		ID:        msgID,
		Type:      msgType,
		From:      f.nodeID,
		To:        targetID,
		Priority:  priority,
		Timestamp: time.Now().UnixNano(),
		TTL:       ttlSeconds,
		Sequence:  atomic.AddUint64(&f.sequence, 1),
	}

	return &Message{
		Header:  header,
		Payload: payloadBytes,
	}, nil
}

// CreateConsensusMessage creates a consensus message
func (f *MessageFramer) CreateConsensusMessage(subType, targetID string, term uint64, priority MessagePriority, ttlSeconds uint32, data interface{}) (*Message, error) {
	// Serialize the consensus data
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal consensus data: %w", err)
	}

	// Create consensus payload
	payload := ConsensusPayload{
		SubType: subType,
		Term:    term,
		Data:    dataBytes,
	}

	return f.CreateMessage("consensus", targetID, priority, ttlSeconds, payload)
}

// CreateManagementMessage creates a management message
func (f *MessageFramer) CreateManagementMessage(subType, targetID string, priority MessagePriority, ttlSeconds uint32, data interface{}) (*Message, error) {
	// Serialize the management data
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal management data: %w", err)
	}

	// Create management payload
	payload := ManagementPayload{
		SubType: subType,
		Data:    dataBytes,
	}

	return f.CreateMessage("management", targetID, priority, ttlSeconds, payload)
}

// CreateRoutingMessage creates a routing message
func (f *MessageFramer) CreateRoutingMessage(subType, targetID string, priority MessagePriority, ttlSeconds uint32, data interface{}) (*Message, error) {
	// Serialize the routing data
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal routing data: %w", err)
	}

	// Create routing payload
	payload := RoutingPayload{
		SubType: subType,
		Data:    dataBytes,
	}

	return f.CreateMessage("routing", targetID, priority, ttlSeconds, payload)
}

// CreateHeartbeatMessage creates a heartbeat message
func (f *MessageFramer) CreateHeartbeatMessage(targetID string) (*Message, error) {
	return f.CreateMessage("heartbeat", targetID, PriorityNormal, 60, nil)
}

// CreateDataMessage creates a data message
func (f *MessageFramer) CreateDataMessage(targetID string, priority MessagePriority, ttlSeconds uint32, data interface{}) (*Message, error) {
	return f.CreateMessage("data", targetID, priority, ttlSeconds, data)
}

// CreateBroadcastMessage creates a broadcast message (empty targetID)
func (f *MessageFramer) CreateBroadcastMessage(msgType string, priority MessagePriority, ttlSeconds uint32, payload interface{}) (*Message, error) {
	return f.CreateMessage(msgType, "", priority, ttlSeconds, payload)
}

// generateMessageID generates a unique message ID
func (f *MessageFramer) generateMessageID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// ValidateMessage validates a message structure
func ValidateMessage(msg *Message) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}

	// Validate header
	if msg.Header.Version == "" {
		return fmt.Errorf("message version is required")
	}
	if msg.Header.ID == "" {
		return fmt.Errorf("message ID is required")
	}
	if msg.Header.Type == "" {
		return fmt.Errorf("message type is required")
	}
	if msg.Header.From == "" {
		return fmt.Errorf("message sender is required")
	}
	if msg.Header.Priority < PriorityLow || msg.Header.Priority > PriorityUrgent {
		return fmt.Errorf("invalid message priority: %d", msg.Header.Priority)
	}
	if msg.Header.Timestamp <= 0 {
		return fmt.Errorf("message timestamp is required")
	}

	// Check if message is expired
	if msg.IsExpired() {
		return fmt.Errorf("message has expired (age: %.2fs, TTL: %ds)", msg.Age(), msg.Header.TTL)
	}

	return nil
}

// MessageStats contains statistics about message framing
type MessageStats struct {
	TotalCreated    uint64
	TotalValidated  uint64
	TotalExpired    uint64
	TotalInvalid    uint64
	AverageAge      float64
	LastMessageTime time.Time
}

// MessageFrameStats tracks framing statistics
type MessageFrameStats struct {
	stats     MessageStats
	startTime time.Time
}

// NewMessageFrameStats creates a new stats tracker
func NewMessageFrameStats() *MessageFrameStats {
	return &MessageFrameStats{
		startTime: time.Now(),
	}
}

// RecordCreated records a message creation
func (s *MessageFrameStats) RecordCreated() {
	atomic.AddUint64(&s.stats.TotalCreated, 1)
	s.stats.LastMessageTime = time.Now()
}

// RecordValidated records a message validation
func (s *MessageFrameStats) RecordValidated() {
	atomic.AddUint64(&s.stats.TotalValidated, 1)
}

// RecordExpired records an expired message
func (s *MessageFrameStats) RecordExpired() {
	atomic.AddUint64(&s.stats.TotalExpired, 1)
}

// RecordInvalid records an invalid message
func (s *MessageFrameStats) RecordInvalid() {
	atomic.AddUint64(&s.stats.TotalInvalid, 1)
}

// GetStats returns current statistics
func (s *MessageFrameStats) GetStats() MessageStats {
	return MessageStats{
		TotalCreated:    atomic.LoadUint64(&s.stats.TotalCreated),
		TotalValidated:  atomic.LoadUint64(&s.stats.TotalValidated),
		TotalExpired:    atomic.LoadUint64(&s.stats.TotalExpired),
		TotalInvalid:    atomic.LoadUint64(&s.stats.TotalInvalid),
		AverageAge:      s.stats.AverageAge,
		LastMessageTime: s.stats.LastMessageTime,
	}
}