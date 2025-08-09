package ws

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"
)

// FrameType represents the type of frame
type FrameType int

const (
	FrameTypeData FrameType = iota
	FrameTypeControl
	FrameTypeHeartbeat
	FrameTypeAck
	FrameTypeWindowUpdate
	FrameTypeRst
)

// Frame represents a message frame with lane_id
type Frame struct {
	Type        FrameType         `json:"type"`
	LaneID      int               `json:"lane_id"`
	StreamID    string            `json:"stream_id,omitempty"`
	Seq         int64             `json:"seq,omitempty"`
	ChunkSeq    int32             `json:"chunk_seq,omitempty"`
	TotalChunks int32             `json:"total_chunks,omitempty"`
	TenantID    string            `json:"tenant_id,omitempty"`
	Payload     []byte            `json:"payload,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Timestamp   int64             `json:"timestamp"`
	Checksum    []byte            `json:"checksum,omitempty"`
	PathID      string            `json:"path_id,omitempty"`
	HopCount    int32             `json:"hop_count,omitempty"`
	TTL         int32             `json:"ttl,omitempty"`
	Priority    int32             `json:"priority,omitempty"`
}

// Size returns the size of the frame payload
func (f *Frame) Size() int {
	return len(f.Payload)
}

// Serialize serializes the frame to bytes
func (f *Frame) Serialize() ([]byte, error) {
	// For now, use JSON serialization
	// TODO: Implement more efficient binary serialization
	return json.Marshal(f)
}

// DeserializeFrame deserializes bytes to a frame
func DeserializeFrame(data []byte) (*Frame, error) {
	var frame Frame
	if err := json.Unmarshal(data, &frame); err != nil {
		return nil, fmt.Errorf("failed to unmarshal frame: %w", err)
	}
	return &frame, nil
}

// NewDataFrame creates a new data frame
func NewDataFrame(laneID int, streamID string, seq int64, payload []byte) *Frame {
	return &Frame{
		Type:      FrameTypeData,
		LaneID:    laneID,
		StreamID:  streamID,
		Seq:       seq,
		Payload:   payload,
		Timestamp: time.Now().UnixNano(),
	}
}

// NewControlFrame creates a new control frame
func NewControlFrame(laneID int, streamID string, payload []byte) *Frame {
	return &Frame{
		Type:      FrameTypeControl,
		LaneID:    laneID,
		StreamID:  streamID,
		Payload:   payload,
		Timestamp: time.Now().UnixNano(),
	}
}

// NewHeartbeatFrame creates a new heartbeat frame
func NewHeartbeatFrame(laneID int) *Frame {
	return &Frame{
		Type:      FrameTypeHeartbeat,
		LaneID:    laneID,
		Timestamp: time.Now().UnixNano(),
	}
}

// NewAckFrame creates a new acknowledgment frame
func NewAckFrame(laneID int, streamID string, seq int64) *Frame {
	return &Frame{
		Type:      FrameTypeAck,
		LaneID:    laneID,
		StreamID:  streamID,
		Seq:       seq,
		Timestamp: time.Now().UnixNano(),
	}
}

// NewWindowUpdateFrame creates a new window update frame
func NewWindowUpdateFrame(laneID int, streamID string, windowSize int32) *Frame {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(windowSize))

	return &Frame{
		Type:      FrameTypeWindowUpdate,
		LaneID:    laneID,
		StreamID:  streamID,
		Payload:   payload,
		Timestamp: time.Now().UnixNano(),
	}
}

// NewRstFrame creates a new reset frame
func NewRstFrame(laneID int, streamID string, reason string) *Frame {
	return &Frame{
		Type:      FrameTypeRst,
		LaneID:    laneID,
		StreamID:  streamID,
		Payload:   []byte(reason),
		Timestamp: time.Now().UnixNano(),
	}
}

// IsControl returns true if this is a control frame
func (f *Frame) IsControl() bool {
	return f.Type == FrameTypeControl || f.Type == FrameTypeHeartbeat ||
		f.Type == FrameTypeAck || f.Type == FrameTypeWindowUpdate ||
		f.Type == FrameTypeRst
}

// IsData returns true if this is a data frame
func (f *Frame) IsData() bool {
	return f.Type == FrameTypeData
}

// IsHeartbeat returns true if this is a heartbeat frame
func (f *Frame) IsHeartbeat() bool {
	return f.Type == FrameTypeHeartbeat
}

// GetWindowSize extracts window size from a window update frame
func (f *Frame) GetWindowSize() (int32, error) {
	if f.Type != FrameTypeWindowUpdate {
		return 0, fmt.Errorf("frame is not a window update frame")
	}

	if len(f.Payload) != 4 {
		return 0, fmt.Errorf("invalid window update payload size")
	}

	return int32(binary.BigEndian.Uint32(f.Payload)), nil
}

// GetRstReason extracts reset reason from a reset frame
func (f *Frame) GetRstReason() (string, error) {
	if f.Type != FrameTypeRst {
		return "", fmt.Errorf("frame is not a reset frame")
	}

	return string(f.Payload), nil
}
