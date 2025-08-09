package ws

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redbco/redb-open/pkg/logger"
)

// LaneClass represents the class/priority of a lane
type LaneClass int

const (
	LaneClassControl  LaneClass = iota // Lane 0: Control traffic (gossip, Raft, acks)
	LaneClassPriority                  // Lane 1: Priority data (system updates, DB updates)
	LaneClassBulk                      // Lane 2+: Bulk data (client data replication)
)

// Lane represents a single WebSocket connection within a VirtualLink
type Lane struct {
	ID        int
	Class     LaneClass
	Priority  int
	Weight    float64
	Conn      *websocket.Conn
	Status    LaneStatus
	Stats     LaneStats
	SendQueue chan *Frame
	RecvQueue chan *Frame
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.RWMutex
	logger    *logger.Logger
}

// LaneStatus represents the current status of a lane
type LaneStatus int

const (
	LaneStatusConnecting LaneStatus = iota
	LaneStatusConnected
	LaneStatusDegraded
	LaneStatusFailed
	LaneStatusClosed
)

// LaneStats tracks performance metrics for a lane
type LaneStats struct {
	MessagesSent     int64
	MessagesReceived int64
	BytesSent        int64
	BytesReceived    int64
	Latency          time.Duration
	LastHeartbeat    time.Time
	LastActivity     time.Time
	SendQueueDepth   int
	RecvQueueDepth   int
}

// VirtualLink represents a logical connection between two nodes
// managing multiple physical WebSocket connections (lanes)
type VirtualLink struct {
	ID           string
	LocalNodeID  string
	RemoteNodeID string
	Lanes        map[int]*Lane
	Config       VirtualLinkConfig
	Status       VirtualLinkStatus
	mu           sync.RWMutex
	logger       *logger.Logger
	ctx          context.Context
	cancel       context.CancelFunc
}

// VirtualLinkConfig holds configuration for a VirtualLink
type VirtualLinkConfig struct {
	MaxLanes           int           // Maximum number of lanes (default: 4)
	CollapsibleMode    bool          // Whether to collapse to single lane for testing
	HeartbeatInterval  time.Duration // Heartbeat interval for health monitoring
	QueueBufferSize    int           // Buffer size for send/recv queues
	AdaptiveScaling    bool          // Whether to enable adaptive lane scaling
	ScaleUpThreshold   time.Duration // Time threshold for scaling up
	ScaleDownThreshold time.Duration // Time threshold for scaling down
	CooldownPeriod     time.Duration // Cooldown between scaling operations
}

// VirtualLinkStatus represents the overall status of a VirtualLink
type VirtualLinkStatus int

const (
	VirtualLinkStatusConnecting VirtualLinkStatus = iota
	VirtualLinkStatusConnected
	VirtualLinkStatusDegraded
	VirtualLinkStatusFailed
	VirtualLinkStatusClosed
)

// NewVirtualLink creates a new VirtualLink instance
func NewVirtualLink(id, localNodeID, remoteNodeID string, config VirtualLinkConfig, logger *logger.Logger) *VirtualLink {
	if config.MaxLanes == 0 {
		config.MaxLanes = 4
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 30 * time.Second
	}
	if config.QueueBufferSize == 0 {
		config.QueueBufferSize = 1000
	}
	if config.ScaleUpThreshold == 0 {
		config.ScaleUpThreshold = 10 * time.Second
	}
	if config.ScaleDownThreshold == 0 {
		config.ScaleDownThreshold = 60 * time.Second
	}
	if config.CooldownPeriod == 0 {
		config.CooldownPeriod = 30 * time.Second
	}

	vl := &VirtualLink{
		ID:           id,
		LocalNodeID:  localNodeID,
		RemoteNodeID: remoteNodeID,
		Lanes:        make(map[int]*Lane),
		Config:       config,
		Status:       VirtualLinkStatusConnecting,
		logger:       logger,
	}

	vl.ctx, vl.cancel = context.WithCancel(context.Background())

	// Initialize baseline lanes
	vl.initializeBaselineLanes()

	return vl
}

// initializeBaselineLanes creates the initial set of lanes
func (vl *VirtualLink) initializeBaselineLanes() {
	// Lane 0: Control (highest priority)
	vl.createLane(0, LaneClassControl, 100, 1.0)

	// Lane 1: Priority data
	vl.createLane(1, LaneClassPriority, 80, 0.8)

	// Lane 2: Bulk data (if not in collapsible mode)
	if !vl.Config.CollapsibleMode {
		vl.createLane(2, LaneClassBulk, 60, 0.6)
	}
}

// createLane creates a new lane with the specified parameters
func (vl *VirtualLink) createLane(id int, class LaneClass, priority int, weight float64) {
	lane := &Lane{
		ID:        id,
		Class:     class,
		Priority:  priority,
		Weight:    weight,
		Status:    LaneStatusConnecting,
		SendQueue: make(chan *Frame, vl.Config.QueueBufferSize),
		RecvQueue: make(chan *Frame, vl.Config.QueueBufferSize),
		logger:    vl.logger,
	}

	lane.ctx, lane.cancel = context.WithCancel(vl.ctx)

	vl.mu.Lock()
	vl.Lanes[id] = lane
	vl.mu.Unlock()

	// Start lane goroutines
	go lane.sendLoop()
	go lane.recvLoop()
	go lane.healthLoop()
}

// OpenStream creates a new stream on the appropriate lane
func (vl *VirtualLink) OpenStream(params StreamParams) (*Stream, error) {
	vl.mu.RLock()
	defer vl.mu.RUnlock()

	// Select appropriate lane based on stream parameters
	laneID := vl.selectLaneForStream(params)
	_, exists := vl.Lanes[laneID]
	if !exists {
		return nil, fmt.Errorf("lane %d not found", laneID)
	}

	stream := &Stream{
		ID:          params.StreamID,
		LaneID:      laneID,
		Class:       params.Class,
		Priority:    params.Priority,
		VirtualLink: vl,
		Status:      StreamStatusOpening,
	}

	return stream, nil
}

// StreamParams defines parameters for opening a stream
type StreamParams struct {
	StreamID string
	Class    LaneClass
	Priority int
	Size     int64 // Estimated size of the stream
}

// selectLaneForStream selects the appropriate lane for a stream
func (vl *VirtualLink) selectLaneForStream(params StreamParams) int {
	// Control streams always go to control lane
	if params.Class == LaneClassControl {
		return 0
	}

	// Priority streams go to priority lane
	if params.Class == LaneClassPriority {
		return 1
	}

	// Bulk streams can be distributed across multiple bulk lanes
	if params.Class == LaneClassBulk {
		// For now, use lane 2 for bulk traffic
		// TODO: Implement load balancing across multiple bulk lanes
		return 2
	}

	// Default to priority lane
	return 1
}

// Send sends a frame to the appropriate lane
func (vl *VirtualLink) Send(frame *Frame, laneID int) error {
	vl.mu.RLock()
	lane, exists := vl.Lanes[laneID]
	vl.mu.RUnlock()

	if !exists {
		return fmt.Errorf("lane %d not found", laneID)
	}

	// Check if lane can accept the frame
	if !lane.canAcceptFrame(frame) {
		return fmt.Errorf("lane %d cannot accept frame", laneID)
	}

	// Send frame to lane's send queue
	select {
	case lane.SendQueue <- frame:
		return nil
	case <-vl.ctx.Done():
		return fmt.Errorf("virtual link closed")
	default:
		return fmt.Errorf("lane %d send queue full", laneID)
	}
}

// canAcceptFrame checks if a lane can accept a frame
func (l *Lane) canAcceptFrame(frame *Frame) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Control lane has strict size limits
	if l.Class == LaneClassControl && frame.Size() > 1024 {
		return false
	}

	// Check queue depth
	if len(l.SendQueue) >= cap(l.SendQueue) {
		return false
	}

	return true
}

// Stats returns statistics for all lanes
func (vl *VirtualLink) Stats() map[int]LaneStats {
	vl.mu.RLock()
	defer vl.mu.RUnlock()

	stats := make(map[int]LaneStats)
	for id, lane := range vl.Lanes {
		lane.mu.RLock()
		stats[id] = lane.Stats
		lane.mu.RUnlock()
	}

	return stats
}

// Backpressure returns the current backpressure state for all lanes
func (vl *VirtualLink) Backpressure() map[int]BackpressureState {
	vl.mu.RLock()
	defer vl.mu.RUnlock()

	backpressure := make(map[int]BackpressureState)
	for id, lane := range vl.Lanes {
		lane.mu.RLock()
		backpressure[id] = BackpressureState{
			QueueDepth: len(lane.SendQueue),
			QueueCap:   cap(lane.SendQueue),
			Status:     lane.Status,
		}
		lane.mu.RUnlock()
	}

	return backpressure
}

// BackpressureState represents the backpressure state of a lane
type BackpressureState struct {
	QueueDepth int
	QueueCap   int
	Status     LaneStatus
}

// Close closes the VirtualLink and all its lanes
func (vl *VirtualLink) Close() error {
	vl.mu.Lock()
	defer vl.mu.Unlock()

	vl.Status = VirtualLinkStatusClosed
	vl.cancel()

	// Close all lanes
	for _, lane := range vl.Lanes {
		lane.Close()
	}

	return nil
}

// sendLoop handles sending frames from the lane's send queue
func (l *Lane) sendLoop() {
	for {
		select {
		case frame := <-l.SendQueue:
			if err := l.sendFrame(frame); err != nil {
				l.logger.Error("Failed to send frame", "lane_id", l.ID, "error", err)
				l.updateStatus(LaneStatusDegraded)
			} else {
				l.updateStats(frame, true)
			}
		case <-l.ctx.Done():
			return
		}
	}
}

// recvLoop handles receiving frames from the WebSocket connection
func (l *Lane) recvLoop() {
	for {
		select {
		case <-l.ctx.Done():
			return
		default:
			// Read frame from WebSocket
			frame, err := l.readFrame()
			if err != nil {
				l.logger.Error("Failed to read frame", "lane_id", l.ID, "error", err)
				l.updateStatus(LaneStatusFailed)
				return
			}

			// Update stats
			l.updateStats(frame, false)

			// Send to receive queue
			select {
			case l.RecvQueue <- frame:
			case <-l.ctx.Done():
				return
			default:
				l.logger.Warn("Receive queue full, dropping frame", "lane_id", l.ID)
			}
		}
	}
}

// healthLoop monitors lane health and sends heartbeats
func (l *Lane) healthLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := l.sendHeartbeat(); err != nil {
				l.logger.Error("Failed to send heartbeat", "lane_id", l.ID, "error", err)
				l.updateStatus(LaneStatusDegraded)
			} else {
				l.updateStatus(LaneStatusConnected)
			}
		case <-l.ctx.Done():
			return
		}
	}
}

// sendFrame sends a frame over the WebSocket connection
func (l *Lane) sendFrame(frame *Frame) error {
	if l.Conn == nil {
		return fmt.Errorf("WebSocket connection not established")
	}

	// Serialize frame
	data, err := frame.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize frame: %w", err)
	}

	// Send over WebSocket
	return l.Conn.WriteMessage(websocket.BinaryMessage, data)
}

// readFrame reads a frame from the WebSocket connection
func (l *Lane) readFrame() (*Frame, error) {
	if l.Conn == nil {
		return nil, fmt.Errorf("WebSocket connection not established")
	}

	// Read message from WebSocket
	_, data, err := l.Conn.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("failed to read WebSocket message: %w", err)
	}

	// Deserialize frame
	frame, err := DeserializeFrame(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize frame: %w", err)
	}

	return frame, nil
}

// sendHeartbeat sends a heartbeat frame
func (l *Lane) sendHeartbeat() error {
	heartbeat := &Frame{
		Type:      FrameTypeHeartbeat,
		LaneID:    l.ID,
		Timestamp: time.Now().UnixNano(),
	}

	return l.sendFrame(heartbeat)
}

// updateStatus updates the lane status
func (l *Lane) updateStatus(status LaneStatus) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.Status != status {
		l.Status = status
		l.Stats.LastActivity = time.Now()
	}
}

// updateStats updates lane statistics
func (l *Lane) updateStats(frame *Frame, sent bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if sent {
		l.Stats.MessagesSent++
		l.Stats.BytesSent += int64(len(frame.Payload))
	} else {
		l.Stats.MessagesReceived++
		l.Stats.BytesReceived += int64(len(frame.Payload))
	}

	l.Stats.LastActivity = time.Now()
	l.Stats.SendQueueDepth = len(l.SendQueue)
	l.Stats.RecvQueueDepth = len(l.RecvQueue)
}

// Close closes the lane
func (l *Lane) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.Status = LaneStatusClosed
	l.cancel()

	if l.Conn != nil {
		l.Conn.Close()
	}
}

// Stream represents a logical stream within a VirtualLink
type Stream struct {
	ID          string
	LaneID      int
	Class       LaneClass
	Priority    int
	VirtualLink *VirtualLink
	Status      StreamStatus
	mu          sync.RWMutex
}

// StreamStatus represents the status of a stream
type StreamStatus int

const (
	StreamStatusOpening StreamStatus = iota
	StreamStatusOpen
	StreamStatusClosed
	StreamStatusFailed
)

// Send sends data over the stream
func (s *Stream) Send(data []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.Status != StreamStatusOpen {
		return fmt.Errorf("stream not open")
	}

	frame := &Frame{
		Type:      FrameTypeData,
		LaneID:    s.LaneID,
		StreamID:  s.ID,
		Payload:   data,
		Timestamp: time.Now().UnixNano(),
	}

	return s.VirtualLink.Send(frame, s.LaneID)
}

// Close closes the stream
func (s *Stream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Status = StreamStatusClosed
	return nil
}
