package ws

import (
	"fmt"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/transport"
)

// WebSocketTransportFactory creates WebSocket transport instances
type WebSocketTransportFactory struct {
	logger *logger.Logger
}

// NewWebSocketTransportFactory creates a new WebSocket transport factory
func NewWebSocketTransportFactory(logger *logger.Logger) *WebSocketTransportFactory {
	return &WebSocketTransportFactory{
		logger: logger,
	}
}

// Create creates a new WebSocket transport instance
func (f *WebSocketTransportFactory) Create(config transport.TransportConfig) (transport.Transport, error) {
	wsConfig := TransportConfig{
		ListenAddr:        config.ListenAddr,
		ReadBufferSize:    config.ReadBufferSize,
		WriteBufferSize:   config.WriteBufferSize,
		MaxMessageSize:    config.MaxMessageSize,
		HandshakeTimeout:  config.HandshakeTimeout,
		WriteTimeout:      config.WriteTimeout,
		PongWait:          config.PongWait,
		PingPeriod:        config.PingPeriod,
		MaxConnections:    config.MaxConnections,
		EnableCompression: config.EnableCompression,
	}

	tm := NewTransportManager(wsConfig, f.logger)
	return NewWebSocketTransportAdapter(tm), nil
}

// WebSocketTransportAdapter adapts the WebSocket transport to the transport interface
type WebSocketTransportAdapter struct {
	*TransportManager
}

// Ensure WebSocketTransportAdapter implements transport.Transport
var _ transport.Transport = (*WebSocketTransportAdapter)(nil)

// NewWebSocketTransportAdapter creates a new WebSocket transport adapter
func NewWebSocketTransportAdapter(tm *TransportManager) *WebSocketTransportAdapter {
	return &WebSocketTransportAdapter{
		TransportManager: tm,
	}
}

// Connect adapts the Connect method to return the transport.Link interface
func (a *WebSocketTransportAdapter) Connect(remoteAddr, nodeID, linkID string) (transport.Link, error) {
	link, err := a.TransportManager.Connect(remoteAddr, nodeID, linkID)
	if err != nil {
		return nil, err
	}
	return &WebSocketLinkAdapter{link}, nil
}

// GetLink adapts the GetLink method to return the transport.Link interface
func (a *WebSocketTransportAdapter) GetLink(linkID string) (transport.Link, bool) {
	link, exists := a.TransportManager.GetLink(linkID)
	if !exists {
		return nil, false
	}
	return &WebSocketLinkAdapter{link}, exists
}

// ListLinks adapts the ListLinks method to return the transport.Link interface
func (a *WebSocketTransportAdapter) ListLinks() []transport.Link {
	links := a.TransportManager.ListLinks()
	adaptedLinks := make([]transport.Link, len(links))
	for i, link := range links {
		adaptedLinks[i] = &WebSocketLinkAdapter{link}
	}
	return adaptedLinks
}

// Stats returns transport statistics
func (a *WebSocketTransportAdapter) Stats() map[string]map[int]transport.LaneStats {
	wsStats := a.TransportManager.Stats()
	stats := make(map[string]map[int]transport.LaneStats)

	for linkID, wsLinkStats := range wsStats {
		stats[linkID] = make(map[int]transport.LaneStats)
		for laneID, wsLaneStats := range wsLinkStats {
			stats[linkID][laneID] = transport.LaneStats{
				MessagesSent:     wsLaneStats.MessagesSent,
				MessagesReceived: wsLaneStats.MessagesReceived,
				BytesSent:        wsLaneStats.BytesSent,
				BytesReceived:    wsLaneStats.BytesReceived,
				Latency:          wsLaneStats.Latency,
				LastHeartbeat:    wsLaneStats.LastHeartbeat,
				LastActivity:     wsLaneStats.LastActivity,
				SendQueueDepth:   wsLaneStats.SendQueueDepth,
				RecvQueueDepth:   wsLaneStats.RecvQueueDepth,
			}
		}
	}

	return stats
}

// WebSocketLinkAdapter adapts the VirtualLink to the transport.Link interface
type WebSocketLinkAdapter struct {
	*VirtualLink
}

// Ensure WebSocketLinkAdapter implements transport.Link
var _ transport.Link = (*WebSocketLinkAdapter)(nil)

// ID returns the link ID
func (a *WebSocketLinkAdapter) ID() string {
	return a.VirtualLink.ID
}

// LocalNodeID returns the local node ID
func (a *WebSocketLinkAdapter) LocalNodeID() string {
	return a.VirtualLink.LocalNodeID
}

// RemoteNodeID returns the remote node ID
func (a *WebSocketLinkAdapter) RemoteNodeID() string {
	return a.VirtualLink.RemoteNodeID
}

// Status returns the link status
func (a *WebSocketLinkAdapter) Status() transport.LinkStatus {
	return transport.LinkStatus(a.VirtualLink.Status)
}

// OpenStream opens a new stream on the link
func (a *WebSocketLinkAdapter) OpenStream(params transport.StreamParams) (transport.Stream, error) {
	wsParams := StreamParams{
		StreamID: params.StreamID,
		Class:    LaneClass(params.Class),
		Priority: params.Priority,
		Size:     params.Size,
	}

	stream, err := a.VirtualLink.OpenStream(wsParams)
	if err != nil {
		return nil, err
	}

	return &WebSocketStreamAdapter{stream}, nil
}

// Send sends data over the link
func (a *WebSocketLinkAdapter) Send(data []byte, laneID int) error {
	frame := &Frame{
		Type:      FrameTypeData,
		LaneID:    laneID,
		Payload:   data,
		Timestamp: time.Now().UnixNano(),
	}

	return a.VirtualLink.Send(frame, laneID)
}

// Stats returns link statistics
func (a *WebSocketLinkAdapter) Stats() map[int]transport.LaneStats {
	wsStats := a.VirtualLink.Stats()
	stats := make(map[int]transport.LaneStats)

	for laneID, wsLaneStats := range wsStats {
		stats[laneID] = transport.LaneStats{
			MessagesSent:     wsLaneStats.MessagesSent,
			MessagesReceived: wsLaneStats.MessagesReceived,
			BytesSent:        wsLaneStats.BytesSent,
			BytesReceived:    wsLaneStats.BytesReceived,
			Latency:          wsLaneStats.Latency,
			LastHeartbeat:    wsLaneStats.LastHeartbeat,
			LastActivity:     wsLaneStats.LastActivity,
			SendQueueDepth:   wsLaneStats.SendQueueDepth,
			RecvQueueDepth:   wsLaneStats.RecvQueueDepth,
		}
	}

	return stats
}

// Backpressure returns the backpressure state
func (a *WebSocketLinkAdapter) Backpressure() map[int]transport.BackpressureState {
	wsBackpressure := a.VirtualLink.Backpressure()
	backpressure := make(map[int]transport.BackpressureState)

	for laneID, wsState := range wsBackpressure {
		backpressure[laneID] = transport.BackpressureState{
			QueueDepth: wsState.QueueDepth,
			QueueCap:   wsState.QueueCap,
			Status:     transport.LaneStatus(wsState.Status),
		}
	}

	return backpressure
}

// WebSocketStreamAdapter adapts the Stream to the transport.Stream interface
type WebSocketStreamAdapter struct {
	*Stream
}

// Ensure WebSocketStreamAdapter implements transport.Stream
var _ transport.Stream = (*WebSocketStreamAdapter)(nil)

// ID returns the stream ID
func (a *WebSocketStreamAdapter) ID() string {
	return a.Stream.ID
}

// LaneID returns the lane ID
func (a *WebSocketStreamAdapter) LaneID() int {
	return a.Stream.LaneID
}

// Class returns the stream class
func (a *WebSocketStreamAdapter) Class() transport.LaneClass {
	return transport.LaneClass(a.Stream.Class)
}

// Priority returns the stream priority
func (a *WebSocketStreamAdapter) Priority() int {
	return a.Stream.Priority
}

// Status returns the stream status
func (a *WebSocketStreamAdapter) Status() transport.StreamStatus {
	return transport.StreamStatus(a.Stream.Status)
}

// Send sends data over the stream
func (a *WebSocketStreamAdapter) Send(data []byte) error {
	return a.Stream.Send(data)
}

// Receive receives data from the stream
func (a *WebSocketStreamAdapter) Receive() ([]byte, error) {
	// TODO: Implement receive functionality
	// For now, return an error as this is not yet implemented
	return nil, fmt.Errorf("receive not yet implemented")
}

// Close closes the stream
func (a *WebSocketStreamAdapter) Close() error {
	return a.Stream.Close()
}
