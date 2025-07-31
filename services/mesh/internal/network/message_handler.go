package network

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/consensus"
	"github.com/redbco/redb-open/services/mesh/internal/messages"
	"github.com/redbco/redb-open/services/mesh/internal/routing"
)

// MeshMessageHandler handles WebSocket messages for the mesh network
type MeshMessageHandler struct {
	router    *routing.Router
	logger    *logger.Logger
	group     *consensus.Group
	validator *messages.MessageValidator
}

// NewMeshMessageHandler creates a new mesh message handler
func NewMeshMessageHandler(router *routing.Router, group *consensus.Group, logger *logger.Logger) *MeshMessageHandler {
	return &MeshMessageHandler{
		router:    router,
		group:     group,
		logger:    logger,
		validator: messages.NewMessageValidator(),
	}
}

// HandleMessage processes incoming WebSocket messages
func (h *MeshMessageHandler) HandleMessage(msg *messages.Message) error {
	// Validate the message
	if err := h.validator.ValidateMessage(msg); err != nil {
		return fmt.Errorf("message validation failed: %w", err)
	}

	h.logger.Debugf("Processing message: (id: %s, type: %s, from: %s, to: %s, priority: %d)",
		msg.Header.ID, msg.Header.Type, msg.Header.From, msg.Header.To, msg.Header.Priority)

	switch msg.Header.Type {
	case "routing":
		return h.handleRoutingMessage(msg)
	case "consensus":
		return h.handleConsensusMessage(msg)
	case "management":
		return h.handleManagementMessage(msg)
	case "heartbeat":
		return h.handleHeartbeatMessage(msg)
	case "data":
		return h.handleDataMessage(msg)
	default:
		return fmt.Errorf("unknown message type: %s", msg.Header.Type)
	}
}

// handleRoutingMessage processes routing-related messages
func (h *MeshMessageHandler) handleRoutingMessage(msg *messages.Message) error {
	var routingPayload messages.RoutingPayload
	if err := msg.UnmarshalPayload(&routingPayload); err != nil {
		return fmt.Errorf("failed to unmarshal routing message: %w", err)
	}

	h.logger.Debugf("Processing routing message: (sub_type: %s, from: %s)",
		routingPayload.SubType, msg.Header.From)

	switch routingPayload.SubType {
	case "route_update":
		return h.handleRouteUpdate(msg, routingPayload)
	case "route_request":
		return h.handleRouteRequest(msg, routingPayload)
	case "route_response":
		return h.handleRouteResponse(msg, routingPayload)
	default:
		return fmt.Errorf("unknown routing sub-type: %s", routingPayload.SubType)
	}
}

// handleConsensusMessage processes consensus-related messages
func (h *MeshMessageHandler) handleConsensusMessage(msg *messages.Message) error {
	var consensusPayload messages.ConsensusPayload
	if err := msg.UnmarshalPayload(&consensusPayload); err != nil {
		return fmt.Errorf("failed to unmarshal consensus message: %w", err)
	}

	h.logger.Debugf("Processing consensus message: (sub_type: %s, term: %d, from: %s, to: %s)",
		consensusPayload.SubType, consensusPayload.Term, msg.Header.From, msg.Header.To)

	switch consensusPayload.SubType {
	case "request_vote":
		return h.handleRequestVote(msg, consensusPayload)
	case "append_entries":
		return h.handleAppendEntries(msg, consensusPayload)
	case "heartbeat":
		return h.handleConsensusHeartbeat(msg, consensusPayload)
	case "config_change":
		return h.handleConfigChange(msg, consensusPayload)
	default:
		return fmt.Errorf("unknown consensus sub-type: %s", consensusPayload.SubType)
	}
}

// handleRequestVote processes vote request messages
func (h *MeshMessageHandler) handleRequestVote(msg *messages.Message, payload messages.ConsensusPayload) error {
	var voteReq messages.RequestVoteMessage
	if err := payload.UnmarshalData(&voteReq); err != nil {
		return fmt.Errorf("failed to unmarshal vote request: %w", err)
	}

	h.logger.Infof("Processing vote request: (from: %s, term: %d, candidate: %s, last_log_index: %d, last_log_term: %d)",
		msg.Header.From, payload.Term, voteReq.CandidateID, voteReq.LastLogIndex, voteReq.LastLogTerm)

	// Forward the vote request to the consensus group
	cmd, err := json.Marshal(voteReq)
	if err != nil {
		return fmt.Errorf("failed to marshal vote request: %w", err)
	}

	return h.group.Apply(cmd)
}

// handleAppendEntries processes log replication messages
func (h *MeshMessageHandler) handleAppendEntries(msg *messages.Message, payload messages.ConsensusPayload) error {
	var appendReq messages.AppendEntriesMessage
	if err := payload.UnmarshalData(&appendReq); err != nil {
		return fmt.Errorf("failed to unmarshal append entries: %w", err)
	}

	h.logger.Infof("Processing append entries: (from: %s, term: %d, leader: %s, prev_log_index: %d, prev_log_term: %d, entries: %d)",
		msg.Header.From, payload.Term, appendReq.LeaderID, appendReq.PrevLogIndex, appendReq.PrevLogTerm, len(appendReq.Entries))

	// Forward the append entries request to the consensus group
	cmd, err := json.Marshal(appendReq)
	if err != nil {
		return fmt.Errorf("failed to marshal append entries: %w", err)
	}

	return h.group.Apply(cmd)
}

// handleConsensusHeartbeat processes consensus heartbeat messages
func (h *MeshMessageHandler) handleConsensusHeartbeat(msg *messages.Message, payload messages.ConsensusPayload) error {
	// Consensus heartbeats are empty append entries messages
	var heartbeat messages.AppendEntriesMessage
	if err := payload.UnmarshalData(&heartbeat); err != nil {
		return fmt.Errorf("failed to unmarshal consensus heartbeat: %w", err)
	}

	h.logger.Debugf("Processing consensus heartbeat: (from: %s, term: %d, leader: %s)",
		msg.Header.From, payload.Term, heartbeat.LeaderID)

	// Forward the heartbeat to the consensus group
	cmd, err := json.Marshal(heartbeat)
	if err != nil {
		return fmt.Errorf("failed to marshal consensus heartbeat: %w", err)
	}

	return h.group.Apply(cmd)
}

// handleConfigChange processes configuration change messages
func (h *MeshMessageHandler) handleConfigChange(msg *messages.Message, payload messages.ConsensusPayload) error {
	var configChange messages.ConfigChangeMessage
	if err := payload.UnmarshalData(&configChange); err != nil {
		return fmt.Errorf("failed to unmarshal config change: %w", err)
	}

	h.logger.Infof("Processing configuration change: (from: %s, term: %d, type: %s, node_id: %s)",
		msg.Header.From, payload.Term, configChange.Type, configChange.NodeID)

	// Forward the configuration change to the consensus group
	cmd, err := json.Marshal(configChange)
	if err != nil {
		return fmt.Errorf("failed to marshal config change: %w", err)
	}

	return h.group.Apply(cmd)
}

// handleManagementMessage processes management-related messages
func (h *MeshMessageHandler) handleManagementMessage(msg *messages.Message) error {
	var managementPayload messages.ManagementPayload
	if err := msg.UnmarshalPayload(&managementPayload); err != nil {
		return fmt.Errorf("failed to unmarshal management message: %w", err)
	}

	h.logger.Debugf("Processing management message: (sub_type: %s, from: %s, to: %s)",
		managementPayload.SubType, msg.Header.From, msg.Header.To)

	switch managementPayload.SubType {
	case "node_discovery":
		return h.handleNodeDiscovery(msg, managementPayload)
	case "connection_management":
		return h.handleConnectionManagement(msg, managementPayload)
	case "topology_update":
		return h.handleTopologyUpdate(msg, managementPayload)
	case "health_status":
		return h.handleHealthStatus(msg, managementPayload)
	default:
		return fmt.Errorf("unknown management sub-type: %s", managementPayload.SubType)
	}
}

// handleNodeDiscovery processes node discovery messages
func (h *MeshMessageHandler) handleNodeDiscovery(msg *messages.Message, payload messages.ManagementPayload) error {
	var discoveryMsg messages.NodeDiscoveryMessage
	if err := payload.UnmarshalData(&discoveryMsg); err != nil {
		return fmt.Errorf("failed to unmarshal node discovery message: %w", err)
	}

	h.logger.Infof("Processing node discovery message: (type: %s, node_id: %s, mesh_id: %s, address: %s)",
		discoveryMsg.Type, discoveryMsg.NodeID, discoveryMsg.MeshID, discoveryMsg.Address)

	switch discoveryMsg.Type {
	case "announce":
		// Add the discovered node to the mesh
		if err := h.group.AddPeer(discoveryMsg.NodeID, discoveryMsg.Address); err != nil {
			return fmt.Errorf("failed to add discovered node: %w", err)
		}
	case "request":
		// Send node information back to the requesting node
		// TODO: Implement node information response
	}

	return nil
}

// handleConnectionManagement processes connection management messages
func (h *MeshMessageHandler) handleConnectionManagement(msg *messages.Message, payload messages.ManagementPayload) error {
	var connMsg messages.ConnectionManagementMessage
	if err := payload.UnmarshalData(&connMsg); err != nil {
		return fmt.Errorf("failed to unmarshal connection management message: %w", err)
	}

	h.logger.Infof("Processing connection management message: (type: %s, peer_id: %s, address: %s, status: %s)",
		connMsg.Type, connMsg.PeerID, connMsg.Address, connMsg.Status)

	switch connMsg.Type {
	case "connect":
		// Add a new connection
		if err := h.group.AddPeer(connMsg.PeerID, connMsg.Address); err != nil {
			return fmt.Errorf("failed to add connection: %w", err)
		}
	case "disconnect":
		// Remove an existing connection
		if err := h.group.RemovePeer(connMsg.PeerID); err != nil {
			return fmt.Errorf("failed to remove connection: %w", err)
		}
	case "status":
		// Update connection status
		// TODO: Implement connection status update
	}

	return nil
}

// handleTopologyUpdate processes network topology update messages
func (h *MeshMessageHandler) handleTopologyUpdate(msg *messages.Message, payload messages.ManagementPayload) error {
	var topologyMsg messages.TopologyUpdateMessage
	if err := payload.UnmarshalData(&topologyMsg); err != nil {
		return fmt.Errorf("failed to unmarshal topology update message: %w", err)
	}

	h.logger.Infof("Processing topology update message: (action: %s, node_id: %s, address: %s, neighbors: %v)",
		topologyMsg.Action, topologyMsg.NodeID, topologyMsg.Address, topologyMsg.Neighbors)

	// Forward the topology update to the consensus group
	cmd, err := json.Marshal(topologyMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal topology update: %w", err)
	}

	return h.group.Apply(cmd)
}

// handleHealthStatus processes health status update messages
func (h *MeshMessageHandler) handleHealthStatus(msg *messages.Message, payload messages.ManagementPayload) error {
	var healthMsg messages.HealthStatusMessage
	if err := payload.UnmarshalData(&healthMsg); err != nil {
		return fmt.Errorf("failed to unmarshal health status message: %w", err)
	}

	h.logger.Infof("Processing health status message: (node_id: %s, status: %s, timestamp: %d)",
		healthMsg.NodeID, healthMsg.Status, healthMsg.Timestamp)

	// Forward the health status to the consensus group
	cmd, err := json.Marshal(healthMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal health status: %w", err)
	}

	return h.group.Apply(cmd)
}

// handleHeartbeatMessage processes general heartbeat messages
func (h *MeshMessageHandler) handleHeartbeatMessage(msg *messages.Message) error {
	h.logger.Debugf("Processing heartbeat message: (from: %s, to: %s)",
		msg.Header.From, msg.Header.To)

	// Update node liveness information
	// TODO: Implement heartbeat processing logic
	return nil
}

// handleDataMessage processes data messages
func (h *MeshMessageHandler) handleDataMessage(msg *messages.Message) error {
	h.logger.Debugf("Processing data message: (from: %s, to: %s, size: %d bytes)",
		msg.Header.From, msg.Header.To, len(msg.Payload))

	// Route the data message to its destination
	if msg.Header.To != "" {
		// Direct message - route to specific node
		// Create a routing message for the data
		routingMsg := routing.Message{
			ID:        msg.Header.ID,
			FromNode:  msg.Header.From,
			ToNode:    msg.Header.To,
			Content:   msg.Payload,
			Metadata:  map[string]string{"type": "data"},
			Timestamp: time.Unix(0, msg.Header.Timestamp),
		}
		return h.router.QueueMessage(&routingMsg)
	} else {
		// Broadcast message - process locally
		// TODO: Implement data message processing logic
		return nil
	}
}

// handleRouteUpdate processes route update messages
func (h *MeshMessageHandler) handleRouteUpdate(msg *messages.Message, payload messages.RoutingPayload) error {
	var routingMsg routing.Message
	if err := payload.UnmarshalData(&routingMsg); err != nil {
		return fmt.Errorf("failed to unmarshal route update: %w", err)
	}

	h.logger.Debugf("Processing route update: (from: %s)", msg.Header.From)

	// Queue the message for routing
	return h.router.QueueMessage(&routingMsg)
}

// handleRouteRequest processes route request messages
func (h *MeshMessageHandler) handleRouteRequest(msg *messages.Message, payload messages.RoutingPayload) error {
	h.logger.Debugf("Processing route request: (from: %s)", msg.Header.From)

	// TODO: Implement route request processing
	// This would typically involve looking up routing information and responding
	return nil
}

// handleRouteResponse processes route response messages
func (h *MeshMessageHandler) handleRouteResponse(msg *messages.Message, payload messages.RoutingPayload) error {
	h.logger.Debugf("Processing route response: (from: %s)", msg.Header.From)

	// TODO: Implement route response processing
	// This would typically involve updating local routing tables
	return nil
}
