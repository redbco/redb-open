package network

import (
	"encoding/json"
	"fmt"

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

	switch msg.Type {
	case "routing":
		return h.handleRoutingMessage(msg)
	case "consensus":
		return h.handleConsensusMessage(msg)
	case "management":
		return h.handleManagementMessage(msg)
	default:
		return fmt.Errorf("unknown message type: %s", msg.Type)
	}
}

// handleRoutingMessage processes routing-related messages
func (h *MeshMessageHandler) handleRoutingMessage(msg *messages.Message) error {
	var routingMsg routing.Message
	if err := msg.UnmarshalContent(&routingMsg); err != nil {
		return fmt.Errorf("failed to unmarshal routing message: %w", err)
	}

	// Queue the message for routing
	return h.router.QueueMessage(&routingMsg)
}

// handleConsensusMessage processes consensus-related messages
func (h *MeshMessageHandler) handleConsensusMessage(msg *messages.Message) error {
	var consensusMsg messages.ConsensusMessage
	if err := msg.UnmarshalContent(&consensusMsg); err != nil {
		return fmt.Errorf("failed to unmarshal consensus message: %w", err)
	}

	h.logger.Debugf("Processing consensus message: (type: %s, term: %d, from: %s, to: %s)",
		consensusMsg.Type, consensusMsg.Term, consensusMsg.From, consensusMsg.To)

	switch consensusMsg.Type {
	case "request_vote":
		return h.handleRequestVote(consensusMsg)
	case "append_entries":
		return h.handleAppendEntries(consensusMsg)
	case "heartbeat":
		return h.handleHeartbeat(consensusMsg)
	case "config_change":
		return h.handleConfigChange(consensusMsg)
	default:
		return fmt.Errorf("unknown consensus message type: %s", consensusMsg.Type)
	}
}

// handleRequestVote processes vote request messages
func (h *MeshMessageHandler) handleRequestVote(msg messages.ConsensusMessage) error {
	var voteReq messages.RequestVoteMessage
	if err := msg.UnmarshalContent(&voteReq); err != nil {
		return fmt.Errorf("failed to unmarshal vote request: %w", err)
	}

	h.logger.Infof("Processing vote request: (from: %s, term: %d, candidate: %s, last_log_index: %d, last_log_term: %d)",
		msg.From, voteReq.Term, voteReq.CandidateID, voteReq.LastLogIndex, voteReq.LastLogTerm)

	// Forward the vote request to the consensus group
	cmd, err := json.Marshal(voteReq)
	if err != nil {
		return fmt.Errorf("failed to marshal vote request: %w", err)
	}

	return h.group.Apply(cmd)
}

// handleAppendEntries processes log replication messages
func (h *MeshMessageHandler) handleAppendEntries(msg messages.ConsensusMessage) error {
	var appendReq messages.AppendEntriesMessage
	if err := msg.UnmarshalContent(&appendReq); err != nil {
		return fmt.Errorf("failed to unmarshal append entries: %w", err)
	}

	h.logger.Infof("Processing append entries: (from: %s, term: %d, leader: %s, prev_log_index: %d, prev_log_term: %d, entries: %d)",
		msg.From, appendReq.Term, appendReq.LeaderID, appendReq.PrevLogIndex, appendReq.PrevLogTerm, len(appendReq.Entries))

	// Forward the append entries request to the consensus group
	cmd, err := json.Marshal(appendReq)
	if err != nil {
		return fmt.Errorf("failed to marshal append entries: %w", err)
	}

	return h.group.Apply(cmd)
}

// handleHeartbeat processes heartbeat messages
func (h *MeshMessageHandler) handleHeartbeat(msg messages.ConsensusMessage) error {
	// Heartbeats are empty append entries messages
	var heartbeat messages.AppendEntriesMessage
	if err := msg.UnmarshalContent(&heartbeat); err != nil {
		return fmt.Errorf("failed to unmarshal heartbeat: %w", err)
	}

	h.logger.Debugf("Processing heartbeat: (from: %s, term: %d, leader: %s)",
		msg.From, heartbeat.Term, heartbeat.LeaderID)

	// Forward the heartbeat to the consensus group
	cmd, err := json.Marshal(heartbeat)
	if err != nil {
		return fmt.Errorf("failed to marshal heartbeat: %w", err)
	}

	return h.group.Apply(cmd)
}

// handleConfigChange processes configuration change messages
func (h *MeshMessageHandler) handleConfigChange(msg messages.ConsensusMessage) error {
	var configChange messages.ConfigChangeMessage
	if err := msg.UnmarshalContent(&configChange); err != nil {
		return fmt.Errorf("failed to unmarshal config change: %w", err)
	}

	h.logger.Infof("Processing configuration change: (from: %s, term: %d, type: %s, node_id: %s)",
		msg.From, configChange.Term, configChange.Type, configChange.NodeID)

	// Forward the configuration change to the consensus group
	cmd, err := json.Marshal(configChange)
	if err != nil {
		return fmt.Errorf("failed to marshal config change: %w", err)
	}

	return h.group.Apply(cmd)
}

// handleManagementMessage processes management-related messages
func (h *MeshMessageHandler) handleManagementMessage(msg *messages.Message) error {
	var managementMsg messages.ManagementMessage
	if err := msg.UnmarshalContent(&managementMsg); err != nil {
		return fmt.Errorf("failed to unmarshal management message: %w", err)
	}

	h.logger.Debugf("Processing management message: (type: %s, from: %s, to: %s)",
		managementMsg.Type, managementMsg.From, managementMsg.To)

	switch managementMsg.Type {
	case "node_discovery":
		return h.handleNodeDiscovery(managementMsg)
	case "connection_management":
		return h.handleConnectionManagement(managementMsg)
	case "topology_update":
		return h.handleTopologyUpdate(managementMsg)
	case "health_status":
		return h.handleHealthStatus(managementMsg)
	default:
		return fmt.Errorf("unknown management message type: %s", managementMsg.Type)
	}
}

// handleNodeDiscovery processes node discovery messages
func (h *MeshMessageHandler) handleNodeDiscovery(msg messages.ManagementMessage) error {
	var discoveryMsg messages.NodeDiscoveryMessage
	if err := msg.UnmarshalContent(&discoveryMsg); err != nil {
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
func (h *MeshMessageHandler) handleConnectionManagement(msg messages.ManagementMessage) error {
	var connMsg messages.ConnectionManagementMessage
	if err := msg.UnmarshalContent(&connMsg); err != nil {
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
func (h *MeshMessageHandler) handleTopologyUpdate(msg messages.ManagementMessage) error {
	var topologyMsg messages.TopologyUpdateMessage
	if err := msg.UnmarshalContent(&topologyMsg); err != nil {
		return fmt.Errorf("failed to unmarshal topology update message: %w", err)
	}

	h.logger.Infof("Processing topology update message: (type: %s, node_id: %s, address: %s, neighbors: %v)",
		topologyMsg.Type, topologyMsg.NodeID, topologyMsg.Address, topologyMsg.Neighbors)

	// Forward the topology update to the consensus group
	cmd, err := json.Marshal(topologyMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal topology update: %w", err)
	}

	return h.group.Apply(cmd)
}

// handleHealthStatus processes health status update messages
func (h *MeshMessageHandler) handleHealthStatus(msg messages.ManagementMessage) error {
	var healthMsg messages.HealthStatusMessage
	if err := msg.UnmarshalContent(&healthMsg); err != nil {
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
