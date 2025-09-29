package mesh

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// ConsensusChecker handles split-brain detection and resolution
type ConsensusChecker struct {
	db           *database.PostgreSQL
	meshManager  *MeshCommunicationManager
	logger       *logger.Logger
	nodeID       uint64
	eventManager *MeshEventManager

	// Consensus state
	lastCheck       time.Time
	splitDetected   bool
	canAcceptWrites bool
}

// ConsensusState represents the current consensus state
type ConsensusState struct {
	TotalNodes          int
	OnlineNodes         int
	SplitDetected       bool
	IsMajorityPartition bool
	SplitStrategy       string
	SeedNodeID          uint64
	CanAcceptWrites     bool
}

// NewConsensusChecker creates a new consensus checker
func NewConsensusChecker(
	db *database.PostgreSQL,
	meshManager *MeshCommunicationManager,
	logger *logger.Logger,
	nodeID uint64,
) *ConsensusChecker {
	return &ConsensusChecker{
		db:              db,
		meshManager:     meshManager,
		logger:          logger,
		nodeID:          nodeID,
		canAcceptWrites: true, // Start optimistically
	}
}

// SetEventManager sets the event manager (circular dependency resolution)
func (c *ConsensusChecker) SetEventManager(eventManager *MeshEventManager) {
	c.eventManager = eventManager
}

// PeriodicCheck performs periodic consensus checks
func (c *ConsensusChecker) PeriodicCheck(ctx context.Context) error {
	// Don't check too frequently
	if time.Since(c.lastCheck) < 10*time.Second {
		return nil
	}

	c.lastCheck = time.Now()
	return c.CheckSplitBrain(ctx)
}

// CheckSplitBrain checks for split-brain scenarios
func (c *ConsensusChecker) CheckSplitBrain(ctx context.Context) error {
	state, err := c.getCurrentConsensusState(ctx)
	if err != nil {
		return fmt.Errorf("failed to get consensus state: %w", err)
	}

	c.logger.Debugf("Consensus check: %d/%d nodes online, split: %t, can write: %t",
		state.OnlineNodes, state.TotalNodes, state.SplitDetected, state.CanAcceptWrites)

	// Detect split-brain condition
	newSplitDetected := c.detectSplitBrain(state)

	// If split-brain state changed, handle it
	if newSplitDetected != c.splitDetected {
		c.splitDetected = newSplitDetected

		if newSplitDetected {
			c.logger.Warnf("Split-brain detected! %d/%d nodes online", state.OnlineNodes, state.TotalNodes)

			// Publish split detected event
			if c.eventManager != nil {
				event := &MeshEvent{
					Type:         corev1.MeshEventType_MESH_EVENT_SPLIT_DETECTED,
					AffectedNode: c.nodeID,
					Metadata: map[string]string{
						"total_nodes":  strconv.Itoa(state.TotalNodes),
						"online_nodes": strconv.Itoa(state.OnlineNodes),
						"strategy":     state.SplitStrategy,
					},
				}

				if err := c.eventManager.PublishEvent(ctx, event); err != nil {
					c.logger.Errorf("Failed to publish split detected event: %v", err)
				}
			}

			return c.HandleSplitBrain(ctx)
		} else {
			c.logger.Infof("Split-brain resolved! %d/%d nodes online", state.OnlineNodes, state.TotalNodes)

			// Publish split resolved event
			if c.eventManager != nil {
				event := &MeshEvent{
					Type:         corev1.MeshEventType_MESH_EVENT_SPLIT_RESOLVED,
					AffectedNode: c.nodeID,
					Metadata: map[string]string{
						"total_nodes":  strconv.Itoa(state.TotalNodes),
						"online_nodes": strconv.Itoa(state.OnlineNodes),
					},
				}

				if err := c.eventManager.PublishEvent(ctx, event); err != nil {
					c.logger.Errorf("Failed to publish split resolved event: %v", err)
				}
			}

			return c.ResolveSplitBrain(ctx)
		}
	}

	// Update database consensus state
	return c.updateConsensusState(ctx, state)
}

// HandleSplitBrain handles a detected split-brain scenario
func (c *ConsensusChecker) HandleSplitBrain(ctx context.Context) error {
	state, err := c.getCurrentConsensusState(ctx)
	if err != nil {
		return fmt.Errorf("failed to get consensus state: %w", err)
	}

	// Determine if we can accept writes based on strategy
	canWrite := c.canAcceptWritesInSplit(state)

	if canWrite != c.canAcceptWrites {
		c.canAcceptWrites = canWrite

		if canWrite {
			c.logger.Infof("Node is in majority partition, continuing to accept writes")
		} else {
			c.logger.Warnf("Node is in minority partition, stopping write acceptance")
		}
	}

	return nil
}

// ResolveSplitBrain handles split-brain resolution
func (c *ConsensusChecker) ResolveSplitBrain(ctx context.Context) error {
	c.logger.Infof("Resolving split-brain scenario")

	// Re-enable writes
	c.canAcceptWrites = true

	// TODO: Trigger full database synchronization
	// This would be handled by the DatabaseSyncManager

	return nil
}

// CanAcceptWrites returns whether this node can currently accept writes
func (c *ConsensusChecker) CanAcceptWrites() bool {
	return c.canAcceptWrites
}

// GetConsensusState returns the current consensus state
func (c *ConsensusChecker) GetConsensusState(ctx context.Context) (*ConsensusState, error) {
	return c.getCurrentConsensusState(ctx)
}

// detectSplitBrain determines if a split-brain condition exists
func (c *ConsensusChecker) detectSplitBrain(state *ConsensusState) bool {
	if state.TotalNodes <= 1 {
		return false // Can't have split-brain with single node
	}

	// Split-brain occurs when we can't reach majority of nodes
	majorityThreshold := state.TotalNodes/2 + 1
	return state.OnlineNodes < majorityThreshold
}

// canAcceptWritesInSplit determines if this node can accept writes during split-brain
func (c *ConsensusChecker) canAcceptWritesInSplit(state *ConsensusState) bool {
	switch state.SplitStrategy {
	case "SEED_NODE_PREVAILS_IN_EVEN_SPLIT":
		// If we have majority, accept writes
		if state.OnlineNodes > state.TotalNodes/2 {
			return true
		}

		// If exactly half and we're the seed node, accept writes
		if state.OnlineNodes == state.TotalNodes/2 && state.TotalNodes%2 == 0 {
			return c.nodeID == state.SeedNodeID
		}

		return false

	case "REQUIRE_MAJORITY":
		// Only accept writes if we have strict majority
		return state.OnlineNodes > state.TotalNodes/2

	default:
		c.logger.Warnf("Unknown split strategy: %s, defaulting to require majority", state.SplitStrategy)
		return state.OnlineNodes > state.TotalNodes/2
	}
}

// getCurrentConsensusState gets the current consensus state from database and mesh
func (c *ConsensusChecker) getCurrentConsensusState(ctx context.Context) (*ConsensusState, error) {
	// Get mesh information
	meshInfo, err := c.getMeshInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get mesh info: %w", err)
	}

	// Get online nodes from mesh service
	onlineNodes, err := c.getOnlineNodesCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get online nodes count: %w", err)
	}

	state := &ConsensusState{
		TotalNodes:          meshInfo.TotalNodes,
		OnlineNodes:         onlineNodes,
		SplitStrategy:       meshInfo.SplitStrategy,
		SeedNodeID:          meshInfo.SeedNodeID,
		SplitDetected:       c.splitDetected,
		IsMajorityPartition: onlineNodes > meshInfo.TotalNodes/2,
		CanAcceptWrites:     c.canAcceptWrites,
	}

	return state, nil
}

// MeshInfo holds mesh configuration information
type MeshInfo struct {
	TotalNodes    int
	SplitStrategy string
	SeedNodeID    uint64
}

// getMeshInfo gets mesh configuration from database
func (c *ConsensusChecker) getMeshInfo(ctx context.Context) (*MeshInfo, error) {
	// First check if mesh table has any rows (clean node check)
	var meshCount int
	err := c.db.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM mesh").Scan(&meshCount)
	if err != nil {
		return nil, fmt.Errorf("failed to check mesh table: %w", err)
	}

	// If no mesh exists (clean node), return default values
	if meshCount == 0 {
		c.logger.Debug("No mesh found - node is clean, using default consensus values")
		return &MeshInfo{
			TotalNodes:    1, // Only this node exists
			SplitStrategy: "SEED_NODE_PREVAILS_IN_EVEN_SPLIT",
			SeedNodeID:    c.nodeID, // This node is the seed by default
		}, nil
	}

	// Mesh exists, get actual configuration
	query := `
		SELECT 
			COALESCE((SELECT COUNT(*) FROM mesh_node_membership WHERE status = 'ACTIVE'), 0) as total_nodes,
			COALESCE(m.split_strategy, 'SEED_NODE_PREVAILS_IN_EVEN_SPLIT') as split_strategy,
			COALESCE((SELECT routing_id FROM nodes WHERE seed_node = TRUE LIMIT 1), 0) as seed_node_id
		FROM mesh m
		WHERE EXISTS (
			SELECT 1 FROM nodes n 
			WHERE n.node_id = (SELECT identity_id FROM localidentity LIMIT 1)
		)
		LIMIT 1
	`

	var info MeshInfo
	err = c.db.Pool().QueryRow(ctx, query).Scan(&info.TotalNodes, &info.SplitStrategy, &info.SeedNodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get mesh info: %w", err)
	}

	return &info, nil
}

// getOnlineNodesCount gets the count of currently online nodes from mesh service
func (c *ConsensusChecker) getOnlineNodesCount(ctx context.Context) (int, error) {
	// Get sessions from mesh control service
	sessionsResp, err := c.meshManager.meshControlClient.GetSessions(ctx, &meshv1.GetSessionsRequest{})
	if err != nil {
		return 0, fmt.Errorf("failed to get sessions from mesh service: %w", err)
	}

	// Count connected sessions + 1 for local node
	onlineCount := len(sessionsResp.Sessions) + 1

	return onlineCount, nil
}

// updateConsensusState updates the consensus state in the database
func (c *ConsensusChecker) updateConsensusState(ctx context.Context, state *ConsensusState) error {
	// Check if mesh table has any rows (clean node check)
	var meshCount int
	err := c.db.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM mesh").Scan(&meshCount)
	if err != nil {
		return fmt.Errorf("failed to check mesh table: %w", err)
	}

	// If no mesh exists (clean node), skip consensus state update
	if meshCount == 0 {
		c.logger.Debug("No mesh found - node is clean, skipping consensus state update")
		return nil
	}

	// Mesh exists, update consensus state
	query := `
		INSERT INTO mesh_consensus_state (mesh_id, total_nodes, online_nodes, split_detected, majority_side, last_consensus_check)
		SELECT m.mesh_id, $1, $2, $3, $4, CURRENT_TIMESTAMP
		FROM mesh m
		WHERE EXISTS (
			SELECT 1 FROM nodes n 
			WHERE n.node_id = (SELECT identity_id FROM localidentity LIMIT 1)
		)
		LIMIT 1
		ON CONFLICT (mesh_id) DO UPDATE SET
			total_nodes = EXCLUDED.total_nodes,
			online_nodes = EXCLUDED.online_nodes,
			split_detected = EXCLUDED.split_detected,
			majority_side = EXCLUDED.majority_side,
			last_consensus_check = EXCLUDED.last_consensus_check
	`

	_, err = c.db.Pool().Exec(ctx, query,
		state.TotalNodes,
		state.OnlineNodes,
		state.SplitDetected,
		state.IsMajorityPartition,
	)

	return err
}

// TriggerConsensusCheck manually triggers a consensus check
func (c *ConsensusChecker) TriggerConsensusCheck(ctx context.Context) (*ConsensusState, error) {
	c.logger.Infof("Manually triggered consensus check")

	if err := c.CheckSplitBrain(ctx); err != nil {
		return nil, err
	}

	return c.getCurrentConsensusState(ctx)
}
