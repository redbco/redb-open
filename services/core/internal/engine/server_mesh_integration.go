package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	"github.com/redbco/redb-open/services/core/internal/mesh"
)

// MeshIntegrationMethods provides mesh-aware implementations for core service operations

// BroadcastWorkspaceUpdate broadcasts workspace changes to all nodes in the mesh
func (s *Server) BroadcastWorkspaceUpdate(ctx context.Context, operation string, workspaceData map[string]interface{}) error {
	if s.engine.GetMeshManager() == nil {
		s.engine.logger.Debug("Mesh manager not available, skipping broadcast")
		return nil
	}

	// Don't block the main operation for mesh communication
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		data := map[string]interface{}{
			"table":     "workspaces",
			"operation": operation,
			"data":      workspaceData,
			"timestamp": time.Now().Unix(),
		}

		if err := s.engine.GetMeshManager().BroadcastDBUpdate(ctx, "workspace_updated", data); err != nil {
			s.engine.logger.Warnf("Failed to broadcast workspace update: %v", err)
		} else {
			s.engine.logger.Debugf("Successfully broadcast workspace %s operation", operation)
		}
	}()

	return nil
}

// QueryRemoteAnchorService queries an anchor service on a remote node
func (s *Server) QueryRemoteAnchorService(ctx context.Context, targetNodeID uint64, databaseID, query string) (map[string]interface{}, error) {
	if s.engine.GetMeshManager() == nil {
		return nil, fmt.Errorf("mesh manager not available")
	}

	queryData := map[string]interface{}{
		"database_id": databaseID,
		"query":       query,
		"timeout":     30,
		"node_id":     s.engine.nodeID,
	}

	result, err := s.engine.GetMeshManager().QueryAnchorService(ctx, targetNodeID, queryData)
	if err != nil {
		return nil, fmt.Errorf("failed to query remote anchor service on node %d: %w", targetNodeID, err)
	}

	return result, nil
}

// SendCommandToNode sends a command to a specific node in the mesh
func (s *Server) SendCommandToNode(ctx context.Context, targetNodeID uint64, command string, params map[string]interface{}) error {
	if s.engine.GetMeshManager() == nil {
		return fmt.Errorf("mesh manager not available")
	}

	message := &mesh.CoreMessage{
		Type:      mesh.MessageTypeCommand,
		Operation: command,
		Data:      params,
		Timestamp: time.Now().Unix(),
	}

	_, err := s.engine.GetMeshManager().SendMessage(ctx, targetNodeID, message)
	if err != nil {
		return fmt.Errorf("failed to send command %s to node %d: %w", command, targetNodeID, err)
	}

	s.engine.logger.Infof("Successfully sent command %s to node %d", command, targetNodeID)
	return nil
}

// GetMeshStatus returns the current status of the mesh
func (s *Server) GetMeshStatus(ctx context.Context) (map[string]interface{}, error) {
	if s.engine.GetMeshManager() == nil {
		return map[string]interface{}{
			"status": "unavailable",
			"reason": "mesh manager not initialized",
		}, nil
	}

	// Get topology information
	topology, err := s.engine.meshControlClient.GetTopology(ctx, &meshv1.GetTopologyRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get mesh topology: %w", err)
	}

	// Get message metrics
	metrics, err := s.engine.meshControlClient.GetMessageMetrics(ctx, &meshv1.GetMessageMetricsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get message metrics: %w", err)
	}

	return map[string]interface{}{
		"status":       "connected",
		"local_node":   topology.Topology.LocalNodeId,
		"epoch":        topology.Topology.CurrentEpoch,
		"neighbors":    len(topology.Topology.Neighbors),
		"routes":       len(topology.Topology.Routes),
		"total_msgs":   metrics.TotalMessages,
		"delivered":    metrics.Delivered,
		"success_rate": metrics.SuccessRate,
	}, nil
}

// RegisterMeshHandlers registers custom message handlers for the core service
func (s *Server) RegisterMeshHandlers() {
	if s.engine.GetMeshManager() == nil {
		return
	}

	meshManager := s.engine.GetMeshManager()

	// Register workspace synchronization handler
	meshManager.RegisterMessageHandler("workspace_sync", s.handleWorkspaceSync)

	// Register database synchronization handler
	meshManager.RegisterMessageHandler("db_sync", s.handleDatabaseSync)

	// Register node status handler
	meshManager.RegisterMessageHandler("node_status", s.handleNodeStatus)

	s.engine.logger.Info("Registered custom mesh message handlers")
}

// handleWorkspaceSync handles workspace synchronization messages
func (s *Server) handleWorkspaceSync(ctx context.Context, msg *meshv1.Received) error {
	var coreMsg mesh.CoreMessage
	if err := json.Unmarshal(msg.Payload, &coreMsg); err != nil {
		return fmt.Errorf("failed to unmarshal workspace sync message: %w", err)
	}

	s.engine.logger.Infof("Handling workspace sync from node %d: operation=%s", msg.SrcNode, coreMsg.Operation)

	switch coreMsg.Operation {
	case "workspace_created":
		return s.handleRemoteWorkspaceCreated(ctx, coreMsg.Data)
	case "workspace_updated":
		return s.handleRemoteWorkspaceUpdated(ctx, coreMsg.Data)
	case "workspace_deleted":
		return s.handleRemoteWorkspaceDeleted(ctx, coreMsg.Data)
	default:
		s.engine.logger.Warnf("Unknown workspace sync operation: %s", coreMsg.Operation)
	}

	return nil
}

// handleDatabaseSync handles database synchronization messages
func (s *Server) handleDatabaseSync(ctx context.Context, msg *meshv1.Received) error {
	var coreMsg mesh.CoreMessage
	if err := json.Unmarshal(msg.Payload, &coreMsg); err != nil {
		return fmt.Errorf("failed to unmarshal database sync message: %w", err)
	}

	s.engine.logger.Infof("Handling database sync from node %d: operation=%s", msg.SrcNode, coreMsg.Operation)

	// Handle database synchronization based on operation
	// This could involve cache invalidation, replication, etc.

	return nil
}

// handleNodeStatus handles node status messages
func (s *Server) handleNodeStatus(ctx context.Context, msg *meshv1.Received) error {
	var coreMsg mesh.CoreMessage
	if err := json.Unmarshal(msg.Payload, &coreMsg); err != nil {
		return fmt.Errorf("failed to unmarshal node status message: %w", err)
	}

	s.engine.logger.Infof("Received node status from node %d: %v", msg.SrcNode, coreMsg.Data)

	// Handle node status updates
	// This could update local node registry, health monitoring, etc.

	return nil
}

// Helper methods for handling remote operations

func (s *Server) handleRemoteWorkspaceCreated(ctx context.Context, data map[string]interface{}) error {
	// Handle remote workspace creation
	// This might involve updating local caches, triggering notifications, etc.
	s.engine.logger.Debugf("Remote workspace created: %v", data)
	return nil
}

func (s *Server) handleRemoteWorkspaceUpdated(ctx context.Context, data map[string]interface{}) error {
	// Handle remote workspace updates
	// This might involve cache invalidation, conflict resolution, etc.
	s.engine.logger.Debugf("Remote workspace updated: %v", data)
	return nil
}

func (s *Server) handleRemoteWorkspaceDeleted(ctx context.Context, data map[string]interface{}) error {
	// Handle remote workspace deletion
	// This might involve cleanup, cache invalidation, etc.
	s.engine.logger.Debugf("Remote workspace deleted: %v", data)
	return nil
}

// MeshAwareWorkspaceOperations demonstrates how to integrate mesh communication
// into existing workspace operations

// Example: Enhanced CreateWorkspace with mesh integration
func (s *Server) CreateWorkspaceWithMeshSync(ctx context.Context, workspaceName, description string) error {
	// 1. Create workspace locally (existing logic)
	// ... local database operations ...

	// 2. Broadcast to mesh
	workspaceData := map[string]interface{}{
		"name":        workspaceName,
		"description": description,
		"created_by":  s.engine.nodeID,
		"created_at":  time.Now().Unix(),
	}

	if err := s.BroadcastWorkspaceUpdate(ctx, "create", workspaceData); err != nil {
		s.engine.logger.Warnf("Failed to broadcast workspace creation: %v", err)
		// Don't fail the operation, just log the warning
	}

	return nil
}

// Example: Cross-node database query
func (s *Server) QueryDatabaseAcrossNodes(ctx context.Context, databaseID, query string) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	// Get list of nodes that might have this database
	topology, err := s.engine.meshControlClient.GetTopology(ctx, &meshv1.GetTopologyRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get topology: %w", err)
	}

	// Query each node in parallel
	type nodeResult struct {
		nodeID uint64
		result map[string]interface{}
		err    error
	}

	resultChan := make(chan nodeResult, len(topology.Topology.Neighbors))

	for _, neighbor := range topology.Topology.Neighbors {
		if neighbor.NodeId == s.engine.nodeID {
			continue // Skip self
		}

		go func(nodeID uint64) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			result, err := s.QueryRemoteAnchorService(ctx, nodeID, databaseID, query)
			resultChan <- nodeResult{nodeID: nodeID, result: result, err: err}
		}(neighbor.NodeId)
	}

	// Collect results
	for i := 0; i < len(topology.Topology.Neighbors)-1; i++ { // -1 to exclude self
		select {
		case result := <-resultChan:
			if result.err != nil {
				s.engine.logger.Warnf("Query failed on node %d: %v", result.nodeID, result.err)
				continue
			}
			results = append(results, result.result)
		case <-time.After(35 * time.Second): // Slightly longer than individual timeout
			s.engine.logger.Warn("Timeout waiting for some node responses")
			break
		}
	}

	return results, nil
}
