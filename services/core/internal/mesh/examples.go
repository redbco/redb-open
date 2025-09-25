package mesh

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
)

// Example usage patterns for the MeshCommunicationManager

// ExampleDatabaseReplication demonstrates how to replicate database changes across nodes
func ExampleDatabaseReplication(mgr *MeshCommunicationManager, operation string, tableData map[string]interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Broadcast database update to all nodes
	return mgr.BroadcastDBUpdate(ctx, operation, map[string]interface{}{
		"table":     "workspaces",
		"operation": operation, // "insert", "update", "delete"
		"data":      tableData,
		"timestamp": time.Now().Unix(),
	})
}

// ExampleAnchorQuery demonstrates how to query anchor services on remote nodes
func ExampleAnchorQuery(mgr *MeshCommunicationManager, targetNodeID uint64, databaseID string, query string) (map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	queryData := map[string]interface{}{
		"database_id": databaseID,
		"query":       query,
		"timeout":     30,
	}

	result, err := mgr.QueryAnchorService(ctx, targetNodeID, queryData)
	if err != nil {
		return nil, fmt.Errorf("anchor query failed: %w", err)
	}

	return result, nil
}

// ExampleCommandExecution demonstrates how to send commands to remote nodes
func ExampleCommandExecution(mgr *MeshCommunicationManager, targetNodeID uint64, command string, params map[string]interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	message := &CoreMessage{
		Type:      MessageTypeCommand,
		Operation: command,
		Data:      params,
		Timestamp: time.Now().Unix(),
	}

	_, err := mgr.SendMessage(ctx, targetNodeID, message)
	return err
}

// ExampleSynchronousRequest demonstrates request-response pattern
func ExampleSynchronousRequest(mgr *MeshCommunicationManager, targetNodeID uint64) (map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	request := &CoreMessage{
		Type:      MessageTypeCommand,
		Operation: "get_status",
		Data:      map[string]interface{}{},
		Timestamp: time.Now().Unix(),
	}

	response, err := mgr.SendMessageWithResponse(ctx, targetNodeID, request, 30*time.Second)
	if err != nil {
		return nil, err
	}

	return response.Data, nil
}

// ExampleCustomMessageHandler demonstrates how to register custom message handlers
func ExampleCustomMessageHandler(mgr *MeshCommunicationManager) {
	// Register a custom handler for workspace synchronization
	mgr.RegisterMessageHandler("workspace_sync", func(ctx context.Context, msg *meshv1.Received) error {
		var coreMsg CoreMessage
		if err := json.Unmarshal(msg.Payload, &coreMsg); err != nil {
			return err
		}

		// Handle workspace synchronization
		switch coreMsg.Operation {
		case "create_workspace":
			// Handle workspace creation
			return handleWorkspaceCreation(coreMsg.Data)
		case "update_workspace":
			// Handle workspace update
			return handleWorkspaceUpdate(coreMsg.Data)
		case "delete_workspace":
			// Handle workspace deletion
			return handleWorkspaceDeletion(coreMsg.Data)
		}

		return nil
	})
}

// Example handler functions (these would be implemented based on your business logic)
func handleWorkspaceCreation(data map[string]interface{}) error {
	// Implement workspace creation logic
	return nil
}

func handleWorkspaceUpdate(data map[string]interface{}) error {
	// Implement workspace update logic
	return nil
}

func handleWorkspaceDeletion(data map[string]interface{}) error {
	// Implement workspace deletion logic
	return nil
}

// ExampleMeshTopologyMonitoring demonstrates how to monitor mesh topology changes
func ExampleMeshTopologyMonitoring(mgr *MeshCommunicationManager) error {
	ctx := context.Background()

	// Get current topology
	topology, err := mgr.meshControlClient.GetTopology(ctx, &meshv1.GetTopologyRequest{})
	if err != nil {
		return fmt.Errorf("failed to get topology: %w", err)
	}

	fmt.Printf("Current mesh topology:\n")
	fmt.Printf("Local Node ID: %d\n", topology.Topology.LocalNodeId)
	fmt.Printf("Current Epoch: %d\n", topology.Topology.CurrentEpoch)
	fmt.Printf("Neighbors: %d\n", len(topology.Topology.Neighbors))

	for _, neighbor := range topology.Topology.Neighbors {
		fmt.Printf("  - Node %d: %s (connected: %v)\n",
			neighbor.NodeId, neighbor.Addr, neighbor.Connected)
	}

	fmt.Printf("Routes: %d\n", len(topology.Topology.Routes))
	for _, route := range topology.Topology.Routes {
		fmt.Printf("  - To Node %d: cost=%d, next_hops=%v\n",
			route.DstNode, route.Cost, route.NextHops)
	}

	return nil
}

// ExampleMessageMetrics demonstrates how to get message metrics
func ExampleMessageMetrics(mgr *MeshCommunicationManager) error {
	ctx := context.Background()

	metrics, err := mgr.meshControlClient.GetMessageMetrics(ctx, &meshv1.GetMessageMetricsRequest{})
	if err != nil {
		return fmt.Errorf("failed to get message metrics: %w", err)
	}

	fmt.Printf("Message Metrics:\n")
	fmt.Printf("Total Messages: %d\n", metrics.TotalMessages)
	fmt.Printf("Delivered: %d\n", metrics.Delivered)
	fmt.Printf("Pending: %d\n", metrics.PendingNode+metrics.PendingClient)
	fmt.Printf("Success Rate: %.2f%%\n", metrics.SuccessRate)
	fmt.Printf("Failure Rate: %.2f%%\n", metrics.FailureRate)

	return nil
}
