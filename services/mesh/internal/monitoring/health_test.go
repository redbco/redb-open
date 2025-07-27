package monitoring

import (
	"context"
	"testing"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestHealthChecker(t *testing.T) {
	logger := logger.New("mesh-test", "1.0.0")
	checker := NewHealthChecker(logger)

	// Test initial status
	assert.Equal(t, "unknown", checker.GetStorageStatus().Status)
	assert.Equal(t, "unknown", checker.GetNetworkStatus().Status)
	assert.Equal(t, "unknown", checker.GetConsensusStatus().Status)
	assert.Equal(t, "unknown", checker.GetRouteStatus().Status)

	// Test updating node status
	nodeID := "test-node"
	checker.UpdateNodeStatus(nodeID, "healthy", "Node is running normally", map[string]interface{}{
		"uptime": "1h",
	})

	status := checker.GetNodeStatus(nodeID)
	assert.Equal(t, "healthy", status.Status)
	assert.Equal(t, "Node is running normally", status.Message)
	assert.Equal(t, "1h", status.Details["uptime"])

	// Test updating storage status
	checker.UpdateStorageStatus("healthy", "Storage is operational", map[string]interface{}{
		"free_space": "10GB",
	})

	status = checker.GetStorageStatus()
	assert.Equal(t, "healthy", status.Status)
	assert.Equal(t, "Storage is operational", status.Message)
	assert.Equal(t, "10GB", status.Details["free_space"])

	// Test updating network status
	checker.UpdateNetworkStatus("degraded", "High latency detected", map[string]interface{}{
		"latency": "500ms",
	})

	status = checker.GetNetworkStatus()
	assert.Equal(t, "degraded", status.Status)
	assert.Equal(t, "High latency detected", status.Message)
	assert.Equal(t, "500ms", status.Details["latency"])

	// Test updating consensus status
	checker.UpdateConsensusStatus("healthy", "Consensus is stable", map[string]interface{}{
		"leader": "node-1",
	})

	status = checker.GetConsensusStatus()
	assert.Equal(t, "healthy", status.Status)
	assert.Equal(t, "Consensus is stable", status.Message)
	assert.Equal(t, "node-1", status.Details["leader"])

	// Test updating route status
	checker.UpdateRouteStatus("healthy", "Routes are optimal", map[string]interface{}{
		"active_routes": 10,
	})

	status = checker.GetRouteStatus()
	assert.Equal(t, "healthy", status.Status)
	assert.Equal(t, "Routes are optimal", status.Message)
	assert.Equal(t, 10, status.Details["active_routes"])

	// Test getting all status
	allStatus := checker.GetAllStatus()
	assert.NotNil(t, allStatus["nodes"])
	assert.NotNil(t, allStatus["storage"])
	assert.NotNil(t, allStatus["network"])
	assert.NotNil(t, allStatus["consensus"])
	assert.NotNil(t, allStatus["routing"])

	// Test health checker start and stop
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := checker.Start(ctx)
	assert.NoError(t, err)

	// Wait for at least one health check
	time.Sleep(50 * time.Millisecond)

	err = checker.Stop()
	assert.NoError(t, err)
}

func TestHealthCheckTimeouts(t *testing.T) {
	logger := logger.New("mesh-test", "1.0.0")
	checker := NewHealthChecker(logger)

	// Test with a very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Start the health checker
	err := checker.Start(ctx)
	assert.NoError(t, err)

	// Wait for health checks to complete
	time.Sleep(10 * time.Millisecond)

	// Check that at least one component is marked as degraded
	allStatus := checker.GetAllStatus()
	hasDegraded := false
	for _, status := range allStatus {
		if s, ok := status.(*HealthStatus); ok && s.Status == "degraded" {
			hasDegraded = true
			break
		}
	}
	assert.True(t, hasDegraded, "Expected at least one component to be marked as degraded due to timeout")

	err = checker.Stop()
	assert.NoError(t, err)
}

func TestConcurrentHealthChecks(t *testing.T) {
	logger := logger.New("mesh-test", "1.0.0")
	checker := NewHealthChecker(logger)

	// Test concurrent updates to the same component
	nodeID := "test-node"
	updates := 100
	done := make(chan struct{})

	for i := 0; i < updates; i++ {
		go func() {
			checker.UpdateNodeStatus(nodeID, "healthy", "Node is running normally", map[string]interface{}{
				"uptime": "1h",
			})
			done <- struct{}{}
		}()
	}

	// Wait for all updates to complete
	for i := 0; i < updates; i++ {
		<-done
	}

	// Verify the final state
	status := checker.GetNodeStatus(nodeID)
	assert.Equal(t, "healthy", status.Status)
	assert.Equal(t, "Node is running normally", status.Message)
	assert.Equal(t, "1h", status.Details["uptime"])
}

func TestHealthStatusDetails(t *testing.T) {
	logger := logger.New("mesh-test", "1.0.0")
	checker := NewHealthChecker(logger)

	// Test different types of details
	details := map[string]interface{}{
		"string":   "value",
		"int":      42,
		"float":    3.14,
		"bool":     true,
		"slice":    []string{"a", "b", "c"},
		"map":      map[string]int{"x": 1, "y": 2},
		"nil":      nil,
		"time":     time.Now(),
		"duration": time.Second,
	}

	checker.UpdateNodeStatus("test-node", "healthy", "Test details", details)
	status := checker.GetNodeStatus("test-node")

	// Verify that all details are preserved
	for k, v := range details {
		assert.Equal(t, v, status.Details[k], "Detail %s should be preserved", k)
	}
}
