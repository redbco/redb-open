package monitoring

import (
	"context"
	"testing"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestMetricsCollector(t *testing.T) {
	logger := logger.New("mesh-test", "1.0.0")
	collector := NewMetricsCollector(logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := collector.Start(ctx)
	assert.NoError(t, err)

	// Test node metrics
	t.Run("NodeMetrics", func(t *testing.T) {
		collector.UpdateNodeMetrics("node1", "active", []string{"node2", "node3"})
		collector.UpdateNodeMetrics("node2", "inactive", []string{"node1"})

		metrics := collector.GetNodeMetrics()
		assert.Contains(t, metrics, "node1")
		assert.Contains(t, metrics, "node2")

		node1 := metrics["node1"].(map[string]interface{})
		assert.Equal(t, "active", node1["status"])
		assert.Equal(t, []string{"node2", "node3"}, node1["peers"])
		assert.NotZero(t, node1["uptime"])
		assert.NotZero(t, node1["last_seen"])
		assert.NotZero(t, node1["start_time"])
		assert.Zero(t, node1["restarts"])

		node2 := metrics["node2"].(map[string]interface{})
		assert.Equal(t, "inactive", node2["status"])
		assert.Equal(t, []string{"node1"}, node2["peers"])
		assert.NotZero(t, node2["restarts"])
	})

	// Test message metrics
	t.Run("MessageMetrics", func(t *testing.T) {
		collector.UpdateMessageMetrics("test", 100*time.Millisecond, 1024, nil)
		collector.UpdateMessageMetrics("test", 200*time.Millisecond, 2048, assert.AnError)

		metrics := collector.GetMessageMetrics()
		assert.Contains(t, metrics, "test")

		msg := metrics["test"].(map[string]interface{})
		assert.Equal(t, int64(2), msg["count"])
		assert.Equal(t, 200*time.Millisecond, msg["latency"])
		assert.Equal(t, int64(1), msg["errors"])
		assert.Equal(t, int64(3072), msg["size"])
	})

	// Test route metrics
	t.Run("RouteMetrics", func(t *testing.T) {
		collector.UpdateRouteMetrics("node3", 150*time.Millisecond, 2, nil)
		collector.UpdateRouteMetrics("node3", 250*time.Millisecond, 3, assert.AnError)

		metrics := collector.GetRouteMetrics()
		assert.Contains(t, metrics, "node3")

		route := metrics["node3"].(map[string]interface{})
		assert.Equal(t, 2, route["count"])
		assert.Equal(t, 250*time.Millisecond, route["latency"])
		assert.Equal(t, 1, route["errors"])
		assert.Equal(t, 3, route["hops"])
	})

	// Test consensus metrics
	t.Run("ConsensusMetrics", func(t *testing.T) {
		collector.UpdateConsensusMetrics(1, 100, 50*time.Millisecond, 1024, 10, nil)
		collector.UpdateConsensusMetrics(2, 200, 100*time.Millisecond, 2048, 20, assert.AnError)

		metrics := collector.GetConsensusMetrics()
		assert.Equal(t, uint64(2), metrics["term"])
		assert.Equal(t, uint64(200), metrics["index"])
		assert.Equal(t, 100*time.Millisecond, metrics["latency"])
		assert.Equal(t, int64(1), metrics["errors"])
		assert.Equal(t, int64(2048), metrics["log_size"])
		assert.Equal(t, int64(20), metrics["log_count"])
	})

	// Test system metrics
	t.Run("SystemMetrics", func(t *testing.T) {
		networkIO := map[string]int64{
			"bytes_sent":     1024,
			"bytes_received": 2048,
		}
		collector.UpdateSystemMetrics(0.5, 0.7, 0.8, networkIO)

		metrics := collector.GetSystemMetrics()
		assert.Equal(t, 0.5, metrics["cpu_usage"])
		assert.Equal(t, 0.7, metrics["memory_usage"])
		assert.Equal(t, 0.8, metrics["disk_usage"])
		assert.Equal(t, networkIO, metrics["network_io"])
	})

	// Test all metrics
	t.Run("AllMetrics", func(t *testing.T) {
		metrics := collector.GetAllMetrics()
		assert.Contains(t, metrics, "nodes")
		assert.Contains(t, metrics, "messages")
		assert.Contains(t, metrics, "routes")
		assert.Contains(t, metrics, "consensus")
		assert.Contains(t, metrics, "system")
	})

	err = collector.Stop()
	assert.NoError(t, err)
}
