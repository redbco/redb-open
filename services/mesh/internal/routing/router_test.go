package routing

import (
	"context"
	"testing"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestRouter(t *testing.T) {
	config := RouterConfig{
		LocalNode: "node1",
		Logger:    logger.New("mesh-test", "1.0.0"),
		Strategy:  NewDistanceVectorStrategy(),
		QueueSize: 100,
	}

	router := NewRouter(config)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := router.Start(ctx)
	assert.NoError(t, err)

	// Test adding routes
	router.AddRoute("node2", "node3", 1)
	router.AddRoute("node2", "node4", 2)

	routes := router.GetRoutes("node2")
	assert.Len(t, routes, 2)
	assert.Equal(t, "node3", routes[0].NextHop)
	assert.Equal(t, 1, routes[0].Cost)
	assert.Equal(t, "node4", routes[1].NextHop)
	assert.Equal(t, 2, routes[1].Cost)

	// Test message routing
	msg := &Message{
		ID:        "msg1",
		FromNode:  "node1",
		ToNode:    "node2",
		Content:   []byte("test"),
		Timestamp: time.Now(),
	}

	err = router.QueueMessage(msg)
	assert.NoError(t, err)

	// Test route removal
	router.RemoveRoute("node2", "node3")
	routes = router.GetRoutes("node2")
	assert.Len(t, routes, 1)
	assert.Equal(t, "node4", routes[0].NextHop)

	// Test metrics
	metrics := router.GetMetrics()
	assert.NotNil(t, metrics)

	// Test shutdown
	err = router.Shutdown()
	assert.NoError(t, err)
}

func TestRouteCache(t *testing.T) {
	config := RouterConfig{
		LocalNode: "node1",
		Logger:    logger.New("mesh-test", "1.0.0"),
		Strategy:  NewDistanceVectorStrategy(),
		CacheTTL:  100 * time.Millisecond,
	}

	router := NewRouter(config)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := router.Start(ctx)
	assert.NoError(t, err)

	// Add a route
	router.AddRoute("node2", "node3", 1)

	// Test cache hit
	msg1 := &Message{
		ID:        "msg1",
		FromNode:  "node1",
		ToNode:    "node2",
		Content:   []byte("test1"),
		Timestamp: time.Now(),
	}

	err = router.QueueMessage(msg1)
	assert.NoError(t, err)

	// Wait for cache to expire
	time.Sleep(200 * time.Millisecond)

	// Test cache miss
	msg2 := &Message{
		ID:        "msg2",
		FromNode:  "node1",
		ToNode:    "node2",
		Content:   []byte("test2"),
		Timestamp: time.Now(),
	}

	err = router.QueueMessage(msg2)
	assert.NoError(t, err)

	err = router.Shutdown()
	assert.NoError(t, err)
}

func TestHealthCheck(t *testing.T) {
	config := RouterConfig{
		LocalNode:   "node1",
		Logger:      logger.New("mesh-test", "1.0.0"),
		Strategy:    NewDistanceVectorStrategy(),
		HealthCheck: 100 * time.Millisecond,
	}

	router := NewRouter(config)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := router.Start(ctx)
	assert.NoError(t, err)

	// Add routes
	router.AddRoute("node2", "node3", 1)
	router.AddRoute("node2", "node4", 2)

	// Wait for health check
	time.Sleep(200 * time.Millisecond)

	routes := router.GetRoutes("node2")
	assert.Len(t, routes, 2)
	assert.True(t, time.Since(routes[0].LastUpdated) < 1*time.Second)
	assert.True(t, time.Since(routes[1].LastUpdated) < 1*time.Second)

	err = router.Shutdown()
	assert.NoError(t, err)
}
