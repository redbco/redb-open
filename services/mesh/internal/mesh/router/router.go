package router

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
)

// Message represents a network message
type Message struct {
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
}

// Route represents a routing table entry
type Route struct {
	Destination string  `json:"destination"`
	NextHop     string  `json:"next_hop"`
	Cost        float64 `json:"cost"`
	LastUpdate  int64   `json:"last_update"`
}

// RouteUpdate represents a routing table update
type RouteUpdate struct {
	Source    string           `json:"source"`
	Routes    map[string]Route `json:"routes"`
	Timestamp time.Time        `json:"timestamp"`
}

// Router handles message routing in the mesh network
type Router struct {
	Network Network
	NodeID  string
	Logger  *logger.Logger
	Routes  map[string]Route
	Metrics map[string]int
	mu      sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
}

// Network defines the interface required by the router
type Network interface {
	SendMessage(target string, msgType string, payload interface{}) error
	Broadcast(msgType string, payload interface{}) error
}

// NewRouter creates a new router instance
func NewRouter(network Network, nodeID string, logger *logger.Logger) *Router {
	ctx, cancel := context.WithCancel(context.Background())
	return &Router{
		Network: network,
		NodeID:  nodeID,
		Logger:  logger,
		Routes:  make(map[string]Route),
		Metrics: make(map[string]int),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start begins the router's background tasks
func (r *Router) Start() {
	go r.broadcastRoutes()
	go r.cleanupStaleRoutes()
}

// Stop gracefully shuts down the router
func (r *Router) Stop() {
	if r.Logger != nil {
		r.Logger.Info("Stopping router...")
	}

	// Cancel context to signal shutdown to all goroutines
	r.cancel()

	// Reduced wait time for faster shutdown
	if r.Logger != nil {
		r.Logger.Info("Waiting for router goroutines to shutdown...")
	}
	time.Sleep(200 * time.Millisecond)

	if r.Logger != nil {
		r.Logger.Info("Router stopped successfully")
	}
}

// GetRoute returns the route for a destination
func (r *Router) GetRoute(destination string) (Route, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	route, exists := r.Routes[destination]
	return route, exists
}

// UpdateRoute updates the routing table with a new route
func (r *Router) UpdateRoute(route Route) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Validate route
	if route.Destination == "" {
		return fmt.Errorf("destination is required")
	}
	if route.NextHop == "" {
		return fmt.Errorf("next hop is required")
	}
	if route.Cost < 0 {
		return fmt.Errorf("cost must be non-negative")
	}

	// Update route
	r.Routes[route.Destination] = route
	r.Metrics[route.Destination] = int(route.Cost)

	// Broadcast update
	go r.broadcastRoutes()

	return nil
}

// HandleRouteUpdate processes a route update message
func (r *Router) HandleRouteUpdate(update RouteUpdate) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Update routes
	for dest, route := range update.Routes {
		if dest != r.NodeID { // Don't update routes to self
			// Check if we have a better route
			existing, exists := r.Routes[dest]
			if !exists || route.Cost < existing.Cost {
				r.Routes[dest] = route
				r.Metrics[dest] = int(route.Cost)
			}
		}
	}

	// Update metrics
	r.Metrics[update.Source]++

	return nil
}

// broadcastRoutes periodically broadcasts the routing table
func (r *Router) broadcastRoutes() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.mu.RLock()
			update := RouteUpdate{
				Source:    r.NodeID,
				Routes:    make(map[string]Route),
				Timestamp: time.Now(),
			}
			for dest, route := range r.Routes {
				update.Routes[dest] = route
			}
			r.mu.RUnlock()

			if err := r.Network.Broadcast("route_update", update); err != nil {
				r.Logger.Error("Failed to broadcast routes: (error: %v)", err)
			}
		}
	}
}

// cleanupStaleRoutes removes routes that haven't been updated recently
func (r *Router) cleanupStaleRoutes() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.mu.Lock()
			now := time.Now().UnixNano()
			for dest, route := range r.Routes {
				if now-route.LastUpdate > 5*time.Minute.Nanoseconds() {
					delete(r.Routes, dest)
					delete(r.Metrics, dest)
				}
			}
			r.mu.Unlock()
		}
	}
}

// GetNextHop returns the next hop for a destination
func (r *Router) GetNextHop(destination string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	route, exists := r.Routes[destination]
	if !exists {
		return "", fmt.Errorf("no route to destination: %s", destination)
	}

	return route.NextHop, nil
}

// GetCost returns the cost to reach a destination
func (r *Router) GetCost(destination string) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cost, exists := r.Metrics[destination]
	if !exists {
		return 0, fmt.Errorf("no route to destination: %s", destination)
	}

	return cost, nil
}

// GetAllRoutes returns a copy of all routes
func (r *Router) GetAllRoutes() map[string]Route {
	r.mu.RLock()
	defer r.mu.RUnlock()

	routes := make(map[string]Route, len(r.Routes))
	for k, v := range r.Routes {
		routes[k] = v
	}

	return routes
}
