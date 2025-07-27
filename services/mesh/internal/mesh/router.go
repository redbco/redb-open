package mesh

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
)

// Route represents a routing table entry
type Route struct {
	Destination string  `json:"destination"`
	NextHop     string  `json:"next_hop"`
	Cost        float64 `json:"cost"`
	LastUpdate  int64   `json:"last_update"`
}

// RouteUpdate represents a route update message
type RouteUpdate struct {
	Routes []Route `json:"routes"`
	From   string  `json:"from"`
	Time   int64   `json:"time"`
}

// Router handles message routing
type Router struct {
	routes   map[string]Route // destination -> route
	metrics  map[string]int   // route -> cost
	mu       sync.RWMutex
	lastSync time.Time
	logger   *logger.Logger
	network  *Network
	nodeID   string
}

// NewRouter creates a new router
func NewRouter(network *Network, nodeID string, logger *logger.Logger) *Router {
	return &Router{
		routes:   make(map[string]Route),
		metrics:  make(map[string]int),
		lastSync: time.Now(),
		logger:   logger,
		network:  network,
		nodeID:   nodeID,
	}
}

// AddRoute adds a route to the routing table
func (r *Router) AddRoute(route Route) error {
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
	r.routes[route.Destination] = route
	r.metrics[route.Destination] = int(route.Cost)

	// Broadcast update
	go r.broadcastRouteUpdate()

	return nil
}

// RemoveRoute removes a route from the routing table
func (r *Router) RemoveRoute(destination string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.routes[destination]; !exists {
		return fmt.Errorf("route not found: %s", destination)
	}

	delete(r.routes, destination)
	delete(r.metrics, destination)

	// Broadcast update
	go r.broadcastRouteUpdate()

	return nil
}

// GetRoute returns the route for a destination
func (r *Router) GetRoute(destination string) (Route, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	route, exists := r.routes[destination]
	if !exists {
		return Route{}, fmt.Errorf("route not found: %s", destination)
	}

	return route, nil
}

// GetRoutes returns all routes
func (r *Router) GetRoutes() map[string]Route {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Create a copy of the routes map
	routes := make(map[string]Route, len(r.routes))
	for k, v := range r.routes {
		routes[k] = v
	}

	return routes
}

// broadcastRouteUpdate broadcasts route updates to all peers
func (r *Router) broadcastRouteUpdate() {
	r.mu.RLock()
	routes := make([]Route, 0, len(r.routes))
	for _, route := range r.routes {
		routes = append(routes, route)
	}
	r.mu.RUnlock()

	update := RouteUpdate{
		Routes: routes,
		From:   r.nodeID,
		Time:   time.Now().UnixNano(),
	}

	data, err := json.Marshal(update)
	if err != nil {
		r.logger.Errorf("Failed to marshal route update: %v", err)
		return
	}

	msg := Message{
		Type:      RouteUpdateMsg,
		From:      r.nodeID,
		Timestamp: time.Now(),
		Payload:   data,
	}

	if err := r.network.Broadcast(string(RouteUpdateMsg), msg); err != nil {
		r.logger.Errorf("Failed to broadcast route update: %v", err)
	}
}

// handleRouteUpdate handles a route update from a peer
func (r *Router) handleRouteUpdate(update RouteUpdate) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if update is stale
	if time.Since(r.lastSync) < time.Second {
		return nil
	}

	// Update routes
	for _, route := range update.Routes {
		// Skip routes from self
		if route.NextHop == r.nodeID {
			continue
		}

		// Update cost to include hop to peer
		route.Cost += 1

		// Check if we have a better route
		existing, exists := r.routes[route.Destination]
		if !exists || route.Cost < existing.Cost {
			r.routes[route.Destination] = route
			r.metrics[route.Destination] = int(route.Cost)
		}
	}

	r.lastSync = time.Now()
	return nil
}

// cleanupStaleRoutes removes routes that haven't been updated recently
func (r *Router) cleanupStaleRoutes(maxAge time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UnixNano()
	for dest, route := range r.routes {
		if now-route.LastUpdate > maxAge.Nanoseconds() {
			delete(r.routes, dest)
			delete(r.metrics, dest)
		}
	}
}

// GetNextHop returns the next hop for a destination
func (r *Router) GetNextHop(destination string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	route, exists := r.routes[destination]
	if !exists {
		return "", fmt.Errorf("no route to destination: %s", destination)
	}

	return route.NextHop, nil
}

// GetCost returns the cost to reach a destination
func (r *Router) GetCost(destination string) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cost, exists := r.metrics[destination]
	if !exists {
		return 0, fmt.Errorf("no route to destination: %s", destination)
	}

	return cost, nil
}
