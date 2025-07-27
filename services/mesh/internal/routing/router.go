package routing

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
)

// Message represents a message in the mesh network
type Message struct {
	ID        string
	FromNode  string
	ToNode    string
	Content   []byte
	Metadata  map[string]string
	Timestamp time.Time
}

// Route represents a route to a destination node
type Route struct {
	Destination string
	NextHop     string
	Cost        int
	LastUpdated time.Time
	Failures    int
	Latency     time.Duration
	SuccessRate float64
}

// Router handles message routing in the mesh network
type Router struct {
	localNode    string
	routingTable map[string][]Route
	messageQueue chan *Message
	logger       *logger.Logger
	mu           sync.RWMutex
	strategy     RoutingStrategy
	routeCache   map[string]*Route
	cacheTTL     time.Duration
	healthCheck  map[string]*time.Ticker
}

// RouterConfig holds configuration for the router
type RouterConfig struct {
	LocalNode   string
	Logger      *logger.Logger
	Strategy    RoutingStrategy
	CacheTTL    time.Duration
	QueueSize   int
	HealthCheck time.Duration
}

// NewRouter creates a new message router
func NewRouter(config RouterConfig) *Router {
	if config.QueueSize == 0 {
		config.QueueSize = 1000
	}
	if config.CacheTTL == 0 {
		config.CacheTTL = 5 * time.Minute
	}
	if config.HealthCheck == 0 {
		config.HealthCheck = 30 * time.Second
	}

	return &Router{
		localNode:    config.LocalNode,
		routingTable: make(map[string][]Route),
		messageQueue: make(chan *Message, config.QueueSize),
		logger:       config.Logger,
		strategy:     config.Strategy,
		routeCache:   make(map[string]*Route),
		cacheTTL:     config.CacheTTL,
		healthCheck:  make(map[string]*time.Ticker),
	}
}

// Start starts the router
func (r *Router) Start(ctx context.Context) error {
	go r.processMessages(ctx)
	go r.cleanupCache(ctx)
	go r.startHealthChecks(ctx)
	return nil
}

// processMessages processes messages from the queue
func (r *Router) processMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-r.messageQueue:
			if err := r.routeMessage(msg); err != nil {
				r.logger.Error("Failed to route message: (message_id: %s, error: %v)", msg.ID, err)
			}
		}
	}
}

// routeMessage routes a message to its destination
func (r *Router) routeMessage(msg *Message) error {
	if msg.ToNode == r.localNode {
		// Message is for this node
		// TODO: Deliver message to local handlers
		return nil
	}

	// Check cache first
	if route := r.getCachedRoute(msg.ToNode); route != nil {
		return r.forwardMessage(msg, route)
	}

	r.mu.RLock()
	routes, exists := r.routingTable[msg.ToNode]
	r.mu.RUnlock()

	if !exists {
		return fmt.Errorf("no route to node %s", msg.ToNode)
	}

	// Use strategy to select best route
	route := r.strategy.SelectRoute(routes)
	if route == nil {
		return fmt.Errorf("no valid routes to node %s", msg.ToNode)
	}

	// Cache the selected route
	r.cacheRoute(msg.ToNode, route)

	return r.forwardMessage(msg, route)
}

// forwardMessage forwards a message to the next hop
func (r *Router) forwardMessage(msg *Message, route *Route) error {
	start := time.Now()
	// TODO: Implement actual message forwarding
	latency := time.Since(start)

	// Update route metrics
	r.strategy.UpdateMetrics(route, true, latency)
	return nil
}

// getCachedRoute returns a cached route if available and valid
func (r *Router) getCachedRoute(destination string) *Route {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if route, exists := r.routeCache[destination]; exists {
		if time.Since(route.LastUpdated) < r.cacheTTL {
			return route
		}
		delete(r.routeCache, destination)
	}
	return nil
}

// cacheRoute caches a route
func (r *Router) cacheRoute(destination string, route *Route) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routeCache[destination] = route
}

// cleanupCache periodically cleans up expired cache entries
func (r *Router) cleanupCache(ctx context.Context) {
	ticker := time.NewTicker(r.cacheTTL)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.mu.Lock()
			now := time.Now()
			for dest, route := range r.routeCache {
				if now.Sub(route.LastUpdated) > r.cacheTTL {
					r.logger.Debug("Removing expired route from cache: (destination: %s, last_updated: %s)",
						dest, route.LastUpdated)
					delete(r.routeCache, dest)
				}
			}
			r.mu.Unlock()
		}
	}
}

// startHealthChecks starts periodic health checks for routes
func (r *Router) startHealthChecks(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			r.checkRouteHealth()
			time.Sleep(30 * time.Second)
		}
	}
}

// checkRouteHealth performs health checks on routes
func (r *Router) checkRouteHealth() {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, routes := range r.routingTable {
		for i := range routes {
			route := &routes[i]
			// TODO: Implement actual health check
			// For now, just update the last checked time
			route.LastUpdated = time.Now()
		}
	}
}

// AddRoute adds a route to the routing table
func (r *Router) AddRoute(destination, nextHop string, cost int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	route := Route{
		Destination: destination,
		NextHop:     nextHop,
		Cost:        cost,
		LastUpdated: time.Now(),
		SuccessRate: 1.0,
	}

	routes := r.routingTable[destination]
	routes = append(routes, route)
	r.routingTable[destination] = routes

	// Start health check for this route
	if _, exists := r.healthCheck[destination]; !exists {
		ticker := time.NewTicker(30 * time.Second)
		r.healthCheck[destination] = ticker
	}
}

// RemoveRoute removes a route from the routing table
func (r *Router) RemoveRoute(destination, nextHop string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	routes := r.routingTable[destination]
	for i, route := range routes {
		if route.NextHop == nextHop {
			routes = append(routes[:i], routes[i+1:]...)
			break
		}
	}

	if len(routes) == 0 {
		delete(r.routingTable, destination)
		// Stop health check
		if ticker, exists := r.healthCheck[destination]; exists {
			ticker.Stop()
			delete(r.healthCheck, destination)
		}
	} else {
		r.routingTable[destination] = routes
	}
}

// GetRoutes returns all routes to a destination
func (r *Router) GetRoutes(destination string) []Route {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.routingTable[destination]
}

// QueueMessage queues a message for routing
func (r *Router) QueueMessage(msg *Message) error {
	select {
	case r.messageQueue <- msg:
		return nil
	default:
		return fmt.Errorf("message queue is full")
	}
}

// Shutdown gracefully shuts down the router
func (r *Router) Shutdown() error {
	close(r.messageQueue)
	for _, ticker := range r.healthCheck {
		ticker.Stop()
	}
	return nil
}

// GetMetrics returns current routing metrics
func (r *Router) GetMetrics() map[string]interface{} {
	return r.strategy.GetMetrics()
}
