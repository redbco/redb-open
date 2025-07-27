package routing

import (
	"time"
)

// RoutingStrategy defines the interface for different routing algorithms
type RoutingStrategy interface {
	// SelectRoute chooses the best route from available routes
	SelectRoute(routes []Route) *Route
	// UpdateMetrics updates routing metrics for a route
	UpdateMetrics(route *Route, success bool, latency time.Duration)
	// GetMetrics returns current routing metrics
	GetMetrics() map[string]interface{}
}

// FloodingStrategy implements a simple flooding routing strategy
type FloodingStrategy struct {
	metrics map[string]interface{}
}

// NewFloodingStrategy creates a new flooding routing strategy
func NewFloodingStrategy() *FloodingStrategy {
	return &FloodingStrategy{
		metrics: make(map[string]interface{}),
	}
}

func (s *FloodingStrategy) SelectRoute(routes []Route) *Route {
	if len(routes) == 0 {
		return nil
	}
	// In flooding, we return all routes
	return &routes[0]
}

func (s *FloodingStrategy) UpdateMetrics(route *Route, success bool, latency time.Duration) {
	// Update flooding-specific metrics
}

func (s *FloodingStrategy) GetMetrics() map[string]interface{} {
	return s.metrics
}

// DistanceVectorStrategy implements distance vector routing
type DistanceVectorStrategy struct {
	metrics map[string]interface{}
}

// NewDistanceVectorStrategy creates a new distance vector routing strategy
func NewDistanceVectorStrategy() *DistanceVectorStrategy {
	return &DistanceVectorStrategy{
		metrics: make(map[string]interface{}),
	}
}

func (s *DistanceVectorStrategy) SelectRoute(routes []Route) *Route {
	if len(routes) == 0 {
		return nil
	}

	// Select route with lowest cost
	bestRoute := &routes[0]
	for i := 1; i < len(routes); i++ {
		if routes[i].Cost < bestRoute.Cost {
			bestRoute = &routes[i]
		}
	}
	return bestRoute
}

func (s *DistanceVectorStrategy) UpdateMetrics(route *Route, success bool, latency time.Duration) {
	// Update distance vector metrics
	if success {
		route.Cost = int(latency.Milliseconds())
	} else {
		route.Cost = route.Cost * 2 // Penalize failed routes
	}
}

func (s *DistanceVectorStrategy) GetMetrics() map[string]interface{} {
	return s.metrics
}

// LinkStateStrategy implements link state routing
type LinkStateStrategy struct {
	metrics map[string]interface{}
}

// NewLinkStateStrategy creates a new link state routing strategy
func NewLinkStateStrategy() *LinkStateStrategy {
	return &LinkStateStrategy{
		metrics: make(map[string]interface{}),
	}
}

func (s *LinkStateStrategy) SelectRoute(routes []Route) *Route {
	if len(routes) == 0 {
		return nil
	}

	// Select route with best quality metrics
	bestRoute := &routes[0]
	for i := 1; i < len(routes); i++ {
		if routes[i].Cost < bestRoute.Cost {
			bestRoute = &routes[i]
		}
	}
	return bestRoute
}

func (s *LinkStateStrategy) UpdateMetrics(route *Route, success bool, latency time.Duration) {
	// Update link state metrics
	if success {
		route.Cost = int(latency.Milliseconds())
	} else {
		route.Cost = route.Cost * 2 // Penalize failed routes
	}
}

func (s *LinkStateStrategy) GetMetrics() map[string]interface{} {
	return s.metrics
}
