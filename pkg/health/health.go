package health

import (
	"sync"
	"time"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
)

// CheckFunc is a function that performs a health check
type CheckFunc func() error

// Check represents a single health check result
type Check struct {
	Name        string
	Status      commonv1.HealthStatus
	Message     string
	LastChecked time.Time
}

// Checker manages health checks for a service
type Checker struct {
	mu          sync.RWMutex
	checks      map[string]*Check
	lastHealthy time.Time
}

// NewChecker creates a new health checker
func NewChecker() *Checker {
	return &Checker{
		checks:      make(map[string]*Check),
		lastHealthy: time.Now(),
	}
}

// RunCheck executes a health check and updates the status
func (c *Checker) RunCheck(name string, checkFunc CheckFunc) {
	status := commonv1.HealthStatus_HEALTH_STATUS_HEALTHY
	message := "OK"

	if err := checkFunc(); err != nil {
		status = commonv1.HealthStatus_HEALTH_STATUS_UNHEALTHY
		message = err.Error()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.checks[name] = &Check{
		Name:        name,
		Status:      status,
		Message:     message,
		LastChecked: time.Now(),
	}

	// Update last healthy time if all checks pass
	if c.isHealthy() {
		c.lastHealthy = time.Now()
	}
}

// GetOverallStatus returns the overall health status
func (c *Checker) GetOverallStatus() commonv1.HealthStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.checks) == 0 {
		return commonv1.HealthStatus_HEALTH_STATUS_HEALTHY
	}

	unhealthyCount := 0
	for _, check := range c.checks {
		if check.Status == commonv1.HealthStatus_HEALTH_STATUS_UNHEALTHY {
			unhealthyCount++
		}
	}

	if unhealthyCount == 0 {
		return commonv1.HealthStatus_HEALTH_STATUS_HEALTHY
	} else if unhealthyCount < len(c.checks) {
		return commonv1.HealthStatus_HEALTH_STATUS_DEGRADED
	}

	return commonv1.HealthStatus_HEALTH_STATUS_UNHEALTHY
}

// GetAllChecks returns all health check results
func (c *Checker) GetAllChecks() []*Check {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var checks []*Check
	for _, check := range c.checks {
		checkCopy := *check
		checks = append(checks, &checkCopy)
	}

	return checks
}

// GetLastHealthyTime returns the last time all checks were healthy
func (c *Checker) GetLastHealthyTime() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastHealthy
}

func (c *Checker) isHealthy() bool {
	for _, check := range c.checks {
		if check.Status != commonv1.HealthStatus_HEALTH_STATUS_HEALTHY {
			return false
		}
	}
	return true
}
