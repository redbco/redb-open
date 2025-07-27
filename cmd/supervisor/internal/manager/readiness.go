package manager

import (
	"context"
	"sync"
	"time"

	"github.com/redbco/redb-open/cmd/supervisor/internal/logger"
)

// ReadinessManager manages system readiness state and callbacks
type ReadinessManager struct {
	mu               sync.RWMutex
	logger           logger.LoggerInterface
	serviceManager   *ServiceManager
	isSystemReady    bool
	systemReadyTime  time.Time
	readinessChecked bool
	lastLogTime      time.Time
	callbacks        []SystemReadyCallback
	checkInterval    time.Duration
}

// NewReadinessManager creates a new readiness manager
func NewReadinessManager(logger logger.LoggerInterface, serviceManager *ServiceManager) *ReadinessManager {
	return &ReadinessManager{
		logger:         logger,
		serviceManager: serviceManager,
		checkInterval:  2 * time.Second, // Check every 2 seconds for faster detection
		callbacks:      make([]SystemReadyCallback, 0),
	}
}

// AddSystemReadyCallback adds a callback to be executed when the system becomes ready
func (rm *ReadinessManager) AddSystemReadyCallback(callback SystemReadyCallback) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.callbacks = append(rm.callbacks, callback)

	// If system is already ready, execute callback immediately
	if rm.isSystemReady {
		go callback()
	}
}

// IsSystemReady returns whether the system is currently ready
func (rm *ReadinessManager) IsSystemReady() bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.isSystemReady
}

// GetSystemReadyTime returns when the system became ready (zero time if not ready)
func (rm *ReadinessManager) GetSystemReadyTime() time.Time {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.systemReadyTime
}

// Start begins monitoring system readiness
func (rm *ReadinessManager) Start(ctx context.Context) {
	ticker := time.NewTicker(rm.checkInterval)
	defer ticker.Stop()

	// Check immediately on start
	rm.checkSystemReadiness()

	for {
		select {
		case <-ctx.Done():
			rm.logger.Info("System readiness monitor stopped")
			return
		case <-ticker.C:
			rm.checkSystemReadiness()
		}
	}
}

// checkSystemReadiness checks if all services are ready and triggers callbacks if needed
func (rm *ReadinessManager) checkSystemReadiness() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// If already ready, no need to check again
	if rm.isSystemReady {
		return
	}

	// Check if all configured services are healthy
	allServicesHealthy := rm.serviceManager.AreAllConfiguredServicesHealthy()

	if allServicesHealthy {
		rm.isSystemReady = true
		rm.systemReadyTime = time.Now()

		// Log system ready status
		rm.logSystemReady()

		// Execute all callbacks
		for _, callback := range rm.callbacks {
			go callback()
		}
	} else {
		// Log current status periodically for debugging (every 20 seconds after first check)
		currentTime := time.Now()
		if !rm.readinessChecked || currentTime.Sub(rm.lastLogTime) > 20*time.Second {
			rm.logSystemStatus()
			rm.readinessChecked = true
			rm.lastLogTime = currentTime
		}
	}
}

// logSystemReady logs the system ready notification
func (rm *ReadinessManager) logSystemReady() {
	serviceStatus := rm.serviceManager.GetConfiguredServiceStatus()

	rm.logger.Info("🎉 SYSTEM READY - All services are up, running, and healthy!")
	rm.logger.Infof("System became ready at: %s", rm.systemReadyTime.Format(time.RFC3339))

	// Log service status summary
	healthyCount := 0
	totalEnabled := 0

	for serviceName, status := range serviceStatus {
		if status != "disabled" {
			totalEnabled++
			if status == "healthy" || status == "degraded but operational" {
				healthyCount++
			}
		}
		rm.logger.Infof("  ✓ %s: %s", serviceName, status)
	}

	rm.logger.Infof("Service summary: %d/%d services healthy and running", healthyCount, totalEnabled)

	// Log structured event information
	rm.logger.Infof("SYSTEM_READY_EVENT: timestamp=%s, services_total=%d, services_healthy=%d",
		rm.systemReadyTime.Format(time.RFC3339), totalEnabled, healthyCount)
}

// logSystemStatus logs current system status (for debugging)
func (rm *ReadinessManager) logSystemStatus() {
	serviceStatus := rm.serviceManager.GetConfiguredServiceStatus()

	rm.logger.Info("Checking system readiness...")

	for serviceName, status := range serviceStatus {
		if status == "healthy" || status == "degraded but operational" {
			rm.logger.Infof("  ✓ %s: %s", serviceName, status)
		} else {
			rm.logger.Infof("  ⏳ %s: %s", serviceName, status)
		}
	}
}

// ForceReadinessCheck forces an immediate readiness check (useful for testing)
func (rm *ReadinessManager) ForceReadinessCheck() {
	rm.checkSystemReadiness()
}
