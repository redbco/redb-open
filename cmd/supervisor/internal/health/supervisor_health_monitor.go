package health

import (
	"context"
	"sync"
	"time"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	supervisorv1 "github.com/redbco/redb-open/api/proto/supervisor/v1"
	"github.com/redbco/redb-open/cmd/supervisor/internal/logger"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ServiceHealth struct {
	ServiceID   string
	ServiceName string
	Status      commonv1.HealthStatus
	LastUpdate  time.Time
	LastHealthy time.Time
}

type Monitor struct {
	mu          sync.RWMutex
	services    map[string]*ServiceHealth
	logger      logger.LoggerInterface
	subscribers map[chan *supervisorv1.ServiceHealthUpdate][]string
	commands    map[string][]*supervisorv1.ServiceCommand
}

func NewMonitor(log logger.LoggerInterface) *Monitor {
	return &Monitor{
		services:    make(map[string]*ServiceHealth),
		logger:      log,
		subscribers: make(map[chan *supervisorv1.ServiceHealthUpdate][]string),
		commands:    make(map[string][]*supervisorv1.ServiceCommand),
	}
}

func (m *Monitor) Start(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.checkServiceHealth()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Monitor) AddService(serviceID, serviceName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.services[serviceID] = &ServiceHealth{
		ServiceID:   serviceID,
		ServiceName: serviceName,
		Status:      commonv1.HealthStatus_HEALTH_STATUS_STARTING,
		LastUpdate:  time.Now(),
		LastHealthy: time.Now(),
	}
}

func (m *Monitor) RemoveService(serviceID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.services, serviceID)
	delete(m.commands, serviceID)
}

func (m *Monitor) UpdateHealth(serviceID string, status commonv1.HealthStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()

	health, exists := m.services[serviceID]
	if !exists {
		return
	}

	oldStatus := health.Status
	health.Status = status
	health.LastUpdate = time.Now()

	if status == commonv1.HealthStatus_HEALTH_STATUS_HEALTHY {
		health.LastHealthy = time.Now()
	}

	// Notify subscribers if status changed
	if oldStatus != status {
		m.notifySubscribers(serviceID, oldStatus, status)
	}
}

func (m *Monitor) Subscribe(serviceIDs []string) chan *supervisorv1.ServiceHealthUpdate {
	m.mu.Lock()
	defer m.mu.Unlock()

	ch := make(chan *supervisorv1.ServiceHealthUpdate, 100)
	m.subscribers[ch] = serviceIDs

	return ch
}

func (m *Monitor) Unsubscribe(ch chan *supervisorv1.ServiceHealthUpdate) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.subscribers, ch)
}

func (m *Monitor) GetPendingCommands(serviceID string) []*supervisorv1.ServiceCommand {
	m.mu.Lock()
	defer m.mu.Unlock()

	commands := m.commands[serviceID]
	m.commands[serviceID] = nil

	return commands
}

func (m *Monitor) checkServiceHealth() {
	m.mu.RLock()
	services := make([]*ServiceHealth, 0, len(m.services))
	for _, svc := range m.services {
		services = append(services, svc)
	}
	m.mu.RUnlock()

	now := time.Now()
	for _, svc := range services {
		// Check for heartbeat timeout
		if now.Sub(svc.LastUpdate) > 30*time.Second {
			m.logger.Warnf("Service %s heartbeat timeout", svc.ServiceName)
			m.UpdateHealth(svc.ServiceID, commonv1.HealthStatus_HEALTH_STATUS_UNHEALTHY)
		}
	}
}

func (m *Monitor) notifySubscribers(serviceID string, oldStatus, newStatus commonv1.HealthStatus) {
	update := &supervisorv1.ServiceHealthUpdate{
		ServiceId: serviceID,
		OldStatus: oldStatus,
		NewStatus: newStatus,
		Timestamp: timestamppb.Now(),
	}

	for ch, serviceIDs := range m.subscribers {
		// Check if subscriber is interested in this service
		interested := len(serviceIDs) == 0
		for _, id := range serviceIDs {
			if id == serviceID {
				interested = true
				break
			}
		}

		if interested {
			select {
			case ch <- update:
			default:
				// Channel full, skip
			}
		}
	}
}
