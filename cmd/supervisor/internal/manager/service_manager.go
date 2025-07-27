package manager

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	supervisorv1 "github.com/redbco/redb-open/api/proto/supervisor/v1"
	"github.com/redbco/redb-open/cmd/supervisor/internal/logger"
	"github.com/redbco/redb-open/cmd/supervisor/internal/superconfig"
)

type ServiceInfo struct {
	ID            string
	Name          string
	State         commonv1.ServiceState
	Health        commonv1.HealthStatus
	Process       *ServiceProcess
	Connection    *grpc.ClientConn
	Controller    supervisorv1.ServiceControllerServiceClient
	Info          *commonv1.ServiceInfo
	Capabilities  *supervisorv1.ServiceCapabilities
	LastHeartbeat time.Time
	StartedAt     time.Time
	Metrics       *supervisorv1.ServiceMetrics
}

type ServiceManager struct {
	mu       sync.RWMutex
	services map[string]*ServiceInfo
	logger   logger.LoggerInterface
	config   *superconfig.Config
}

func New(log logger.LoggerInterface, config *superconfig.Config) *ServiceManager {
	return &ServiceManager{
		services: make(map[string]*ServiceInfo),
		logger:   log,
		config:   config,
	}
}

func (m *ServiceManager) RegisterService(ctx context.Context, info *commonv1.ServiceInfo, capabilities *supervisorv1.ServiceCapabilities) (string, *supervisorv1.ServiceConfiguration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Generate service ID
	serviceID := uuid.New().String()

	// Check if service already registered
	for _, svc := range m.services {
		if svc.Name == info.Name && svc.Info.InstanceId == info.InstanceId {
			m.logger.Warnf("Service %s with instance ID %s already registered, updating registration", info.Name, info.InstanceId)
			return svc.ID, nil, nil
		}
	}

	// Connect to service with retry logic
	var conn *grpc.ClientConn
	var err error

	maxRetries := 3
	retryDelay := time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		conn, err = m.connectToService(ctx, info.Host, int(info.Port))
		if err != nil {
			m.logger.Warnf("Failed to connect to service %s (attempt %d/%d): %v", info.Name, attempt, maxRetries, err)
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				retryDelay *= 2
				continue
			}
			return "", nil, fmt.Errorf("failed to connect to service after %d attempts: %w", maxRetries, err)
		}
		break
	}

	// Create service info
	svcInfo := &ServiceInfo{
		ID:            serviceID,
		Name:          info.Name,
		State:         commonv1.ServiceState_SERVICE_STATE_STARTING,
		Health:        commonv1.HealthStatus_HEALTH_STATUS_STARTING,
		Connection:    conn,
		Controller:    supervisorv1.NewServiceControllerServiceClient(conn),
		Info:          info,
		Capabilities:  capabilities,
		LastHeartbeat: time.Now(),
		StartedAt:     time.Now(),
	}

	m.services[serviceID] = svcInfo

	// Get service configuration
	svcConfig, exists := m.config.Services[info.Name]
	if !exists {
		m.logger.Infof("Registered service %s with ID %s (no config found)", info.Name, serviceID)
		return serviceID, nil, nil
	}

	configuration := &supervisorv1.ServiceConfiguration{
		Config:      svcConfig.Config,
		Environment: svcConfig.Environment,
	}

	m.logger.Infof("Registered service %s with ID %s", info.Name, serviceID)
	return serviceID, configuration, nil
}

func (m *ServiceManager) UnregisterService(serviceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	svc, exists := m.services[serviceID]
	if !exists {
		return fmt.Errorf("service not found")
	}

	// Close connection
	if svc.Connection != nil {
		svc.Connection.Close()
	}

	delete(m.services, serviceID)
	m.logger.Infof("Unregistered service %s (ID: %s)", svc.Name, serviceID)

	return nil
}

func (m *ServiceManager) StartService(ctx context.Context, name string, config superconfig.ServiceConfig) error {
	// Check if service is already running
	m.mu.RLock()
	for _, svc := range m.services {
		if svc.Name == name && (svc.State == commonv1.ServiceState_SERVICE_STATE_RUNNING || svc.State == commonv1.ServiceState_SERVICE_STATE_STARTING) {
			m.mu.RUnlock()
			return fmt.Errorf("service %s is already running or starting", name)
		}
	}
	m.mu.RUnlock()

	// Start service process
	process := NewServiceProcess(name, config)
	if err := process.Start(ctx); err != nil {
		return fmt.Errorf("failed to start process: %w", err)
	}

	// Wait for service to register with exponential backoff
	timeout := time.After(60 * time.Second) // Increased timeout
	checkInterval := time.Second
	maxCheckInterval := 5 * time.Second

	for {
		select {
		case <-timeout:
			m.logger.Errorf("Service %s failed to register within timeout, stopping process", name)
			process.Stop(ctx)
			return fmt.Errorf("service failed to register within timeout")
		case <-time.After(checkInterval):
			if m.isServiceRegistered(name) {
				m.logger.Infof("Service %s started successfully", name)
				return nil
			}
			// Exponential backoff for check interval
			if checkInterval < maxCheckInterval {
				checkInterval = time.Duration(float64(checkInterval) * 1.5)
				if checkInterval > maxCheckInterval {
					checkInterval = maxCheckInterval
				}
			}
		}
	}
}

func (m *ServiceManager) StopService(ctx context.Context, serviceID string, force bool, gracePeriod time.Duration) error {
	m.mu.RLock()
	svc, exists := m.services[serviceID]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("service not found")
	}

	// Send stop command to service with a reasonable timeout
	if svc.Controller != nil {
		req := &supervisorv1.StopRequest{
			GracePeriod: durationpb.New(gracePeriod),
			SaveState:   true,
		}

		// Use a shorter timeout for the stop command to avoid blocking
		stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		if _, err := svc.Controller.Stop(stopCtx, req); err != nil {
			// During shutdown, various connection errors are expected if services are already stopping
			if stopCtx.Err() == context.DeadlineExceeded {
				m.logger.Infof("Stop command to %s timed out (service may already be shutting down)", svc.Name)
			} else if strings.Contains(err.Error(), "connection is closing") ||
				strings.Contains(err.Error(), "Canceled") {
				m.logger.Infof("Service %s connection already closing during shutdown", svc.Name)
			} else {
				m.logger.Warnf("Failed to send stop command to %s: %v", svc.Name, err)
			}
			// Don't return error - continue with process termination if needed
		} else {
			m.logger.Infof("Stop command sent successfully to %s", svc.Name)
		}
	}

	// Stop process if exists
	if svc.Process != nil {
		if err := svc.Process.Stop(ctx); err != nil && !force {
			return fmt.Errorf("failed to stop process: %w", err)
		}
	}

	return nil
}

func (m *ServiceManager) StopAllServices(ctx context.Context) error {
	m.mu.RLock()
	services := make([]*ServiceInfo, 0, len(m.services))
	serviceIDs := make([]string, 0, len(m.services))
	for _, svc := range m.services {
		services = append(services, svc)
		serviceIDs = append(serviceIDs, svc.ID)
	}
	m.mu.RUnlock()

	if len(services) == 0 {
		return nil
	}

	m.logger.Infof("Initiating shutdown for %d services...", len(services))

	// Send stop commands to all services in reverse dependency order
	// Note: Some services may unregister themselves before receiving the stop command
	for i := len(services) - 1; i >= 0; i-- {
		svc := services[i]

		// Check if service still exists (it may have unregistered already)
		m.mu.RLock()
		_, stillExists := m.services[svc.ID]
		m.mu.RUnlock()

		if !stillExists {
			m.logger.Infof("Service %s has already unregistered", svc.Name)
			continue
		}

		m.logger.Infof("Stopping service: %s", svc.Name)

		if err := m.StopService(ctx, svc.ID, false, 30*time.Second); err != nil {
			// Don't log as error if service already unregistered (expected during shutdown)
			if err.Error() == "service not found" {
				m.logger.Infof("Service %s unregistered during shutdown", svc.Name)
			} else {
				m.logger.Errorf("Failed to stop %s: %v", svc.Name, err)
			}
		}
	}

	// Wait for services to unregister themselves with a timeout
	unregisterTimeout := 10 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		// Use remaining context time if less than our default
		remaining := time.Until(deadline)
		if remaining < unregisterTimeout {
			unregisterTimeout = remaining
		}
	}

	m.logger.Infof("Waiting up to %v for services to unregister...", unregisterTimeout)
	startTime := time.Now()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(unregisterTimeout):
			m.mu.RLock()
			remaining := len(m.services)
			m.mu.RUnlock()
			if remaining > 0 {
				m.logger.Warnf("Timeout waiting for service unregistration - %d services still registered", remaining)
			}
			return nil
		case <-ticker.C:
			m.mu.RLock()
			remainingServices := len(m.services)
			m.mu.RUnlock()

			if remainingServices == 0 {
				elapsed := time.Since(startTime)
				m.logger.Infof("All services unregistered successfully (took %v)", elapsed)
				return nil
			}

			// Log progress every 2 seconds
			if elapsed := time.Since(startTime); elapsed > 0 && int(elapsed/time.Second)%2 == 0 {
				m.logger.Infof("Still waiting for %d services to unregister...", remainingServices)
			}
		}
	}
}

func (m *ServiceManager) UpdateHeartbeat(serviceID string, health commonv1.HealthStatus, metrics *supervisorv1.ServiceMetrics) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	svc, exists := m.services[serviceID]
	if !exists {
		return fmt.Errorf("service not found")
	}

	svc.LastHeartbeat = time.Now()
	svc.Health = health
	svc.Metrics = metrics

	// If service is healthy and currently starting, transition to running
	if health == commonv1.HealthStatus_HEALTH_STATUS_HEALTHY &&
		svc.State == commonv1.ServiceState_SERVICE_STATE_STARTING {
		svc.State = commonv1.ServiceState_SERVICE_STATE_RUNNING
		m.logger.Infof("Service %s transitioned to RUNNING state", svc.Name)
	}

	return nil
}

func (m *ServiceManager) GetServiceStatus(serviceID string) (*supervisorv1.ServiceStatus, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	svc, exists := m.services[serviceID]
	if !exists {
		return nil, fmt.Errorf("service not found")
	}

	return &supervisorv1.ServiceStatus{
		Info:          svc.Info,
		State:         svc.State,
		Health:        svc.Health,
		StartedAt:     timestamppb.New(svc.StartedAt),
		LastHeartbeat: timestamppb.New(svc.LastHeartbeat),
		Metrics:       svc.Metrics,
	}, nil
}

func (m *ServiceManager) ListServices(stateFilter commonv1.ServiceState, namePattern string) []*supervisorv1.ServiceStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []*supervisorv1.ServiceStatus

	for _, svc := range m.services {
		// Apply filters
		if stateFilter != commonv1.ServiceState_SERVICE_STATE_UNSPECIFIED && svc.State != stateFilter {
			continue
		}

		if namePattern != "" && !matchPattern(svc.Name, namePattern) {
			continue
		}

		status := &supervisorv1.ServiceStatus{
			Info:          svc.Info,
			State:         svc.State,
			Health:        svc.Health,
			StartedAt:     timestamppb.New(svc.StartedAt),
			LastHeartbeat: timestamppb.New(svc.LastHeartbeat),
			Metrics:       svc.Metrics,
		}

		results = append(results, status)
	}

	return results
}

func (m *ServiceManager) IsServiceHealthy(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, svc := range m.services {
		if svc.Name == name {
			return svc.Health == commonv1.HealthStatus_HEALTH_STATUS_HEALTHY
		}
	}

	return false
}

func (m *ServiceManager) GetService(serviceID string) (*ServiceInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	svc, exists := m.services[serviceID]
	return svc, exists
}

func (m *ServiceManager) connectToService(ctx context.Context, host string, port int) (*grpc.ClientConn, error) {
	addr := fmt.Sprintf("%s:%d", host, port)

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithBlock())

	// Add keepalive options for more robust connections
	opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                10 * time.Second,
		Timeout:             3 * time.Second,
		PermitWithoutStream: true,
	}))

	// Use a more reasonable timeout for service connections
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	m.logger.Infof("Attempting to connect to service at %s", addr)
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
	}

	m.logger.Infof("Successfully connected to service at %s", addr)
	return conn, nil
}

func (m *ServiceManager) isServiceRegistered(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, svc := range m.services {
		if svc.Name == name {
			return true
		}
	}

	return false
}

func matchPattern(name, pattern string) bool {
	// Simple pattern matching, can be enhanced
	return name == pattern || pattern == "*"
}

// SystemReadyCallback defines the function signature for system ready callbacks
type SystemReadyCallback func()

// AreAllConfiguredServicesHealthy checks if all enabled and required services are running and healthy
func (m *ServiceManager) AreAllConfiguredServicesHealthy() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for serviceName, serviceConfig := range m.config.Services {
		if !serviceConfig.Enabled {
			continue
		}

		// Find the service by name
		var found bool
		var isHealthy bool
		for _, svc := range m.services {
			if svc.Name == serviceName {
				found = true
				// Accept both HEALTHY and DEGRADED services as operational
				// DEGRADED means some health checks failed but the service is still functional
				isHealthy = (svc.Health == commonv1.HealthStatus_HEALTH_STATUS_HEALTHY ||
					svc.Health == commonv1.HealthStatus_HEALTH_STATUS_DEGRADED) &&
					svc.State == commonv1.ServiceState_SERVICE_STATE_RUNNING
				break
			}
		}

		// If service is required and not found or not healthy, return false
		if serviceConfig.Required && (!found || !isHealthy) {
			return false
		}

		// If service is enabled but not required, we still want it to be healthy if it exists
		if found && !isHealthy {
			return false
		}
	}

	return true
}

// GetConfiguredServiceStatus returns status information for all configured services
func (m *ServiceManager) GetConfiguredServiceStatus() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := make(map[string]string)

	for serviceName, serviceConfig := range m.config.Services {
		if !serviceConfig.Enabled {
			status[serviceName] = "disabled"
			continue
		}

		// Find the service by name
		var found bool
		var svcStatus string
		for _, svc := range m.services {
			if svc.Name == serviceName {
				found = true
				if (svc.Health == commonv1.HealthStatus_HEALTH_STATUS_HEALTHY ||
					svc.Health == commonv1.HealthStatus_HEALTH_STATUS_DEGRADED) &&
					svc.State == commonv1.ServiceState_SERVICE_STATE_RUNNING {
					if svc.Health == commonv1.HealthStatus_HEALTH_STATUS_HEALTHY {
						svcStatus = "healthy"
					} else {
						svcStatus = "degraded but operational"
					}
				} else {
					svcStatus = fmt.Sprintf("unhealthy (state: %s, health: %s)",
						svc.State.String(), svc.Health.String())
				}
				break
			}
		}

		if !found {
			if serviceConfig.Required {
				svcStatus = "not started (required)"
			} else {
				svcStatus = "not started (optional)"
			}
		}

		status[serviceName] = svcStatus
	}

	return status
}
