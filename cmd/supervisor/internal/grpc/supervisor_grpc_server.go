package grpc

import (
	"context"
	"fmt"
	"io"
	"time"

	supervisorv1 "github.com/redbco/redb-open/api/proto/supervisor/v1"
	"github.com/redbco/redb-open/cmd/supervisor/internal/health"
	"github.com/redbco/redb-open/cmd/supervisor/internal/logger"
	"github.com/redbco/redb-open/cmd/supervisor/internal/manager"
)

type SupervisorServer struct {
	supervisorv1.UnimplementedSupervisorServiceServer

	serviceManager *manager.ServiceManager
	healthMonitor  *health.Monitor
	logStore       *logger.Store
	logger         logger.LoggerInterface
}

func NewSupervisorServer(
	serviceManager *manager.ServiceManager,
	healthMonitor *health.Monitor,
	logStore *logger.Store,
	log logger.LoggerInterface,
) *SupervisorServer {
	return &SupervisorServer{
		serviceManager: serviceManager,
		healthMonitor:  healthMonitor,
		logStore:       logStore,
		logger:         log,
	}
}

func (s *SupervisorServer) RegisterService(ctx context.Context, req *supervisorv1.RegisterServiceRequest) (*supervisorv1.RegisterServiceResponse, error) {
	s.logger.Infof("Registration request from %s", req.Service.Name)

	serviceID, config, err := s.serviceManager.RegisterService(ctx, req.Service, req.Capabilities)
	if err != nil {
		return &supervisorv1.RegisterServiceResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	// Add to health monitor
	s.healthMonitor.AddService(serviceID, req.Service.Name)

	return &supervisorv1.RegisterServiceResponse{
		Success:       true,
		Message:       "Service registered successfully",
		ServiceId:     serviceID,
		InitialConfig: config,
	}, nil
}

func (s *SupervisorServer) UnregisterService(ctx context.Context, req *supervisorv1.UnregisterServiceRequest) (*supervisorv1.UnregisterServiceResponse, error) {
	s.logger.Infof("Unregistration request for service %s: %s", req.ServiceId, req.Reason)

	if err := s.serviceManager.UnregisterService(req.ServiceId); err != nil {
		return &supervisorv1.UnregisterServiceResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	// Remove from health monitor
	s.healthMonitor.RemoveService(req.ServiceId)

	return &supervisorv1.UnregisterServiceResponse{
		Success: true,
		Message: "Service unregistered successfully",
	}, nil
}

func (s *SupervisorServer) StartService(ctx context.Context, req *supervisorv1.StartServiceRequest) (*supervisorv1.StartServiceResponse, error) {
	// Implementation depends on whether we're starting a new process
	// or sending a start command to an existing service
	return &supervisorv1.StartServiceResponse{
		Success: false,
		Message: "Not implemented",
	}, nil
}

func (s *SupervisorServer) StopService(ctx context.Context, req *supervisorv1.StopServiceRequest) (*supervisorv1.StopServiceResponse, error) {
	gracePeriod := 30 * time.Second
	if req.GracePeriod != nil {
		gracePeriod = req.GracePeriod.AsDuration()
	}

	if err := s.serviceManager.StopService(ctx, req.ServiceId, req.Force, gracePeriod); err != nil {
		return &supervisorv1.StopServiceResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &supervisorv1.StopServiceResponse{
		Success: true,
		Message: "Service stop initiated",
	}, nil
}

func (s *SupervisorServer) GetServiceStatus(ctx context.Context, req *supervisorv1.GetServiceStatusRequest) (*supervisorv1.GetServiceStatusResponse, error) {
	status, err := s.serviceManager.GetServiceStatus(req.ServiceId)
	if err != nil {
		return nil, err
	}

	return &supervisorv1.GetServiceStatusResponse{
		Status: status,
	}, nil
}

func (s *SupervisorServer) ListServices(ctx context.Context, req *supervisorv1.ListServicesRequest) (*supervisorv1.ListServicesResponse, error) {
	services := s.serviceManager.ListServices(req.StateFilter, req.NamePattern)

	return &supervisorv1.ListServicesResponse{
		Services: services,
	}, nil
}

func (s *SupervisorServer) StreamLogs(stream supervisorv1.SupervisorService_StreamLogsServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&supervisorv1.LogStreamResponse{
				Acknowledged: true,
			})
		}
		if err != nil {
			return err
		}

		// Store log entry
		s.logStore.Store(req.Entry)

		// Log microservice entry to console and file if logger supports it
		if unifiedLogger, ok := s.logger.(logger.UnifiedLoggerInterface); ok {
			unifiedLogger.LogMicroserviceEntry(req.Entry)
		}

		// Could send individual acknowledgments if needed
	}
}

func (s *SupervisorServer) SendHeartbeat(ctx context.Context, req *supervisorv1.HeartbeatRequest) (*supervisorv1.HeartbeatResponse, error) {
	// Update service heartbeat
	if err := s.serviceManager.UpdateHeartbeat(req.ServiceId, req.HealthStatus, req.Metrics); err != nil {
		return nil, err
	}

	// Update health monitor
	s.healthMonitor.UpdateHealth(req.ServiceId, req.HealthStatus)

	// Get any pending commands or config updates
	_, exists := s.serviceManager.GetService(req.ServiceId)
	if !exists {
		return nil, fmt.Errorf("service not found")
	}

	resp := &supervisorv1.HeartbeatResponse{
		Acknowledged: true,
	}

	// Check for configuration updates
	// This would be implemented based on your config management strategy

	// Check for pending commands
	commands := s.healthMonitor.GetPendingCommands(req.ServiceId)
	resp.Commands = commands

	return resp, nil
}

func (s *SupervisorServer) WatchServiceHealth(req *supervisorv1.WatchServiceHealthRequest, stream supervisorv1.SupervisorService_WatchServiceHealthServer) error {
	// Subscribe to health updates
	updates := s.healthMonitor.Subscribe(req.ServiceIds)
	defer s.healthMonitor.Unsubscribe(updates)

	for {
		select {
		case update := <-updates:
			if err := stream.Send(update); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}
