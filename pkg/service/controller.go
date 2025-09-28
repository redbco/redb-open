package service

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	supervisorv1 "github.com/redbco/redb-open/api/proto/supervisor/v1"
)

// ControllerServer implements the ServiceControllerService
type ControllerServer struct {
	supervisorv1.UnimplementedServiceControllerServiceServer
	service *BaseService
}

func NewControllerServer(service *BaseService) *ControllerServer {
	return &ControllerServer{
		service: service,
	}
}

func (s *ControllerServer) Start(ctx context.Context, req *supervisorv1.StartRequest) (*supervisorv1.StartResponse, error) {
	s.service.Logger.Info("Received start command")

	s.service.setState(commonv1.ServiceState_SERVICE_STATE_STARTING)

	// Apply configuration
	if req.Config != nil {
		s.service.Config.Update(req.Config.Config)
	}

	// Initialize and start the service implementation
	if err := s.service.impl.Initialize(ctx, s.service.Config); err != nil {
		return &supervisorv1.StartResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	if err := s.service.impl.Start(ctx); err != nil {
		return &supervisorv1.StartResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	s.service.setState(commonv1.ServiceState_SERVICE_STATE_RUNNING)

	return &supervisorv1.StartResponse{
		Success: true,
		Message: "Service started successfully",
	}, nil
}

func (s *ControllerServer) Stop(ctx context.Context, req *supervisorv1.StopRequest) (*supervisorv1.StopResponse, error) {
	s.service.Logger.Info("Controller received stop command")

	gracePeriod := 30 * time.Second
	if req.GracePeriod != nil {
		gracePeriod = req.GracePeriod.AsDuration()
	}

	s.service.Logger.Debug("Signaling stop to service")
	// Signal stop - use sync.Once to prevent panic from double-closing
	s.service.stopOnce.Do(func() {
		close(s.service.stopCh)
	})
	s.service.Logger.Debug("Stop signal sent")

	// Wait for graceful shutdown or timeout
	s.service.Logger.Debug("Waiting for service shutdown")
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-s.service.stoppedCh:
			// Service stopped normally
		case <-ctx.Done():
			// Context cancelled (e.g., supervisor shutting down)
			s.service.Logger.Debug("Controller context cancelled during shutdown wait")
		}
	}()

	select {
	case <-done:
		s.service.Logger.Info("Service shutdown completed")
		return &supervisorv1.StopResponse{
			Success: true,
			Message: "Service stopped successfully",
		}, nil
	case <-ctx.Done():
		s.service.Logger.Info("Controller context cancelled")
		return &supervisorv1.StopResponse{
			Success: true,
			Message: "Service shutdown cancelled by supervisor",
		}, nil
	case <-time.After(gracePeriod):
		s.service.Logger.Errorf("Service shutdown timed out after %v", gracePeriod)
		return &supervisorv1.StopResponse{
			Success: false,
			Message: "Shutdown timeout exceeded",
		}, nil
	}
}

func (s *ControllerServer) GetHealth(ctx context.Context, req *supervisorv1.GetHealthRequest) (*supervisorv1.GetHealthResponse, error) {
	checks := s.service.HealthChecker.GetAllChecks()

	var healthChecks []*supervisorv1.HealthCheck
	for _, check := range checks {
		healthChecks = append(healthChecks, &supervisorv1.HealthCheck{
			Name:        check.Name,
			Status:      check.Status,
			Message:     check.Message,
			LastChecked: timestamppb.New(check.LastChecked),
		})
	}

	return &supervisorv1.GetHealthResponse{
		Status:      s.service.HealthChecker.GetOverallStatus(),
		Checks:      healthChecks,
		LastHealthy: timestamppb.New(s.service.HealthChecker.GetLastHealthyTime()),
	}, nil
}

func (s *ControllerServer) Configure(ctx context.Context, req *supervisorv1.ConfigureRequest) (*supervisorv1.ConfigureResponse, error) {
	s.service.Logger.Info("Received configure command")

	if req.Config != nil {
		oldConfig := s.service.Config.GetAll()
		s.service.Config.Update(req.Config.Config)

		// Check if restart is required
		restartRequired := s.service.Config.RequiresRestart(oldConfig)

		if req.RestartRequired || restartRequired {
			// Trigger restart
			return &supervisorv1.ConfigureResponse{
				Success:    true,
				Message:    "Configuration updated, restart required",
				Restarting: true,
			}, nil
		}
	}

	return &supervisorv1.ConfigureResponse{
		Success:    true,
		Message:    "Configuration updated successfully",
		Restarting: false,
	}, nil
}
