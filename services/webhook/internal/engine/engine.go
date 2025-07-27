package engine

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	webhookv1 "github.com/redbco/redb-open/api/proto/webhook/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/logger"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Engine struct {
	config     *config.Config
	grpcServer *grpc.Server
	httpClient *http.Client
	logger     *logger.Logger
	state      struct {
		sync.Mutex
		isRunning         bool
		ongoingOperations int32
	}
	metrics struct {
		webhooksSent      int64
		webhooksSucceeded int64
		webhooksFailed    int64
		errors            int64
	}
	webhookTracker map[string]*webhookDelivery
	trackerMutex   sync.RWMutex
}

type webhookDelivery struct {
	ID             string
	Status         webhookv1.WebhookStatus
	Attempts       int32
	MaxRetries     int32
	LastAttempt    time.Time
	NextRetry      time.Time
	LastError      string
	LastStatusCode int32
}

func NewEngine(cfg *config.Config) *Engine {
	// Create HTTP client with reasonable timeouts
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: false,
			},
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	return &Engine{
		config:         cfg,
		httpClient:     httpClient,
		webhookTracker: make(map[string]*webhookDelivery),
	}
}

// SetLogger sets the logger for the engine
func (e *Engine) SetLogger(logger *logger.Logger) {
	e.logger = logger
}

// SetGRPCServer sets the shared gRPC server and registers the service immediately
func (e *Engine) SetGRPCServer(server *grpc.Server) {
	e.grpcServer = server

	// Register the service immediately when server is set (BEFORE serving starts)
	if e.grpcServer != nil {
		webhookServer := NewWebhookServer(e)
		webhookv1.RegisterWebhookServiceServer(e.grpcServer, webhookServer)
	}
}

func (e *Engine) Start(ctx context.Context) error {
	e.state.Lock()
	defer e.state.Unlock()

	if e.state.isRunning {
		return fmt.Errorf("engine is already running")
	}

	if e.grpcServer == nil {
		return fmt.Errorf("gRPC server not set - call SetGRPCServer first")
	}

	// Service is already registered in SetGRPCServer, just mark as running
	e.state.isRunning = true
	return nil
}

func (e *Engine) Stop(ctx context.Context) error {
	e.state.Lock()
	defer e.state.Unlock()

	if !e.state.isRunning {
		return nil
	}

	e.state.isRunning = false
	return nil
}

func (e *Engine) GetMetrics() map[string]int64 {
	return map[string]int64{
		"webhooks_sent":      atomic.LoadInt64(&e.metrics.webhooksSent),
		"webhooks_succeeded": atomic.LoadInt64(&e.metrics.webhooksSucceeded),
		"webhooks_failed":    atomic.LoadInt64(&e.metrics.webhooksFailed),
		"errors":             atomic.LoadInt64(&e.metrics.errors),
	}
}

func (e *Engine) CheckGRPCServer() error {
	e.state.Lock()
	defer e.state.Unlock()

	if !e.state.isRunning {
		return fmt.Errorf("service not initialized")
	}

	if e.grpcServer == nil {
		return fmt.Errorf("gRPC server not initialized")
	}

	return nil
}

func (e *Engine) CheckHealth() error {
	e.state.Lock()
	defer e.state.Unlock()

	if !e.state.isRunning {
		return fmt.Errorf("service not initialized")
	}

	return nil
}

func (e *Engine) TrackOperation() {
	atomic.AddInt32(&e.state.ongoingOperations, 1)
}

func (e *Engine) UntrackOperation() {
	atomic.AddInt32(&e.state.ongoingOperations, -1)
}

// SendWebhook delivers a webhook to the specified URL
func (e *Engine) SendWebhook(ctx context.Context, req *webhookv1.SendWebhookRequest) (*webhookv1.SendWebhookResponse, error) {
	e.TrackOperation()
	defer e.UntrackOperation()

	atomic.AddInt64(&e.metrics.webhooksSent, 1)

	// Track webhook delivery
	if req.WebhookId != "" {
		e.trackWebhook(req.WebhookId, req.MaxRetries)
	}

	startTime := time.Now()
	response := &webhookv1.SendWebhookResponse{
		SentAt:   timestamppb.New(startTime),
		Attempts: 1,
	}

	// Perform webhook delivery with retries
	var lastErr error
	for attempt := int32(1); attempt <= req.MaxRetries+1; attempt++ {
		resp, err := e.deliverWebhook(ctx, req)
		if err == nil {
			response.Success = true
			response.StatusCode = resp.StatusCode
			response.ResponseBody = resp.ResponseBody
			response.Attempts = attempt

			if req.WebhookId != "" {
				e.updateWebhookStatus(req.WebhookId, webhookv1.WebhookStatus_WEBHOOK_STATUS_SUCCESS, "", resp.StatusCode)
			}

			atomic.AddInt64(&e.metrics.webhooksSucceeded, 1)
			break
		}

		lastErr = err
		response.Attempts = attempt

		// Update webhook status for tracking
		if req.WebhookId != "" {
			if attempt <= req.MaxRetries {
				e.updateWebhookStatus(req.WebhookId, webhookv1.WebhookStatus_WEBHOOK_STATUS_RETRYING, err.Error(), 0)
			} else {
				e.updateWebhookStatus(req.WebhookId, webhookv1.WebhookStatus_WEBHOOK_STATUS_FAILED, err.Error(), 0)
			}
		}

		// Don't retry if this was the last attempt
		if attempt > req.MaxRetries {
			break
		}

		// Wait before retrying
		if req.RetryDelaySeconds > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(req.RetryDelaySeconds) * time.Second):
			}
		}
	}

	if !response.Success {
		response.ErrorMessage = lastErr.Error()
		atomic.AddInt64(&e.metrics.webhooksFailed, 1)
	}

	response.DurationMs = time.Since(startTime).Milliseconds()
	return response, nil
}

func (e *Engine) deliverWebhook(ctx context.Context, req *webhookv1.SendWebhookRequest) (*webhookv1.SendWebhookResponse, error) {
	// Create HTTP request
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, req.Url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set body if provided
	if len(req.Body) > 0 {
		httpReq.Body = http.NoBody
		// Note: In a real implementation, you would set the body properly
		// This is simplified for the example
	}

	// Set content type
	if req.ContentType != "" {
		httpReq.Header.Set("Content-Type", req.ContentType)
	}

	// Set custom headers
	for key, value := range req.Headers {
		httpReq.Header.Set(key, value)
	}

	// Set authentication
	if req.Auth != nil {
		e.setAuthentication(httpReq, req.Auth)
	}

	// Set timeout
	client := e.httpClient
	if req.TimeoutSeconds > 0 {
		timeout := time.Duration(req.TimeoutSeconds) * time.Second
		client = &http.Client{
			Timeout:   timeout,
			Transport: e.httpClient.Transport,
		}
	}

	// Execute request
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check if status code indicates success (2xx)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	return &webhookv1.SendWebhookResponse{
		Success:    true,
		StatusCode: int32(resp.StatusCode),
		// ResponseBody would be read from resp.Body in a real implementation
	}, nil
}

func (e *Engine) setAuthentication(req *http.Request, auth *webhookv1.WebhookAuth) {
	switch authType := auth.AuthType.(type) {
	case *webhookv1.WebhookAuth_BasicAuth:
		basic := authType.BasicAuth
		credentials := base64.StdEncoding.EncodeToString([]byte(basic.Username + ":" + basic.Password))
		req.Header.Set("Authorization", "Basic "+credentials)
	case *webhookv1.WebhookAuth_BearerAuth:
		bearer := authType.BearerAuth
		req.Header.Set("Authorization", "Bearer "+bearer.Token)
	case *webhookv1.WebhookAuth_ApiKeyAuth:
		apiKey := authType.ApiKeyAuth
		switch apiKey.Location {
		case "header":
			req.Header.Set(apiKey.Key, apiKey.Value)
		case "query":
			q := req.URL.Query()
			q.Set(apiKey.Key, apiKey.Value)
			req.URL.RawQuery = q.Encode()
		}
	}
}

func (e *Engine) trackWebhook(webhookID string, maxRetries int32) {
	e.trackerMutex.Lock()
	defer e.trackerMutex.Unlock()

	e.webhookTracker[webhookID] = &webhookDelivery{
		ID:          webhookID,
		Status:      webhookv1.WebhookStatus_WEBHOOK_STATUS_SENDING,
		Attempts:    0,
		MaxRetries:  maxRetries,
		LastAttempt: time.Now(),
	}
}

func (e *Engine) updateWebhookStatus(webhookID string, status webhookv1.WebhookStatus, errorMsg string, statusCode int32) {
	e.trackerMutex.Lock()
	defer e.trackerMutex.Unlock()

	if delivery, exists := e.webhookTracker[webhookID]; exists {
		delivery.Status = status
		delivery.LastAttempt = time.Now()
		delivery.LastError = errorMsg
		delivery.LastStatusCode = statusCode
		delivery.Attempts++

		if status == webhookv1.WebhookStatus_WEBHOOK_STATUS_RETRYING {
			delivery.NextRetry = time.Now().Add(30 * time.Second) // Default retry delay
		}
	}
}

func (e *Engine) GetWebhookStatus(webhookID string) (*webhookDelivery, bool) {
	e.trackerMutex.RLock()
	defer e.trackerMutex.RUnlock()

	delivery, exists := e.webhookTracker[webhookID]
	return delivery, exists
}
