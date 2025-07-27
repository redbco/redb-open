package engine

import (
	"context"
	"fmt"

	webhookv1 "github.com/redbco/redb-open/api/proto/webhook/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// WebhookServer implements the WebhookService gRPC interface
type WebhookServer struct {
	webhookv1.UnimplementedWebhookServiceServer
	engine *Engine
}

// NewWebhookServer creates a new webhook gRPC server
func NewWebhookServer(engine *Engine) *WebhookServer {
	return &WebhookServer{
		engine: engine,
	}
}

// SendWebhook handles individual webhook delivery requests
func (s *WebhookServer) SendWebhook(ctx context.Context, req *webhookv1.SendWebhookRequest) (*webhookv1.SendWebhookResponse, error) {
	// Validate request
	if req.Url == "" {
		return nil, fmt.Errorf("URL is required")
	}
	if req.Method == "" {
		req.Method = "POST" // Default to POST
	}

	// Set default values if not provided
	if req.TimeoutSeconds <= 0 {
		req.TimeoutSeconds = 30 // Default 30 seconds
	}
	if req.MaxRetries < 0 {
		req.MaxRetries = 0 // No retries by default
	}
	if req.RetryDelaySeconds <= 0 {
		req.RetryDelaySeconds = 5 // Default 5 seconds retry delay
	}

	// Delegate to engine
	return s.engine.SendWebhook(ctx, req)
}

// SendWebhookBatch handles batch webhook delivery requests
func (s *WebhookServer) SendWebhookBatch(ctx context.Context, req *webhookv1.SendWebhookBatchRequest) (*webhookv1.SendWebhookBatchResponse, error) {
	if len(req.Webhooks) == 0 {
		return &webhookv1.SendWebhookBatchResponse{
			Results:      []*webhookv1.SendWebhookResponse{},
			SuccessCount: 0,
			FailureCount: 0,
		}, nil
	}

	results := make([]*webhookv1.SendWebhookResponse, len(req.Webhooks))
	var successCount, failureCount int32

	// Process each webhook in the batch
	for i, webhook := range req.Webhooks {
		result, err := s.SendWebhook(ctx, webhook)
		if err != nil {
			// Create error response
			results[i] = &webhookv1.SendWebhookResponse{
				Success:      false,
				ErrorMessage: err.Error(),
				SentAt:       timestamppb.Now(),
				Attempts:     1,
			}
			failureCount++

			// If fail_fast is enabled, stop processing
			if req.FailFast {
				// Fill remaining results with error
				for j := i + 1; j < len(req.Webhooks); j++ {
					results[j] = &webhookv1.SendWebhookResponse{
						Success:      false,
						ErrorMessage: "batch processing stopped due to previous failure",
						SentAt:       timestamppb.Now(),
						Attempts:     0,
					}
					failureCount++
				}
				break
			}
		} else {
			results[i] = result
			if result.Success {
				successCount++
			} else {
				failureCount++
			}
		}
	}

	return &webhookv1.SendWebhookBatchResponse{
		Results:      results,
		SuccessCount: successCount,
		FailureCount: failureCount,
	}, nil
}

// GetWebhookStatus returns the status of a tracked webhook
func (s *WebhookServer) GetWebhookStatus(ctx context.Context, req *webhookv1.GetWebhookStatusRequest) (*webhookv1.GetWebhookStatusResponse, error) {
	if req.WebhookId == "" {
		return nil, fmt.Errorf("webhook_id is required")
	}

	delivery, exists := s.engine.GetWebhookStatus(req.WebhookId)
	if !exists {
		return nil, fmt.Errorf("webhook with ID %s not found", req.WebhookId)
	}

	response := &webhookv1.GetWebhookStatusResponse{
		WebhookId:      delivery.ID,
		Status:         delivery.Status,
		Attempts:       delivery.Attempts,
		MaxRetries:     delivery.MaxRetries,
		LastAttemptAt:  timestamppb.New(delivery.LastAttempt),
		LastError:      delivery.LastError,
		LastStatusCode: delivery.LastStatusCode,
	}

	if !delivery.NextRetry.IsZero() {
		response.NextRetryAt = timestamppb.New(delivery.NextRetry)
	}

	return response, nil
}
