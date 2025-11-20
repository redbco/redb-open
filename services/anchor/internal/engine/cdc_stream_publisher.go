package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	streamv1 "github.com/redbco/redb-open/api/proto/stream/v1"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// CDCStreamPublisher publishes CDC events to stream platforms (Kafka, Kinesis, etc.)
type CDCStreamPublisher struct {
	sourceAdapter   adapter.Connection
	streamClient    streamv1.StreamServiceClient
	integrationName string
	topicName       string
	logger          *logger.Logger
	stats           *adapter.CDCStatistics
	mappingRules    []adapter.TransformationRule
}

// NewCDCStreamPublisher creates a new CDC to stream publisher
func NewCDCStreamPublisher(
	sourceAdapter adapter.Connection,
	streamServiceEndpoint string,
	integrationName string,
	topicName string,
	mappingRulesJSON []byte,
	logger *logger.Logger,
) (*CDCStreamPublisher, error) {
	// Connect to stream service
	conn, err := grpc.Dial(streamServiceEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to stream service: %w", err)
	}

	publisher := &CDCStreamPublisher{
		sourceAdapter:   sourceAdapter,
		streamClient:    streamv1.NewStreamServiceClient(conn),
		integrationName: integrationName,
		topicName:       topicName,
		logger:          logger,
		stats:           adapter.NewCDCStatistics(),
	}

	// Parse mapping rules if provided
	if len(mappingRulesJSON) > 0 {
		var rules []adapter.TransformationRule
		if err := json.Unmarshal(mappingRulesJSON, &rules); err != nil {
			return nil, fmt.Errorf("failed to parse mapping rules: %w", err)
		}
		publisher.mappingRules = rules
	}

	return publisher, nil
}

// PublishEvent publishes a CDC event to a stream
func (p *CDCStreamPublisher) PublishEvent(ctx context.Context, rawEvent map[string]interface{}) error {
	startTime := time.Now()

	// Parse CDC event using source adapter
	event, err := p.sourceAdapter.ReplicationOperations().ParseEvent(ctx, rawEvent)
	if err != nil {
		p.stats.RecordFailure()
		if p.logger != nil {
			p.logger.Errorf("Failed to parse CDC event: %v", err)
		}
		return fmt.Errorf("parse event failed: %w", err)
	}

	// Apply transformations if mapping rules exist
	if len(p.mappingRules) > 0 {
		transformedData, err := p.applyTransformations(ctx, event.Data)
		if err != nil {
			p.stats.RecordFailure()
			if p.logger != nil {
				p.logger.Errorf("Failed to apply transformations: %v", err)
			}
			return fmt.Errorf("transformation failed: %w", err)
		}
		event.Data = transformedData

		// Transform old data for UPDATE/DELETE operations
		if len(event.OldData) > 0 {
			transformedOldData, err := p.applyTransformations(ctx, event.OldData)
			if err != nil {
				if p.logger != nil {
					p.logger.Warnf("Failed to transform old_data: %v", err)
				}
			} else {
				event.OldData = transformedOldData
			}
		}
	}

	// Convert CDC event to stream message format
	messageBytes, partitionKey, headers, err := p.convertCDCEventToStreamMessage(event)
	if err != nil {
		p.stats.RecordFailure()
		if p.logger != nil {
			p.logger.Errorf("Failed to convert CDC event to stream message: %v", err)
		}
		return fmt.Errorf("conversion failed: %w", err)
	}

	// Publish to stream using ProduceMessages
	produceReq := &streamv1.ProduceMessagesRequest{
		TenantId:  "", // Will be set by service
		StreamId:  p.integrationName,
		TopicName: p.topicName,
		Messages: []*streamv1.StreamMessage{
			{
				Key:     []byte(partitionKey),
				Value:   messageBytes,
				Headers: headers,
			},
		},
	}

	resp, err := p.streamClient.ProduceMessages(ctx, produceReq)
	if err != nil {
		p.stats.RecordFailure()
		if p.logger != nil {
			p.logger.Errorf("Failed to publish message to stream: %v", err)
		}
		return fmt.Errorf("publish failed: %w", err)
	}

	// Record success
	latency := time.Since(startTime)
	p.stats.RecordEvent(event, latency)

	if p.logger != nil {
		p.logger.Infof("Published CDC event to stream %s/%s: operation=%s, table=%s, count=%d",
			p.integrationName, p.topicName, event.Operation, event.TableName, resp.MessagesProduced)
	}

	return nil
}

// convertCDCEventToStreamMessage converts a CDC event to stream message format
func (p *CDCStreamPublisher) convertCDCEventToStreamMessage(event *adapter.CDCEvent) ([]byte, string, map[string]string, error) {
	// Build message payload with CDC event structure
	payload := map[string]interface{}{
		"operation":  string(event.Operation),
		"table_name": event.TableName,
		"timestamp":  event.Timestamp.Unix(),
		"data":       event.Data,
	}

	// Add old data for UPDATE/DELETE
	if len(event.OldData) > 0 {
		payload["old_data"] = event.OldData
	}

	// Add schema name if available
	if event.SchemaName != "" {
		payload["schema_name"] = event.SchemaName
	}

	// Serialize payload
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, "", nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Generate partition key from primary key or use table name
	partitionKey := event.TableName
	if event.TransactionID != "" {
		partitionKey = event.TransactionID
	}

	// Build headers
	headers := p.buildMessageHeaders(event)

	return payloadBytes, partitionKey, headers, nil
}

// buildMessageHeaders creates metadata headers for the stream message
func (p *CDCStreamPublisher) buildMessageHeaders(event *adapter.CDCEvent) map[string]string {
	headers := map[string]string{
		"cdc.operation":   string(event.Operation),
		"cdc.table":       event.TableName,
		"cdc.timestamp":   fmt.Sprintf("%d", event.Timestamp.Unix()),
		"cdc.source_type": "redb-cdc",
	}

	if event.SchemaName != "" {
		headers["cdc.schema"] = event.SchemaName
	}

	if event.TransactionID != "" {
		headers["cdc.transaction_id"] = event.TransactionID
	}

	return headers
}

// applyTransformations applies mapping rules to transform data
func (p *CDCStreamPublisher) applyTransformations(ctx context.Context, data map[string]interface{}) (map[string]interface{}, error) {
	if len(p.mappingRules) == 0 {
		return data, nil
	}

	result := make(map[string]interface{})

	for _, rule := range p.mappingRules {
		sourceValue, exists := data[rule.SourceColumn]
		if !exists {
			// Skip if source column doesn't exist in this event
			continue
		}

		// Apply transformation based on type
		transformedValue, err := p.applyTransformation(rule.TransformationType, sourceValue)
		if err != nil {
			if p.logger != nil {
				p.logger.Warnf("Failed to apply transformation %s: %v", rule.TransformationType, err)
			}
			// Use original value if transformation fails
			transformedValue = sourceValue
		}

		result[rule.TargetColumn] = transformedValue
	}

	// Include unmapped columns from source (pass-through)
	for key, value := range data {
		if _, exists := result[key]; !exists {
			result[key] = value
		}
	}

	return result, nil
}

// applyTransformation applies a single transformation
func (p *CDCStreamPublisher) applyTransformation(transformationType string, value interface{}) (interface{}, error) {
	switch transformationType {
	case "direct_mapping", "":
		return value, nil
	case "uppercase":
		if str, ok := value.(string); ok {
			return strings.ToUpper(str), nil
		}
	case "lowercase":
		if str, ok := value.(string); ok {
			return strings.ToLower(str), nil
		}
	case "trim":
		if str, ok := value.(string); ok {
			return strings.TrimSpace(str), nil
		}
	default:
		if p.logger != nil {
			p.logger.Warnf("Unknown transformation type: %s", transformationType)
		}
	}

	return value, nil
}

// GetStatistics returns CDC statistics
func (p *CDCStreamPublisher) GetStatistics() *adapter.CDCStatistics {
	return p.stats
}

// Close closes the stream publisher
func (p *CDCStreamPublisher) Close() error {
	// Cleanup resources if needed
	return nil
}
