package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	streamv1 "github.com/redbco/redb-open/api/proto/stream/v1"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// StreamDatabaseConsumer consumes messages from streams and writes them to databases
type StreamDatabaseConsumer struct {
	streamClient    streamv1.StreamServiceClient
	targetAdapter   adapter.Connection
	integrationName string
	topicName       string
	targetTable     string
	logger          *logger.Logger
	stats           *ConsumerStatistics
	mappingRules    []adapter.TransformationRule
	running         bool
	stopChan        chan struct{}
	wg              sync.WaitGroup
}

// ConsumerStatistics tracks stream consumer metrics
type ConsumerStatistics struct {
	mu              sync.RWMutex
	MessagesRead    int64
	MessagesWritten int64
	MessagesFailed  int64
	LastMessageTime time.Time
	TotalLatency    time.Duration
	StartTime       time.Time
}

// NewStreamDatabaseConsumer creates a new stream to database consumer
func NewStreamDatabaseConsumer(
	streamServiceEndpoint string,
	targetAdapter adapter.Connection,
	integrationName string,
	topicName string,
	targetTable string,
	mappingRulesJSON []byte,
	logger *logger.Logger,
) (*StreamDatabaseConsumer, error) {
	// Connect to stream service
	conn, err := grpc.Dial(streamServiceEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to stream service: %w", err)
	}

	consumer := &StreamDatabaseConsumer{
		streamClient:    streamv1.NewStreamServiceClient(conn),
		targetAdapter:   targetAdapter,
		integrationName: integrationName,
		topicName:       topicName,
		targetTable:     targetTable,
		logger:          logger,
		stats: &ConsumerStatistics{
			StartTime: time.Now(),
		},
		stopChan: make(chan struct{}),
	}

	// Parse mapping rules if provided
	if len(mappingRulesJSON) > 0 {
		var rules []adapter.TransformationRule
		if err := json.Unmarshal(mappingRulesJSON, &rules); err != nil {
			return nil, fmt.Errorf("failed to parse mapping rules: %w", err)
		}
		consumer.mappingRules = rules
	}

	return consumer, nil
}

// Start begins consuming messages from the stream
func (c *StreamDatabaseConsumer) Start(ctx context.Context) error {
	if c.running {
		return fmt.Errorf("consumer is already running")
	}

	c.running = true
	c.wg.Add(1)

	go func() {
		defer c.wg.Done()
		c.consumeLoop(ctx)
	}()

	if c.logger != nil {
		c.logger.Infof("Started stream consumer for %s/%s -> %s", c.integrationName, c.topicName, c.targetTable)
	}

	return nil
}

// Stop stops the consumer gracefully
func (c *StreamDatabaseConsumer) Stop() error {
	if !c.running {
		return nil
	}

	close(c.stopChan)
	c.wg.Wait()
	c.running = false

	if c.logger != nil {
		c.logger.Infof("Stopped stream consumer for %s/%s", c.integrationName, c.topicName)
	}

	return nil
}

// consumeLoop continuously consumes messages from the stream
func (c *StreamDatabaseConsumer) consumeLoop(ctx context.Context) {
	for {
		select {
		case <-c.stopChan:
			return
		case <-ctx.Done():
			return
		default:
			if err := c.consumeBatch(ctx); err != nil {
				if c.logger != nil {
					c.logger.Errorf("Error consuming batch: %v", err)
				}
				// Back off on error
				time.Sleep(5 * time.Second)
			}
		}
	}
}

// consumeBatch consumes and processes a batch of messages
func (c *StreamDatabaseConsumer) consumeBatch(ctx context.Context) error {
	// Consume messages from stream
	consumeReq := &streamv1.ConsumeMessagesRequest{
		TenantId:        "", // Will be set by service
		StreamId:        c.integrationName,
		TopicNames:      []string{c.topicName},
		ConsumerGroupId: "redb-anchor",
		MaxMessages:     100,
		TimeoutSeconds:  30,
	}

	stream, err := c.streamClient.ConsumeMessages(ctx, consumeReq)
	if err != nil {
		return fmt.Errorf("failed to consume from stream: %w", err)
	}

	// Receive and process messages
	for {
		resp, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("failed to receive message: %w", err)
		}

		// Process each message in the response
		for _, msg := range resp.Messages {
			if err := c.processMessage(ctx, msg); err != nil {
				c.recordFailure()
				if c.logger != nil {
					c.logger.Errorf("Failed to process message: %v", err)
				}
				// Continue processing other messages
				continue
			}

			c.recordSuccess()
		}
	}
}

// processMessage processes a single stream message
func (c *StreamDatabaseConsumer) processMessage(ctx context.Context, msg *streamv1.StreamMessage) error {
	startTime := time.Now()
	c.recordMessageRead()

	// Parse message payload
	var payload map[string]interface{}
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		return fmt.Errorf("failed to parse message payload: %w", err)
	}

	// Extract data from payload
	data, ok := payload["data"].(map[string]interface{})
	if !ok {
		// If no nested data, use entire payload as data
		data = payload
	}

	// Apply mapping transformations
	if len(c.mappingRules) > 0 {
		transformedData, err := c.applyTransformations(ctx, data)
		if err != nil {
			if c.logger != nil {
				c.logger.Warnf("Failed to apply transformations: %v", err)
			}
			// Continue with untransformed data
		} else {
			data = transformedData
		}
	}

	// Convert to database record format
	record, err := c.convertToDataOps(data)
	if err != nil {
		return fmt.Errorf("failed to convert to data ops format: %w", err)
	}

	// Write to target database
	if err := c.writeToDatabase(ctx, record); err != nil {
		return fmt.Errorf("failed to write to database: %w", err)
	}

	// Record metrics
	c.recordLatency(time.Since(startTime))

	if c.logger != nil {
		c.logger.Debugf("Processed message from %s/%s and wrote to %s",
			c.integrationName, c.topicName, c.targetTable)
	}

	return nil
}

// writeToDatabase writes a record to the target database
func (c *StreamDatabaseConsumer) writeToDatabase(ctx context.Context, record map[string]interface{}) error {
	// Get data operations interface
	dataOps := c.targetAdapter.DataOperations()
	if dataOps == nil {
		return fmt.Errorf("target adapter does not support data operations")
	}

	// Execute insert using the Insert method
	rowsAffected, err := dataOps.Insert(ctx, c.targetTable, []map[string]interface{}{record})
	if err != nil {
		return fmt.Errorf("insert failed: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no records were inserted")
	}

	c.recordMessageWritten()
	return nil
}

// convertToDataOps converts stream data to database record format
func (c *StreamDatabaseConsumer) convertToDataOps(data map[string]interface{}) (map[string]interface{}, error) {
	// Create a new record with normalized field names
	record := make(map[string]interface{})

	for key, value := range data {
		// Handle nested structures
		if nestedMap, ok := value.(map[string]interface{}); ok {
			// Flatten nested structures or convert to JSON
			jsonBytes, err := json.Marshal(nestedMap)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal nested structure: %w", err)
			}
			record[key] = string(jsonBytes)
		} else {
			record[key] = value
		}
	}

	return record, nil
}

// applyTransformations applies mapping rules to transform data
func (c *StreamDatabaseConsumer) applyTransformations(ctx context.Context, data map[string]interface{}) (map[string]interface{}, error) {
	if len(c.mappingRules) == 0 {
		return data, nil
	}

	result := make(map[string]interface{})

	for _, rule := range c.mappingRules {
		sourceValue, exists := data[rule.SourceColumn]
		if !exists {
			// Skip if source field doesn't exist
			continue
		}

		// Apply transformation
		transformedValue, err := c.applyTransformation(rule.TransformationType, sourceValue)
		if err != nil {
			if c.logger != nil {
				c.logger.Warnf("Failed to apply transformation %s: %v", rule.TransformationType, err)
			}
			transformedValue = sourceValue
		}

		result[rule.TargetColumn] = transformedValue
	}

	// Include unmapped fields (pass-through)
	for key, value := range data {
		if _, exists := result[key]; !exists {
			result[key] = value
		}
	}

	return result, nil
}

// applyTransformation applies a single transformation
func (c *StreamDatabaseConsumer) applyTransformation(transformationType string, value interface{}) (interface{}, error) {
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
		if c.logger != nil {
			c.logger.Warnf("Unknown transformation type: %s", transformationType)
		}
	}

	return value, nil
}

// Statistics tracking methods
func (c *StreamDatabaseConsumer) recordMessageRead() {
	c.stats.mu.Lock()
	defer c.stats.mu.Unlock()
	c.stats.MessagesRead++
	c.stats.LastMessageTime = time.Now()
}

func (c *StreamDatabaseConsumer) recordMessageWritten() {
	c.stats.mu.Lock()
	defer c.stats.mu.Unlock()
	c.stats.MessagesWritten++
}

func (c *StreamDatabaseConsumer) recordSuccess() {
	// Already tracked via recordMessageWritten
}

func (c *StreamDatabaseConsumer) recordFailure() {
	c.stats.mu.Lock()
	defer c.stats.mu.Unlock()
	c.stats.MessagesFailed++
}

func (c *StreamDatabaseConsumer) recordLatency(latency time.Duration) {
	c.stats.mu.Lock()
	defer c.stats.mu.Unlock()
	c.stats.TotalLatency += latency
}

// GetStatistics returns consumer statistics
func (c *StreamDatabaseConsumer) GetStatistics() *ConsumerStatistics {
	c.stats.mu.RLock()
	defer c.stats.mu.RUnlock()

	return &ConsumerStatistics{
		MessagesRead:    c.stats.MessagesRead,
		MessagesWritten: c.stats.MessagesWritten,
		MessagesFailed:  c.stats.MessagesFailed,
		LastMessageTime: c.stats.LastMessageTime,
		TotalLatency:    c.stats.TotalLatency,
		StartTime:       c.stats.StartTime,
	}
}
