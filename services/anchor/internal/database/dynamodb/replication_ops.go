package dynamodb

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// ReplicationOps implements adapter.ReplicationOperator for DynamoDB.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether replication is supported.
func (r *ReplicationOps) IsSupported() bool {
	return true
}

// GetSupportedMechanisms returns the supported replication mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	return []string{"dynamodb_streams"}
}

// CheckPrerequisites checks if replication prerequisites are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	// Check if DynamoDB Streams is enabled on tables
	// This will be verified per-table when starting replication
	return nil
}

// Connect creates a new replication connection using DynamoDB Streams.
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	// Get AWS config from connection and create DynamoDB Streams client
	// Note: We need to access the underlying AWS config which is stored during connection
	// For now, we'll create the streams client using the same region as DynamoDB client
	awsCfg := aws.Config{
		Region: r.conn.client.Options().Region,
	}
	streamsClient := dynamodbstreams.NewFromConfig(awsCfg)

	// Create the replication source
	source := &DynamoDBReplicationSource{
		id:             config.ReplicationID,
		databaseID:     config.DatabaseID,
		client:         r.conn.client,
		streamsClient:  streamsClient,
		config:         config,
		active:         0,
		stopChan:       make(chan struct{}),
		shardIterators: make(map[string]string),
	}

	// Wrap the event handler to match the expected signature
	if config.EventHandler != nil {
		source.eventHandler = func(event map[string]interface{}) error {
			config.EventHandler(event)
			return nil
		}
	}

	// Set starting position if provided
	if config.StartPosition != "" {
		if err := source.SetPosition(config.StartPosition); err != nil {
			return nil, adapter.WrapError(dbcapabilities.DynamoDB, "set_start_position", err)
		}
	}

	return source, nil
}

// GetStatus returns the replication status.
func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	return map[string]interface{}{
		"database_id": r.conn.id,
		"mechanism":   "dynamodb_streams",
	}, nil
}

// GetLag returns the replication lag.
func (r *ReplicationOps) GetLag(ctx context.Context) (map[string]interface{}, error) {
	// DynamoDB Streams lag is typically very low (< 1 second)
	return map[string]interface{}{
		"database_id": r.conn.id,
		"mechanism":   "dynamodb_streams",
		"note":        "DynamoDB Streams typically have < 1 second lag",
	}, nil
}

// ListSlots lists all replication slots (not applicable for DynamoDB).
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(
		dbcapabilities.DynamoDB,
		"list replication slots",
		"DynamoDB uses Streams, not replication slots",
	)
}

// DropSlot drops a replication slot (not applicable for DynamoDB).
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	return adapter.NewUnsupportedOperationError(
		dbcapabilities.DynamoDB,
		"drop replication slot",
		"DynamoDB uses Streams, not replication slots",
	)
}

// ListPublications lists all publications (not applicable for DynamoDB).
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(
		dbcapabilities.DynamoDB,
		"list publications",
		"DynamoDB uses Streams, not publications",
	)
}

// DropPublication drops a publication (not applicable for DynamoDB).
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	return adapter.NewUnsupportedOperationError(
		dbcapabilities.DynamoDB,
		"drop publication",
		"DynamoDB uses Streams, not publications",
	)
}

// DynamoDBReplicationSource implements adapter.ReplicationSource for DynamoDB Streams.
type DynamoDBReplicationSource struct {
	id             string
	databaseID     string
	client         *dynamodb.Client
	streamsClient  *dynamodbstreams.Client
	config         adapter.ReplicationConfig
	streamArns     []string          // Stream ARNs for each table
	shardIterators map[string]string // Map of shardId -> iterator
	active         int32
	stopChan       chan struct{}
	mu             sync.RWMutex
	eventHandler   func(map[string]interface{}) error
	checkpointFn   func(context.Context, string) error
	lastPosition   string
}

// GetSourceID returns the replication source ID.
func (d *DynamoDBReplicationSource) GetSourceID() string {
	return d.id
}

// GetDatabaseID returns the database ID.
func (d *DynamoDBReplicationSource) GetDatabaseID() string {
	return d.databaseID
}

// GetStatus returns the replication source status.
func (d *DynamoDBReplicationSource) GetStatus() map[string]interface{} {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return map[string]interface{}{
		"source_id":     d.id,
		"database_id":   d.databaseID,
		"active":        d.IsActive(),
		"mechanism":     "dynamodb_streams",
		"stream_count":  len(d.streamArns),
		"shard_count":   len(d.shardIterators),
		"last_position": d.lastPosition,
	}
}

// GetMetadata returns the replication source metadata.
func (d *DynamoDBReplicationSource) GetMetadata() map[string]interface{} {
	return map[string]interface{}{
		"source_type":     "dynamodb_streams",
		"database_type":   "dynamodb",
		"replication_id":  d.id,
		"database_id":     d.databaseID,
		"supported_ops":   []string{"INSERT", "MODIFY", "REMOVE"},
		"resume_capable":  true,
		"transaction_log": false,
	}
}

// IsActive returns whether the replication source is active.
func (d *DynamoDBReplicationSource) IsActive() bool {
	return atomic.LoadInt32(&d.active) == 1
}

// Start starts the replication source.
func (d *DynamoDBReplicationSource) Start() error {
	if d.IsActive() {
		return adapter.NewDatabaseError(
			dbcapabilities.DynamoDB,
			"start_replication",
			adapter.ErrInvalidConfiguration,
		).WithContext("error", "replication source is already active")
	}

	ctx := context.Background()

	// Get stream ARNs for configured tables
	for _, tableName := range d.config.TableNames {
		describeOutput, err := d.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			return adapter.WrapError(dbcapabilities.DynamoDB, "describe_table", err)
		}

		if describeOutput.Table.LatestStreamArn == nil {
			return adapter.NewDatabaseError(
				dbcapabilities.DynamoDB,
				"start_replication",
				adapter.ErrConfigurationError,
			).WithContext("table", tableName).WithContext("error", "DynamoDB Streams not enabled on table")
		}

		d.streamArns = append(d.streamArns, *describeOutput.Table.LatestStreamArn)
	}

	// Initialize shard iterators for each stream
	for _, streamArn := range d.streamArns {
		// Describe stream to get shards
		describeStreamOutput, err := d.streamsClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
			StreamArn: aws.String(streamArn),
		})
		if err != nil {
			return adapter.WrapError(dbcapabilities.DynamoDB, "describe_stream", err)
		}

		// Get shard iterators
		for _, shard := range describeStreamOutput.StreamDescription.Shards {
			// Determine iterator type based on start position
			iteratorType := types.ShardIteratorTypeLatest
			if d.lastPosition != "" {
				iteratorType = types.ShardIteratorTypeAfterSequenceNumber
			}

			getIteratorOutput, err := d.streamsClient.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
				StreamArn:         aws.String(streamArn),
				ShardId:           shard.ShardId,
				ShardIteratorType: iteratorType,
			})
			if err != nil {
				return adapter.WrapError(dbcapabilities.DynamoDB, "get_shard_iterator", err)
			}

			if getIteratorOutput.ShardIterator != nil {
				d.mu.Lock()
				d.shardIterators[*shard.ShardId] = *getIteratorOutput.ShardIterator
				d.mu.Unlock()
			}
		}
	}

	atomic.StoreInt32(&d.active, 1)

	// Start event processing in goroutines (one per shard)
	d.mu.RLock()
	for shardId, iterator := range d.shardIterators {
		go d.processShardEvents(shardId, iterator)
	}
	d.mu.RUnlock()

	return nil
}

// processShardEvents processes events from a single shard.
func (d *DynamoDBReplicationSource) processShardEvents(shardId string, initialIterator string) {
	ctx := context.Background()
	iterator := initialIterator

	for d.IsActive() {
		select {
		case <-d.stopChan:
			return
		default:
			// Get records from the shard
			getRecordsOutput, err := d.streamsClient.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
				ShardIterator: aws.String(iterator),
			})
			if err != nil {
				// Log error and continue with backoff
				time.Sleep(1 * time.Second)
				continue
			}

			// Process records
			for _, record := range getRecordsOutput.Records {
				// Convert DynamoDB Stream record to event map
				event := d.streamRecordToMap(record)

				// Call event handler if set
				if d.eventHandler != nil {
					if err := d.eventHandler(event); err != nil {
						// Log error but continue processing
						continue
					}
				}

				// Update last position
				if record.Dynamodb != nil && record.Dynamodb.SequenceNumber != nil {
					d.mu.Lock()
					d.lastPosition = *record.Dynamodb.SequenceNumber
					d.mu.Unlock()
				}
			}

			// Update iterator for next batch
			if getRecordsOutput.NextShardIterator == nil {
				// Shard is closed
				return
			}
			iterator = *getRecordsOutput.NextShardIterator

			// Small delay to avoid throttling
			if len(getRecordsOutput.Records) == 0 {
				time.Sleep(1 * time.Second)
			}
		}
	}
}

// streamRecordToMap converts a DynamoDB Stream record to a map for event handling.
func (d *DynamoDBReplicationSource) streamRecordToMap(record types.Record) map[string]interface{} {
	event := make(map[string]interface{})

	if record.EventName != "" {
		event["event_name"] = string(record.EventName)
	}

	if record.Dynamodb != nil {
		if record.Dynamodb.Keys != nil {
			event["keys"] = record.Dynamodb.Keys
		}
		if record.Dynamodb.NewImage != nil {
			event["new_image"] = record.Dynamodb.NewImage
		}
		if record.Dynamodb.OldImage != nil {
			event["old_image"] = record.Dynamodb.OldImage
		}
		if record.Dynamodb.SequenceNumber != nil {
			event["sequence_number"] = *record.Dynamodb.SequenceNumber
		}
		if record.Dynamodb.StreamViewType != "" {
			event["stream_view_type"] = string(record.Dynamodb.StreamViewType)
		}
	}

	if record.EventSource != nil {
		event["event_source"] = *record.EventSource
	}

	return event
}

// Stop stops the replication source.
func (d *DynamoDBReplicationSource) Stop() error {
	if !d.IsActive() {
		return nil
	}

	atomic.StoreInt32(&d.active, 0)
	close(d.stopChan)

	return nil
}

// Close closes the replication source.
func (d *DynamoDBReplicationSource) Close() error {
	return d.Stop()
}

// GetPosition returns the current replication position (sequence number).
func (d *DynamoDBReplicationSource) GetPosition() (string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return d.lastPosition, nil
}

// SetPosition sets the starting replication position for resume.
func (d *DynamoDBReplicationSource) SetPosition(position string) error {
	if position == "" {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.lastPosition = position
	return nil
}

// SaveCheckpoint persists the current replication position.
func (d *DynamoDBReplicationSource) SaveCheckpoint(ctx context.Context, position string) error {
	if d.checkpointFn != nil {
		return d.checkpointFn(ctx, position)
	}
	return nil
}

// SetCheckpointFunc sets the callback function for persisting checkpoints.
func (d *DynamoDBReplicationSource) SetCheckpointFunc(fn func(context.Context, string) error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.checkpointFn = fn
}

// GetClient returns the underlying DynamoDB client (for internal use).
func (r *ReplicationOps) GetClient() *dynamodb.Client {
	return r.conn.client
}
