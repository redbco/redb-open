package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redbco/redb-open/pkg/logger"
)

// Service provides CRUD operations for streams in the database
type Service struct {
	db     *pgxpool.Pool
	logger *logger.Logger
}

// NewService creates a new stream service
func NewService(db *pgxpool.Pool, logger *logger.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// Stream represents a stream connection
type Stream struct {
	ID                string
	TenantID          string
	Name              string
	Description       string
	Platform          string
	Version           string
	RegionID          *string
	ConnectionConfig  map[string]interface{}
	CredentialKey     string
	Metadata          map[string]interface{}
	MonitoredTopics   []string
	ConnectedToNodeID int64
	OwnerID           string
	Status            string
	Created           time.Time
	Updated           time.Time
}

// Create creates a new stream
func (s *Service) Create(ctx context.Context, tenantID, name, description, platform string, connectionConfig map[string]interface{}, monitoredTopics []string, nodeID int64, ownerID string) (*Stream, error) {
	connectionConfigJSON, err := json.Marshal(connectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal connection config: %w", err)
	}

	metadataJSON, _ := json.Marshal(map[string]interface{}{})
	monitoredTopicsJSON, err := json.Marshal(monitoredTopics)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal monitored topics: %w", err)
	}

	query := `
		INSERT INTO streams (
			tenant_id, stream_name, stream_description, stream_platform,
			connection_config, stream_metadata, monitored_topics,
			connected_to_node_id, owner_id, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING stream_id, stream_version, created, updated
	`

	var stream Stream
	stream.TenantID = tenantID
	stream.Name = name
	stream.Description = description
	stream.Platform = platform
	stream.ConnectionConfig = connectionConfig
	stream.MonitoredTopics = monitoredTopics
	stream.ConnectedToNodeID = nodeID
	stream.OwnerID = ownerID
	stream.Status = "STATUS_PENDING"

	err = s.db.QueryRow(ctx, query,
		tenantID, name, description, platform,
		connectionConfigJSON, metadataJSON, monitoredTopicsJSON,
		nodeID, ownerID, "STATUS_PENDING",
	).Scan(&stream.ID, &stream.Version, &stream.Created, &stream.Updated)

	if err != nil {
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	return &stream, nil
}

// Get retrieves a stream by tenant ID and name
func (s *Service) Get(ctx context.Context, tenantID, name string) (*Stream, error) {
	query := `
		SELECT 
			stream_id, tenant_id, stream_name, stream_description, stream_platform,
			stream_version, stream_region_id, connection_config, credential_key,
			stream_metadata, monitored_topics, connected_to_node_id, owner_id,
			status, created, updated
		FROM streams
		WHERE tenant_id = $1 AND stream_name = $2
	`

	var stream Stream
	var connectionConfigJSON, metadataJSON, monitoredTopicsJSON []byte
	var regionID *string

	err := s.db.QueryRow(ctx, query, tenantID, name).Scan(
		&stream.ID,
		&stream.TenantID,
		&stream.Name,
		&stream.Description,
		&stream.Platform,
		&stream.Version,
		&regionID,
		&connectionConfigJSON,
		&stream.CredentialKey,
		&metadataJSON,
		&monitoredTopicsJSON,
		&stream.ConnectedToNodeID,
		&stream.OwnerID,
		&stream.Status,
		&stream.Created,
		&stream.Updated,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get stream: %w", err)
	}

	stream.RegionID = regionID

	// Parse JSON fields
	if len(connectionConfigJSON) > 0 {
		if err := json.Unmarshal(connectionConfigJSON, &stream.ConnectionConfig); err != nil {
			return nil, fmt.Errorf("failed to unmarshal connection config: %w", err)
		}
	}

	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &stream.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	}

	if len(monitoredTopicsJSON) > 0 {
		if err := json.Unmarshal(monitoredTopicsJSON, &stream.MonitoredTopics); err != nil {
			return nil, fmt.Errorf("failed to unmarshal monitored topics: %w", err)
		}
	}

	return &stream, nil
}

// List returns all streams for a tenant
func (s *Service) List(ctx context.Context, tenantID string) ([]*Stream, error) {
	query := `
		SELECT 
			stream_id, tenant_id, stream_name, stream_description, stream_platform,
			stream_version, stream_region_id, connection_config, credential_key,
			stream_metadata, monitored_topics, connected_to_node_id, owner_id,
			status, created, updated
		FROM streams
		WHERE tenant_id = $1
		ORDER BY stream_name
	`

	rows, err := s.db.Query(ctx, query, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to list streams: %w", err)
	}
	defer rows.Close()

	var streams []*Stream
	for rows.Next() {
		var stream Stream
		var connectionConfigJSON, metadataJSON, monitoredTopicsJSON []byte
		var regionID *string

		err := rows.Scan(
			&stream.ID,
			&stream.TenantID,
			&stream.Name,
			&stream.Description,
			&stream.Platform,
			&stream.Version,
			&regionID,
			&connectionConfigJSON,
			&stream.CredentialKey,
			&metadataJSON,
			&monitoredTopicsJSON,
			&stream.ConnectedToNodeID,
			&stream.OwnerID,
			&stream.Status,
			&stream.Created,
			&stream.Updated,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan stream: %w", err)
		}

		stream.RegionID = regionID

		// Parse JSON fields
		if len(connectionConfigJSON) > 0 {
			json.Unmarshal(connectionConfigJSON, &stream.ConnectionConfig)
		}
		if len(metadataJSON) > 0 {
			json.Unmarshal(metadataJSON, &stream.Metadata)
		}
		if len(monitoredTopicsJSON) > 0 {
			json.Unmarshal(monitoredTopicsJSON, &stream.MonitoredTopics)
		}

		streams = append(streams, &stream)
	}

	return streams, nil
}

// Delete deletes a stream
func (s *Service) Delete(ctx context.Context, tenantID, name string) error {
	query := `DELETE FROM streams WHERE tenant_id = $1 AND stream_name = $2`
	_, err := s.db.Exec(ctx, query, tenantID, name)
	if err != nil {
		return fmt.Errorf("failed to delete stream: %w", err)
	}
	return nil
}

// UpdateStatus updates the status of a stream
func (s *Service) UpdateStatus(ctx context.Context, streamID, status string) error {
	query := `UPDATE streams SET status = $1, updated = CURRENT_TIMESTAMP WHERE stream_id = $2`
	_, err := s.db.Exec(ctx, query, status, streamID)
	if err != nil {
		return fmt.Errorf("failed to update stream status: %w", err)
	}
	return nil
}
