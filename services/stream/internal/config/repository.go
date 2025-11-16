package config

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redbco/redb-open/pkg/stream/adapter"
	"github.com/redbco/redb-open/pkg/streamcapabilities"
	"google.golang.org/grpc"
)

// Repository handles database operations for stream configurations
type Repository struct {
	db       *pgxpool.Pool
	coreConn *grpc.ClientConn
}

// NewRepository creates a new configuration repository
func NewRepository(pool *pgxpool.Pool, coreConn *grpc.ClientConn) *Repository {
	return &Repository{
		db:       pool,
		coreConn: coreConn,
	}
}

// StreamConfig represents the configuration for a stream connection
type StreamConfig struct {
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

// GetStreamConfigByID retrieves a stream configuration by its ID
func (r *Repository) GetStreamConfigByID(ctx context.Context, streamID string) (*StreamConfig, error) {
	query := `
		SELECT 
			stream_id, tenant_id, stream_name, stream_description, stream_platform,
			stream_version, stream_region_id, connection_config, credential_key,
			stream_metadata, monitored_topics, connected_to_node_id, owner_id,
			status, created, updated
		FROM streams
		WHERE stream_id = $1
	`

	var config StreamConfig
	var connectionConfigJSON, metadataJSON, monitoredTopicsJSON []byte
	var regionID *string

	err := r.db.QueryRow(ctx, query, streamID).Scan(
		&config.ID,
		&config.TenantID,
		&config.Name,
		&config.Description,
		&config.Platform,
		&config.Version,
		&regionID,
		&connectionConfigJSON,
		&config.CredentialKey,
		&metadataJSON,
		&monitoredTopicsJSON,
		&config.ConnectedToNodeID,
		&config.OwnerID,
		&config.Status,
		&config.Created,
		&config.Updated,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("stream not found: %s", streamID)
		}
		return nil, fmt.Errorf("failed to query stream: %w", err)
	}

	config.RegionID = regionID

	// Parse JSON fields
	if len(connectionConfigJSON) > 0 {
		if err := json.Unmarshal(connectionConfigJSON, &config.ConnectionConfig); err != nil {
			return nil, fmt.Errorf("failed to unmarshal connection config: %w", err)
		}
	}

	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &config.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	}

	if len(monitoredTopicsJSON) > 0 {
		if err := json.Unmarshal(monitoredTopicsJSON, &config.MonitoredTopics); err != nil {
			return nil, fmt.Errorf("failed to unmarshal monitored topics: %w", err)
		}
	}

	return &config, nil
}

// ToConnectionConfig converts a StreamConfig to an adapter.ConnectionConfig
func (sc *StreamConfig) ToConnectionConfig() *adapter.ConnectionConfig {
	config := adapter.NewConnectionConfig(sc.ID, streamcapabilities.StreamPlatform(sc.Platform))

	// Parse connection config fields
	if brokers, ok := sc.ConnectionConfig["brokers"].([]interface{}); ok {
		config.Brokers = make([]string, len(brokers))
		for i, b := range brokers {
			if str, ok := b.(string); ok {
				config.Brokers[i] = str
			}
		}
	}

	if region, ok := sc.ConnectionConfig["region"].(string); ok {
		config.Region = region
	}

	if project, ok := sc.ConnectionConfig["project"].(string); ok {
		config.Project = project
	}

	if namespace, ok := sc.ConnectionConfig["namespace"].(string); ok {
		config.Namespace = namespace
	}

	if endpoint, ok := sc.ConnectionConfig["endpoint"].(string); ok {
		config.Endpoint = endpoint
	}

	// Authentication
	if username, ok := sc.ConnectionConfig["username"].(string); ok {
		config.Username = username
	}

	if password, ok := sc.ConnectionConfig["password"].(string); ok {
		config.Password = password
	}

	if saslMech, ok := sc.ConnectionConfig["sasl_mechanism"].(string); ok {
		config.SASLMechanism = saslMech
	}

	if tlsEnabled, ok := sc.ConnectionConfig["tls_enabled"].(bool); ok {
		config.TLSEnabled = tlsEnabled
	}

	if tlsSkipVerify, ok := sc.ConnectionConfig["tls_skip_verify"].(bool); ok {
		config.TLSSkipVerify = tlsSkipVerify
	}

	// Consumer config
	if groupID, ok := sc.ConnectionConfig["group_id"].(string); ok {
		config.GroupID = groupID
	}

	// Copy additional configuration
	for k, v := range sc.ConnectionConfig {
		if str, ok := v.(string); ok {
			config.Configuration[k] = str
		}
	}

	return config
}

// UpdateStreamConnectionStatus updates the connection status of a stream
func (r *Repository) UpdateStreamConnectionStatus(ctx context.Context, streamID string, connected bool, message string) error {
	var status string
	if connected {
		status = "STATUS_CONNECTED"
	} else {
		status = "STATUS_DISCONNECTED"
	}

	query := `
		UPDATE streams
		SET status = $1, updated = CURRENT_TIMESTAMP
		WHERE stream_id = $2
	`

	_, err := r.db.Exec(ctx, query, status, streamID)
	if err != nil {
		return fmt.Errorf("failed to update stream status: %w", err)
	}

	return nil
}

// ListStreams returns all streams
func (r *Repository) ListStreams(ctx context.Context) ([]*StreamConfig, error) {
	query := `
		SELECT 
			stream_id, tenant_id, stream_name, stream_description, stream_platform,
			stream_version, stream_region_id, connection_config, credential_key,
			stream_metadata, monitored_topics, connected_to_node_id, owner_id,
			status, created, updated
		FROM streams
		ORDER BY stream_name
	`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query streams: %w", err)
	}
	defer rows.Close()

	var streams []*StreamConfig
	for rows.Next() {
		var config StreamConfig
		var connectionConfigJSON, metadataJSON, monitoredTopicsJSON []byte
		var regionID *string

		err := rows.Scan(
			&config.ID,
			&config.TenantID,
			&config.Name,
			&config.Description,
			&config.Platform,
			&config.Version,
			&regionID,
			&connectionConfigJSON,
			&config.CredentialKey,
			&metadataJSON,
			&monitoredTopicsJSON,
			&config.ConnectedToNodeID,
			&config.OwnerID,
			&config.Status,
			&config.Created,
			&config.Updated,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan stream: %w", err)
		}

		config.RegionID = regionID

		// Parse JSON fields
		if len(connectionConfigJSON) > 0 {
			if err := json.Unmarshal(connectionConfigJSON, &config.ConnectionConfig); err != nil {
				return nil, fmt.Errorf("failed to unmarshal connection config: %w", err)
			}
		}

		if len(metadataJSON) > 0 {
			if err := json.Unmarshal(metadataJSON, &config.Metadata); err != nil {
				return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
			}
		}

		if len(monitoredTopicsJSON) > 0 {
			if err := json.Unmarshal(monitoredTopicsJSON, &config.MonitoredTopics); err != nil {
				return nil, fmt.Errorf("failed to unmarshal monitored topics: %w", err)
			}
		}

		streams = append(streams, &config)
	}

	return streams, nil
}
