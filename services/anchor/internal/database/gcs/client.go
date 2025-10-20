package gcs

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCSClient wraps the Google Cloud Storage client with reDB-specific functionality.
type GCSClient struct {
	client    *storage.Client
	bucket    string // Bucket name (treated as "database")
	projectID string
}

// NewGCSClient creates a new GCS client from a database connection config.
func NewGCSClient(ctx context.Context, cfg adapter.ConnectionConfig) (*GCSClient, error) {
	var opts []option.ClientOption

	// Add credentials if provided
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
	} else if cfg.CredentialsJSON != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(cfg.CredentialsJSON)))
	}

	// Create GCS client
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return &GCSClient{
		client:    client,
		bucket:    cfg.DatabaseName, // In GCS, bucket = database
		projectID: cfg.ProjectID,
	}, nil
}

// NewGCSClientFromInstance creates a new GCS client from an instance config.
func NewGCSClientFromInstance(ctx context.Context, cfg adapter.InstanceConfig) (*GCSClient, error) {
	// Convert to ConnectionConfig and create client
	connCfg := adapter.ConnectionConfig{
		Host:            cfg.Host,
		Port:            cfg.Port,
		ProjectID:       cfg.ProjectID,
		CredentialsFile: cfg.CredentialsFile,
		CredentialsJSON: cfg.CredentialsJSON,
	}

	return NewGCSClient(ctx, connCfg)
}

// Ping tests the GCS connection by checking if bucket exists.
func (c *GCSClient) Ping(ctx context.Context) error {
	if c.bucket != "" {
		// Check if bucket exists
		_, err := c.client.Bucket(c.bucket).Attrs(ctx)
		if err != nil {
			return fmt.Errorf("failed to check bucket: %w", err)
		}
		return nil
	}

	// Just list buckets to verify connectivity
	it := c.client.Buckets(ctx, c.projectID)
	_, err := it.Next()
	if err != nil && err != iterator.Done {
		return fmt.Errorf("failed to list buckets: %w", err)
	}

	return nil
}

// Close closes the GCS client.
func (c *GCSClient) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// ListBuckets lists all GCS buckets in the project.
func (c *GCSClient) ListBuckets(ctx context.Context) ([]string, error) {
	if c.projectID == "" {
		return nil, fmt.Errorf("project ID is required to list buckets")
	}

	it := c.client.Buckets(ctx, c.projectID)
	buckets := make([]string, 0)

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list buckets: %w", err)
		}
		buckets = append(buckets, attrs.Name)
	}

	return buckets, nil
}

// CreateBucket creates a new GCS bucket.
func (c *GCSClient) CreateBucket(ctx context.Context, name string, options map[string]interface{}) error {
	if c.projectID == "" {
		return fmt.Errorf("project ID is required to create bucket")
	}

	attrs := &storage.BucketAttrs{
		Location: "US", // Default location
	}

	// Get location from options if provided
	if location, ok := options["location"].(string); ok && location != "" {
		attrs.Location = location
	}

	// Get storage class from options if provided
	if storageClass, ok := options["storageClass"].(string); ok && storageClass != "" {
		attrs.StorageClass = storageClass
	}

	err := c.client.Bucket(name).Create(ctx, c.projectID, attrs)
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	return nil
}

// DeleteBucket deletes a GCS bucket.
func (c *GCSClient) DeleteBucket(ctx context.Context, name string) error {
	err := c.client.Bucket(name).Delete(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete bucket: %w", err)
	}

	return nil
}

// GetBucket returns the current bucket name.
func (c *GCSClient) GetBucket() string {
	return c.bucket
}

// Client returns the underlying GCS client.
func (c *GCSClient) Client() *storage.Client {
	return c.client
}

// ProjectID returns the GCP project ID.
func (c *GCSClient) ProjectID() string {
	return c.projectID
}
