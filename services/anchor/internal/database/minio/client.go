package minio

import (
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// MinIOClient wraps the MinIO client with reDB-specific functionality.
type MinIOClient struct {
	client *minio.Client
	bucket string // Bucket name (treated as "database")
	region string
}

// NewMinIOClient creates a new MinIO client from a database connection config.
func NewMinIOClient(ctx context.Context, cfg adapter.ConnectionConfig) (*MinIOClient, error) {
	// Default port for MinIO
	port := cfg.Port
	if port == 0 {
		port = 9000
	}

	endpoint := fmt.Sprintf("%s:%d", cfg.Host, port)

	// Create MinIO client
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken),
		Secure: cfg.SSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create MinIO client: %w", err)
	}

	return &MinIOClient{
		client: minioClient,
		bucket: cfg.DatabaseName, // In MinIO, bucket = database
		region: cfg.Region,
	}, nil
}

// NewMinIOClientFromInstance creates a new MinIO client from an instance config.
func NewMinIOClientFromInstance(ctx context.Context, cfg adapter.InstanceConfig) (*MinIOClient, error) {
	// Convert to ConnectionConfig and create client
	connCfg := adapter.ConnectionConfig{
		Host:            cfg.Host,
		Port:            cfg.Port,
		AccessKeyID:     cfg.AccessKeyID,
		SecretAccessKey: cfg.SecretAccessKey,
		SessionToken:    cfg.SessionToken,
		Region:          cfg.Region,
		SSL:             cfg.SSL,
	}

	return NewMinIOClient(ctx, connCfg)
}

// Ping tests the MinIO connection by listing buckets.
func (c *MinIOClient) Ping(ctx context.Context) error {
	if c.bucket != "" {
		// Check if bucket exists
		exists, err := c.client.BucketExists(ctx, c.bucket)
		if err != nil {
			return fmt.Errorf("failed to check bucket: %w", err)
		}
		if !exists {
			return fmt.Errorf("bucket does not exist: %s", c.bucket)
		}
		return nil
	}

	// Just list buckets to verify connectivity
	_, err := c.client.ListBuckets(ctx)
	return err
}

// ListBuckets lists all MinIO buckets.
func (c *MinIOClient) ListBuckets(ctx context.Context) ([]string, error) {
	buckets, err := c.client.ListBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list buckets: %w", err)
	}

	names := make([]string, 0, len(buckets))
	for _, bucket := range buckets {
		names = append(names, bucket.Name)
	}

	return names, nil
}

// CreateBucket creates a new MinIO bucket.
func (c *MinIOClient) CreateBucket(ctx context.Context, name string, options map[string]interface{}) error {
	// Get region from options if provided
	region := c.region
	if r, ok := options["region"].(string); ok && r != "" {
		region = r
	}

	err := c.client.MakeBucket(ctx, name, minio.MakeBucketOptions{
		Region: region,
	})
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	return nil
}

// DeleteBucket deletes a MinIO bucket.
func (c *MinIOClient) DeleteBucket(ctx context.Context, name string) error {
	err := c.client.RemoveBucket(ctx, name)
	if err != nil {
		return fmt.Errorf("failed to delete bucket: %w", err)
	}

	return nil
}

// GetBucket returns the current bucket name.
func (c *MinIOClient) GetBucket() string {
	return c.bucket
}

// Client returns the underlying MinIO client.
func (c *MinIOClient) Client() *minio.Client {
	return c.client
}
