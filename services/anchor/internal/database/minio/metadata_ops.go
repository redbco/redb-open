package minio

import (
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
)

// MetadataOps implements metadata operations for MinIO.
type MetadataOps struct {
	conn         *Connection
	instanceConn *InstanceConnection
}

// CollectDatabaseMetadata collects metadata about the MinIO bucket.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	if m.conn == nil {
		return nil, fmt.Errorf("no connection available")
	}

	bucket := m.conn.client.GetBucket()
	if bucket == "" {
		return nil, fmt.Errorf("no bucket specified")
	}

	metadata := make(map[string]interface{})
	metadata["bucket_name"] = bucket
	metadata["database_type"] = "minio"

	// Count objects
	count, err := m.countObjects(ctx, m.conn.client, bucket)
	if err == nil {
		metadata["object_count"] = count
	}

	return metadata, nil
}

// CollectInstanceMetadata collects metadata about the MinIO instance.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	var client *MinIOClient

	if m.conn != nil {
		client = m.conn.client
	} else if m.instanceConn != nil {
		client = m.instanceConn.client
	} else {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})
	metadata["database_type"] = "minio"

	// List buckets
	buckets, err := client.ListBuckets(ctx)
	if err == nil {
		metadata["bucket_count"] = len(buckets)
		metadata["buckets"] = buckets
	}

	return metadata, nil
}

// GetVersion returns the MinIO version.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	return "MinIO", nil
}

// GetUniqueIdentifier returns the bucket name as unique identifier.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	if m.conn != nil {
		bucket := m.conn.client.GetBucket()
		return fmt.Sprintf("minio::%s", bucket), nil
	}

	if m.instanceConn != nil {
		return "minio::instance", nil
	}

	return "minio::unknown", nil
}

// GetDatabaseSize returns the total size of objects in the bucket.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	if m.conn == nil {
		return 0, fmt.Errorf("no connection available")
	}

	bucket := m.conn.client.GetBucket()
	if bucket == "" {
		return 0, fmt.Errorf("no bucket specified")
	}

	objectCh := m.conn.client.Client().ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Recursive: true,
	})

	var totalSize int64
	for object := range objectCh {
		if object.Err != nil {
			return 0, fmt.Errorf("failed to list objects: %w", object.Err)
		}
		totalSize += object.Size
	}

	return totalSize, nil
}

// GetTableCount returns the number of "tables" (prefixes) in the bucket.
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	if m.conn == nil {
		return 0, fmt.Errorf("no connection available")
	}

	bucket := m.conn.client.GetBucket()
	if bucket == "" {
		return 0, fmt.Errorf("no bucket specified")
	}

	objectCh := m.conn.client.Client().ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Recursive: false,
	})

	count := 1 // Start with root
	seen := make(map[string]bool)

	for object := range objectCh {
		if object.Err != nil {
			return 0, fmt.Errorf("failed to list objects: %w", object.Err)
		}

		// Extract prefix from key
		if idx := len(object.Key) - 1; idx >= 0 && object.Key[idx] == '/' {
			prefix := object.Key[:idx]
			if !seen[prefix] && prefix != "" {
				seen[prefix] = true
				count++
			}
		}
	}

	return count, nil
}

// ExecuteCommand is not supported for MinIO.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	return nil, fmt.Errorf("ExecuteCommand not supported for MinIO")
}

// countObjects counts the total number of objects in a bucket.
func (m *MetadataOps) countObjects(ctx context.Context, client *MinIOClient, bucket string) (int64, error) {
	objectCh := client.Client().ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Recursive: true,
	})

	var count int64
	for object := range objectCh {
		if object.Err != nil {
			return 0, object.Err
		}
		count++
	}

	return count, nil
}
