package gcs

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// MetadataOps implements metadata operations for GCS.
type MetadataOps struct {
	conn         *Connection
	instanceConn *InstanceConnection
}

// CollectDatabaseMetadata collects metadata about the GCS bucket.
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
	metadata["database_type"] = "gcs"
	metadata["project_id"] = m.conn.client.ProjectID()

	// Get bucket attributes
	attrs, err := m.conn.client.Client().Bucket(bucket).Attrs(ctx)
	if err == nil {
		metadata["location"] = attrs.Location
		metadata["storage_class"] = attrs.StorageClass
		metadata["created"] = attrs.Created
		metadata["versioning_enabled"] = attrs.VersioningEnabled
	}

	// Count objects
	count, err := m.countObjects(ctx, m.conn.client, bucket)
	if err == nil {
		metadata["object_count"] = count
	}

	return metadata, nil
}

// CollectInstanceMetadata collects metadata about the GCS instance.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	var client *GCSClient

	if m.conn != nil {
		client = m.conn.client
	} else if m.instanceConn != nil {
		client = m.instanceConn.client
	} else {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})
	metadata["database_type"] = "gcs"
	metadata["project_id"] = client.ProjectID()

	// List buckets
	buckets, err := client.ListBuckets(ctx)
	if err == nil {
		metadata["bucket_count"] = len(buckets)
		metadata["buckets"] = buckets
	}

	return metadata, nil
}

// GetVersion returns the GCS version.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	return "Google Cloud Storage", nil
}

// GetUniqueIdentifier returns the bucket name and project ID as unique identifier.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	if m.conn != nil {
		bucket := m.conn.client.GetBucket()
		projectID := m.conn.client.ProjectID()
		return fmt.Sprintf("gcs::%s::%s", projectID, bucket), nil
	}

	if m.instanceConn != nil {
		projectID := m.instanceConn.client.ProjectID()
		return fmt.Sprintf("gcs::%s::instance", projectID), nil
	}

	return "gcs::unknown", nil
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

	it := m.conn.client.Client().Bucket(bucket).Objects(ctx, nil)

	var totalSize int64
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("failed to list objects: %w", err)
		}

		// Skip prefix entries
		if attrs.Prefix != "" {
			continue
		}

		totalSize += attrs.Size
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

	query := &storage.Query{
		Delimiter: "/",
		Prefix:    "",
	}

	it := m.conn.client.Client().Bucket(bucket).Objects(ctx, query)

	count := 1 // Start with root
	seen := make(map[string]bool)

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("failed to list objects: %w", err)
		}

		// Check for common prefix (delimiter match)
		if attrs.Prefix != "" {
			if !seen[attrs.Prefix] {
				seen[attrs.Prefix] = true
				count++
			}
		}
	}

	return count, nil
}

// ExecuteCommand is not supported for GCS.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	return nil, fmt.Errorf("ExecuteCommand not supported for GCS")
}

// countObjects counts the total number of objects in a bucket.
func (m *MetadataOps) countObjects(ctx context.Context, client *GCSClient, bucket string) (int64, error) {
	it := client.Client().Bucket(bucket).Objects(ctx, nil)

	var count int64
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}

		// Skip prefix entries
		if attrs.Prefix != "" {
			continue
		}

		count++
	}

	return count, nil
}
