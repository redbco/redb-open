package s3

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// MetadataOps implements metadata operations for S3.
type MetadataOps struct {
	conn         *Connection
	instanceConn *InstanceConnection
}

// CollectDatabaseMetadata collects metadata about the S3 bucket.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	var bucket string
	var client *S3Client

	if m.conn != nil {
		bucket = m.conn.client.GetBucket()
		client = m.conn.client
	} else if m.instanceConn != nil {
		return nil, fmt.Errorf("database metadata requires a bucket connection")
	} else {
		return nil, fmt.Errorf("no connection available")
	}

	if bucket == "" {
		return nil, fmt.Errorf("no bucket specified")
	}

	metadata := make(map[string]interface{})
	metadata["bucket_name"] = bucket
	metadata["database_type"] = "s3"

	// Get bucket location
	locationResult, err := client.Client().GetBucketLocation(ctx, &s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})
	if err == nil && locationResult.LocationConstraint != "" {
		metadata["region"] = string(locationResult.LocationConstraint)
	}

	// Get bucket versioning
	versioningResult, err := client.Client().GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucket),
	})
	if err == nil {
		metadata["versioning_status"] = string(versioningResult.Status)
	}

	// Count objects
	count, err := m.countObjects(ctx, client, bucket)
	if err == nil {
		metadata["object_count"] = count
	}

	return metadata, nil
}

// CollectInstanceMetadata collects metadata about the S3 account/instance.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	var client *S3Client

	if m.conn != nil {
		client = m.conn.client
	} else if m.instanceConn != nil {
		client = m.instanceConn.client
	} else {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})
	metadata["database_type"] = "s3"

	// List buckets
	buckets, err := client.ListBuckets(ctx)
	if err == nil {
		metadata["bucket_count"] = len(buckets)
		metadata["buckets"] = buckets
	}

	return metadata, nil
}

// GetVersion returns the S3 API version (not applicable).
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	return "AWS S3 API", nil
}

// GetUniqueIdentifier returns the bucket ARN or account ID.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	if m.conn != nil {
		bucket := m.conn.client.GetBucket()
		if bucket != "" {
			// Return bucket name as identifier
			return fmt.Sprintf("s3::%s", bucket), nil
		}
	}

	return "s3::unknown", nil
}

// GetDatabaseSize returns the total size of objects in the bucket.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	if m.conn == nil {
		return 0, fmt.Errorf("no bucket connection available")
	}

	bucket := m.conn.client.GetBucket()
	if bucket == "" {
		return 0, fmt.Errorf("no bucket specified")
	}

	var totalSize int64
	var continuationToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: continuationToken,
		}

		result, err := m.conn.client.Client().ListObjectsV2(ctx, input)
		if err != nil {
			return 0, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range result.Contents {
			if obj.Size != nil {
				totalSize += *obj.Size
			}
		}

		if !*result.IsTruncated {
			break
		}

		continuationToken = result.NextContinuationToken
	}

	return totalSize, nil
}

// GetTableCount returns the number of "tables" (prefixes) in the bucket.
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	if m.conn == nil {
		return 0, fmt.Errorf("no bucket connection available")
	}

	bucket := m.conn.client.GetBucket()
	if bucket == "" {
		return 0, fmt.Errorf("no bucket specified")
	}

	// List common prefixes
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int32(1000),
	}

	result, err := m.conn.client.Client().ListObjectsV2(ctx, input)
	if err != nil {
		return 0, fmt.Errorf("failed to list prefixes: %w", err)
	}

	// Add 1 for root
	return len(result.CommonPrefixes) + 1, nil
}

// ExecuteCommand is not supported for S3.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	return nil, fmt.Errorf("ExecuteCommand not supported for S3")
}

// countObjects counts the total number of objects in a bucket.
func (m *MetadataOps) countObjects(ctx context.Context, client *S3Client, bucket string) (int64, error) {
	var count int64
	var continuationToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: continuationToken,
		}

		result, err := client.Client().ListObjectsV2(ctx, input)
		if err != nil {
			return 0, err
		}

		count += int64(len(result.Contents))

		if !*result.IsTruncated {
			break
		}

		continuationToken = result.NextContinuationToken
	}

	return count, nil
}
