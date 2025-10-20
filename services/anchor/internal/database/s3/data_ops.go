package s3

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// DataOps implements data operations for S3.
type DataOps struct {
	conn *Connection
}

// Fetch retrieves objects from S3 (treating them as "rows").
func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
	return d.FetchWithColumns(ctx, table, nil, limit)
}

// FetchWithColumns retrieves objects with specific metadata fields.
func (d *DataOps) FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return nil, fmt.Errorf("no bucket specified")
	}

	// Convert table name to prefix
	prefix := ""
	if table != "" && table != "root" {
		prefix = table + "/"
	}

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(int32(limit)),
	}

	result, err := d.conn.client.Client().ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	rows := make([]map[string]interface{}, 0, len(result.Contents))
	for _, obj := range result.Contents {
		row := make(map[string]interface{})

		if obj.Key != nil {
			row["key"] = *obj.Key
		}
		if obj.Size != nil {
			row["size"] = *obj.Size
		}
		if obj.LastModified != nil {
			row["last_modified"] = *obj.LastModified
		}
		if obj.ETag != nil {
			row["etag"] = strings.Trim(*obj.ETag, "\"")
		}
		if obj.StorageClass != "" {
			row["storage_class"] = string(obj.StorageClass)
		}

		rows = append(rows, row)
	}

	return rows, nil
}

// Insert uploads objects to S3.
func (d *DataOps) Insert(ctx context.Context, table string, data []map[string]interface{}) (int64, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return 0, fmt.Errorf("no bucket specified")
	}

	prefix := ""
	if table != "" && table != "root" {
		prefix = table + "/"
	}

	var count int64
	for _, row := range data {
		key, ok := row["key"].(string)
		if !ok || key == "" {
			continue
		}

		// Add prefix if not already present
		fullKey := key
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			fullKey = prefix + key
		}

		// Get content (body)
		var body []byte
		if content, ok := row["content"]; ok {
			switch v := content.(type) {
			case []byte:
				body = v
			case string:
				body = []byte(v)
			default:
				body = []byte(fmt.Sprintf("%v", v))
			}
		}

		// Upload object
		input := &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(fullKey),
			Body:   strings.NewReader(string(body)),
		}

		// Add content type if provided
		if contentType, ok := row["content_type"].(string); ok {
			input.ContentType = aws.String(contentType)
		}

		_, err := d.conn.client.Client().PutObject(ctx, input)
		if err != nil {
			return count, fmt.Errorf("failed to upload object %s: %w", fullKey, err)
		}

		count++
	}

	return count, nil
}

// Update is not directly supported in S3 (objects are immutable).
// This operation will overwrite objects.
func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	// For S3, update is the same as insert (overwrite)
	return d.Insert(ctx, table, data)
}

// Upsert uploads or overwrites objects.
func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	// For S3, upsert is the same as insert
	return d.Insert(ctx, table, data)
}

// Delete removes objects from S3.
func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return 0, fmt.Errorf("no bucket specified")
	}

	// If key is specified in conditions, delete that specific object
	if key, ok := conditions["key"].(string); ok {
		_, err := d.conn.client.Client().DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return 0, fmt.Errorf("failed to delete object: %w", err)
		}
		return 1, nil
	}

	// Otherwise, delete all objects in the prefix
	prefix := ""
	if table != "" && table != "root" {
		prefix = table + "/"
	}

	// List objects to delete
	objects, err := d.listAllObjects(ctx, bucket, prefix)
	if err != nil {
		return 0, err
	}

	// Delete objects
	var count int64
	for _, key := range objects {
		_, err := d.conn.client.Client().DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return count, fmt.Errorf("failed to delete object %s: %w", key, err)
		}
		count++
	}

	return count, nil
}

// listAllObjects lists all object keys with the given prefix.
func (d *DataOps) listAllObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
	var keys []string
	var continuationToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		}

		result, err := d.conn.client.Client().ListObjectsV2(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range result.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}

		if !*result.IsTruncated {
			break
		}

		continuationToken = result.NextContinuationToken
	}

	return keys, nil
}

// Stream retrieves objects in batches for large datasets.
func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return adapter.StreamResult{}, fmt.Errorf("no bucket specified")
	}

	prefix := ""
	if params.Table != "" && params.Table != "root" {
		prefix = params.Table + "/"
	}

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(params.BatchSize),
	}

	// Use cursor for pagination
	if params.OrderBy != "" {
		input.StartAfter = aws.String(params.OrderBy)
	}

	result, err := d.conn.client.Client().ListObjectsV2(ctx, input)
	if err != nil {
		return adapter.StreamResult{}, fmt.Errorf("failed to list objects: %w", err)
	}

	rows := make([]map[string]interface{}, 0, len(result.Contents))
	for _, obj := range result.Contents {
		row := make(map[string]interface{})

		if obj.Key != nil {
			row["key"] = *obj.Key
		}
		if obj.Size != nil {
			row["size"] = *obj.Size
		}
		if obj.LastModified != nil {
			row["last_modified"] = *obj.LastModified
		}
		if obj.ETag != nil {
			row["etag"] = strings.Trim(*obj.ETag, "\"")
		}

		rows = append(rows, row)
	}

	nextCursor := ""
	if result.IsTruncated != nil && *result.IsTruncated && len(result.Contents) > 0 {
		if result.Contents[len(result.Contents)-1].Key != nil {
			nextCursor = *result.Contents[len(result.Contents)-1].Key
		}
	}

	return adapter.StreamResult{
		Data:       rows,
		HasMore:    result.IsTruncated != nil && *result.IsTruncated,
		NextCursor: nextCursor,
	}, nil
}

// ExecuteQuery is not supported for S3.
func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	return nil, fmt.Errorf("ExecuteQuery not supported for S3")
}

// ExecuteCountQuery counts objects in a prefix.
func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return 0, fmt.Errorf("no bucket specified")
	}

	// Simple count implementation
	var count int64
	var continuationToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: continuationToken,
		}

		result, err := d.conn.client.Client().ListObjectsV2(ctx, input)
		if err != nil {
			return 0, fmt.Errorf("failed to count objects: %w", err)
		}

		count += int64(len(result.Contents))

		if !*result.IsTruncated {
			break
		}

		continuationToken = result.NextContinuationToken
	}

	return count, nil
}

// GetRowCount returns the number of objects in a prefix.
func (d *DataOps) GetRowCount(ctx context.Context, table string, whereClause string) (int64, bool, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return 0, false, fmt.Errorf("no bucket specified")
	}

	prefix := ""
	if table != "" && table != "root" {
		prefix = table + "/"
	}

	var count int64
	var continuationToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		}

		result, err := d.conn.client.Client().ListObjectsV2(ctx, input)
		if err != nil {
			return 0, false, fmt.Errorf("failed to count objects: %w", err)
		}

		count += int64(len(result.Contents))

		if !*result.IsTruncated {
			break
		}

		continuationToken = result.NextContinuationToken
	}

	return count, true, nil
}

// Wipe deletes all objects in the bucket.
func (d *DataOps) Wipe(ctx context.Context) error {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return fmt.Errorf("no bucket specified")
	}

	// List all objects
	objects, err := d.listAllObjects(ctx, bucket, "")
	if err != nil {
		return err
	}

	// Delete all objects
	for _, key := range objects {
		_, err := d.conn.client.Client().DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return fmt.Errorf("failed to delete object %s: %w", key, err)
		}
	}

	return nil
}

// GetObject retrieves the content of a specific object.
func (d *DataOps) GetObject(ctx context.Context, key string) ([]byte, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return nil, fmt.Errorf("no bucket specified")
	}

	result, err := d.conn.client.Client().GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer result.Body.Close()

	return io.ReadAll(result.Body)
}

// PutObject uploads content to a specific object.
func (d *DataOps) PutObject(ctx context.Context, key string, content []byte, contentType string) error {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return fmt.Errorf("no bucket specified")
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(string(content)),
	}

	if contentType != "" {
		input.ContentType = aws.String(contentType)
	}

	_, err := d.conn.client.Client().PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to put object: %w", err)
	}

	return nil
}
