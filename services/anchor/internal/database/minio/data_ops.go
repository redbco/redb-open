package minio

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// DataOps implements data operations for MinIO.
type DataOps struct {
	conn *Connection
}

// Fetch retrieves objects from MinIO (treating them as "rows").
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

	objectCh := d.conn.client.Client().ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
		MaxKeys:   limit,
	})

	rows := make([]map[string]interface{}, 0)

	for object := range objectCh {
		if object.Err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", object.Err)
		}

		row := make(map[string]interface{})
		row["key"] = object.Key
		row["size"] = object.Size
		row["last_modified"] = object.LastModified
		row["etag"] = strings.Trim(object.ETag, "\"")
		row["storage_class"] = object.StorageClass

		rows = append(rows, row)

		if len(rows) >= limit {
			break
		}
	}

	return rows, nil
}

// Insert uploads objects to MinIO.
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

		// Get content type
		contentType := "application/octet-stream"
		if ct, ok := row["content_type"].(string); ok {
			contentType = ct
		}

		// Upload object
		_, err := d.conn.client.Client().PutObject(ctx, bucket, fullKey, bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{
			ContentType: contentType,
		})
		if err != nil {
			return count, fmt.Errorf("failed to upload object %s: %w", fullKey, err)
		}

		count++
	}

	return count, nil
}

// Update is not directly supported in MinIO (objects are immutable).
func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	// For MinIO, update is the same as insert (overwrite)
	return d.Insert(ctx, table, data)
}

// Upsert uploads or overwrites objects.
func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	// For MinIO, upsert is the same as insert
	return d.Insert(ctx, table, data)
}

// Delete removes objects from MinIO.
func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return 0, fmt.Errorf("no bucket specified")
	}

	// If key is specified in conditions, delete that specific object
	if key, ok := conditions["key"].(string); ok {
		err := d.conn.client.Client().RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{})
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

	// List and delete objects
	objectCh := d.conn.client.Client().ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})

	var count int64
	for object := range objectCh {
		if object.Err != nil {
			return count, fmt.Errorf("failed to list objects: %w", object.Err)
		}

		err := d.conn.client.Client().RemoveObject(ctx, bucket, object.Key, minio.RemoveObjectOptions{})
		if err != nil {
			return count, fmt.Errorf("failed to delete object %s: %w", object.Key, err)
		}
		count++
	}

	return count, nil
}

// Stream retrieves objects in batches.
func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return adapter.StreamResult{}, fmt.Errorf("no bucket specified")
	}

	prefix := ""
	if params.Table != "" && params.Table != "root" {
		prefix = params.Table + "/"
	}

	objectCh := d.conn.client.Client().ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
		MaxKeys:   int(params.BatchSize),
	})

	rows := make([]map[string]interface{}, 0)
	var lastKey string

	for object := range objectCh {
		if object.Err != nil {
			return adapter.StreamResult{}, fmt.Errorf("failed to list objects: %w", object.Err)
		}

		row := make(map[string]interface{})
		row["key"] = object.Key
		row["size"] = object.Size
		row["last_modified"] = object.LastModified
		row["etag"] = strings.Trim(object.ETag, "\"")

		rows = append(rows, row)
		lastKey = object.Key

		if len(rows) >= int(params.BatchSize) {
			break
		}
	}

	hasMore := len(rows) == int(params.BatchSize)

	return adapter.StreamResult{
		Data:       rows,
		HasMore:    hasMore,
		NextCursor: lastKey,
	}, nil
}

// ExecuteQuery is not supported for MinIO.
func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	return nil, fmt.Errorf("ExecuteQuery not supported for MinIO")
}

// ExecuteCountQuery counts objects in a prefix.
func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return 0, fmt.Errorf("no bucket specified")
	}

	objectCh := d.conn.client.Client().ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Recursive: true,
	})

	var count int64
	for object := range objectCh {
		if object.Err != nil {
			return 0, fmt.Errorf("failed to count objects: %w", object.Err)
		}
		count++
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

	objectCh := d.conn.client.Client().ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})

	var count int64
	for object := range objectCh {
		if object.Err != nil {
			return 0, false, fmt.Errorf("failed to count objects: %w", object.Err)
		}
		count++
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
	objectCh := d.conn.client.Client().ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Recursive: true,
	})

	// Delete all objects
	for object := range objectCh {
		if object.Err != nil {
			return fmt.Errorf("failed to list objects: %w", object.Err)
		}

		err := d.conn.client.Client().RemoveObject(ctx, bucket, object.Key, minio.RemoveObjectOptions{})
		if err != nil {
			return fmt.Errorf("failed to delete object %s: %w", object.Key, err)
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

	object, err := d.conn.client.Client().GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer object.Close()

	return io.ReadAll(object)
}

// PutObject uploads content to a specific object.
func (d *DataOps) PutObject(ctx context.Context, key string, content []byte, contentType string) error {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return fmt.Errorf("no bucket specified")
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}

	_, err := d.conn.client.Client().PutObject(ctx, bucket, key, bytes.NewReader(content), int64(len(content)), minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("failed to put object: %w", err)
	}

	return nil
}
