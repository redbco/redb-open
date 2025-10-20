package gcs

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"google.golang.org/api/iterator"
)

// DataOps implements data operations for GCS.
type DataOps struct {
	conn *Connection
}

// Fetch retrieves objects from GCS (treating them as "rows").
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

	query := &storage.Query{
		Prefix: prefix,
	}

	it := d.conn.client.Client().Bucket(bucket).Objects(ctx, query)

	rows := make([]map[string]interface{}, 0)
	count := 0

	for {
		if count >= limit {
			break
		}

		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		// Skip prefix entries
		if attrs.Prefix != "" {
			continue
		}

		row := make(map[string]interface{})
		row["name"] = attrs.Name
		row["size"] = attrs.Size
		row["updated"] = attrs.Updated
		row["content_type"] = attrs.ContentType
		row["storage_class"] = attrs.StorageClass

		if len(attrs.MD5) > 0 {
			row["md5"] = base64.StdEncoding.EncodeToString(attrs.MD5)
		}
		if attrs.CRC32C != 0 {
			row["crc32c"] = attrs.CRC32C
		}

		rows = append(rows, row)
		count++
	}

	return rows, nil
}

// Insert uploads objects to GCS.
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
		name, ok := row["name"].(string)
		if !ok || name == "" {
			continue
		}

		// Add prefix if not already present
		fullName := name
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			fullName = prefix + name
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
		writer := d.conn.client.Client().Bucket(bucket).Object(fullName).NewWriter(ctx)
		writer.ContentType = contentType

		_, err := writer.Write(body)
		if err != nil {
			writer.Close()
			return count, fmt.Errorf("failed to write object %s: %w", fullName, err)
		}

		err = writer.Close()
		if err != nil {
			return count, fmt.Errorf("failed to upload object %s: %w", fullName, err)
		}

		count++
	}

	return count, nil
}

// Update is not directly supported in GCS (objects are immutable).
func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	// For GCS, update is the same as insert (overwrite)
	return d.Insert(ctx, table, data)
}

// Upsert uploads or overwrites objects.
func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	// For GCS, upsert is the same as insert
	return d.Insert(ctx, table, data)
}

// Delete removes objects from GCS.
func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return 0, fmt.Errorf("no bucket specified")
	}

	// If name is specified in conditions, delete that specific object
	if name, ok := conditions["name"].(string); ok {
		err := d.conn.client.Client().Bucket(bucket).Object(name).Delete(ctx)
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
	query := &storage.Query{
		Prefix: prefix,
	}

	it := d.conn.client.Client().Bucket(bucket).Objects(ctx, query)

	var count int64
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return count, fmt.Errorf("failed to list objects: %w", err)
		}

		// Skip prefix entries
		if attrs.Prefix != "" {
			continue
		}

		err = d.conn.client.Client().Bucket(bucket).Object(attrs.Name).Delete(ctx)
		if err != nil {
			return count, fmt.Errorf("failed to delete object %s: %w", attrs.Name, err)
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

	query := &storage.Query{
		Prefix: prefix,
	}

	it := d.conn.client.Client().Bucket(bucket).Objects(ctx, query)

	rows := make([]map[string]interface{}, 0)
	var lastName string
	count := 0

	for {
		if count >= int(params.BatchSize) {
			break
		}

		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return adapter.StreamResult{}, fmt.Errorf("failed to list objects: %w", err)
		}

		// Skip prefix entries
		if attrs.Prefix != "" {
			continue
		}

		row := make(map[string]interface{})
		row["name"] = attrs.Name
		row["size"] = attrs.Size
		row["updated"] = attrs.Updated
		row["content_type"] = attrs.ContentType

		rows = append(rows, row)
		lastName = attrs.Name
		count++
	}

	hasMore := count == int(params.BatchSize)

	return adapter.StreamResult{
		Data:       rows,
		HasMore:    hasMore,
		NextCursor: lastName,
	}, nil
}

// ExecuteQuery is not supported for GCS.
func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	return nil, fmt.Errorf("ExecuteQuery not supported for GCS")
}

// ExecuteCountQuery counts objects in a prefix.
func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return 0, fmt.Errorf("no bucket specified")
	}

	it := d.conn.client.Client().Bucket(bucket).Objects(ctx, nil)

	var count int64
	for {
		_, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("failed to count objects: %w", err)
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

	query := &storage.Query{
		Prefix: prefix,
	}

	it := d.conn.client.Client().Bucket(bucket).Objects(ctx, query)

	var count int64
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, false, fmt.Errorf("failed to count objects: %w", err)
		}

		// Skip prefix entries
		if attrs.Prefix != "" {
			continue
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
	it := d.conn.client.Client().Bucket(bucket).Objects(ctx, nil)

	// Delete all objects
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		// Skip prefix entries
		if attrs.Prefix != "" {
			continue
		}

		err = d.conn.client.Client().Bucket(bucket).Object(attrs.Name).Delete(ctx)
		if err != nil {
			return fmt.Errorf("failed to delete object %s: %w", attrs.Name, err)
		}
	}

	return nil
}

// GetObject retrieves the content of a specific object.
func (d *DataOps) GetObject(ctx context.Context, name string) ([]byte, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return nil, fmt.Errorf("no bucket specified")
	}

	reader, err := d.conn.client.Client().Bucket(bucket).Object(name).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

// PutObject uploads content to a specific object.
func (d *DataOps) PutObject(ctx context.Context, name string, content []byte, contentType string) error {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return fmt.Errorf("no bucket specified")
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}

	writer := d.conn.client.Client().Bucket(bucket).Object(name).NewWriter(ctx)
	writer.ContentType = contentType

	_, err := io.Copy(writer, bytes.NewReader(content))
	if err != nil {
		writer.Close()
		return fmt.Errorf("failed to write object: %w", err)
	}

	err = writer.Close()
	if err != nil {
		return fmt.Errorf("failed to upload object: %w", err)
	}

	return nil
}
