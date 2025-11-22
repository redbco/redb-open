package minio

import (
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements schema operations for MinIO.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema retrieves the "schema" of MinIO (buckets and object prefixes).
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	bucket := s.conn.client.GetBucket()
	if bucket == "" {
		return nil, fmt.Errorf("no bucket specified")
	}

	// List objects with common prefixes to simulate "tables"
	prefixes, err := s.listPrefixes(ctx, bucket)
	if err != nil {
		return nil, err
	}

	tablesMap := make(map[string]unifiedmodel.Table)

	// Create columns map
	columnsMap := map[string]unifiedmodel.Column{
		"key":           {Name: "key", DataType: "string", Nullable: false},
		"size":          {Name: "size", DataType: "integer", Nullable: true},
		"last_modified": {Name: "last_modified", DataType: "timestamp", Nullable: true},
		"etag":          {Name: "etag", DataType: "string", Nullable: true},
		"storage_class": {Name: "storage_class", DataType: "string", Nullable: true},
	}

	// Add a "root" table for objects at the bucket root
	tablesMap["root"] = unifiedmodel.Table{
		Name:    "root",
		Columns: columnsMap,
	}

	// Add a table for each prefix
	for _, prefix := range prefixes {
		tablesMap[prefix] = unifiedmodel.Table{
			Name:    prefix,
			Columns: columnsMap,
		}
	}

	model := &unifiedmodel.UnifiedModel{
		DatabaseType: s.conn.Type(),
		Tables:       tablesMap,
		Blobs:        make(map[string]unifiedmodel.Blob),
	}

	// Also list objects and create Blob entries (sample only)
	if err := s.discoverBlobs(ctx, bucket, model); err != nil {
		return nil, err
	}

	return model, nil
}

// discoverBlobs lists objects in the bucket and adds them as Blob entries (sample only).
func (s *SchemaOps) discoverBlobs(ctx context.Context, bucket string, model *unifiedmodel.UnifiedModel) error {
	// List a sample of objects (limit to prevent large listings)
	const maxObjects = 100

	objectCh := s.conn.client.Client().ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:       "",
		Recursive:    true,
		MaxKeys:      maxObjects,
		WithMetadata: true,
	})

	for object := range objectCh {
		if object.Err != nil {
			return fmt.Errorf("failed to list objects: %w", object.Err)
		}

		// Skip directory markers
		if len(object.Key) > 0 && object.Key[len(object.Key)-1] == '/' {
			continue
		}

		blob := unifiedmodel.Blob{
			Name:         object.Key,
			Bucket:       bucket,
			Path:         object.Key,
			Size:         object.Size,
			ContentType:  object.ContentType,
			StorageClass: object.StorageClass,
			Encryption:   "", // MinIO encryption info is not in ObjectInfo
			ETag:         object.ETag,
			Options: map[string]any{
				"bucket": bucket,
			},
		}

		if !object.LastModified.IsZero() {
			blob.Options["last_modified"] = object.LastModified.String()
		}

		if object.UserMetadata != nil && len(object.UserMetadata) > 0 {
			blob.Options["metadata"] = object.UserMetadata
		}

		model.Blobs[object.Key] = blob
	}

	return nil
}

// listPrefixes lists common prefixes (simulating "folders") in the bucket.
func (s *SchemaOps) listPrefixes(ctx context.Context, bucket string) ([]string, error) {
	objectCh := s.conn.client.Client().ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    "",
		Recursive: false,
	})

	prefixes := make([]string, 0)
	seen := make(map[string]bool)

	for object := range objectCh {
		if object.Err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", object.Err)
		}

		// Extract prefix from key
		if idx := len(object.Key) - 1; idx >= 0 && object.Key[idx] == '/' {
			prefix := object.Key[:idx]
			if !seen[prefix] && prefix != "" {
				seen[prefix] = true
				prefixes = append(prefixes, prefix)
			}
		}
	}

	return prefixes, nil
}

// CreateStructure creates MinIO "structure" (prefixes).
// Note: MinIO doesn't require explicit structure creation.
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	// MinIO doesn't need explicit schema creation
	// Prefixes are created implicitly when objects are uploaded
	return nil
}

// ListTables lists all "tables" (prefixes) in the bucket.
func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	bucket := s.conn.client.GetBucket()
	if bucket == "" {
		return nil, fmt.Errorf("no bucket specified")
	}

	prefixes, err := s.listPrefixes(ctx, bucket)
	if err != nil {
		return nil, err
	}

	// Add root as a "table"
	tables := []string{"root"}
	tables = append(tables, prefixes...)

	return tables, nil
}

// GetTableSchema retrieves the schema for a specific "table" (prefix).
func (s *SchemaOps) GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error) {
	columnsMap := map[string]unifiedmodel.Column{
		"key":           {Name: "key", DataType: "string", Nullable: false},
		"size":          {Name: "size", DataType: "integer", Nullable: true},
		"last_modified": {Name: "last_modified", DataType: "timestamp", Nullable: true},
		"etag":          {Name: "etag", DataType: "string", Nullable: true},
		"storage_class": {Name: "storage_class", DataType: "string", Nullable: true},
		"content_type":  {Name: "content_type", DataType: "string", Nullable: true},
	}

	table := &unifiedmodel.Table{
		Name:    tableName,
		Columns: columnsMap,
	}

	return table, nil
}
