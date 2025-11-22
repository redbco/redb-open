package gcs

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"google.golang.org/api/iterator"
)

// SchemaOps implements schema operations for GCS.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema retrieves the "schema" of GCS (buckets and object prefixes).
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
		"name":          {Name: "name", DataType: "string", Nullable: false},
		"size":          {Name: "size", DataType: "integer", Nullable: true},
		"updated":       {Name: "updated", DataType: "timestamp", Nullable: true},
		"content_type":  {Name: "content_type", DataType: "string", Nullable: true},
		"md5":           {Name: "md5", DataType: "string", Nullable: true},
		"crc32c":        {Name: "crc32c", DataType: "string", Nullable: true},
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

	// Also list objects and create Blobs (primary container for object storage)
	if err := s.discoverBlobs(ctx, bucket, model); err != nil {
		return nil, err
	}

	return model, nil
}

// discoverBlobs lists objects in the bucket and adds them as Blob entries (sample only).
func (s *SchemaOps) discoverBlobs(ctx context.Context, bucket string, model *unifiedmodel.UnifiedModel) error {
	// List a sample of objects (limit to prevent large listings)
	const maxObjects = 100

	query := &storage.Query{}

	it := s.conn.client.Client().Bucket(bucket).Objects(ctx, query)

	count := 0
	for count < maxObjects {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		// Skip directories (prefixes)
		if attrs.Prefix != "" {
			continue
		}

		blob := unifiedmodel.Blob{
			Name:         attrs.Name,
			Bucket:       bucket,
			Path:         attrs.Name,
			Size:         attrs.Size,
			ContentType:  attrs.ContentType,
			StorageClass: string(attrs.StorageClass),
			Encryption:   "",
			ETag:         fmt.Sprintf("%x", attrs.MD5),
			Options: map[string]any{
				"generation":     attrs.Generation,
				"metageneration": attrs.Metageneration,
				"crc32c":         attrs.CRC32C,
			},
		}

		if !attrs.Updated.IsZero() {
			blob.Options["updated"] = attrs.Updated.String()
		}

		model.Blobs[attrs.Name] = blob
		count++
	}

	return nil
}

// listPrefixes lists common prefixes (simulating "folders") in the bucket.
func (s *SchemaOps) listPrefixes(ctx context.Context, bucket string) ([]string, error) {
	query := &storage.Query{
		Delimiter: "/",
		Prefix:    "",
	}

	it := s.conn.client.Client().Bucket(bucket).Objects(ctx, query)

	prefixes := make([]string, 0)
	seen := make(map[string]bool)

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		// Check for common prefix (delimiter match)
		if attrs.Prefix != "" {
			prefix := strings.TrimSuffix(attrs.Prefix, "/")
			if !seen[prefix] && prefix != "" {
				seen[prefix] = true
				prefixes = append(prefixes, prefix)
			}
		}
	}

	return prefixes, nil
}

// CreateStructure creates GCS "structure" (prefixes).
// Note: GCS doesn't require explicit structure creation.
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	// GCS doesn't need explicit schema creation
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
		"name":          {Name: "name", DataType: "string", Nullable: false},
		"size":          {Name: "size", DataType: "integer", Nullable: true},
		"updated":       {Name: "updated", DataType: "timestamp", Nullable: true},
		"content_type":  {Name: "content_type", DataType: "string", Nullable: true},
		"md5":           {Name: "md5", DataType: "string", Nullable: true},
		"crc32c":        {Name: "crc32c", DataType: "string", Nullable: true},
		"storage_class": {Name: "storage_class", DataType: "string", Nullable: true},
	}

	table := &unifiedmodel.Table{
		Name:    tableName,
		Columns: columnsMap,
	}

	return table, nil
}
