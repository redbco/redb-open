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
	}

	return model, nil
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
