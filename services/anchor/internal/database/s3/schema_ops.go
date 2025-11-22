package s3

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements schema operations for S3.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema retrieves the "schema" of S3 (buckets and object prefixes).
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

	tables := make([]*unifiedmodel.Table, 0, len(prefixes)+1)

	// Create columns map
	columnsMap := map[string]unifiedmodel.Column{
		"key":           {Name: "key", DataType: "string", Nullable: false},
		"size":          {Name: "size", DataType: "integer", Nullable: true},
		"last_modified": {Name: "last_modified", DataType: "timestamp", Nullable: true},
		"etag":          {Name: "etag", DataType: "string", Nullable: true},
		"storage_class": {Name: "storage_class", DataType: "string", Nullable: true},
	}

	// Add a "root" table for objects at the bucket root
	tables = append(tables, &unifiedmodel.Table{
		Name:    "root",
		Columns: columnsMap,
	})

	// Add a table for each prefix
	for _, prefix := range prefixes {
		tables = append(tables, &unifiedmodel.Table{
			Name:    prefix,
			Columns: columnsMap,
		})
	}

	// Convert tables slice to map
	tablesMap := make(map[string]unifiedmodel.Table)
	for _, t := range tables {
		tablesMap[t.Name] = *t
	}

	// Also list objects and create Blobs (primary container for object storage)
	blobs, err := s.discoverBlobs(ctx, bucket)
	if err != nil {
		return nil, err
	}

	model := &unifiedmodel.UnifiedModel{
		DatabaseType: s.conn.Type(),
		Tables:       tablesMap,
		Blobs:        blobs,
	}

	return model, nil
}

// discoverBlobs lists objects in the bucket and converts them to Blob entries (sample only).
func (s *SchemaOps) discoverBlobs(ctx context.Context, bucket string) (map[string]unifiedmodel.Blob, error) {
	// List a sample of objects (limit to prevent large listings)
	const maxObjects = 100
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int32(int32(maxObjects)),
	}

	result, err := s.conn.client.Client().ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	blobs := make(map[string]unifiedmodel.Blob)
	for _, obj := range result.Contents {
		if obj.Key == nil {
			continue
		}

		key := *obj.Key
		var storageClass, encryption, etag string
		var size int64

		if obj.StorageClass != "" {
			storageClass = string(obj.StorageClass)
		}
		if obj.ETag != nil {
			etag = *obj.ETag
		}
		if obj.Size != nil {
			size = *obj.Size
		}

		blob := unifiedmodel.Blob{
			Name:         key,
			Bucket:       bucket,
			Path:         key,
			Size:         size,
			ContentType:  "", // Would need HeadObject call to get content type
			StorageClass: storageClass,
			Encryption:   encryption,
			ETag:         etag,
			Options: map[string]any{
				"bucket": bucket,
			},
		}

		if obj.LastModified != nil {
			blob.Options["last_modified"] = obj.LastModified.String()
		}

		blobs[key] = blob
	}

	return blobs, nil
}

// listPrefixes lists common prefixes (simulating "folders") in the bucket.
func (s *SchemaOps) listPrefixes(ctx context.Context, bucket string) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int32(1000),
	}

	result, err := s.conn.client.Client().ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to list prefixes: %w", err)
	}

	prefixes := make([]string, 0, len(result.CommonPrefixes))
	for _, prefix := range result.CommonPrefixes {
		if prefix.Prefix != nil {
			// Remove trailing slash
			p := *prefix.Prefix
			if len(p) > 0 && p[len(p)-1] == '/' {
				p = p[:len(p)-1]
			}
			prefixes = append(prefixes, p)
		}
	}

	return prefixes, nil
}

// CreateStructure creates S3 "structure" (prefixes) from a UnifiedModel.
// Note: S3 doesn't have explicit folders, they're simulated by object keys.
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	// For S3, we don't need to create explicit structure
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
