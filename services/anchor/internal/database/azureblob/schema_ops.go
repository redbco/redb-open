package azureblob

import (
	"context"
	"fmt"
	"strings"

	azcontainer "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements schema operations for Azure Blob Storage.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema retrieves the "schema" of Azure Blob (containers and blob prefixes).
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	container := s.conn.client.GetContainer()
	if container == "" {
		return nil, fmt.Errorf("no container specified")
	}

	// List blobs with common prefixes to simulate "tables"
	prefixes, err := s.listPrefixes(ctx, container)
	if err != nil {
		return nil, err
	}

	tablesMap := make(map[string]unifiedmodel.Table)

	// Create columns map
	columnsMap := map[string]unifiedmodel.Column{
		"name":          {Name: "name", DataType: "string", Nullable: false},
		"size":          {Name: "size", DataType: "integer", Nullable: true},
		"last_modified": {Name: "last_modified", DataType: "timestamp", Nullable: true},
		"content_type":  {Name: "content_type", DataType: "string", Nullable: true},
		"content_md5":   {Name: "content_md5", DataType: "string", Nullable: true},
		"etag":          {Name: "etag", DataType: "string", Nullable: true},
		"tier":          {Name: "tier", DataType: "string", Nullable: true},
	}

	// Add a "root" table for blobs at the container root
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

	// Also list blobs and create Blob entries (sample only)
	if err := s.discoverBlobs(ctx, container, model); err != nil {
		return nil, err
	}

	return model, nil
}

// discoverBlobs lists blobs in the container and adds them as Blob entries (sample only).
func (s *SchemaOps) discoverBlobs(ctx context.Context, container string, model *unifiedmodel.UnifiedModel) error {
	containerClient := s.conn.client.Client().ServiceClient().NewContainerClient(container)

	// List a sample of blobs (limit to prevent large listings)
	const maxBlobs = 100
	pager := containerClient.NewListBlobsFlatPager(&azcontainer.ListBlobsFlatOptions{
		MaxResults: int32Ptr(int32(maxBlobs)),
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list blobs: %w", err)
		}

		for _, blobItem := range page.Segment.BlobItems {
			if blobItem.Name == nil {
				continue
			}

			name := *blobItem.Name
			var contentType, etag, tier, encryption string
			var size int64

			if blobItem.Properties != nil {
				if blobItem.Properties.ContentLength != nil {
					size = *blobItem.Properties.ContentLength
				}
				if blobItem.Properties.ContentType != nil {
					contentType = *blobItem.Properties.ContentType
				}
				if blobItem.Properties.ETag != nil {
					etag = string(*blobItem.Properties.ETag)
				}
				if blobItem.Properties.AccessTier != nil {
					tier = string(*blobItem.Properties.AccessTier)
				}
				if blobItem.Properties.ServerEncrypted != nil && *blobItem.Properties.ServerEncrypted {
					encryption = "server-side"
				}
			}

			blob := unifiedmodel.Blob{
				Name:         name,
				Bucket:       container,
				Path:         name,
				Size:         size,
				ContentType:  contentType,
				StorageClass: tier,
				Encryption:   encryption,
				ETag:         etag,
				Options: map[string]any{
					"container": container,
				},
			}

			if blobItem.Properties != nil && blobItem.Properties.LastModified != nil {
				blob.Options["last_modified"] = blobItem.Properties.LastModified.String()
			}

			model.Blobs[name] = blob
		}

		// Only process first page (sample)
		break
	}

	return nil
}

// int32Ptr is a helper function to get a pointer to an int32.
func int32Ptr(v int32) *int32 {
	return &v
}

// listPrefixes lists common prefixes (simulating "folders") in the container.
func (s *SchemaOps) listPrefixes(ctx context.Context, container string) ([]string, error) {
	containerClient := s.conn.client.Client().ServiceClient().NewContainerClient(container)

	pager := containerClient.NewListBlobsHierarchyPager("/", &azcontainer.ListBlobsHierarchyOptions{
		Prefix: nil,
	})

	prefixes := make([]string, 0)
	seen := make(map[string]bool)

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list blobs: %w", err)
		}

		// Add blob prefixes
		for _, prefix := range page.Segment.BlobPrefixes {
			if prefix.Name != nil {
				prefixName := strings.TrimSuffix(*prefix.Name, "/")
				if !seen[prefixName] && prefixName != "" {
					seen[prefixName] = true
					prefixes = append(prefixes, prefixName)
				}
			}
		}
	}

	return prefixes, nil
}

// CreateStructure creates Azure Blob "structure" (prefixes).
// Note: Azure Blob doesn't require explicit structure creation.
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	// Azure Blob doesn't need explicit schema creation
	// Prefixes are created implicitly when blobs are uploaded
	return nil
}

// ListTables lists all "tables" (prefixes) in the container.
func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	container := s.conn.client.GetContainer()
	if container == "" {
		return nil, fmt.Errorf("no container specified")
	}

	prefixes, err := s.listPrefixes(ctx, container)
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
		"last_modified": {Name: "last_modified", DataType: "timestamp", Nullable: true},
		"content_type":  {Name: "content_type", DataType: "string", Nullable: true},
		"content_md5":   {Name: "content_md5", DataType: "string", Nullable: true},
		"etag":          {Name: "etag", DataType: "string", Nullable: true},
		"tier":          {Name: "tier", DataType: "string", Nullable: true},
	}

	table := &unifiedmodel.Table{
		Name:    tableName,
		Columns: columnsMap,
	}

	return table, nil
}
