package azureblob

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	azcontainer "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// DataOps implements data operations for Azure Blob Storage.
type DataOps struct {
	conn *Connection
}

// Fetch retrieves blobs from Azure Blob Storage (treating them as "rows").
func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
	return d.FetchWithColumns(ctx, table, nil, limit)
}

// FetchWithColumns retrieves blobs with specific metadata fields.
func (d *DataOps) FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error) {
	container := d.conn.client.GetContainer()
	if container == "" {
		return nil, fmt.Errorf("no container specified")
	}

	// Convert table name to prefix
	prefix := ""
	if table != "" && table != "root" {
		prefix = table + "/"
	}

	containerClient := d.conn.client.Client().ServiceClient().NewContainerClient(container)

	pager := containerClient.NewListBlobsFlatPager(&azcontainer.ListBlobsFlatOptions{
		Prefix:     to.Ptr(prefix),
		MaxResults: to.Ptr(int32(limit)),
	})

	rows := make([]map[string]interface{}, 0)

	for pager.More() && len(rows) < limit {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list blobs: %w", err)
		}

		for _, blobItem := range page.Segment.BlobItems {
			if blobItem.Name == nil {
				continue
			}

			row := make(map[string]interface{})
			row["name"] = *blobItem.Name

			if blobItem.Properties != nil {
				if blobItem.Properties.ContentLength != nil {
					row["size"] = *blobItem.Properties.ContentLength
				}
				if blobItem.Properties.LastModified != nil {
					row["last_modified"] = *blobItem.Properties.LastModified
				}
				if blobItem.Properties.ContentType != nil {
					row["content_type"] = *blobItem.Properties.ContentType
				}
				if blobItem.Properties.ContentMD5 != nil {
					row["content_md5"] = base64.StdEncoding.EncodeToString(blobItem.Properties.ContentMD5)
				}
				if blobItem.Properties.ETag != nil {
					row["etag"] = string(*blobItem.Properties.ETag)
				}
				if blobItem.Properties.AccessTier != nil {
					row["tier"] = string(*blobItem.Properties.AccessTier)
				}
			}

			rows = append(rows, row)

			if len(rows) >= limit {
				break
			}
		}
	}

	return rows, nil
}

// Insert uploads blobs to Azure Blob Storage.
func (d *DataOps) Insert(ctx context.Context, table string, data []map[string]interface{}) (int64, error) {
	container := d.conn.client.GetContainer()
	if container == "" {
		return 0, fmt.Errorf("no container specified")
	}

	prefix := ""
	if table != "" && table != "root" {
		prefix = table + "/"
	}

	containerClient := d.conn.client.Client().ServiceClient().NewContainerClient(container)

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

		// Upload blob
		blobClient := containerClient.NewBlockBlobClient(fullName)
		_, err := blobClient.UploadBuffer(ctx, body, &blockblob.UploadBufferOptions{
			HTTPHeaders: &blob.HTTPHeaders{
				BlobContentType: to.Ptr(contentType),
			},
		})
		if err != nil {
			return count, fmt.Errorf("failed to upload blob %s: %w", fullName, err)
		}

		count++
	}

	return count, nil
}

// Update is not directly supported in Azure Blob (blobs are immutable).
func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	// For Azure Blob, update is the same as insert (overwrite)
	return d.Insert(ctx, table, data)
}

// Upsert uploads or overwrites blobs.
func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	// For Azure Blob, upsert is the same as insert
	return d.Insert(ctx, table, data)
}

// Delete removes blobs from Azure Blob Storage.
func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
	container := d.conn.client.GetContainer()
	if container == "" {
		return 0, fmt.Errorf("no container specified")
	}

	containerClient := d.conn.client.Client().ServiceClient().NewContainerClient(container)

	// If name is specified in conditions, delete that specific blob
	if name, ok := conditions["name"].(string); ok {
		blobClient := containerClient.NewBlobClient(name)
		_, err := blobClient.Delete(ctx, nil)
		if err != nil {
			return 0, fmt.Errorf("failed to delete blob: %w", err)
		}
		return 1, nil
	}

	// Otherwise, delete all blobs in the prefix
	prefix := ""
	if table != "" && table != "root" {
		prefix = table + "/"
	}

	// List and delete blobs
	pager := containerClient.NewListBlobsFlatPager(&azcontainer.ListBlobsFlatOptions{
		Prefix: to.Ptr(prefix),
	})

	var count int64
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return count, fmt.Errorf("failed to list blobs: %w", err)
		}

		for _, blobItem := range page.Segment.BlobItems {
			if blobItem.Name == nil {
				continue
			}

			blobClient := containerClient.NewBlobClient(*blobItem.Name)
			_, err = blobClient.Delete(ctx, nil)
			if err != nil {
				return count, fmt.Errorf("failed to delete blob %s: %w", *blobItem.Name, err)
			}
			count++
		}
	}

	return count, nil
}

// Stream retrieves blobs in batches.
func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	container := d.conn.client.GetContainer()
	if container == "" {
		return adapter.StreamResult{}, fmt.Errorf("no container specified")
	}

	prefix := ""
	if params.Table != "" && params.Table != "root" {
		prefix = params.Table + "/"
	}

	containerClient := d.conn.client.Client().ServiceClient().NewContainerClient(container)

	pager := containerClient.NewListBlobsFlatPager(&azcontainer.ListBlobsFlatOptions{
		Prefix:     to.Ptr(prefix),
		MaxResults: to.Ptr(int32(params.BatchSize)),
	})

	rows := make([]map[string]interface{}, 0)
	var lastName string

	if pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return adapter.StreamResult{}, fmt.Errorf("failed to list blobs: %w", err)
		}

		for _, blobItem := range page.Segment.BlobItems {
			if blobItem.Name == nil {
				continue
			}

			row := make(map[string]interface{})
			row["name"] = *blobItem.Name

			if blobItem.Properties != nil {
				if blobItem.Properties.ContentLength != nil {
					row["size"] = *blobItem.Properties.ContentLength
				}
				if blobItem.Properties.LastModified != nil {
					row["last_modified"] = *blobItem.Properties.LastModified
				}
			}

			rows = append(rows, row)
			lastName = *blobItem.Name

			if len(rows) >= int(params.BatchSize) {
				break
			}
		}
	}

	hasMore := len(rows) == int(params.BatchSize)

	return adapter.StreamResult{
		Data:       rows,
		HasMore:    hasMore,
		NextCursor: lastName,
	}, nil
}

// ExecuteQuery is not supported for Azure Blob Storage.
func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	return nil, fmt.Errorf("ExecuteQuery not supported for Azure Blob Storage")
}

// ExecuteCountQuery counts blobs in a prefix.
func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	container := d.conn.client.GetContainer()
	if container == "" {
		return 0, fmt.Errorf("no container specified")
	}

	containerClient := d.conn.client.Client().ServiceClient().NewContainerClient(container)

	pager := containerClient.NewListBlobsFlatPager(nil)

	var count int64
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to count blobs: %w", err)
		}
		count += int64(len(page.Segment.BlobItems))
	}

	return count, nil
}

// GetRowCount returns the number of blobs in a prefix.
func (d *DataOps) GetRowCount(ctx context.Context, table string, whereClause string) (int64, bool, error) {
	container := d.conn.client.GetContainer()
	if container == "" {
		return 0, false, fmt.Errorf("no container specified")
	}

	prefix := ""
	if table != "" && table != "root" {
		prefix = table + "/"
	}

	containerClient := d.conn.client.Client().ServiceClient().NewContainerClient(container)

	pager := containerClient.NewListBlobsFlatPager(&azcontainer.ListBlobsFlatOptions{
		Prefix: to.Ptr(prefix),
	})

	var count int64
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return 0, false, fmt.Errorf("failed to count blobs: %w", err)
		}
		count += int64(len(page.Segment.BlobItems))
	}

	return count, true, nil
}

// Wipe deletes all blobs in the container.
func (d *DataOps) Wipe(ctx context.Context) error {
	container := d.conn.client.GetContainer()
	if container == "" {
		return fmt.Errorf("no container specified")
	}

	containerClient := d.conn.client.Client().ServiceClient().NewContainerClient(container)

	// List all blobs
	pager := containerClient.NewListBlobsFlatPager(nil)

	// Delete all blobs
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list blobs: %w", err)
		}

		for _, blobItem := range page.Segment.BlobItems {
			if blobItem.Name == nil {
				continue
			}

			blobClient := containerClient.NewBlobClient(*blobItem.Name)
			_, err = blobClient.Delete(ctx, nil)
			if err != nil {
				return fmt.Errorf("failed to delete blob %s: %w", *blobItem.Name, err)
			}
		}
	}

	return nil
}

// GetBlob retrieves the content of a specific blob.
func (d *DataOps) GetBlob(ctx context.Context, name string) ([]byte, error) {
	container := d.conn.client.GetContainer()
	if container == "" {
		return nil, fmt.Errorf("no container specified")
	}

	containerClient := d.conn.client.Client().ServiceClient().NewContainerClient(container)
	blobClient := containerClient.NewBlobClient(name)

	response, err := blobClient.DownloadStream(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get blob: %w", err)
	}
	defer response.Body.Close()

	return io.ReadAll(response.Body)
}

// PutBlob uploads content to a specific blob.
func (d *DataOps) PutBlob(ctx context.Context, name string, content []byte, contentType string) error {
	container := d.conn.client.GetContainer()
	if container == "" {
		return fmt.Errorf("no container specified")
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}

	containerClient := d.conn.client.Client().ServiceClient().NewContainerClient(container)
	blobClient := containerClient.NewBlockBlobClient(name)

	_, err := blobClient.UploadBuffer(ctx, content, &blockblob.UploadBufferOptions{
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: to.Ptr(contentType),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to put blob: %w", err)
	}

	return nil
}
