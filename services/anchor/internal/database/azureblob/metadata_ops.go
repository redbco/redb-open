package azureblob

import (
	"context"
	"fmt"

	azcontainer "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

// MetadataOps implements metadata operations for Azure Blob Storage.
type MetadataOps struct {
	conn         *Connection
	instanceConn *InstanceConnection
}

// CollectDatabaseMetadata collects metadata about the Azure Blob container.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	if m.conn == nil {
		return nil, fmt.Errorf("no connection available")
	}

	container := m.conn.client.GetContainer()
	if container == "" {
		return nil, fmt.Errorf("no container specified")
	}

	metadata := make(map[string]interface{})
	metadata["container_name"] = container
	metadata["database_type"] = "azureblob"

	// Get container properties
	containerClient := m.conn.client.Client().ServiceClient().NewContainerClient(container)
	props, err := containerClient.GetProperties(ctx, nil)
	if err == nil {
		if props.LastModified != nil {
			metadata["last_modified"] = *props.LastModified
		}
		if props.ETag != nil {
			metadata["etag"] = string(*props.ETag)
		}
		if props.Metadata != nil {
			metadata["metadata"] = props.Metadata
		}
	}

	// Count blobs
	count, err := m.countBlobs(ctx, m.conn.client, container)
	if err == nil {
		metadata["blob_count"] = count
	}

	return metadata, nil
}

// CollectInstanceMetadata collects metadata about the Azure Blob Storage account.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	var client *AzureBlobClient

	if m.conn != nil {
		client = m.conn.client
	} else if m.instanceConn != nil {
		client = m.instanceConn.client
	} else {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})
	metadata["database_type"] = "azureblob"
	metadata["account_name"] = client.accountName

	// List containers
	containers, err := client.ListContainers(ctx)
	if err == nil {
		metadata["container_count"] = len(containers)
		metadata["containers"] = containers
	}

	return metadata, nil
}

// GetVersion returns the Azure Blob Storage version.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	return "Azure Blob Storage", nil
}

// GetUniqueIdentifier returns the container name as unique identifier.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	if m.conn != nil {
		container := m.conn.client.GetContainer()
		return fmt.Sprintf("azureblob::%s", container), nil
	}

	if m.instanceConn != nil {
		return "azureblob::instance", nil
	}

	return "azureblob::unknown", nil
}

// GetDatabaseSize returns the total size of blobs in the container.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	if m.conn == nil {
		return 0, fmt.Errorf("no connection available")
	}

	container := m.conn.client.GetContainer()
	if container == "" {
		return 0, fmt.Errorf("no container specified")
	}

	containerClient := m.conn.client.Client().ServiceClient().NewContainerClient(container)

	pager := containerClient.NewListBlobsFlatPager(nil)

	var totalSize int64
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to list blobs: %w", err)
		}

		for _, blobItem := range page.Segment.BlobItems {
			if blobItem.Properties != nil && blobItem.Properties.ContentLength != nil {
				totalSize += *blobItem.Properties.ContentLength
			}
		}
	}

	return totalSize, nil
}

// GetTableCount returns the number of "tables" (prefixes) in the container.
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	if m.conn == nil {
		return 0, fmt.Errorf("no connection available")
	}

	container := m.conn.client.GetContainer()
	if container == "" {
		return 0, fmt.Errorf("no container specified")
	}

	containerClient := m.conn.client.Client().ServiceClient().NewContainerClient(container)

	pager := containerClient.NewListBlobsHierarchyPager("/", &azcontainer.ListBlobsHierarchyOptions{})

	count := 1 // Start with root
	seen := make(map[string]bool)

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to list blobs: %w", err)
		}

		// Count blob prefixes
		for _, prefix := range page.Segment.BlobPrefixes {
			if prefix.Name != nil && !seen[*prefix.Name] {
				seen[*prefix.Name] = true
				count++
			}
		}
	}

	return count, nil
}

// ExecuteCommand is not supported for Azure Blob Storage.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	return nil, fmt.Errorf("ExecuteCommand not supported for Azure Blob Storage")
}

// countBlobs counts the total number of blobs in a container.
func (m *MetadataOps) countBlobs(ctx context.Context, client *AzureBlobClient, container string) (int64, error) {
	containerClient := client.Client().ServiceClient().NewContainerClient(container)

	pager := containerClient.NewListBlobsFlatPager(nil)

	var count int64
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return 0, err
		}
		count += int64(len(page.Segment.BlobItems))
	}

	return count, nil
}
