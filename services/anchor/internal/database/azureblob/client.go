package azureblob

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// AzureBlobClient wraps the Azure Blob client with reDB-specific functionality.
type AzureBlobClient struct {
	client        *azblob.Client
	container     string // Container name (treated as "database")
	accountName   string
	accountKey    string
	connectionStr string
}

// NewAzureBlobClient creates a new Azure Blob client from a database connection config.
func NewAzureBlobClient(ctx context.Context, cfg adapter.ConnectionConfig) (*AzureBlobClient, error) {
	var client *azblob.Client
	var err error

	// Use connection string if provided
	if cfg.ConnectionString != "" {
		client, err = azblob.NewClientFromConnectionString(cfg.ConnectionString, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob client from connection string: %w", err)
		}
	} else {
		// Build connection string from components
		accountName := cfg.Username
		accountKey := cfg.Password

		if accountName == "" || accountKey == "" {
			return nil, fmt.Errorf("Azure Blob requires account name and account key")
		}

		connStr := fmt.Sprintf("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net",
			accountName, accountKey)

		client, err = azblob.NewClientFromConnectionString(connStr, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob client: %w", err)
		}
	}

	return &AzureBlobClient{
		client:        client,
		container:     cfg.DatabaseName, // In Azure Blob, container = database
		accountName:   cfg.Username,
		accountKey:    cfg.Password,
		connectionStr: cfg.ConnectionString,
	}, nil
}

// NewAzureBlobClientFromInstance creates a new Azure Blob client from an instance config.
func NewAzureBlobClientFromInstance(ctx context.Context, cfg adapter.InstanceConfig) (*AzureBlobClient, error) {
	// Convert to ConnectionConfig and create client
	connCfg := adapter.ConnectionConfig{
		Host:             cfg.Host,
		Port:             cfg.Port,
		Username:         cfg.Username,
		Password:         cfg.Password,
		ConnectionString: cfg.ConnectionString,
	}

	return NewAzureBlobClient(ctx, connCfg)
}

// Ping tests the Azure Blob connection by checking if container exists.
func (c *AzureBlobClient) Ping(ctx context.Context) error {
	if c.container != "" {
		// Check if container exists
		containerClient := c.client.ServiceClient().NewContainerClient(c.container)
		_, err := containerClient.GetProperties(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to check container: %w", err)
		}
		return nil
	}

	// Just list containers to verify connectivity
	pager := c.client.NewListContainersPager(nil)
	_, err := pager.NextPage(ctx)
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	return nil
}

// ListContainers lists all Azure Blob containers.
func (c *AzureBlobClient) ListContainers(ctx context.Context) ([]string, error) {
	pager := c.client.NewListContainersPager(nil)
	containers := make([]string, 0)

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list containers: %w", err)
		}

		for _, containerItem := range page.ContainerItems {
			if containerItem.Name != nil {
				containers = append(containers, *containerItem.Name)
			}
		}
	}

	return containers, nil
}

// CreateContainer creates a new Azure Blob container.
func (c *AzureBlobClient) CreateContainer(ctx context.Context, name string, options map[string]interface{}) error {
	containerClient := c.client.ServiceClient().NewContainerClient(name)

	// Set access level from options
	opts := &container.CreateOptions{}
	if accessLevel, ok := options["access_level"].(string); ok {
		switch accessLevel {
		case "blob":
			opts.Access = to.Ptr(container.PublicAccessTypeBlob)
		case "container":
			opts.Access = to.Ptr(container.PublicAccessTypeContainer)
		}
		// Default is private (no access level set)
	}

	_, err := containerClient.Create(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to create container: %w", err)
	}

	return nil
}

// DeleteContainer deletes an Azure Blob container.
func (c *AzureBlobClient) DeleteContainer(ctx context.Context, name string) error {
	containerClient := c.client.ServiceClient().NewContainerClient(name)

	_, err := containerClient.Delete(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to delete container: %w", err)
	}

	return nil
}

// GetContainer returns the current container name.
func (c *AzureBlobClient) GetContainer() string {
	return c.container
}

// Client returns the underlying Azure Blob client.
func (c *AzureBlobClient) Client() *azblob.Client {
	return c.client
}
