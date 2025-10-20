package bigquery

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// BigQueryClient wraps the BigQuery client with reDB-specific functionality.
type BigQueryClient struct {
	client    *bigquery.Client
	projectID string
	datasetID string
	location  string
}

// NewBigQueryClient creates a new BigQuery client from a database connection config.
func NewBigQueryClient(ctx context.Context, cfg adapter.ConnectionConfig) (*BigQueryClient, error) {
	var opts []option.ClientOption

	// Add credentials if provided
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
	} else if cfg.CredentialsJSON != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(cfg.CredentialsJSON)))
	}

	// Create BigQuery client
	client, err := bigquery.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	return &BigQueryClient{
		client:    client,
		projectID: cfg.ProjectID,
		datasetID: cfg.DatabaseName, // In BigQuery, dataset = database
		location:  cfg.Location,
	}, nil
}

// NewBigQueryClientFromInstance creates a new BigQuery client from an instance config.
func NewBigQueryClientFromInstance(ctx context.Context, cfg adapter.InstanceConfig) (*BigQueryClient, error) {
	// Convert to ConnectionConfig and create client
	connCfg := adapter.ConnectionConfig{
		ProjectID:       cfg.ProjectID,
		CredentialsFile: cfg.CredentialsFile,
		CredentialsJSON: cfg.CredentialsJSON,
		Location:        cfg.Location,
	}

	return NewBigQueryClient(ctx, connCfg)
}

// Ping tests the BigQuery connection.
func (c *BigQueryClient) Ping(ctx context.Context) error {
	// Try to list datasets to verify connectivity
	it := c.client.Datasets(ctx)
	_, err := it.Next()
	if err != nil && err != iterator.Done {
		return fmt.Errorf("ping failed: %w", err)
	}
	return nil
}

// Close closes the BigQuery client.
func (c *BigQueryClient) Close() error {
	return c.client.Close()
}

// ListDatasets lists all datasets in the project.
func (c *BigQueryClient) ListDatasets(ctx context.Context) ([]string, error) {
	it := c.client.Datasets(ctx)
	datasets := make([]string, 0)

	for {
		dataset, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list datasets: %w", err)
		}
		datasets = append(datasets, dataset.DatasetID)
	}

	return datasets, nil
}

// CreateDataset creates a new dataset.
func (c *BigQueryClient) CreateDataset(ctx context.Context, datasetID string, options map[string]interface{}) error {
	dataset := c.client.Dataset(datasetID)

	meta := &bigquery.DatasetMetadata{
		Location: c.location,
	}

	// Apply options
	if description, ok := options["description"].(string); ok {
		meta.Description = description
	}

	err := dataset.Create(ctx, meta)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	return nil
}

// DeleteDataset deletes a dataset.
func (c *BigQueryClient) DeleteDataset(ctx context.Context, datasetID string) error {
	dataset := c.client.Dataset(datasetID)

	// Delete all tables in the dataset first
	err := dataset.DeleteWithContents(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete dataset: %w", err)
	}

	return nil
}

// GetDataset returns a dataset reference.
func (c *BigQueryClient) GetDataset() *bigquery.Dataset {
	return c.client.Dataset(c.datasetID)
}

// GetProjectID returns the project ID.
func (c *BigQueryClient) GetProjectID() string {
	return c.projectID
}

// GetDatasetID returns the dataset ID.
func (c *BigQueryClient) GetDatasetID() string {
	return c.datasetID
}

// Client returns the underlying BigQuery client.
func (c *BigQueryClient) Client() *bigquery.Client {
	return c.client
}
