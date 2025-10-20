package bigquery

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

// MetadataOps implements metadata operations for BigQuery.
type MetadataOps struct {
	conn         *Connection
	instanceConn *InstanceConnection
}

// CollectDatabaseMetadata collects metadata about the BigQuery dataset.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	if m.conn == nil {
		return nil, fmt.Errorf("no connection available")
	}

	datasetID := m.conn.client.GetDatasetID()
	if datasetID == "" {
		return nil, fmt.Errorf("no dataset specified")
	}

	metadata := make(map[string]interface{})
	metadata["dataset_id"] = datasetID
	metadata["project_id"] = m.conn.client.GetProjectID()
	metadata["database_type"] = "bigquery"

	// Get dataset metadata
	dataset := m.conn.client.GetDataset()
	datasetMeta, err := dataset.Metadata(ctx)
	if err == nil {
		metadata["location"] = datasetMeta.Location
		metadata["description"] = datasetMeta.Description
		metadata["created_time"] = datasetMeta.CreationTime
		metadata["last_modified"] = datasetMeta.LastModifiedTime
	}

	// Count tables
	count, err := m.countTables(ctx, dataset)
	if err == nil {
		metadata["table_count"] = count
	}

	return metadata, nil
}

// CollectInstanceMetadata collects metadata about the BigQuery project.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	var client *BigQueryClient

	if m.conn != nil {
		client = m.conn.client
	} else if m.instanceConn != nil {
		client = m.instanceConn.client
	} else {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})
	metadata["project_id"] = client.GetProjectID()
	metadata["database_type"] = "bigquery"

	// List datasets
	datasets, err := client.ListDatasets(ctx)
	if err == nil {
		metadata["dataset_count"] = len(datasets)
		metadata["datasets"] = datasets
	}

	return metadata, nil
}

// GetVersion returns the BigQuery version (API version).
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	return "BigQuery API v2", nil
}

// GetUniqueIdentifier returns the project ID as unique identifier.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	if m.conn != nil {
		projectID := m.conn.client.GetProjectID()
		datasetID := m.conn.client.GetDatasetID()
		return fmt.Sprintf("bigquery::%s::%s", projectID, datasetID), nil
	}

	if m.instanceConn != nil {
		projectID := m.instanceConn.client.GetProjectID()
		return fmt.Sprintf("bigquery::%s", projectID), nil
	}

	return "bigquery::unknown", nil
}

// GetDatabaseSize returns the size of the dataset in bytes.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	if m.conn == nil {
		return 0, fmt.Errorf("no connection available")
	}

	projectID := m.conn.client.GetProjectID()
	datasetID := m.conn.client.GetDatasetID()
	if datasetID == "" {
		return 0, fmt.Errorf("no dataset specified")
	}

	// Query INFORMATION_SCHEMA to get dataset size
	query := fmt.Sprintf("SELECT SUM(size_bytes) as total_size FROM `%s.%s.__TABLES__`", projectID, datasetID)

	q := m.conn.client.Client().Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get dataset size: %w", err)
	}

	var totalSize int64
	for {
		var row []interface{}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("failed to read size: %w", err)
		}

		if len(row) > 0 {
			if size, ok := row[0].(int64); ok {
				totalSize = size
			}
		}
	}

	return totalSize, nil
}

// GetTableCount returns the number of tables in the dataset.
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	if m.conn == nil {
		return 0, fmt.Errorf("no connection available")
	}

	dataset := m.conn.client.GetDataset()
	return m.countTables(ctx, dataset)
}

// countTables counts tables in a dataset.
func (m *MetadataOps) countTables(ctx context.Context, dataset *bigquery.Dataset) (int, error) {
	it := dataset.Tables(ctx)
	count := 0

	for {
		_, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}
		count++
	}

	return count, nil
}

// ExecuteCommand executes a BigQuery SQL command.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	if m.conn == nil {
		return nil, fmt.Errorf("no connection available")
	}

	query := m.conn.client.Client().Query(command)
	job, err := query.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("job failed: %w", err)
	}
	if status.Err() != nil {
		return nil, fmt.Errorf("job error: %w", status.Err())
	}

	return []byte(fmt.Sprintf("Command executed successfully. Job ID: %s", job.ID())), nil
}
