package influxdb

import (
	"context"
	"fmt"
)

// MetadataOps implements metadata operations for InfluxDB.
type MetadataOps struct {
	conn         *Connection
	instanceConn *InstanceConnection
}

// CollectDatabaseMetadata collects metadata about the InfluxDB bucket.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	var bucket string
	var client *InfluxDBClient

	if m.conn != nil {
		bucket = m.conn.client.GetBucket()
		client = m.conn.client
	} else {
		return nil, fmt.Errorf("no connection available")
	}

	if bucket == "" {
		return nil, fmt.Errorf("no bucket specified")
	}

	metadata := make(map[string]interface{})
	metadata["bucket_name"] = bucket
	metadata["database_type"] = "influxdb"
	metadata["org"] = client.GetOrg()

	return metadata, nil
}

// CollectInstanceMetadata collects metadata about the InfluxDB instance.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	var client *InfluxDBClient

	if m.conn != nil {
		client = m.conn.client
	} else if m.instanceConn != nil {
		client = m.instanceConn.client
	} else {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})
	metadata["database_type"] = "influxdb"
	metadata["org"] = client.GetOrg()

	// Get health info
	health, err := client.Client().Health(ctx)
	if err == nil {
		metadata["health_status"] = health.Status
		metadata["health_message"] = health.Message
		metadata["version"] = health.Version
	}

	// List buckets
	buckets, err := client.ListBuckets(ctx)
	if err == nil {
		metadata["bucket_count"] = len(buckets)
		metadata["buckets"] = buckets
	}

	return metadata, nil
}

// GetVersion returns the InfluxDB version.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	var client *InfluxDBClient

	if m.conn != nil {
		client = m.conn.client
	} else if m.instanceConn != nil {
		client = m.instanceConn.client
	} else {
		return "", fmt.Errorf("no connection available")
	}

	health, err := client.Client().Health(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get version: %w", err)
	}

	if health.Version != nil {
		return *health.Version, nil
	}

	return "unknown", nil
}

// GetUniqueIdentifier returns the instance identifier.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	if m.conn != nil {
		bucket := m.conn.client.GetBucket()
		org := m.conn.client.GetOrg()
		return fmt.Sprintf("influxdb::%s::%s", org, bucket), nil
	}

	if m.instanceConn != nil {
		org := m.instanceConn.client.GetOrg()
		return fmt.Sprintf("influxdb::%s", org), nil
	}

	return "influxdb::unknown", nil
}

// GetDatabaseSize returns an estimate of database size.
// Note: InfluxDB doesn't provide a direct API for bucket size.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	// InfluxDB doesn't provide a simple API to get bucket size
	// This would require querying bucket metrics which may not be available
	return 0, fmt.Errorf("GetDatabaseSize not directly supported for InfluxDB")
}

// GetTableCount returns the number of measurements in the bucket.
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	if m.conn == nil {
		return 0, fmt.Errorf("no bucket connection available")
	}

	bucket := m.conn.client.GetBucket()
	if bucket == "" {
		return 0, fmt.Errorf("no bucket specified")
	}

	query := fmt.Sprintf(`
		import "influxdata/influxdb/schema"
		schema.measurements(bucket: "%s")
	`, bucket)

	queryAPI := m.conn.client.GetQueryAPI()
	result, err := queryAPI.Query(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to count measurements: %w", err)
	}
	defer result.Close()

	count := 0
	seen := make(map[string]bool)

	for result.Next() {
		if measurement, ok := result.Record().ValueByKey("_value").(string); ok {
			if !seen[measurement] {
				seen[measurement] = true
				count++
			}
		}
	}

	if result.Err() != nil {
		return 0, fmt.Errorf("query error: %w", result.Err())
	}

	return count, nil
}

// ExecuteCommand executes an InfluxDB command (Flux query).
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	if m.conn == nil {
		return nil, fmt.Errorf("no connection available")
	}

	queryAPI := m.conn.client.GetQueryAPI()
	result, err := queryAPI.Query(ctx, command)
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %w", err)
	}
	defer result.Close()

	output := []byte{}
	for result.Next() {
		output = append(output, []byte(fmt.Sprintf("%v\n", result.Record().Values()))...)
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("query error: %w", result.Err())
	}

	return output, nil
}
