package influxdb

import (
	"context"
	"fmt"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/domain"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// InfluxDBClient wraps the InfluxDB client with reDB-specific functionality.
type InfluxDBClient struct {
	client influxdb2.Client
	org    string
	bucket string
}

// NewInfluxDBClient creates a new InfluxDB client from a database connection config.
func NewInfluxDBClient(ctx context.Context, cfg adapter.ConnectionConfig) (*InfluxDBClient, error) {
	// Build server URL
	serverURL := fmt.Sprintf("http://%s:%d", cfg.Host, cfg.Port)
	if cfg.SSL {
		serverURL = fmt.Sprintf("https://%s:%d", cfg.Host, cfg.Port)
	}

	// Create InfluxDB client
	client := influxdb2.NewClient(serverURL, cfg.Token)

	return &InfluxDBClient{
		client: client,
		org:    cfg.Organization,
		bucket: cfg.DatabaseName, // In InfluxDB, bucket = database
	}, nil
}

// NewInfluxDBClientFromInstance creates a new InfluxDB client from an instance config.
func NewInfluxDBClientFromInstance(ctx context.Context, cfg adapter.InstanceConfig) (*InfluxDBClient, error) {
	// Convert to ConnectionConfig and create client
	connCfg := adapter.ConnectionConfig{
		Host:         cfg.Host,
		Port:         cfg.Port,
		Token:        cfg.Token,
		Organization: cfg.Organization,
		SSL:          cfg.SSL,
	}

	return NewInfluxDBClient(ctx, connCfg)
}

// Ping tests the InfluxDB connection.
func (c *InfluxDBClient) Ping(ctx context.Context) error {
	health, err := c.client.Health(ctx)
	if err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}

	if health.Status != "pass" {
		msg := ""
		if health.Message != nil {
			msg = *health.Message
		}
		return fmt.Errorf("influxdb health check failed: %s", msg)
	}

	return nil
}

// Close closes the InfluxDB client.
func (c *InfluxDBClient) Close() error {
	c.client.Close()
	return nil
}

// ListBuckets lists all InfluxDB buckets in the organization.
func (c *InfluxDBClient) ListBuckets(ctx context.Context) ([]string, error) {
	bucketsAPI := c.client.BucketsAPI()
	buckets, err := bucketsAPI.GetBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list buckets: %w", err)
	}

	names := make([]string, 0, len(*buckets))
	for _, bucket := range *buckets {
		names = append(names, bucket.Name)
	}

	return names, nil
}

// CreateBucket creates a new InfluxDB bucket.
func (c *InfluxDBClient) CreateBucket(ctx context.Context, name string, options map[string]interface{}) error {
	bucketsAPI := c.client.BucketsAPI()

	// Get retention period from options (in seconds)
	retentionSeconds := 0 // 0 = infinite
	if r, ok := options["retention"].(int); ok {
		retentionSeconds = r
	}

	// Find organization
	orgsAPI := c.client.OrganizationsAPI()
	org, err := orgsAPI.FindOrganizationByName(ctx, c.org)
	if err != nil {
		return fmt.Errorf("failed to find organization: %w", err)
	}

	// Create bucket metadata
	bucket := &domain.Bucket{
		Name:           name,
		OrgID:          org.Id,
		RetentionRules: domain.RetentionRules{},
	}

	if retentionSeconds > 0 {
		bucket.RetentionRules = append(bucket.RetentionRules, domain.RetentionRule{
			EverySeconds: int64(retentionSeconds),
		})
	}

	_, err = bucketsAPI.CreateBucket(ctx, bucket)
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	return nil
}

// DeleteBucket deletes an InfluxDB bucket.
func (c *InfluxDBClient) DeleteBucket(ctx context.Context, name string) error {
	bucketsAPI := c.client.BucketsAPI()

	// Find bucket by name
	bucket, err := bucketsAPI.FindBucketByName(ctx, name)
	if err != nil {
		return fmt.Errorf("failed to find bucket: %w", err)
	}

	err = bucketsAPI.DeleteBucket(ctx, bucket)
	if err != nil {
		return fmt.Errorf("failed to delete bucket: %w", err)
	}

	return nil
}

// GetWriteAPI returns the write API for the current bucket.
func (c *InfluxDBClient) GetWriteAPI() api.WriteAPI {
	return c.client.WriteAPI(c.org, c.bucket)
}

// GetWriteAPIBlocking returns the blocking write API for the current bucket.
func (c *InfluxDBClient) GetWriteAPIBlocking() api.WriteAPIBlocking {
	return c.client.WriteAPIBlocking(c.org, c.bucket)
}

// GetQueryAPI returns the query API.
func (c *InfluxDBClient) GetQueryAPI() api.QueryAPI {
	return c.client.QueryAPI(c.org)
}

// GetOrg returns the organization name.
func (c *InfluxDBClient) GetOrg() string {
	return c.org
}

// GetBucket returns the bucket name.
func (c *InfluxDBClient) GetBucket() string {
	return c.bucket
}

// Client returns the underlying InfluxDB client.
func (c *InfluxDBClient) Client() influxdb2.Client {
	return c.client
}
