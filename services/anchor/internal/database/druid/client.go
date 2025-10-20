package druid

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// DruidClient wraps the Druid HTTP API client.
type DruidClient struct {
	brokerURL  string
	routerURL  string
	httpClient *http.Client
}

// NewDruidClient creates a new Druid client from a database connection config.
func NewDruidClient(ctx context.Context, cfg adapter.ConnectionConfig) (*DruidClient, error) {
	// Build base URLs
	scheme := "http"
	if cfg.SSL {
		scheme = "https"
	}

	// Default ports
	brokerPort := cfg.Port
	if brokerPort == 0 {
		brokerPort = 8082 // Default Druid broker port
	}

	routerPort := 8888 // Default Druid router port

	brokerURL := fmt.Sprintf("%s://%s:%d", scheme, cfg.Host, brokerPort)
	routerURL := fmt.Sprintf("%s://%s:%d", scheme, cfg.Host, routerPort)

	client := &DruidClient{
		brokerURL: brokerURL,
		routerURL: routerURL,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}

	// Test connection
	if err := client.Ping(ctx); err != nil {
		return nil, err
	}

	return client, nil
}

// NewDruidClientFromInstance creates a new Druid client from an instance config.
func NewDruidClientFromInstance(ctx context.Context, cfg adapter.InstanceConfig) (*DruidClient, error) {
	// Convert to ConnectionConfig and create client
	connCfg := adapter.ConnectionConfig{
		Host: cfg.Host,
		Port: cfg.Port,
		SSL:  cfg.SSL,
	}

	return NewDruidClient(ctx, connCfg)
}

// Ping tests the Druid connection.
func (c *DruidClient) Ping(ctx context.Context) error {
	url := fmt.Sprintf("%s/status", c.brokerURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to Druid: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Druid status check failed with status: %d", resp.StatusCode)
	}

	return nil
}

// QuerySQL executes a SQL query against Druid.
func (c *DruidClient) QuerySQL(ctx context.Context, query string) (*QueryResult, error) {
	url := fmt.Sprintf("%s/druid/v2/sql", c.brokerURL)

	reqBody := map[string]interface{}{
		"query": query,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result []map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &QueryResult{
		Data: result,
	}, nil
}

// QueryNative executes a native Druid query.
func (c *DruidClient) QueryNative(ctx context.Context, querySpec map[string]interface{}) ([]interface{}, error) {
	url := fmt.Sprintf("%s/druid/v2", c.brokerURL)

	jsonBody, err := json.Marshal(querySpec)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result []interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return result, nil
}

// ListDatasources lists all datasources in the Druid cluster.
func (c *DruidClient) ListDatasources(ctx context.Context) ([]string, error) {
	url := fmt.Sprintf("%s/druid/coordinator/v1/datasources", c.routerURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to list datasources: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list datasources failed with status %d: %s", resp.StatusCode, string(body))
	}

	var datasources []string
	if err := json.Unmarshal(body, &datasources); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return datasources, nil
}

// GetDatasourceSchema retrieves the schema for a specific datasource.
func (c *DruidClient) GetDatasourceSchema(ctx context.Context, datasource string) (*DatasourceSchema, error) {
	// Get datasource metadata
	url := fmt.Sprintf("%s/druid/coordinator/v1/datasources/%s", c.routerURL, datasource)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get datasource metadata: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get datasource metadata failed with status %d: %s", resp.StatusCode, string(body))
	}

	var metadata map[string]interface{}
	if err := json.Unmarshal(body, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// Query datasource to discover dimensions and metrics
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 1", datasource)
	result, err := c.QuerySQL(ctx, query)
	if err != nil {
		// If query fails, return minimal schema
		return &DatasourceSchema{
			Datasource: datasource,
			Dimensions: []string{},
			Metrics:    []string{},
		}, nil
	}

	// Extract column names from result
	var dimensions []string
	var metrics []string

	if len(result.Data) > 0 {
		for col := range result.Data[0] {
			// Simple heuristic: __time is time column, others are dimensions/metrics
			if col == "__time" {
				continue
			}
			dimensions = append(dimensions, col)
		}
	}

	return &DatasourceSchema{
		Datasource: datasource,
		Dimensions: dimensions,
		Metrics:    metrics,
	}, nil
}

// DropDatasource deletes a datasource from Druid.
func (c *DruidClient) DropDatasource(ctx context.Context, datasource string) error {
	url := fmt.Sprintf("%s/druid/coordinator/v1/datasources/%s", c.routerURL, datasource)

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to drop datasource: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("drop datasource failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// QueryResult represents a Druid SQL query result.
type QueryResult struct {
	Data []map[string]interface{}
}

// DatasourceSchema represents the schema of a Druid datasource.
type DatasourceSchema struct {
	Datasource string
	Dimensions []string
	Metrics    []string
}
