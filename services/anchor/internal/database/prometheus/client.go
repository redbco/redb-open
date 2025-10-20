package prometheus

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// PrometheusClient wraps the Prometheus HTTP API client.
type PrometheusClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewPrometheusClient creates a new Prometheus client from a database connection config.
func NewPrometheusClient(ctx context.Context, cfg adapter.ConnectionConfig) (*PrometheusClient, error) {
	// Build base URL
	scheme := "http"
	if cfg.SSL {
		scheme = "https"
	}

	port := cfg.Port
	if port == 0 {
		port = 9090 // Default Prometheus port
	}

	baseURL := fmt.Sprintf("%s://%s:%d", scheme, cfg.Host, port)

	client := &PrometheusClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	// Test connection
	if err := client.Ping(ctx); err != nil {
		return nil, err
	}

	return client, nil
}

// NewPrometheusClientFromInstance creates a new Prometheus client from an instance config.
func NewPrometheusClientFromInstance(ctx context.Context, cfg adapter.InstanceConfig) (*PrometheusClient, error) {
	// Convert to ConnectionConfig and create client
	connCfg := adapter.ConnectionConfig{
		Host: cfg.Host,
		Port: cfg.Port,
		SSL:  cfg.SSL,
	}

	return NewPrometheusClient(ctx, connCfg)
}

// Ping tests the Prometheus connection.
func (c *PrometheusClient) Ping(ctx context.Context) error {
	url := fmt.Sprintf("%s/-/healthy", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to Prometheus: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Prometheus health check failed with status: %d", resp.StatusCode)
	}

	return nil
}

// Query executes a PromQL query at a single point in time.
func (c *PrometheusClient) Query(ctx context.Context, query string, ts time.Time) (*QueryResult, error) {
	params := url.Values{}
	params.Add("query", query)
	if !ts.IsZero() {
		params.Add("time", fmt.Sprintf("%d", ts.Unix()))
	}

	url := fmt.Sprintf("%s/api/v1/query?%s", c.baseURL, params.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

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

	var result QueryResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if result.Status != "success" {
		return nil, fmt.Errorf("query failed: %s", result.Error)
	}

	return &result, nil
}

// QueryRange executes a PromQL query over a time range.
func (c *PrometheusClient) QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration) (*QueryResult, error) {
	params := url.Values{}
	params.Add("query", query)
	params.Add("start", fmt.Sprintf("%d", start.Unix()))
	params.Add("end", fmt.Sprintf("%d", end.Unix()))
	params.Add("step", fmt.Sprintf("%ds", int(step.Seconds())))

	url := fmt.Sprintf("%s/api/v1/query_range?%s", c.baseURL, params.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

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

	var result QueryResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if result.Status != "success" {
		return nil, fmt.Errorf("query failed: %s", result.Error)
	}

	return &result, nil
}

// GetLabels retrieves all label names.
func (c *PrometheusClient) GetLabels(ctx context.Context) ([]string, error) {
	url := fmt.Sprintf("%s/api/v1/labels", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get labels: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get labels failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
		Error  string   `json:"error,omitempty"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if result.Status != "success" {
		return nil, fmt.Errorf("get labels failed: %s", result.Error)
	}

	return result.Data, nil
}

// GetLabelValues retrieves all values for a specific label.
func (c *PrometheusClient) GetLabelValues(ctx context.Context, label string) ([]string, error) {
	url := fmt.Sprintf("%s/api/v1/label/%s/values", c.baseURL, url.PathEscape(label))

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get label values: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get label values failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
		Error  string   `json:"error,omitempty"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if result.Status != "success" {
		return nil, fmt.Errorf("get label values failed: %s", result.Error)
	}

	return result.Data, nil
}

// GetMetricMetadata retrieves metadata for metrics.
func (c *PrometheusClient) GetMetricMetadata(ctx context.Context, metric string) ([]MetricMetadata, error) {
	params := url.Values{}
	if metric != "" {
		params.Add("metric", metric)
	}

	url := fmt.Sprintf("%s/api/v1/metadata?%s", c.baseURL, params.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get metadata failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Status string                      `json:"status"`
		Data   map[string][]MetricMetadata `json:"data"`
		Error  string                      `json:"error,omitempty"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if result.Status != "success" {
		return nil, fmt.Errorf("get metadata failed: %s", result.Error)
	}

	// Flatten the map into a slice
	var metadata []MetricMetadata
	for _, metadataList := range result.Data {
		metadata = append(metadata, metadataList...)
	}

	return metadata, nil
}

// GetSeriesNames retrieves all metric names (series).
func (c *PrometheusClient) GetSeriesNames(ctx context.Context) ([]string, error) {
	url := fmt.Sprintf("%s/api/v1/label/__name__/values", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get series names: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get series names failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
		Error  string   `json:"error,omitempty"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if result.Status != "success" {
		return nil, fmt.Errorf("get series names failed: %s", result.Error)
	}

	return result.Data, nil
}

// QueryResult represents a Prometheus query result.
type QueryResult struct {
	Status string          `json:"status"`
	Data   QueryResultData `json:"data"`
	Error  string          `json:"error,omitempty"`
}

// QueryResultData represents the data portion of a query result.
type QueryResultData struct {
	ResultType string        `json:"resultType"`
	Result     []interface{} `json:"result"`
}

// MetricMetadata represents metadata for a metric.
type MetricMetadata struct {
	Type string `json:"type"`
	Help string `json:"help"`
	Unit string `json:"unit"`
}
