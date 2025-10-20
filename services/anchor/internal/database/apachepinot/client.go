package apachepinot

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

// PinotClient wraps the Pinot HTTP API client.
type PinotClient struct {
	brokerURL     string
	controllerURL string
	httpClient    *http.Client
}

// NewPinotClient creates a new Pinot client from a database connection config.
func NewPinotClient(ctx context.Context, cfg adapter.ConnectionConfig) (*PinotClient, error) {
	// Build base URLs
	scheme := "http"
	if cfg.SSL {
		scheme = "https"
	}

	// Default ports
	brokerPort := cfg.Port
	if brokerPort == 0 {
		brokerPort = 8099 // Default Pinot broker port
	}

	controllerPort := 9000 // Default Pinot controller port

	brokerURL := fmt.Sprintf("%s://%s:%d", scheme, cfg.Host, brokerPort)
	controllerURL := fmt.Sprintf("%s://%s:%d", scheme, cfg.Host, controllerPort)

	client := &PinotClient{
		brokerURL:     brokerURL,
		controllerURL: controllerURL,
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

// NewPinotClientFromInstance creates a new Pinot client from an instance config.
func NewPinotClientFromInstance(ctx context.Context, cfg adapter.InstanceConfig) (*PinotClient, error) {
	// Convert to ConnectionConfig and create client
	connCfg := adapter.ConnectionConfig{
		Host: cfg.Host,
		Port: cfg.Port,
		SSL:  cfg.SSL,
	}

	return NewPinotClient(ctx, connCfg)
}

// Ping tests the Pinot connection.
func (c *PinotClient) Ping(ctx context.Context) error {
	url := fmt.Sprintf("%s/health", c.brokerURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to Pinot: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Pinot health check failed with status: %d", resp.StatusCode)
	}

	return nil
}

// QuerySQL executes a SQL query against Pinot.
func (c *PinotClient) QuerySQL(ctx context.Context, query string) (*QueryResult, error) {
	url := fmt.Sprintf("%s/query/sql", c.brokerURL)

	reqBody := map[string]interface{}{
		"sql": query,
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

	var result QueryResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// Convert to standard format
	return c.convertQueryResponse(&result), nil
}

// ListTables lists all tables in the Pinot cluster.
func (c *PinotClient) ListTables(ctx context.Context) ([]string, error) {
	url := fmt.Sprintf("%s/tables", c.controllerURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list tables failed with status %d: %s", resp.StatusCode, string(body))
	}

	var response struct {
		Tables []string `json:"tables"`
	}

	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return response.Tables, nil
}

// GetTableSchema retrieves the schema for a specific table.
func (c *PinotClient) GetTableSchema(ctx context.Context, tableName string) (*TableSchema, error) {
	url := fmt.Sprintf("%s/tables/%s/schema", c.controllerURL, tableName)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get table schema failed with status %d: %s", resp.StatusCode, string(body))
	}

	var schema TableSchema
	if err := json.Unmarshal(body, &schema); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &schema, nil
}

// DropTable deletes a table from Pinot.
func (c *PinotClient) DropTable(ctx context.Context, tableName string) error {
	url := fmt.Sprintf("%s/tables/%s", c.controllerURL, tableName)

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("drop table failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// convertQueryResponse converts Pinot's response format to standard format.
func (c *PinotClient) convertQueryResponse(resp *QueryResponse) *QueryResult {
	var data []map[string]interface{}

	if len(resp.ResultTable.Rows) > 0 && len(resp.ResultTable.DataSchema.ColumnNames) > 0 {
		for _, row := range resp.ResultTable.Rows {
			rowMap := make(map[string]interface{})
			for i, colName := range resp.ResultTable.DataSchema.ColumnNames {
				if i < len(row) {
					rowMap[colName] = row[i]
				}
			}
			data = append(data, rowMap)
		}
	}

	return &QueryResult{
		Data: data,
	}
}

// QueryResponse represents a Pinot query response.
type QueryResponse struct {
	ResultTable struct {
		DataSchema struct {
			ColumnNames     []string `json:"columnNames"`
			ColumnDataTypes []string `json:"columnDataTypes"`
		} `json:"dataSchema"`
		Rows [][]interface{} `json:"rows"`
	} `json:"resultTable"`
}

// QueryResult represents a standardized query result.
type QueryResult struct {
	Data []map[string]interface{}
}

// TableSchema represents a Pinot table schema.
type TableSchema struct {
	SchemaName          string      `json:"schemaName"`
	DimensionFieldSpecs []FieldSpec `json:"dimensionFieldSpecs"`
	MetricFieldSpecs    []FieldSpec `json:"metricFieldSpecs"`
	DateTimeFieldSpecs  []FieldSpec `json:"dateTimeFieldSpecs"`
}

// FieldSpec represents a field specification in Pinot.
type FieldSpec struct {
	Name     string `json:"name"`
	DataType string `json:"dataType"`
}
