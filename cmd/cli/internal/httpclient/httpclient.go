package httpclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/redbco/redb-open/cmd/cli/internal/config"
)

type HTTPClient struct {
	client *http.Client
}

type APIError struct {
	Status   int    `json:"status"`
	Message  string `json:"message"`
	Code     string `json:"code"`
	ErrorMsg string `json:"error"`
}

func (e APIError) Error() string {
	// Prioritize the most descriptive error message
	message := e.Message
	if message == "" {
		message = e.ErrorMsg
	}
	if message == "" {
		message = fmt.Sprintf("HTTP %d error", e.Status)
	}
	return message
}

// NewClient creates a new HTTP client with configuration
func NewClient() *HTTPClient {
	cfg := config.GetConfig()
	timeout := time.Duration(cfg.Timeout) * time.Second

	return &HTTPClient{
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// makeRequest performs an HTTP request with authentication if available
func (c *HTTPClient) makeRequest(method, url string, body interface{}, requireAuth bool) (*http.Response, error) {
	var reqBody io.Reader

	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %v", err)
		}
		reqBody = bytes.NewBuffer(jsonData)
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Add authentication if required and available
	if requireAuth {
		username, authErr := config.GetUsername()
		if authErr != nil {
			return nil, fmt.Errorf("authentication required but no user logged in: %v", authErr)
		}

		token, authErr := config.GetToken(username)
		if authErr != nil {
			return nil, fmt.Errorf("authentication required but no valid token found: %v", authErr)
		}

		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to perform request: %v", err)
	}

	return resp, nil
}

// handleResponse processes the HTTP response and handles errors
func (c *HTTPClient) handleResponse(resp *http.Response, result interface{}) error {
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	if resp.StatusCode >= 400 {
		var apiErr APIError
		if err := json.Unmarshal(body, &apiErr); err != nil {
			// If we can't parse the error response, return a generic error
			return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
		}
		apiErr.Status = resp.StatusCode
		return apiErr
	}

	if result != nil && len(body) > 0 {
		if err := json.Unmarshal(body, result); err != nil {
			return fmt.Errorf("failed to unmarshal response: %v", err)
		}
	}

	return nil
}

// Get performs a GET request
func (c *HTTPClient) Get(url string, result interface{}, requireAuth bool) error {
	resp, err := c.makeRequest("GET", url, nil, requireAuth)
	if err != nil {
		return err
	}
	return c.handleResponse(resp, result)
}

// Post performs a POST request
func (c *HTTPClient) Post(url string, body, result interface{}, requireAuth bool) error {
	resp, err := c.makeRequest("POST", url, body, requireAuth)
	if err != nil {
		return err
	}
	return c.handleResponse(resp, result)
}

// Put performs a PUT request
func (c *HTTPClient) Put(url string, body, result interface{}, requireAuth bool) error {
	resp, err := c.makeRequest("PUT", url, body, requireAuth)
	if err != nil {
		return err
	}
	return c.handleResponse(resp, result)
}

// Delete performs a DELETE request
func (c *HTTPClient) Delete(url string, requireAuth bool) error {
	resp, err := c.makeRequest("DELETE", url, nil, requireAuth)
	if err != nil {
		return err
	}
	return c.handleResponse(resp, nil)
}

// GetClient returns a singleton HTTP client
var client *HTTPClient

func GetClient() *HTTPClient {
	if client == nil {
		client = NewClient()
	}
	return client
}
