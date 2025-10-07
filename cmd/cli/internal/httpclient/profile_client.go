package httpclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/redbco/redb-open/cmd/cli/internal/profile"
)

// ProfileHTTPClient is a profile-aware HTTP client that handles authentication using profiles
type ProfileHTTPClient struct {
	client         *http.Client
	profileManager *profile.ProfileManager
}

// NewProfileClient creates a new profile-aware HTTP client
func NewProfileClient() (*ProfileHTTPClient, error) {
	pm, err := profile.NewProfileManager()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize profile manager: %v", err)
	}

	return &ProfileHTTPClient{
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		profileManager: pm,
	}, nil
}

// makeAuthenticatedRequest performs an HTTP request with profile-based authentication
func (c *ProfileHTTPClient) makeAuthenticatedRequest(method, url string, body interface{}) (*http.Response, error) {
	// Get active profile
	activeProfileName, err := c.profileManager.GetActiveProfile()
	if err != nil {
		return nil, fmt.Errorf("no active profile found: %v", err)
	}

	prof, err := c.profileManager.GetProfile(activeProfileName)
	if err != nil {
		return nil, fmt.Errorf("failed to get active profile '%s': %v", activeProfileName, err)
	}

	// Check if logged in
	if !prof.IsLoggedIn() {
		return nil, fmt.Errorf("profile '%s' is not logged in or session has expired", activeProfileName)
	}

	// Check if access token needs refresh
	if prof.IsAccessTokenExpired() && !prof.IsRefreshTokenExpired() {
		// Attempt to refresh the token
		if err := c.profileManager.RefreshTokens(activeProfileName); err != nil {
			return nil, fmt.Errorf("failed to refresh access token: %v", err)
		}

		// Reload the profile with updated tokens
		prof, err = c.profileManager.GetProfile(activeProfileName)
		if err != nil {
			return nil, fmt.Errorf("failed to reload profile after token refresh: %v", err)
		}
	}

	// Prepare request body
	var reqBody io.Reader
	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %v", err)
		}
		reqBody = bytes.NewBuffer(jsonData)
	}

	// Create request
	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+prof.AccessToken)

	// Perform request
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to perform request: %v", err)
	}

	return resp, nil
}

// handleResponse processes the HTTP response and handles errors
func (c *ProfileHTTPClient) handleResponse(resp *http.Response, result interface{}) error {
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

// Get performs an authenticated GET request using the active profile
func (c *ProfileHTTPClient) Get(url string, result interface{}) error {
	resp, err := c.makeAuthenticatedRequest("GET", url, nil)
	if err != nil {
		return err
	}
	return c.handleResponse(resp, result)
}

// Post performs an authenticated POST request using the active profile
func (c *ProfileHTTPClient) Post(url string, body, result interface{}) error {
	resp, err := c.makeAuthenticatedRequest("POST", url, body)
	if err != nil {
		return err
	}
	return c.handleResponse(resp, result)
}

// PostStream performs an authenticated POST request and returns the response for streaming
func (c *ProfileHTTPClient) PostStream(url string, body interface{}) (*http.Response, error) {
	return c.makeAuthenticatedRequest("POST", url, body)
}

// Put performs an authenticated PUT request using the active profile
func (c *ProfileHTTPClient) Put(url string, body, result interface{}) error {
	resp, err := c.makeAuthenticatedRequest("PUT", url, body)
	if err != nil {
		return err
	}
	return c.handleResponse(resp, result)
}

// Delete performs an authenticated DELETE request using the active profile
func (c *ProfileHTTPClient) Delete(url string) error {
	resp, err := c.makeAuthenticatedRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	return c.handleResponse(resp, nil)
}

// GetActiveProfile returns the current active profile
func (c *ProfileHTTPClient) GetActiveProfile() (*profile.Profile, error) {
	activeProfileName, err := c.profileManager.GetActiveProfile()
	if err != nil {
		return nil, fmt.Errorf("no active profile found: %v", err)
	}

	return c.profileManager.GetProfile(activeProfileName)
}

// GetProfileClient returns a singleton profile-aware HTTP client
var profileClient *ProfileHTTPClient

func GetProfileClient() (*ProfileHTTPClient, error) {
	if profileClient == nil {
		var err error
		profileClient, err = NewProfileClient()
		if err != nil {
			return nil, err
		}
	}
	return profileClient, nil
}
