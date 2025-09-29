package profile

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// NodeStatusResponse represents the response from the status endpoint
type NodeStatusResponse struct {
	Status      string `json:"status"`
	Description string `json:"description"`
	Reachable   bool   `json:"reachable"`
	MeshStatus  string `json:"mesh_status,omitempty"`
}

// StatusChecker handles checking node status
type StatusChecker struct {
	client *http.Client
}

// NewStatusChecker creates a new status checker
func NewStatusChecker() *StatusChecker {
	return &StatusChecker{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// CheckNodeStatus checks the status of a node
func (sc *StatusChecker) CheckNodeStatus(ctx context.Context, profile *Profile) (NodeStatus, string, error) {
	statusURL := profile.GetStatusURL()

	req, err := http.NewRequestWithContext(ctx, "GET", statusURL, nil)
	if err != nil {
		return NodeStatusUnreachable, "", fmt.Errorf("failed to create request: %v", err)
	}

	resp, err := sc.client.Do(req)
	if err != nil {
		return NodeStatusUnreachable, fmt.Sprintf("Failed to connect to %s", profile.GetBaseURL()), err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return NodeStatusUnreachable, fmt.Sprintf("HTTP %d from %s", resp.StatusCode, statusURL), fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var statusResp NodeStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&statusResp); err != nil {
		return NodeStatusUnreachable, "Invalid response format", fmt.Errorf("failed to decode response: %v", err)
	}

	// Map API response to our NodeStatus enum
	var status NodeStatus
	switch statusResp.Status {
	case "not_initialized":
		status = NodeStatusNotInitialized
	case "initialized_no_users":
		status = NodeStatusInitializedNoUsers
	case "ready_no_mesh":
		status = NodeStatusReadyNoMesh
	case "ready_with_mesh":
		status = NodeStatusReadyWithMesh
	default:
		status = NodeStatusUnreachable
	}

	return status, statusResp.Description, nil
}

// UpdateProfileStatus updates the cached status information for a profile
func (pm *ProfileManager) UpdateProfileStatus(profileName string) error {
	profile, err := pm.GetProfile(profileName)
	if err != nil {
		return err
	}

	checker := NewStatusChecker()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	status, _, err := checker.CheckNodeStatus(ctx, profile)

	// Update profile with status information
	profile.LastStatus = status
	profile.LastStatusTime = time.Now()
	if err != nil {
		profile.LastError = err.Error()
	} else {
		profile.LastError = ""
	}

	// Save updated profile
	return pm.UpdateProfile(profile)
}

// GetStatusDescription returns a human-readable description of the node status
func GetStatusDescription(status NodeStatus) string {
	switch status {
	case NodeStatusUnreachable:
		return "Node is unreachable or not responding"
	case NodeStatusNotInitialized:
		return "Node has not been initialized (--initialize not run)"
	case NodeStatusInitializedNoUsers:
		return "Node initialized but no users created yet"
	case NodeStatusReadyNoMesh:
		return "Node has users and is ready, but not part of a mesh"
	case NodeStatusReadyWithMesh:
		return "Node is ready and part of a mesh"
	default:
		return "Unknown status"
	}
}

// GetStatusColor returns a color code for terminal display
func GetStatusColor(status NodeStatus) string {
	switch status {
	case NodeStatusUnreachable:
		return "\033[31m" // Red
	case NodeStatusNotInitialized:
		return "\033[33m" // Yellow
	case NodeStatusInitializedNoUsers:
		return "\033[33m" // Yellow
	case NodeStatusReadyNoMesh:
		return "\033[32m" // Green
	case NodeStatusReadyWithMesh:
		return "\033[32m" // Green
	default:
		return "\033[37m" // White
	}
}

// ResetColor returns the terminal color reset code
func ResetColor() string {
	return "\033[0m"
}
