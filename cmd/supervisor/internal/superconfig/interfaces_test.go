package superconfig

import (
	"testing"

	"github.com/redbco/redb-open/pkg/configprovider"
)

// TestConfigImplementsInterfaces verifies that Config implements all required interfaces
func TestConfigImplementsInterfaces(t *testing.T) {
	// Create a sample config
	config := &Config{
		Keyring: KeyringConfig{
			Backend:     "file",
			Path:        "./test-keyring.json",
			MasterKey:   "test-key",
			ServiceName: "redb",
		},
		InstanceGroup: InstanceGroupConfig{
			GroupID:    "test-group",
			PortOffset: 1000,
		},
	}

	// Test KeyringConfigProvider interface
	var keyringProvider configprovider.KeyringConfigProvider = config
	if keyringProvider.GetKeyringBackend() != "file" {
		t.Errorf("Expected backend 'file', got '%s'", keyringProvider.GetKeyringBackend())
	}
	expectedPath := "./test-keyring.json-test-group" // Path should include group ID for isolation
	if keyringProvider.GetKeyringPath() != expectedPath {
		t.Errorf("Expected path '%s', got '%s'", expectedPath, keyringProvider.GetKeyringPath())
	}
	if keyringProvider.GetKeyringMasterKey() != "test-key" {
		t.Errorf("Expected master key 'test-key', got '%s'", keyringProvider.GetKeyringMasterKey())
	}
	if keyringProvider.GetKeyringBaseServiceName() != "redb" {
		t.Errorf("Expected base service name 'redb', got '%s'", keyringProvider.GetKeyringBaseServiceName())
	}

	// Test InstanceConfigProvider interface
	var instanceProvider configprovider.InstanceConfigProvider = config
	if instanceProvider.GetInstanceGroupID() != "test-group" {
		t.Errorf("Expected group ID 'test-group', got '%s'", instanceProvider.GetInstanceGroupID())
	}
	if instanceProvider.GetPortOffset() != 1000 {
		t.Errorf("Expected port offset 1000, got %d", instanceProvider.GetPortOffset())
	}

	// Test ServiceNameProvider interface
	var serviceNameProvider configprovider.ServiceNameProvider = config
	expectedDatabaseService := "redb-test-group-database"
	if actual := serviceNameProvider.GetKeyringServiceName("database"); actual != expectedDatabaseService {
		t.Errorf("Expected database service name '%s', got '%s'", expectedDatabaseService, actual)
	}

	// Test MultiInstanceConfigProvider interface (composite interface)
	var multiProvider configprovider.MultiInstanceConfigProvider = config
	if multiProvider.GetKeyringBackend() != "file" {
		t.Errorf("MultiInstanceConfigProvider: Expected backend 'file', got '%s'", multiProvider.GetKeyringBackend())
	}
	if multiProvider.GetInstanceGroupID() != "test-group" {
		t.Errorf("MultiInstanceConfigProvider: Expected group ID 'test-group', got '%s'", multiProvider.GetInstanceGroupID())
	}
	if multiProvider.GetKeyringServiceName("node") != "redb-test-group-node" {
		t.Errorf("MultiInstanceConfigProvider: Expected node service name 'redb-test-group-node', got '%s'", multiProvider.GetKeyringServiceName("node"))
	}

	t.Log("All interface implementations verified successfully")
}
