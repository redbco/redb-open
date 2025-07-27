package edgedb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	gel "github.com/geldata/gel-go"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CreateReplicationSource sets up a replication source
func CreateReplicationSource(client *gel.Client, typeName string, databaseID string, eventHandler func(map[string]interface{})) (*EdgeDBReplicationSourceDetails, error) {
	// Generate unique ID for the replication source
	sourceID := fmt.Sprintf("source_%s_%s", databaseID, common.GenerateUniqueID())

	// Split module and type name if provided in format "module::type"
	var module, typeOnly string
	if typeName == "" {
		return nil, fmt.Errorf("type name cannot be empty")
	}

	if strings.Contains(typeName, "::") {
		parts := strings.Split(typeName, "::")
		module = parts[0]
		typeOnly = parts[1]
	} else if typeName[0] == '@' {
		// This is a global subscription
		module = ""
		typeOnly = ""
	} else if typeName[0] == '#' {
		// This is a module subscription
		module = typeName[1:]
		typeOnly = ""
	} else {
		// Assume default module if not specified
		module = "default"
		typeOnly = typeName
	}

	details := &EdgeDBReplicationSourceDetails{
		SourceID:   sourceID,
		ModuleName: module,
		TypeName:   typeOnly,
		DatabaseID: databaseID,
	}

	// Start listening for changes
	go listenForChanges(client, details, eventHandler)

	return details, nil
}

// ReconnectToReplicationSource reconnects to an existing replication source
func ReconnectToReplicationSource(client *gel.Client, details *EdgeDBReplicationSourceDetails, eventHandler func(map[string]interface{})) error {
	// Start listening for changes
	go listenForChanges(client, details, eventHandler)

	return nil
}

func listenForChanges(client *gel.Client, details *EdgeDBReplicationSourceDetails, eventHandler func(map[string]interface{})) {
	ctx := context.Background()

	// Create a polling mechanism since EdgeDB doesn't have a built-in change data capture mechanism
	// This is a simplified approach and would need to be enhanced for production use

	// Keep track of the last seen objects
	lastSeen := make(map[string]map[string]interface{})

	for {
		// Query the current state of objects
		var objects []map[string]interface{}
		var err error

		if details.TypeName != "" {
			// Query specific type
			query := fmt.Sprintf("SELECT %s::%s { id, @* }", details.ModuleName, details.TypeName)
			err = client.Query(ctx, query, &objects)
		} else if details.ModuleName != "" {
			// Query all types in module
			query := fmt.Sprintf(`
				WITH MODULE schema
				SELECT ObjectType {
					name,
					module: { name }
				}
				FILTER .module.name = '%s'
			`, details.ModuleName)

			var types []struct {
				Name   string `edgedb:"name"`
				Module struct {
					Name string `edgedb:"name"`
				} `edgedb:"module"`
			}

			err = client.Query(ctx, query, &types)
			if err == nil {
				for _, t := range types {
					var typeObjects []map[string]interface{}
					typeQuery := fmt.Sprintf("SELECT %s::%s { id, @* }", t.Module.Name, t.Name)
					typeErr := client.Query(ctx, typeQuery, &typeObjects)
					if typeErr == nil {
						objects = append(objects, typeObjects...)
					}
				}
			}
		} else {
			// Global query - all user-defined types
			query := `
				WITH MODULE schema
				SELECT ObjectType {
					name,
					module: { name }
				}
				FILTER .module.name NOT LIKE 'std::%'
				   AND .module.name NOT LIKE 'schema::%'
				   AND .module.name NOT LIKE 'sys::%'
				   AND .module.name NOT LIKE 'cfg::%'
				   AND .module.name NOT LIKE 'math::%'
				   AND .module.name NOT LIKE 'cal::%'
				   AND .module.name NOT LIKE 'ext::%'
			`

			var types []struct {
				Name   string `edgedb:"name"`
				Module struct {
					Name string `edgedb:"name"`
				} `edgedb:"module"`
			}

			err = client.Query(ctx, query, &types)
			if err == nil {
				for _, t := range types {
					var typeObjects []map[string]interface{}
					typeQuery := fmt.Sprintf("SELECT %s::%s { id, @* }", t.Module.Name, t.Name)
					typeErr := client.Query(ctx, typeQuery, &typeObjects)
					if typeErr == nil {
						objects = append(objects, typeObjects...)
					}
				}
			}
		}

		if err != nil {
			log.Printf("Error querying objects: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Compare with last seen state to detect changes
		currentSeen := make(map[string]map[string]interface{})

		for _, obj := range objects {
			id, ok := obj["id"].(string)
			if !ok {
				continue
			}

			currentSeen[id] = obj

			if prev, exists := lastSeen[id]; !exists {
				// New object
				event := map[string]interface{}{
					"table":     details.TypeName,
					"operation": "INSERT",
					"data":      obj,
				}
				eventHandler(event)
			} else {
				// Check if object was updated
				if !mapsEqual(prev, obj) {
					event := map[string]interface{}{
						"table":     details.TypeName,
						"operation": "UPDATE",
						"data":      obj,
						"old_data":  prev,
					}
					eventHandler(event)
				}
			}
		}

		// Check for deleted objects
		for id, obj := range lastSeen {
			if _, exists := currentSeen[id]; !exists {
				// Object was deleted
				event := map[string]interface{}{
					"table":     details.TypeName,
					"operation": "DELETE",
					"old_data":  obj,
				}
				eventHandler(event)
			}
		}

		// Update last seen state
		lastSeen = currentSeen

		// Wait before next poll
		time.Sleep(1 * time.Second)
	}
}

// mapsEqual compares two maps to check if they are equal
func mapsEqual(m1, m2 map[string]interface{}) bool {
	if len(m1) != len(m2) {
		return false
	}

	for k, v1 := range m1 {
		v2, ok := m2[k]
		if !ok {
			return false
		}

		// Convert both values to JSON for comparison
		j1, err1 := json.Marshal(v1)
		j2, err2 := json.Marshal(v2)

		if err1 != nil || err2 != nil || string(j1) != string(j2) {
			return false
		}
	}

	return true
}
