package neo4j

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CreateReplicationSource sets up a replication source
func CreateReplicationSource(driver neo4j.DriverWithContext, labelOrType string, isRelationship bool, databaseID string, eventHandler func(map[string]interface{})) (*Neo4jReplicationSourceDetails, error) {
	// Generate unique name for the source
	sourceName := fmt.Sprintf("source_%s_%s", databaseID, common.GenerateUniqueID())

	details := &Neo4jReplicationSourceDetails{
		SourceName:  sourceName,
		DatabaseID:  databaseID,
		LabelOrType: labelOrType,
	}

	// Start listening for changes
	go listenForChanges(driver, details, isRelationship, eventHandler)

	return details, nil
}

// ReconnectToReplicationSource reconnects to an existing replication source
func ReconnectToReplicationSource(driver neo4j.DriverWithContext, details *Neo4jReplicationSourceDetails, isRelationship bool, eventHandler func(map[string]interface{})) error {
	// Start listening for changes
	go listenForChanges(driver, details, isRelationship, eventHandler)

	return nil
}

func listenForChanges(driver neo4j.DriverWithContext, details *Neo4jReplicationSourceDetails, isRelationship bool, eventHandler func(map[string]interface{})) {
	// Neo4j doesn't have built-in change data capture like PostgreSQL's logical replication
	// We'll implement a polling mechanism to detect changes

	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	// Get initial state
	var lastState map[string]interface{}
	var err error

	if isRelationship {
		lastState, err = getRelationshipState(ctx, session, details.LabelOrType)
	} else {
		lastState, err = getNodeState(ctx, session, details.LabelOrType)
	}

	if err != nil {
		log.Printf("Error getting initial state: %v", err)
		return
	}

	// Poll for changes
	for {
		time.Sleep(5 * time.Second)

		var currentState map[string]interface{}
		if isRelationship {
			currentState, err = getRelationshipState(ctx, session, details.LabelOrType)
		} else {
			currentState, err = getNodeState(ctx, session, details.LabelOrType)
		}

		if err != nil {
			log.Printf("Error getting current state: %v", err)
			continue
		}

		// Detect changes
		changes := detectChanges(lastState, currentState)

		// Send events for changes
		for _, change := range changes {
			event := map[string]interface{}{
				"label_or_type": details.LabelOrType,
				"operation":     change.Operation,
				"data":          change.Data,
				"old_data":      change.OldData,
			}
			eventHandler(event)
		}

		lastState = currentState
	}
}

func getNodeState(ctx context.Context, session neo4j.SessionWithContext, label string) (map[string]interface{}, error) {
	query := fmt.Sprintf("MATCH (n:`%s`) RETURN id(n) as id, properties(n) as props", label)

	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return nil, err
	}

	state := make(map[string]interface{})
	for result.Next(ctx) {
		record := result.Record()
		id, _ := record.Get("id")
		props, _ := record.Get("props")

		state[fmt.Sprintf("%v", id)] = props
	}

	return state, nil
}

func getRelationshipState(ctx context.Context, session neo4j.SessionWithContext, relType string) (map[string]interface{}, error) {
	query := fmt.Sprintf("MATCH ()-[r:`%s`]->() RETURN id(r) as id, properties(r) as props", relType)

	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return nil, err
	}

	state := make(map[string]interface{})
	for result.Next(ctx) {
		record := result.Record()
		id, _ := record.Get("id")
		props, _ := record.Get("props")

		state[fmt.Sprintf("%v", id)] = props
	}

	return state, nil
}

func detectChanges(oldState, newState map[string]interface{}) []Neo4jReplicationChange {
	var changes []Neo4jReplicationChange

	// Check for updates and deletions
	for id, oldProps := range oldState {
		if newProps, exists := newState[id]; exists {
			// Check if properties have changed
			if !arePropsEqual(oldProps, newProps) {
				changes = append(changes, Neo4jReplicationChange{
					Operation: "UPDATE",
					Data:      newProps.(map[string]interface{}),
					OldData:   oldProps.(map[string]interface{}),
				})
			}
		} else {
			// Node/relationship was deleted
			changes = append(changes, Neo4jReplicationChange{
				Operation: "DELETE",
				OldData:   oldProps.(map[string]interface{}),
			})
		}
	}

	// Check for insertions
	for id, newProps := range newState {
		if _, exists := oldState[id]; !exists {
			// New node/relationship
			changes = append(changes, Neo4jReplicationChange{
				Operation: "INSERT",
				Data:      newProps.(map[string]interface{}),
			})
		}
	}

	return changes
}

func arePropsEqual(a, b interface{}) bool {
	aMap, aOk := a.(map[string]interface{})
	bMap, bOk := b.(map[string]interface{})

	if !aOk || !bOk {
		return false
	}

	if len(aMap) != len(bMap) {
		return false
	}

	for key, aVal := range aMap {
		bVal, exists := bMap[key]
		if !exists {
			return false
		}

		// Simple equality check - could be enhanced for complex types
		if fmt.Sprintf("%v", aVal) != fmt.Sprintf("%v", bVal) {
			return false
		}
	}

	return true
}
