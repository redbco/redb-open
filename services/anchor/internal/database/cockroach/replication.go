package cockroach

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CreateReplicationSource sets up a replication source using CockroachDB's changefeed
func CreateReplicationSource(db *sql.DB, tableName string, databaseID string, eventHandler func(map[string]interface{})) (*CockroachReplicationSourceDetails, error) {
	// Generate unique ID for the changefeed
	changefeedID := fmt.Sprintf("feed_%s_%s", databaseID, common.GenerateUniqueID())

	// For simplicity, we'll use a file sink in this example
	// In a real implementation, you might use Kafka, cloud storage, or webhook
	sinkURI := fmt.Sprintf("file:///tmp/changefeed_%s", changefeedID)

	// Create the changefeed
	query := fmt.Sprintf(
		"CREATE CHANGEFEED FOR TABLE %s INTO '%s' WITH updated, resolved='10s'",
		tableName, sinkURI)

	_, err := db.Exec(query)
	if err != nil {
		return nil, fmt.Errorf("error creating changefeed: %v", err)
	}

	details := &CockroachReplicationSourceDetails{
		ChangefeedID:   changefeedID,
		TableName:      tableName,
		DatabaseID:     databaseID,
		SinkURI:        sinkURI,
		ResolvedOption: "10s",
	}

	// Start listening for replication events
	go listenForReplicationEvents(db, details, eventHandler)

	return details, nil
}

// ReconnectToReplicationSource reconnects to an existing replication source
func ReconnectToReplicationSource(db *sql.DB, details *CockroachReplicationSourceDetails, eventHandler func(map[string]interface{})) error {
	// Verify that the changefeed still exists
	var exists bool
	err := db.QueryRow("SELECT EXISTS(SELECT job_id FROM [SHOW JOBS] WHERE description LIKE '%changefeed%' AND description LIKE $1)",
		"%"+details.ChangefeedID+"%").Scan(&exists)
	if err != nil {
		return fmt.Errorf("error checking changefeed: %v", err)
	}

	if !exists {
		return fmt.Errorf("changefeed %s does not exist", details.ChangefeedID)
	}

	// Start listening for replication events
	go listenForReplicationEvents(db, details, eventHandler)

	return nil
}

func listenForReplicationEvents(db *sql.DB, details *CockroachReplicationSourceDetails, eventHandler func(map[string]interface{})) {
	for {
		changes, err := getReplicationChanges(db, details.SinkURI)
		if err != nil {
			log.Printf("Error getting replication changes: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		for _, change := range changes {
			event := map[string]interface{}{
				"table":     details.TableName,
				"operation": change.Operation,
				"data":      change.Data,
				"old_data":  change.OldData,
			}
			eventHandler(event)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func getReplicationChanges(db *sql.DB, sinkURI string) ([]CockroachReplicationChange, error) {
	// In a real implementation, you would read from the sink (file, Kafka, etc.)
	// This is a simplified example that simulates reading changes

	// For file sink, you might read the file content
	// For this example, we'll return an empty slice
	// In a real implementation, you would parse the changefeed output

	return []CockroachReplicationChange{}, nil
}
