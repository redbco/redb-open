package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// CreateReplicationSource sets up a replication source using MongoDB Change Streams
func CreateReplicationSource(db *mongo.Database, collectionName string, databaseID string, eventHandler func(map[string]interface{})) (*MongoDBReplicationSourceDetails, error) {
	// Validate inputs
	if collectionName == "" {
		return nil, fmt.Errorf("collection name cannot be empty")
	}

	// Create replication details
	details := &MongoDBReplicationSourceDetails{
		CollectionName: collectionName,
		DatabaseID:     databaseID,
	}

	// Start listening for replication events
	go listenForReplicationEvents(db, details, eventHandler)

	return details, nil
}

// ReconnectToReplicationSource reconnects to an existing replication source
func ReconnectToReplicationSource(db *mongo.Database, details *MongoDBReplicationSourceDetails, eventHandler func(map[string]interface{})) error {
	// Verify that the collection still exists
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	collections, err := db.ListCollectionNames(ctx, bson.D{{Key: "name", Value: details.CollectionName}})
	if err != nil {
		return fmt.Errorf("error checking collection: %v", err)
	}

	if len(collections) == 0 {
		return fmt.Errorf("collection %s does not exist", details.CollectionName)
	}

	// Start listening for replication events
	go listenForReplicationEvents(db, details, eventHandler)

	return nil
}

// listenForReplicationEvents listens for changes in the specified collection using Change Streams
func listenForReplicationEvents(db *mongo.Database, details *MongoDBReplicationSourceDetails, eventHandler func(map[string]interface{})) {
	// Get collection
	collection := db.Collection(details.CollectionName)

	// Set up change stream options
	streamOptions := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	// If we have a resume token, use it
	if details.ResumeToken != "" {
		var resumeToken bson.Raw
		if err := json.Unmarshal([]byte(details.ResumeToken), &resumeToken); err == nil {
			streamOptions.SetResumeAfter(resumeToken)
		}
	}

	// Create pipeline for the change stream
	pipeline := bson.A{}

	for {
		// Create change stream
		ctx, cancel := context.WithCancel(context.Background())
		stream, err := collection.Watch(ctx, pipeline, streamOptions)

		if err != nil {
			log.Printf("Error creating change stream: %v", err)
			cancel()
			time.Sleep(5 * time.Second)
			continue
		}

		// Process change events
		for stream.Next(ctx) {
			var changeEvent bson.M
			if err := stream.Decode(&changeEvent); err != nil {
				log.Printf("Error decoding change event: %v", err)
				continue
			}

			// Extract operation type
			operationType, _ := changeEvent["operationType"].(string)

			// Extract document data
			var documentData map[string]interface{}
			var oldDocumentData map[string]interface{}

			// For inserts and replaces, get the full document
			if fullDoc, ok := changeEvent["fullDocument"].(bson.M); ok {
				documentData = convertBSONMToMap(fullDoc)
			}

			// For updates, get the update description
			if updateDesc, ok := changeEvent["updateDescription"].(bson.M); ok {
				if updatedFields, ok := updateDesc["updatedFields"].(bson.M); ok {
					if documentData == nil {
						documentData = make(map[string]interface{})
					}
					for k, v := range convertBSONMToMap(updatedFields) {
						documentData[k] = v
					}
				}
			}

			// For deletes, get the document key
			if documentKey, ok := changeEvent["documentKey"].(bson.M); ok {
				if documentData == nil {
					documentData = convertBSONMToMap(documentKey)
				}
			}

			// Create event to send to handler
			event := map[string]interface{}{
				"table":     details.CollectionName,
				"operation": operationType,
				"data":      documentData,
				"old_data":  oldDocumentData,
			}

			// Save resume token
			if resumeToken := stream.ResumeToken(); resumeToken != nil {
				tokenBytes, _ := json.Marshal(resumeToken)
				details.ResumeToken = string(tokenBytes)
			}

			// Send event to handler
			eventHandler(event)
		}

		// Check for errors
		if err := stream.Err(); err != nil {
			log.Printf("Error in change stream: %v", err)
		}

		// Close the stream and context
		stream.Close(ctx)
		cancel()

		// Wait before reconnecting
		time.Sleep(5 * time.Second)
	}
}

// GetReplicationChanges gets changes from a MongoDB change stream
func GetReplicationChanges(db *mongo.Database, collectionName string, resumeToken string) ([]MongoDBReplicationChange, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get collection
	collection := db.Collection(collectionName)

	// Set up change stream options
	streamOptions := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	// If we have a resume token, use it
	if resumeToken != "" {
		var resumeTokenDoc bson.Raw
		if err := json.Unmarshal([]byte(resumeToken), &resumeTokenDoc); err == nil {
			streamOptions.SetResumeAfter(resumeTokenDoc)
		}
	}

	// Create pipeline for the change stream
	pipeline := bson.A{}

	// Create change stream
	stream, err := collection.Watch(ctx, pipeline, streamOptions)
	if err != nil {
		return nil, fmt.Errorf("error creating change stream: %v", err)
	}
	defer stream.Close(ctx)

	var changes []MongoDBReplicationChange

	// Process change events
	for stream.Next(ctx) {
		var changeEvent bson.M
		if err := stream.Decode(&changeEvent); err != nil {
			return nil, fmt.Errorf("error decoding change event: %v", err)
		}

		// Extract operation type
		operationType, _ := changeEvent["operationType"].(string)

		// Extract document data
		var documentData map[string]interface{}
		var oldDocumentData map[string]interface{}

		// For inserts and replaces, get the full document
		if fullDoc, ok := changeEvent["fullDocument"].(bson.M); ok {
			documentData = convertBSONMToMap(fullDoc)
		}

		// For updates, get the update description
		if updateDesc, ok := changeEvent["updateDescription"].(bson.M); ok {
			if updatedFields, ok := updateDesc["updatedFields"].(bson.M); ok {
				if documentData == nil {
					documentData = make(map[string]interface{})
				}
				for k, v := range convertBSONMToMap(updatedFields) {
					documentData[k] = v
				}
			}
		}

		// For deletes, get the document key
		if documentKey, ok := changeEvent["documentKey"].(bson.M); ok {
			if documentData == nil {
				documentData = convertBSONMToMap(documentKey)
			}
		}

		// Create change object
		change := MongoDBReplicationChange{
			Operation: operationType,
			Data:      documentData,
			OldData:   oldDocumentData,
		}

		changes = append(changes, change)
	}

	// Check for errors
	if err := stream.Err(); err != nil {
		return nil, fmt.Errorf("error in change stream: %v", err)
	}

	return changes, nil
}

// Helper function to convert bson.M to map[string]interface{}
func convertBSONMToMap(m bson.M) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		switch val := v.(type) {
		case bson.M:
			result[k] = convertBSONMToMap(val)
		case bson.A:
			result[k] = convertBSONAToSlice(val)
		case bson.D:
			// Convert bson.D to map
			nestedMap := make(map[string]interface{})
			for _, elem := range val {
				nestedMap[elem.Key] = elem.Value
			}
			result[k] = convertBSONMToMap(nestedMap)
		case bson.ObjectID:
			result[k] = val.Hex()
		case bson.DateTime:
			result[k] = time.Unix(0, int64(val)*int64(time.Millisecond)).Format(time.RFC3339)
		default:
			result[k] = v
		}
	}
	return result
}

// Helper function to convert bson.A to []interface{}
func convertBSONAToSlice(a bson.A) []interface{} {
	result := make([]interface{}, len(a))
	for i, v := range a {
		switch val := v.(type) {
		case bson.M:
			result[i] = convertBSONMToMap(val)
		case bson.A:
			result[i] = convertBSONAToSlice(val)
		case bson.D:
			// Convert bson.D to map
			nestedMap := make(map[string]interface{})
			for _, elem := range val {
				nestedMap[elem.Key] = elem.Value
			}
			result[i] = convertBSONMToMap(nestedMap)
		case bson.ObjectID:
			result[i] = val.Hex()
		case bson.DateTime:
			result[i] = time.Unix(0, int64(val)*int64(time.Millisecond)).Format(time.RFC3339)
		default:
			result[i] = v
		}
	}
	return result
}
