package cosmosdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

// FetchData retrieves data from a specified container (table equivalent in CosmosDB)
func FetchData(db interface{}, containerName string, limit int) ([]map[string]interface{}, error) {
	client, ok := db.(*azcosmos.Client)
	if !ok {
		return nil, fmt.Errorf("invalid CosmosDB client type")
	}

	if containerName == "" {
		return nil, fmt.Errorf("container name cannot be empty")
	}

	// Parse containerName to extract database and container
	// Format: database.container or just container (assuming default database)
	parts := strings.Split(containerName, ".")
	var databaseName, actualContainerName string

	if len(parts) == 2 {
		databaseName, actualContainerName = parts[0], parts[1]
	} else {
		// Assume a default database name or use the first available database
		databases, err := listDatabasesFromClient(client)
		if err != nil || len(databases) == 0 {
			return nil, fmt.Errorf("no database found and none specified in container name")
		}
		databaseName = databases[0]
		actualContainerName = containerName
	}

	// Get container reference
	database, err := client.NewDatabase(databaseName)
	if err != nil {
		return nil, fmt.Errorf("error creating database client: %v", err)
	}
	container, err := database.NewContainer(actualContainerName)
	if err != nil {
		return nil, fmt.Errorf("error creating container client: %v", err)
	}

	// Build query
	query := "SELECT * FROM c"
	if limit > 0 {
		query = fmt.Sprintf("SELECT TOP %d * FROM c", limit)
	}

	// Execute query
	queryPager := container.NewQueryItemsPager(query, azcosmos.PartitionKey{}, nil)

	var data []map[string]interface{}
	for queryPager.More() {
		response, err := queryPager.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("error executing query: %v", err)
		}

		for _, item := range response.Items {
			var row map[string]interface{}
			err := json.Unmarshal(item, &row)
			if err != nil {
				return nil, fmt.Errorf("error unmarshaling item: %v", err)
			}
			data = append(data, row)
		}
	}

	return data, nil
}

// InsertData inserts data into a specified container
func InsertData(db interface{}, containerName string, data []map[string]interface{}) (int64, error) {
	client, ok := db.(*azcosmos.Client)
	if !ok {
		return 0, fmt.Errorf("invalid CosmosDB client type")
	}

	if containerName == "" {
		return 0, fmt.Errorf("container name cannot be empty")
	}

	if len(data) == 0 {
		return 0, nil
	}

	// Parse containerName
	parts := strings.Split(containerName, ".")
	var databaseName, actualContainerName string

	if len(parts) == 2 {
		databaseName, actualContainerName = parts[0], parts[1]
	} else {
		databases, err := listDatabasesFromClient(client)
		if err != nil || len(databases) == 0 {
			return 0, fmt.Errorf("no database found and none specified in container name")
		}
		databaseName = databases[0]
		actualContainerName = containerName
	}

	// Get container reference
	database, err := client.NewDatabase(databaseName)
	if err != nil {
		return 0, fmt.Errorf("error creating database client: %v", err)
	}
	container, err := database.NewContainer(actualContainerName)
	if err != nil {
		return 0, fmt.Errorf("error creating container client: %v", err)
	}

	var insertedCount int64
	ctx := context.Background()

	// Insert each item
	for _, row := range data {
		// Ensure the item has an id field
		if _, exists := row["id"]; !exists {
			// Generate an ID if not provided
			row["id"] = generateID()
		}

		// Determine partition key (assuming it's either "id" or a field named "partitionKey")
		partitionKey := determinePartitionKey(row)

		// Marshal the item
		itemBytes, err := json.Marshal(row)
		if err != nil {
			return insertedCount, fmt.Errorf("error marshaling item: %v", err)
		}

		// Insert the item
		_, err = container.CreateItem(ctx, partitionKey, itemBytes, nil)
		if err != nil {
			return insertedCount, fmt.Errorf("error inserting item: %v", err)
		}

		insertedCount++
	}

	return insertedCount, nil
}

// UpdateData updates existing data in a specified container
func UpdateData(db interface{}, containerName string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	client, ok := db.(*azcosmos.Client)
	if !ok {
		return 0, fmt.Errorf("invalid CosmosDB client type")
	}

	if containerName == "" {
		return 0, fmt.Errorf("container name cannot be empty")
	}

	if len(data) == 0 {
		return 0, nil
	}

	if len(whereColumns) == 0 {
		return 0, fmt.Errorf("whereColumns cannot be empty for CosmosDB updates")
	}

	// Parse containerName
	parts := strings.Split(containerName, ".")
	var databaseName, actualContainerName string

	if len(parts) == 2 {
		databaseName, actualContainerName = parts[0], parts[1]
	} else {
		databases, err := listDatabasesFromClient(client)
		if err != nil || len(databases) == 0 {
			return 0, fmt.Errorf("no database found and none specified in container name")
		}
		databaseName = databases[0]
		actualContainerName = containerName
	}

	// Get container reference
	database, err := client.NewDatabase(databaseName)
	if err != nil {
		return 0, fmt.Errorf("error creating database client: %v", err)
	}
	container, err := database.NewContainer(actualContainerName)
	if err != nil {
		return 0, fmt.Errorf("error creating container client: %v", err)
	}

	var updatedCount int64
	ctx := context.Background()

	// Update each item
	for _, row := range data {
		// Get the ID from the row (required for updates)
		id, exists := row["id"]
		if !exists {
			return updatedCount, fmt.Errorf("id field is required for updates")
		}

		partitionKey := determinePartitionKey(row)

		// Marshal the updated item
		itemBytes, err := json.Marshal(row)
		if err != nil {
			return updatedCount, fmt.Errorf("error marshaling item: %v", err)
		}

		// Replace the item
		_, err = container.ReplaceItem(ctx, partitionKey, fmt.Sprint(id), itemBytes, nil)
		if err != nil {
			return updatedCount, fmt.Errorf("error updating item: %v", err)
		}

		updatedCount++
	}

	return updatedCount, nil
}

// UpsertData inserts or updates data based on unique constraints
func UpsertData(db interface{}, containerName string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	client, ok := db.(*azcosmos.Client)
	if !ok {
		return 0, fmt.Errorf("invalid CosmosDB client type")
	}

	if containerName == "" {
		return 0, fmt.Errorf("container name cannot be empty")
	}

	if len(data) == 0 {
		return 0, nil
	}

	// Parse containerName
	parts := strings.Split(containerName, ".")
	var databaseName, actualContainerName string

	if len(parts) == 2 {
		databaseName, actualContainerName = parts[0], parts[1]
	} else {
		databases, err := listDatabasesFromClient(client)
		if err != nil || len(databases) == 0 {
			return 0, fmt.Errorf("no database found and none specified in container name")
		}
		databaseName = databases[0]
		actualContainerName = containerName
	}

	// Get container reference
	database, err := client.NewDatabase(databaseName)
	if err != nil {
		return 0, fmt.Errorf("error creating database client: %v", err)
	}
	container, err := database.NewContainer(actualContainerName)
	if err != nil {
		return 0, fmt.Errorf("error creating container client: %v", err)
	}

	var upsertedCount int64
	ctx := context.Background()

	// Upsert each item
	for _, row := range data {
		// Ensure the item has an id field
		if _, exists := row["id"]; !exists {
			row["id"] = generateID()
		}

		partitionKey := determinePartitionKey(row)

		// Marshal the item
		itemBytes, err := json.Marshal(row)
		if err != nil {
			return upsertedCount, fmt.Errorf("error marshaling item: %v", err)
		}

		// Upsert the item
		_, err = container.UpsertItem(ctx, partitionKey, itemBytes, nil)
		if err != nil {
			return upsertedCount, fmt.Errorf("error upserting item: %v", err)
		}

		upsertedCount++
	}

	return upsertedCount, nil
}

// WipeDatabase removes all data and objects from the database
func WipeDatabase(db interface{}) error {
	client, ok := db.(*azcosmos.Client)
	if !ok {
		return fmt.Errorf("invalid CosmosDB client type")
	}

	// List all databases
	databases, err := listDatabasesFromClient(client)
	if err != nil {
		return fmt.Errorf("error listing databases: %v", err)
	}

	ctx := context.Background()

	// Delete all databases
	for _, databaseName := range databases {
		database, err := client.NewDatabase(databaseName)
		if err != nil {
			return fmt.Errorf("error creating database client: %v", err)
		}
		_, err = database.Delete(ctx, nil)
		if err != nil {
			return fmt.Errorf("error deleting database %s: %v", databaseName, err)
		}
	}

	return nil
}

// Helper functions

// listDatabasesFromClient lists all databases in the CosmosDB account
func listDatabasesFromClient(client *azcosmos.Client) ([]string, error) {
	ctx := context.Background()

	queryPager := client.NewQueryDatabasesPager("SELECT * FROM c", nil)

	var databases []string
	for queryPager.More() {
		response, err := queryPager.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, item := range response.Databases {
			databases = append(databases, item.ID)
		}
	}

	return databases, nil
}

// generateID generates a unique ID for CosmosDB items
func generateID() string {
	// Simple UUID-like generator for demo purposes
	// In production, you might want to use a proper UUID library
	return fmt.Sprintf("item_%d", randomInt())
}

// randomInt generates a random integer (simplified for demo)
func randomInt() int64 {
	// This is a placeholder - in production use proper random generation
	return 123456789
}

// determinePartitionKey determines the partition key for a CosmosDB item
func determinePartitionKey(row map[string]interface{}) azcosmos.PartitionKey {
	// Try common partition key fields
	if pk, exists := row["partitionKey"]; exists {
		return azcosmos.NewPartitionKeyString(fmt.Sprint(pk))
	}

	if id, exists := row["id"]; exists {
		return azcosmos.NewPartitionKeyString(fmt.Sprint(id))
	}

	// If no specific partition key found, use the id as partition key
	return azcosmos.NewPartitionKeyString("default")
}
