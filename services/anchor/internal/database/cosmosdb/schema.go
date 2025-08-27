package cosmosdb

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// DiscoverSchema discovers the database schema for CosmosDB and returns a UnifiedModel
func DiscoverSchema(db interface{}) (*unifiedmodel.UnifiedModel, error) {
	client, ok := db.(*azcosmos.Client)
	if !ok {
		return nil, fmt.Errorf("invalid CosmosDB client type")
	}

	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.CosmosDB,
		Collections:  make(map[string]unifiedmodel.Collection),
		Databases:    make(map[string]unifiedmodel.Database),
		Functions:    make(map[string]unifiedmodel.Function),
		Triggers:     make(map[string]unifiedmodel.Trigger),
		Procedures:   make(map[string]unifiedmodel.Procedure),
		Indexes:      make(map[string]unifiedmodel.Index),
	}

	// List all databases
	ctx := context.Background()
	queryPager := client.NewQueryDatabasesPager("SELECT * FROM c", nil)

	for queryPager.More() {
		response, err := queryPager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error listing databases: %v", err)
		}

		for _, dbItem := range response.Databases {
			// Add database to unified model
			um.Databases[dbItem.ID] = unifiedmodel.Database{
				Name: dbItem.ID,
			}

			// For each database, get its containers
			database, err := client.NewDatabase(dbItem.ID)
			if err != nil {
				return nil, fmt.Errorf("error creating database client: %v", err)
			}
			containerPager := database.NewQueryContainersPager("SELECT * FROM c", nil)

			for containerPager.More() {
				containerResponse, err := containerPager.NextPage(ctx)
				if err != nil {
					continue // Skip containers we can't read
				}

				for _, containerItem := range containerResponse.Containers {
					// Create a collection for each container
					collection := unifiedmodel.Collection{
						Name:    containerItem.ID,
						Owner:   dbItem.ID, // Store database name in owner
						Fields:  make(map[string]unifiedmodel.Field),
						Indexes: make(map[string]unifiedmodel.Index),
					}

					// Add basic fields that all CosmosDB documents have
					collection.Fields["id"] = unifiedmodel.Field{
						Name: "id",
						Type: "string",
					}

					// Add partition key field if available
					collection.Fields["_partitionKey"] = unifiedmodel.Field{
						Name: "_partitionKey",
						Type: "string",
					}

					um.Collections[containerItem.ID] = collection
				}
			}
		}
	}

	return um, nil
}

// CreateStructure creates database structures based on parameters
func CreateStructure(db interface{}, params common.StructureParams) error {
	client, ok := db.(*azcosmos.Client)
	if !ok {
		return fmt.Errorf("invalid CosmosDB client type")
	}

	ctx := context.Background()

	// For CosmosDB, we need to create databases and containers
	// Group tables by schema (which represents databases in CosmosDB)
	databaseContainers := make(map[string][]common.TableInfo)

	for _, tableInfo := range params.Tables {
		schema := tableInfo.Schema
		if schema == "" {
			schema = "DefaultDatabase"
		}
		databaseContainers[schema] = append(databaseContainers[schema], tableInfo)
	}

	// Create databases and their containers
	for databaseName, containers := range databaseContainers {
		// Create database
		database, err := client.NewDatabase(databaseName)
		if err != nil {
			return fmt.Errorf("error creating database client: %v", err)
		}
		_, err = database.Read(ctx, nil)
		if err != nil {
			// Database doesn't exist, create it
			_, err = client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: databaseName}, nil)
			if err != nil {
				return fmt.Errorf("error creating database %s: %v", databaseName, err)
			}
		}

		// Create containers
		for _, containerInfo := range containers {
			containerProperties := azcosmos.ContainerProperties{
				ID: containerInfo.Name,
				PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
					Paths: []string{"/id"}, // Default partition key
				},
			}

			_, err := database.CreateContainer(ctx, containerProperties, nil)
			if err != nil {
				return fmt.Errorf("error creating container %s in database %s: %v",
					containerInfo.Name, databaseName, err)
			}
		}
	}

	return nil
}

// DiscoverDetails fetches database details
func DiscoverDetails(db interface{}) (map[string]interface{}, error) {
	_, ok := db.(*azcosmos.Client)
	if !ok {
		return nil, fmt.Errorf("invalid CosmosDB client type")
	}

	// Note: CosmosDB client doesn't provide direct access to account-level details
	// In a real implementation, you would need additional Azure SDK clients
	details := make(map[string]interface{})
	details["uniqueIdentifier"] = "cosmosdb-account"
	details["databaseType"] = "cosmosdb"
	details["databaseEdition"] = "Azure Cosmos DB"
	details["version"] = "latest"
	details["databaseSize"] = int64(0)      // Would need to calculate from all containers
	details["accountName"] = "unknown"      // Would need to be extracted from config
	details["region"] = "unknown"           // Would need to be extracted from config
	details["consistencyLevel"] = "Session" // Default CosmosDB consistency level
	details["api"] = "SQL"                  // Assuming SQL API

	return details, nil
}

// CollectDatabaseMetadata collects metadata from a database
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	client, ok := db.(*azcosmos.Client)
	if !ok {
		return nil, fmt.Errorf("invalid CosmosDB client type")
	}

	metadata := make(map[string]interface{})
	metadata["database_type"] = "cosmosdb"
	metadata["api"] = "SQL"

	// Count databases and containers
	var databaseCount, containerCount int
	queryPager := client.NewQueryDatabasesPager("SELECT * FROM c", nil)

	for queryPager.More() {
		response, err := queryPager.NextPage(ctx)
		if err != nil {
			continue
		}

		databaseCount += len(response.Databases)

		for _, dbItem := range response.Databases {
			database, err := client.NewDatabase(dbItem.ID)
			if err != nil {
				continue
			}
			containerPager := database.NewQueryContainersPager("SELECT * FROM c", nil)

			for containerPager.More() {
				containerResponse, err := containerPager.NextPage(ctx)
				if err != nil {
					continue
				}
				containerCount += len(containerResponse.Containers)
			}
		}
	}

	metadata["database_count"] = databaseCount
	metadata["container_count"] = containerCount

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from a database instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	return map[string]interface{}{
		"instance_type": "cosmosdb",
		"description":   "Azure Cosmos DB instance with SQL API",
		"features": []string{
			"Multi-model database",
			"Global distribution",
			"Automatic scaling",
			"Multiple consistency levels",
			"Change feed",
		},
	}, nil
}

// ExecuteCommand executes a command on a database
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) {
	// CosmosDB supports SQL-like queries but not general SQL commands
	return nil, fmt.Errorf("CosmosDB supports SQL-like queries but not general SQL command execution. Command: %s", command)
}

// CreateDatabase creates a new database
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	client, ok := db.(*azcosmos.Client)
	if !ok {
		return fmt.Errorf("invalid CosmosDB client type")
	}

	// Create database in CosmosDB
	_, err := client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: databaseName}, nil)
	if err != nil {
		return fmt.Errorf("error creating CosmosDB database %s: %v", databaseName, err)
	}

	return nil
}

// DropDatabase drops a database
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	client, ok := db.(*azcosmos.Client)
	if !ok {
		return fmt.Errorf("invalid CosmosDB client type")
	}

	// Delete database in CosmosDB
	database, err := client.NewDatabase(databaseName)
	if err != nil {
		return fmt.Errorf("error creating database client: %v", err)
	}
	_, err = database.Delete(ctx, nil)
	if err != nil {
		return fmt.Errorf("error deleting CosmosDB database %s: %v", databaseName, err)
	}

	return nil
}
