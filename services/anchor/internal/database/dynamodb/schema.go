package dynamodb

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// DiscoverSchema discovers the database schema for DynamoDB and returns a UnifiedModel
func DiscoverSchema(db interface{}) (*unifiedmodel.UnifiedModel, error) {
	client, ok := db.(*dynamodb.Client)
	if !ok {
		return nil, fmt.Errorf("invalid DynamoDB client type")
	}

	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.DynamoDB,
		Tables:       make(map[string]unifiedmodel.Table),
		Indexes:      make(map[string]unifiedmodel.Index),
	}

	// List all tables
	listTablesResult, err := client.ListTables(context.Background(), &dynamodb.ListTablesInput{})
	if err != nil {
		return nil, fmt.Errorf("error listing tables: %v", err)
	}

	// Get detailed information for each table and populate unified model directly
	for _, tableName := range listTablesResult.TableNames {
		if err := describeTableUnified(client, tableName, um); err != nil {
			return nil, fmt.Errorf("error describing table %s: %v", tableName, err)
		}
	}

	return um, nil
}

// CreateStructure creates database objects from a UnifiedModel
func CreateStructure(db interface{}, um *unifiedmodel.UnifiedModel) error {
	client, ok := db.(*dynamodb.Client)
	if !ok {
		return fmt.Errorf("invalid DynamoDB client type")
	}

	if um == nil {
		return fmt.Errorf("unified model cannot be nil")
	}

	// Create tables from UnifiedModel
	for _, table := range um.Tables {
		if err := createTableFromUnified(client, table); err != nil {
			return fmt.Errorf("error creating table %s: %v", table.Name, err)
		}
	}

	return nil
}

// describeTableUnified gets detailed information about a DynamoDB table and populates UnifiedModel
func describeTableUnified(client *dynamodb.Client, tableName string, um *unifiedmodel.UnifiedModel) error {
	// Describe the table
	describeResult, err := client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return fmt.Errorf("error describing table: %v", err)
	}

	table := describeResult.Table

	// Create unified table
	unifiedTable := unifiedmodel.Table{
		Name:        *table.TableName,
		Columns:     make(map[string]unifiedmodel.Column),
		Indexes:     make(map[string]unifiedmodel.Index),
		Constraints: make(map[string]unifiedmodel.Constraint),
	}

	// Convert attribute definitions to columns
	for _, attr := range table.AttributeDefinitions {
		column := unifiedmodel.Column{
			Name:     *attr.AttributeName,
			DataType: mapAttributeTypeToDataType(attr.AttributeType),
			Nullable: true, // DynamoDB attributes are generally nullable
		}

		// Check if this is a key attribute
		for _, keyElement := range table.KeySchema {
			if *keyElement.AttributeName == *attr.AttributeName {
				column.IsPrimaryKey = true
				if keyElement.KeyType == types.KeyTypeHash {
					column.IsPartitionKey = true
				}
				break
			}
		}

		unifiedTable.Columns[*attr.AttributeName] = column
	}

	// Convert Global Secondary Indexes
	for _, gsi := range table.GlobalSecondaryIndexes {
		var indexColumns []string
		for _, keyElement := range gsi.KeySchema {
			indexColumns = append(indexColumns, *keyElement.AttributeName)
		}

		index := unifiedmodel.Index{
			Name:    *gsi.IndexName,
			Columns: indexColumns,
			Unique:  false, // GSIs are not unique in DynamoDB
		}

		unifiedTable.Indexes[*gsi.IndexName] = index
		um.Indexes[*gsi.IndexName] = index
	}

	// Convert Local Secondary Indexes
	for _, lsi := range table.LocalSecondaryIndexes {
		var indexColumns []string
		for _, keyElement := range lsi.KeySchema {
			indexColumns = append(indexColumns, *keyElement.AttributeName)
		}

		index := unifiedmodel.Index{
			Name:    *lsi.IndexName,
			Columns: indexColumns,
			Unique:  false, // LSIs are not unique in DynamoDB
		}

		unifiedTable.Indexes[*lsi.IndexName] = index
		um.Indexes[*lsi.IndexName] = index
	}

	// Add table to unified model
	um.Tables[*table.TableName] = unifiedTable

	return nil
}

// mapAttributeTypeToDataType maps DynamoDB attribute types to unified model data types
func mapAttributeTypeToDataType(attrType types.ScalarAttributeType) string {
	switch attrType {
	case types.ScalarAttributeTypeS:
		return "string"
	case types.ScalarAttributeTypeN:
		return "number"
	case types.ScalarAttributeTypeB:
		return "binary"
	default:
		return "string" // Default to string
	}
}

// createTableFromUnified creates a DynamoDB table from UnifiedModel Table
func createTableFromUnified(client *dynamodb.Client, table unifiedmodel.Table) error {
	if table.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	// Build attribute definitions from columns
	var attributeDefinitions []types.AttributeDefinition
	var keySchema []types.KeySchemaElement

	// Find primary key and partition key
	var partitionKey, sortKey string
	for _, column := range table.Columns {
		if column.IsPrimaryKey {
			if partitionKey == "" {
				partitionKey = column.Name
			} else if sortKey == "" {
				sortKey = column.Name
			}
		}

		// Add attribute definition
		attributeDefinitions = append(attributeDefinitions, types.AttributeDefinition{
			AttributeName: aws.String(column.Name),
			AttributeType: mapDataTypeToAttributeType(column.DataType),
		})
	}

	// Build key schema
	if partitionKey != "" {
		keySchema = append(keySchema, types.KeySchemaElement{
			AttributeName: aws.String(partitionKey),
			KeyType:       types.KeyTypeHash,
		})
	}
	if sortKey != "" {
		keySchema = append(keySchema, types.KeySchemaElement{
			AttributeName: aws.String(sortKey),
			KeyType:       types.KeyTypeRange,
		})
	}

	// Build global secondary indexes
	var globalSecondaryIndexes []types.GlobalSecondaryIndex
	for _, index := range table.Indexes {
		if len(index.Columns) > 0 {
			var gsiKeySchema []types.KeySchemaElement
			gsiKeySchema = append(gsiKeySchema, types.KeySchemaElement{
				AttributeName: aws.String(index.Columns[0]),
				KeyType:       types.KeyTypeHash,
			})
			if len(index.Columns) > 1 {
				gsiKeySchema = append(gsiKeySchema, types.KeySchemaElement{
					AttributeName: aws.String(index.Columns[1]),
					KeyType:       types.KeyTypeRange,
				})
			}

			globalSecondaryIndexes = append(globalSecondaryIndexes, types.GlobalSecondaryIndex{
				IndexName: aws.String(index.Name),
				KeySchema: gsiKeySchema,
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(5),
					WriteCapacityUnits: aws.Int64(5),
				},
			})
		}
	}

	// Create table input
	createTableInput := &dynamodb.CreateTableInput{
		TableName:            aws.String(table.Name),
		AttributeDefinitions: attributeDefinitions,
		KeySchema:            keySchema,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	}

	if len(globalSecondaryIndexes) > 0 {
		createTableInput.GlobalSecondaryIndexes = globalSecondaryIndexes
	}

	// Create the table
	_, err := client.CreateTable(context.Background(), createTableInput)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	// Wait for table to become active
	waiter := dynamodb.NewTableExistsWaiter(client)
	return waiter.Wait(context.Background(), &dynamodb.DescribeTableInput{
		TableName: aws.String(table.Name),
	}, 5*time.Minute)
}

// mapDataTypeToAttributeType maps unified model data types to DynamoDB attribute types
func mapDataTypeToAttributeType(dataType string) types.ScalarAttributeType {
	switch dataType {
	case "string", "varchar", "text":
		return types.ScalarAttributeTypeS
	case "number", "int", "bigint", "decimal":
		return types.ScalarAttributeTypeN
	case "binary", "blob":
		return types.ScalarAttributeTypeB
	default:
		return types.ScalarAttributeTypeS // Default to string
	}
}
