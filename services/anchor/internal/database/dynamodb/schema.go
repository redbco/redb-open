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
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
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

	// Get detailed information for each table and convert to unified model
	for _, tableName := range listTablesResult.TableNames {
		tableInfo, err := describeTable(client, tableName)
		if err != nil {
			return nil, fmt.Errorf("error describing table %s: %v", tableName, err)
		}

		// Convert to unified table
		unifiedTable := ConvertDynamoDBTable(*tableInfo)
		um.Tables[tableInfo.Name] = unifiedTable

		// Add global secondary indexes as separate index entries
		for _, idx := range tableInfo.Indexes {
			um.Indexes[idx.Name] = unifiedmodel.Index{
				Name:    idx.Name,
				Columns: idx.Columns,
				Unique:  idx.IsUnique,
			}
		}
	}

	return um, nil
}

// CreateStructure creates database structures based on parameters
func CreateStructure(db interface{}, params common.StructureParams) error {
	client, ok := db.(*dynamodb.Client)
	if !ok {
		return fmt.Errorf("invalid DynamoDB client type")
	}

	// Create tables based on the structure parameters
	for _, tableInfo := range params.Tables {
		err := createTable(client, tableInfo)
		if err != nil {
			return fmt.Errorf("error creating table %s: %v", tableInfo.Name, err)
		}
	}

	return nil
}

// describeTable gets detailed information about a DynamoDB table
func describeTable(client *dynamodb.Client, tableName string) (*common.TableInfo, error) {
	// Describe the table
	describeResult, err := client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, fmt.Errorf("error describing table: %v", err)
	}

	table := describeResult.Table

	// Convert DynamoDB table description to common.TableInfo
	tableInfo := &common.TableInfo{
		Schema:      "default", // DynamoDB doesn't have schemas like SQL databases
		Name:        *table.TableName,
		TableType:   "table",
		Columns:     []common.ColumnInfo{},
		PrimaryKey:  []string{},
		Indexes:     []common.IndexInfo{},
		Constraints: []common.Constraint{},
	}

	// Process key schema (primary key)
	for _, keyElement := range table.KeySchema {
		tableInfo.PrimaryKey = append(tableInfo.PrimaryKey, *keyElement.AttributeName)
	}

	// Process attribute definitions
	for _, attrDef := range table.AttributeDefinitions {
		dataType := convertDynamoDBType(attrDef.AttributeType)
		isPrimaryKey := contains(tableInfo.PrimaryKey, *attrDef.AttributeName)

		column := common.ColumnInfo{
			Name:         *attrDef.AttributeName,
			DataType:     dataType,
			IsNullable:   !isPrimaryKey, // Primary key attributes are not nullable
			IsPrimaryKey: isPrimaryKey,
			IsArray:      false,
			IsUnique:     isPrimaryKey,
		}

		tableInfo.Columns = append(tableInfo.Columns, column)
	}

	// Process Global Secondary Indexes (GSI)
	for _, gsi := range table.GlobalSecondaryIndexes {
		if gsi.IndexName != nil {
			var indexColumns []string
			for _, keyElement := range gsi.KeySchema {
				indexColumns = append(indexColumns, *keyElement.AttributeName)
			}

			index := common.IndexInfo{
				Name:     *gsi.IndexName,
				Columns:  indexColumns,
				IsUnique: false, // GSIs in DynamoDB are not unique by default
			}

			tableInfo.Indexes = append(tableInfo.Indexes, index)
		}
	}

	// Process Local Secondary Indexes (LSI)
	for _, lsi := range table.LocalSecondaryIndexes {
		if lsi.IndexName != nil {
			var indexColumns []string
			for _, keyElement := range lsi.KeySchema {
				indexColumns = append(indexColumns, *keyElement.AttributeName)
			}

			index := common.IndexInfo{
				Name:     *lsi.IndexName,
				Columns:  indexColumns,
				IsUnique: false, // LSIs in DynamoDB are not unique by default
			}

			tableInfo.Indexes = append(tableInfo.Indexes, index)
		}
	}

	return tableInfo, nil
}

// createTable creates a DynamoDB table based on table info
func createTable(client *dynamodb.Client, tableInfo common.TableInfo) error {
	// Build attribute definitions
	var attributeDefinitions []types.AttributeDefinition
	var keySchema []types.KeySchemaElement

	// Add primary key attributes
	for i, pkColumn := range tableInfo.PrimaryKey {
		// Find the column info to get the data type
		var dataType types.ScalarAttributeType
		for _, column := range tableInfo.Columns {
			if column.Name == pkColumn {
				dataType = convertToDynamoDBType(column.DataType)
				break
			}
		}

		attributeDefinitions = append(attributeDefinitions, types.AttributeDefinition{
			AttributeName: aws.String(pkColumn),
			AttributeType: dataType,
		})

		// First primary key is HASH, second (if exists) is RANGE
		keyType := types.KeyTypeHash
		if i > 0 {
			keyType = types.KeyTypeRange
		}

		keySchema = append(keySchema, types.KeySchemaElement{
			AttributeName: aws.String(pkColumn),
			KeyType:       keyType,
		})
	}

	// Create table input
	createTableInput := &dynamodb.CreateTableInput{
		TableName:            aws.String(tableInfo.Name),
		AttributeDefinitions: attributeDefinitions,
		KeySchema:            keySchema,
		BillingMode:          types.BillingModePayPerRequest, // Use on-demand billing
	}

	// Create the table
	_, err := client.CreateTable(context.Background(), createTableInput)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	// Wait for table to be active
	waiter := dynamodb.NewTableExistsWaiter(client)
	err = waiter.Wait(context.Background(), &dynamodb.DescribeTableInput{
		TableName: aws.String(tableInfo.Name),
	}, 5*time.Minute)
	if err != nil {
		return fmt.Errorf("error waiting for table to be active: %v", err)
	}

	return nil
}

// convertDynamoDBType converts DynamoDB attribute type to common type
func convertDynamoDBType(attrType types.ScalarAttributeType) string {
	switch attrType {
	case types.ScalarAttributeTypeS:
		return "string"
	case types.ScalarAttributeTypeN:
		return "number"
	case types.ScalarAttributeTypeB:
		return "binary"
	default:
		return "string"
	}
}

// convertToDynamoDBType converts common data type to DynamoDB attribute type
func convertToDynamoDBType(dataType string) types.ScalarAttributeType {
	switch dataType {
	case "string", "varchar", "text", "char":
		return types.ScalarAttributeTypeS
	case "number", "int", "integer", "bigint", "decimal", "float", "double":
		return types.ScalarAttributeTypeN
	case "binary", "blob", "bytea":
		return types.ScalarAttributeTypeB
	default:
		return types.ScalarAttributeTypeS
	}
}

// contains checks if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
