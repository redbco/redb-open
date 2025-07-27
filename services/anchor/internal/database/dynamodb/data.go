package dynamodb

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// FetchData retrieves data from a specified table
func FetchData(db interface{}, tableName string, limit int) ([]map[string]interface{}, error) {
	client, ok := db.(*dynamodb.Client)
	if !ok {
		return nil, fmt.Errorf("invalid DynamoDB client type")
	}

	if tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	// Prepare scan input
	scanInput := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	}

	// Set limit if specified
	if limit > 0 {
		scanInput.Limit = aws.Int32(int32(limit))
	}

	// Execute scan operation
	result, err := client.Scan(context.Background(), scanInput)
	if err != nil {
		return nil, fmt.Errorf("error scanning table %s: %v", tableName, err)
	}

	// Convert DynamoDB items to map[string]interface{}
	var data []map[string]interface{}
	for _, item := range result.Items {
		row := make(map[string]interface{})
		err := attributevalue.UnmarshalMap(item, &row)
		if err != nil {
			return nil, fmt.Errorf("error unmarshaling DynamoDB item: %v", err)
		}
		data = append(data, row)
	}

	return data, nil
}

// InsertData inserts data into a specified table
func InsertData(db interface{}, tableName string, data []map[string]interface{}) (int64, error) {
	client, ok := db.(*dynamodb.Client)
	if !ok {
		return 0, fmt.Errorf("invalid DynamoDB client type")
	}

	if tableName == "" {
		return 0, fmt.Errorf("table name cannot be empty")
	}

	if len(data) == 0 {
		return 0, nil
	}

	var insertedCount int64

	// DynamoDB batch write can handle up to 25 items per request
	batchSize := 25
	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]
		err := insertBatch(client, tableName, batch)
		if err != nil {
			return insertedCount, fmt.Errorf("error inserting batch starting at index %d: %v", i, err)
		}

		insertedCount += int64(len(batch))
	}

	return insertedCount, nil
}

// UpdateData updates existing data in a specified table
func UpdateData(db interface{}, tableName string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	client, ok := db.(*dynamodb.Client)
	if !ok {
		return 0, fmt.Errorf("invalid DynamoDB client type")
	}

	if tableName == "" {
		return 0, fmt.Errorf("table name cannot be empty")
	}

	if len(data) == 0 {
		return 0, nil
	}

	if len(whereColumns) == 0 {
		return 0, fmt.Errorf("whereColumns cannot be empty for DynamoDB updates")
	}

	var updatedCount int64

	// Update each item individually
	for _, row := range data {
		err := updateItem(client, tableName, row, whereColumns)
		if err != nil {
			return updatedCount, fmt.Errorf("error updating item: %v", err)
		}
		updatedCount++
	}

	return updatedCount, nil
}

// UpsertData inserts or updates data based on unique constraints
func UpsertData(db interface{}, tableName string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	client, ok := db.(*dynamodb.Client)
	if !ok {
		return 0, fmt.Errorf("invalid DynamoDB client type")
	}

	if tableName == "" {
		return 0, fmt.Errorf("table name cannot be empty")
	}

	if len(data) == 0 {
		return 0, nil
	}

	var upsertedCount int64

	// For DynamoDB, upsert is handled using PutItem which overwrites the entire item
	// DynamoDB batch write can handle up to 25 items per request
	batchSize := 25
	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]
		err := upsertBatch(client, tableName, batch)
		if err != nil {
			return upsertedCount, fmt.Errorf("error upserting batch starting at index %d: %v", i, err)
		}

		upsertedCount += int64(len(batch))
	}

	return upsertedCount, nil
}

// WipeDatabase removes all data and objects from the database
func WipeDatabase(db interface{}) error {
	client, ok := db.(*dynamodb.Client)
	if !ok {
		return fmt.Errorf("invalid DynamoDB client type")
	}

	// List all tables
	listTablesResult, err := client.ListTables(context.Background(), &dynamodb.ListTablesInput{})
	if err != nil {
		return fmt.Errorf("error listing tables: %v", err)
	}

	// Delete all tables
	for _, tableName := range listTablesResult.TableNames {
		err := deleteTable(client, tableName)
		if err != nil {
			return fmt.Errorf("error deleting table %s: %v", tableName, err)
		}
	}

	return nil
}

// insertBatch inserts a batch of items into DynamoDB
func insertBatch(client *dynamodb.Client, tableName string, data []map[string]interface{}) error {
	writeRequests := make([]types.WriteRequest, 0, len(data))

	for _, row := range data {
		// Marshal the row to DynamoDB format
		item, err := attributevalue.MarshalMap(row)
		if err != nil {
			return fmt.Errorf("error marshaling item: %v", err)
		}

		writeRequests = append(writeRequests, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: item,
			},
		})
	}

	// Execute batch write
	batchWriteInput := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tableName: writeRequests,
		},
	}

	_, err := client.BatchWriteItem(context.Background(), batchWriteInput)
	if err != nil {
		return fmt.Errorf("error executing batch write: %v", err)
	}

	return nil
}

// updateItem updates a single item in DynamoDB
func updateItem(client *dynamodb.Client, tableName string, data map[string]interface{}, keyColumns []string) error {
	// Build key from keyColumns
	key := make(map[string]types.AttributeValue)
	updateExpression := "SET "
	expressionAttributeValues := make(map[string]types.AttributeValue)
	expressionAttributeNames := make(map[string]string)

	var updateParts []string
	valueIndex := 0

	for column, value := range data {
		// Check if this is a key column
		isKeyColumn := false
		for _, keyCol := range keyColumns {
			if column == keyCol {
				isKeyColumn = true
				break
			}
		}

		if isKeyColumn {
			// Add to key
			attrValue, err := attributevalue.Marshal(value)
			if err != nil {
				return fmt.Errorf("error marshaling key value for column %s: %v", column, err)
			}
			key[column] = attrValue
		} else {
			// Add to update expression
			valueIndex++
			valuePlaceholder := fmt.Sprintf(":val%d", valueIndex)
			namePlaceholder := fmt.Sprintf("#attr%d", valueIndex)

			attrValue, err := attributevalue.Marshal(value)
			if err != nil {
				return fmt.Errorf("error marshaling value for column %s: %v", column, err)
			}

			expressionAttributeValues[valuePlaceholder] = attrValue
			expressionAttributeNames[namePlaceholder] = column
			updateParts = append(updateParts, fmt.Sprintf("%s = %s", namePlaceholder, valuePlaceholder))
		}
	}

	if len(key) == 0 {
		return fmt.Errorf("no key columns found in data")
	}

	if len(updateParts) == 0 {
		return fmt.Errorf("no non-key columns found to update")
	}

	updateExpression += joinStrings(updateParts, ", ")

	// Execute update
	updateInput := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(tableName),
		Key:                       key,
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeValues: expressionAttributeValues,
		ExpressionAttributeNames:  expressionAttributeNames,
	}

	_, err := client.UpdateItem(context.Background(), updateInput)
	if err != nil {
		return fmt.Errorf("error updating item: %v", err)
	}

	return nil
}

// upsertBatch upserts a batch of items into DynamoDB
func upsertBatch(client *dynamodb.Client, tableName string, data []map[string]interface{}) error {
	writeRequests := make([]types.WriteRequest, 0, len(data))

	for _, row := range data {
		// Marshal the row to DynamoDB format
		item, err := attributevalue.MarshalMap(row)
		if err != nil {
			return fmt.Errorf("error marshaling item: %v", err)
		}

		// PutRequest performs an upsert in DynamoDB
		writeRequests = append(writeRequests, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: item,
			},
		})
	}

	// Execute batch write
	batchWriteInput := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tableName: writeRequests,
		},
	}

	_, err := client.BatchWriteItem(context.Background(), batchWriteInput)
	if err != nil {
		return fmt.Errorf("error executing batch write: %v", err)
	}

	return nil
}

// deleteTable deletes a table and waits for deletion to complete
func deleteTable(client *dynamodb.Client, tableName string) error {
	// Delete the table
	_, err := client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return fmt.Errorf("error deleting table: %v", err)
	}

	// Wait for table to be deleted with a 5 minute timeout
	waiter := dynamodb.NewTableNotExistsWaiter(client)
	err = waiter.Wait(context.Background(), &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}, 5*time.Minute)
	if err != nil {
		return fmt.Errorf("error waiting for table deletion: %v", err)
	}

	return nil
}

// joinStrings joins a slice of strings with a separator
func joinStrings(parts []string, separator string) string {
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return parts[0]
	}

	result := parts[0]
	for i := 1; i < len(parts); i++ {
		result += separator + parts[i]
	}
	return result
}
