package mongodb

import (
	"context"
	"fmt"
	"time"

	"github.com/redbco/redb-open/services/anchor/internal/database/common"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// FetchData retrieves data from a specified collection
func FetchData(db *mongo.Database, collectionName string, limit int) ([]map[string]interface{}, error) {
	if collectionName == "" {
		return nil, fmt.Errorf("collection name cannot be empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get collection
	collection := db.Collection(collectionName)

	// Build and execute query
	findOptions := options.Find()
	if limit > 0 {
		findOptions.SetLimit(int64(limit))
	}

	cursor, err := collection.Find(ctx, bson.D{}, findOptions)
	if err != nil {
		return nil, fmt.Errorf("error querying collection %s: %v", collectionName, err)
	}
	defer cursor.Close(ctx)

	var result []map[string]interface{}
	if err := cursor.All(ctx, &result); err != nil {
		return nil, fmt.Errorf("error decoding documents: %v", err)
	}

	// Convert BSON types to standard Go types for better JSON serialization
	for i := range result {
		convertBSONTypes(result[i])
	}

	return result, nil
}

// InsertData inserts data into a specified collection
func InsertData(db *mongo.Database, collectionName string, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get collection
	collection := db.Collection(collectionName)

	// Convert data to interface slice for InsertMany
	documents := make([]interface{}, len(data))
	for i, doc := range data {
		// Handle _id field if not present
		if _, hasID := doc["_id"]; !hasID {
			// In v2, ObjectID is directly in bson package
			doc["_id"] = bson.NewObjectID()
		}
		documents[i] = doc
	}

	// Insert documents
	result, err := collection.InsertMany(ctx, documents)
	if err != nil {
		return 0, fmt.Errorf("error inserting documents: %v", err)
	}

	return int64(len(result.InsertedIDs)), nil
}

// UpsertData inserts or updates data in a specified collection based on unique constraints
func UpsertData(db *mongo.Database, collectionName string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get collection
	collection := db.Collection(collectionName)

	// Prepare bulk operations
	var operations []mongo.WriteModel
	var totalRowsAffected int64

	for _, doc := range data {
		// Create filter based on unique columns
		filter := make(map[string]interface{})
		for _, col := range uniqueColumns {
			if value, exists := doc[col]; exists {
				filter[col] = value
			}
		}

		// If no unique columns specified or no values found, use _id if present
		if len(filter) == 0 {
			if id, hasID := doc["_id"]; hasID {
				filter["_id"] = id
			} else {
				// If no unique constraints and no _id, generate one
				doc["_id"] = bson.NewObjectID()
				filter["_id"] = doc["_id"]
			}
		}

		// Create update document with $set operator
		updateDoc := bson.D{{Key: "$set", Value: doc}}

		// Create upsert operation
		operation := mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(updateDoc).
			SetUpsert(true)

		operations = append(operations, operation)
	}

	// Execute bulk write
	if len(operations) > 0 {
		opts := options.BulkWrite().SetOrdered(false)
		result, err := collection.BulkWrite(ctx, operations, opts)
		if err != nil {
			return 0, fmt.Errorf("error executing bulk upsert on collection %s: %v", collectionName, err)
		}

		totalRowsAffected = result.UpsertedCount + result.ModifiedCount
	}

	return totalRowsAffected, nil
}

// UpdateData updates existing data in a specified collection based on a condition
func UpdateData(db *mongo.Database, collectionName string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get collection
	collection := db.Collection(collectionName)

	// Prepare bulk operations
	var operations []mongo.WriteModel
	var totalRowsAffected int64

	for _, doc := range data {
		// Create filter based on where columns
		filter := make(map[string]interface{})
		for _, col := range whereColumns {
			if value, exists := doc[col]; exists {
				filter[col] = value
			}
		}

		// If no where columns specified, use _id if present
		if len(filter) == 0 {
			if id, hasID := doc["_id"]; hasID {
				filter["_id"] = id
			} else {
				return 0, fmt.Errorf("no where columns specified and no _id found in document")
			}
		}

		// Create update document with only non-where columns
		updateFields := make(map[string]interface{})
		for key, value := range doc {
			isWhereColumn := false
			for _, whereCol := range whereColumns {
				if key == whereCol {
					isWhereColumn = true
					break
				}
			}
			if !isWhereColumn {
				updateFields[key] = value
			}
		}

		// Create update document with $set operator
		updateDoc := bson.D{{Key: "$set", Value: updateFields}}

		// Create update operation (no upsert for updates)
		operation := mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(updateDoc)

		operations = append(operations, operation)
	}

	// Execute bulk write
	if len(operations) > 0 {
		opts := options.BulkWrite().SetOrdered(false)
		result, err := collection.BulkWrite(ctx, operations, opts)
		if err != nil {
			return 0, fmt.Errorf("error executing bulk update on collection %s: %v", collectionName, err)
		}

		totalRowsAffected = result.ModifiedCount
	}

	return totalRowsAffected, nil
}

// DeleteData deletes documents from a specified collection
func DeleteData(db *mongo.Database, collectionName string, filter map[string]interface{}) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get collection
	collection := db.Collection(collectionName)

	// Convert filter to BSON
	filterDoc := toBSONDoc(filter)

	// Perform delete
	result, err := collection.DeleteMany(ctx, filterDoc)
	if err != nil {
		return 0, fmt.Errorf("error deleting documents: %v", err)
	}

	return result.DeletedCount, nil
}

// AggregateData performs an aggregation pipeline on a collection
func AggregateData(db *mongo.Database, collectionName string, pipeline []map[string]interface{}) ([]map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get collection
	collection := db.Collection(collectionName)

	// Convert pipeline to BSON
	pipelineBSON := make([]bson.D, len(pipeline))
	for i, stage := range pipeline {
		pipelineBSON[i] = toBSONDoc(stage)
	}

	// Execute aggregation
	cursor, err := collection.Aggregate(ctx, pipelineBSON)
	if err != nil {
		return nil, fmt.Errorf("error executing aggregation: %v", err)
	}
	defer cursor.Close(ctx)

	var result []map[string]interface{}
	if err := cursor.All(ctx, &result); err != nil {
		return nil, fmt.Errorf("error decoding aggregation results: %v", err)
	}

	// Convert BSON types to standard Go types
	for i := range result {
		convertBSONTypes(result[i])
	}

	return result, nil
}

// WipeDatabase removes all data from the database
func WipeDatabase(db *mongo.Database) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Get all collection names
	collections, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("error listing collections: %v", err)
	}

	// Drop each collection
	for _, collName := range collections {
		// Skip system collections
		if collName == "system.profile" || collName == "system.js" {
			continue
		}

		if err := db.Collection(collName).Drop(ctx); err != nil {
			return fmt.Errorf("error dropping collection %s: %v", collName, err)
		}
	}

	return nil
}

// FindDocuments finds documents in a collection based on a filter
func FindDocuments(db *mongo.Database, collectionName string, filter map[string]interface{}, findOpts *common.FindOptions) ([]map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get collection
	collection := db.Collection(collectionName)

	// Convert filter to BSON
	filterDoc := toBSONDoc(filter)

	// Set find options - create a new options.Find() instead of using FindOptions.Find()
	mongoFindOpts := options.Find()
	if findOpts != nil {
		if findOpts.Limit > 0 {
			mongoFindOpts.SetLimit(int64(findOpts.Limit))
		}
		if findOpts.Skip > 0 {
			mongoFindOpts.SetSkip(int64(findOpts.Skip))
		}
		if len(findOpts.Sort) > 0 {
			sortDoc := bson.D{}
			for field, order := range findOpts.Sort {
				var sortOrder int
				if order == "asc" || order == "ascending" || order == "1" {
					sortOrder = 1
				} else {
					sortOrder = -1
				}
				sortDoc = append(sortDoc, bson.E{Key: field, Value: sortOrder})
			}
			mongoFindOpts.SetSort(sortDoc)
		}
		if len(findOpts.Projection) > 0 {
			projectionDoc := bson.D{}
			for field, include := range findOpts.Projection {
				projectionDoc = append(projectionDoc, bson.E{Key: field, Value: include})
			}
			mongoFindOpts.SetProjection(projectionDoc)
		}
	}

	// Execute find
	cursor, err := collection.Find(ctx, filterDoc, mongoFindOpts)
	if err != nil {
		return nil, fmt.Errorf("error finding documents: %v", err)
	}
	defer cursor.Close(ctx)

	var result []map[string]interface{}
	if err := cursor.All(ctx, &result); err != nil {
		return nil, fmt.Errorf("error decoding documents: %v", err)
	}

	// Convert BSON types to standard Go types
	for i := range result {
		convertBSONTypes(result[i])
	}

	return result, nil
}

// CountDocuments counts documents in a collection based on a filter
func CountDocuments(db *mongo.Database, collectionName string, filter map[string]interface{}) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get collection
	collection := db.Collection(collectionName)

	// Convert filter to BSON
	filterDoc := toBSONDoc(filter)

	// Count documents
	count, err := collection.CountDocuments(ctx, filterDoc)
	if err != nil {
		return 0, fmt.Errorf("error counting documents: %v", err)
	}

	return count, nil
}

// Helper function to convert map to BSON document
func toBSONDoc(m map[string]interface{}) bson.D {
	doc := bson.D{}
	for k, v := range m {
		// Handle nested maps
		if nestedMap, ok := v.(map[string]interface{}); ok {
			doc = append(doc, bson.E{Key: k, Value: toBSONDoc(nestedMap)})
		} else if nestedSlice, ok := v.([]interface{}); ok {
			// Handle arrays
			doc = append(doc, bson.E{Key: k, Value: convertSliceToBSON(nestedSlice)})
		} else {
			doc = append(doc, bson.E{Key: k, Value: v})
		}
	}
	return doc
}

// Helper function to convert slice to BSON array
func convertSliceToBSON(slice []interface{}) interface{} {
	result := make(bson.A, len(slice))
	for i, v := range slice {
		if nestedMap, ok := v.(map[string]interface{}); ok {
			result[i] = toBSONDoc(nestedMap)
		} else if nestedSlice, ok := v.([]interface{}); ok {
			result[i] = convertSliceToBSON(nestedSlice)
		} else {
			result[i] = v
		}
	}
	return result
}

// Helper function to convert BSON types to standard Go types for better JSON serialization
func convertBSONTypes(doc map[string]interface{}) {
	for k, v := range doc {
		switch val := v.(type) {
		case bson.ObjectID:
			doc[k] = val.Hex()
		case bson.DateTime:
			doc[k] = time.Unix(0, int64(val)*int64(time.Millisecond)).Format(time.RFC3339)
		case bson.Binary:
			doc[k] = string(val.Data)
		case bson.Decimal128:
			str := val.String()
			doc[k] = str
		case bson.D:
			// In v2, bson.D doesn't have Map() method, so we need to convert manually
			nestedMap := make(map[string]interface{})
			for _, elem := range val {
				nestedMap[elem.Key] = elem.Value
			}
			convertBSONTypes(nestedMap)
			doc[k] = nestedMap
		case bson.A:
			arr := make([]interface{}, len(val))
			for i, item := range val {
				arr[i] = item
				if nestedDoc, ok := item.(map[string]interface{}); ok {
					convertBSONTypes(nestedDoc)
				}
			}
			doc[k] = arr
		case map[string]interface{}:
			convertBSONTypes(val)
		case []interface{}:
			for i, item := range val {
				if nestedDoc, ok := item.(map[string]interface{}); ok {
					convertBSONTypes(nestedDoc)
					val[i] = nestedDoc
				}
			}
		}
	}
}
