package mongodb

import (
	"context"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/anchor/internal/database/common"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// DiscoverSchema fetches the current schema of a MongoDB database
func DiscoverSchema(db *mongo.Database) (*MongoDBSchema, error) {
	schema := &MongoDBSchema{}
	var err error

	// Get collections
	schema.Collections, err = discoverCollections(db)
	if err != nil {
		return nil, fmt.Errorf("error discovering collections: %v", err)
	}

	// Get indexes
	schema.Indexes, err = discoverIndexes(db)
	if err != nil {
		return nil, fmt.Errorf("error discovering indexes: %v", err)
	}

	// Get functions (stored JavaScript)
	schema.Functions, err = discoverFunctions(db)
	if err != nil {
		return nil, fmt.Errorf("error discovering functions: %v", err)
	}

	// MongoDB doesn't have traditional schemas, but we can include database info
	schema.Schemas, err = getSchemas(db)
	if err != nil {
		return nil, fmt.Errorf("error getting schemas: %v", err)
	}

	return schema, nil
}

// discoverCollections fetches all collections in the database with their details
func discoverCollections(db *mongo.Database) ([]common.CollectionInfo, error) {
	ctx := context.Background()

	// Get all collection names
	collectionNames, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("error listing collections: %v", err)
	}

	var collections []common.CollectionInfo
	for _, collName := range collectionNames {
		// Get collection
		coll := db.Collection(collName)

		// Get collection stats
		statsCmd := bson.D{{Key: "collStats", Value: collName}}
		statsResult := db.RunCommand(ctx, statsCmd)
		var statsDoc bson.M
		if err := statsResult.Decode(&statsDoc); err != nil {
			return nil, fmt.Errorf("error getting stats for collection %s: %v", collName, err)
		}

		// Extract collection size and count
		var size int64 = 0
		var count int64 = 0

		if sizeVal, ok := statsDoc["size"]; ok {
			switch s := sizeVal.(type) {
			case int64:
				size = s
			case int32:
				size = int64(s)
			case float64:
				size = int64(s)
			}
		}

		if countVal, ok := statsDoc["count"]; ok {
			switch c := countVal.(type) {
			case int64:
				count = c
			case int32:
				count = int64(c)
			case float64:
				count = int64(c)
			}
		}

		// Get collection options
		collOptions := make(map[string]interface{})
		if optionsVal, ok := statsDoc["options"].(bson.M); ok {
			for k, v := range optionsVal {
				collOptions[k] = v
			}
		}

		// Get sample documents
		findOptions := options.Find().SetLimit(5)
		cursor, err := coll.Find(ctx, bson.D{}, findOptions)
		if err != nil {
			return nil, fmt.Errorf("error getting sample documents for collection %s: %v", collName, err)
		}

		var sampleDocs []map[string]interface{}
		if err = cursor.All(ctx, &sampleDocs); err != nil {
			cursor.Close(ctx)
			return nil, fmt.Errorf("error decoding sample documents for collection %s: %v", collName, err)
		}

		// Create collection info
		collInfo := common.CollectionInfo{
			Name:       collName,
			Options:    collOptions,
			SampleDocs: sampleDocs,
			Count:      count,
			Size:       size,
		}

		collections = append(collections, collInfo)
	}

	return collections, nil
}

// discoverIndexes fetches all indexes for all collections
func discoverIndexes(db *mongo.Database) ([]common.IndexInfo, error) {
	ctx := context.Background()

	// Get all collection names
	collectionNames, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("error listing collections: %v", err)
	}

	var allIndexes []common.IndexInfo

	for _, collName := range collectionNames {
		// Get collection
		coll := db.Collection(collName)

		// Get indexes
		cursor, err := coll.Indexes().List(ctx)
		if err != nil {
			return nil, fmt.Errorf("error listing indexes for collection %s: %v", collName, err)
		}

		var indexes []bson.M
		if err = cursor.All(ctx, &indexes); err != nil {
			cursor.Close(ctx)
			return nil, fmt.Errorf("error decoding indexes for collection %s: %v", collName, err)
		}

		for _, idx := range indexes {
			var indexInfo common.IndexInfo

			// Get index name
			if name, ok := idx["name"].(string); ok {
				indexInfo.Name = name
			} else {
				continue // Skip if no name
			}

			// Get index keys
			if key, ok := idx["key"].(bson.M); ok {
				var columns []string
				for field := range key {
					columns = append(columns, field)
				}
				indexInfo.Columns = columns
			}

			// Check if unique
			if unique, ok := idx["unique"].(bool); ok {
				indexInfo.IsUnique = unique
			}

			// Add collection name to index info
			indexInfo.Name = fmt.Sprintf("%s.%s", collName, indexInfo.Name)

			allIndexes = append(allIndexes, indexInfo)
		}
	}

	return allIndexes, nil
}

// discoverFunctions fetches JavaScript functions stored in the system.js collection
func discoverFunctions(db *mongo.Database) ([]common.FunctionInfo, error) {
	ctx := context.Background()

	// Check if system.js collection exists
	systemJSExists := false
	collections, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("error listing collections: %v", err)
	}

	for _, coll := range collections {
		if coll == "system.js" {
			systemJSExists = true
			break
		}
	}

	if !systemJSExists {
		// No stored functions
		return []common.FunctionInfo{}, nil
	}

	// Get all stored JavaScript functions
	systemJS := db.Collection("system.js")
	cursor, err := systemJS.Find(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("error finding stored JavaScript functions: %v", err)
	}

	var functions []common.FunctionInfo
	var results []bson.M

	if err = cursor.All(ctx, &results); err != nil {
		cursor.Close(ctx)
		return nil, fmt.Errorf("error decoding stored JavaScript functions: %v", err)
	}

	for _, result := range results {
		var functionInfo common.FunctionInfo

		// Get function name
		if name, ok := result["_id"].(string); ok {
			functionInfo.Name = name
		} else {
			continue // Skip if no name
		}

		// Get function body
		if value, ok := result["value"].(string); ok {
			functionInfo.Body = value
		} else if value, ok := result["value"].(bson.JavaScript); ok {
			functionInfo.Body = string(value)
		}

		functionInfo.Schema = "system.js"
		functionInfo.ReturnType = "javascript"

		functions = append(functions, functionInfo)
	}

	return functions, nil
}

// getSchemas returns database schema information (MongoDB doesn't have traditional schemas)
func getSchemas(db *mongo.Database) ([]common.DatabaseSchemaInfo, error) {
	// In MongoDB, the database itself is the closest concept to a schema
	dbName := db.Name()

	schema := common.DatabaseSchemaInfo{
		Name:        dbName,
		Description: "MongoDB database",
	}

	return []common.DatabaseSchemaInfo{schema}, nil
}

// CreateStructure creates database objects based on the provided parameters
func CreateStructure(db *mongo.Database, params common.StructureParams) error {
	ctx := context.Background()

	// Create collections
	for _, collInfo := range params.Tables {
		// In MongoDB, tables are collections
		collName := collInfo.Name

		// Check if collection already exists
		collections, err := db.ListCollectionNames(ctx, bson.D{{Key: "name", Value: collName}})
		if err != nil {
			return fmt.Errorf("error checking if collection exists: %v", err)
		}

		if len(collections) > 0 {
			// Collection already exists
			continue
		}

		// Create the collection
		createOpts := options.CreateCollection()

		// Set options based on table info
		if collInfo.TableType == "capped" {
			// For capped collections
			if size, ok := getOptionInt64(collInfo.Constraints, "size"); ok {
				createOpts.SetCapped(true)
				createOpts.SetSizeInBytes(size)

				if maxDocs, ok := getOptionInt64(collInfo.Constraints, "max"); ok {
					createOpts.SetMaxDocuments(maxDocs)
				}
			}
		}

		// Create collection
		if err := db.CreateCollection(ctx, collName, createOpts); err != nil {
			return fmt.Errorf("error creating collection %s: %v", collName, err)
		}

		// Create indexes
		if len(collInfo.Indexes) > 0 {
			coll := db.Collection(collName)
			var indexModels []mongo.IndexModel

			for _, idx := range collInfo.Indexes {
				// Create index keys
				keys := bson.D{}
				for _, col := range idx.Columns {
					// Default to ascending index
					keys = append(keys, bson.E{Key: col, Value: 1})
				}

				// Set index options
				indexOpts := options.Index()
				if idx.IsUnique {
					indexOpts.SetUnique(true)
				}

				if idx.Name != "" {
					indexOpts.SetName(idx.Name)
				}

				// Add index model
				model := mongo.IndexModel{
					Keys:    keys,
					Options: indexOpts,
				}

				indexModels = append(indexModels, model)
			}

			// Create indexes
			if len(indexModels) > 0 {
				_, err := coll.Indexes().CreateMany(ctx, indexModels)
				if err != nil {
					return fmt.Errorf("error creating indexes for collection %s: %v", collName, err)
				}
			}
		}
	}

	// Create stored JavaScript functions
	if len(params.Functions) > 0 {
		systemJS := db.Collection("system.js")

		for _, funcInfo := range params.Functions {
			// Create function document
			funcDoc := bson.D{
				{Key: "_id", Value: funcInfo.Name},
				{Key: "value", Value: bson.JavaScript(funcInfo.Body)},
			}

			// Insert or update function
			opts := options.Replace().SetUpsert(true)
			_, err := systemJS.ReplaceOne(ctx, bson.D{{Key: "_id", Value: funcInfo.Name}}, funcDoc, opts)
			if err != nil {
				return fmt.Errorf("error creating function %s: %v", funcInfo.Name, err)
			}
		}
	}

	return nil
}

// Helper function to extract int64 options from constraints
func getOptionInt64(constraints []common.Constraint, optionName string) (int64, bool) {
	for _, constraint := range constraints {
		if constraint.Type == "option" && constraint.Name == optionName {
			if constraint.Definition != "" {
				var value int64
				_, err := fmt.Sscanf(constraint.Definition, "%d", &value)
				if err == nil {
					return value, true
				}
			}
		}
	}
	return 0, false
}

// InferSchema analyzes sample documents to infer a schema for a collection
func InferSchema(db *mongo.Database, collectionName string, sampleSize int) (map[string]interface{}, error) {
	ctx := context.Background()

	// Get collection
	coll := db.Collection(collectionName)

	// Get sample documents
	findOptions := options.Find().SetLimit(int64(sampleSize))
	cursor, err := coll.Find(ctx, bson.D{}, findOptions)
	if err != nil {
		return nil, fmt.Errorf("error getting sample documents: %v", err)
	}

	var documents []bson.M
	if err = cursor.All(ctx, &documents); err != nil {
		cursor.Close(ctx)
		return nil, fmt.Errorf("error decoding sample documents: %v", err)
	}

	if len(documents) == 0 {
		return map[string]interface{}{
			"fields": map[string]interface{}{},
		}, nil
	}

	// Analyze documents to infer schema
	fieldTypes := make(map[string]map[string]int)

	for _, doc := range documents {
		analyzeDocument(doc, "", fieldTypes)
	}

	// Build schema
	schema := make(map[string]interface{})
	fields := make(map[string]interface{})

	for fieldPath, types := range fieldTypes {
		// Determine most common type
		mostCommonType := ""
		maxCount := 0

		for typeName, count := range types {
			if count > maxCount {
				maxCount = count
				mostCommonType = typeName
			}
		}

		// Skip _id field
		if fieldPath == "_id" {
			continue
		}

		// Handle nested fields
		if strings.Contains(fieldPath, ".") {
			parts := strings.Split(fieldPath, ".")
			createNestedField(fields, parts, mostCommonType)
		} else {
			fields[fieldPath] = map[string]interface{}{
				"type": mostCommonType,
			}
		}
	}

	schema["fields"] = fields
	return schema, nil
}

// analyzeDocument recursively analyzes a document to determine field types
func analyzeDocument(doc bson.M, prefix string, fieldTypes map[string]map[string]int) {
	for key, value := range doc {
		fieldPath := key
		if prefix != "" {
			fieldPath = prefix + "." + key
		}

		// Initialize type map if not exists
		if _, exists := fieldTypes[fieldPath]; !exists {
			fieldTypes[fieldPath] = make(map[string]int)
		}

		// Determine type
		switch v := value.(type) {
		case string:
			fieldTypes[fieldPath]["string"]++
		case int, int32, int64:
			fieldTypes[fieldPath]["integer"]++
		case float32, float64:
			fieldTypes[fieldPath]["number"]++
		case bool:
			fieldTypes[fieldPath]["boolean"]++
		case bson.M:
			fieldTypes[fieldPath]["object"]++
			analyzeDocument(v, fieldPath, fieldTypes)
		case bson.A:
			fieldTypes[fieldPath]["array"]++
			// Analyze array elements if not empty
			if len(v) > 0 {
				for i, elem := range v {
					if i >= 5 {
						break // Limit analysis to first 5 elements
					}
					if elemDoc, ok := elem.(bson.M); ok {
						analyzeDocument(elemDoc, fieldPath+".[]", fieldTypes)
					}
				}
			}
		case nil:
			fieldTypes[fieldPath]["null"]++
		default:
			fieldTypes[fieldPath]["unknown"]++
		}
	}
}

// createNestedField creates a nested field structure in the schema
func createNestedField(fields map[string]interface{}, parts []string, fieldType string) {
	if len(parts) == 1 {
		fields[parts[0]] = map[string]interface{}{
			"type": fieldType,
		}
		return
	}

	current := parts[0]

	// Create object if it doesn't exist
	if _, exists := fields[current]; !exists {
		fields[current] = map[string]interface{}{
			"type":       "object",
			"properties": make(map[string]interface{}),
		}
	}

	// Get properties
	currentField := fields[current].(map[string]interface{})
	properties, ok := currentField["properties"].(map[string]interface{})
	if !ok {
		properties = make(map[string]interface{})
		currentField["properties"] = properties
	}

	// Recurse
	createNestedField(properties, parts[1:], fieldType)
}
