package mongodb

import (
	"context"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// DiscoverSchema fetches the current schema of a MongoDB database and returns a UnifiedModel
func DiscoverSchema(db *mongo.Database) (*unifiedmodel.UnifiedModel, error) {
	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.MongoDB,
		Collections:  make(map[string]unifiedmodel.Collection),
		Indexes:      make(map[string]unifiedmodel.Index),
		Functions:    make(map[string]unifiedmodel.Function),
		Databases:    make(map[string]unifiedmodel.Database),
	}

	var err error

	// Get collections directly as UnifiedModel types
	err = discoverCollectionsUnified(db, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering collections: %v", err)
	}

	// Get indexes directly as UnifiedModel types
	err = discoverIndexesUnified(db, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering indexes: %v", err)
	}

	// Get functions directly as UnifiedModel types
	err = discoverFunctionsUnified(db, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering functions: %v", err)
	}

	// Get database info directly as UnifiedModel types
	err = getSchemasUnified(db, um)
	if err != nil {
		return nil, fmt.Errorf("error getting schemas: %v", err)
	}

	return um, nil
}

// discoverCollectionsUnified discovers collections directly into UnifiedModel
func discoverCollectionsUnified(db *mongo.Database, um *unifiedmodel.UnifiedModel) error {
	ctx := context.Background()

	// Get all collection names
	collectionNames, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("error listing collections: %v", err)
	}

	for _, collName := range collectionNames {
		// Get collection
		coll := db.Collection(collName)

		// Get collection stats
		statsCmd := bson.D{{Key: "collStats", Value: collName}}
		statsResult := db.RunCommand(ctx, statsCmd)
		var statsDoc bson.M
		if err := statsResult.Decode(&statsDoc); err != nil {
			return fmt.Errorf("error getting stats for collection %s: %v", collName, err)
		}

		// Create unified collection
		unifiedCollection := unifiedmodel.Collection{
			Name:    collName,
			Fields:  make(map[string]unifiedmodel.Field),
			Indexes: make(map[string]unifiedmodel.Index),
			Options: make(map[string]any),
		}

		// Extract collection size and count
		if sizeVal, ok := statsDoc["size"]; ok {
			unifiedCollection.Options["size"] = sizeVal
		}
		if countVal, ok := statsDoc["count"]; ok {
			unifiedCollection.Options["count"] = countVal
		}

		// Get collection options
		if optionsVal, ok := statsDoc["options"].(bson.M); ok {
			for k, v := range optionsVal {
				unifiedCollection.Options[k] = v
			}
		}

		// Get sample documents for field inference
		findOptions := options.Find().SetLimit(5)
		cursor, err := coll.Find(ctx, bson.D{}, findOptions)
		if err != nil {
			return fmt.Errorf("error getting sample documents for collection %s: %v", collName, err)
		}

		var sampleDocs []map[string]interface{}
		if err = cursor.All(ctx, &sampleDocs); err != nil {
			cursor.Close(ctx)
			return fmt.Errorf("error decoding sample documents for collection %s: %v", collName, err)
		}

		// Infer fields from sample documents
		for _, sampleDoc := range sampleDocs {
			for fieldName, fieldValue := range sampleDoc {
				if fieldName == "_id" {
					continue // Skip the MongoDB ObjectId field
				}

				fieldType := inferFieldType(fieldValue)
				unifiedCollection.Fields[fieldName] = unifiedmodel.Field{
					Name: fieldName,
					Type: fieldType,
				}
			}
		}

		// Get indexes for this collection
		indexCursor, err := coll.Indexes().List(ctx)
		if err != nil {
			return fmt.Errorf("failed to list indexes for collection %s: %v", collName, err)
		}

		for indexCursor.Next(ctx) {
			var indexDoc bson.M
			if err := indexCursor.Decode(&indexDoc); err != nil {
				continue
			}

			// Extract index information
			indexName, ok := indexDoc["name"].(string)
			if !ok {
				continue
			}

			// Handle both bson.D and bson.M for the key field
			var fields []string
			switch keyValue := indexDoc["key"].(type) {
			case bson.D:
				for _, elem := range keyValue {
					fields = append(fields, elem.Key)
				}
			case bson.M:
				for field := range keyValue {
					fields = append(fields, field)
				}
			default:
				// Skip if we can't parse the key structure
				continue
			}

			isUnique := false
			if unique, exists := indexDoc["unique"]; exists {
				isUnique = unique.(bool)
			}

			unifiedCollection.Indexes[indexName] = unifiedmodel.Index{
				Name:   indexName,
				Fields: fields,
				Unique: isUnique,
			}
		}
		indexCursor.Close(ctx)

		um.Collections[collName] = unifiedCollection
	}

	return nil
}

// discoverIndexesUnified discovers indexes directly into UnifiedModel (global indexes)
func discoverIndexesUnified(db *mongo.Database, um *unifiedmodel.UnifiedModel) error {
	// MongoDB indexes are typically collection-specific and are already handled
	// in discoverCollectionsUnified. This function is for any global indexes
	// that might exist at the database level.

	// For now, MongoDB doesn't have database-level indexes separate from collections,
	// so this is a placeholder for compatibility with the unified interface.
	return nil
}

// discoverFunctionsUnified discovers functions directly into UnifiedModel
func discoverFunctionsUnified(db *mongo.Database, um *unifiedmodel.UnifiedModel) error {
	ctx := context.Background()

	// Check if system.js collection exists
	systemJSExists := false
	collections, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("error listing collections: %v", err)
	}

	for _, coll := range collections {
		if coll == "system.js" {
			systemJSExists = true
			break
		}
	}

	if !systemJSExists {
		// No stored functions
		return nil
	}

	// Get all stored JavaScript functions
	systemJS := db.Collection("system.js")
	cursor, err := systemJS.Find(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("error finding stored JavaScript functions: %v", err)
	}

	var results []bson.M
	if err = cursor.All(ctx, &results); err != nil {
		cursor.Close(ctx)
		return fmt.Errorf("error decoding stored JavaScript functions: %v", err)
	}

	for _, result := range results {
		// Get function name
		name, ok := result["_id"].(string)
		if !ok {
			continue // Skip if no name
		}

		// Get function body
		var body string
		if value, ok := result["value"].(string); ok {
			body = value
		} else if value, ok := result["value"].(bson.JavaScript); ok {
			body = string(value)
		}

		um.Functions[name] = unifiedmodel.Function{
			Name:       name,
			Language:   "javascript",
			Returns:    "javascript",
			Definition: body,
		}
	}

	return nil
}

// getSchemasUnified gets schemas directly into UnifiedModel
func getSchemasUnified(db *mongo.Database, um *unifiedmodel.UnifiedModel) error {
	// In MongoDB, the database itself is the closest concept to a schema
	dbName := db.Name()

	um.Databases[dbName] = unifiedmodel.Database{
		Name:    dbName,
		Comment: "MongoDB database",
	}

	return nil
}

// CreateStructure creates database objects from a UnifiedModel
func CreateStructure(db *mongo.Database, um *unifiedmodel.UnifiedModel) error {
	if um == nil {
		return fmt.Errorf("unified model cannot be nil")
	}

	ctx := context.Background()

	// Create collections
	for _, collection := range um.Collections {
		collName := collection.Name

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

		// Set options based on collection info
		if collection.Options != nil {
			if capped, ok := collection.Options["capped"].(bool); ok && capped {
				createOpts.SetCapped(true)

				if size, ok := collection.Options["size"].(int64); ok {
					createOpts.SetSizeInBytes(size)
				}
				if maxDocs, ok := collection.Options["max"].(int64); ok {
					createOpts.SetMaxDocuments(maxDocs)
				}
			}
		}

		// Create collection
		if err := db.CreateCollection(ctx, collName, createOpts); err != nil {
			return fmt.Errorf("error creating collection %s: %v", collName, err)
		}

		// Create indexes
		if len(collection.Indexes) > 0 {
			coll := db.Collection(collName)
			var indexModels []mongo.IndexModel

			for _, idx := range collection.Indexes {
				// Create index keys
				keys := bson.D{}
				for _, field := range idx.Fields {
					// Default to ascending index
					keys = append(keys, bson.E{Key: field, Value: 1})
				}

				// Set index options
				indexOpts := options.Index()
				if idx.Unique {
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
	if len(um.Functions) > 0 {
		systemJS := db.Collection("system.js")

		for _, function := range um.Functions {
			// Create function document
			funcDoc := bson.D{
				{Key: "_id", Value: function.Name},
				{Key: "value", Value: bson.JavaScript(function.Definition)},
			}

			// Insert or update function
			opts := options.Replace().SetUpsert(true)
			_, err := systemJS.ReplaceOne(ctx, bson.D{{Key: "_id", Value: function.Name}}, funcDoc, opts)
			if err != nil {
				return fmt.Errorf("error creating function %s: %v", function.Name, err)
			}
		}
	}

	return nil
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
	currentField, ok := fields[current].(map[string]interface{})
	if !ok {
		return
	}
	properties, ok := currentField["properties"].(map[string]interface{})
	if !ok {
		properties = make(map[string]interface{})
		currentField["properties"] = properties
	}

	// Recurse
	createNestedField(properties, parts[1:], fieldType)
}
