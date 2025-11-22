package mongodb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// DiscoverSchema fetches the current schema of a MongoDB database and returns a UnifiedModel
func DiscoverSchema(db *mongo.Database) (*unifiedmodel.UnifiedModel, error) {
	// Create the unified model - only initialize maps that MongoDB actually uses
	// This matches the pattern used by PostgreSQL, MySQL, and MariaDB adapters
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.MongoDB,
		Collections:  make(map[string]unifiedmodel.Collection),
		Databases:    make(map[string]unifiedmodel.Database),
		Indexes:      make(map[string]unifiedmodel.Index),
		Functions:    make(map[string]unifiedmodel.Function),
	}

	var err error

	// First check if we have deployed schema metadata
	// This handles the case where collections are empty after deployment
	deployedSchema, err := loadDeployedSchemaMetadata(db)
	if err == nil && deployedSchema != nil {
		// Use the deployed schema metadata as the base
		*um = *deployedSchema
	}

	// Get collections directly as UnifiedModel types
	// This will merge with deployed schema metadata or discover from data
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

	// Validate database connection
	if db == nil {
		return fmt.Errorf("database connection is nil")
	}

	// Get all collection names
	collectionNames, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("error listing collections (database connection may be invalid): %v", err)
	}

	// Check if there are any collections at all
	if len(collectionNames) == 0 {
		return nil // Not an error, just an empty database
	}

	// Track collections processed and skipped
	var processingErrors []string

	for _, collName := range collectionNames {
		// Skip internal redb metadata collection and MongoDB system collections
		if collName == "__redb_schema_metadata" || strings.HasPrefix(collName, "system.") {
			continue
		}

		// Process the collection and capture any errors
		if err := processCollection(ctx, db, collName, um); err != nil {
			errMsg := fmt.Sprintf("collection '%s': %v", collName, err)
			processingErrors = append(processingErrors, errMsg)
			continue
		}
	}

	if len(um.Collections) == 0 && len(collectionNames) > 0 {
		if len(processingErrors) > 0 {
			return fmt.Errorf("failed to process any collections. Errors: %v", processingErrors)
		}
	}

	return nil
}

// processCollection processes a single collection and adds it to the UnifiedModel
func processCollection(ctx context.Context, db *mongo.Database, collName string, um *unifiedmodel.UnifiedModel) error {
	// Get collection
	coll := db.Collection(collName)
	if coll == nil {
		return fmt.Errorf("failed to get collection handle")
	}

	// Get collection stats
	statsCmd := bson.D{{Key: "collStats", Value: collName}}
	statsResult := db.RunCommand(ctx, statsCmd)
	var statsDoc bson.M
	if err := statsResult.Decode(&statsDoc); err != nil {
		return fmt.Errorf("error getting stats: %v", err)
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
		return fmt.Errorf("error getting sample documents: %v", err)
	}

	var sampleDocs []map[string]interface{}
	if err = cursor.All(ctx, &sampleDocs); err != nil {
		cursor.Close(ctx)
		return fmt.Errorf("error decoding sample documents: %v", err)
	}
	cursor.Close(ctx)

	// Infer fields from sample documents
	for _, sampleDoc := range sampleDocs {
		for fieldName, fieldValue := range sampleDoc {
			fieldType := inferFieldType(fieldValue)
			field := unifiedmodel.Field{
				Name: fieldName,
				Type: fieldType,
			}

			// Mark _id field as required (it's always present in MongoDB)
			if fieldName == "_id" {
				field.Required = true
				if field.Options == nil {
					field.Options = make(map[string]any)
				}
				field.Options["primary_key"] = true
			}

			unifiedCollection.Fields[fieldName] = field
		}
	}

	// If no documents were found, still include the _id field as it's always present
	if len(sampleDocs) == 0 {
		unifiedCollection.Fields["_id"] = unifiedmodel.Field{
			Name:     "_id",
			Type:     "objectid",
			Required: true,
			Options: map[string]any{
				"primary_key": true,
			},
		}
	}

	// Get indexes for this collection
	indexCursor, err := coll.Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("failed to list indexes: %v", err)
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

	// Store the deployed schema metadata in a special collection
	// This allows us to discover the schema even when collections are empty
	err := storeDeployedSchemaMetadata(ctx, db, um)
	if err != nil {
		return fmt.Errorf("failed to store deployed schema metadata: %v", err)
	}

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

// storeDeployedSchemaMetadata stores the deployed schema metadata in a special collection
// This allows schema discovery to work even when user collections are empty
func storeDeployedSchemaMetadata(ctx context.Context, db *mongo.Database, um *unifiedmodel.UnifiedModel) error {
	metaColl := db.Collection("__redb_schema_metadata")

	// Create metadata document
	metaDoc := bson.M{
		"_id":         "deployed_schema",
		"schema_type": string(um.DatabaseType),
		"deployed_at": bson.DateTime(time.Now().UnixMilli()),
		"collections": make(bson.M),
		"indexes":     make(bson.M),
		"functions":   make(bson.M),
		"databases":   make(bson.M),
	}

	// Store collection metadata
	collections := make(bson.M)
	for name, collection := range um.Collections {
		collMeta := bson.M{
			"name":    collection.Name,
			"fields":  make(bson.M),
			"indexes": make(bson.M),
			"options": collection.Options,
		}

		// Store field metadata
		fields := make(bson.M)
		for fieldName, field := range collection.Fields {
			fields[fieldName] = bson.M{
				"name":     field.Name,
				"type":     field.Type,
				"required": field.Required,
				"options":  field.Options,
			}
		}
		collMeta["fields"] = fields

		// Store index metadata
		indexes := make(bson.M)
		for indexName, index := range collection.Indexes {
			indexes[indexName] = bson.M{
				"name":   index.Name,
				"fields": index.Fields,
				"unique": index.Unique,
			}
		}
		collMeta["indexes"] = indexes

		collections[name] = collMeta
	}
	metaDoc["collections"] = collections

	// Store global indexes metadata
	indexes := make(bson.M)
	for name, index := range um.Indexes {
		indexes[name] = bson.M{
			"name":   index.Name,
			"fields": index.Fields,
			"unique": index.Unique,
		}
	}
	metaDoc["indexes"] = indexes

	// Store functions metadata
	functions := make(bson.M)
	for name, function := range um.Functions {
		functions[name] = bson.M{
			"name":       function.Name,
			"returns":    function.Returns,
			"definition": function.Definition,
		}
	}
	metaDoc["functions"] = functions

	// Store databases metadata
	databases := make(bson.M)
	for name, database := range um.Databases {
		databases[name] = bson.M{
			"name":    database.Name,
			"comment": database.Comment,
		}
	}
	metaDoc["databases"] = databases

	// Upsert the metadata document
	opts := options.Replace().SetUpsert(true)
	_, err := metaColl.ReplaceOne(ctx, bson.M{"_id": "deployed_schema"}, metaDoc, opts)
	if err != nil {
		return fmt.Errorf("failed to store schema metadata: %v", err)
	}

	return nil
}

// loadDeployedSchemaMetadata loads the deployed schema metadata from the special collection
// Returns nil if no metadata exists (not an error condition)
func loadDeployedSchemaMetadata(db *mongo.Database) (*unifiedmodel.UnifiedModel, error) {
	ctx := context.Background()
	metaColl := db.Collection("__redb_schema_metadata")

	// Try to find the deployed schema metadata
	var metaDoc bson.M
	err := metaColl.FindOne(ctx, bson.M{"_id": "deployed_schema"}).Decode(&metaDoc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// No metadata exists, which is fine - return nil
			return nil, nil
		}
		return nil, fmt.Errorf("failed to load schema metadata: %v", err)
	}

	// Parse the metadata back into a UnifiedModel - only initialize maps that MongoDB uses
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.MongoDB,
		Collections:  make(map[string]unifiedmodel.Collection),
		Databases:    make(map[string]unifiedmodel.Database),
		Indexes:      make(map[string]unifiedmodel.Index),
		Functions:    make(map[string]unifiedmodel.Function),
	}

	// Parse collections metadata
	if collectionsData, ok := metaDoc["collections"].(bson.M); ok {
		for name, collData := range collectionsData {
			if collMeta, ok := collData.(bson.M); ok {
				collection := unifiedmodel.Collection{
					Name:    name,
					Fields:  make(map[string]unifiedmodel.Field),
					Indexes: make(map[string]unifiedmodel.Index),
					Options: make(map[string]any),
				}

				// Parse fields
				if fieldsData, ok := collMeta["fields"].(bson.M); ok {
					for fieldName, fieldData := range fieldsData {
						if fieldMeta, ok := fieldData.(bson.M); ok {
							field := unifiedmodel.Field{
								Name: fieldName,
							}

							if fieldType, ok := fieldMeta["type"].(string); ok {
								field.Type = fieldType
							}
							if required, ok := fieldMeta["required"].(bool); ok {
								field.Required = required
							}
							if options, ok := fieldMeta["options"].(bson.M); ok {
								field.Options = make(map[string]any)
								for k, v := range options {
									field.Options[k] = v
								}
							}

							collection.Fields[fieldName] = field
						}
					}
				}

				// Parse indexes
				if indexesData, ok := collMeta["indexes"].(bson.M); ok {
					for indexName, indexData := range indexesData {
						if indexMeta, ok := indexData.(bson.M); ok {
							index := unifiedmodel.Index{
								Name: indexName,
							}

							if fields, ok := indexMeta["fields"].(bson.A); ok {
								for _, field := range fields {
									if fieldStr, ok := field.(string); ok {
										index.Fields = append(index.Fields, fieldStr)
									}
								}
							}
							if unique, ok := indexMeta["unique"].(bool); ok {
								index.Unique = unique
							}

							collection.Indexes[indexName] = index
						}
					}
				}

				// Parse options
				if options, ok := collMeta["options"].(bson.M); ok {
					for k, v := range options {
						collection.Options[k] = v
					}
				}

				um.Collections[name] = collection
			}
		}
	}

	// Parse global indexes metadata
	if indexesData, ok := metaDoc["indexes"].(bson.M); ok {
		for name, indexData := range indexesData {
			if indexMeta, ok := indexData.(bson.M); ok {
				index := unifiedmodel.Index{
					Name: name,
				}

				if fields, ok := indexMeta["fields"].(bson.A); ok {
					for _, field := range fields {
						if fieldStr, ok := field.(string); ok {
							index.Fields = append(index.Fields, fieldStr)
						}
					}
				}
				if unique, ok := indexMeta["unique"].(bool); ok {
					index.Unique = unique
				}

				um.Indexes[name] = index
			}
		}
	}

	// Parse functions metadata
	if functionsData, ok := metaDoc["functions"].(bson.M); ok {
		for name, functionData := range functionsData {
			if functionMeta, ok := functionData.(bson.M); ok {
				function := unifiedmodel.Function{
					Name: name,
				}

				if returns, ok := functionMeta["returns"].(string); ok {
					function.Returns = returns
				}
				if definition, ok := functionMeta["definition"].(string); ok {
					function.Definition = definition
				}

				um.Functions[name] = function
			}
		}
	}

	// Parse databases metadata
	if databasesData, ok := metaDoc["databases"].(bson.M); ok {
		for name, dbData := range databasesData {
			if dbMeta, ok := dbData.(bson.M); ok {
				database := unifiedmodel.Database{
					Name: name,
				}

				if comment, ok := dbMeta["comment"].(string); ok {
					database.Comment = comment
				}

				um.Databases[name] = database
			}
		}
	}

	return um, nil
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
