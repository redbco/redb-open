package generators

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// MongoDBGenerator implements MongoDB-specific generation for document databases
type MongoDBGenerator struct {
	BaseGenerator
}

// NewMongoDBGenerator creates a new MongoDB generator
func NewMongoDBGenerator() *MongoDBGenerator {
	return &MongoDBGenerator{}
}

// Override BaseGenerator methods to provide MongoDB-specific implementations

// Structural organization
func (mg *MongoDBGenerator) GenerateCreateDatabaseSQL(database unifiedmodel.Database) (string, error) {
	if database.Name == "" {
		return "", fmt.Errorf("database name cannot be empty")
	}

	// MongoDB creates databases implicitly, but we can use a database
	return fmt.Sprintf("use %s;", database.Name), nil
}

// Primary Data Containers
func (mg *MongoDBGenerator) GenerateCreateCollectionSQL(collection unifiedmodel.Collection) (string, error) {
	if collection.Name == "" {
		return "", fmt.Errorf("collection name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("db.createCollection('%s'", collection.Name))

	// Add collection options
	options := make(map[string]interface{})

	// Add capped collection options
	if capped, ok := collection.Options["capped"].(bool); ok && capped {
		options["capped"] = true
		if size, ok := collection.Options["size"].(int); ok {
			options["size"] = size
		}
		if max, ok := collection.Options["max"].(int); ok {
			options["max"] = max
		}
	}

	// Add validator if schema is defined
	if len(collection.Fields) > 0 {
		// Convert map to slice for the validator
		var fields []unifiedmodel.Field
		for _, field := range collection.Fields {
			fields = append(fields, field)
		}
		validator := mg.generateJSONSchemaValidator(fields)
		options["validator"] = map[string]interface{}{
			"$jsonSchema": validator,
		}
		options["validationLevel"] = "strict"
		options["validationAction"] = "error"
	}

	// Add collation
	if collation, ok := collection.Options["collation"].(map[string]interface{}); ok {
		options["collation"] = collation
	}

	if len(options) > 0 {
		optionsJSON, _ := json.Marshal(options)
		sb.WriteString(fmt.Sprintf(", %s", string(optionsJSON)))
	}

	sb.WriteString(");")

	return sb.String(), nil
}

// Integrity, performance and identity objects
func (mg *MongoDBGenerator) GenerateCreateIndexSQL(index unifiedmodel.Index) (string, error) {
	if index.Name == "" {
		return "", fmt.Errorf("index name cannot be empty")
	}

	var sb strings.Builder

	// Determine collection name - this should be provided in context
	collectionName := "unknown_collection"
	if collection, ok := index.Options["collection"].(string); ok && collection != "" {
		collectionName = collection
	}

	sb.WriteString(fmt.Sprintf("db.%s.createIndex(", collectionName))

	// Build index specification
	indexSpec := make(map[string]interface{})

	// Add fields/columns
	if len(index.Fields) > 0 {
		for _, field := range index.Fields {
			indexSpec[field] = 1 // Default ascending
		}
	} else if len(index.Columns) > 0 {
		for _, col := range index.Columns {
			indexSpec[col] = 1 // Default ascending
		}
	} else if index.Expression != "" {
		// For text indexes or complex expressions
		if strings.Contains(strings.ToLower(index.Expression), "text") {
			indexSpec["$**"] = "text" // Text index on all fields
		} else {
			return "", fmt.Errorf("unsupported index expression for MongoDB: %s", index.Expression)
		}
	} else {
		return "", fmt.Errorf("index must have fields, columns, or expression")
	}

	indexSpecJSON, _ := json.Marshal(indexSpec)
	sb.WriteString(string(indexSpecJSON))

	// Add index options
	options := make(map[string]interface{})
	options["name"] = index.Name

	if index.Unique {
		options["unique"] = true
	}

	if sparse, ok := index.Options["sparse"].(bool); ok && sparse {
		options["sparse"] = true
	}

	if background, ok := index.Options["background"].(bool); ok && background {
		options["background"] = true
	}

	// Add TTL for time-based collections
	if ttl, ok := index.Options["expireAfterSeconds"].(int); ok {
		options["expireAfterSeconds"] = ttl
	}

	// Add partial filter expression
	if partialFilter, ok := index.Options["partialFilterExpression"].(map[string]interface{}); ok {
		options["partialFilterExpression"] = partialFilter
	}

	if len(options) > 0 {
		optionsJSON, _ := json.Marshal(options)
		sb.WriteString(fmt.Sprintf(", %s", string(optionsJSON)))
	}

	sb.WriteString(");")

	return sb.String(), nil
}

// Executable code objects (MongoDB uses JavaScript functions)
func (mg *MongoDBGenerator) GenerateCreateFunctionSQL(fn unifiedmodel.Function) (string, error) {
	if fn.Name == "" {
		return "", fmt.Errorf("function name cannot be empty")
	}

	// MongoDB stored functions are JavaScript functions
	var sb strings.Builder
	sb.WriteString("db.system.js.save({\n")
	sb.WriteString(fmt.Sprintf("  _id: '%s',\n", fn.Name))
	sb.WriteString("  value: function(")

	// Add arguments
	var argNames []string
	for _, arg := range fn.Arguments {
		argNames = append(argNames, arg.Name)
	}
	sb.WriteString(strings.Join(argNames, ", "))
	sb.WriteString(") {\n")

	// Add function body
	sb.WriteString(fmt.Sprintf("    %s\n", fn.Definition))
	sb.WriteString("  }\n")
	sb.WriteString("});")

	return sb.String(), nil
}

// Security and access control
func (mg *MongoDBGenerator) GenerateCreateUserSQL(user unifiedmodel.DBUser) (string, error) {
	if user.Name == "" {
		return "", fmt.Errorf("user name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString("db.createUser({\n")
	sb.WriteString(fmt.Sprintf("  user: '%s',\n", user.Name))

	// Add password if specified in options
	if password, ok := user.Options["password"].(string); ok && password != "" {
		sb.WriteString(fmt.Sprintf("  pwd: '%s',\n", password))
	}

	// Add roles
	sb.WriteString("  roles: [")
	if len(user.Roles) > 0 {
		var roleSpecs []string
		for _, role := range user.Roles {
			// Check if role includes database specification
			if strings.Contains(role, "@") {
				parts := strings.Split(role, "@")
				roleSpecs = append(roleSpecs, fmt.Sprintf("{ role: '%s', db: '%s' }", parts[0], parts[1]))
			} else {
				roleSpecs = append(roleSpecs, fmt.Sprintf("'%s'", role))
			}
		}
		sb.WriteString(strings.Join(roleSpecs, ", "))
	}
	sb.WriteString("]\n")
	sb.WriteString("});")

	return sb.String(), nil
}

func (mg *MongoDBGenerator) GenerateCreateRoleSQL(role unifiedmodel.DBRole) (string, error) {
	if role.Name == "" {
		return "", fmt.Errorf("role name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString("db.createRole({\n")
	sb.WriteString(fmt.Sprintf("  role: '%s',\n", role.Name))

	// Add privileges (from options)
	sb.WriteString("  privileges: [")
	if privileges, ok := role.Labels["privileges"]; ok {
		sb.WriteString(privileges)
	}
	sb.WriteString("],\n")

	// Add parent roles
	sb.WriteString("  roles: [")
	if len(role.ParentRoles) > 0 {
		var parentRoleSpecs []string
		for _, parentRole := range role.ParentRoles {
			parentRoleSpecs = append(parentRoleSpecs, fmt.Sprintf("'%s'", parentRole))
		}
		sb.WriteString(strings.Join(parentRoleSpecs, ", "))
	}
	sb.WriteString("]\n")
	sb.WriteString("});")

	return sb.String(), nil
}

// Specialized Data Containers
func (mg *MongoDBGenerator) GenerateCreateDocumentSQL(document unifiedmodel.Document) (string, error) {
	if document.Key == "" {
		return "", fmt.Errorf("document key cannot be empty")
	}

	// MongoDB documents are inserted into collections
	collectionName := "documents" // Default collection
	if collection, ok := document.Fields["collection"].(string); ok && collection != "" {
		collectionName = collection
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("db.%s.insertOne(", collectionName))

	// Convert document fields to JSON
	if len(document.Fields) > 0 {
		// Add the key as _id if not already present
		docFields := make(map[string]interface{})
		for k, v := range document.Fields {
			docFields[k] = v
		}
		if _, exists := docFields["_id"]; !exists {
			docFields["_id"] = document.Key
		}

		contentJSON, err := json.Marshal(docFields)
		if err != nil {
			return "", fmt.Errorf("failed to marshal document fields: %w", err)
		}
		sb.WriteString(string(contentJSON))
	} else {
		sb.WriteString(fmt.Sprintf("{ _id: '%s' }", document.Key))
	}

	sb.WriteString(");")

	return sb.String(), nil
}

// Advanced analytics
func (mg *MongoDBGenerator) GenerateCreateProjectionSQL(projection unifiedmodel.Projection) (string, error) {
	if projection.Name == "" {
		return "", fmt.Errorf("projection name cannot be empty")
	}

	// MongoDB projections are typically used in aggregation pipelines
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("// Projection: %s\n", projection.Name))
	sb.WriteString("db.collection.aggregate([\n")
	sb.WriteString("  {\n")
	sb.WriteString("    $project: {\n")

	// Add projection fields from definition or options
	if projection.Definition != "" {
		// Use the definition directly if provided
		sb.WriteString(fmt.Sprintf("      %s", projection.Definition))
	} else if fields, ok := projection.Options["fields"].([]string); ok && len(fields) > 0 {
		var fieldSpecs []string
		for _, field := range fields {
			fieldSpecs = append(fieldSpecs, fmt.Sprintf("      %s: 1", field))
		}
		sb.WriteString(strings.Join(fieldSpecs, ",\n"))
	} else {
		sb.WriteString("      _id: 1") // Default projection
	}

	sb.WriteString("\n    }\n")
	sb.WriteString("  }\n")
	sb.WriteString("]);")

	return sb.String(), nil
}

// High-level generation methods
func (mg *MongoDBGenerator) GenerateSchema(model *unifiedmodel.UnifiedModel) (string, []string, error) {
	if model == nil {
		return "", nil, fmt.Errorf("unified model cannot be nil")
	}

	var statements []string
	var warnings []string

	// Generate in dependency order for MongoDB

	// 1. Database selection
	for _, database := range model.Databases {
		stmt, err := mg.GenerateCreateDatabaseSQL(database)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate database %s: %v", database.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 2. Collections (primary data containers)
	for _, collection := range model.Collections {
		stmt, err := mg.GenerateCreateCollectionSQL(collection)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate collection %s: %v", collection.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 3. Indexes (depend on collections)
	for _, index := range model.Indexes {
		stmt, err := mg.GenerateCreateIndexSQL(index)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate index %s: %v", index.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 4. Functions (JavaScript stored functions)
	for _, fn := range model.Functions {
		stmt, err := mg.GenerateCreateFunctionSQL(fn)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate function %s: %v", fn.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 5. Users and Roles
	for _, role := range model.Roles {
		stmt, err := mg.GenerateCreateRoleSQL(role)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate role %s: %v", role.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	for _, user := range model.Users {
		stmt, err := mg.GenerateCreateUserSQL(user)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate user %s: %v", user.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 6. Documents (sample data)
	for _, document := range model.Documents {
		stmt, err := mg.GenerateCreateDocumentSQL(document)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate document %s: %v", document.Key, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 7. Projections (aggregation pipelines)
	for _, projection := range model.Projections {
		stmt, err := mg.GenerateCreateProjectionSQL(projection)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate projection %s: %v", projection.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// Combine all statements
	fullScript := strings.Join(statements, "\n\n")

	return fullScript, warnings, nil
}

func (mg *MongoDBGenerator) GenerateCreateStatements(model *unifiedmodel.UnifiedModel) ([]string, error) {
	if model == nil {
		return nil, fmt.Errorf("unified model cannot be nil")
	}

	var statements []string

	// Generate in dependency order (same as GenerateSchema but with error propagation)

	// 1. Database selection
	for _, database := range model.Databases {
		stmt, err := mg.GenerateCreateDatabaseSQL(database)
		if err != nil {
			return nil, fmt.Errorf("failed to generate database %s: %w", database.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 2. Collections
	for _, collection := range model.Collections {
		stmt, err := mg.GenerateCreateCollectionSQL(collection)
		if err != nil {
			return nil, fmt.Errorf("failed to generate collection %s: %w", collection.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 3. Indexes
	for _, index := range model.Indexes {
		stmt, err := mg.GenerateCreateIndexSQL(index)
		if err != nil {
			return nil, fmt.Errorf("failed to generate index %s: %w", index.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 4. Functions
	for _, fn := range model.Functions {
		stmt, err := mg.GenerateCreateFunctionSQL(fn)
		if err != nil {
			return nil, fmt.Errorf("failed to generate function %s: %w", fn.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 5. Roles
	for _, role := range model.Roles {
		stmt, err := mg.GenerateCreateRoleSQL(role)
		if err != nil {
			return nil, fmt.Errorf("failed to generate role %s: %w", role.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 6. Users
	for _, user := range model.Users {
		stmt, err := mg.GenerateCreateUserSQL(user)
		if err != nil {
			return nil, fmt.Errorf("failed to generate user %s: %w", user.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 7. Documents
	for _, document := range model.Documents {
		stmt, err := mg.GenerateCreateDocumentSQL(document)
		if err != nil {
			return nil, fmt.Errorf("failed to generate document %s: %w", document.Key, err)
		}
		statements = append(statements, stmt)
	}

	// 8. Projections
	for _, projection := range model.Projections {
		stmt, err := mg.GenerateCreateProjectionSQL(projection)
		if err != nil {
			return nil, fmt.Errorf("failed to generate projection %s: %w", projection.Name, err)
		}
		statements = append(statements, stmt)
	}

	return statements, nil
}

// Helper methods

func (mg *MongoDBGenerator) generateJSONSchemaValidator(fields []unifiedmodel.Field) map[string]interface{} {
	validator := map[string]interface{}{
		"bsonType": "object",
	}

	// Add required fields
	var required []string
	properties := make(map[string]interface{})

	for _, field := range fields {
		if field.Required {
			required = append(required, field.Name)
		}

		// Generate property definition
		property := map[string]interface{}{
			"bsonType": mg.convertToBSONType(field.Type),
		}

		// Add description if available
		if description, ok := field.Options["description"].(string); ok && description != "" {
			property["description"] = description
		}

		properties[field.Name] = property
	}

	if len(required) > 0 {
		validator["required"] = required
	}

	if len(properties) > 0 {
		validator["properties"] = properties
	}

	return validator
}

func (mg *MongoDBGenerator) convertToBSONType(fieldType string) string {
	switch strings.ToLower(fieldType) {
	case "string", "varchar", "text":
		return "string"
	case "int", "integer":
		return "int"
	case "long", "bigint":
		return "long"
	case "double", "float", "decimal", "numeric":
		return "double"
	case "boolean", "bool":
		return "bool"
	case "date", "datetime", "timestamp":
		return "date"
	case "objectid":
		return "objectId"
	case "binary", "bytea":
		return "binData"
	case "object", "json", "jsonb":
		return "object"
	case "array":
		return "array"
	default:
		// Handle array types
		if strings.HasSuffix(fieldType, "[]") {
			return "array"
		}
		// Default to string for unknown types
		return "string"
	}
}
