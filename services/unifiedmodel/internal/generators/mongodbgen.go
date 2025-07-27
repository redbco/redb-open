package generators

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type MongoDBGenerator struct {
	BaseGenerator
}

func (mg *MongoDBGenerator) GenerateCreateStatements(schema interface{}) ([]string, error) {
	mongoSchema, ok := schema.(models.MongoDBSchema)
	if !ok {
		// Try to convert from map[string]interface{} if direct type assertion fails
		schemaMap, ok := schema.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid schema type for MongoDB generator")
		}

		// Convert the map to MongoDBSchema
		schemaBytes, err := json.Marshal(schemaMap)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal schema: %v", err)
		}

		if err := json.Unmarshal(schemaBytes, &mongoSchema); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to MongoDBSchema: %v", err)
		}
	}

	var statements []string

	// Generate collection creation statements
	for _, collection := range mongoSchema.Collections {
		createStmt := mg.generateCollectionStatement(collection)
		statements = append(statements, createStmt)

		// Generate validator schema if there are fields
		if len(collection.Fields) > 0 {
			validatorStmt := mg.generateValidatorStatement(collection)
			statements = append(statements, validatorStmt)
		}
	}

	return statements, nil
}

func (mg *MongoDBGenerator) generateCollectionStatement(collection models.MongoCollection) string {
	return fmt.Sprintf("db.createCollection('%s');", collection.Name)
}

func (mg *MongoDBGenerator) generateValidatorStatement(collection models.MongoCollection) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("db.runCommand({ collMod: '%s',", collection.Name))
	sb.WriteString("  validator: { $jsonSchema: { bsonType: 'object', required: [")

	// Add required fields
	var requiredFields []string
	for _, field := range collection.Fields {
		if field.Required {
			requiredFields = append(requiredFields, fmt.Sprintf("'%s'", field.Name))
		}
	}
	sb.WriteString(strings.Join(requiredFields, ", "))
	sb.WriteString("], ")

	// Add properties
	sb.WriteString("properties: { ")

	var propertyDefs []string
	for _, field := range collection.Fields {
		propDef := mg.generatePropertyDefinition(field)
		propertyDefs = append(propertyDefs, propDef)
	}

	sb.WriteString(strings.Join(propertyDefs, ", "))
	sb.WriteString(" } } }, ")
	sb.WriteString("validationLevel: 'strict', ")
	sb.WriteString("validationAction: 'error'")
	sb.WriteString("});")

	return sb.String()
}

func (mg *MongoDBGenerator) generatePropertyDefinition(field models.MongoField) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("'%s': {", field.Name))

	// Handle array types
	if strings.HasSuffix(field.Type, "[]") {
		sb.WriteString("bsonType: 'array', items: {")
		baseType := strings.TrimSuffix(field.Type, "[]")
		sb.WriteString(fmt.Sprintf("bsonType: '%s'", mg.convertToBSONType(baseType)))
		sb.WriteString("}")
	} else {
		sb.WriteString(fmt.Sprintf("bsonType: '%s'", mg.convertToBSONType(field.Type)))

		// Add default value if specified
		if field.Default != nil {
			sb.WriteString(fmt.Sprintf(", default: %v", field.Default))
		}
	}

	sb.WriteString("}")
	return sb.String()
}

func (mg *MongoDBGenerator) convertToBSONType(mongoType string) string {
	switch strings.ToLower(mongoType) {
	case "string":
		return "string"
	case "int", "integer":
		return "int"
	case "long":
		return "long"
	case "double", "decimal":
		return "double"
	case "boolean":
		return "bool"
	case "date":
		return "date"
	case "objectid":
		return "objectId"
	case "binary":
		return "binData"
	case "object":
		return "object"
	default:
		// Handle custom types or return string as default
		return "string"
	}
}
