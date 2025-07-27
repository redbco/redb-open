package adapters

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type MongoDBIngester struct{}

func (m *MongoDBIngester) IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error) {
	var mongoSchema models.MongoDBSchema
	if err := json.Unmarshal(rawSchema, &mongoSchema); err != nil {
		return nil, nil, err
	}

	unifiedModel := &models.UnifiedModel{}
	warnings := []string{}

	// Convert collections to tables
	for _, collection := range mongoSchema.Collections {
		unifiedTable := models.Table{
			Name:      collection.Name,
			TableType: convertMongoTableType(collection.TableType),
		}

		// Convert fields to columns
		for _, field := range collection.Fields {
			dataType, typeWarnings := convertMongoType(field.Type)
			warnings = append(warnings, typeWarnings...)

			unifiedCol := models.Column{
				Name:         field.Name,
				IsNullable:   !field.Required,
				IsPrimaryKey: field.IsPrimaryKey,
				DefaultValue: convertMongoDefault(field.Default),
				DataType:     dataType,
			}
			unifiedTable.Columns = append(unifiedTable.Columns, unifiedCol)
		}

		unifiedModel.Tables = append(unifiedModel.Tables, unifiedTable)
	}

	return unifiedModel, warnings, nil
}

type MongoDBExporter struct{}

func (m *MongoDBExporter) ExportSchema(model *models.UnifiedModel) (interface{}, []string, error) {
	mongoSchema := models.MongoDBSchema{
		SchemaType: "mongodb",
	}
	warnings := []string{}

	// Convert tables to collections
	for _, table := range model.Tables {
		collection := models.MongoCollection{
			Name:      table.Name,
			TableType: convertToMongoTableType(table.TableType), // Add this line
		}

		// Convert columns to fields
		for _, col := range table.Columns {
			field := models.MongoField{
				Name:         col.Name,
				Required:     !col.IsNullable,
				IsPrimaryKey: col.IsPrimaryKey,
				Type:         convertToMongoType(col.DataType),
				Default:      convertToMongoDefault(col.DefaultValue),
			}
			collection.Fields = append(collection.Fields, field)
		}

		mongoSchema.Collections = append(mongoSchema.Collections, collection)
	}

	return mongoSchema, warnings, nil
}

// Helper functions for type conversion
func convertMongoType(mongoType string) (models.DataType, []string) {
	// Initialize with basic category as default
	dataType := models.DataType{
		TypeCategory: "basic",
	}
	warnings := []string{}

	// Handle array types first
	if strings.HasSuffix(mongoType, "[]") {
		dataType.TypeCategory = "array"
		dataType.IsArray = true
		dataType.ArrayDimensions = strings.Count(mongoType, "[]")
		mongoType = strings.TrimSuffix(mongoType, strings.Repeat("[]", dataType.ArrayDimensions))
	}

	// Handle specific MongoDB types
	switch strings.ToLower(mongoType) {
	case "string":
		dataType.BaseType = "text"

	case "objectid":
		dataType.TypeCategory = "basic"
		dataType.BaseType = "string"
		dataType.Modifiers = []string{"objectid"}

	case "int", "integer":
		dataType.BaseType = "integer"

	case "long":
		dataType.BaseType = "bigint"

	case "double", "decimal":
		dataType.BaseType = "decimal"
		dataType.Precision = 18
		dataType.Scale = 2

	case "boolean":
		dataType.BaseType = "boolean"

	case "date":
		dataType.BaseType = "timestamp"

	case "binary":
		dataType.BaseType = "binary"

	case "object":
		dataType.TypeCategory = "composite"
		dataType.IsComposite = true
		dataType.BaseType = "object"

	case "array":
		dataType.TypeCategory = "array"
		dataType.IsArray = true
		dataType.ArrayDimensions = 1
		dataType.BaseType = "any"

	default:
		// Handle custom types
		if strings.HasPrefix(mongoType, "enum.") {
			dataType.TypeCategory = "enum"
			dataType.IsEnum = true
			dataType.BaseType = strings.TrimPrefix(mongoType, "enum.")
		} else if strings.HasPrefix(mongoType, "composite.") {
			dataType.TypeCategory = "composite"
			dataType.IsComposite = true
			dataType.BaseType = strings.TrimPrefix(mongoType, "composite.")
		} else {
			// Default to string for unknown types
			dataType.BaseType = "text"
			warnings = append(warnings, fmt.Sprintf("Unknown MongoDB type '%s' defaulting to text", mongoType))
		}
	}

	return dataType, warnings
}

func convertToMongoType(commonType models.DataType) string {
	var mongoType string

	switch commonType.TypeCategory {
	case "basic":
		switch strings.ToLower(commonType.BaseType) {
		case "text", "varchar", "char", "string":
			mongoType = "String"
		case "integer":
			mongoType = "Int"
		case "bigint":
			mongoType = "Long"
		case "decimal":
			mongoType = "Double"
		case "boolean":
			mongoType = "Boolean"
		case "timestamp":
			mongoType = "Date"
		case "binary":
			mongoType = "Binary"
		default:
			mongoType = "String" // Default to String for unknown types
		}

	case "array":
		baseType := convertToMongoType(models.DataType{
			TypeCategory: "basic",
			BaseType:     commonType.BaseType,
		})
		mongoType = baseType + strings.Repeat("[]", commonType.ArrayDimensions)

	case "enum":
		mongoType = "enum." + commonType.BaseType

	case "composite":
		mongoType = "composite." + commonType.BaseType

	case "extension":
		// Handle MongoDB-specific extensions
		if commonType.ExtensionName == "mongodb" {
			switch commonType.BaseType {
			case "objectid":
				mongoType = "ObjectId"
			default:
				mongoType = commonType.BaseType
			}
		} else {
			mongoType = "String" // Default to String for unknown extensions
		}

	default:
		mongoType = "String" // Default to String for unknown categories
	}

	return mongoType
}

func convertMongoDefault(mongoDefault interface{}) *string {
	if mongoDefault == nil {
		return nil
	}
	// Convert the default value to string
	str := fmt.Sprintf("%v", mongoDefault)
	return &str
}

func convertToMongoDefault(defaultValue *string) interface{} {
	if defaultValue == nil {
		return nil
	}
	// You might want to add more sophisticated conversion logic here
	return *defaultValue
}

func convertMongoTableType(mongoTableType string) string {
	if mongoTableType == "" {
		return "unified.standard"
	}

	// Remove the "mongodb." prefix if it exists and add "unified." prefix
	tableType := mongoTableType
	if len(tableType) > 8 && tableType[:8] == "mongodb." {
		tableType = tableType[8:]
	}
	return "unified." + tableType
}

func convertToMongoTableType(unifiedTableType string) string {
	if unifiedTableType == "" {
		return "mongodb.standard"
	}

	// Remove the "unified." prefix if it exists and add "mongodb." prefix
	tableType := unifiedTableType
	if len(tableType) > 8 && tableType[:8] == "unified." {
		tableType = tableType[8:]
	}
	return "mongodb." + tableType
}
