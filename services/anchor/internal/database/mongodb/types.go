package mongodb

import (
	"go.mongodb.org/mongo-driver/v2/bson"
)

// inferFieldType infers the field type from a sample value
func inferFieldType(value interface{}) string {
	switch value.(type) {
	case string:
		return "string"
	case int, int32, int64:
		return "integer"
	case float32, float64:
		return "number"
	case bool:
		return "boolean"
	case []interface{}:
		return "array"
	case map[string]interface{}, bson.M:
		return "object"
	case nil:
		return "null"
	default:
		return "mixed"
	}
}

// MongoDBReplicationSourceDetails contains information about a MongoDB change stream
type MongoDBReplicationSourceDetails struct {
	CollectionName string `json:"collection_name"`
	DatabaseID     string `json:"database_id"`
	ResumeToken    string `json:"resume_token,omitempty"`
}

// MongoDBReplicationChange represents a change in MongoDB
type MongoDBReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}

// MongoDBSchemaField represents a field in a MongoDB schema
type MongoDBSchemaField struct {
	Type       string                        `json:"type"`
	Properties map[string]MongoDBSchemaField `json:"properties,omitempty"`
	Items      *MongoDBSchemaField           `json:"items,omitempty"`
}

// MongoDBCollectionSchema represents the inferred schema for a MongoDB collection
type MongoDBCollectionSchema struct {
	Fields map[string]MongoDBSchemaField `json:"fields"`
}

// MongoDBValidationRule represents a validation rule for a MongoDB collection
type MongoDBValidationRule struct {
	Rule       bson.D `json:"rule"`
	Level      string `json:"level"`
	ActionType string `json:"actionType"`
}
