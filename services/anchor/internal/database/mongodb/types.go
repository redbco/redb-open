package mongodb

import (
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// CreateMongoDBUnifiedModel creates a UnifiedModel for MongoDB with database details
func CreateMongoDBUnifiedModel(uniqueIdentifier, version string, databaseSize int64) *unifiedmodel.UnifiedModel {
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.MongoDB,
		Collections:  make(map[string]unifiedmodel.Collection),
		Indexes:      make(map[string]unifiedmodel.Index),
		Functions:    make(map[string]unifiedmodel.Function),
		Databases:    make(map[string]unifiedmodel.Database),
	}
	return um
}

// ConvertMongoDBCollectionToUnified converts common.CollectionInfo to unifiedmodel.Collection
func ConvertMongoDBCollectionToUnified(collectionInfo common.CollectionInfo) unifiedmodel.Collection {
	collection := unifiedmodel.Collection{
		Name:    collectionInfo.Name,
		Fields:  make(map[string]unifiedmodel.Field),
		Indexes: make(map[string]unifiedmodel.Index),
		Options: collectionInfo.Options,
	}

	// Convert sample documents to inferred fields
	for _, sampleDoc := range collectionInfo.SampleDocs {
		for fieldName, fieldValue := range sampleDoc {
			if fieldName == "_id" {
				continue // Skip the MongoDB ObjectId field
			}

			fieldType := inferFieldType(fieldValue)
			collection.Fields[fieldName] = unifiedmodel.Field{
				Name: fieldName,
				Type: fieldType,
			}
		}
	}

	// Convert indexes
	for _, idx := range collectionInfo.Indexes {
		index := unifiedmodel.Index{
			Name:    idx.Name,
			Columns: idx.Columns, // In MongoDB, these are field names
			Unique:  idx.IsUnique,
		}
		collection.Indexes[idx.Name] = index
	}

	return collection
}

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
