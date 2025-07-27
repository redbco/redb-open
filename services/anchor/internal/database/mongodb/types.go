package mongodb

import (
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// MongoDBDetails contains information about a MongoDB database
type MongoDBDetails struct {
	UniqueIdentifier string `json:"uniqueIdentifier"`
	DatabaseType     string `json:"databaseType"`
	DatabaseEdition  string `json:"databaseEdition"`
	Version          string `json:"version"`
	DatabaseSize     int64  `json:"databaseSize"`
}

// MongoDBSchema represents the schema of a MongoDB database
type MongoDBSchema struct {
	Collections []common.CollectionInfo     `json:"collections"`
	Indexes     []common.IndexInfo          `json:"indexes"`
	Functions   []common.FunctionInfo       `json:"functions"`
	Schemas     []common.DatabaseSchemaInfo `json:"schemas"`
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
