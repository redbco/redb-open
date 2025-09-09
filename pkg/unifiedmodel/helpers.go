// Package unifiedmodel helpers provides utility functions for working with UnifiedModel schemas.
// These are commonly needed functions that can be used by multiple services.

package unifiedmodel

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// SerializeSchema converts a UnifiedModel to JSON bytes for storage or transmission.
func SerializeSchema(schema *UnifiedModel) ([]byte, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}

	return json.MarshalIndent(schema, "", "  ")
}

// DeserializeSchema converts JSON bytes back to a UnifiedModel.
func DeserializeSchema(data []byte) (*UnifiedModel, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data cannot be empty")
	}

	var schema UnifiedModel
	err := json.Unmarshal(data, &schema)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize schema: %w", err)
	}

	return &schema, nil
}

// GetSchemaInfo returns basic information about a schema.
func GetSchemaInfo(schema *UnifiedModel) SchemaInfo {
	if schema == nil {
		return SchemaInfo{}
	}

	return SchemaInfo{
		DatabaseType:      string(schema.DatabaseType),
		SchemaID:          GenerateSchemaID(schema),
		Hash:              GenerateSchemaHash(schema),
		TotalObjects:      countTotalObjects(schema),
		Tables:            len(schema.Tables),
		Collections:       len(schema.Collections),
		Views:             len(schema.Views),
		MaterializedViews: len(schema.MaterializedViews),
		Indexes:           len(schema.Indexes),
		Constraints:       len(schema.Constraints),
		Functions:         len(schema.Functions),
		Procedures:        len(schema.Procedures),
		IsEmpty:           IsSchemaEmpty(schema),
	}
}

// GetObjectNames returns sorted lists of object names by type.
func GetObjectNames(schema *UnifiedModel) ObjectNames {
	if schema == nil {
		return ObjectNames{}
	}

	return ObjectNames{
		Tables:            getSortedKeys(schema.Tables),
		Collections:       getSortedKeys(schema.Collections),
		Views:             getSortedKeys(schema.Views),
		MaterializedViews: getSortedKeys(schema.MaterializedViews),
		Indexes:           getSortedKeys(schema.Indexes),
		Constraints:       getSortedKeys(schema.Constraints),
		Functions:         getSortedKeys(schema.Functions),
		Procedures:        getSortedKeys(schema.Procedures),
		Triggers:          getSortedKeys(schema.Triggers),
		Sequences:         getSortedKeys(schema.Sequences),
		Types:             getSortedKeys(schema.Types),
		Users:             getSortedKeys(schema.Users),
		Roles:             getSortedKeys(schema.Roles),
	}
}

// GetTable retrieves a table by name.
func (schema *UnifiedModel) GetTable(name string) (*Table, bool) {
	if schema == nil || schema.Tables == nil {
		return nil, false
	}
	table, exists := schema.Tables[name]
	return &table, exists
}

// GetCollection retrieves a collection by name.
func (schema *UnifiedModel) GetCollection(name string) (*Collection, bool) {
	if schema == nil || schema.Collections == nil {
		return nil, false
	}
	collection, exists := schema.Collections[name]
	return &collection, exists
}

// GetView retrieves a view by name.
func (schema *UnifiedModel) GetView(name string) (*View, bool) {
	if schema == nil || schema.Views == nil {
		return nil, false
	}
	view, exists := schema.Views[name]
	return &view, exists
}

// GetIndex retrieves an index by name.
func (schema *UnifiedModel) GetIndex(name string) (*Index, bool) {
	if schema == nil || schema.Indexes == nil {
		return nil, false
	}
	index, exists := schema.Indexes[name]
	return &index, exists
}

// GetConstraint retrieves a constraint by name.
func (schema *UnifiedModel) GetConstraint(name string) (*Constraint, bool) {
	if schema == nil || schema.Constraints == nil {
		return nil, false
	}
	constraint, exists := schema.Constraints[name]
	return &constraint, exists
}

// GetFunction retrieves a function by name.
func (schema *UnifiedModel) GetFunction(name string) (*Function, bool) {
	if schema == nil || schema.Functions == nil {
		return nil, false
	}
	function, exists := schema.Functions[name]
	return &function, exists
}

// AddTable adds a table to the schema.
func (schema *UnifiedModel) AddTable(table Table) {
	if schema == nil {
		return
	}
	if schema.Tables == nil {
		schema.Tables = make(map[string]Table)
	}
	schema.Tables[table.Name] = table
}

// AddCollection adds a collection to the schema.
func (schema *UnifiedModel) AddCollection(collection Collection) {
	if schema == nil {
		return
	}
	if schema.Collections == nil {
		schema.Collections = make(map[string]Collection)
	}
	schema.Collections[collection.Name] = collection
}

// AddIndex adds an index to the schema.
func (schema *UnifiedModel) AddIndex(index Index) {
	if schema == nil {
		return
	}
	if schema.Indexes == nil {
		schema.Indexes = make(map[string]Index)
	}
	schema.Indexes[index.Name] = index
}

// AddConstraint adds a constraint to the schema.
func (schema *UnifiedModel) AddConstraint(constraint Constraint) {
	if schema == nil {
		return
	}
	if schema.Constraints == nil {
		schema.Constraints = make(map[string]Constraint)
	}
	schema.Constraints[constraint.Name] = constraint
}

// RemoveTable removes a table from the schema.
func (schema *UnifiedModel) RemoveTable(name string) bool {
	if schema == nil || schema.Tables == nil {
		return false
	}
	_, exists := schema.Tables[name]
	if exists {
		delete(schema.Tables, name)
	}
	return exists
}

// RemoveCollection removes a collection from the schema.
func (schema *UnifiedModel) RemoveCollection(name string) bool {
	if schema == nil || schema.Collections == nil {
		return false
	}
	_, exists := schema.Collections[name]
	if exists {
		delete(schema.Collections, name)
	}
	return exists
}

// RemoveIndex removes an index from the schema.
func (schema *UnifiedModel) RemoveIndex(name string) bool {
	if schema == nil || schema.Indexes == nil {
		return false
	}
	_, exists := schema.Indexes[name]
	if exists {
		delete(schema.Indexes, name)
	}
	return exists
}

// HasObject checks if an object of a specific type and name exists.
func (schema *UnifiedModel) HasObject(objectType ObjectType, name string) bool {
	if schema == nil {
		return false
	}

	switch objectType {
	case ObjectTypeTable:
		_, exists := schema.Tables[name]
		return exists
	case ObjectTypeCollection:
		_, exists := schema.Collections[name]
		return exists
	case ObjectTypeView:
		_, exists := schema.Views[name]
		return exists
	case ObjectTypeMaterializedView:
		_, exists := schema.MaterializedViews[name]
		return exists
	case ObjectTypeNode:
		_, exists := schema.Nodes[name]
		return exists
	case ObjectTypeGraph:
		_, exists := schema.Graphs[name]
		return exists
	case ObjectTypeVector:
		_, exists := schema.Vectors[name]
		return exists
	case ObjectTypeVectorIndex:
		_, exists := schema.VectorIndexes[name]
		return exists
	case ObjectTypeSearchIndex:
		_, exists := schema.SearchIndexes[name]
		return exists
	case ObjectTypeDocument:
		_, exists := schema.Documents[name]
		return exists
	default:
		return false
	}
}

// GetObjectsByType returns all objects of a specific type.
func (schema *UnifiedModel) GetObjectsByType(objectType ObjectType) map[string]interface{} {
	if schema == nil {
		return nil
	}

	result := make(map[string]interface{})

	switch objectType {
	case ObjectTypeTable:
		for name, obj := range schema.Tables {
			result[name] = obj
		}
	case ObjectTypeCollection:
		for name, obj := range schema.Collections {
			result[name] = obj
		}
	case ObjectTypeView:
		for name, obj := range schema.Views {
			result[name] = obj
		}
	case ObjectTypeMaterializedView:
		for name, obj := range schema.MaterializedViews {
			result[name] = obj
		}
	case ObjectTypeNode:
		for name, obj := range schema.Nodes {
			result[name] = obj
		}
	case ObjectTypeGraph:
		for name, obj := range schema.Graphs {
			result[name] = obj
		}
	case ObjectTypeVector:
		for name, obj := range schema.Vectors {
			result[name] = obj
		}
	case ObjectTypeVectorIndex:
		for name, obj := range schema.VectorIndexes {
			result[name] = obj
		}
	case ObjectTypeSearchIndex:
		for name, obj := range schema.SearchIndexes {
			result[name] = obj
		}
	case ObjectTypeDocument:
		for name, obj := range schema.Documents {
			result[name] = obj
		}
	}

	return result
}

// FilterObjects returns a new schema containing only objects that match the filter function.
func FilterObjects(schema *UnifiedModel, filter func(objectType ObjectType, name string, obj interface{}) bool) *UnifiedModel {
	if schema == nil {
		return nil
	}

	filtered := &UnifiedModel{
		DatabaseType:      schema.DatabaseType,
		Tables:            make(map[string]Table),
		Collections:       make(map[string]Collection),
		Views:             make(map[string]View),
		MaterializedViews: make(map[string]MaterializedView),
		Indexes:           make(map[string]Index),
		Constraints:       make(map[string]Constraint),
		Functions:         make(map[string]Function),
		Procedures:        make(map[string]Procedure),
	}

	// Filter tables
	for name, table := range schema.Tables {
		if filter(ObjectTypeTable, name, table) {
			filtered.Tables[name] = table
		}
	}

	// Filter collections
	for name, collection := range schema.Collections {
		if filter(ObjectTypeCollection, name, collection) {
			filtered.Collections[name] = collection
		}
	}

	// Filter views
	for name, view := range schema.Views {
		if filter(ObjectTypeView, name, view) {
			filtered.Views[name] = view
		}
	}

	// Filter materialized views
	for name, matView := range schema.MaterializedViews {
		if filter(ObjectTypeMaterializedView, name, matView) {
			filtered.MaterializedViews[name] = matView
		}
	}

	return filtered
}

// FindObjectReferences finds all objects that reference a specific object.
func FindObjectReferences(schema *UnifiedModel, objectType ObjectType, objectName string) []ObjectReference {
	if schema == nil {
		return nil
	}

	var references []ObjectReference

	// Search for foreign key references in constraints
	for constraintName, constraint := range schema.Constraints {
		if constraint.Type == ConstraintTypeForeignKey {
			// Check if this constraint references the target object
			if constraint.Reference.Table == objectName && objectType == ObjectTypeTable {
				references = append(references, ObjectReference{
					SourceType:    "constraint",
					SourceName:    constraintName,
					TargetType:    string(objectType),
					TargetName:    objectName,
					ReferenceType: "foreign_key",
				})
			}
		}
	}

	// Search for index references (indexes reference columns within tables)
	for indexName, index := range schema.Indexes {
		for _, columnName := range index.Columns {
			if columnName == objectName {
				references = append(references, ObjectReference{
					SourceType:    "index",
					SourceName:    indexName,
					TargetType:    string(objectType),
					TargetName:    objectName,
					ReferenceType: "index_column",
				})
			}
		}
	}

	// Search for view dependencies
	for viewName, view := range schema.Views {
		if strings.Contains(strings.ToLower(view.Definition), strings.ToLower(objectName)) {
			references = append(references, ObjectReference{
				SourceType:    "view",
				SourceName:    viewName,
				TargetType:    string(objectType),
				TargetName:    objectName,
				ReferenceType: "view_dependency",
			})
		}
	}

	return references
}

// Supporting types

type SchemaInfo struct {
	DatabaseType      string `json:"database_type"`
	SchemaID          string `json:"schema_id"`
	Hash              string `json:"hash"`
	TotalObjects      int    `json:"total_objects"`
	Tables            int    `json:"tables"`
	Collections       int    `json:"collections"`
	Views             int    `json:"views"`
	MaterializedViews int    `json:"materialized_views"`
	Indexes           int    `json:"indexes"`
	Constraints       int    `json:"constraints"`
	Functions         int    `json:"functions"`
	Procedures        int    `json:"procedures"`
	IsEmpty           bool   `json:"is_empty"`
}

type ObjectNames struct {
	Tables            []string `json:"tables"`
	Collections       []string `json:"collections"`
	Views             []string `json:"views"`
	MaterializedViews []string `json:"materialized_views"`
	Indexes           []string `json:"indexes"`
	Constraints       []string `json:"constraints"`
	Functions         []string `json:"functions"`
	Procedures        []string `json:"procedures"`
	Triggers          []string `json:"triggers"`
	Sequences         []string `json:"sequences"`
	Types             []string `json:"types"`
	Users             []string `json:"users"`
	Roles             []string `json:"roles"`
}

type ObjectReference struct {
	SourceType    string `json:"source_type"`    // Type of object making the reference
	SourceName    string `json:"source_name"`    // Name of object making the reference
	TargetType    string `json:"target_type"`    // Type of object being referenced
	TargetName    string `json:"target_name"`    // Name of object being referenced
	ReferenceType string `json:"reference_type"` // Type of reference (foreign_key, index_on_table, etc.)
}

// Helper functions

func getSortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
