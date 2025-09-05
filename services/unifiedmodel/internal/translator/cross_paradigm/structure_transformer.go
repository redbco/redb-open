package cross_paradigm

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// StructureTransformer handles structural transformations between paradigms
type StructureTransformer struct{}

// NewStructureTransformer creates a new structure transformer
func NewStructureTransformer() *StructureTransformer {
	return &StructureTransformer{}
}

// NormalizeCollection converts a collection to normalized relational tables
func (st *StructureTransformer) NormalizeCollection(collection unifiedmodel.Collection, enrichmentCtx *EnrichmentContext) (map[string]unifiedmodel.Table, error) {
	tables := make(map[string]unifiedmodel.Table)

	// Create main table from collection
	mainTable := unifiedmodel.Table{
		Name:        collection.Name,
		Owner:       collection.Owner,
		Comment:     collection.Comment,
		Labels:      collection.Labels,
		Options:     collection.Options,
		Columns:     make(map[string]unifiedmodel.Column),
		Indexes:     make(map[string]unifiedmodel.Index),
		Constraints: make(map[string]unifiedmodel.Constraint),
	}

	// Add primary key column (use int32 for MongoDB compatibility)
	mainTable.Columns["id"] = unifiedmodel.Column{
		Name:          "id",
		DataType:      "int32", // Use MongoDB-compatible type for proper conversion
		Nullable:      false,
		IsPrimaryKey:  true,
		AutoIncrement: true,
	}

	// Convert fields to columns, extracting nested objects to separate tables
	for fieldName, field := range collection.Fields {
		if st.isNestedObject(field) {
			// Create separate table for nested object
			nestedTable, err := st.createNestedTable(collection.Name, fieldName, field)
			if err != nil {
				return nil, fmt.Errorf("failed to create nested table for field %s: %w", fieldName, err)
			}
			tables[fmt.Sprintf("%s_%s", collection.Name, fieldName)] = nestedTable

			// Add foreign key reference in main table
			mainTable.Columns[fieldName+"_id"] = unifiedmodel.Column{
				Name:     fieldName + "_id",
				DataType: "int32", // Use MongoDB-compatible type
				Nullable: true,
			}
		} else if st.isArray(field) {
			// Create junction table for array fields
			junctionTable, err := st.createArrayTable(collection.Name, fieldName, field)
			if err != nil {
				return nil, fmt.Errorf("failed to create array table for field %s: %w", fieldName, err)
			}
			tables[fmt.Sprintf("%s_%s", collection.Name, fieldName)] = junctionTable
		} else {
			// Convert simple field to column
			column := st.fieldToColumn(fieldName, field)
			mainTable.Columns[fieldName] = column
		}
	}

	tables[collection.Name] = mainTable
	return tables, nil
}

// DenormalizeTable converts a relational table to a denormalized collection
func (st *StructureTransformer) DenormalizeTable(table unifiedmodel.Table, enrichmentCtx *EnrichmentContext, schema *unifiedmodel.UnifiedModel) (unifiedmodel.Collection, error) {
	collection := unifiedmodel.Collection{
		Name:    table.Name,
		Owner:   table.Owner,
		Comment: table.Comment,
		Labels:  table.Labels,
		Options: table.Options,
		Fields:  make(map[string]unifiedmodel.Field),
	}

	// Convert columns to fields
	for columnName, column := range table.Columns {
		if st.isForeignKeyColumn(columnName) {
			// Handle foreign key relationships
			if err := st.embedRelatedData(&collection, columnName, column, enrichmentCtx, schema); err != nil {
				// If embedding fails, convert as regular field
				field := st.columnToField(columnName, column)
				collection.Fields[columnName] = field
			}
		} else {
			// Convert regular column to field
			field := st.columnToField(columnName, column)
			collection.Fields[columnName] = field
		}
	}

	return collection, nil
}

// TableToNode converts a relational table to a graph node
func (st *StructureTransformer) TableToNode(table unifiedmodel.Table, enrichmentCtx *EnrichmentContext) (unifiedmodel.Node, error) {
	node := unifiedmodel.Node{
		Label:      table.Name,
		Properties: make(map[string]unifiedmodel.Property),
		Indexes:    make(map[string]unifiedmodel.Index),
	}

	// Convert columns to properties, excluding foreign keys (they become relationships)
	for columnName, column := range table.Columns {
		if !st.isForeignKeyColumn(columnName) {
			property := st.columnToProperty(columnName, column)
			node.Properties[columnName] = property
		}
	}

	// Convert indexes
	for indexName, index := range table.Indexes {
		node.Indexes[indexName] = index
	}

	return node, nil
}

// NodeToTable converts a graph node to a relational table
func (st *StructureTransformer) NodeToTable(node unifiedmodel.Node, enrichmentCtx *EnrichmentContext) (unifiedmodel.Table, error) {
	table := unifiedmodel.Table{
		Name:        node.Label,
		Columns:     make(map[string]unifiedmodel.Column),
		Indexes:     make(map[string]unifiedmodel.Index),
		Constraints: make(map[string]unifiedmodel.Constraint),
	}

	// Add primary key column
	table.Columns["id"] = unifiedmodel.Column{
		Name:          "id",
		DataType:      "int32", // Use MongoDB-compatible type
		Nullable:      false,
		IsPrimaryKey:  true,
		AutoIncrement: true,
	}

	// Convert properties to columns
	for propertyName, property := range node.Properties {
		column := st.propertyToColumn(propertyName, property)
		table.Columns[propertyName] = column
	}

	// Convert indexes
	for indexName, index := range node.Indexes {
		table.Indexes[indexName] = index
	}

	return table, nil
}

// NodeToCollection converts a graph node to a document collection
func (st *StructureTransformer) NodeToCollection(node unifiedmodel.Node, enrichmentCtx *EnrichmentContext) (unifiedmodel.Collection, error) {
	collection := unifiedmodel.Collection{
		Name:   node.Label,
		Fields: make(map[string]unifiedmodel.Field),
	}

	// Convert properties to fields
	for propertyName, property := range node.Properties {
		field := st.propertyToField(propertyName, property)
		collection.Fields[propertyName] = field
	}

	return collection, nil
}

// CollectionToVectorIndex converts a collection to a vector index
func (st *StructureTransformer) CollectionToVectorIndex(collection unifiedmodel.Collection, enrichmentCtx *EnrichmentContext) (unifiedmodel.VectorIndex, error) {
	vectorIndex := unifiedmodel.VectorIndex{
		Name:       collection.Name + "_vector_index",
		On:         collection.Name,
		Fields:     make([]string, 0),
		Metric:     "cosine", // Default metric
		Dimension:  768,      // Default dimension for common embedding models
		Parameters: make(map[string]any),
	}

	// Identify text fields for vectorization
	for fieldName, field := range collection.Fields {
		if st.isTextualField(field) {
			vectorIndex.Fields = append(vectorIndex.Fields, fieldName)
		}
	}

	// If no text fields found, include all fields
	if len(vectorIndex.Fields) == 0 {
		for fieldName := range collection.Fields {
			vectorIndex.Fields = append(vectorIndex.Fields, fieldName)
		}
	}

	return vectorIndex, nil
}

// Helper methods for type checking and conversion

func (st *StructureTransformer) isNestedObject(field unifiedmodel.Field) bool {
	return field.Type == "object" || field.Type == "document" || strings.Contains(field.Type, "nested")
}

func (st *StructureTransformer) isArray(field unifiedmodel.Field) bool {
	return field.Type == "array" || strings.HasSuffix(field.Type, "[]")
}

func (st *StructureTransformer) isForeignKeyColumn(columnName string) bool {
	lowerName := strings.ToLower(columnName)
	// Don't treat plain "id" as foreign key - it's usually a primary key
	if lowerName == "id" {
		return false
	}
	return strings.HasSuffix(lowerName, "_id") ||
		strings.Contains(lowerName, "_ref")
}

func (st *StructureTransformer) isTextualField(field unifiedmodel.Field) bool {
	lowerType := strings.ToLower(field.Type)
	return lowerType == "string" ||
		lowerType == "text" ||
		lowerType == "varchar" ||
		strings.Contains(lowerType, "char")
}

// Conversion methods between different object types

func (st *StructureTransformer) fieldToColumn(fieldName string, field unifiedmodel.Field) unifiedmodel.Column {
	return unifiedmodel.Column{
		Name:     fieldName,
		DataType: field.Type, // Preserve original type for proper conversion
		Nullable: !field.Required,
		Options:  field.Options,
	}
}

func (st *StructureTransformer) columnToField(columnName string, column unifiedmodel.Column) unifiedmodel.Field {
	return unifiedmodel.Field{
		Name:     columnName,
		Type:     column.DataType, // Preserve original type for proper conversion
		Required: !column.Nullable,
		Options:  column.Options,
	}
}

func (st *StructureTransformer) columnToProperty(columnName string, column unifiedmodel.Column) unifiedmodel.Property {
	return unifiedmodel.Property{
		Name:    columnName,
		Type:    column.DataType, // Preserve original type for proper conversion
		Options: column.Options,
	}
}

func (st *StructureTransformer) propertyToColumn(propertyName string, property unifiedmodel.Property) unifiedmodel.Column {
	return unifiedmodel.Column{
		Name:     propertyName,
		DataType: st.mapPropertyTypeToColumnType(property.Type),
		Nullable: true, // Default for graph properties
		Options:  property.Options,
	}
}

func (st *StructureTransformer) propertyToField(propertyName string, property unifiedmodel.Property) unifiedmodel.Field {
	return unifiedmodel.Field{
		Name:    propertyName,
		Type:    st.mapPropertyTypeToFieldType(property.Type),
		Options: property.Options,
	}
}

// Type mapping methods

func (st *StructureTransformer) mapFieldTypeToColumnType(fieldType string) string {
	switch strings.ToLower(fieldType) {
	case "string", "text":
		return "varchar(255)"
	case "number", "integer":
		return "integer"
	case "float", "double":
		return "double precision"
	case "boolean":
		return "boolean"
	case "date", "datetime":
		return "timestamp"
	case "object", "document":
		return "jsonb"
	case "array":
		return "jsonb"
	default:
		return "text"
	}
}

func (st *StructureTransformer) mapPropertyTypeToColumnType(propertyType string) string {
	return st.mapFieldTypeToColumnType(propertyType) // Same mapping for now
}

func (st *StructureTransformer) mapPropertyTypeToFieldType(propertyType string) string {
	return propertyType // Direct mapping for now
}

// Helper methods for creating related tables

func (st *StructureTransformer) createNestedTable(parentName, fieldName string, field unifiedmodel.Field) (unifiedmodel.Table, error) {
	table := unifiedmodel.Table{
		Name:        fmt.Sprintf("%s_%s", parentName, fieldName),
		Columns:     make(map[string]unifiedmodel.Column),
		Constraints: make(map[string]unifiedmodel.Constraint),
	}

	// Add primary key
	table.Columns["id"] = unifiedmodel.Column{
		Name:          "id",
		DataType:      "int32", // Use MongoDB-compatible type
		Nullable:      false,
		IsPrimaryKey:  true,
		AutoIncrement: true,
	}

	// Add foreign key to parent
	table.Columns[parentName+"_id"] = unifiedmodel.Column{
		Name:     parentName + "_id",
		DataType: "int32", // Use MongoDB-compatible type
		Nullable: false,
	}

	// Add constraint for foreign key
	table.Constraints[fmt.Sprintf("fk_%s_%s", parentName, fieldName)] = unifiedmodel.Constraint{
		Name:    fmt.Sprintf("fk_%s_%s", parentName, fieldName),
		Type:    unifiedmodel.ConstraintTypeForeignKey,
		Columns: []string{parentName + "_id"},
		Reference: unifiedmodel.Reference{
			Table:   parentName,
			Columns: []string{"id"},
		},
	}

	// For now, add a generic data column - in practice, this would parse the nested structure
	table.Columns["data"] = unifiedmodel.Column{
		Name:     "data",
		DataType: "jsonb",
		Nullable: true,
	}

	return table, nil
}

func (st *StructureTransformer) createArrayTable(parentName, fieldName string, field unifiedmodel.Field) (unifiedmodel.Table, error) {
	table := unifiedmodel.Table{
		Name:        fmt.Sprintf("%s_%s", parentName, fieldName),
		Columns:     make(map[string]unifiedmodel.Column),
		Constraints: make(map[string]unifiedmodel.Constraint),
	}

	// Add foreign key to parent
	table.Columns[parentName+"_id"] = unifiedmodel.Column{
		Name:     parentName + "_id",
		DataType: "int32", // Use MongoDB-compatible type
		Nullable: false,
	}

	// Add value column
	table.Columns["value"] = unifiedmodel.Column{
		Name:     "value",
		DataType: st.mapFieldTypeToColumnType(field.Type),
		Nullable: true,
	}

	// Add order column for array ordering
	table.Columns["array_index"] = unifiedmodel.Column{
		Name:     "array_index",
		DataType: "int32", // Use MongoDB-compatible type
		Nullable: false,
	}

	// Add constraint for foreign key
	table.Constraints[fmt.Sprintf("fk_%s_%s", parentName, fieldName)] = unifiedmodel.Constraint{
		Name:    fmt.Sprintf("fk_%s_%s", parentName, fieldName),
		Type:    unifiedmodel.ConstraintTypeForeignKey,
		Columns: []string{parentName + "_id"},
		Reference: unifiedmodel.Reference{
			Table:   parentName,
			Columns: []string{"id"},
		},
	}

	return table, nil
}

func (st *StructureTransformer) embedRelatedData(collection *unifiedmodel.Collection, columnName string, column unifiedmodel.Column, enrichmentCtx *EnrichmentContext, schema *unifiedmodel.UnifiedModel) error {
	// This is a simplified implementation
	// In practice, this would look up the related table and embed its data

	// For now, preserve the original column type but mark it as a foreign key reference
	collection.Fields[columnName] = unifiedmodel.Field{
		Name:     columnName,
		Type:     column.DataType, // Preserve original type for proper conversion
		Required: !column.Nullable,
		Options: map[string]any{
			"reference_type": "foreign_key",
			"is_foreign_key": true,
		},
	}

	return nil
}
