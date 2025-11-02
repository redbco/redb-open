package unifiedmodel

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/unifiedmodel/resource"
)

// ResourceLocation represents a resolved location within a resource
type ResourceLocation struct {
	// Address is the original resource address
	Address *resource.ResourceAddress

	// ParentObject is the containing object (Table, Collection, Topic, etc.)
	ParentObject interface{}

	// TargetObject is the specific target (Column, Field, Property, etc.)
	TargetObject interface{}

	// DataType is the data type of the target
	DataType string

	// IsNested indicates if this location is within a nested structure
	IsNested bool

	// NestedPath is the path within nested structures (e.g., JSON path)
	NestedPath []string

	// IsStreaming indicates if this is a streaming resource
	IsStreaming bool

	// SchemaInfo contains schema information for structured data
	SchemaInfo *ResourceSchemaInfo
}

// ResourceSchemaInfo represents schema information for structured data
type ResourceSchemaInfo struct {
	// Format specifies the schema format (Avro, Protobuf, JSON, etc.)
	Format resource.SchemaFormat

	// Version is the schema version
	Version string

	// Fields lists the schema fields
	Fields []SchemaField

	// Registry is the schema registry URL (for Kafka, etc.)
	Registry string

	// Definition contains the full schema definition
	Definition interface{}
}

// SchemaField represents a single field in a schema
type SchemaField struct {
	// Name is the field name
	Name string

	// Type is the field data type
	Type string

	// Required indicates if the field is required
	Required bool

	// DefaultVal is the default value
	DefaultVal interface{}

	// Description is the field description
	Description string

	// Nested contains nested fields for complex types
	Nested []SchemaField
}

// Navigate resolves a resource address to a specific location
func Navigate(ctx context.Context, addr *resource.ResourceAddress) (*ResourceLocation, error) {
	if addr == nil {
		return nil, fmt.Errorf("resource address is nil")
	}

	if err := addr.Validate(); err != nil {
		return nil, fmt.Errorf("invalid resource address: %w", err)
	}

	// Route to appropriate navigator based on protocol
	switch addr.Protocol {
	case resource.ProtocolDatabase:
		return nil, fmt.Errorf("database navigation requires UnifiedModel - use NavigateDatabase")
	case resource.ProtocolStream:
		return NavigateStream(ctx, addr)
	case resource.ProtocolWebhook:
		return NavigateWebhook(ctx, addr)
	case resource.ProtocolMCP:
		return NavigateMCP(ctx, addr)
	default:
		return nil, fmt.Errorf("unsupported protocol: %s", addr.Protocol)
	}
}

// NavigateDatabase navigates a database resource using UnifiedModel
func NavigateDatabase(um *UnifiedModel, addr *resource.ResourceAddress) (*ResourceLocation, error) {
	if um == nil {
		return nil, fmt.Errorf("unified model is nil")
	}

	if addr.Protocol != resource.ProtocolDatabase {
		return nil, fmt.Errorf("not a database protocol address")
	}

	location := &ResourceLocation{
		Address: addr,
	}

	// Navigate based on object type
	switch addr.ObjectType {
	case resource.ObjectTypeTable:
		return navigateTable(um, addr, location)
	case resource.ObjectTypeCollection:
		return navigateCollection(um, addr, location)
	case resource.ObjectTypeView:
		return navigateView(um, addr, location)
	case resource.ObjectTypeMaterializedView:
		return navigateMaterializedView(um, addr, location)
	case resource.ObjectTypeNode:
		return navigateNode(um, addr, location)
	case resource.ObjectTypeRelationship:
		return navigateRelationship(um, addr, location)
	default:
		return nil, fmt.Errorf("unsupported database object type: %s", addr.ObjectType)
	}
}

// navigateTable navigates to a table location
func navigateTable(um *UnifiedModel, addr *resource.ResourceAddress, location *ResourceLocation) (*ResourceLocation, error) {
	// Find the table
	table, exists := um.Tables[addr.ObjectName]
	if !exists {
		return nil, fmt.Errorf("table not found: %s", addr.ObjectName)
	}

	location.ParentObject = table

	// If no path segments, return the table itself
	if len(addr.PathSegments) == 0 {
		return location, nil
	}

	// Navigate through path segments
	return navigateTablePath(table, addr, location)
}

// navigateTablePath navigates through table path segments
func navigateTablePath(table Table, addr *resource.ResourceAddress, location *ResourceLocation) (*ResourceLocation, error) {
	// First segment should typically be a column
	if addr.PathSegments[0].Type != resource.SegmentTypeColumn {
		return nil, fmt.Errorf("expected column segment, got: %s", addr.PathSegments[0].Type)
	}

	columnName := addr.PathSegments[0].Name
	column, exists := table.Columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column not found: %s", columnName)
	}

	location.TargetObject = column
	location.DataType = column.DataType

	// If there are more segments, navigate into nested structures
	if len(addr.PathSegments) > 1 {
		return navigateNestedStructure(column.DataType, addr.PathSegments[1:], location)
	}

	return location, nil
}

// navigateCollection navigates to a collection location
func navigateCollection(um *UnifiedModel, addr *resource.ResourceAddress, location *ResourceLocation) (*ResourceLocation, error) {
	// Find the collection
	collection, exists := um.Collections[addr.ObjectName]
	if !exists {
		return nil, fmt.Errorf("collection not found: %s", addr.ObjectName)
	}

	location.ParentObject = collection

	// If no path segments, return the collection itself
	if len(addr.PathSegments) == 0 {
		return location, nil
	}

	// Navigate through fields
	if addr.PathSegments[0].Type != resource.SegmentTypeField {
		return nil, fmt.Errorf("expected field segment, got: %s", addr.PathSegments[0].Type)
	}

	fieldName := addr.PathSegments[0].Name
	field, exists := collection.Fields[fieldName]
	if !exists {
		return nil, fmt.Errorf("field not found: %s", fieldName)
	}

	location.TargetObject = field
	location.DataType = field.Type

	// Navigate nested fields if present
	if len(addr.PathSegments) > 1 {
		return navigateNestedStructure(field.Type, addr.PathSegments[1:], location)
	}

	return location, nil
}

// navigateView navigates to a view location
func navigateView(um *UnifiedModel, addr *resource.ResourceAddress, location *ResourceLocation) (*ResourceLocation, error) {
	view, exists := um.Views[addr.ObjectName]
	if !exists {
		return nil, fmt.Errorf("view not found: %s", addr.ObjectName)
	}

	location.ParentObject = view

	if len(addr.PathSegments) == 0 {
		return location, nil
	}

	// Navigate to column
	if addr.PathSegments[0].Type != resource.SegmentTypeColumn {
		return nil, fmt.Errorf("expected column segment, got: %s", addr.PathSegments[0].Type)
	}

	columnName := addr.PathSegments[0].Name
	column, exists := view.Columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column not found in view: %s", columnName)
	}

	location.TargetObject = column
	location.DataType = column.DataType

	if len(addr.PathSegments) > 1 {
		return navigateNestedStructure(column.DataType, addr.PathSegments[1:], location)
	}

	return location, nil
}

// navigateMaterializedView navigates to a materialized view location
func navigateMaterializedView(um *UnifiedModel, addr *resource.ResourceAddress, location *ResourceLocation) (*ResourceLocation, error) {
	matView, exists := um.MaterializedViews[addr.ObjectName]
	if !exists {
		return nil, fmt.Errorf("materialized view not found: %s", addr.ObjectName)
	}

	location.ParentObject = matView

	if len(addr.PathSegments) == 0 {
		return location, nil
	}

	// Navigate to column
	if addr.PathSegments[0].Type != resource.SegmentTypeColumn {
		return nil, fmt.Errorf("expected column segment, got: %s", addr.PathSegments[0].Type)
	}

	columnName := addr.PathSegments[0].Name
	column, exists := matView.Columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column not found in materialized view: %s", columnName)
	}

	location.TargetObject = column
	location.DataType = column.DataType

	if len(addr.PathSegments) > 1 {
		return navigateNestedStructure(column.DataType, addr.PathSegments[1:], location)
	}

	return location, nil
}

// navigateNode navigates to a graph node location
func navigateNode(um *UnifiedModel, addr *resource.ResourceAddress, location *ResourceLocation) (*ResourceLocation, error) {
	node, exists := um.Nodes[addr.ObjectName]
	if !exists {
		return nil, fmt.Errorf("node not found: %s", addr.ObjectName)
	}

	location.ParentObject = node

	if len(addr.PathSegments) == 0 {
		return location, nil
	}

	// Navigate to property
	if addr.PathSegments[0].Type != resource.SegmentTypeProperty {
		return nil, fmt.Errorf("expected property segment, got: %s", addr.PathSegments[0].Type)
	}

	propertyName := addr.PathSegments[0].Name
	property, exists := node.Properties[propertyName]
	if !exists {
		return nil, fmt.Errorf("property not found in node: %s", propertyName)
	}

	location.TargetObject = property
	location.DataType = property.Type

	if len(addr.PathSegments) > 1 {
		return navigateNestedStructure(property.Type, addr.PathSegments[1:], location)
	}

	return location, nil
}

// navigateRelationship navigates to a relationship location
func navigateRelationship(um *UnifiedModel, addr *resource.ResourceAddress, location *ResourceLocation) (*ResourceLocation, error) {
	relationship, exists := um.Relationships[addr.ObjectName]
	if !exists {
		return nil, fmt.Errorf("relationship not found: %s", addr.ObjectName)
	}

	location.ParentObject = relationship

	if len(addr.PathSegments) == 0 {
		return location, nil
	}

	// Navigate to property
	if addr.PathSegments[0].Type != resource.SegmentTypeProperty {
		return nil, fmt.Errorf("expected property segment, got: %s", addr.PathSegments[0].Type)
	}

	propertyName := addr.PathSegments[0].Name
	property, exists := relationship.Properties[propertyName]
	if !exists {
		return nil, fmt.Errorf("property not found in relationship: %s", propertyName)
	}

	location.TargetObject = property
	location.DataType = property.Type

	if len(addr.PathSegments) > 1 {
		return navigateNestedStructure(property.Type, addr.PathSegments[1:], location)
	}

	return location, nil
}

// navigateNestedStructure navigates through nested structures (JSON, etc.)
func navigateNestedStructure(dataType string, segments []resource.PathSegment, location *ResourceLocation) (*ResourceLocation, error) {
	// Check if data type supports nesting
	dtCap := GetDataTypeCapability(dataType)
	if !dtCap.CanNavigate {
		return nil, fmt.Errorf("data type %s does not support navigation", dataType)
	}

	location.IsNested = true
	location.NestedPath = make([]string, len(segments))

	// Build nested path from segments
	for i, seg := range segments {
		location.NestedPath[i] = seg.Name
	}

	return location, nil
}

// NavigateStream navigates a stream resource (implementation stub)
func NavigateStream(ctx context.Context, addr *resource.ResourceAddress) (*ResourceLocation, error) {
	// This would integrate with actual streaming platforms
	// For now, return a basic location
	location := &ResourceLocation{
		Address:     addr,
		IsStreaming: true,
	}

	// In a full implementation, this would:
	// 1. Connect to the stream provider (Kafka, MQTT, etc.)
	// 2. Fetch schema from schema registry if available
	// 3. Validate the path segments against the schema
	// 4. Return location with schema info

	return location, nil
}

// NavigateWebhook navigates a webhook resource (implementation stub)
func NavigateWebhook(ctx context.Context, addr *resource.ResourceAddress) (*ResourceLocation, error) {
	// This would integrate with webhook endpoint configurations
	location := &ResourceLocation{
		Address: addr,
	}

	// In a full implementation, this would:
	// 1. Look up webhook endpoint configuration
	// 2. Validate path segments (body, headers, query params)
	// 3. Return location with endpoint info

	return location, nil
}

// NavigateMCP navigates an MCP resource (implementation stub)
func NavigateMCP(ctx context.Context, addr *resource.ResourceAddress) (*ResourceLocation, error) {
	// This would integrate with MCP server connections
	location := &ResourceLocation{
		Address: addr,
	}

	// In a full implementation, this would:
	// 1. Connect to MCP server
	// 2. List resources/tools/prompts
	// 3. Validate the address against available resources
	// 4. Return location with resource info

	return location, nil
}

// ListAddressableLocations returns all addressable locations within an object
func ListAddressableLocations(um *UnifiedModel, protocol resource.ResourceProtocol, objectPath string) ([]*resource.ResourceAddress, error) {
	if protocol != resource.ProtocolDatabase {
		return nil, fmt.Errorf("listing only supported for database protocol currently")
	}

	// Parse object path to determine what to list
	// For now, return an error indicating this is a stub
	return nil, fmt.Errorf("ListAddressableLocations not yet implemented")
}

// AreCompatible checks if two resource addresses are compatible for mapping
func AreCompatible(source, target *resource.ResourceAddress) (bool, error) {
	// Basic compatibility checks
	if source == nil || target == nil {
		return false, fmt.Errorf("source or target address is nil")
	}

	// Data scope can map to data scope
	if source.Scope == resource.ScopeData && target.Scope == resource.ScopeData {
		return true, nil
	}

	// Metadata scope can map to data scope (e.g., column names to JSON array)
	if source.Scope == resource.ScopeMetadata && target.Scope == resource.ScopeData {
		return true, nil
	}

	// Schema scope should generally map to schema scope
	if source.Scope == resource.ScopeSchema && target.Scope == resource.ScopeSchema {
		return true, nil
	}

	// Other combinations may not be compatible
	return false, fmt.Errorf("incompatible scopes: %s -> %s", source.Scope, target.Scope)
}
