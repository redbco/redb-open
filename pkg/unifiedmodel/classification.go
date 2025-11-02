package unifiedmodel

import "github.com/redbco/redb-open/pkg/unifiedmodel/resource"

// ObjectCapability describes the capabilities of a UnifiedModel object type
type ObjectCapability struct {
	// CanStoreData indicates if this object can store actual data values
	CanStoreData bool

	// CanHaveMetadata indicates if this object has metadata properties
	CanHaveMetadata bool

	// SupportsNesting indicates if this object can contain nested structures
	SupportsNesting bool

	// SupportedSelectors lists the selector types this object supports
	SupportedSelectors []resource.SelectorType

	// IsStreaming indicates if this is a streaming data source
	IsStreaming bool

	// IsStateless indicates if this object doesn't persist state
	IsStateless bool

	// RequiresSchema indicates if schema registration is required
	RequiresSchema bool
}

// DataTypeCapability describes the capabilities of a data type
type DataTypeCapability struct {
	// IsStructured indicates if the type has internal structure (JSON, Avro, etc.)
	IsStructured bool

	// IsArray indicates if the type represents an array/list
	IsArray bool

	// CanNavigate indicates if the type supports path navigation
	CanNavigate bool

	// ElementType specifies the type of array elements or nested structures
	ElementType string

	// SchemaFormat specifies the schema format for structured data
	SchemaFormat resource.SchemaFormat
}

// GetObjectCapability returns the capabilities for a given object type
func GetObjectCapability(objType resource.ObjectType) ObjectCapability {
	switch objType {
	// Primary data containers - can store data
	case resource.ObjectTypeTable:
		return ObjectCapability{
			CanStoreData:       true,
			CanHaveMetadata:    true,
			SupportsNesting:    true,
			SupportedSelectors: []resource.SelectorType{resource.SelectorWildcard},
		}

	case resource.ObjectTypeCollection:
		return ObjectCapability{
			CanStoreData:       true,
			CanHaveMetadata:    true,
			SupportsNesting:    true,
			SupportedSelectors: []resource.SelectorType{resource.SelectorJSONPath, resource.SelectorWildcard, resource.SelectorKey},
		}

	case resource.ObjectTypeView, resource.ObjectTypeMaterializedView:
		return ObjectCapability{
			CanStoreData:       true,
			CanHaveMetadata:    true,
			SupportsNesting:    true,
			SupportedSelectors: []resource.SelectorType{resource.SelectorWildcard},
		}

	case resource.ObjectTypeNode:
		return ObjectCapability{
			CanStoreData:       true,
			CanHaveMetadata:    true,
			SupportsNesting:    true,
			SupportedSelectors: []resource.SelectorType{resource.SelectorWildcard, resource.SelectorKey},
		}

	case resource.ObjectTypeRelationship:
		return ObjectCapability{
			CanStoreData:       true,
			CanHaveMetadata:    true,
			SupportsNesting:    true,
			SupportedSelectors: []resource.SelectorType{resource.SelectorWildcard, resource.SelectorKey},
		}

	case resource.ObjectTypeExternalTable, resource.ObjectTypeForeignTable:
		return ObjectCapability{
			CanStoreData:       true,
			CanHaveMetadata:    true,
			SupportsNesting:    true,
			SupportedSelectors: []resource.SelectorType{resource.SelectorWildcard},
		}

	// Stream object types
	case resource.ObjectTypeTopic, resource.ObjectTypeQueue, resource.ObjectTypeStream:
		return ObjectCapability{
			CanStoreData:       true,
			CanHaveMetadata:    true,
			SupportsNesting:    true,
			SupportedSelectors: []resource.SelectorType{resource.SelectorJSONPath, resource.SelectorXPath, resource.SelectorWildcard},
			IsStreaming:        true,
			RequiresSchema:     true,
		}

	case resource.ObjectTypePartition:
		return ObjectCapability{
			CanStoreData:       true,
			CanHaveMetadata:    true,
			SupportsNesting:    false,
			SupportedSelectors: []resource.SelectorType{},
			IsStreaming:        true,
		}

	// Webhook object types
	case resource.ObjectTypeEndpoint:
		return ObjectCapability{
			CanStoreData:       true,
			CanHaveMetadata:    true,
			SupportsNesting:    true,
			SupportedSelectors: []resource.SelectorType{resource.SelectorJSONPath, resource.SelectorXPath, resource.SelectorWildcard},
			IsStateless:        true,
		}

	case resource.ObjectTypeRequest, resource.ObjectTypeResponse:
		return ObjectCapability{
			CanStoreData:       true,
			CanHaveMetadata:    true,
			SupportsNesting:    true,
			SupportedSelectors: []resource.SelectorType{resource.SelectorJSONPath, resource.SelectorXPath, resource.SelectorWildcard},
			IsStateless:        true,
		}

	// MCP object types
	case resource.ObjectTypeResource, resource.ObjectTypeTool, resource.ObjectTypePrompt:
		return ObjectCapability{
			CanStoreData:       true,
			CanHaveMetadata:    true,
			SupportsNesting:    true,
			SupportedSelectors: []resource.SelectorType{resource.SelectorJSONPath, resource.SelectorWildcard, resource.SelectorKey},
		}

	default:
		// Unknown object type - return conservative capabilities
		return ObjectCapability{
			CanStoreData:       false,
			CanHaveMetadata:    true,
			SupportsNesting:    false,
			SupportedSelectors: []resource.SelectorType{},
		}
	}
}

// GetDataTypeCapability returns capabilities for a data type string
func GetDataTypeCapability(dataType string) DataTypeCapability {
	// Normalize the data type string to lowercase for comparison
	dt := dataType

	switch {
	// JSON types
	case containsSubstring(dt, "json") || containsSubstring(dt, "jsonb"):
		return DataTypeCapability{
			IsStructured: true,
			IsArray:      false,
			CanNavigate:  true,
			SchemaFormat: resource.SchemaJSON,
		}

	// XML types
	case containsSubstring(dt, "xml"):
		return DataTypeCapability{
			IsStructured: true,
			IsArray:      false,
			CanNavigate:  true,
			SchemaFormat: resource.SchemaXML,
		}

	// Array types
	case containsSubstring(dt, "array") || containsSubstring(dt, "[]") || containsSubstring(dt, "list"):
		return DataTypeCapability{
			IsStructured: false,
			IsArray:      true,
			CanNavigate:  true,
		}

	// Avro types
	case containsSubstring(dt, "avro"):
		return DataTypeCapability{
			IsStructured: true,
			IsArray:      false,
			CanNavigate:  true,
			SchemaFormat: resource.SchemaAvro,
		}

	// Protobuf types
	case containsSubstring(dt, "protobuf") || containsSubstring(dt, "proto"):
		return DataTypeCapability{
			IsStructured: true,
			IsArray:      false,
			CanNavigate:  true,
			SchemaFormat: resource.SchemaProtobuf,
		}

	// Map/dictionary types
	case containsSubstring(dt, "map") || containsSubstring(dt, "dict") || containsSubstring(dt, "hstore"):
		return DataTypeCapability{
			IsStructured: true,
			IsArray:      false,
			CanNavigate:  true,
		}

	// Composite/struct types
	case containsSubstring(dt, "composite") || containsSubstring(dt, "struct") || containsSubstring(dt, "record"):
		return DataTypeCapability{
			IsStructured: true,
			IsArray:      false,
			CanNavigate:  true,
		}

	// Binary/blob types (not navigable)
	case containsSubstring(dt, "blob") || containsSubstring(dt, "binary") || containsSubstring(dt, "bytea"):
		return DataTypeCapability{
			IsStructured: false,
			IsArray:      false,
			CanNavigate:  false,
		}

	// Scalar types (not structured)
	default:
		return DataTypeCapability{
			IsStructured: false,
			IsArray:      false,
			CanNavigate:  false,
		}
	}
}

// CanStoreValues checks if an object type at a given path can store data values
func CanStoreValues(objType resource.ObjectType, path []resource.PathSegment) bool {
	cap := GetObjectCapability(objType)
	if !cap.CanStoreData {
		return false
	}

	// If path is empty, check if the object itself can store data
	if len(path) == 0 {
		return true
	}

	// Check if the last path segment represents a value storage location
	lastSeg := path[len(path)-1]
	switch lastSeg.Type {
	case resource.SegmentTypeColumn, resource.SegmentTypeField, resource.SegmentTypeProperty:
		return true
	case resource.SegmentTypeElement:
		// Array elements can store values
		return true
	case resource.SegmentTypeKey:
		// Map keys can reference values
		return true
	case resource.SegmentTypeBody:
		// Request/response bodies can store values
		return true
	case resource.SegmentTypeParameter:
		// Parameters can store values
		return true
	default:
		return false
	}
}

// IsMetadataProperty checks if a property name refers to metadata
func IsMetadataProperty(objType resource.ObjectType, propertyName string) bool {
	// Common metadata property names
	metadataProps := map[string]bool{
		"name":        true,
		"type":        true,
		"names":       true,
		"types":       true,
		"default":     true,
		"nullable":    true,
		"required":    true,
		"description": true,
		"comment":     true,
		"schema":      true,
		"format":      true,
		"version":     true,
		"created":     true,
		"updated":     true,
		"owner":       true,
		"permissions": true,
	}

	return metadataProps[propertyName]
}

// SupportsSelector checks if an object type supports a specific selector type
func SupportsSelector(objType resource.ObjectType, selectorType resource.SelectorType) bool {
	cap := GetObjectCapability(objType)
	for _, supported := range cap.SupportedSelectors {
		if supported == selectorType {
			return true
		}
	}
	return false
}

// containsSubstring is a helper function to check if a string contains a substring
func containsSubstring(s, substr string) bool {
	// Simple substring check
	return len(s) >= len(substr) && (s == substr ||
		(len(s) > len(substr) && (s[:len(substr)] == substr ||
			s[len(s)-len(substr):] == substr ||
			findSubstringInString(s, substr))))
}

func findSubstringInString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
