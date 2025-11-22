package resource

import (
	"fmt"
	"net/url"
	"strings"
)

// ProtocolTemplate is the protocol for template-based virtual resources
const ProtocolTemplate ResourceProtocol = "template"

// TemplateAddress represents an unresolved template-based resource address
type TemplateAddress struct {
	// Namespace identifies the template namespace (e.g., "default", "staging", "prod")
	Namespace string

	// ObjectType is the type of object (table, collection, etc.)
	ObjectType ObjectType

	// ObjectName is the name of the object
	ObjectName string

	// PathSegments contains the hierarchical path to the specific resource
	PathSegments []PathSegment

	// ExpectedType is the expected data type (from query parameter)
	ExpectedType string

	// Metadata contains additional template metadata
	Metadata map[string]interface{}
}

// String returns the string representation of a TemplateAddress
func (ta *TemplateAddress) String() string {
	uri := fmt.Sprintf("template://%s/database/%s/%s",
		ta.Namespace,
		ta.ObjectType,
		ta.ObjectName,
	)

	// Add path segments
	for _, segment := range ta.PathSegments {
		uri += fmt.Sprintf("/%s/%s", segment.Type, segment.Name)
	}

	// Add query parameters if present
	if ta.ExpectedType != "" {
		uri += fmt.Sprintf("?type=%s", url.QueryEscape(ta.ExpectedType))
	}

	return uri
}

// ParseTemplateURI parses a template:// URI into a TemplateAddress
// Format: template://{namespace}/database/{object_type}/{object_name}/{segment_type}/{segment_name}?type={data_type}
// Examples:
//   - template://default/database/collection/users/field/email?type=string
//   - template://staging/database/table/orders/column/total?type=decimal
func ParseTemplateURI(uri string) (*TemplateAddress, error) {
	if !strings.HasPrefix(uri, "template://") {
		return nil, fmt.Errorf("URI must start with 'template://': %s", uri)
	}

	// Remove protocol
	withoutProtocol := strings.TrimPrefix(uri, "template://")

	// Split on '?' to separate path from query parameters
	parts := strings.SplitN(withoutProtocol, "?", 2)
	pathPart := parts[0]
	var queryPart string
	if len(parts) > 1 {
		queryPart = parts[1]
	}

	// Parse path segments
	pathSegments := strings.Split(pathPart, "/")
	if len(pathSegments) < 4 {
		return nil, fmt.Errorf("template URI must have format: template://{namespace}/database/{object_type}/{object_name}[/{segment_type}/{segment_name}]*")
	}

	ta := &TemplateAddress{
		Namespace:    pathSegments[0],
		PathSegments: []PathSegment{},
		Metadata:     make(map[string]interface{}),
	}

	// Validate and parse "database" segment
	if pathSegments[1] != "database" {
		return nil, fmt.Errorf("second segment must be 'database', got: %s", pathSegments[1])
	}

	// Parse object type
	ta.ObjectType = ObjectType(pathSegments[2])

	// Parse object name
	ta.ObjectName = pathSegments[3]

	// Parse remaining path segments (must come in pairs: type/name)
	remaining := pathSegments[4:]
	if len(remaining)%2 != 0 {
		return nil, fmt.Errorf("path segments must come in pairs (type/name)")
	}

	for i := 0; i < len(remaining); i += 2 {
		segmentType := SegmentType(remaining[i])
		segmentName := remaining[i+1]
		ta.PathSegments = append(ta.PathSegments, PathSegment{
			Type: segmentType,
			Name: segmentName,
		})
	}

	// Parse query parameters
	if queryPart != "" {
		query, err := url.ParseQuery(queryPart)
		if err != nil {
			return nil, fmt.Errorf("failed to parse query parameters: %w", err)
		}

		if typeParam := query.Get("type"); typeParam != "" {
			ta.ExpectedType = typeParam
		}

		// Store all other query params in metadata
		for key, values := range query {
			if key != "type" && len(values) > 0 {
				ta.Metadata[key] = values[0]
			}
		}
	}

	return ta, nil
}

// ValidateTemplateAddress validates a template address structure
func ValidateTemplateAddress(ta *TemplateAddress) error {
	if ta == nil {
		return fmt.Errorf("template address is nil")
	}

	if ta.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}

	if ta.ObjectType == "" {
		return fmt.Errorf("object type is required")
	}

	if ta.ObjectName == "" {
		return fmt.Errorf("object name is required")
	}

	// Validate object type is a known type
	validObjectTypes := map[ObjectType]bool{
		ObjectTypeTable:            true,
		ObjectTypeCollection:       true,
		ObjectTypeView:             true,
		ObjectTypeMaterializedView: true,
		ObjectTypeNode:             true,
		ObjectTypeRelationship:     true,
	}

	if !validObjectTypes[ta.ObjectType] {
		return fmt.Errorf("invalid object type for template: %s", ta.ObjectType)
	}

	return nil
}

// ToResourceURI converts a TemplateAddress to a concrete ResourceAddress URI
// This is used after a virtual resource has been bound to a real database
func (ta *TemplateAddress) ToResourceURI(databaseID string) string {
	uri := fmt.Sprintf("redb://data/database/%s/%s/%s",
		databaseID,
		ta.ObjectType,
		ta.ObjectName,
	)

	// Add path segments
	for _, segment := range ta.PathSegments {
		uri += fmt.Sprintf("/%s/%s", segment.Type, segment.Name)
	}

	return uri
}

// GetContainerURI returns the URI for the container (without item segments)
func (ta *TemplateAddress) GetContainerURI() string {
	return fmt.Sprintf("template://%s/database/%s/%s",
		ta.Namespace,
		ta.ObjectType,
		ta.ObjectName,
	)
}

// GetItemName returns the name of the item if path segments exist
func (ta *TemplateAddress) GetItemName() string {
	if len(ta.PathSegments) > 0 {
		return ta.PathSegments[len(ta.PathSegments)-1].Name
	}
	return ""
}

// GetItemType returns the type of the item if path segments exist
func (ta *TemplateAddress) GetItemType() SegmentType {
	if len(ta.PathSegments) > 0 {
		return ta.PathSegments[len(ta.PathSegments)-1].Type
	}
	return ""
}

// IsContainerAddress returns true if this addresses a container (no path segments)
func (ta *TemplateAddress) IsContainerAddress() bool {
	return len(ta.PathSegments) == 0
}

// IsItemAddress returns true if this addresses an item within a container
func (ta *TemplateAddress) IsItemAddress() bool {
	return len(ta.PathSegments) > 0
}

// TemplateURIBuilder helps construct template URIs programmatically
type TemplateURIBuilder struct {
	namespace    string
	objectType   ObjectType
	objectName   string
	segments     []PathSegment
	expectedType string
	metadata     map[string]string
}

// NewTemplateURIBuilder creates a new template URI builder
func NewTemplateURIBuilder(namespace string, objectType ObjectType, objectName string) *TemplateURIBuilder {
	if namespace == "" {
		namespace = "default"
	}
	return &TemplateURIBuilder{
		namespace:  namespace,
		objectType: objectType,
		objectName: objectName,
		segments:   []PathSegment{},
		metadata:   make(map[string]string),
	}
}

// WithField adds a field segment
func (b *TemplateURIBuilder) WithField(name string) *TemplateURIBuilder {
	b.segments = append(b.segments, PathSegment{
		Type: SegmentTypeField,
		Name: name,
	})
	return b
}

// WithColumn adds a column segment
func (b *TemplateURIBuilder) WithColumn(name string) *TemplateURIBuilder {
	b.segments = append(b.segments, PathSegment{
		Type: SegmentTypeColumn,
		Name: name,
	})
	return b
}

// WithProperty adds a property segment
func (b *TemplateURIBuilder) WithProperty(name string) *TemplateURIBuilder {
	b.segments = append(b.segments, PathSegment{
		Type: SegmentTypeProperty,
		Name: name,
	})
	return b
}

// WithSegment adds a custom segment
func (b *TemplateURIBuilder) WithSegment(segmentType SegmentType, name string) *TemplateURIBuilder {
	b.segments = append(b.segments, PathSegment{
		Type: segmentType,
		Name: name,
	})
	return b
}

// WithType sets the expected data type
func (b *TemplateURIBuilder) WithType(dataType string) *TemplateURIBuilder {
	b.expectedType = dataType
	return b
}

// WithMetadata adds a metadata key-value pair
func (b *TemplateURIBuilder) WithMetadata(key, value string) *TemplateURIBuilder {
	b.metadata[key] = value
	return b
}

// Build constructs the final template URI string
func (b *TemplateURIBuilder) Build() string {
	uri := fmt.Sprintf("template://%s/database/%s/%s",
		b.namespace,
		b.objectType,
		b.objectName,
	)

	// Add segments
	for _, segment := range b.segments {
		uri += fmt.Sprintf("/%s/%s", segment.Type, segment.Name)
	}

	// Add query parameters
	queryParams := []string{}
	if b.expectedType != "" {
		queryParams = append(queryParams, fmt.Sprintf("type=%s", url.QueryEscape(b.expectedType)))
	}
	for key, value := range b.metadata {
		queryParams = append(queryParams, fmt.Sprintf("%s=%s", url.QueryEscape(key), url.QueryEscape(value)))
	}

	if len(queryParams) > 0 {
		uri += "?" + strings.Join(queryParams, "&")
	}

	return uri
}

// BuildAddress constructs a TemplateAddress struct
func (b *TemplateURIBuilder) BuildAddress() *TemplateAddress {
	metadata := make(map[string]interface{})
	for k, v := range b.metadata {
		metadata[k] = v
	}

	return &TemplateAddress{
		Namespace:    b.namespace,
		ObjectType:   b.objectType,
		ObjectName:   b.objectName,
		PathSegments: b.segments,
		ExpectedType: b.expectedType,
		Metadata:     metadata,
	}
}
