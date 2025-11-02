package resource

import (
	"fmt"
	"strings"
)

// BuildResourceURI constructs a URI string from a ResourceAddress
func BuildResourceURI(addr *ResourceAddress) (string, error) {
	if addr == nil {
		return "", fmt.Errorf("resource address is nil")
	}

	if err := addr.Validate(); err != nil {
		return "", fmt.Errorf("invalid resource address: %w", err)
	}

	var uri string
	switch addr.Protocol {
	case ProtocolDatabase:
		uri = buildDatabaseURI(addr)
	case ProtocolStream:
		uri = buildStreamURI(addr)
	case ProtocolWebhook:
		uri = buildWebhookURI(addr)
	case ProtocolMCP:
		uri = buildMCPURI(addr)
	default:
		return "", fmt.Errorf("unsupported protocol: %s", addr.Protocol)
	}

	// Append selector if present
	if addr.Selector != nil {
		uri += "#" + addr.Selector.Expression
	}

	return uri, nil
}

// buildDatabaseURI constructs a database protocol URI
// Format: redb://[scope]/database/{id}/{object-type}/{name}/...
func buildDatabaseURI(addr *ResourceAddress) string {
	parts := []string{
		string(addr.Protocol) + ":/",
		string(addr.Scope),
		"database",
		addr.DatabaseID,
		string(addr.ObjectType),
	}

	if addr.ObjectName != "" {
		parts = append(parts, addr.ObjectName)
	}

	// Add path segments
	for _, seg := range addr.PathSegments {
		parts = append(parts, string(seg.Type))
		if seg.Name != "" {
			name := seg.Name
			if seg.Index != nil {
				name = fmt.Sprintf("%s[%d]", name, *seg.Index)
			}
			parts = append(parts, name)
		}
	}

	return strings.Join(parts, "/")
}

// buildStreamURI constructs a stream protocol URI
// Format: stream://{provider}/{conn-id}/{object-type}/{name}/...
func buildStreamURI(addr *ResourceAddress) string {
	parts := []string{
		string(addr.Protocol) + ":/" + string(addr.StreamProvider),
		addr.ConnectionID,
		string(addr.ObjectType),
	}

	if addr.ObjectName != "" {
		parts = append(parts, addr.ObjectName)
	}

	// Add path segments
	for _, seg := range addr.PathSegments {
		parts = append(parts, string(seg.Type))
		if seg.Name != "" {
			name := seg.Name
			if seg.Index != nil {
				name = fmt.Sprintf("%s[%d]", name, *seg.Index)
			}
			parts = append(parts, name)
		}
	}

	return strings.Join(parts, "/")
}

// buildWebhookURI constructs a webhook protocol URI
// Format: webhook://{endpoint-id}/{direction}/{component}/...
func buildWebhookURI(addr *ResourceAddress) string {
	parts := []string{
		string(addr.Protocol) + ":/" + addr.ConnectionID,
		addr.ObjectName, // direction: request or response
	}

	// Add path segments
	for _, seg := range addr.PathSegments {
		parts = append(parts, string(seg.Type))
		if seg.Name != "" {
			name := seg.Name
			if seg.Index != nil {
				name = fmt.Sprintf("%s[%d]", name, *seg.Index)
			}
			parts = append(parts, name)
		}
	}

	return strings.Join(parts, "/")
}

// buildMCPURI constructs an MCP protocol URI
// Format: mcp://{server-id}/{object-type}/{name}/...
func buildMCPURI(addr *ResourceAddress) string {
	parts := []string{
		string(addr.Protocol) + ":/" + addr.ServerID,
		string(addr.ObjectType),
	}

	if addr.ObjectName != "" {
		parts = append(parts, addr.ObjectName)
	}

	// Add path segments
	for _, seg := range addr.PathSegments {
		parts = append(parts, string(seg.Type))
		if seg.Name != "" {
			name := seg.Name
			if seg.Index != nil {
				name = fmt.Sprintf("%s[%d]", name, *seg.Index)
			}
			parts = append(parts, name)
		}
	}

	return strings.Join(parts, "/")
}

// MustBuildResourceURI is like BuildResourceURI but panics on error
func MustBuildResourceURI(addr *ResourceAddress) string {
	uri, err := BuildResourceURI(addr)
	if err != nil {
		panic(err)
	}
	return uri
}

// NewDatabaseAddress creates a resource address for a database object
func NewDatabaseAddress(scope ResourceScope, databaseID string, objectType ObjectType, objectName string) *ResourceAddress {
	return &ResourceAddress{
		Protocol:     ProtocolDatabase,
		Scope:        scope,
		ResourceType: TypeDatabase,
		DatabaseID:   databaseID,
		ObjectType:   objectType,
		ObjectName:   objectName,
		PathSegments: []PathSegment{},
	}
}

// NewStreamAddress creates a resource address for a stream object
func NewStreamAddress(provider StreamProvider, connectionID string, objectType ObjectType, objectName string) *ResourceAddress {
	return &ResourceAddress{
		Protocol:       ProtocolStream,
		Scope:          ScopeData,
		ResourceType:   TypeStream,
		StreamProvider: provider,
		ConnectionID:   connectionID,
		ObjectType:     objectType,
		ObjectName:     objectName,
		PathSegments:   []PathSegment{},
	}
}

// NewWebhookAddress creates a resource address for a webhook endpoint
func NewWebhookAddress(endpointID, direction string) *ResourceAddress {
	return &ResourceAddress{
		Protocol:     ProtocolWebhook,
		Scope:        ScopeData,
		ResourceType: TypeWebhook,
		ConnectionID: endpointID,
		ObjectType:   ObjectTypeEndpoint,
		ObjectName:   direction, // "request" or "response"
		PathSegments: []PathSegment{},
	}
}

// NewMCPAddress creates a resource address for an MCP resource
func NewMCPAddress(serverID string, objectType ObjectType, objectName string) *ResourceAddress {
	return &ResourceAddress{
		Protocol:     ProtocolMCP,
		Scope:        ScopeData,
		ResourceType: TypeMCP,
		ServerID:     serverID,
		ObjectType:   objectType,
		ObjectName:   objectName,
		PathSegments: []PathSegment{},
	}
}

// AddPathSegment adds a path segment to the resource address
func (r *ResourceAddress) AddPathSegment(segType SegmentType, name string) *ResourceAddress {
	r.PathSegments = append(r.PathSegments, PathSegment{
		Type: segType,
		Name: name,
	})
	return r
}

// AddPathSegmentWithIndex adds a path segment with an array index
func (r *ResourceAddress) AddPathSegmentWithIndex(segType SegmentType, name string, index int) *ResourceAddress {
	r.PathSegments = append(r.PathSegments, PathSegment{
		Type:  segType,
		Name:  name,
		Index: &index,
	})
	return r
}

// WithSelector adds a selector to the resource address
func (r *ResourceAddress) WithSelector(selectorType SelectorType, expression string) *ResourceAddress {
	r.Selector = &Selector{
		Type:       selectorType,
		Expression: expression,
	}
	return r
}

// WithJSONPathSelector adds a JSONPath selector
func (r *ResourceAddress) WithJSONPathSelector(jsonPath string) *ResourceAddress {
	return r.WithSelector(SelectorJSONPath, jsonPath)
}

// WithMetadata adds or updates metadata
func (r *ResourceAddress) WithMetadata(key string, value interface{}) *ResourceAddress {
	if r.Metadata == nil {
		r.Metadata = make(map[string]interface{})
	}
	r.Metadata[key] = value
	return r
}
