package resource

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// ParseResourceURI parses a resource URI string into a ResourceAddress
//
// Supported formats:
//
// Database resources:
//
//	redb://data/database/{id}/table/{name}/column/{col}
//	redb://metadata/database/{id}/table/{name}/columns/names
//	redb://data/database/{id}/collection/{name}/field/{field}
//	redb://data/database/{id}/node/{label}/property/{prop}
//
// Stream resources:
//
//	stream://kafka/{conn-id}/topic/{topic}/schema/avro/field/{field}
//	stream://mqtt/{conn-id}/topic/{topic}/field/{field}
//	stream://kinesis/{conn-id}/stream/{stream}/field/{field}
//
// Webhook resources:
//
//	webhook://{endpoint-id}/request/body/field/{field}
//	webhook://{endpoint-id}/response/body/field/{field}
//	webhook://{endpoint-id}/request/header/{header-name}
//
// MCP resources:
//
//	mcp://{server-id}/resource/{resource-name}/field/{field}
//	mcp://{server-id}/tool/{tool-name}/parameter/{param}
//
// Selectors (appended with #):
//
//	redb://data/database/{id}/table/users/column/profile#$.address.city
func ParseResourceURI(uri string) (*ResourceAddress, error) {
	if uri == "" {
		return nil, fmt.Errorf("empty URI")
	}

	// Split selector if present (indicated by #)
	var selectorExpr string
	if idx := strings.Index(uri, "#"); idx != -1 {
		selectorExpr = uri[idx+1:]
		uri = uri[:idx]
	}

	// Parse the URI
	parsedURL, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid URI format: %w", err)
	}

	protocol := ResourceProtocol(parsedURL.Scheme)
	if protocol == "" {
		return nil, fmt.Errorf("missing protocol scheme")
	}

	// Parse based on protocol
	var addr *ResourceAddress
	switch protocol {
	case ProtocolDatabase:
		addr, err = parseDatabaseURI(parsedURL)
	case ProtocolStream:
		addr, err = parseStreamURI(parsedURL)
	case ProtocolWebhook:
		addr, err = parseWebhookURI(parsedURL)
	case ProtocolMCP:
		addr, err = parseMCPURI(parsedURL)
	default:
		return nil, fmt.Errorf("unsupported protocol: %s", protocol)
	}

	if err != nil {
		return nil, err
	}

	// Parse selector if present
	if selectorExpr != "" {
		selector, err := parseSelector(selectorExpr)
		if err != nil {
			return nil, fmt.Errorf("invalid selector: %w", err)
		}
		addr.Selector = selector
	}

	return addr, nil
}

// parseDatabaseURI parses a database protocol URI
// Format: redb://[scope]/database/{id}/{object-type}/{name}/...
func parseDatabaseURI(parsedURL *url.URL) (*ResourceAddress, error) {
	// For URIs like redb://data/database/..., the URL parser treats:
	// - Host as "data" (the scope)
	// - Path as "/database/..."
	// So we need to extract scope from Host and prepend it to the path parts
	
	var scope string
	var pathStr string
	
	if parsedURL.Host != "" {
		// New format: redb://data/database/...
		scope = parsedURL.Host
		pathStr = parsedURL.Path
	} else {
		// Old format: redb:/data/database/... (for backward compatibility)
		// In this case, everything is in the path
		pathStr = parsedURL.Path
	}
	
	parts := strings.Split(strings.Trim(pathStr, "/"), "/")
	
	// If scope wasn't in host, it should be the first part of the path
	if scope == "" && len(parts) > 0 {
		scope = parts[0]
		parts = parts[1:]
	}
	
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid database URI format, expected: redb://[scope]/database/{id}/{object-type}/{name}")
	}

	addr := &ResourceAddress{
		Protocol:     ProtocolDatabase,
		Scope:        ResourceScope(scope),
		ResourceType: TypeDatabase,
	}

	// Validate scope
	if addr.Scope != ScopeData && addr.Scope != ScopeMetadata && addr.Scope != ScopeSchema {
		return nil, fmt.Errorf("invalid scope: %s, must be data, metadata, or schema", addr.Scope)
	}

	// parts[0] should be "database"
	if parts[0] != "database" {
		return nil, fmt.Errorf("expected 'database' in path, got: %s", parts[0])
	}

	addr.DatabaseID = parts[1]
	addr.ObjectType = ObjectType(parts[2])

	if len(parts) > 3 {
		addr.ObjectName = parts[3]
	}

	// Parse remaining path segments
	if len(parts) > 4 {
		segments, err := parsePathSegments(parts[4:])
		if err != nil {
			return nil, fmt.Errorf("error parsing path segments: %w", err)
		}
		addr.PathSegments = segments
	}

	return addr, nil
}

// parseStreamURI parses a stream protocol URI
// Format: stream://{provider}/{conn-id}/{object-type}/{name}/...
func parseStreamURI(parsedURL *url.URL) (*ResourceAddress, error) {
	parts := strings.Split(strings.Trim(parsedURL.Path, "/"), "/")
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid stream URI format, expected: stream://{provider}/{conn-id}/{object-type}/{name}")
	}

	// The host is the stream provider
	provider := StreamProvider(parsedURL.Host)
	if provider == "" {
		return nil, fmt.Errorf("stream provider is required")
	}

	addr := &ResourceAddress{
		Protocol:       ProtocolStream,
		Scope:          ScopeData, // Streams default to data scope
		ResourceType:   TypeStream,
		StreamProvider: provider,
		ConnectionID:   parts[0],
	}

	if len(parts) > 1 {
		addr.ObjectType = ObjectType(parts[1])
	}

	if len(parts) > 2 {
		addr.ObjectName = parts[2]
	}

	// Parse remaining path segments (e.g., schema, field, partition)
	if len(parts) > 3 {
		segments, err := parsePathSegments(parts[3:])
		if err != nil {
			return nil, fmt.Errorf("error parsing path segments: %w", err)
		}
		addr.PathSegments = segments
	}

	// Check for schema format in path
	for i, seg := range addr.PathSegments {
		if seg.Type == SegmentType("schema") && i+1 < len(addr.PathSegments) {
			addr.SchemaFormat = SchemaFormat(addr.PathSegments[i+1].Name)
		}
	}

	return addr, nil
}

// parseWebhookURI parses a webhook protocol URI
// Format: webhook://{endpoint-id}/{direction}/{component}/...
func parseWebhookURI(parsedURL *url.URL) (*ResourceAddress, error) {
	parts := strings.Split(strings.Trim(parsedURL.Path, "/"), "/")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid webhook URI format, expected: webhook://{endpoint-id}/{direction}/{component}")
	}

	addr := &ResourceAddress{
		Protocol:     ProtocolWebhook,
		Scope:        ScopeData, // Webhooks default to data scope
		ResourceType: TypeWebhook,
		ConnectionID: parsedURL.Host,
		ObjectType:   ObjectTypeEndpoint,
	}

	// First part is direction (request/response)
	direction := parts[0]
	if direction != "request" && direction != "response" {
		return nil, fmt.Errorf("webhook direction must be 'request' or 'response', got: %s", direction)
	}
	addr.ObjectName = direction

	// Parse remaining path segments
	if len(parts) > 1 {
		segments, err := parsePathSegments(parts[1:])
		if err != nil {
			return nil, fmt.Errorf("error parsing path segments: %w", err)
		}
		addr.PathSegments = segments
	}

	return addr, nil
}

// parseMCPURI parses an MCP protocol URI
// Format: mcp://{server-id}/{object-type}/{name}/...
func parseMCPURI(parsedURL *url.URL) (*ResourceAddress, error) {
	parts := strings.Split(strings.Trim(parsedURL.Path, "/"), "/")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid MCP URI format, expected: mcp://{server-id}/{object-type}/{name}")
	}

	addr := &ResourceAddress{
		Protocol:     ProtocolMCP,
		Scope:        ScopeData, // MCP defaults to data scope
		ResourceType: TypeMCP,
		ServerID:     parsedURL.Host,
	}

	addr.ObjectType = ObjectType(parts[0])
	if len(parts) > 1 {
		addr.ObjectName = parts[1]
	}

	// Parse remaining path segments
	if len(parts) > 2 {
		segments, err := parsePathSegments(parts[2:])
		if err != nil {
			return nil, fmt.Errorf("error parsing path segments: %w", err)
		}
		addr.PathSegments = segments
	}

	return addr, nil
}

// parsePathSegments parses a series of path components into PathSegments
func parsePathSegments(parts []string) ([]PathSegment, error) {
	segments := make([]PathSegment, 0, len(parts)/2)

	for i := 0; i < len(parts); i += 2 {
		segType := SegmentType(parts[i])
		if i+1 >= len(parts) {
			// Last segment without a value - treat as a flag/modifier
			segments = append(segments, PathSegment{
				Type: segType,
			})
			break
		}

		name := parts[i+1]

		// Check if name contains array index notation: field[0]
		var index *int
		if strings.Contains(name, "[") {
			name, index = parseArrayIndex(name)
		}

		segments = append(segments, PathSegment{
			Type:  segType,
			Name:  name,
			Index: index,
		})
	}

	return segments, nil
}

// parseArrayIndex extracts array index from name[index] format
func parseArrayIndex(name string) (string, *int) {
	start := strings.Index(name, "[")
	end := strings.Index(name, "]")
	if start == -1 || end == -1 || end <= start {
		return name, nil
	}

	indexStr := name[start+1 : end]
	if idx, err := strconv.Atoi(indexStr); err == nil {
		baseName := name[:start]
		return baseName, &idx
	}

	return name, nil
}

// parseSelector parses a selector expression
func parseSelector(expr string) (*Selector, error) {
	if expr == "" {
		return nil, fmt.Errorf("empty selector expression")
	}

	selector := &Selector{
		Expression: expr,
	}

	// Determine selector type based on prefix or pattern
	if strings.HasPrefix(expr, "$.") || strings.HasPrefix(expr, "$[") {
		selector.Type = SelectorJSONPath
	} else if strings.HasPrefix(expr, "/") || strings.HasPrefix(expr, "//") {
		selector.Type = SelectorXPath
	} else if expr == "*" {
		selector.Type = SelectorWildcard
	} else if _, err := strconv.Atoi(expr); err == nil {
		selector.Type = SelectorIndex
	} else {
		selector.Type = SelectorKey
	}

	return selector, nil
}
