package resource

import (
	"fmt"
	"strings"
)

// ValidateAddress performs comprehensive validation on a resource address
func ValidateAddress(addr *ResourceAddress) error {
	if addr == nil {
		return fmt.Errorf("resource address is nil")
	}

	// Basic validation
	if err := addr.Validate(); err != nil {
		return err
	}

	// Protocol-specific validation
	switch addr.Protocol {
	case ProtocolDatabase:
		return validateDatabaseAddress(addr)
	case ProtocolStream:
		return validateStreamAddress(addr)
	case ProtocolWebhook:
		return validateWebhookAddress(addr)
	case ProtocolMCP:
		return validateMCPAddress(addr)
	default:
		return fmt.Errorf("unknown protocol: %s", addr.Protocol)
	}
}

// validateDatabaseAddress validates a database resource address
func validateDatabaseAddress(addr *ResourceAddress) error {
	// Check scope validity
	validScopes := map[ResourceScope]bool{
		ScopeData:     true,
		ScopeMetadata: true,
		ScopeSchema:   true,
	}
	if !validScopes[addr.Scope] {
		return fmt.Errorf("invalid scope for database: %s", addr.Scope)
	}

	// Check object type validity
	validObjectTypes := map[ObjectType]bool{
		ObjectTypeTable:            true,
		ObjectTypeCollection:       true,
		ObjectTypeView:             true,
		ObjectTypeMaterializedView: true,
		ObjectTypeNode:             true,
		ObjectTypeRelationship:     true,
		ObjectTypeExternalTable:    true,
		ObjectTypeForeignTable:     true,
	}
	if !validObjectTypes[addr.ObjectType] {
		return fmt.Errorf("invalid object type for database: %s", addr.ObjectType)
	}

	// Validate path segments
	return validatePathSegments(addr.PathSegments, addr.ObjectType)
}

// validateStreamAddress validates a stream resource address
func validateStreamAddress(addr *ResourceAddress) error {
	// Check stream provider
	if addr.StreamProvider == "" {
		return fmt.Errorf("stream provider is required")
	}

	validProviders := map[StreamProvider]bool{
		StreamKafka:    true,
		StreamMQTT:     true,
		StreamKinesis:  true,
		StreamRabbitMQ: true,
		StreamPulsar:   true,
		StreamRedis:    true,
		StreamNATS:     true,
		StreamEventHub: true,
		StreamCloudRun: true,
	}
	if !validProviders[addr.StreamProvider] {
		return fmt.Errorf("invalid stream provider: %s", addr.StreamProvider)
	}

	// Check object type validity for streams
	validObjectTypes := map[ObjectType]bool{
		ObjectTypeTopic:     true,
		ObjectTypeQueue:     true,
		ObjectTypeStream:    true,
		ObjectTypePartition: true,
	}
	if !validObjectTypes[addr.ObjectType] {
		return fmt.Errorf("invalid object type for stream: %s", addr.ObjectType)
	}

	return validatePathSegments(addr.PathSegments, addr.ObjectType)
}

// validateWebhookAddress validates a webhook resource address
func validateWebhookAddress(addr *ResourceAddress) error {
	// Check direction (stored in ObjectName)
	if addr.ObjectName != "request" && addr.ObjectName != "response" {
		return fmt.Errorf("webhook direction must be 'request' or 'response', got: %s", addr.ObjectName)
	}

	return validatePathSegments(addr.PathSegments, addr.ObjectType)
}

// validateMCPAddress validates an MCP resource address
func validateMCPAddress(addr *ResourceAddress) error {
	// Check object type validity for MCP
	validObjectTypes := map[ObjectType]bool{
		ObjectTypeResource: true,
		ObjectTypeTool:     true,
		ObjectTypePrompt:   true,
	}
	if !validObjectTypes[addr.ObjectType] {
		return fmt.Errorf("invalid object type for MCP: %s", addr.ObjectType)
	}

	return validatePathSegments(addr.PathSegments, addr.ObjectType)
}

// validatePathSegments validates path segments for an object type
func validatePathSegments(segments []PathSegment, objectType ObjectType) error {
	for i, seg := range segments {
		// Check if segment type is valid for this position
		if seg.Type == "" {
			return fmt.Errorf("segment %d has empty type", i)
		}

		// Validate segment type based on object type
		if err := validateSegmentType(seg.Type, objectType); err != nil {
			return fmt.Errorf("invalid segment %d: %w", i, err)
		}

		// If segment has an index, it should be non-negative
		if seg.Index != nil && *seg.Index < 0 {
			return fmt.Errorf("segment %d has negative index: %d", i, *seg.Index)
		}
	}

	return nil
}

// validateSegmentType checks if a segment type is valid for an object type
func validateSegmentType(segType SegmentType, objType ObjectType) error {
	// Define valid segment types for each object type
	validSegments := getValidSegmentTypes(objType)

	for _, valid := range validSegments {
		if segType == valid {
			return nil
		}
	}

	return fmt.Errorf("segment type '%s' not valid for object type '%s'", segType, objType)
}

// getValidSegmentTypes returns valid segment types for an object type
func getValidSegmentTypes(objType ObjectType) []SegmentType {
	switch objType {
	case ObjectTypeTable, ObjectTypeView, ObjectTypeMaterializedView,
		ObjectTypeExternalTable, ObjectTypeForeignTable:
		return []SegmentType{SegmentTypeColumn, SegmentTypeField, SegmentTypeElement, SegmentTypeKey}

	case ObjectTypeCollection:
		return []SegmentType{SegmentTypeField, SegmentTypeElement, SegmentTypeKey}

	case ObjectTypeNode, ObjectTypeRelationship:
		return []SegmentType{SegmentTypeProperty, SegmentTypeField, SegmentTypeElement, SegmentTypeKey}

	case ObjectTypeTopic, ObjectTypeQueue, ObjectTypeStream:
		return []SegmentType{SegmentTypeField, SegmentTypePartition, SegmentTypeKey, SegmentTypeElement}

	case ObjectTypeEndpoint, ObjectTypeRequest, ObjectTypeResponse:
		return []SegmentType{SegmentTypeBody, SegmentTypeHeader, SegmentTypeQuery, SegmentTypePath, SegmentTypeField, SegmentTypeElement}

	case ObjectTypeResource, ObjectTypeTool, ObjectTypePrompt:
		return []SegmentType{SegmentTypeField, SegmentTypeParameter, SegmentTypeElement, SegmentTypeKey}

	default:
		// Allow any segment type for unknown object types
		return []SegmentType{
			SegmentTypeColumn, SegmentTypeField, SegmentTypeProperty, SegmentTypeElement,
			SegmentTypeKey, SegmentTypePartition, SegmentTypeHeader, SegmentTypeQuery,
			SegmentTypeParameter, SegmentTypeBody, SegmentTypePath, SegmentTypeAttributes,
		}
	}
}

// CompatibilityReport describes the compatibility between two addresses
type CompatibilityReport struct {
	// Compatible indicates if the addresses are compatible
	Compatible bool

	// Reason explains why addresses are compatible or incompatible
	Reason string

	// Warnings lists potential issues even if compatible
	Warnings []string

	// SuggestedTransformations lists recommended transformations
	SuggestedTransformations []string

	// TypeMismatch indicates if there's a data type mismatch
	TypeMismatch bool

	// RequiresTransformation indicates if transformation is required
	RequiresTransformation bool
}

// CheckCompatibility performs a detailed compatibility check between two addresses
func CheckCompatibility(source, target *ResourceAddress) (*CompatibilityReport, error) {
	report := &CompatibilityReport{
		Compatible:               false,
		Warnings:                 []string{},
		SuggestedTransformations: []string{},
	}

	// Validate both addresses first
	if err := ValidateAddress(source); err != nil {
		return report, fmt.Errorf("invalid source address: %w", err)
	}
	if err := ValidateAddress(target); err != nil {
		return report, fmt.Errorf("invalid target address: %w", err)
	}

	// Check scope compatibility
	if !areScopesCompatible(source.Scope, target.Scope) {
		report.Reason = fmt.Sprintf("incompatible scopes: %s -> %s", source.Scope, target.Scope)
		return report, nil
	}

	// Check protocol compatibility
	if err := checkProtocolCompatibility(source, target, report); err != nil {
		report.Reason = err.Error()
		return report, nil
	}

	// Check if transformation is required
	if requiresTransformation(source, target) {
		report.RequiresTransformation = true
		report.SuggestedTransformations = suggestTransformations(source, target)
	}

	// Check for warnings
	report.Warnings = checkWarnings(source, target)

	// If we got here, addresses are compatible
	report.Compatible = true
	report.Reason = "addresses are compatible"

	return report, nil
}

// areScopesCompatible checks if two scopes are compatible
func areScopesCompatible(source, target ResourceScope) bool {
	// Data to data is always compatible
	if source == ScopeData && target == ScopeData {
		return true
	}

	// Metadata to data is compatible (e.g., column names to values)
	if source == ScopeMetadata && target == ScopeData {
		return true
	}

	// Schema to schema is compatible
	if source == ScopeSchema && target == ScopeSchema {
		return true
	}

	// Schema to metadata might be compatible in some cases
	if source == ScopeSchema && target == ScopeMetadata {
		return true
	}

	return false
}

// checkProtocolCompatibility validates protocol-level compatibility
func checkProtocolCompatibility(source, target *ResourceAddress, report *CompatibilityReport) error {
	// Any protocol can map to database
	if target.Protocol == ProtocolDatabase {
		return nil
	}

	// Database can map to any protocol
	if source.Protocol == ProtocolDatabase {
		if target.Protocol == ProtocolStream {
			report.Warnings = append(report.Warnings, "mapping from database to stream may require continuous synchronization")
		}
		return nil
	}

	// Stream to stream with different providers
	if source.Protocol == ProtocolStream && target.Protocol == ProtocolStream {
		if source.StreamProvider != target.StreamProvider {
			report.Warnings = append(report.Warnings, fmt.Sprintf("cross-platform streaming: %s -> %s", source.StreamProvider, target.StreamProvider))
		}
		return nil
	}

	// Webhook to database or stream is common
	if source.Protocol == ProtocolWebhook {
		return nil
	}

	// MCP to any other protocol
	if source.Protocol == ProtocolMCP || target.Protocol == ProtocolMCP {
		return nil
	}

	return nil
}

// requiresTransformation determines if transformation is required
func requiresTransformation(source, target *ResourceAddress) bool {
	// Metadata to data always requires transformation
	if source.Scope == ScopeMetadata && target.Scope == ScopeData {
		return true
	}

	// Cross-protocol usually requires transformation
	if source.Protocol != target.Protocol {
		return true
	}

	// Different nesting levels may require transformation
	if len(source.PathSegments) != len(target.PathSegments) {
		return true
	}

	return false
}

// suggestTransformations suggests appropriate transformations
func suggestTransformations(source, target *ResourceAddress) []string {
	suggestions := []string{}

	// Metadata to data transformations
	if source.Scope == ScopeMetadata && target.Scope == ScopeData {
		if strings.Contains(source.String(), "names") {
			suggestions = append(suggestions, "array_join: convert array of names to delimited string")
			suggestions = append(suggestions, "json_array: convert to JSON array")
		}
		if strings.Contains(source.String(), "types") {
			suggestions = append(suggestions, "type_mapping: map data types between systems")
		}
	}

	// Stream to database transformations
	if source.Protocol == ProtocolStream && target.Protocol == ProtocolDatabase {
		suggestions = append(suggestions, "batch_aggregation: aggregate streaming data before insert")
		suggestions = append(suggestions, "deduplication: remove duplicate messages")
	}

	// Cross-format transformations
	if source.SchemaFormat != "" && target.SchemaFormat != "" && source.SchemaFormat != target.SchemaFormat {
		suggestions = append(suggestions, fmt.Sprintf("schema_conversion: %s -> %s", source.SchemaFormat, target.SchemaFormat))
	}

	return suggestions
}

// checkWarnings identifies potential issues
func checkWarnings(source, target *ResourceAddress) []string {
	warnings := []string{}

	// Warn about streaming to non-streaming
	if source.IsStream() && !target.IsStream() {
		warnings = append(warnings, "source is streaming but target is not - may need buffering")
	}

	// Warn about stateless sources
	if source.Protocol == ProtocolWebhook {
		warnings = append(warnings, "webhook sources are stateless - ensure proper event capture")
	}

	// Warn about nested structure flattening
	if len(source.PathSegments) > len(target.PathSegments) {
		warnings = append(warnings, "source has deeper nesting than target - data may be flattened")
	}

	// Warn about nested structure expansion
	if len(source.PathSegments) < len(target.PathSegments) {
		warnings = append(warnings, "target has deeper nesting than source - may need default values")
	}

	return warnings
}

// MustValidateAddress is like ValidateAddress but panics on error
func MustValidateAddress(addr *ResourceAddress) {
	if err := ValidateAddress(addr); err != nil {
		panic(err)
	}
}
