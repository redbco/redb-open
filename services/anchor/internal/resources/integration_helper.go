package resources

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redbco/redb-open/pkg/models"
)

// PopulateWebhookResources creates resource registry entries for a webhook
func (r *Repository) PopulateWebhookResources(ctx context.Context, webhookID, tenantID, workspaceID, ownerID, nodeID, integrationID string, webhookData map[string]interface{}) error {
	// Convert nodeID string to int64
	nodeIDInt64, err := strconv.ParseInt(nodeID, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse node ID '%s' as int64: %w", nodeID, err)
	}

	// Create container for the webhook
	webhookName, _ := webhookData["name"].(string)
	webhookURL, _ := webhookData["url"].(string)

	container := &models.ResourceContainer{
		TenantID:          tenantID,
		WorkspaceID:       workspaceID,
		ResourceURI:       fmt.Sprintf("webhook://%s/%s", workspaceID, webhookName),
		Protocol:          "webhook",
		Scope:             "data",
		ObjectType:        "webhook",
		ObjectName:        webhookName,
		IntegrationID:     &integrationID,
		ConnectedToNodeID: &nodeIDInt64,
		OwnerID:           ownerID,
		Status:            "STATUS_ACTIVE",
		StatusMessage:     "Webhook active",
		Online:            true,
		ContainerMetadata: map[string]interface{}{
			"webhook_id": webhookID,
			"url":        webhookURL,
		},
		EnrichedMetadata: map[string]interface{}{},
		Created:          time.Now(),
		Updated:          time.Now(),
	}

	if err := r.CreateContainer(ctx, container); err != nil {
		return fmt.Errorf("failed to create webhook container: %w", err)
	}

	// Create items for webhook parameters (headers, query params, etc.)
	if headers, ok := webhookData["headers"].(map[string]interface{}); ok {
		for headerName, headerValue := range headers {
			item := &models.ResourceItem{
				ContainerID:       container.ContainerID,
				TenantID:          tenantID,
				WorkspaceID:       workspaceID,
				ResourceURI:       fmt.Sprintf("webhook://%s/%s#header.%s", workspaceID, webhookName, headerName),
				Protocol:          "webhook",
				Scope:             "metadata",
				ItemType:          "header",
				ItemName:          headerName,
				ItemPath:          []string{"headers", headerName},
				DataType:          "string",
				UnifiedDataType:   ptrString("STRING"),
				IsNullable:        true,
				ConnectedToNodeID: &nodeIDInt64,
				Status:            "STATUS_ACTIVE",
				Online:            true,
				ItemMetadata: map[string]interface{}{
					"default_value": headerValue,
				},
				EnrichedMetadata: map[string]interface{}{},
				Created:          time.Now(),
				Updated:          time.Now(),
			}
			if err := r.CreateItem(ctx, item); err != nil {
				return fmt.Errorf("failed to create webhook header item: %w", err)
			}
		}
	}

	return nil
}

// PopulateMCPResources creates resource registry entries for an MCP resource
func (r *Repository) PopulateMCPResources(ctx context.Context, mcpResourceID, tenantID, workspaceID, ownerID, nodeID, mcpServerID string, resourceData map[string]interface{}) error {
	// Convert nodeID string to int64
	nodeIDInt64, err := strconv.ParseInt(nodeID, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse node ID '%s' as int64: %w", nodeID, err)
	}

	// Create container for the MCP resource
	resourceName, _ := resourceData["name"].(string)
	resourceType, _ := resourceData["type"].(string)

	container := &models.ResourceContainer{
		TenantID:          tenantID,
		WorkspaceID:       workspaceID,
		ResourceURI:       fmt.Sprintf("mcp://%s/%s/%s", workspaceID, mcpServerID, resourceName),
		Protocol:          "mcp",
		Scope:             "data",
		ObjectType:        resourceType,
		ObjectName:        resourceName,
		MCPServerID:       &mcpServerID,
		ConnectedToNodeID: &nodeIDInt64,
		OwnerID:           ownerID,
		Status:            "STATUS_ACTIVE",
		StatusMessage:     "MCP resource active",
		Online:            true,
		ContainerMetadata: map[string]interface{}{
			"mcp_resource_id": mcpResourceID,
			"resource_type":   resourceType,
		},
		EnrichedMetadata: map[string]interface{}{},
		Created:          time.Now(),
		Updated:          time.Now(),
	}

	if err := r.CreateContainer(ctx, container); err != nil {
		return fmt.Errorf("failed to create MCP resource container: %w", err)
	}

	// If resource has a schema, create items for each field
	if schema, ok := resourceData["schema"].(map[string]interface{}); ok {
		if properties, ok := schema["properties"].(map[string]interface{}); ok {
			ordinal := 0
			for fieldName, fieldDef := range properties {
				fieldDefMap, _ := fieldDef.(map[string]interface{})
				fieldType, _ := fieldDefMap["type"].(string)

				item := &models.ResourceItem{
					ContainerID:       container.ContainerID,
					TenantID:          tenantID,
					WorkspaceID:       workspaceID,
					ResourceURI:       fmt.Sprintf("mcp://%s/%s/%s#%s", workspaceID, mcpServerID, resourceName, fieldName),
					Protocol:          "mcp",
					Scope:             "schema",
					ItemType:          "field",
					ItemName:          fieldName,
					ItemPath:          []string{fieldName},
					DataType:          fieldType,
					UnifiedDataType:   ptrString(mapJSONTypeToUnified(fieldType)),
					IsNullable:        true,
					HasSchema:         true,
					SchemaFormat:      ptrString("json_schema"),
					SchemaDefinition:  fieldDefMap,
					ConnectedToNodeID: &nodeIDInt64,
					Status:            "STATUS_ACTIVE",
					Online:            true,
					ItemMetadata:      map[string]interface{}{},
					EnrichedMetadata:  map[string]interface{}{},
					OrdinalPosition:   &ordinal,
					Created:           time.Now(),
					Updated:           time.Now(),
				}
				ordinal++

				if err := r.CreateItem(ctx, item); err != nil {
					return fmt.Errorf("failed to create MCP resource item: %w", err)
				}
			}
		}
	}

	return nil
}

// PopulateStreamResources creates resource registry entries for a stream/topic
func (r *Repository) PopulateStreamResources(ctx context.Context, streamID, tenantID, workspaceID, ownerID, nodeID, integrationID string, streamData map[string]interface{}) error {
	// Convert nodeID string to int64
	nodeIDInt64, err := strconv.ParseInt(nodeID, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse node ID '%s' as int64: %w", nodeID, err)
	}

	// Create container for the stream
	streamName, _ := streamData["name"].(string)
	streamType, _ := streamData["type"].(string) // kafka, rabbitmq, etc.

	container := &models.ResourceContainer{
		TenantID:          tenantID,
		WorkspaceID:       workspaceID,
		ResourceURI:       fmt.Sprintf("stream://%s/%s/%s", workspaceID, streamType, streamName),
		Protocol:          "stream",
		Scope:             "data",
		ObjectType:        "topic",
		ObjectName:        streamName,
		IntegrationID:     &integrationID,
		ConnectedToNodeID: &nodeIDInt64,
		OwnerID:           ownerID,
		Status:            "STATUS_ACTIVE",
		StatusMessage:     "Stream active",
		Online:            true,
		ContainerMetadata: map[string]interface{}{
			"stream_id":   streamID,
			"stream_type": streamType,
		},
		EnrichedMetadata: map[string]interface{}{},
		Created:          time.Now(),
		Updated:          time.Now(),
	}

	if err := r.CreateContainer(ctx, container); err != nil {
		return fmt.Errorf("failed to create stream container: %w", err)
	}

	// If stream has a schema, create items for message structure
	if schema, ok := streamData["schema"].(map[string]interface{}); ok {
		if fields, ok := schema["fields"].([]interface{}); ok {
			for i, fieldDef := range fields {
				fieldDefMap, _ := fieldDef.(map[string]interface{})
				fieldName, _ := fieldDefMap["name"].(string)
				fieldType, _ := fieldDefMap["type"].(string)

				item := &models.ResourceItem{
					ContainerID:            container.ContainerID,
					TenantID:               tenantID,
					WorkspaceID:            workspaceID,
					ResourceURI:            fmt.Sprintf("stream://%s/%s/%s#%s", workspaceID, streamType, streamName, fieldName),
					Protocol:               "stream",
					Scope:                  "schema",
					ItemType:               "field",
					ItemName:               fieldName,
					ItemPath:               []string{fieldName},
					DataType:               fieldType,
					UnifiedDataType:        ptrString(mapJSONTypeToUnified(fieldType)),
					IsNullable:             true,
					HasSchema:              true,
					SchemaFormat:           ptrString("json_schema"),
					SchemaDefinition:       fieldDefMap,
					SchemaValidationMode:   "evolving",
					SchemaMismatchAction:   "accept_and_log",
					AllowNewFields:         true,
					AllowFieldTypeWidening: true,
					ConnectedToNodeID: &nodeIDInt64,
					Status:                 "STATUS_ACTIVE",
					Online:                 true,
					ItemMetadata:           map[string]interface{}{},
					EnrichedMetadata:       map[string]interface{}{},
					OrdinalPosition:        ptrInt(i),
					Created:                time.Now(),
					Updated:                time.Now(),
				}

				if err := r.CreateItem(ctx, item); err != nil {
					return fmt.Errorf("failed to create stream item: %w", err)
				}
			}
		}
	}

	return nil
}

// Helper functions

func ptrString(s string) *string {
	return &s
}

func ptrInt(i int) *int {
	return &i
}

// mapJSONTypeToUnified maps JSON schema types to unified data types
func mapJSONTypeToUnified(jsonType string) string {
	switch jsonType {
	case "string":
		return "STRING"
	case "number":
		return "DOUBLE"
	case "integer":
		return "INTEGER"
	case "boolean":
		return "BOOLEAN"
	case "array":
		return "ARRAY"
	case "object":
		return "OBJECT"
	case "null":
		return "NULL"
	default:
		return "STRING"
	}
}
