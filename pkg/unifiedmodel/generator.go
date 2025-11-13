package unifiedmodel

import (
	"fmt"
	"strconv"
	"strings"

	pb "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/pkg/models"
	"github.com/redbco/redb-open/pkg/unifiedmodel/resource"
)

// GenerateContainerURI generates a URI for a container resource
func GenerateContainerURI(protocol, scope, dbID, objType, objName string) string {
	return fmt.Sprintf("%s://%s/database/%s/%s/%s", protocol, scope, dbID, objType, objName)
}

// GenerateItemURI generates a URI for an item resource
func GenerateItemURI(containerURI, itemType, itemName string, path []string) string {
	uri := fmt.Sprintf("%s/%s/%s", containerURI, itemType, itemName)

	// Add path segments for nested items
	for i := 0; i < len(path); i += 2 {
		if i+1 < len(path) {
			uri += fmt.Sprintf("/%s/%s", path[i], path[i+1])
		}
	}

	return uri
}

// generateItemDisplayName generates a human-readable display name for a resource item
// Format: {databaseName}.{containerName}.{itemName}
func generateItemDisplayName(databaseName, containerName, itemName string) string {
	return fmt.Sprintf("%s.%s.%s", databaseName, containerName, itemName)
}

// PopulateResourcesFromUnifiedModel generates resource containers and items from a UnifiedModel
// enrichedAnalysis provides optional table-level classification data (PrimaryCategory, ClassificationConfidence)
func PopulateResourcesFromUnifiedModel(
	um *UnifiedModel,
	dbID, nodeID, tenantID, workspaceID, ownerID, databaseName string,
	enrichedAnalysis *pb.AnalyzeSchemaEnrichedResponse,
) ([]*models.ResourceContainer, []*models.ResourceItem, error) {
	containers := []*models.ResourceContainer{}
	items := []*models.ResourceItem{}

	protocol := string(resource.ProtocolDatabase)
	scope := string(resource.ScopeData)
	databaseType := string(um.DatabaseType)

	// Convert nodeID string to int64
	nodeIDInt64, err := strconv.ParseInt(nodeID, 10, 64)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse node ID '%s' as int64: %w", nodeID, err)
	}

	// Create a map from table/container names to their enriched metadata for quick lookup
	enrichmentMap := make(map[string]*pb.EnrichedTableMetadata)
	// Also create a nested map for column-level lookups: table name -> column name -> column metadata
	columnEnrichmentMap := make(map[string]map[string]*pb.EnrichedColumnMetadata)
	if enrichedAnalysis != nil {
		for _, table := range enrichedAnalysis.Tables {
			enrichmentMap[table.Name] = table
			// Build column map for this table
			colMap := make(map[string]*pb.EnrichedColumnMetadata)
			for _, col := range table.Columns {
				colMap[col.Name] = col
			}
			columnEnrichmentMap[table.Name] = colMap
		}
	}

	// Process Tables
	for tableName, table := range um.Tables {
		// Create container for table
		containerURI := GenerateContainerURI(protocol, scope, dbID, "table", tableName)
		container := &models.ResourceContainer{
			TenantID:                      tenantID,
			WorkspaceID:                   workspaceID,
			ResourceURI:                   containerURI,
			Protocol:                      protocol,
			Scope:                         scope,
			ObjectType:                    "table",
			ObjectName:                    tableName,
			DatabaseID:                    &dbID,
			ConnectedToNodeID:             nodeIDInt64,
			OwnerID:                       ownerID,
			Status:                        "STATUS_CREATED",
			Online:                        true,
			ContainerMetadata:             map[string]interface{}{},
			EnrichedMetadata:              map[string]interface{}{},
			DatabaseType:                  &databaseType,
			ItemCount:                     len(table.Columns),
			ContainerClassificationSource: "auto", // Default value
		}

		// Populate classification fields if enrichment data is available
		if enrichment, ok := enrichmentMap[tableName]; ok && enrichment.PrimaryCategory != "" {
			container.ContainerClassification = &enrichment.PrimaryCategory
			container.ContainerClassificationConfidence = &enrichment.ClassificationConfidence
		}

		containers = append(containers, container)

		// Process columns
		for colName, column := range table.Columns {
			itemURI := GenerateItemURI(containerURI, "column", colName, nil)
			item := &models.ResourceItem{
				TenantID:               tenantID,
				WorkspaceID:            workspaceID,
				ResourceURI:            itemURI,
				Protocol:               protocol,
				Scope:                  scope,
				ItemType:               "column",
				ItemName:               colName,
				ItemDisplayName:        generateItemDisplayName(databaseName, tableName, colName),
				ItemPath:               []string{},
				DataType:               column.DataType,
				IsNullable:             column.Nullable,
				IsPrimaryKey:           column.IsPrimaryKey,
				IsUnique:               false, // Column struct doesn't have IsUnique field
				IsIndexed:              false, // Could be determined from indexes
				IsRequired:             !column.Nullable,
				IsArray:                false,
				ArrayDimensions:        1,
				DefaultValue:           getStringPtr(column.Default),
				Constraints:            []map[string]interface{}{},
				IsCustomType:           false,
				HasSchema:              false,
				SchemaEvolutionVersion: 1,
				SchemaValidationMode:   "strict",
				SchemaMismatchAction:   "reject",
				AllowNewFields:         false,
				AllowFieldTypeWidening: false,
				AllowFieldRemoval:      false,
				SchemaEvolutionLog:     []map[string]interface{}{},
				NestedItems:            []map[string]interface{}{},
				ConnectedToNodeID:      nodeIDInt64,
				Status:                 "STATUS_CREATED",
				Online:                 true,
				ItemMetadata:           map[string]interface{}{},
				EnrichedMetadata:       map[string]interface{}{},
				IsPrivileged:           false,
				OrdinalPosition:        column.OrdinalPosition,
			}

			// Apply enriched column data if available
			if colEnrichments, ok := columnEnrichmentMap[tableName]; ok {
				if enrichedCol, colOk := colEnrichments[colName]; colOk {
					item.IsPrivileged = enrichedCol.IsPrivilegedData
					if enrichedCol.DataCategory != "" {
						item.PrivilegedClassification = &enrichedCol.DataCategory
					}
					if enrichedCol.PrivilegedConfidence > 0 {
						item.DetectionConfidence = &enrichedCol.PrivilegedConfidence
					}
					if item.IsPrivileged {
						detectionMethod := "auto"
						item.DetectionMethod = &detectionMethod
					}
				}
			}

			// Handle array types
			if strings.Contains(column.DataType, "[]") || strings.Contains(column.DataType, "ARRAY") {
				item.IsArray = true
			}

			items = append(items, item)
		}
	}

	// Process Collections (MongoDB, etc.)
	for collectionName, collection := range um.Collections {
		containerURI := GenerateContainerURI(protocol, scope, dbID, "collection", collectionName)
		container := &models.ResourceContainer{
			TenantID:                      tenantID,
			WorkspaceID:                   workspaceID,
			ResourceURI:                   containerURI,
			Protocol:                      protocol,
			Scope:                         scope,
			ObjectType:                    "collection",
			ObjectName:                    collectionName,
			DatabaseID:                    &dbID,
			ConnectedToNodeID:             nodeIDInt64,
			OwnerID:                       ownerID,
			Status:                        "STATUS_CREATED",
			Online:                        true,
			ContainerMetadata:             map[string]interface{}{},
			EnrichedMetadata:              map[string]interface{}{},
			DatabaseType:                  &databaseType,
			ItemCount:                     len(collection.Fields),
			ContainerClassificationSource: "auto", // Default value
		}

		// Populate classification fields if enrichment data is available
		if enrichment, ok := enrichmentMap[collectionName]; ok && enrichment.PrimaryCategory != "" {
			container.ContainerClassification = &enrichment.PrimaryCategory
			container.ContainerClassificationConfidence = &enrichment.ClassificationConfidence
		}

		containers = append(containers, container)

		// Process fields
		for fieldName, field := range collection.Fields {
			itemURI := GenerateItemURI(containerURI, "field", fieldName, nil)
			item := &models.ResourceItem{
				TenantID:               tenantID,
				WorkspaceID:            workspaceID,
				ResourceURI:            itemURI,
				Protocol:               protocol,
				Scope:                  scope,
				ItemType:               "field",
				ItemName:               fieldName,
				ItemDisplayName:        generateItemDisplayName(databaseName, collectionName, fieldName),
				ItemPath:               []string{},
				DataType:               field.Type,
				IsNullable:             !field.Required,
				IsPrimaryKey:           false,
				IsUnique:               false,
				IsIndexed:              false,
				IsRequired:             field.Required,
				IsArray:                false,
				ArrayDimensions:        1,
				Constraints:            []map[string]interface{}{},
				IsCustomType:           false,
				HasSchema:              false,
				SchemaEvolutionVersion: 1,
				SchemaValidationMode:   "evolving", // MongoDB typically allows schema evolution
				SchemaMismatchAction:   "evolve_schema",
				AllowNewFields:         true,
				AllowFieldTypeWidening: true,
				AllowFieldRemoval:      false,
				SchemaEvolutionLog:     []map[string]interface{}{},
				NestedItems:            []map[string]interface{}{},
				ConnectedToNodeID:      nodeIDInt64,
				Status:                 "STATUS_CREATED",
				Online:                 true,
				ItemMetadata:           map[string]interface{}{},
				EnrichedMetadata:       map[string]interface{}{},
				IsPrivileged:           false,
			}

			// Apply enriched field data if available
			if colEnrichments, ok := columnEnrichmentMap[collectionName]; ok {
				if enrichedCol, colOk := colEnrichments[fieldName]; colOk {
					item.IsPrivileged = enrichedCol.IsPrivilegedData
					if enrichedCol.DataCategory != "" {
						item.PrivilegedClassification = &enrichedCol.DataCategory
					}
					if enrichedCol.PrivilegedConfidence > 0 {
						item.DetectionConfidence = &enrichedCol.PrivilegedConfidence
					}
					if item.IsPrivileged {
						detectionMethod := "auto"
						item.DetectionMethod = &detectionMethod
					}
				}
			}

			items = append(items, item)
		}
	}

	// Process Views
	for viewName, view := range um.Views {
		containerURI := GenerateContainerURI(protocol, scope, dbID, "view", viewName)
		container := &models.ResourceContainer{
			TenantID:          tenantID,
			WorkspaceID:       workspaceID,
			ResourceURI:       containerURI,
			Protocol:          protocol,
			Scope:             scope,
			ObjectType:        "view",
			ObjectName:        viewName,
			DatabaseID:        &dbID,
			ConnectedToNodeID: nodeIDInt64,
			OwnerID:           ownerID,
			Status:            "STATUS_CREATED",
			Online:            true,
			ContainerMetadata: map[string]interface{}{
				"definition": view.Definition,
			},
			EnrichedMetadata: map[string]interface{}{},
			DatabaseType:     &databaseType,
			ItemCount:        len(view.Columns),
		}
		containers = append(containers, container)

		// Process view columns
		for colName, column := range view.Columns {
			itemURI := GenerateItemURI(containerURI, "column", colName, nil)
			item := &models.ResourceItem{
				TenantID:               tenantID,
				WorkspaceID:            workspaceID,
				ResourceURI:            itemURI,
				Protocol:               protocol,
				Scope:                  scope,
				ItemType:               "column",
				ItemName:               colName,
				ItemDisplayName:        generateItemDisplayName(databaseName, viewName, colName),
				ItemPath:               []string{},
				DataType:               column.DataType,
				IsNullable:             column.Nullable,
				IsPrimaryKey:           false,
				IsUnique:               false,
				IsIndexed:              false,
				IsRequired:             !column.Nullable,
				IsArray:                false,
				ArrayDimensions:        1,
				Constraints:            []map[string]interface{}{},
				IsCustomType:           false,
				HasSchema:              false,
				SchemaEvolutionVersion: 1,
				SchemaValidationMode:   "strict",
				SchemaMismatchAction:   "reject",
				AllowNewFields:         false,
				AllowFieldTypeWidening: false,
				AllowFieldRemoval:      false,
				SchemaEvolutionLog:     []map[string]interface{}{},
				NestedItems:            []map[string]interface{}{},
				ConnectedToNodeID:      nodeIDInt64,
				Status:                 "STATUS_CREATED",
				Online:                 true,
				ItemMetadata:           map[string]interface{}{},
				EnrichedMetadata:       map[string]interface{}{},
				IsPrivileged:           false,
				OrdinalPosition:        column.OrdinalPosition,
			}

			// Apply enriched column data if available
			if colEnrichments, ok := columnEnrichmentMap[viewName]; ok {
				if enrichedCol, colOk := colEnrichments[colName]; colOk {
					item.IsPrivileged = enrichedCol.IsPrivilegedData
					if enrichedCol.DataCategory != "" {
						item.PrivilegedClassification = &enrichedCol.DataCategory
					}
					if enrichedCol.PrivilegedConfidence > 0 {
						item.DetectionConfidence = &enrichedCol.PrivilegedConfidence
					}
					if item.IsPrivileged {
						detectionMethod := "auto"
						item.DetectionMethod = &detectionMethod
					}
				}
			}

			items = append(items, item)
		}
	}

	// Process Materialized Views
	for mvName, mv := range um.MaterializedViews {
		containerURI := GenerateContainerURI(protocol, scope, dbID, "materialized_view", mvName)
		container := &models.ResourceContainer{
			TenantID:          tenantID,
			WorkspaceID:       workspaceID,
			ResourceURI:       containerURI,
			Protocol:          protocol,
			Scope:             scope,
			ObjectType:        "materialized_view",
			ObjectName:        mvName,
			DatabaseID:        &dbID,
			ConnectedToNodeID: nodeIDInt64,
			OwnerID:           ownerID,
			Status:            "STATUS_CREATED",
			Online:            true,
			ContainerMetadata: map[string]interface{}{
				"definition": mv.Definition,
			},
			EnrichedMetadata: map[string]interface{}{},
			DatabaseType:     &databaseType,
			ItemCount:        len(mv.Columns),
		}
		containers = append(containers, container)

		// Process materialized view columns
		for colName, column := range mv.Columns {
			itemURI := GenerateItemURI(containerURI, "column", colName, nil)
			item := &models.ResourceItem{
				TenantID:               tenantID,
				WorkspaceID:            workspaceID,
				ResourceURI:            itemURI,
				Protocol:               protocol,
				Scope:                  scope,
				ItemType:               "column",
				ItemName:               colName,
				ItemDisplayName:        generateItemDisplayName(databaseName, mvName, colName),
				ItemPath:               []string{},
				DataType:               column.DataType,
				IsNullable:             column.Nullable,
				IsPrimaryKey:           false,
				IsUnique:               false,
				IsIndexed:              false,
				IsRequired:             !column.Nullable,
				IsArray:                false,
				ArrayDimensions:        1,
				Constraints:            []map[string]interface{}{},
				IsCustomType:           false,
				HasSchema:              false,
				SchemaEvolutionVersion: 1,
				SchemaValidationMode:   "strict",
				SchemaMismatchAction:   "reject",
				AllowNewFields:         false,
				AllowFieldTypeWidening: false,
				AllowFieldRemoval:      false,
				SchemaEvolutionLog:     []map[string]interface{}{},
				NestedItems:            []map[string]interface{}{},
				ConnectedToNodeID:      nodeIDInt64,
				Status:                 "STATUS_CREATED",
				Online:                 true,
				ItemMetadata:           map[string]interface{}{},
				EnrichedMetadata:       map[string]interface{}{},
				IsPrivileged:           false,
				OrdinalPosition:        column.OrdinalPosition,
			}

			// Apply enriched column data if available
			if colEnrichments, ok := columnEnrichmentMap[mvName]; ok {
				if enrichedCol, colOk := colEnrichments[colName]; colOk {
					item.IsPrivileged = enrichedCol.IsPrivilegedData
					if enrichedCol.DataCategory != "" {
						item.PrivilegedClassification = &enrichedCol.DataCategory
					}
					if enrichedCol.PrivilegedConfidence > 0 {
						item.DetectionConfidence = &enrichedCol.PrivilegedConfidence
					}
					if item.IsPrivileged {
						detectionMethod := "auto"
						item.DetectionMethod = &detectionMethod
					}
				}
			}

			items = append(items, item)
		}
	}

	// Process Graph Nodes (Neo4j, etc.)
	for nodeName, node := range um.Nodes {
		containerURI := GenerateContainerURI(protocol, scope, dbID, "node", nodeName)
		container := &models.ResourceContainer{
			TenantID:          tenantID,
			WorkspaceID:       workspaceID,
			ResourceURI:       containerURI,
			Protocol:          protocol,
			Scope:             scope,
			ObjectType:        "node",
			ObjectName:        nodeName,
			DatabaseID:        &dbID,
			ConnectedToNodeID: nodeIDInt64,
			OwnerID:           ownerID,
			Status:            "STATUS_CREATED",
			Online:            true,
			ContainerMetadata: map[string]interface{}{
				"label": node.Label, // Node has Label field, not Labels
			},
			EnrichedMetadata:              map[string]interface{}{},
			DatabaseType:                  &databaseType,
			ItemCount:                     len(node.Properties),
			ContainerClassificationSource: "auto", // Default value
		}

		// Populate classification fields if enrichment data is available
		if enrichment, ok := enrichmentMap[nodeName]; ok && enrichment.PrimaryCategory != "" {
			container.ContainerClassification = &enrichment.PrimaryCategory
			container.ContainerClassificationConfidence = &enrichment.ClassificationConfidence
		}

		containers = append(containers, container)

		// Process node properties
		for propName, prop := range node.Properties {
			itemURI := GenerateItemURI(containerURI, "property", propName, nil)
			item := &models.ResourceItem{
				TenantID:               tenantID,
				WorkspaceID:            workspaceID,
				ResourceURI:            itemURI,
				Protocol:               protocol,
				Scope:                  scope,
				ItemType:               "property",
				ItemName:               propName,
				ItemDisplayName:        generateItemDisplayName(databaseName, nodeName, propName),
				ItemPath:               []string{},
				DataType:               prop.Type,
				IsNullable:             true, // Property struct doesn't have Required field
				IsPrimaryKey:           false,
				IsUnique:               false,
				IsIndexed:              false,
				IsRequired:             false, // Property struct doesn't have Required field
				IsArray:                false,
				ArrayDimensions:        1,
				Constraints:            []map[string]interface{}{},
				IsCustomType:           false,
				HasSchema:              false,
				SchemaEvolutionVersion: 1,
				SchemaValidationMode:   "permissive",
				SchemaMismatchAction:   "accept_and_log",
				AllowNewFields:         true,
				AllowFieldTypeWidening: true,
				AllowFieldRemoval:      false,
				SchemaEvolutionLog:     []map[string]interface{}{},
				NestedItems:            []map[string]interface{}{},
				ConnectedToNodeID:      nodeIDInt64,
				Status:                 "STATUS_CREATED",
				Online:                 true,
				ItemMetadata:           map[string]interface{}{},
				EnrichedMetadata:       map[string]interface{}{},
				IsPrivileged:           false,
			}

			// Apply enriched property data if available
			if colEnrichments, ok := columnEnrichmentMap[nodeName]; ok {
				if enrichedCol, colOk := colEnrichments[propName]; colOk {
					item.IsPrivileged = enrichedCol.IsPrivilegedData
					if enrichedCol.DataCategory != "" {
						item.PrivilegedClassification = &enrichedCol.DataCategory
					}
					if enrichedCol.PrivilegedConfidence > 0 {
						item.DetectionConfidence = &enrichedCol.PrivilegedConfidence
					}
					if item.IsPrivileged {
						detectionMethod := "auto"
						item.DetectionMethod = &detectionMethod
					}
				}
			}

			items = append(items, item)
		}
	}

	// Process Graph Relationships
	for relName, rel := range um.Relationships {
		containerURI := GenerateContainerURI(protocol, scope, dbID, "relationship", relName)
		container := &models.ResourceContainer{
			TenantID:          tenantID,
			WorkspaceID:       workspaceID,
			ResourceURI:       containerURI,
			Protocol:          protocol,
			Scope:             scope,
			ObjectType:        "relationship",
			ObjectName:        relName,
			DatabaseID:        &dbID,
			ConnectedToNodeID: nodeIDInt64,
			OwnerID:           ownerID,
			Status:            "STATUS_CREATED",
			Online:            true,
			ContainerMetadata: map[string]interface{}{
				"type":       rel.Type,
				"from_label": rel.FromLabel, // Relationship has FromLabel field, not From
				"to_label":   rel.ToLabel,   // Relationship has ToLabel field, not To
			},
			EnrichedMetadata:              map[string]interface{}{},
			DatabaseType:                  &databaseType,
			ItemCount:                     len(rel.Properties),
			ContainerClassificationSource: "auto", // Default value
		}

		// Populate classification fields if enrichment data is available
		if enrichment, ok := enrichmentMap[relName]; ok && enrichment.PrimaryCategory != "" {
			container.ContainerClassification = &enrichment.PrimaryCategory
			container.ContainerClassificationConfidence = &enrichment.ClassificationConfidence
		}

		containers = append(containers, container)

		// Process relationship properties
		for propName, prop := range rel.Properties {
			itemURI := GenerateItemURI(containerURI, "property", propName, nil)
			item := &models.ResourceItem{
				TenantID:               tenantID,
				WorkspaceID:            workspaceID,
				ResourceURI:            itemURI,
				Protocol:               protocol,
				Scope:                  scope,
				ItemType:               "property",
				ItemName:               propName,
				ItemDisplayName:        generateItemDisplayName(databaseName, relName, propName),
				ItemPath:               []string{},
				DataType:               prop.Type,
				IsNullable:             true, // Property struct doesn't have Required field
				IsPrimaryKey:           false,
				IsUnique:               false,
				IsIndexed:              false,
				IsRequired:             false, // Property struct doesn't have Required field
				IsArray:                false,
				ArrayDimensions:        1,
				Constraints:            []map[string]interface{}{},
				IsCustomType:           false,
				HasSchema:              false,
				SchemaEvolutionVersion: 1,
				SchemaValidationMode:   "permissive",
				SchemaMismatchAction:   "accept_and_log",
				AllowNewFields:         true,
				AllowFieldTypeWidening: true,
				AllowFieldRemoval:      false,
				SchemaEvolutionLog:     []map[string]interface{}{},
				NestedItems:            []map[string]interface{}{},
				ConnectedToNodeID:      nodeIDInt64,
				Status:                 "STATUS_CREATED",
				Online:                 true,
				ItemMetadata:           map[string]interface{}{},
				EnrichedMetadata:       map[string]interface{}{},
				IsPrivileged:           false,
			}

			// Apply enriched property data if available
			if colEnrichments, ok := columnEnrichmentMap[relName]; ok {
				if enrichedCol, colOk := colEnrichments[propName]; colOk {
					item.IsPrivileged = enrichedCol.IsPrivilegedData
					if enrichedCol.DataCategory != "" {
						item.PrivilegedClassification = &enrichedCol.DataCategory
					}
					if enrichedCol.PrivilegedConfidence > 0 {
						item.DetectionConfidence = &enrichedCol.PrivilegedConfidence
					}
					if item.IsPrivileged {
						detectionMethod := "auto"
						item.DetectionMethod = &detectionMethod
					}
				}
			}

			items = append(items, item)
		}
	}

	return containers, items, nil
}

// getStringPtr returns a pointer to a string, or nil if the string is empty
func getStringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// GenerateStreamContainerURI generates a URI for a stream container resource
func GenerateStreamContainerURI(provider, connectionID, objType, objName string) string {
	return fmt.Sprintf("stream://%s/%s/%s/%s", provider, connectionID, objType, objName)
}

// GenerateWebhookContainerURI generates a URI for a webhook container resource
func GenerateWebhookContainerURI(endpointID, direction string) string {
	return fmt.Sprintf("webhook://%s/%s", endpointID, direction)
}

// GenerateMCPContainerURI generates a URI for an MCP container resource
func GenerateMCPContainerURI(serverID, objType, objName string) string {
	return fmt.Sprintf("mcp://%s/%s/%s", serverID, objType, objName)
}
