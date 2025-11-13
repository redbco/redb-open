package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redbco/redb-open/pkg/models"
)

// Repository provides data access methods for resource containers and items
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository creates a new resource repository
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// CreateContainer creates a new resource container
func (r *Repository) CreateContainer(ctx context.Context, container *models.ResourceContainer) error {
	containerMetadataJSON, err := json.Marshal(container.ContainerMetadata)
	if err != nil {
		return fmt.Errorf("error marshaling container metadata: %w", err)
	}

	enrichedMetadataJSON, err := json.Marshal(container.EnrichedMetadata)
	if err != nil {
		return fmt.Errorf("error marshaling enriched metadata: %w", err)
	}

	query := `
		INSERT INTO resource_containers (
			tenant_id, workspace_id, resource_uri, protocol, scope, object_type, object_name,
			database_id, instance_id, integration_id, mcpserver_id,
			connected_to_node_id, owner_id, status, status_message, last_seen, online,
			container_metadata, enriched_metadata, database_type, vendor, item_count, size_bytes,
			container_classification, container_classification_confidence, container_classification_source
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26
		)
		RETURNING container_id, created, updated
	`

	err = r.pool.QueryRow(ctx, query,
		container.TenantID, container.WorkspaceID, container.ResourceURI, container.Protocol, container.Scope,
		container.ObjectType, container.ObjectName, container.DatabaseID, container.InstanceID,
		container.IntegrationID, container.MCPServerID, container.ConnectedToNodeID, container.OwnerID,
		container.Status, container.StatusMessage, container.LastSeen, container.Online,
		containerMetadataJSON, enrichedMetadataJSON, container.DatabaseType, container.Vendor,
		container.ItemCount, container.SizeBytes,
		container.ContainerClassification, container.ContainerClassificationConfidence, container.ContainerClassificationSource,
	).Scan(&container.ContainerID, &container.Created, &container.Updated)

	if err != nil {
		return fmt.Errorf("error creating resource container: %w", err)
	}

	return nil
}

// UpdateContainer updates an existing resource container
func (r *Repository) UpdateContainer(ctx context.Context, containerID string, updates map[string]interface{}) error {
	// Build dynamic UPDATE query based on provided fields
	query := "UPDATE resource_containers SET "
	args := []interface{}{}
	argIdx := 1

	for key, value := range updates {
		if argIdx > 1 {
			query += ", "
		}

		// Handle JSONB fields separately
		if key == "container_metadata" || key == "enriched_metadata" {
			jsonData, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("error marshaling %s: %w", key, err)
			}
			query += fmt.Sprintf("%s = $%d", key, argIdx)
			args = append(args, jsonData)
		} else {
			query += fmt.Sprintf("%s = $%d", key, argIdx)
			args = append(args, value)
		}
		argIdx++
	}

	query += fmt.Sprintf(", updated = CURRENT_TIMESTAMP WHERE container_id = $%d", argIdx)
	args = append(args, containerID)

	result, err := r.pool.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("error updating resource container: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("resource container with ID %s not found", containerID)
	}

	return nil
}

// GetContainer retrieves a resource container by ID
func (r *Repository) GetContainer(ctx context.Context, containerID string) (*models.ResourceContainer, error) {
	query := `
		SELECT container_id, tenant_id, workspace_id, resource_uri, protocol, scope,
		       object_type, object_name, database_id, instance_id, integration_id, mcpserver_id,
		       connected_to_node_id, owner_id, status, status_message, last_seen, online,
		       container_metadata, enriched_metadata, database_type, vendor, item_count, size_bytes,
		       container_classification, container_classification_confidence, container_classification_source,
		       created, updated
		FROM resource_containers
		WHERE container_id = $1
	`

	container := &models.ResourceContainer{}
	var containerMetadataJSON, enrichedMetadataJSON []byte

	err := r.pool.QueryRow(ctx, query, containerID).Scan(
		&container.ContainerID, &container.TenantID, &container.WorkspaceID, &container.ResourceURI,
		&container.Protocol, &container.Scope, &container.ObjectType, &container.ObjectName,
		&container.DatabaseID, &container.InstanceID, &container.IntegrationID, &container.MCPServerID,
		&container.ConnectedToNodeID, &container.OwnerID, &container.Status, &container.StatusMessage,
		&container.LastSeen, &container.Online, &containerMetadataJSON, &enrichedMetadataJSON,
		&container.DatabaseType, &container.Vendor, &container.ItemCount, &container.SizeBytes,
		&container.ContainerClassification, &container.ContainerClassificationConfidence, &container.ContainerClassificationSource,
		&container.Created, &container.Updated,
	)

	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("resource container with ID %s not found", containerID)
	}
	if err != nil {
		return nil, fmt.Errorf("error getting resource container: %w", err)
	}

	// Unmarshal JSONB fields
	if err := json.Unmarshal(containerMetadataJSON, &container.ContainerMetadata); err != nil {
		return nil, fmt.Errorf("error unmarshaling container metadata: %w", err)
	}
	if err := json.Unmarshal(enrichedMetadataJSON, &container.EnrichedMetadata); err != nil {
		return nil, fmt.Errorf("error unmarshaling enriched metadata: %w", err)
	}

	return container, nil
}

// GetContainerByURI retrieves a resource container by URI
func (r *Repository) GetContainerByURI(ctx context.Context, uri string) (*models.ResourceContainer, error) {
	query := `
		SELECT container_id, tenant_id, workspace_id, resource_uri, protocol, scope,
		       object_type, object_name, database_id, instance_id, integration_id, mcpserver_id,
		       connected_to_node_id, owner_id, status, status_message, last_seen, online,
		       container_metadata, enriched_metadata, database_type, vendor, item_count, size_bytes,
		       container_classification, container_classification_confidence, container_classification_source,
		       created, updated
		FROM resource_containers
		WHERE resource_uri = $1
	`

	container := &models.ResourceContainer{}
	var containerMetadataJSON, enrichedMetadataJSON []byte

	err := r.pool.QueryRow(ctx, query, uri).Scan(
		&container.ContainerID, &container.TenantID, &container.WorkspaceID, &container.ResourceURI,
		&container.Protocol, &container.Scope, &container.ObjectType, &container.ObjectName,
		&container.DatabaseID, &container.InstanceID, &container.IntegrationID, &container.MCPServerID,
		&container.ConnectedToNodeID, &container.OwnerID, &container.Status, &container.StatusMessage,
		&container.LastSeen, &container.Online, &containerMetadataJSON, &enrichedMetadataJSON,
		&container.DatabaseType, &container.Vendor, &container.ItemCount, &container.SizeBytes,
		&container.ContainerClassification, &container.ContainerClassificationConfidence, &container.ContainerClassificationSource,
		&container.Created, &container.Updated,
	)

	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("resource container with URI %s not found", uri)
	}
	if err != nil {
		return nil, fmt.Errorf("error getting resource container: %w", err)
	}

	// Unmarshal JSONB fields
	if err := json.Unmarshal(containerMetadataJSON, &container.ContainerMetadata); err != nil {
		return nil, fmt.Errorf("error unmarshaling container metadata: %w", err)
	}
	if err := json.Unmarshal(enrichedMetadataJSON, &container.EnrichedMetadata); err != nil {
		return nil, fmt.Errorf("error unmarshaling enriched metadata: %w", err)
	}

	return container, nil
}

// ListContainers retrieves resource containers with optional filters
func (r *Repository) ListContainers(ctx context.Context, filter *models.ResourceContainerFilter) ([]*models.ResourceContainer, error) {
	query := `
		SELECT container_id, tenant_id, workspace_id, resource_uri, protocol, scope,
		       object_type, object_name, database_id, instance_id, integration_id, mcpserver_id,
		       connected_to_node_id, owner_id, status, status_message, last_seen, online,
		       container_metadata, enriched_metadata, database_type, vendor, item_count, size_bytes,
		       container_classification, container_classification_confidence, container_classification_source,
		       created, updated
		FROM resource_containers
		WHERE 1=1
	`

	args := []interface{}{}
	argIdx := 1

	// Apply filters
	if filter.TenantID != nil {
		query += fmt.Sprintf(" AND tenant_id = $%d", argIdx)
		args = append(args, *filter.TenantID)
		argIdx++
	}
	if filter.WorkspaceID != nil {
		query += fmt.Sprintf(" AND workspace_id = $%d", argIdx)
		args = append(args, *filter.WorkspaceID)
		argIdx++
	}
	if filter.DatabaseID != nil {
		query += fmt.Sprintf(" AND database_id = $%d", argIdx)
		args = append(args, *filter.DatabaseID)
		argIdx++
	}
	if filter.InstanceID != nil {
		query += fmt.Sprintf(" AND instance_id = $%d", argIdx)
		args = append(args, *filter.InstanceID)
		argIdx++
	}
	if filter.IntegrationID != nil {
		query += fmt.Sprintf(" AND integration_id = $%d", argIdx)
		args = append(args, *filter.IntegrationID)
		argIdx++
	}
	if filter.MCPServerID != nil {
		query += fmt.Sprintf(" AND mcpserver_id = $%d", argIdx)
		args = append(args, *filter.MCPServerID)
		argIdx++
	}
	if filter.NodeID != nil {
		query += fmt.Sprintf(" AND connected_to_node_id = $%d", argIdx)
		args = append(args, *filter.NodeID)
		argIdx++
	}
	if filter.Protocol != nil {
		query += fmt.Sprintf(" AND protocol = $%d", argIdx)
		args = append(args, *filter.Protocol)
		argIdx++
	}
	if filter.Scope != nil {
		query += fmt.Sprintf(" AND scope = $%d", argIdx)
		args = append(args, *filter.Scope)
		argIdx++
	}
	if filter.ObjectType != nil {
		query += fmt.Sprintf(" AND object_type = $%d", argIdx)
		args = append(args, *filter.ObjectType)
		argIdx++
	}
	if filter.DatabaseType != nil {
		query += fmt.Sprintf(" AND database_type = $%d", argIdx)
		args = append(args, *filter.DatabaseType)
		argIdx++
	}
	if filter.Status != nil {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, *filter.Status)
		argIdx++
	}
	if filter.Online != nil {
		query += fmt.Sprintf(" AND online = $%d", argIdx)
		args = append(args, *filter.Online)
		argIdx++
	}

	query += " ORDER BY created DESC"

	// Apply pagination
	if filter.Limit != nil && *filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIdx)
		args = append(args, *filter.Limit)
		argIdx++
	}
	if filter.Offset != nil && *filter.Offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argIdx)
		args = append(args, *filter.Offset)
		argIdx++
	}

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error listing resource containers: %w", err)
	}
	defer rows.Close()

	containers := []*models.ResourceContainer{}
	for rows.Next() {
		container := &models.ResourceContainer{}
		var containerMetadataJSON, enrichedMetadataJSON []byte

		err := rows.Scan(
			&container.ContainerID, &container.TenantID, &container.WorkspaceID, &container.ResourceURI,
			&container.Protocol, &container.Scope, &container.ObjectType, &container.ObjectName,
			&container.DatabaseID, &container.InstanceID, &container.IntegrationID, &container.MCPServerID,
			&container.ConnectedToNodeID, &container.OwnerID, &container.Status, &container.StatusMessage,
			&container.LastSeen, &container.Online, &containerMetadataJSON, &enrichedMetadataJSON,
			&container.DatabaseType, &container.Vendor, &container.ItemCount, &container.SizeBytes,
			&container.ContainerClassification, &container.ContainerClassificationConfidence, &container.ContainerClassificationSource,
			&container.Created, &container.Updated,
		)
		if err != nil {
			return nil, fmt.Errorf("error scanning resource container: %w", err)
		}

		// Unmarshal JSONB fields
		if err := json.Unmarshal(containerMetadataJSON, &container.ContainerMetadata); err != nil {
			return nil, fmt.Errorf("error unmarshaling container metadata: %w", err)
		}
		if err := json.Unmarshal(enrichedMetadataJSON, &container.EnrichedMetadata); err != nil {
			return nil, fmt.Errorf("error unmarshaling enriched metadata: %w", err)
		}

		containers = append(containers, container)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating resource containers: %w", err)
	}

	return containers, nil
}

// ListContainersByDatabase retrieves all resource containers for a database
func (r *Repository) ListContainersByDatabase(ctx context.Context, databaseID string) ([]*models.ResourceContainer, error) {
	filter := &models.ResourceContainerFilter{
		DatabaseID: &databaseID,
	}
	return r.ListContainers(ctx, filter)
}

// ListContainersByNode retrieves all resource containers for a node
func (r *Repository) ListContainersByNode(ctx context.Context, nodeID string) ([]*models.ResourceContainer, error) {
	// Convert nodeID string to int64
	nodeIDInt64, err := strconv.ParseInt(nodeID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse node ID '%s' as int64: %w", nodeID, err)
	}

	filter := &models.ResourceContainerFilter{
		NodeID: &nodeIDInt64,
	}
	return r.ListContainers(ctx, filter)
}

// SetContainerOnline sets the online status of a resource container
func (r *Repository) SetContainerOnline(ctx context.Context, containerID string, online bool) error {
	query := `
		UPDATE resource_containers 
		SET online = $1, last_seen = CURRENT_TIMESTAMP, updated = CURRENT_TIMESTAMP
		WHERE container_id = $2
	`

	result, err := r.pool.Exec(ctx, query, online, containerID)
	if err != nil {
		return fmt.Errorf("error setting container online status: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("resource container with ID %s not found", containerID)
	}

	return nil
}

// DeleteContainer deletes a resource container
func (r *Repository) DeleteContainer(ctx context.Context, containerID string) error {
	query := "DELETE FROM resource_containers WHERE container_id = $1"

	result, err := r.pool.Exec(ctx, query, containerID)
	if err != nil {
		return fmt.Errorf("error deleting resource container: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("resource container with ID %s not found", containerID)
	}

	return nil
}

// CreateItem creates a new resource item
func (r *Repository) CreateItem(ctx context.Context, item *models.ResourceItem) error {
	constraintsJSON, err := json.Marshal(item.Constraints)
	if err != nil {
		return fmt.Errorf("error marshaling constraints: %w", err)
	}

	customTypeDefJSON, err := json.Marshal(item.CustomTypeDefinition)
	if err != nil {
		return fmt.Errorf("error marshaling custom type definition: %w", err)
	}

	schemaDefJSON, err := json.Marshal(item.SchemaDefinition)
	if err != nil {
		return fmt.Errorf("error marshaling schema definition: %w", err)
	}

	schemaEvolutionLogJSON, err := json.Marshal(item.SchemaEvolutionLog)
	if err != nil {
		return fmt.Errorf("error marshaling schema evolution log: %w", err)
	}

	nestedItemsJSON, err := json.Marshal(item.NestedItems)
	if err != nil {
		return fmt.Errorf("error marshaling nested items: %w", err)
	}

	itemMetadataJSON, err := json.Marshal(item.ItemMetadata)
	if err != nil {
		return fmt.Errorf("error marshaling item metadata: %w", err)
	}

	enrichedMetadataJSON, err := json.Marshal(item.EnrichedMetadata)
	if err != nil {
		return fmt.Errorf("error marshaling enriched metadata: %w", err)
	}

	query := `
		INSERT INTO resource_items (
			container_id, tenant_id, workspace_id, resource_uri, protocol, scope,
			item_type, item_name, item_display_name, item_path, data_type, unified_data_type,
			is_nullable, is_primary_key, is_unique, is_indexed, is_required, is_array, array_dimensions,
			default_value, constraints,
			is_custom_type, custom_type_name, custom_type_definition,
			has_schema, schema_format, schema_definition, schema_version, schema_evolution_version,
			schema_validation_mode, schema_mismatch_action, allow_new_fields, allow_field_type_widening, allow_field_removal,
			schema_evolution_log, nested_items,
			max_length, precision, scale,
			connected_to_node_id, status, online, item_metadata, enriched_metadata, item_comment,
			is_privileged, privileged_classification, detection_confidence, detection_method,
			ordinal_position
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
			$21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38,
			$39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50
		)
		RETURNING item_id, created, updated
	`

	err = r.pool.QueryRow(ctx, query,
		item.ContainerID, item.TenantID, item.WorkspaceID, item.ResourceURI, item.Protocol, item.Scope,
		item.ItemType, item.ItemName, item.ItemDisplayName, item.ItemPath, item.DataType, item.UnifiedDataType,
		item.IsNullable, item.IsPrimaryKey, item.IsUnique, item.IsIndexed, item.IsRequired, item.IsArray, item.ArrayDimensions,
		item.DefaultValue, constraintsJSON,
		item.IsCustomType, item.CustomTypeName, customTypeDefJSON,
		item.HasSchema, item.SchemaFormat, schemaDefJSON, item.SchemaVersion, item.SchemaEvolutionVersion,
		item.SchemaValidationMode, item.SchemaMismatchAction, item.AllowNewFields, item.AllowFieldTypeWidening, item.AllowFieldRemoval,
		schemaEvolutionLogJSON, nestedItemsJSON,
		item.MaxLength, item.Precision, item.Scale,
		item.ConnectedToNodeID, item.Status, item.Online, itemMetadataJSON, enrichedMetadataJSON, item.ItemComment,
		item.IsPrivileged, item.PrivilegedClassification, item.DetectionConfidence, item.DetectionMethod,
		item.OrdinalPosition,
	).Scan(&item.ItemID, &item.Created, &item.Updated)

	if err != nil {
		return fmt.Errorf("error creating resource item: %w", err)
	}

	return nil
}

// UpdateItem updates an existing resource item
func (r *Repository) UpdateItem(ctx context.Context, itemID string, updates map[string]interface{}) error {
	// Build dynamic UPDATE query based on provided fields
	query := "UPDATE resource_items SET "
	args := []interface{}{}
	argIdx := 1

	jsonFields := map[string]bool{
		"constraints":            true,
		"custom_type_definition": true,
		"schema_definition":      true,
		"schema_evolution_log":   true,
		"nested_items":           true,
		"item_metadata":          true,
		"enriched_metadata":      true,
	}

	for key, value := range updates {
		if argIdx > 1 {
			query += ", "
		}

		// Handle JSONB fields separately
		if jsonFields[key] {
			jsonData, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("error marshaling %s: %w", key, err)
			}
			query += fmt.Sprintf("%s = $%d", key, argIdx)
			args = append(args, jsonData)
		} else {
			query += fmt.Sprintf("%s = $%d", key, argIdx)
			args = append(args, value)
		}
		argIdx++
	}

	query += fmt.Sprintf(", updated = CURRENT_TIMESTAMP WHERE item_id = $%d", argIdx)
	args = append(args, itemID)

	result, err := r.pool.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("error updating resource item: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("resource item with ID %s not found", itemID)
	}

	return nil
}

// GetItem retrieves a resource item by ID
func (r *Repository) GetItem(ctx context.Context, itemID string) (*models.ResourceItem, error) {
	query := `
		SELECT item_id, container_id, tenant_id, workspace_id, resource_uri, protocol, scope,
		       item_type, item_name, item_display_name, item_path, data_type, unified_data_type,
		       is_nullable, is_primary_key, is_unique, is_indexed, is_required, is_array, array_dimensions,
		       default_value, constraints,
		       is_custom_type, custom_type_name, custom_type_definition,
		       has_schema, schema_format, schema_definition, schema_version, schema_evolution_version,
		       schema_validation_mode, schema_mismatch_action, allow_new_fields, allow_field_type_widening, allow_field_removal,
		       schema_evolution_log, nested_items,
		       max_length, precision, scale,
		       connected_to_node_id, status, online, item_metadata, enriched_metadata, item_comment,
		       is_privileged, privileged_classification, detection_confidence, detection_method,
		       ordinal_position, created, updated
		FROM resource_items
		WHERE item_id = $1
	`

	item := &models.ResourceItem{}
	var constraintsJSON, customTypeDefJSON, schemaDefJSON, schemaEvolutionLogJSON,
		nestedItemsJSON, itemMetadataJSON, enrichedMetadataJSON []byte

	err := r.pool.QueryRow(ctx, query, itemID).Scan(
		&item.ItemID, &item.ContainerID, &item.TenantID, &item.WorkspaceID, &item.ResourceURI, &item.Protocol, &item.Scope,
		&item.ItemType, &item.ItemName, &item.ItemDisplayName, &item.ItemPath, &item.DataType, &item.UnifiedDataType,
		&item.IsNullable, &item.IsPrimaryKey, &item.IsUnique, &item.IsIndexed, &item.IsRequired, &item.IsArray, &item.ArrayDimensions,
		&item.DefaultValue, &constraintsJSON,
		&item.IsCustomType, &item.CustomTypeName, &customTypeDefJSON,
		&item.HasSchema, &item.SchemaFormat, &schemaDefJSON, &item.SchemaVersion, &item.SchemaEvolutionVersion,
		&item.SchemaValidationMode, &item.SchemaMismatchAction, &item.AllowNewFields, &item.AllowFieldTypeWidening, &item.AllowFieldRemoval,
		&schemaEvolutionLogJSON, &nestedItemsJSON,
		&item.MaxLength, &item.Precision, &item.Scale,
		&item.ConnectedToNodeID, &item.Status, &item.Online, &itemMetadataJSON, &enrichedMetadataJSON, &item.ItemComment,
		&item.IsPrivileged, &item.PrivilegedClassification, &item.DetectionConfidence, &item.DetectionMethod,
		&item.OrdinalPosition, &item.Created, &item.Updated,
	)

	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("resource item with ID %s not found", itemID)
	}
	if err != nil {
		return nil, fmt.Errorf("error getting resource item: %w", err)
	}

	// Unmarshal JSONB fields
	if err := json.Unmarshal(constraintsJSON, &item.Constraints); err != nil {
		return nil, fmt.Errorf("error unmarshaling constraints: %w", err)
	}
	if err := json.Unmarshal(customTypeDefJSON, &item.CustomTypeDefinition); err != nil {
		return nil, fmt.Errorf("error unmarshaling custom type definition: %w", err)
	}
	if err := json.Unmarshal(schemaDefJSON, &item.SchemaDefinition); err != nil {
		return nil, fmt.Errorf("error unmarshaling schema definition: %w", err)
	}
	if err := json.Unmarshal(schemaEvolutionLogJSON, &item.SchemaEvolutionLog); err != nil {
		return nil, fmt.Errorf("error unmarshaling schema evolution log: %w", err)
	}
	if err := json.Unmarshal(nestedItemsJSON, &item.NestedItems); err != nil {
		return nil, fmt.Errorf("error unmarshaling nested items: %w", err)
	}
	if err := json.Unmarshal(itemMetadataJSON, &item.ItemMetadata); err != nil {
		return nil, fmt.Errorf("error unmarshaling item metadata: %w", err)
	}
	if err := json.Unmarshal(enrichedMetadataJSON, &item.EnrichedMetadata); err != nil {
		return nil, fmt.Errorf("error unmarshaling enriched metadata: %w", err)
	}

	return item, nil
}

// GetItemByURI retrieves a resource item by URI
func (r *Repository) GetItemByURI(ctx context.Context, uri string) (*models.ResourceItem, error) {
	query := `
		SELECT item_id, container_id, tenant_id, workspace_id, resource_uri, protocol, scope,
		       item_type, item_name, item_display_name, item_path, data_type, unified_data_type,
		       is_nullable, is_primary_key, is_unique, is_indexed, is_required, is_array, array_dimensions,
		       default_value, constraints,
		       is_custom_type, custom_type_name, custom_type_definition,
		       has_schema, schema_format, schema_definition, schema_version, schema_evolution_version,
		       schema_validation_mode, schema_mismatch_action, allow_new_fields, allow_field_type_widening, allow_field_removal,
		       schema_evolution_log, nested_items,
		       max_length, precision, scale,
		       connected_to_node_id, status, online, item_metadata, enriched_metadata, item_comment,
		       is_privileged, privileged_classification, detection_confidence, detection_method,
		       ordinal_position, created, updated
		FROM resource_items
		WHERE resource_uri = $1
	`

	item := &models.ResourceItem{}
	var constraintsJSON, customTypeDefJSON, schemaDefJSON, schemaEvolutionLogJSON,
		nestedItemsJSON, itemMetadataJSON, enrichedMetadataJSON []byte

	err := r.pool.QueryRow(ctx, query, uri).Scan(
		&item.ItemID, &item.ContainerID, &item.TenantID, &item.WorkspaceID, &item.ResourceURI, &item.Protocol, &item.Scope,
		&item.ItemType, &item.ItemName, &item.ItemDisplayName, &item.ItemPath, &item.DataType, &item.UnifiedDataType,
		&item.IsNullable, &item.IsPrimaryKey, &item.IsUnique, &item.IsIndexed, &item.IsRequired, &item.IsArray, &item.ArrayDimensions,
		&item.DefaultValue, &constraintsJSON,
		&item.IsCustomType, &item.CustomTypeName, &customTypeDefJSON,
		&item.HasSchema, &item.SchemaFormat, &schemaDefJSON, &item.SchemaVersion, &item.SchemaEvolutionVersion,
		&item.SchemaValidationMode, &item.SchemaMismatchAction, &item.AllowNewFields, &item.AllowFieldTypeWidening, &item.AllowFieldRemoval,
		&schemaEvolutionLogJSON, &nestedItemsJSON,
		&item.MaxLength, &item.Precision, &item.Scale,
		&item.ConnectedToNodeID, &item.Status, &item.Online, &itemMetadataJSON, &enrichedMetadataJSON, &item.ItemComment,
		&item.IsPrivileged, &item.PrivilegedClassification, &item.DetectionConfidence, &item.DetectionMethod,
		&item.OrdinalPosition, &item.Created, &item.Updated,
	)

	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("resource item with URI %s not found", uri)
	}
	if err != nil {
		return nil, fmt.Errorf("error getting resource item: %w", err)
	}

	// Unmarshal JSONB fields
	if err := json.Unmarshal(constraintsJSON, &item.Constraints); err != nil {
		return nil, fmt.Errorf("error unmarshaling constraints: %w", err)
	}
	if err := json.Unmarshal(customTypeDefJSON, &item.CustomTypeDefinition); err != nil {
		return nil, fmt.Errorf("error unmarshaling custom type definition: %w", err)
	}
	if err := json.Unmarshal(schemaDefJSON, &item.SchemaDefinition); err != nil {
		return nil, fmt.Errorf("error unmarshaling schema definition: %w", err)
	}
	if err := json.Unmarshal(schemaEvolutionLogJSON, &item.SchemaEvolutionLog); err != nil {
		return nil, fmt.Errorf("error unmarshaling schema evolution log: %w", err)
	}
	if err := json.Unmarshal(nestedItemsJSON, &item.NestedItems); err != nil {
		return nil, fmt.Errorf("error unmarshaling nested items: %w", err)
	}
	if err := json.Unmarshal(itemMetadataJSON, &item.ItemMetadata); err != nil {
		return nil, fmt.Errorf("error unmarshaling item metadata: %w", err)
	}
	if err := json.Unmarshal(enrichedMetadataJSON, &item.EnrichedMetadata); err != nil {
		return nil, fmt.Errorf("error unmarshaling enriched metadata: %w", err)
	}

	return item, nil
}

// ListItemsByContainer retrieves all resource items for a container
func (r *Repository) ListItemsByContainer(ctx context.Context, containerID string) ([]*models.ResourceItem, error) {
	filter := &models.ResourceItemFilter{
		ContainerID: &containerID,
	}
	return r.ListItems(ctx, filter)
}

// ListPrivilegedItems retrieves all privileged resource items for a workspace
func (r *Repository) ListPrivilegedItems(ctx context.Context, workspaceID string) ([]*models.ResourceItem, error) {
	isPrivileged := true
	filter := &models.ResourceItemFilter{
		WorkspaceID:  &workspaceID,
		IsPrivileged: &isPrivileged,
	}
	return r.ListItems(ctx, filter)
}

// ListItems retrieves resource items with optional filters
func (r *Repository) ListItems(ctx context.Context, filter *models.ResourceItemFilter) ([]*models.ResourceItem, error) {
	query := `
		SELECT item_id, container_id, tenant_id, workspace_id, resource_uri, protocol, scope,
		       item_type, item_name, item_display_name, item_path, data_type, unified_data_type,
		       is_nullable, is_primary_key, is_unique, is_indexed, is_required, is_array, array_dimensions,
		       default_value, constraints,
		       is_custom_type, custom_type_name, custom_type_definition,
		       has_schema, schema_format, schema_definition, schema_version, schema_evolution_version,
		       schema_validation_mode, schema_mismatch_action, allow_new_fields, allow_field_type_widening, allow_field_removal,
		       schema_evolution_log, nested_items,
		       max_length, precision, scale,
		       connected_to_node_id, status, online, item_metadata, enriched_metadata, item_comment,
		       is_privileged, privileged_classification, detection_confidence, detection_method,
		       ordinal_position, created, updated
		FROM resource_items
		WHERE 1=1
	`

	args := []interface{}{}
	argIdx := 1

	// Apply filters
	if filter.TenantID != nil {
		query += fmt.Sprintf(" AND tenant_id = $%d", argIdx)
		args = append(args, *filter.TenantID)
		argIdx++
	}
	if filter.WorkspaceID != nil {
		query += fmt.Sprintf(" AND workspace_id = $%d", argIdx)
		args = append(args, *filter.WorkspaceID)
		argIdx++
	}
	if filter.ContainerID != nil {
		query += fmt.Sprintf(" AND container_id = $%d", argIdx)
		args = append(args, *filter.ContainerID)
		argIdx++
	}
	if filter.NodeID != nil {
		query += fmt.Sprintf(" AND connected_to_node_id = $%d", argIdx)
		args = append(args, *filter.NodeID)
		argIdx++
	}
	if filter.Protocol != nil {
		query += fmt.Sprintf(" AND protocol = $%d", argIdx)
		args = append(args, *filter.Protocol)
		argIdx++
	}
	if filter.Scope != nil {
		query += fmt.Sprintf(" AND scope = $%d", argIdx)
		args = append(args, *filter.Scope)
		argIdx++
	}
	if filter.ItemType != nil {
		query += fmt.Sprintf(" AND item_type = $%d", argIdx)
		args = append(args, *filter.ItemType)
		argIdx++
	}
	if filter.DataType != nil {
		query += fmt.Sprintf(" AND data_type = $%d", argIdx)
		args = append(args, *filter.DataType)
		argIdx++
	}
	if filter.UnifiedDataType != nil {
		query += fmt.Sprintf(" AND unified_data_type = $%d", argIdx)
		args = append(args, *filter.UnifiedDataType)
		argIdx++
	}
	if filter.IsPrimaryKey != nil {
		query += fmt.Sprintf(" AND is_primary_key = $%d", argIdx)
		args = append(args, *filter.IsPrimaryKey)
		argIdx++
	}
	if filter.IsUnique != nil {
		query += fmt.Sprintf(" AND is_unique = $%d", argIdx)
		args = append(args, *filter.IsUnique)
		argIdx++
	}
	if filter.IsIndexed != nil {
		query += fmt.Sprintf(" AND is_indexed = $%d", argIdx)
		args = append(args, *filter.IsIndexed)
		argIdx++
	}
	if filter.IsPrivileged != nil {
		query += fmt.Sprintf(" AND is_privileged = $%d", argIdx)
		args = append(args, *filter.IsPrivileged)
		argIdx++
	}
	if filter.PrivilegedClassification != nil {
		query += fmt.Sprintf(" AND privileged_classification = $%d", argIdx)
		args = append(args, *filter.PrivilegedClassification)
		argIdx++
	}
	if filter.IsCustomType != nil {
		query += fmt.Sprintf(" AND is_custom_type = $%d", argIdx)
		args = append(args, *filter.IsCustomType)
		argIdx++
	}
	if filter.HasSchema != nil {
		query += fmt.Sprintf(" AND has_schema = $%d", argIdx)
		args = append(args, *filter.HasSchema)
		argIdx++
	}
	if filter.SchemaFormat != nil {
		query += fmt.Sprintf(" AND schema_format = $%d", argIdx)
		args = append(args, *filter.SchemaFormat)
		argIdx++
	}
	if filter.Online != nil {
		query += fmt.Sprintf(" AND online = $%d", argIdx)
		args = append(args, *filter.Online)
		argIdx++
	}
	if filter.Status != nil {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, *filter.Status)
		argIdx++
	}

	query += " ORDER BY ordinal_position ASC NULLS LAST, created DESC"

	// Apply pagination
	if filter.Limit != nil && *filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIdx)
		args = append(args, *filter.Limit)
		argIdx++
	}
	if filter.Offset != nil && *filter.Offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argIdx)
		args = append(args, *filter.Offset)
		argIdx++
	}

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error listing resource items: %w", err)
	}
	defer rows.Close()

	items := []*models.ResourceItem{}
	for rows.Next() {
		item := &models.ResourceItem{}
		var constraintsJSON, customTypeDefJSON, schemaDefJSON, schemaEvolutionLogJSON,
			nestedItemsJSON, itemMetadataJSON, enrichedMetadataJSON []byte

		err := rows.Scan(
			&item.ItemID, &item.ContainerID, &item.TenantID, &item.WorkspaceID, &item.ResourceURI, &item.Protocol, &item.Scope,
			&item.ItemType, &item.ItemName, &item.ItemDisplayName, &item.ItemPath, &item.DataType, &item.UnifiedDataType,
			&item.IsNullable, &item.IsPrimaryKey, &item.IsUnique, &item.IsIndexed, &item.IsRequired, &item.IsArray, &item.ArrayDimensions,
			&item.DefaultValue, &constraintsJSON,
			&item.IsCustomType, &item.CustomTypeName, &customTypeDefJSON,
			&item.HasSchema, &item.SchemaFormat, &schemaDefJSON, &item.SchemaVersion, &item.SchemaEvolutionVersion,
			&item.SchemaValidationMode, &item.SchemaMismatchAction, &item.AllowNewFields, &item.AllowFieldTypeWidening, &item.AllowFieldRemoval,
			&schemaEvolutionLogJSON, &nestedItemsJSON,
			&item.MaxLength, &item.Precision, &item.Scale,
			&item.ConnectedToNodeID, &item.Status, &item.Online, &itemMetadataJSON, &enrichedMetadataJSON, &item.ItemComment,
			&item.IsPrivileged, &item.PrivilegedClassification, &item.DetectionConfidence, &item.DetectionMethod,
			&item.OrdinalPosition, &item.Created, &item.Updated,
		)
		if err != nil {
			return nil, fmt.Errorf("error scanning resource item: %w", err)
		}

		// Unmarshal JSONB fields
		if err := json.Unmarshal(constraintsJSON, &item.Constraints); err != nil {
			return nil, fmt.Errorf("error unmarshaling constraints: %w", err)
		}
		if err := json.Unmarshal(customTypeDefJSON, &item.CustomTypeDefinition); err != nil {
			return nil, fmt.Errorf("error unmarshaling custom type definition: %w", err)
		}
		if err := json.Unmarshal(schemaDefJSON, &item.SchemaDefinition); err != nil {
			return nil, fmt.Errorf("error unmarshaling schema definition: %w", err)
		}
		if err := json.Unmarshal(schemaEvolutionLogJSON, &item.SchemaEvolutionLog); err != nil {
			return nil, fmt.Errorf("error unmarshaling schema evolution log: %w", err)
		}
		if err := json.Unmarshal(nestedItemsJSON, &item.NestedItems); err != nil {
			return nil, fmt.Errorf("error unmarshaling nested items: %w", err)
		}
		if err := json.Unmarshal(itemMetadataJSON, &item.ItemMetadata); err != nil {
			return nil, fmt.Errorf("error unmarshaling item metadata: %w", err)
		}
		if err := json.Unmarshal(enrichedMetadataJSON, &item.EnrichedMetadata); err != nil {
			return nil, fmt.Errorf("error unmarshaling enriched metadata: %w", err)
		}

		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating resource items: %w", err)
	}

	return items, nil
}

// DeleteItem deletes a resource item
func (r *Repository) DeleteItem(ctx context.Context, itemID string) error {
	query := "DELETE FROM resource_items WHERE item_id = $1"

	result, err := r.pool.Exec(ctx, query, itemID)
	if err != nil {
		return fmt.Errorf("error deleting resource item: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("resource item with ID %s not found", itemID)
	}

	return nil
}
