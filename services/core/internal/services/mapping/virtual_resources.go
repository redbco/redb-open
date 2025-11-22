package mapping

import (
	"context"
	"fmt"
	"time"

	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/models"
	"github.com/redbco/redb-open/pkg/unifiedmodel/resource"
)

// VirtualResourceService handles virtual resource container and item management
type VirtualResourceService struct {
	db *database.PostgreSQL
}

// NewVirtualResourceService creates a new virtual resource service
func NewVirtualResourceService(db *database.PostgreSQL) *VirtualResourceService {
	return &VirtualResourceService{
		db: db,
	}
}

// CreateVirtualContainerParams contains parameters for creating a virtual container
type CreateVirtualContainerParams struct {
	TenantID          string
	WorkspaceID       string
	OwnerID           string
	ObjectType        string // "table", "collection", etc.
	ObjectName        string
	Namespace         string // defaults to "default"
	BindingMode       string // "template", "unbound", "bound", "auto_bind"
	BoundDatabaseID   *string
	VirtualSource     string // "user", "inferred", "template", "mcp"
	ContainerMetadata map[string]interface{}
}

// CreateVirtual Container creates a virtual resource container not linked to any database
func (s *VirtualResourceService) CreateVirtualContainer(
	ctx context.Context,
	params CreateVirtualContainerParams,
) (*models.ResourceContainer, error) {
	// Set defaults
	if params.Namespace == "" {
		params.Namespace = "default"
	}
	if params.BindingMode == "" {
		params.BindingMode = "unbound"
	}
	if params.VirtualSource == "" {
		params.VirtualSource = "user"
	}

	// Validate binding mode
	validBindingModes := map[string]bool{
		"template":  true,
		"unbound":   true,
		"bound":     true,
		"auto_bind": true,
	}
	if !validBindingModes[params.BindingMode] {
		return nil, fmt.Errorf("invalid binding_mode: %s", params.BindingMode)
	}

	// If binding_mode is "bound", bound_database_id must be provided
	if params.BindingMode == "bound" && params.BoundDatabaseID == nil {
		return nil, fmt.Errorf("bound_database_id required when binding_mode is 'bound'")
	}

	// Generate template URI
	protocol := string(resource.ProtocolTemplate)
	scope := string(resource.ScopeData)
	objectType := params.ObjectType
	objectName := params.ObjectName

	resourceURI := fmt.Sprintf("%s://%s/%s/%s/%s",
		protocol,
		params.Namespace,
		"database", // Always use "database" segment for containers
		objectType,
		objectName,
	)

	//  Check if virtual container with same name already exists in workspace
	var existingCount int
	err := s.db.Pool().QueryRow(ctx, `
		SELECT COUNT(*)
		FROM resource_containers
		WHERE workspace_id = $1
		  AND object_name = $2
		  AND is_virtual = true
	`, params.WorkspaceID, params.ObjectName).Scan(&existingCount)

	if err != nil {
		return nil, fmt.Errorf("failed to check existing virtual containers: %w", err)
	}

	if existingCount > 0 {
		return nil, fmt.Errorf("virtual container with name '%s' already exists in workspace", params.ObjectName)
	}

	// Create the virtual container
	container := &models.ResourceContainer{
		TenantID:                      params.TenantID,
		WorkspaceID:                   params.WorkspaceID,
		ResourceURI:                   resourceURI,
		Protocol:                      protocol,
		Scope:                         scope,
		ObjectType:                    objectType,
		ObjectName:                    objectName,
		DatabaseID:                    nil, // Virtual containers have no database
		ConnectedToNodeID:             nil, // Virtual containers have no node
		IsVirtual:                     true,
		VirtualSource:                 params.VirtualSource,
		VirtualNamespace:              params.Namespace,
		BindingMode:                   params.BindingMode,
		BoundDatabaseID:               params.BoundDatabaseID,
		ReconciliationStatus:          "pending",
		ReconciliationDetails:         make(map[string]interface{}),
		OwnerID:                       params.OwnerID,
		Status:                        "STATUS_CREATED",
		Online:                        true,
		ContainerMetadata:             params.ContainerMetadata,
		EnrichedMetadata:              make(map[string]interface{}),
		ItemCount:                     0,
		SizeBytes:                     0,
		ContainerClassificationSource: "manual",
	}

	// Insert into database
	query := `
		INSERT INTO resource_containers (
			tenant_id, workspace_id, resource_uri, protocol, scope,
			object_type, object_name, is_virtual, virtual_source,
			virtual_namespace, binding_mode, bound_database_id,
			reconciliation_status, reconciliation_details,
			owner_id, status, online, container_metadata,
			enriched_metadata, item_count, size_bytes,
			container_classification_source, created, updated
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16, $17, $18, $19,
			$20, $21, $22, NOW(), NOW()
		)
		RETURNING container_id, created, updated
	`

	err = s.db.Pool().QueryRow(ctx, query,
		container.TenantID,
		container.WorkspaceID,
		container.ResourceURI,
		container.Protocol,
		container.Scope,
		container.ObjectType,
		container.ObjectName,
		container.IsVirtual,
		container.VirtualSource,
		container.VirtualNamespace,
		container.BindingMode,
		container.BoundDatabaseID,
		container.ReconciliationStatus,
		container.ReconciliationDetails,
		container.OwnerID,
		container.Status,
		container.Online,
		container.ContainerMetadata,
		container.EnrichedMetadata,
		container.ItemCount,
		container.SizeBytes,
		container.ContainerClassificationSource,
	).Scan(&container.ContainerID, &container.Created, &container.Updated)

	if err != nil {
		return nil, fmt.Errorf("failed to insert virtual container: %w", err)
	}

	return container, nil
}

// CreateVirtualItemParams contains parameters for creating a virtual item
type CreateVirtualItemParams struct {
	ContainerID   string
	TenantID      string
	WorkspaceID   string
	OwnerID       string
	ItemName      string
	DataType      string
	IsNullable    bool
	IsPrimaryKey  bool
	IsUnique      bool
	IsRequired    bool
	IsArray       bool
	VirtualSource string // "user", "inferred", "from_mapping", "mcp"
	ItemMetadata  map[string]interface{}
}

// CreateVirtualItem creates a virtual resource item within a container
func (s *VirtualResourceService) CreateVirtualItem(
	ctx context.Context,
	params CreateVirtualItemParams,
) (*models.ResourceItem, error) {
	// Get parent container to build URI and validate
	var container models.ResourceContainer
	err := s.db.Pool().QueryRow(ctx, `
		SELECT container_id, resource_uri, protocol, scope, object_type, object_name,
		       is_virtual, binding_mode, tenant_id, workspace_id
		FROM resource_containers
		WHERE container_id = $1
	`, params.ContainerID).Scan(
		&container.ContainerID,
		&container.ResourceURI,
		&container.Protocol,
		&container.Scope,
		&container.ObjectType,
		&container.ObjectName,
		&container.IsVirtual,
		&container.BindingMode,
		&container.TenantID,
		&container.WorkspaceID,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to find container: %w", err)
	}

	// Set defaults
	if params.VirtualSource == "" {
		params.VirtualSource = "user"
	}

	// Determine item type based on container object type
	itemType := "field"
	if container.ObjectType == "table" || container.ObjectType == "view" {
		itemType = "column"
	} else if container.ObjectType == "node" || container.ObjectType == "relationship" {
		itemType = "property"
	}

	// Generate item URI
	itemURI := fmt.Sprintf("%s/%s/%s", container.ResourceURI, itemType, params.ItemName)

	// Create virtual item
	item := &models.ResourceItem{
		ContainerID:            params.ContainerID,
		TenantID:               params.TenantID,
		WorkspaceID:            params.WorkspaceID,
		ResourceURI:            itemURI,
		Protocol:               container.Protocol,
		Scope:                  container.Scope,
		ItemType:               itemType,
		ItemName:               params.ItemName,
		ItemDisplayName:        params.ItemName,
		ItemPath:               []string{},
		DataType:               params.DataType,
		IsNullable:             params.IsNullable,
		IsPrimaryKey:           params.IsPrimaryKey,
		IsUnique:               params.IsUnique,
		IsRequired:             params.IsRequired,
		IsArray:                params.IsArray,
		ArrayDimensions:        1,
		Constraints:            []map[string]interface{}{},
		SchemaEvolutionVersion: 1,
		SchemaValidationMode:   "strict",
		SchemaMismatchAction:   "reject",
		SchemaEvolutionLog:     []map[string]interface{}{},
		NestedItems:            []map[string]interface{}{},
		ConnectedToNodeID:      nil, // Virtual items have no node
		IsVirtual:              true,
		VirtualSource:          params.VirtualSource,
		BindingMode:            nil, // Inherits from container by default
		ReconciliationStatus:   "pending",
		ReconciliationDetails:  make(map[string]interface{}),
		Status:                 "STATUS_CREATED",
		Online:                 true,
		ItemMetadata:           params.ItemMetadata,
		EnrichedMetadata:       make(map[string]interface{}),
		IsPrivileged:           false,
	}

	// Insert into database
	query := `
		INSERT INTO resource_items (
			container_id, tenant_id, workspace_id, resource_uri, protocol, scope,
			item_type, item_name, item_display_name, item_path, data_type,
			is_nullable, is_primary_key, is_unique, is_required, is_array,
			array_dimensions, constraints, schema_evolution_version,
			schema_validation_mode, schema_mismatch_action, schema_evolution_log,
			nested_items, is_virtual, virtual_source, binding_mode,
			reconciliation_status, reconciliation_details, status, online,
			item_metadata, enriched_metadata, is_privileged, created, updated
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16, $17, $18, $19,
			$20, $21, $22, $23, $24, $25, $26, $27, $28,
			$29, $30, $31, $32, $33, NOW(), NOW()
		)
		RETURNING item_id, created, updated
	`

	err = s.db.Pool().QueryRow(ctx, query,
		item.ContainerID,
		item.TenantID,
		item.WorkspaceID,
		item.ResourceURI,
		item.Protocol,
		item.Scope,
		item.ItemType,
		item.ItemName,
		item.ItemDisplayName,
		item.ItemPath,
		item.DataType,
		item.IsNullable,
		item.IsPrimaryKey,
		item.IsUnique,
		item.IsRequired,
		item.IsArray,
		item.ArrayDimensions,
		item.Constraints,
		item.SchemaEvolutionVersion,
		item.SchemaValidationMode,
		item.SchemaMismatchAction,
		item.SchemaEvolutionLog,
		item.NestedItems,
		item.IsVirtual,
		item.VirtualSource,
		item.BindingMode,
		item.ReconciliationStatus,
		item.ReconciliationDetails,
		item.Status,
		item.Online,
		item.ItemMetadata,
		item.EnrichedMetadata,
		item.IsPrivileged,
	).Scan(&item.ItemID, &item.Created, &item.Updated)

	if err != nil {
		return nil, fmt.Errorf("failed to insert virtual item: %w", err)
	}

	// Update container item count
	_, err = s.db.Pool().Exec(ctx, `
		UPDATE resource_containers
		SET item_count = item_count + 1, updated = NOW()
		WHERE container_id = $1
	`, params.ContainerID)

	if err != nil {
		return nil, fmt.Errorf("failed to update container item count: %w", err)
	}

	return item, nil
}

// BindVirtualContainer binds a virtual container to a specific database
func (s *VirtualResourceService) BindVirtualContainer(
	ctx context.Context,
	containerID string,
	databaseID string,
) error {
	// Update binding mode and bound_database_id
	result, err := s.db.Pool().Exec(ctx, `
		UPDATE resource_containers
		SET binding_mode = 'bound',
		    bound_database_id = $2,
		    reconciliation_status = 'pending',
		    updated = NOW()
		WHERE container_id = $1
		  AND is_virtual = true
	`, containerID, databaseID)

	if err != nil {
		return fmt.Errorf("failed to bind virtual container: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("virtual container not found or already bound")
	}

	return nil
}

// UnbindVirtualContainer removes database binding from a virtual container
func (s *VirtualResourceService) UnbindVirtualContainer(
	ctx context.Context,
	containerID string,
) error {
	result, err := s.db.Pool().Exec(ctx, `
		UPDATE resource_containers
		SET binding_mode = 'unbound',
		    bound_database_id = NULL,
		    reconciliation_status = 'pending',
		    reconciled_container_id = NULL,
		    reconciled_at = NULL,
		    updated = NOW()
		WHERE container_id = $1
		  AND is_virtual = true
	`, containerID)

	if err != nil {
		return fmt.Errorf("failed to unbind virtual container: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("virtual container not found")
	}

	return nil
}

// ListVirtualContainers retrieves all virtual containers for a workspace
func (s *VirtualResourceService) ListVirtualContainers(
	ctx context.Context,
	workspaceID string,
	includeOrphaned bool,
) ([]*models.ResourceContainer, error) {
	query := `
		SELECT container_id, tenant_id, workspace_id, resource_uri, protocol, scope,
		       object_type, object_name, database_id, is_virtual, virtual_source,
		       virtual_namespace, binding_mode, bound_database_id,
		       reconciliation_status, reconciled_container_id, reconciliation_details,
		       reciled_at, owner_id, status, status_message, online,
		       container_metadata, enriched_metadata, item_count, size_bytes,
		       containerclassification, container_classification_confidence,
		       container_classification_source, created, updated
		FROM resource_containers
		WHERE workspace_id = $1
		  AND is_virtual = true
	`

	if !includeOrphaned {
		query += " AND reconciliation_status != 'orphaned'"
	}

	query += " ORDER BY created DESC"

	rows, err := s.db.Pool().Query(ctx, query, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("failed to list virtual containers: %w", err)
	}
	defer rows.Close()

	containers := []*models.ResourceContainer{}
	for rows.Next() {
		var c models.ResourceContainer
		err := rows.Scan(
			&c.ContainerID,
			&c.TenantID,
			&c.WorkspaceID,
			&c.ResourceURI,
			&c.Protocol,
			&c.Scope,
			&c.ObjectType,
			&c.ObjectName,
			&c.DatabaseID,
			&c.IsVirtual,
			&c.VirtualSource,
			&c.VirtualNamespace,
			&c.BindingMode,
			&c.BoundDatabaseID,
			&c.ReconciliationStatus,
			&c.ReconciledContainerID,
			&c.ReconciliationDetails,
			&c.ReconciledAt,
			&c.OwnerID,
			&c.Status,
			&c.StatusMessage,
			&c.Online,
			&c.ContainerMetadata,
			&c.EnrichedMetadata,
			&c.ItemCount,
			&c.SizeBytes,
			&c.ContainerClassification,
			&c.ContainerClassificationConfidence,
			&c.ContainerClassificationSource,
			&c.Created,
			&c.Updated,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan virtual container: %w", err)
		}
		containers = append(containers, &c)
	}

	return containers, nil
}

// ListVirtualItems retrieves all virtual items for a container
func (s *VirtualResourceService) ListVirtualItems(
	ctx context.Context,
	containerID string,
) ([]*models.ResourceItem, error) {
	query := `
		SELECT item_id, container_id, tenant_id, workspace_id, resource_uri,
		       protocol, scope, item_type, item_name, item_display_name,
		       item_path, data_type, is_nullable, is_primary_key, is_unique,
		       is_required, is_array, is_virtual, virtual_source, binding_mode,
		       reconciliation_status, reconciled_item_id, reconciliation_details,
		       reconciled_at, status, online, item_metadata, enriched_metadata,
		       is_privileged, created, updated
		FROM resource_items
		WHERE container_id = $1
		  AND is_virtual = true
		ORDER BY item_name
	`

	rows, err := s.db.Pool().Query(ctx, query, containerID)
	if err != nil {
		return nil, fmt.Errorf("failed to list virtual items: %w", err)
	}
	defer rows.Close()

	items := []*models.ResourceItem{}
	for rows.Next() {
		var item models.ResourceItem
		err := rows.Scan(
			&item.ItemID,
			&item.ContainerID,
			&item.TenantID,
			&item.WorkspaceID,
			&item.ResourceURI,
			&item.Protocol,
			&item.Scope,
			&item.ItemType,
			&item.ItemName,
			&item.ItemDisplayName,
			&item.ItemPath,
			&item.DataType,
			&item.IsNullable,
			&item.IsPrimaryKey,
			&item.IsUnique,
			&item.IsRequired,
			&item.IsArray,
			&item.IsVirtual,
			&item.VirtualSource,
			&item.BindingMode,
			&item.ReconciliationStatus,
			&item.ReconciledItemID,
			&item.ReconciliationDetails,
			&item.ReconciledAt,
			&item.Status,
			&item.Online,
			&item.ItemMetadata,
			&item.EnrichedMetadata,
			&item.IsPrivileged,
			&item.Created,
			&item.Updated,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan virtual item: %w", err)
		}
		items = append(items, &item)
	}

	return items, nil
}

// DeleteVirtualContainer deletes a virtual container and its items
func (s *VirtualResourceService) DeleteVirtualContainer(
	ctx context.Context,
	containerID string,
) error {
	// Items will be cascade deleted due to foreign key constraint
	result, err := s.db.Pool().Exec(ctx, `
		DELETE FROM resource_containers
		WHERE container_id = $1
		  AND is_virtual = true
	`, containerID)

	if err != nil {
		return fmt.Errorf("failed to delete virtual container: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("virtual container not found")
	}

	return nil
}

// DeleteOrphanedVirtualResources deletes all orphaned virtual resources in a workspace
func (s *VirtualResourceService) DeleteOrphanedVirtualResources(
	ctx context.Context,
	workspaceID string,
	olderThan *time.Duration,
) (int, error) {
	query := `
		DELETE FROM resource_containers
		WHERE workspace_id = $1
		  AND is_virtual = true
		  AND reconciliation_status = 'orphaned'
	`

	args := []interface{}{workspaceID}

	// Optionally filter by age
	if olderThan != nil {
		query += " AND updated < NOW() - $2::interval"
		args = append(args, olderThan.String())
	}

	result, err := s.db.Pool().Exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to delete orphaned virtual resources: %w", err)
	}

	return int(result.RowsAffected()), nil
}
