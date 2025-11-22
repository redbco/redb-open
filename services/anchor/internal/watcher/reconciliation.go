package watcher

import (
	"context"
	"fmt"
	"strings"

	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/pkg/models"
)

// ReconciliationReport contains the results of a reconciliation operation
type ReconciliationReport struct {
	MatchedContainers  int
	MatchedItems       int
	ConflictedItems    int
	OrphanedContainers int
	OrphanedItems      int
	Conflicts          []ConflictDetail
}

// ConflictDetail describes a type mismatch between virtual and discovered resources
type ConflictDetail struct {
	VirtualItemID    string
	DiscoveredItemID string
	ItemName         string
	VirtualType      string
	DiscoveredType   string
	Suggestion       string // "widen_type", "user_resolve", "accept_discovered"
}

// reconcileVirtualResources matches virtual resources with newly discovered ones
func (w *SchemaWatcher) reconcileVirtualResources(
	ctx context.Context,
	databaseID string,
	discoveredContainers []*models.ResourceContainer,
	discoveredItems []*models.ResourceItem,
) error {
	w.logInfo("Starting reconciliation for database %s", databaseID)

	report := &ReconciliationReport{
		Conflicts: []ConflictDetail{},
	}

	// 1. Find virtual containers eligible for reconciliation with this database
	eligibleContainers, err := w.findEligibleVirtualContainers(ctx, databaseID)
	if err != nil {
		return fmt.Errorf("failed to find eligible virtual containers: %w", err)
	}

	if len(eligibleContainers) == 0 {
		w.logInfo("No eligible virtual containers found for database %s", databaseID)
		return nil
	}

	w.logInfo("Found %d eligible virtual containers for reconciliation", len(eligibleContainers))

	// 2. Match containers
	containerMatches := matchContainers(eligibleContainers, discoveredContainers)
	for _, match := range containerMatches {
		err := w.reconcileContainer(ctx, match.Virtual, match.Discovered)
		if err != nil {
			w.logWarn("Failed to reconcile container %s: %v", match.Virtual.ContainerID, err)
			continue
		}
		report.MatchedContainers++
	}

	// 3. Match items for each matched container
	for _, match := range containerMatches {
		// Get virtual items for this container
		virtualItems, err := w.getVirtualItems(ctx, match.Virtual.ContainerID)
		if err != nil {
			w.logWarn("Failed to get virtual items for container %s: %v", match.Virtual.ContainerID, err)
			continue
		}

		// Get discovered items for the matched container
		discoveredItemsForContainer := filterItemsByContainer(discoveredItems, match.Discovered.ContainerID)

		// Match items
		itemMatches, conflicts := matchItems(virtualItems, discoveredItemsForContainer)

		// Reconcile matched items
		for _, itemMatch := range itemMatches {
			err := w.reconcileItem(ctx, itemMatch.Virtual, itemMatch.Discovered)
			if err != nil {
				w.logWarn("Failed to reconcile item %s: %v", itemMatch.Virtual.ItemID, err)
				continue
			}
			report.MatchedItems++
		}

		// Record conflicts
		for _, conflict := range conflicts {
			err := w.markItemConflict(ctx, conflict)
			if err != nil {
				w.logWarn("Failed to mark item conflict: %v", err)
			}
			report.ConflictedItems++
			report.Conflicts = append(report.Conflicts, conflict)
		}

		// Mark orphaned items (virtual items that didn't match)
		matchedVirtualIDs := make(map[string]bool)
		for _, m := range itemMatches {
			matchedVirtualIDs[m.Virtual.ItemID] = true
		}
		for _, conflict := range conflicts {
			matchedVirtualIDs[conflict.VirtualItemID] = true
		}

		for _, vItem := range virtualItems {
			if !matchedVirtualIDs[vItem.ItemID] {
				err := w.markItemOrphaned(ctx, vItem.ItemID)
				if err != nil {
					w.logWarn("Failed to mark item as orphaned: %v", err)
				}
				report.OrphanedItems++
			}
		}
	}

	// 4. Mark unmatched virtual containers as orphaned
	matchedVirtualContainerIDs := make(map[string]bool)
	for _, m := range containerMatches {
		matchedVirtualContainerIDs[m.Virtual.ContainerID] = true
	}
	for _, vc := range eligibleContainers {
		if !matchedVirtualContainerIDs[vc.ContainerID] {
			err := w.markContainerOrphaned(ctx, vc.ContainerID)
			if err != nil {
				w.logWarn("Failed to mark container as orphaned: %v", err)
			}
			report.OrphanedContainers++
		}
	}

	w.logInfo("Reconciliation complete: matched %d containers, %d items; %d conflicts, %d orphans",
		report.MatchedContainers, report.MatchedItems, report.ConflictedItems,
		report.OrphanedContainers+report.OrphanedItems)

	// Trigger template URI resolution if any items were matched
	if report.MatchedItems > 0 && w.mappingClient != nil {
		// Get workspace ID from the first eligible container
		var workspaceID string
		if len(eligibleContainers) > 0 {
			workspaceID = eligibleContainers[0].WorkspaceID
		}

		if workspaceID != "" {
			w.logInfo("Triggering template URI resolution for workspace %s", workspaceID)

			resolveReq := &corev1.ResolveTemplateURIsRequest{
				WorkspaceId: workspaceID,
			}

			resolveResp, err := w.mappingClient.ResolveTemplateURIsInWorkspace(ctx, resolveReq)
			if err != nil {
				w.logWarn("Failed to trigger template URI resolution: %v", err)
			} else {
				w.logInfo("Template resolution complete: %d mappings resolved, %d rules updated",
					resolveResp.MappingsResolved, resolveResp.RulesResolved)
			}
		}
	}

	return nil
}

// findEligibleVirtualContainers finds virtual containers that should participate in reconciliation
func (w *SchemaWatcher) findEligibleVirtualContainers(ctx context.Context, databaseID string) ([]*models.ResourceContainer, error) {
	query := `
		SELECT container_id, tenant_id, workspace_id, resource_uri, protocol, scope,
		       object_type, object_name, is_virtual, virtual_source, virtual_namespace,
		       binding_mode, bound_database_id, reconciliation_status,
		       created, updated
		FROM resource_containers
		WHERE is_virtual = true
		  AND (
		    (binding_mode = 'bound' AND bound_database_id = $1)
		    OR binding_mode = 'auto_bind'
		  )
		  AND reconciliation_status IN ('pending', 'orphaned')
	`

	rows, err := w.db.Pool().Query(ctx, query, databaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to query eligible virtual containers: %w", err)
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
			&c.IsVirtual,
			&c.VirtualSource,
			&c.VirtualNamespace,
			&c.BindingMode,
			&c.BoundDatabaseID,
			&c.ReconciliationStatus,
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

// ContainerMatch represents a matched virtual and discovered container
type ContainerMatch struct {
	Virtual    *models.ResourceContainer
	Discovered *models.ResourceContainer
}

// matchContainers finds matches between virtual and discovered containers
func matchContainers(
	virtualContainers []*models.ResourceContainer,
	discoveredContainers []*models.ResourceContainer,
) []ContainerMatch {
	matches := []ContainerMatch{}

	for _, discovered := range discoveredContainers {
		for _, virtual := range virtualContainers {
			// Match by object_name and object_type
			if virtual.ObjectName == discovered.ObjectName &&
				virtual.ObjectType == discovered.ObjectType {
				matches = append(matches, ContainerMatch{
					Virtual:    virtual,
					Discovered: discovered,
				})
				break // Only match once
			}
		}
	}

	return matches
}

// reconcileContainer updates the virtual container to point to the discovered one
func (w *SchemaWatcher) reconcileContainer(
	ctx context.Context,
	virtual *models.ResourceContainer,
	discovered *models.ResourceContainer,
) error {
	query := `
		UPDATE resource_containers
		SET reconciliation_status = 'matched',
		    reconciled_container_id = $2,
		    reconciled_at = NOW(),
		    updated = NOW()
		WHERE container_id = $1
	`

	_, err := w.db.Pool().Exec(ctx, query, virtual.ContainerID, discovered.ContainerID)
	if err != nil {
		return fmt.Errorf("failed to update virtual container reconciliation: %w", err)
	}

	w.logInfo("Reconciled virtual container %s with discovered container %s",
		virtual.ObjectName, discovered.ObjectName)

	return nil
}

// getVirtualItems retrieves virtual items for a container
func (w *SchemaWatcher) getVirtualItems(ctx context.Context, containerID string) ([]*models.ResourceItem, error) {
	query := `
		SELECT item_id, container_id, tenant_id, workspace_id, resource_uri,
		       protocol, scope, item_type, item_name, data_type,
		       is_virtual, virtual_source, reconciliation_status,
		       created, updated
		FROM resource_items
		WHERE container_id = $1
		  AND is_virtual = true
		  AND reconciliation_status IN ('pending', 'orphaned', 'conflict')
	`

	rows, err := w.db.Pool().Query(ctx, query, containerID)
	if err != nil {
		return nil, fmt.Errorf("failed to query virtual items: %w", err)
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
			&item.DataType,
			&item.IsVirtual,
			&item.VirtualSource,
			&item.ReconciliationStatus,
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

// filterItemsByContainer filters items to only those belonging to a specific container
func filterItemsByContainer(items []*models.ResourceItem, containerID string) []*models.ResourceItem {
	filtered := []*models.ResourceItem{}
	for _, item := range items {
		if item.ContainerID == containerID {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

// ItemMatch represents a matched virtual and discovered item
type ItemMatch struct {
	Virtual    *models.ResourceItem
	Discovered *models.ResourceItem
}

// matchItems finds matches between virtual and discovered items
func matchItems(
	virtualItems []*models.ResourceItem,
	discoveredItems []*models.ResourceItem,
) ([]ItemMatch, []ConflictDetail) {
	matches := []ItemMatch{}
	conflicts := []ConflictDetail{}

	for _, virtual := range virtualItems {
		for _, discovered := range discoveredItems {
			// Match by item_name
			if virtual.ItemName == discovered.ItemName {
				// Check type compatibility
				compatible, suggestion := checkTypeCompatibility(virtual.DataType, discovered.DataType)

				if compatible {
					matches = append(matches, ItemMatch{
						Virtual:    virtual,
						Discovered: discovered,
					})
				} else {
					conflicts = append(conflicts, ConflictDetail{
						VirtualItemID:    virtual.ItemID,
						DiscoveredItemID: discovered.ItemID,
						ItemName:         virtual.ItemName,
						VirtualType:      virtual.DataType,
						DiscoveredType:   discovered.DataType,
						Suggestion:       suggestion,
					})
				}
				break
			}
		}
	}

	return matches, conflicts
}

// checkTypeCompatibility determines if two data types are compatible
func checkTypeCompatibility(virtualType, discoveredType string) (compatible bool, suggestion string) {
	// Normalize types for comparison
	vType := strings.ToLower(strings.TrimSpace(virtualType))
	dType := strings.ToLower(strings.TrimSpace(discoveredType))

	// Exact match
	if vType == dType {
		return true, "exact_match"
	}

	// Safe numeric widening
	numericWidenings := map[string][]string{
		"int16":    {"int32", "int64", "integer", "bigint"},
		"int32":    {"int64", "bigint"},
		"integer":  {"bigint", "int64"},
		"smallint": {"integer", "int", "bigint", "int32", "int64"},
		"float":    {"double", "float64", "double precision"},
		"float32":  {"float64", "double", "double precision"},
		"real":     {"double precision", "double"},
	}

	if widenings, ok := numericWidenings[vType]; ok {
		for _, wider := range widenings {
			if strings.Contains(dType, wider) {
				return true, "widen_type"
			}
		}
	}

	// String length widening
	if strings.HasPrefix(vType, "varchar") && strings.HasPrefix(dType, "varchar") {
		return true, "widen_varchar"
	}
	if strings.HasPrefix(vType, "char") && strings.HasPrefix(dType, "varchar") {
		return true, "widen_to_varchar"
	}

	// Text types compatibility
	textTypes := []string{"text", "varchar", "char", "string"}
	vIsText := false
	dIsText := false
	for _, tt := range textTypes {
		if strings.Contains(vType, tt) {
			vIsText = true
		}
		if strings.Contains(dType, tt) {
			dIsText = true
		}
	}
	if vIsText && dIsText {
		return true, "text_compatible"
	}

	// Incompatible types
	return false, "user_resolve"
}

// reconcileItem updates the virtual item to point to the discovered one
func (w *SchemaWatcher) reconcileItem(
	ctx context.Context,
	virtual *models.ResourceItem,
	discovered *models.ResourceItem,
) error {
	query := `
		UPDATE resource_items
		SET reconciliation_status = 'matched',
		    reconciled_item_id = $2,
		    reconciled_at = NOW(),
		    updated = NOW()
		WHERE item_id = $1
	`

	_, err := w.db.Pool().Exec(ctx, query, virtual.ItemID, discovered.ItemID)
	if err != nil {
		return fmt.Errorf("failed to update virtual item reconciliation: %w", err)
	}

	w.logDebug("Reconciled virtual item %s with discovered item %s",
		virtual.ItemName, discovered.ItemName)

	return nil
}

// markItemConflict marks an item as having a type conflict
func (w *SchemaWatcher) markItemConflict(ctx context.Context, conflict ConflictDetail) error {
	reconciliationDetails := map[string]interface{}{
		"virtual_type":    conflict.VirtualType,
		"discovered_type": conflict.DiscoveredType,
		"suggestion":      conflict.Suggestion,
		"item_name":       conflict.ItemName,
	}

	query := `
		UPDATE resource_items
		SET reconciliation_status = 'conflict',
		    reconciliation_details = $2,
		    updated = NOW()
		WHERE item_id = $1
	`

	_, err := w.db.Pool().Exec(ctx, query, conflict.VirtualItemID, reconciliationDetails)
	if err != nil {
		return fmt.Errorf("failed to mark item conflict: %w", err)
	}

	w.logWarn("Type conflict for item %s: virtual=%s, discovered=%s",
		conflict.ItemName, conflict.VirtualType, conflict.DiscoveredType)

	return nil
}

// markItemOrphaned marks a virtual item as orphaned (no match found)
func (w *SchemaWatcher) markItemOrphaned(ctx context.Context, itemID string) error {
	query := `
		UPDATE resource_items
		SET reconciliation_status = 'orphaned',
		    updated = NOW()
		WHERE item_id = $1
	`

	_, err := w.db.Pool().Exec(ctx, query, itemID)
	if err != nil {
		return fmt.Errorf("failed to mark item as orphaned: %w", err)
	}

	return nil
}

// markContainerOrphaned marks a virtual container as orphaned (no match found)
func (w *SchemaWatcher) markContainerOrphaned(ctx context.Context, containerID string) error {
	query := `
		UPDATE resource_containers
		SET reconciliation_status = 'orphaned',
		    updated = NOW()
		WHERE container_id = $1
	`

	_, err := w.db.Pool().Exec(ctx, query, containerID)
	if err != nil {
		return fmt.Errorf("failed to mark container as orphaned: %w", err)
	}

	return nil
}
