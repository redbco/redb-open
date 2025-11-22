package mapping

import (
	"context"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/models"
	"github.com/redbco/redb-open/pkg/unifiedmodel/resource"
)

// TemplateResolutionService handles resolution of template URIs to concrete resource URIs
type TemplateResolutionService struct {
	db *database.PostgreSQL
}

// NewTemplateResolutionService creates a new template resolution service
func NewTemplateResolutionService(db *database.PostgreSQL) *TemplateResolutionService {
	return &TemplateResolutionService{
		db: db,
	}
}

// ResolutionResult contains the result of template URI resolution
type ResolutionResult struct {
	OriginalURI  string
	ResolvedURI  string
	WasResolved  bool
	IsVirtual    bool
	IsTemplate   bool
	ErrorMessage string
}

// ResolveTemplateURIsInMapping resolves all template URIs in a mapping and its rules
func (s *TemplateResolutionService) ResolveTemplateURIsInMapping(
	ctx context.Context,
	mappingID string,
) (*MappingResolutionReport, error) {
	// Get the mapping
	var mID string
	var workspaceID string
	err := s.db.Pool().QueryRow(ctx, `
		SELECT mapping_id, workspace_id
		FROM mappings
		WHERE mapping_id = $1
	`, mappingID).Scan(&mID, &workspaceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get mapping: %w", err)
	}

	report := &MappingResolutionReport{
		MappingID:     mID,
		RulesResolved: 0,
		RulesTotal:    0,
		Results:       []ResolutionResult{},
	}

	// Get all mapping rules for this mapping
	rows, err := s.db.Pool().Query(ctx, `
		SELECT rule_id, source_item_uri, target_item_uri
		FROM mapping_rules
		WHERE mapping_id = $1
	`, mappingID)
	if err != nil {
		return nil, fmt.Errorf("failed to get mapping rules: %w", err)
	}
	defer rows.Close()

	type rule struct {
		RuleID        string
		SourceItemURI string
		TargetItemURI string
	}

	rules := []rule{}
	for rows.Next() {
		var r rule
		if err := rows.Scan(&r.RuleID, &r.SourceItemURI, &r.TargetItemURI); err != nil {
			return nil, fmt.Errorf("failed to scan rule: %w", err)
		}
		rules = append(rules, r)
	}

	report.RulesTotal = len(rules)

	// Resolve each rule
	for _, r := range rules {
		// Try to resolve source URI
		sourceResult := s.resolveURI(ctx, r.SourceItemURI)
		report.Results = append(report.Results, sourceResult)

		// Try to resolve target URI
		targetResult := s.resolveURI(ctx, r.TargetItemURI)
		report.Results = append(report.Results, targetResult)

		// If both resolved, update the rule
		if sourceResult.WasResolved || targetResult.WasResolved {
			newSourceURI := r.SourceItemURI
			newTargetURI := r.TargetItemURI

			if sourceResult.WasResolved {
				newSourceURI = sourceResult.ResolvedURI
			}
			if targetResult.WasResolved {
				newTargetURI = targetResult.ResolvedURI
			}

			// Update the rule
			_, err := s.db.Pool().Exec(ctx, `
				UPDATE mapping_rules
				SET source_item_uri = $2,
				    target_item_uri = $3,
				    updated = NOW()
				WHERE rule_id = $1
			`, r.RuleID, newSourceURI, newTargetURI)

			if err != nil {
				return nil, fmt.Errorf("failed to update rule %s: %w", r.RuleID, err)
			}

			report.RulesResolved++
		}
	}

	// If any rules were resolved, invalidate the mapping for re-validation
	if report.RulesResolved > 0 {
		_, err = s.db.Pool().Exec(ctx, `
			UPDATE mappings
			SET validated = false,
			    validated_at = NULL,
			    updated = NOW()
			WHERE mapping_id = $1
		`, mappingID)
		if err != nil {
			return nil, fmt.Errorf("failed to invalidate mapping: %w", err)
		}
	}

	return report, nil
}

// resolveURI attempts to resolve a single URI
func (s *TemplateResolutionService) resolveURI(ctx context.Context, uri string) ResolutionResult {
	result := ResolutionResult{
		OriginalURI: uri,
		ResolvedURI: uri,
		WasResolved: false,
	}

	// Check if it's a template URI
	if !strings.HasPrefix(uri, "template://") {
		result.IsTemplate = false
		// Check if it points to a virtual resource
		resolvedURI, isVirtual, err := s.getReconciledResourceURI(ctx, uri)
		if err == nil && isVirtual && resolvedURI != uri {
			result.ResolvedURI = resolvedURI
			result.WasResolved = true
			result.IsVirtual = true
		}
		return result
	}

	result.IsTemplate = true

	// Parse template URI to validate it
	_, err := resource.ParseTemplateURI(uri)
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("failed to parse template URI: %v", err)
		return result
	}

	// Find the virtual resource_item with this template URI
	var itemID, reconciledItemID string
	var reconciliationStatus string
	err = s.db.Pool().QueryRow(ctx, `
		SELECT item_id, reconciliation_status, reconciled_item_id
		FROM resource_items
		WHERE resource_uri = $1
		  AND is_virtual = true
	`, uri).Scan(&itemID, &reconciliationStatus, &reconciledItemID)

	if err != nil {
		// Virtual item not found - might not have been created yet
		result.ErrorMessage = "virtual item not found"
		return result
	}

	// Check if it's been reconciled
	if reconciliationStatus != models.ReconciliationStatusMatched {
		result.ErrorMessage = fmt.Sprintf("virtual item not reconciled (status: %s)", reconciliationStatus)
		return result
	}

	if reconciledItemID == "" {
		result.ErrorMessage = "virtual item marked as matched but no reconciled_item_id"
		return result
	}

	// Get the real resource URI
	var realResourceURI string
	err = s.db.Pool().QueryRow(ctx, `
		SELECT resource_uri
		FROM resource_items
		WHERE item_id = $1
		  AND is_virtual = false
	`, reconciledItemID).Scan(&realResourceURI)

	if err != nil {
		result.ErrorMessage = fmt.Sprintf("failed to get real resource URI: %v", err)
		return result
	}

	result.ResolvedURI = realResourceURI
	result.WasResolved = true
	return result
}

// getReconciledResourceURI gets the reconciled URI for a virtual resource URI (non-template)
func (s *TemplateResolutionService) getReconciledResourceURI(
	ctx context.Context,
	virtualURI string,
) (string, bool, error) {
	// Check if this URI belongs to a virtual resource that has been reconciled
	var reconciliationStatus string
	var reconciledItemID *string
	var isVirtual bool

	err := s.db.Pool().QueryRow(ctx, `
		SELECT is_virtual, reconciliation_status, reconciled_item_id
		FROM resource_items
		WHERE resource_uri = $1
	`, virtualURI).Scan(&isVirtual, &reconciliationStatus, &reconciledItemID)

	if err != nil {
		// Not a virtual resource
		return virtualURI, false, nil
	}

	if !isVirtual {
		return virtualURI, false, nil
	}

	// It's virtual - check if reconciled
	if reconciliationStatus != models.ReconciliationStatusMatched || reconciledItemID == nil {
		return virtualURI, true, nil
	}

	// Get the real URI
	var realURI string
	err = s.db.Pool().QueryRow(ctx, `
		SELECT resource_uri
		FROM resource_items
		WHERE item_id = $1
		  AND is_virtual = false
	`, *reconciledItemID).Scan(&realURI)

	if err != nil {
		return virtualURI, true, fmt.Errorf("failed to get real URI: %w", err)
	}

	return realURI, true, nil
}

// MappingResolutionReport contains the results of resolving a mapping
type MappingResolutionReport struct {
	MappingID     string
	RulesTotal    int
	RulesResolved int
	Results       []ResolutionResult
}

// ResolveAllMappingsInWorkspace resolves template URIs in all mappings for a workspace
func (s *TemplateResolutionService) ResolveAllMappingsInWorkspace(
	ctx context.Context,
	workspaceID string,
) (int, error) {
	// Find all mappings that have template URIs or point to virtual resources
	rows, err := s.db.Pool().Query(ctx, `
		SELECT DISTINCT m.mapping_id
		FROM mappings m
		JOIN mapping_rules mr ON m.mapping_id = mr.mapping_id
		WHERE m.workspace_id = $1
		  AND (
		    mr.source_item_uri LIKE 'template://%'
		    OR mr.target_item_uri LIKE 'template://%'
		    OR EXISTS (
		      SELECT 1 FROM resource_items ri
		      WHERE (ri.resource_uri = mr.source_item_uri OR ri.resource_uri = mr.target_item_uri)
		        AND ri.is_virtual = true
		        AND ri.reconciliation_status = 'matched'
		    )
		  )
	`, workspaceID)
	if err != nil {
		return 0, fmt.Errorf("failed to find mappings: %w", err)
	}
	defer rows.Close()

	mappingIDs := []string{}
	for rows.Next() {
		var mappingID string
		if err := rows.Scan(&mappingID); err != nil {
			return 0, fmt.Errorf("failed to scan mapping ID: %w", err)
		}
		mappingIDs = append(mappingIDs, mappingID)
	}

	resolvedCount := 0
	for _, mappingID := range mappingIDs {
		report, err := s.ResolveTemplateURIsInMapping(ctx, mappingID)
		if err != nil {
			// Log but continue
			continue
		}
		if report.RulesResolved > 0 {
			resolvedCount++
		}
	}

	return resolvedCount, nil
}
