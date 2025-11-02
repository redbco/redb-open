package mapping

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redbco/redb-open/pkg/unifiedmodel/resource"
)

// CreateMappingRuleWithResourceURIs creates a new mapping rule using the new resource URI format
func (s *Service) CreateMappingRuleWithResourceURIs(
	ctx context.Context,
	tenantID, workspaceID, name, description string,
	sourceURI, targetURI string,
	transformationName string,
	transformationOptions map[string]interface{},
	metadata map[string]interface{},
	ownerID string,
) (*Rule, error) {
	s.logger.Infof("Creating mapping rule with resource URIs for tenant: %s, workspace: %s, name: %s", tenantID, workspaceID, name)

	// Parse and validate source URI
	sourceAddr, err := resource.ParseResourceURI(sourceURI)
	if err != nil {
		return nil, fmt.Errorf("invalid source URI: %w", err)
	}

	if err := resource.ValidateAddress(sourceAddr); err != nil {
		return nil, fmt.Errorf("source URI validation failed: %w", err)
	}

	// Parse and validate target URI
	targetAddr, err := resource.ParseResourceURI(targetURI)
	if err != nil {
		return nil, fmt.Errorf("invalid target URI: %w", err)
	}

	if err := resource.ValidateAddress(targetAddr); err != nil {
		return nil, fmt.Errorf("target URI validation failed: %w", err)
	}

	// Check compatibility
	compatReport, err := resource.CheckCompatibility(sourceAddr, targetAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to check compatibility: %w", err)
	}

	if !compatReport.Compatible {
		s.logger.Warnf("Source and target may be incompatible: %s", compatReport.Reason)
	}

	// Log warnings
	for _, warning := range compatReport.Warnings {
		s.logger.Warnf("Compatibility warning: %s", warning)
	}

	// Store URIs in metadata
	if metadata == nil {
		metadata = make(map[string]interface{})
	}

	// Store new format
	metadata["source_resource_uri"] = sourceURI
	metadata["target_resource_uri"] = targetURI

	// Store compatibility info
	if len(compatReport.Warnings) > 0 {
		metadata["compatibility_warnings"] = compatReport.Warnings
	}
	if len(compatReport.SuggestedTransformations) > 0 {
		metadata["suggested_transformations"] = compatReport.SuggestedTransformations
	}

	metadata["transformation_name"] = transformationName
	if transformationOptions != nil {
		metadata["transformation_options"] = transformationOptions
	}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Check if mapping rule already exists
	var exists bool
	err = s.db.Pool().QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM mapping_rules WHERE tenant_id = $1 AND workspace_id = $2 AND mapping_rule_name = $3)",
		tenantID, workspaceID, name).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check mapping rule existence: %w", err)
	}
	if exists {
		return nil, fmt.Errorf("mapping rule with name '%s' already exists", name)
	}

	// Insert the mapping rule
	query := `
		INSERT INTO mapping_rules (
			tenant_id, workspace_id, mapping_rule_name, mapping_rule_description,
			mapping_rule_metadata, mapping_rule_workflow_type, owner_id
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING mapping_rule_id, tenant_id, workspace_id, mapping_rule_name, mapping_rule_description,
			mapping_rule_metadata, mapping_rule_workflow_type, owner_id, created, updated
	`

	var rule Rule
	var metadataBytes []byte
	err = s.db.Pool().QueryRow(ctx, query,
		tenantID, workspaceID, name, description, metadataJSON, "simple", ownerID,
	).Scan(
		&rule.ID,
		&rule.TenantID,
		&rule.WorkspaceID,
		&rule.Name,
		&rule.Description,
		&metadataBytes,
		&rule.WorkflowType,
		&rule.OwnerID,
		&rule.Created,
		&rule.Updated,
	)
	if err != nil {
		s.logger.Errorf("Failed to create mapping rule: %v", err)
		return nil, err
	}

	// Parse metadata
	if len(metadataBytes) > 0 {
		if err := json.Unmarshal(metadataBytes, &rule.Metadata); err != nil {
			s.logger.Warnf("Failed to parse metadata: %v", err)
		}
	}

	s.logger.Infof("Successfully created mapping rule with resource URIs: %s", name)
	return &rule, nil
}

// GetResourceURIsFromRule extracts resource URIs from a mapping rule
func GetResourceURIsFromRule(rule *Rule) (sourceURI, targetURI string, ok bool) {
	if rule.Metadata == nil {
		return "", "", false
	}

	// Get new format URIs
	if sourceVal, exists := rule.Metadata["source_resource_uri"]; exists {
		if source, ok := sourceVal.(string); ok {
			sourceURI = source
		}
	}

	if targetVal, exists := rule.Metadata["target_resource_uri"]; exists {
		if target, ok := targetVal.(string); ok {
			targetURI = target
		}
	}

	return sourceURI, targetURI, (sourceURI != "" && targetURI != "")
}

// ValidateMappingRuleURIs validates the resource URIs in a mapping rule
func (s *Service) ValidateMappingRuleURIs(ctx context.Context, tenantID, workspaceID, ruleName string) (bool, []string, error) {
	// Get the mapping rule
	rule, err := s.GetMappingRuleByName(ctx, tenantID, workspaceID, ruleName)
	if err != nil {
		return false, nil, err
	}

	// Extract URIs
	sourceURI, targetURI, ok := GetResourceURIsFromRule(rule)
	if !ok {
		return false, []string{"no valid resource URIs found"}, nil
	}

	errors := []string{}

	// Parse and validate source
	sourceAddr, err := resource.ParseResourceURI(sourceURI)
	if err != nil {
		errors = append(errors, fmt.Sprintf("source URI parse error: %v", err))
	} else {
		if err := resource.ValidateAddress(sourceAddr); err != nil {
			errors = append(errors, fmt.Sprintf("source URI validation error: %v", err))
		}
	}

	// Parse and validate target
	targetAddr, err := resource.ParseResourceURI(targetURI)
	if err != nil {
		errors = append(errors, fmt.Sprintf("target URI parse error: %v", err))
	} else {
		if err := resource.ValidateAddress(targetAddr); err != nil {
			errors = append(errors, fmt.Sprintf("target URI validation error: %v", err))
		}
	}

	// Check compatibility if both are valid
	if sourceAddr != nil && targetAddr != nil {
		report, err := resource.CheckCompatibility(sourceAddr, targetAddr)
		if err != nil {
			errors = append(errors, fmt.Sprintf("compatibility check error: %v", err))
		} else {
			if !report.Compatible {
				errors = append(errors, fmt.Sprintf("incompatible: %s", report.Reason))
			}
			for _, warning := range report.Warnings {
				errors = append(errors, fmt.Sprintf("warning: %s", warning))
			}
		}
	}

	return len(errors) == 0, errors, nil
}

// UpdateMappingRuleResourceURIs updates the resource URIs for an existing mapping rule
func (s *Service) UpdateMappingRuleResourceURIs(
	ctx context.Context,
	tenantID, workspaceID, ruleName string,
	sourceURI, targetURI string,
) (*Rule, error) {
	s.logger.Infof("Updating resource URIs for mapping rule: %s", ruleName)

	// Get existing rule
	rule, err := s.GetMappingRuleByName(ctx, tenantID, workspaceID, ruleName)
	if err != nil {
		return nil, err
	}

	// Parse and validate new URIs
	sourceAddr, err := resource.ParseResourceURI(sourceURI)
	if err != nil {
		return nil, fmt.Errorf("invalid source URI: %w", err)
	}

	targetAddr, err := resource.ParseResourceURI(targetURI)
	if err != nil {
		return nil, fmt.Errorf("invalid target URI: %w", err)
	}

	if err := resource.ValidateAddress(sourceAddr); err != nil {
		return nil, fmt.Errorf("source URI validation failed: %w", err)
	}

	if err := resource.ValidateAddress(targetAddr); err != nil {
		return nil, fmt.Errorf("target URI validation failed: %w", err)
	}

	// Update metadata
	if rule.Metadata == nil {
		rule.Metadata = make(map[string]interface{})
	}

	rule.Metadata["source_resource_uri"] = sourceURI
	rule.Metadata["target_resource_uri"] = targetURI

	metadataJSON, err := json.Marshal(rule.Metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Update in database
	query := `
		UPDATE mapping_rules
		SET mapping_rule_metadata = $1, updated = CURRENT_TIMESTAMP
		WHERE mapping_rule_id = $2
		RETURNING mapping_rule_id, tenant_id, workspace_id, mapping_rule_name, mapping_rule_description,
			mapping_rule_metadata, mapping_rule_workflow_type, owner_id, created, updated
	`

	var updatedRule Rule
	var metadataBytes []byte
	err = s.db.Pool().QueryRow(ctx, query, metadataJSON, rule.ID).Scan(
		&updatedRule.ID,
		&updatedRule.TenantID,
		&updatedRule.WorkspaceID,
		&updatedRule.Name,
		&updatedRule.Description,
		&metadataBytes,
		&updatedRule.WorkflowType,
		&updatedRule.OwnerID,
		&updatedRule.Created,
		&updatedRule.Updated,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to update mapping rule: %w", err)
	}

	// Parse metadata
	if len(metadataBytes) > 0 {
		if err := json.Unmarshal(metadataBytes, &updatedRule.Metadata); err != nil {
			s.logger.Warnf("Failed to parse metadata: %v", err)
		}
	}

	s.logger.Infof("Successfully updated resource URIs for mapping rule: %s", ruleName)
	return &updatedRule, nil
}
