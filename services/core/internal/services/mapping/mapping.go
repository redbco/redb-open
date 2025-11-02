package mapping

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// Service handles mapping-related operations
type Service struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewService creates a new mapping service
func NewService(db *database.PostgreSQL, logger *logger.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// Mapping represents a mapping in the system
type Mapping struct {
	ID                 string
	TenantID           string
	WorkspaceID        string
	Name               string
	Description        string
	MappingType        string
	PolicyIDs          []string
	OwnerID            string
	Validated          bool
	ValidatedAt        *time.Time
	ValidationErrors   []string
	ValidationWarnings []string
	Created            time.Time
	Updated            time.Time
	MappingRuleCount   int32
}

// Rule represents a mapping rule in the system
type Rule struct {
	ID           string
	TenantID     string
	WorkspaceID  string
	Name         string
	Description  string
	Metadata     map[string]interface{}
	WorkflowType string // 'simple' or 'dag'
	OwnerID      string
	Created      time.Time
	Updated      time.Time
	MappingCount int32
}

// Create creates a new mapping
func (s *Service) Create(ctx context.Context, tenantID, workspaceID, mappingType, name, description, ownerID string) (*Mapping, error) {
	s.logger.Infof("Creating mapping in database for tenant: %s, workspace: %s, name: %s", tenantID, workspaceID, name)

	// Check if the tenant exists
	var tenantExists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)", tenantID).Scan(&tenantExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check tenant existence: %w", err)
	}
	if !tenantExists {
		return nil, errors.New("tenant not found")
	}

	// Check if the workspace exists and belongs to the tenant
	var workspaceExists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM workspaces WHERE workspace_id = $1 AND tenant_id = $2)", workspaceID, tenantID).Scan(&workspaceExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check workspace existence: %w", err)
	}
	if !workspaceExists {
		return nil, errors.New("workspace not found in tenant")
	}

	// Check if mapping with the same name already exists in this workspace
	var exists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM mappings WHERE tenant_id = $1 AND workspace_id = $2 AND mapping_name = $3)", tenantID, workspaceID, name).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check mapping existence: %w", err)
	}
	if exists {
		return nil, errors.New("mapping with this name already exists in the workspace")
	}

	// Insert the mapping into the database
	query := `
		INSERT INTO mappings (tenant_id, workspace_id, mapping_name, mapping_description, mapping_type, owner_id)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING mapping_id, tenant_id, workspace_id, mapping_name, mapping_description, mapping_type, COALESCE(policy_ids, '{}') as policy_ids, owner_id, created, updated
	`

	var mapping Mapping
	err = s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, name, description, mappingType, ownerID).Scan(
		&mapping.ID,
		&mapping.TenantID,
		&mapping.WorkspaceID,
		&mapping.Name,
		&mapping.Description,
		&mapping.MappingType,
		&mapping.PolicyIDs,
		&mapping.OwnerID,
		&mapping.Created,
		&mapping.Updated,
	)
	if err != nil {
		s.logger.Errorf("Failed to create mapping: %v", err)
		return nil, err
	}

	return &mapping, nil
}

// Get retrieves a mapping by name (since the API uses mapping_name)
func (s *Service) Get(ctx context.Context, tenantID, workspaceID, mappingName string) (*Mapping, error) {
	s.logger.Infof("Retrieving mapping from database with name: %s", mappingName)
	query := `
		SELECT mapping_id, tenant_id, workspace_id, mapping_name, mapping_description, mapping_type,
		       COALESCE(policy_ids, '{}') as policy_ids, owner_id, validated, validated_at,
		       validation_errors, validation_warnings, created, updated
		FROM mappings
		WHERE tenant_id = $1 AND workspace_id = $2 AND mapping_name = $3
	`

	var mapping Mapping
	var validationErrorsJSON, validationWarningsJSON []byte
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, mappingName).Scan(
		&mapping.ID,
		&mapping.TenantID,
		&mapping.WorkspaceID,
		&mapping.Name,
		&mapping.Description,
		&mapping.MappingType,
		&mapping.PolicyIDs,
		&mapping.OwnerID,
		&mapping.Validated,
		&mapping.ValidatedAt,
		&validationErrorsJSON,
		&validationWarningsJSON,
		&mapping.Created,
		&mapping.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("mapping not found")
		}
		s.logger.Errorf("Failed to get mapping: %v", err)
		return nil, err
	}

	// Unmarshal JSON fields
	if len(validationErrorsJSON) > 0 {
		if err := json.Unmarshal(validationErrorsJSON, &mapping.ValidationErrors); err != nil {
			s.logger.Warnf("Failed to unmarshal validation errors: %v", err)
		}
	}
	if len(validationWarningsJSON) > 0 {
		if err := json.Unmarshal(validationWarningsJSON, &mapping.ValidationWarnings); err != nil {
			s.logger.Warnf("Failed to unmarshal validation warnings: %v", err)
		}
	}

	// Get mapping rule count
	mappingRuleCount, err := s.GetMappingRuleCount(ctx, mapping.ID)
	if err != nil {
		s.logger.Warnf("Failed to get mapping rule count for mapping %s: %v", mappingName, err)
		mappingRuleCount = 0
	}
	mapping.MappingRuleCount = mappingRuleCount

	return &mapping, nil
}

// List retrieves all mappings for a workspace
func (s *Service) List(ctx context.Context, tenantID, workspaceID string) ([]*Mapping, error) {
	s.logger.Infof("Listing mappings for tenant: %s, workspace: %s", tenantID, workspaceID)
	query := `
		SELECT m.mapping_id, m.tenant_id, m.workspace_id, m.mapping_name, m.mapping_description, m.mapping_type,
		       COALESCE(m.policy_ids, '{}') as policy_ids, m.owner_id, m.validated, m.validated_at,
		       m.validation_errors, m.validation_warnings, m.created, m.updated,
		       COALESCE(COUNT(mrm.mapping_rule_id), 0) as mapping_rule_count
		FROM mappings m
		LEFT JOIN mapping_rule_mappings mrm ON m.mapping_id = mrm.mapping_id
		WHERE m.tenant_id = $1 AND m.workspace_id = $2
		GROUP BY m.mapping_id, m.tenant_id, m.workspace_id, m.mapping_name, m.mapping_description, m.mapping_type,
		         m.policy_ids, m.owner_id, m.validated, m.validated_at, m.validation_errors, m.validation_warnings,
		         m.created, m.updated
		ORDER BY m.mapping_name
	`

	rows, err := s.db.Pool().Query(ctx, query, tenantID, workspaceID)
	if err != nil {
		s.logger.Errorf("Failed to list mappings: %v", err)
		return nil, err
	}
	defer rows.Close()

	var mappings []*Mapping
	for rows.Next() {
		var mapping Mapping
		var validationErrorsJSON, validationWarningsJSON []byte
		err := rows.Scan(
			&mapping.ID,
			&mapping.TenantID,
			&mapping.WorkspaceID,
			&mapping.Name,
			&mapping.Description,
			&mapping.MappingType,
			&mapping.PolicyIDs,
			&mapping.OwnerID,
			&mapping.Validated,
			&mapping.ValidatedAt,
			&validationErrorsJSON,
			&validationWarningsJSON,
			&mapping.Created,
			&mapping.Updated,
			&mapping.MappingRuleCount,
		)
		if err != nil {
			s.logger.Errorf("Failed to scan mapping: %v", err)
			return nil, err
		}

		// Unmarshal JSON fields
		if len(validationErrorsJSON) > 0 {
			if err := json.Unmarshal(validationErrorsJSON, &mapping.ValidationErrors); err != nil {
				s.logger.Warnf("Failed to unmarshal validation errors: %v", err)
			}
		}
		if len(validationWarningsJSON) > 0 {
			if err := json.Unmarshal(validationWarningsJSON, &mapping.ValidationWarnings); err != nil {
				s.logger.Warnf("Failed to unmarshal validation warnings: %v", err)
			}
		}

		mappings = append(mappings, &mapping)
	}

	if err = rows.Err(); err != nil {
		s.logger.Errorf("Error after scanning mappings: %v", err)
		return nil, err
	}

	return mappings, nil
}

// GetMappingRuleCount returns the number of mapping rules attached to a mapping
func (s *Service) GetMappingRuleCount(ctx context.Context, mappingID string) (int32, error) {
	var count int32
	err := s.db.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM mapping_rule_mappings WHERE mapping_id = $1", mappingID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get mapping rule count: %w", err)
	}
	return count, nil
}

// GetMappingCount returns the number of mappings that use a mapping rule
func (s *Service) GetMappingCount(ctx context.Context, mappingRuleID string) (int32, error) {
	var count int32
	err := s.db.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM mapping_rule_mappings WHERE mapping_rule_id = $1", mappingRuleID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get mapping count: %w", err)
	}
	return count, nil
}

// GetMappingsForRule retrieves all mappings that use a specific mapping rule
func (s *Service) GetMappingsForRule(ctx context.Context, tenantID, workspaceID, mappingRuleName string) ([]*Mapping, error) {
	s.logger.Infof("Retrieving mappings for mapping rule: %s", mappingRuleName)

	// Check if mapping rule exists
	rule, err := s.GetMappingRuleByName(ctx, tenantID, workspaceID, mappingRuleName)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT m.mapping_id, m.tenant_id, m.workspace_id, m.mapping_name, m.mapping_description, 
		       m.mapping_type, COALESCE(m.policy_ids, '{}') as policy_ids, m.owner_id, m.created, m.updated
		FROM mappings m
		INNER JOIN mapping_rule_mappings mrm ON m.mapping_id = mrm.mapping_id
		WHERE m.tenant_id = $1 AND m.workspace_id = $2 AND mrm.mapping_rule_id = $3
		ORDER BY m.mapping_name
	`

	rows, err := s.db.Pool().Query(ctx, query, tenantID, workspaceID, rule.ID)
	if err != nil {
		s.logger.Errorf("Failed to get mappings for mapping rule: %v", err)
		return nil, err
	}
	defer rows.Close()

	var mappings []*Mapping
	for rows.Next() {
		var mapping Mapping
		err := rows.Scan(
			&mapping.ID,
			&mapping.TenantID,
			&mapping.WorkspaceID,
			&mapping.Name,
			&mapping.Description,
			&mapping.MappingType,
			&mapping.PolicyIDs,
			&mapping.OwnerID,
			&mapping.Created,
			&mapping.Updated,
		)
		if err != nil {
			s.logger.Errorf("Failed to scan mapping: %v", err)
			return nil, err
		}
		mappings = append(mappings, &mapping)
	}

	if err = rows.Err(); err != nil {
		s.logger.Errorf("Error after scanning mappings: %v", err)
		return nil, err
	}

	return mappings, nil
}

// Update updates a mapping
func (s *Service) Update(ctx context.Context, tenantID, workspaceID, mappingName string, updates map[string]interface{}) (*Mapping, error) {
	s.logger.Infof("Updating mapping with name: %s", mappingName)

	if len(updates) == 0 {
		return s.Get(ctx, tenantID, workspaceID, mappingName)
	}

	// Check if mapping exists
	_, err := s.Get(ctx, tenantID, workspaceID, mappingName)
	if err != nil {
		return nil, err
	}

	// Build dynamic update query
	setParts := []string{}
	args := []interface{}{tenantID, workspaceID, mappingName}
	argIndex := 4

	for field, value := range updates {
		switch field {
		case "mapping_name", "mapping_description", "mapping_type", "policy_ids":
			setParts = append(setParts, fmt.Sprintf("%s = $%d", field, argIndex))
			args = append(args, value)
			argIndex++
		default:
			s.logger.Warnf("Ignoring invalid update field: %s", field)
		}
	}

	if len(setParts) == 0 {
		return s.Get(ctx, tenantID, workspaceID, mappingName)
	}

	// Add updated timestamp
	setParts = append(setParts, "updated = CURRENT_TIMESTAMP")

	setClause := setParts[0]
	for i := 1; i < len(setParts); i++ {
		setClause += ", " + setParts[i]
	}

	query := fmt.Sprintf(`
		UPDATE mappings 
		SET %s
		WHERE tenant_id = $1 AND workspace_id = $2 AND mapping_name = $3
		RETURNING mapping_id, tenant_id, workspace_id, mapping_name, mapping_description, mapping_type,
		          COALESCE(policy_ids, '{}') as policy_ids, owner_id, created, updated
	`, setClause)

	var mapping Mapping
	err = s.db.Pool().QueryRow(ctx, query, args...).Scan(
		&mapping.ID,
		&mapping.TenantID,
		&mapping.WorkspaceID,
		&mapping.Name,
		&mapping.Description,
		&mapping.MappingType,
		&mapping.PolicyIDs,
		&mapping.OwnerID,
		&mapping.Created,
		&mapping.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("mapping not found")
		}
		s.logger.Errorf("Failed to update mapping: %v", err)
		return nil, err
	}

	return &mapping, nil
}

// Delete deletes a mapping and optionally deletes associated mapping rules
func (s *Service) Delete(ctx context.Context, tenantID, workspaceID, mappingName string, keepRules bool) error {
	s.logger.Infof("Deleting mapping with name: %s (keepRules=%v)", mappingName, keepRules)

	// Check if mapping exists
	mapping, err := s.Get(ctx, tenantID, workspaceID, mappingName)
	if err != nil {
		return err
	}

	// Check for existing relationships using this mapping
	var relationshipCount int
	err = s.db.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM relationships WHERE tenant_id = $1 AND workspace_id = $2 AND mapping_id = $3",
		tenantID, workspaceID, mapping.ID).Scan(&relationshipCount)
	if err != nil {
		return fmt.Errorf("failed to check for existing relationships: %w", err)
	}
	if relationshipCount > 0 {
		return errors.New("cannot delete mapping that is being used by relationships")
	}

	// Check for existing MCP resources using this mapping
	var mcpResourceCount int
	err = s.db.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM mcpresources WHERE tenant_id = $1 AND workspace_id = $2 AND mapping_id = $3",
		tenantID, workspaceID, mapping.ID).Scan(&mcpResourceCount)
	if err != nil {
		return fmt.Errorf("failed to check for existing MCP resources: %w", err)
	}
	if mcpResourceCount > 0 {
		return errors.New("cannot delete mapping that is being used by MCP resources")
	}

	// If keepRules is false, get all rule IDs attached to this mapping before deletion
	var ruleIDsToCheck []string
	if !keepRules {
		query := `
			SELECT mapping_rule_id 
			FROM mapping_rule_mappings 
			WHERE mapping_id = $1
		`
		rows, err := s.db.Pool().Query(ctx, query, mapping.ID)
		if err != nil {
			return fmt.Errorf("failed to query mapping rules: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var ruleID string
			if err := rows.Scan(&ruleID); err != nil {
				return fmt.Errorf("failed to scan rule ID: %w", err)
			}
			ruleIDsToCheck = append(ruleIDsToCheck, ruleID)
		}

		if err = rows.Err(); err != nil {
			return fmt.Errorf("error iterating rule IDs: %w", err)
		}
	}

	// Delete mapping (this will cascade delete mapping_rule_mappings entries)
	result, err := s.db.Pool().Exec(ctx, "DELETE FROM mappings WHERE tenant_id = $1 AND workspace_id = $2 AND mapping_name = $3",
		tenantID, workspaceID, mappingName)
	if err != nil {
		return fmt.Errorf("failed to delete mapping: %w", err)
	}

	if result.RowsAffected() == 0 {
		return errors.New("mapping not found")
	}

	// If keepRules is false, delete rules that are no longer attached to any mapping
	if !keepRules && len(ruleIDsToCheck) > 0 {
		for _, ruleID := range ruleIDsToCheck {
			// Check if this rule is still attached to any mappings
			var attachmentCount int
			err = s.db.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM mapping_rule_mappings WHERE mapping_rule_id = $1", ruleID).Scan(&attachmentCount)
			if err != nil {
				s.logger.Warnf("Failed to check attachments for rule %s: %v", ruleID, err)
				continue
			}

			// If no remaining attachments, delete the rule
			if attachmentCount == 0 {
				_, err = s.db.Pool().Exec(ctx, "DELETE FROM mapping_rules WHERE mapping_rule_id = $1", ruleID)
				if err != nil {
					s.logger.Warnf("Failed to delete orphaned rule %s: %v", ruleID, err)
				} else {
					s.logger.Infof("Deleted orphaned mapping rule: %s", ruleID)
				}
			}
		}
	}

	return nil
}

// AttachMappingRule attaches a mapping rule to a mapping
func (s *Service) AttachMappingRule(ctx context.Context, tenantID, workspaceID, mappingName, ruleName string, order *int64) error {
	s.logger.Infof("Attaching mapping rule with name: %s to mapping with name: %s", ruleName, mappingName)

	// Check if mapping exists
	mapping, err := s.Get(ctx, tenantID, workspaceID, mappingName)
	if err != nil {
		return err
	}

	// Check if mapping rule exists
	rule, err := s.GetMappingRuleByName(ctx, tenantID, workspaceID, ruleName)
	if err != nil {
		return err
	}

	// Check if mapping rule is already attached to the mapping
	var attached bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM mapping_rule_mappings WHERE mapping_id = $1 AND mapping_rule_id = $2)",
		mapping.ID, rule.ID).Scan(&attached)
	if err != nil {
		return fmt.Errorf("failed to check if mapping rule is already attached: %w", err)
	}
	if attached {
		return errors.New("mapping rule is already attached to the mapping")
	}

	// Determine the order value
	orderValue := int64(0)
	if order != nil {
		orderValue = *order
	} else {
		// Auto-assign order as MAX(order) + 1
		err = s.db.Pool().QueryRow(ctx, "SELECT COALESCE(MAX(mapping_rule_order), -1) + 1 FROM mapping_rule_mappings WHERE mapping_id = $1", mapping.ID).Scan(&orderValue)
		if err != nil {
			return fmt.Errorf("failed to get next order value: %w", err)
		}
	}

	// Insert the mapping rule into the mapping_rule_mappings table
	query := `
		INSERT INTO mapping_rule_mappings (mapping_rule_id, mapping_id, mapping_rule_order)
		VALUES ($1, $2, $3)
	`

	_, err = s.db.Pool().Exec(ctx, query, rule.ID, mapping.ID, orderValue)
	if err != nil {
		return fmt.Errorf("failed to attach mapping rule: %w", err)
	}

	return nil
}

// DetachMappingRule detaches a mapping rule from a mapping
func (s *Service) DetachMappingRule(ctx context.Context, tenantID, workspaceID, mappingName, ruleName string) error {
	s.logger.Infof("Detaching mapping rule with name: %s from mapping with name: %s", ruleName, mappingName)

	// Check if mapping exists
	mapping, err := s.Get(ctx, tenantID, workspaceID, mappingName)
	if err != nil {
		return err
	}

	// Check if mapping rule exists
	rule, err := s.GetMappingRuleByName(ctx, tenantID, workspaceID, ruleName)
	if err != nil {
		return err
	}

	// Check if mapping rule is attached to the mapping
	var attached bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM mapping_rule_mappings WHERE mapping_id = $1 AND mapping_rule_id = $2)",
		mapping.ID, rule.ID).Scan(&attached)
	if err != nil {
		return fmt.Errorf("failed to check if mapping rule is attached: %w", err)
	}
	if !attached {
		return errors.New("mapping rule is not attached to the mapping")
	}

	// Delete the mapping rule from the mapping_rule_mappings table
	query := `
		DELETE FROM mapping_rule_mappings WHERE mapping_id = $1 AND mapping_rule_id = $2
	`

	_, err = s.db.Pool().Exec(ctx, query, mapping.ID, rule.ID)
	if err != nil {
		return fmt.Errorf("failed to detach mapping rule: %w", err)
	}

	// After detaching, check if this rule is still attached to any other mappings
	var remainingAttachments int
	err = s.db.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM mapping_rule_mappings WHERE mapping_rule_id = $1", rule.ID).Scan(&remainingAttachments)
	if err != nil {
		return fmt.Errorf("failed to check remaining attachments: %w", err)
	}

	// If the rule is not attached to any other mappings, delete it completely
	if remainingAttachments == 0 {
		s.logger.Infof("Mapping rule %s has no remaining attachments, deleting completely", ruleName)
		_, err = s.db.Pool().Exec(ctx, "DELETE FROM mapping_rules WHERE mapping_rule_id = $1", rule.ID)
		if err != nil {
			return fmt.Errorf("failed to delete orphaned mapping rule: %w", err)
		}
	}

	return nil
}

// ListMappingRules retrieves all mapping rules for a workspace
func (s *Service) ListMappingRules(ctx context.Context, tenantID, workspaceID string) ([]*Rule, error) {
	s.logger.Infof("Listing mapping rules for tenant: %s, workspace: %s", tenantID, workspaceID)

	query := `
		SELECT mr.mapping_rule_id, mr.tenant_id, mr.workspace_id, mr.mapping_rule_name, mr.mapping_rule_description, 
			mr.mapping_rule_metadata, mr.mapping_rule_workflow_type, mr.owner_id, mr.created, mr.updated,
			COALESCE(COUNT(mrm.mapping_id), 0) as mapping_count
		FROM mapping_rules mr
		LEFT JOIN mapping_rule_mappings mrm ON mr.mapping_rule_id = mrm.mapping_rule_id
		WHERE mr.tenant_id = $1 AND mr.workspace_id = $2
		GROUP BY mr.mapping_rule_id, mr.tenant_id, mr.workspace_id, mr.mapping_rule_name, mr.mapping_rule_description, 
		         mr.mapping_rule_metadata, mr.mapping_rule_workflow_type, mr.owner_id, mr.created, mr.updated
		ORDER BY mr.mapping_rule_name
	`

	rows, err := s.db.Pool().Query(ctx, query, tenantID, workspaceID)
	if err != nil {
		s.logger.Errorf("Failed to list mapping rules: %v", err)
		return nil, err
	}
	defer rows.Close()

	var rules []*Rule
	for rows.Next() {
		var rule Rule
		var metadataBytes []byte
		err := rows.Scan(
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
			&rule.MappingCount,
		)
		if err != nil {
			s.logger.Errorf("Failed to scan mapping rule: %v", err)
			return nil, err
		}

		// Parse metadata
		if len(metadataBytes) > 0 {
			if err := json.Unmarshal(metadataBytes, &rule.Metadata); err != nil {
				s.logger.Warnf("Failed to parse metadata: %v", err)
			}
		}

		rules = append(rules, &rule)
	}

	if err = rows.Err(); err != nil {
		s.logger.Errorf("Error after scanning mapping rules: %v", err)
		return nil, err
	}

	return rules, nil
}

// GetMappingRuleByName retrieves a mapping rule by name
func (s *Service) GetMappingRuleByName(ctx context.Context, tenantID, workspaceID, name string) (*Rule, error) {
	s.logger.Infof("Retrieving mapping rule with name: %s", name)

	query := `
		SELECT mapping_rule_id, tenant_id, workspace_id, mapping_rule_name, mapping_rule_description, 
			mapping_rule_metadata, mapping_rule_workflow_type, owner_id, created, updated
		FROM mapping_rules
		WHERE tenant_id = $1 AND workspace_id = $2 AND mapping_rule_name = $3
	`

	var rule Rule
	var metadataBytes []byte
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, name).Scan(
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
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("mapping rule not found")
		}
		s.logger.Errorf("Failed to get mapping rule: %v", err)
		return nil, err
	}

	// Parse metadata
	if len(metadataBytes) > 0 {
		if err := json.Unmarshal(metadataBytes, &rule.Metadata); err != nil {
			s.logger.Warnf("Failed to parse metadata: %v", err)
		}
	}

	// Get mapping count
	mappingCount, err := s.GetMappingCount(ctx, rule.ID)
	if err != nil {
		s.logger.Warnf("Failed to get mapping count for mapping rule %s: %v", name, err)
		mappingCount = 0
	}
	rule.MappingCount = mappingCount

	return &rule, nil
}

// CreateMappingRule creates a new mapping rule
// NOTE: This is a simplified version for the new workflow-based schema
// The old parameters are kept for backward compatibility but stored in metadata
func (s *Service) CreateMappingRule(ctx context.Context, tenantID, workspaceID, name, description, sourceIdentifier, targetIdentifier, transformationName string, transformationOptions map[string]interface{}, metadata map[string]interface{}, ownerID string) (*Rule, error) {
	s.logger.Infof("Creating mapping rule in database for tenant: %s, workspace: %s, name: %s", tenantID, workspaceID, name)

	// Check if the tenant exists
	var tenantExists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)", tenantID).Scan(&tenantExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check tenant existence: %w", err)
	}
	if !tenantExists {
		return nil, errors.New("tenant not found")
	}

	// Check if the workspace exists and belongs to the tenant
	var workspaceExists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM workspaces WHERE workspace_id = $1 AND tenant_id = $2)", workspaceID, tenantID).Scan(&workspaceExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check workspace existence: %w", err)
	}
	if !workspaceExists {
		return nil, errors.New("workspace not found in tenant")
	}

	// Check if mapping rule with the same name already exists in this workspace
	var exists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM mapping_rules WHERE tenant_id = $1 AND workspace_id = $2 AND mapping_rule_name = $3)", tenantID, workspaceID, name).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check mapping rule existence: %w", err)
	}
	if exists {
		return nil, errors.New("mapping rule with this name already exists in the workspace")
	}

	// Store the resource URIs in metadata
	if metadata == nil {
		metadata = make(map[string]interface{})
	}
	metadata["source_resource_uri"] = sourceIdentifier // Note: parameter name is sourceIdentifier for backward compat, but stores as source_resource_uri
	metadata["target_resource_uri"] = targetIdentifier // Note: parameter name is targetIdentifier for backward compat, but stores as target_resource_uri
	metadata["transformation_name"] = transformationName
	if transformationOptions != nil {
		metadata["transformation_options"] = transformationOptions
	}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Insert the mapping rule into the database with simplified schema
	query := `
		INSERT INTO mapping_rules (tenant_id, workspace_id, mapping_rule_name, mapping_rule_description, 
			mapping_rule_metadata, mapping_rule_workflow_type, owner_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING mapping_rule_id, tenant_id, workspace_id, mapping_rule_name, mapping_rule_description, 
			mapping_rule_metadata, mapping_rule_workflow_type, owner_id, created, updated
	`

	var rule Rule
	var metadataBytes []byte
	err = s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, name, description, metadataJSON, "simple", ownerID).Scan(
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

	return &rule, nil
}

// ModifyMappingRule modifies a mapping rule
func (s *Service) ModifyMappingRule(ctx context.Context, tenantID, workspaceID, name string, updates map[string]interface{}) (*Rule, error) {
	s.logger.Infof("Modifying mapping rule with name: %s", name)

	// Check if mapping rule exists
	_, err := s.GetMappingRuleByName(ctx, tenantID, workspaceID, name)
	if err != nil {
		return nil, err
	}

	// Build dynamic update query
	setParts := []string{}
	args := []interface{}{tenantID, workspaceID, name}
	argIndex := 4

	for field, value := range updates {
		switch field {
		case "mapping_rule_name", "mapping_rule_description", "mapping_rule_metadata", "mapping_rule_workflow_type":
			setParts = append(setParts, fmt.Sprintf("%s = $%d", field, argIndex))
			args = append(args, value)
			argIndex++
		}
	}

	if len(setParts) == 0 {
		return s.GetMappingRuleByName(ctx, tenantID, workspaceID, name)
	}

	// Add updated timestamp
	setParts = append(setParts, "updated = CURRENT_TIMESTAMP")

	setClause := setParts[0]
	for i := 1; i < len(setParts); i++ {
		setClause += ", " + setParts[i]
	}

	query := fmt.Sprintf(`
		UPDATE mapping_rules
		SET %s
		WHERE tenant_id = $1 AND workspace_id = $2 AND mapping_rule_name = $3
		RETURNING mapping_rule_id, tenant_id, workspace_id, mapping_rule_name, mapping_rule_description, 
			mapping_rule_metadata, mapping_rule_workflow_type, owner_id, created, updated
	`, setClause)

	var rule Rule
	var metadataBytes []byte
	err = s.db.Pool().QueryRow(ctx, query, args...).Scan(
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
		s.logger.Errorf("Failed to modify mapping rule: %v", err)
		return nil, err
	}

	// Parse metadata
	if len(metadataBytes) > 0 {
		if err := json.Unmarshal(metadataBytes, &rule.Metadata); err != nil {
			s.logger.Warnf("Failed to parse metadata: %v", err)
		}
	}

	return &rule, nil
}

// DeleteMappingRule deletes a mapping rule
func (s *Service) DeleteMappingRule(ctx context.Context, tenantID, workspaceID, name string) error {
	s.logger.Infof("Deleting mapping rule with name: %s", name)

	// Check if mapping rule exists
	rule, err := s.GetMappingRuleByName(ctx, tenantID, workspaceID, name)
	if err != nil {
		return err
	}

	// Check if mapping rule is attached to any mappings
	var attachmentCount int
	err = s.db.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM mapping_rule_mappings WHERE mapping_rule_id = $1",
		rule.ID).Scan(&attachmentCount)
	if err != nil {
		return fmt.Errorf("failed to check for existing mapping attachments: %w", err)
	}
	if attachmentCount > 0 {
		return errors.New("cannot delete mapping rule that is attached to mappings")
	}

	// Delete mapping rule
	result, err := s.db.Pool().Exec(ctx, "DELETE FROM mapping_rules WHERE tenant_id = $1 AND workspace_id = $2 AND mapping_rule_name = $3",
		tenantID, workspaceID, name)
	if err != nil {
		return fmt.Errorf("failed to delete mapping rule: %w", err)
	}

	if result.RowsAffected() == 0 {
		return errors.New("mapping rule not found")
	}

	return nil
}

// UpdateMappingRuleOrder updates the order of a mapping rule within a mapping
func (s *Service) UpdateMappingRuleOrder(ctx context.Context, mappingID, ruleID string, newOrder int) error {
	s.logger.Infof("Updating order for mapping rule %s in mapping %s to %d", ruleID, mappingID, newOrder)

	// Check if the mapping rule is attached to the mapping
	var exists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM mapping_rule_mappings WHERE mapping_id = $1 AND mapping_rule_id = $2)", mappingID, ruleID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if mapping rule is attached: %w", err)
	}
	if !exists {
		return errors.New("mapping rule is not attached to this mapping")
	}

	// Get the current order
	var currentOrder int
	err = s.db.Pool().QueryRow(ctx, "SELECT mapping_rule_order FROM mapping_rule_mappings WHERE mapping_id = $1 AND mapping_rule_id = $2", mappingID, ruleID).Scan(&currentOrder)
	if err != nil {
		return fmt.Errorf("failed to get current order: %w", err)
	}

	if currentOrder == newOrder {
		// No change needed
		return nil
	}

	// Begin transaction for reordering
	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Shift other rules to make room
	if newOrder < currentOrder {
		// Moving up, shift rules down
		_, err = tx.Exec(ctx, `
			UPDATE mapping_rule_mappings 
			SET mapping_rule_order = mapping_rule_order + 1
			WHERE mapping_id = $1 AND mapping_rule_order >= $2 AND mapping_rule_order < $3
		`, mappingID, newOrder, currentOrder)
	} else {
		// Moving down, shift rules up
		_, err = tx.Exec(ctx, `
			UPDATE mapping_rule_mappings 
			SET mapping_rule_order = mapping_rule_order - 1
			WHERE mapping_id = $1 AND mapping_rule_order > $2 AND mapping_rule_order <= $3
		`, mappingID, currentOrder, newOrder)
	}
	if err != nil {
		return fmt.Errorf("failed to shift rules: %w", err)
	}

	// Update the target rule's order
	_, err = tx.Exec(ctx, `
		UPDATE mapping_rule_mappings 
		SET mapping_rule_order = $1
		WHERE mapping_id = $2 AND mapping_rule_id = $3
	`, newOrder, mappingID, ruleID)
	if err != nil {
		return fmt.Errorf("failed to update rule order: %w", err)
	}

	// Commit transaction
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	s.logger.Infof("Successfully updated mapping rule order")
	return nil
}

// GetMappingRulesForMapping retrieves all mapping rules attached to a specific mapping by name
func (s *Service) GetMappingRulesForMapping(ctx context.Context, tenantID, workspaceID, mappingName string) ([]*Rule, error) {
	s.logger.Infof("Retrieving mapping rules for mapping: %s", mappingName)

	// Check if mapping exists
	mapping, err := s.Get(ctx, tenantID, workspaceID, mappingName)
	if err != nil {
		return nil, err
	}

	return s.GetMappingRulesForMappingByID(ctx, tenantID, workspaceID, mapping.ID)
}

// GetMappingRulesForMappingByID retrieves all mapping rules attached to a specific mapping by ID
func (s *Service) GetMappingRulesForMappingByID(ctx context.Context, tenantID, workspaceID, mappingID string) ([]*Rule, error) {
	s.logger.Infof("Retrieving mapping rules for mapping ID: %s", mappingID)

	query := `
		SELECT mr.mapping_rule_id, mr.tenant_id, mr.workspace_id, mr.mapping_rule_name, mr.mapping_rule_description, 
			mr.mapping_rule_metadata, mr.mapping_rule_workflow_type, mr.owner_id, mr.created, mr.updated
		FROM mapping_rules mr
		INNER JOIN mapping_rule_mappings mrm ON mr.mapping_rule_id = mrm.mapping_rule_id
		WHERE mr.tenant_id = $1 AND mr.workspace_id = $2 AND mrm.mapping_id = $3
		ORDER BY mrm.mapping_rule_order, mr.mapping_rule_name
	`

	rows, err := s.db.Pool().Query(ctx, query, tenantID, workspaceID, mappingID)
	if err != nil {
		s.logger.Errorf("Failed to get mapping rules for mapping: %v", err)
		return nil, err
	}
	defer rows.Close()

	var rules []*Rule
	for rows.Next() {
		var rule Rule
		var metadataBytes []byte
		err := rows.Scan(
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
			s.logger.Errorf("Failed to scan mapping rule: %v", err)
			return nil, err
		}

		// Parse metadata
		if len(metadataBytes) > 0 {
			if err := json.Unmarshal(metadataBytes, &rule.Metadata); err != nil {
				s.logger.Warnf("Failed to parse metadata: %v", err)
			}
		}

		rules = append(rules, &rule)
	}

	if err = rows.Err(); err != nil {
		s.logger.Errorf("Error after scanning mapping rules: %v", err)
		return nil, err
	}

	return rules, nil
}

// GetByID retrieves a mapping by its ID
func (s *Service) GetByID(ctx context.Context, mappingID string) (*Mapping, error) {
	query := `
		SELECT mapping_id, tenant_id, workspace_id, mapping_name, mapping_description, 
		       mapping_type, policy_ids, owner_id, validated, validated_at, 
		       validation_errors, validation_warnings, created, updated
		FROM mappings
		WHERE mapping_id = $1
	`

	mapping := &Mapping{}
	var validationErrorsJSON, validationWarningsJSON []byte
	err := s.db.Pool().QueryRow(ctx, query, mappingID).Scan(
		&mapping.ID,
		&mapping.TenantID,
		&mapping.WorkspaceID,
		&mapping.Name,
		&mapping.Description,
		&mapping.MappingType,
		&mapping.PolicyIDs,
		&mapping.OwnerID,
		&mapping.Validated,
		&mapping.ValidatedAt,
		&validationErrorsJSON,
		&validationWarningsJSON,
		&mapping.Created,
		&mapping.Updated,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("mapping not found")
		}
		return nil, fmt.Errorf("failed to get mapping: %w", err)
	}

	// Unmarshal JSON fields
	if len(validationErrorsJSON) > 0 {
		if err := json.Unmarshal(validationErrorsJSON, &mapping.ValidationErrors); err != nil {
			return nil, fmt.Errorf("failed to unmarshal validation errors: %w", err)
		}
	}
	if len(validationWarningsJSON) > 0 {
		if err := json.Unmarshal(validationWarningsJSON, &mapping.ValidationWarnings); err != nil {
			return nil, fmt.Errorf("failed to unmarshal validation warnings: %w", err)
		}
	}

	return mapping, nil
}

// GetRulesByMappingID retrieves all rules associated with a mapping
func (s *Service) GetRulesByMappingID(ctx context.Context, mappingID string) ([]*Rule, error) {
	query := `
		SELECT 
			mr.mapping_rule_id,
			mr.tenant_id,
			mr.workspace_id,
			mr.mapping_rule_name,
			mr.mapping_rule_description,
			mr.mapping_rule_metadata,
			mr.mapping_rule_workflow_type,
			mr.owner_id,
			mr.created,
			mr.updated
		FROM mapping_rules mr
		JOIN mapping_rule_mappings mrm ON mr.mapping_rule_id = mrm.mapping_rule_id
		WHERE mrm.mapping_id = $1
		ORDER BY mrm.mapping_rule_order
	`

	rows, err := s.db.Pool().Query(ctx, query, mappingID)
	if err != nil {
		return nil, fmt.Errorf("failed to query mapping rules: %w", err)
	}
	defer rows.Close()

	rules := []*Rule{}
	for rows.Next() {
		rule := &Rule{}
		var metadataBytes []byte
		err := rows.Scan(
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
			return nil, fmt.Errorf("failed to scan mapping rule: %w", err)
		}

		// Parse metadata
		if len(metadataBytes) > 0 {
			if err := json.Unmarshal(metadataBytes, &rule.Metadata); err != nil {
				s.logger.Warnf("Failed to parse metadata: %v", err)
			}
		}

		rules = append(rules, rule)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating mapping rules: %w", err)
	}

	return rules, nil
}

// UpdateValidationStatus updates the validation status of a mapping
func (s *Service) UpdateValidationStatus(ctx context.Context, mappingID string, isValid bool, errors, warnings []string) error {
	query := `
		UPDATE mappings
		SET validated = $1,
		    validated_at = CURRENT_TIMESTAMP,
		    validation_errors = $2,
		    validation_warnings = $3,
		    updated = CURRENT_TIMESTAMP
		WHERE mapping_id = $4
	`

	errorsJSON, err := json.Marshal(errors)
	if err != nil {
		return fmt.Errorf("failed to marshal errors: %w", err)
	}

	warningsJSON, err := json.Marshal(warnings)
	if err != nil {
		return fmt.Errorf("failed to marshal warnings: %w", err)
	}

	_, err = s.db.Pool().Exec(ctx, query, isValid, errorsJSON, warningsJSON, mappingID)
	if err != nil {
		return fmt.Errorf("failed to update validation status: %w", err)
	}

	return nil
}

// InvalidateMapping invalidates a mapping's validation status (sets validated to false and clears validation data)
func (s *Service) InvalidateMapping(ctx context.Context, mappingID string) error {
	query := `
		UPDATE mappings
		SET validated = false,
		    validated_at = NULL,
		    validation_errors = '[]',
		    validation_warnings = '[]',
		    updated = CURRENT_TIMESTAMP
		WHERE mapping_id = $1
	`

	_, err := s.db.Pool().Exec(ctx, query, mappingID)
	if err != nil {
		return fmt.Errorf("failed to invalidate mapping: %w", err)
	}

	return nil
}

// InvalidateMappingsByTarget invalidates all mappings that target a specific database and table
func (s *Service) InvalidateMappingsByTarget(ctx context.Context, workspaceID, databaseID, tableName string) error {
	// Find all mapping rules that target the specified database and table
	// Then invalidate the associated mappings
	query := `
		UPDATE mappings m
		SET validated = false,
		    validated_at = NULL,
		    validation_errors = '[]',
		    validation_warnings = '[]',
		    updated = CURRENT_TIMESTAMP
		WHERE m.workspace_id = $1
		AND m.mapping_id IN (
			SELECT DISTINCT mrm.mapping_id
			FROM mapping_rule_mappings mrm
			JOIN mapping_rules mr ON mrm.mapping_rule_id = mr.mapping_rule_id
			WHERE mr.mapping_rule_metadata->>'target_resource_uri' LIKE $2
		)
	`

	// The target URI format is: redb://database_id/dbname/table/table_name/column/%
	targetPattern := fmt.Sprintf("redb://%s/%%/table/%s/%%", databaseID, tableName)

	result, err := s.db.Pool().Exec(ctx, query, workspaceID, targetPattern)
	if err != nil {
		return fmt.Errorf("failed to invalidate mappings by target: %w", err)
	}

	rowsAffected := result.RowsAffected()
	if rowsAffected > 0 {
		s.logger.Infof("Invalidated %d mapping(s) targeting %s.%s", rowsAffected, databaseID, tableName)
	}

	return nil
}

// GetByName retrieves a mapping by its name
func (s *Service) GetByName(ctx context.Context, tenantID, workspaceID, mappingName string) (*Mapping, error) {
	query := `
		SELECT mapping_id, tenant_id, workspace_id, mapping_name, mapping_description, 
		       mapping_type, policy_ids, owner_id, validated, validated_at,
		       validation_errors, validation_warnings, created, updated
		FROM mappings
		WHERE tenant_id = $1 AND workspace_id = $2 AND mapping_name = $3
	`

	mapping := &Mapping{}
	var validationErrorsJSON, validationWarningsJSON []byte
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, mappingName).Scan(
		&mapping.ID,
		&mapping.TenantID,
		&mapping.WorkspaceID,
		&mapping.Name,
		&mapping.Description,
		&mapping.MappingType,
		&mapping.PolicyIDs,
		&mapping.OwnerID,
		&mapping.Validated,
		&mapping.ValidatedAt,
		&validationErrorsJSON,
		&validationWarningsJSON,
		&mapping.Created,
		&mapping.Updated,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("mapping not found")
		}
		return nil, fmt.Errorf("failed to get mapping: %w", err)
	}

	// Unmarshal JSON fields
	if len(validationErrorsJSON) > 0 {
		if err := json.Unmarshal(validationErrorsJSON, &mapping.ValidationErrors); err != nil {
			return nil, fmt.Errorf("failed to unmarshal validation errors: %w", err)
		}
	}
	if len(validationWarningsJSON) > 0 {
		if err := json.Unmarshal(validationWarningsJSON, &mapping.ValidationWarnings); err != nil {
			return nil, fmt.Errorf("failed to unmarshal validation warnings: %w", err)
		}
	}

	return mapping, nil
}
