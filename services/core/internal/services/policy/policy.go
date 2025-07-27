package policy

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// Service handles policy-related operations
type Service struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewService creates a new policy service
func NewService(db *database.PostgreSQL, logger *logger.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// Policy represents a policy in the system
type Policy struct {
	ID          string
	TenantID    string
	Name        string
	Description string
	Type        string
	Effect      string
	Actions     []string
	Resources   []string
	Conditions  map[string]interface{}
	Priority    int32
	Enabled     bool
	OwnerID     string
	Created     time.Time
	Updated     time.Time
}

// Create creates a new policy
func (s *Service) Create(ctx context.Context, tenantID, name, description, policyType, effect string, actions, resources []string, conditions map[string]interface{}, priority int32, ownerID string) (*Policy, error) {
	s.logger.Infof("Creating policy in database for tenant: %s, name: %s", tenantID, name)

	// Check if the tenant exists
	var tenantExists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)", tenantID).Scan(&tenantExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check tenant existence: %w", err)
	}
	if !tenantExists {
		return nil, errors.New("tenant not found")
	}

	// Check if policy with the same name already exists in this tenant
	var exists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM policies WHERE tenant_id = $1 AND policy_name = $2)", tenantID, name).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check policy existence: %w", err)
	}
	if exists {
		return nil, errors.New("policy with this name already exists in the tenant")
	}

	// Insert the policy into the database
	query := `
		INSERT INTO policies (tenant_id, policy_name, policy_description, policy_type, 
		                     policy_effect, policy_actions, policy_resources, 
		                     policy_conditions, policy_priority, owner_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING policy_id, tenant_id, policy_name, policy_description, policy_type, 
		          policy_effect, COALESCE(policy_actions, '{}') as policy_actions, 
		          COALESCE(policy_resources, '{}') as policy_resources,
		          COALESCE(policy_conditions, '{}') as policy_conditions, 
		          policy_priority, policy_enabled, owner_id, created, updated
	`

	var policy Policy
	err = s.db.Pool().QueryRow(ctx, query, tenantID, name, description, policyType, effect, actions, resources, conditions, priority, ownerID).Scan(
		&policy.ID,
		&policy.TenantID,
		&policy.Name,
		&policy.Description,
		&policy.Type,
		&policy.Effect,
		&policy.Actions,
		&policy.Resources,
		&policy.Conditions,
		&policy.Priority,
		&policy.Enabled,
		&policy.OwnerID,
		&policy.Created,
		&policy.Updated,
	)
	if err != nil {
		s.logger.Errorf("Failed to create policy: %v", err)
		return nil, err
	}

	return &policy, nil
}

// Get retrieves a policy by ID
func (s *Service) Get(ctx context.Context, tenantID, id string) (*Policy, error) {
	s.logger.Infof("Retrieving policy from database with ID: %s", id)
	query := `
		SELECT policy_id, tenant_id, policy_name, policy_description, policy_type, 
		       policy_effect, COALESCE(policy_actions, '{}') as policy_actions, 
		       COALESCE(policy_resources, '{}') as policy_resources,
		       COALESCE(policy_conditions, '{}') as policy_conditions, 
		       policy_priority, policy_enabled, owner_id, created, updated
		FROM policies
		WHERE tenant_id = $1 AND policy_id = $2
	`

	var policy Policy
	err := s.db.Pool().QueryRow(ctx, query, tenantID, id).Scan(
		&policy.ID,
		&policy.TenantID,
		&policy.Name,
		&policy.Description,
		&policy.Type,
		&policy.Effect,
		&policy.Actions,
		&policy.Resources,
		&policy.Conditions,
		&policy.Priority,
		&policy.Enabled,
		&policy.OwnerID,
		&policy.Created,
		&policy.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("policy not found")
		}
		s.logger.Errorf("Failed to get policy: %v", err)
		return nil, err
	}

	return &policy, nil
}

// List retrieves all policies for a tenant
func (s *Service) List(ctx context.Context, tenantID string) ([]*Policy, error) {
	s.logger.Infof("Listing policies for tenant: %s", tenantID)
	query := `
		SELECT policy_id, tenant_id, policy_name, policy_description, policy_type, 
		       policy_effect, COALESCE(policy_actions, '{}') as policy_actions, 
		       COALESCE(policy_resources, '{}') as policy_resources,
		       COALESCE(policy_conditions, '{}') as policy_conditions, 
		       policy_priority, policy_enabled, owner_id, created, updated
		FROM policies
		WHERE tenant_id = $1
		ORDER BY policy_priority DESC, policy_name
	`

	rows, err := s.db.Pool().Query(ctx, query, tenantID)
	if err != nil {
		s.logger.Errorf("Failed to list policies: %v", err)
		return nil, err
	}
	defer rows.Close()

	var policies []*Policy
	for rows.Next() {
		var policy Policy
		err := rows.Scan(
			&policy.ID,
			&policy.TenantID,
			&policy.Name,
			&policy.Description,
			&policy.Type,
			&policy.Effect,
			&policy.Actions,
			&policy.Resources,
			&policy.Conditions,
			&policy.Priority,
			&policy.Enabled,
			&policy.OwnerID,
			&policy.Created,
			&policy.Updated,
		)
		if err != nil {
			s.logger.Errorf("Failed to scan policy: %v", err)
			return nil, err
		}
		policies = append(policies, &policy)
	}

	if err = rows.Err(); err != nil {
		s.logger.Errorf("Error after scanning policies: %v", err)
		return nil, err
	}

	return policies, nil
}

// Update updates a policy
func (s *Service) Update(ctx context.Context, tenantID, id string, updates map[string]interface{}) (*Policy, error) {
	s.logger.Infof("Updating policy with ID: %s", id)

	if len(updates) == 0 {
		return s.Get(ctx, tenantID, id)
	}

	// Check if policy exists
	_, err := s.Get(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}

	// Build dynamic update query
	setParts := []string{}
	args := []interface{}{tenantID, id}
	argIndex := 3

	for field, value := range updates {
		switch field {
		case "policy_name", "policy_description", "policy_type", "policy_effect",
			"policy_actions", "policy_resources", "policy_conditions",
			"policy_priority", "policy_enabled":
			setParts = append(setParts, fmt.Sprintf("%s = $%d", field, argIndex))
			args = append(args, value)
			argIndex++
		default:
			s.logger.Warnf("Ignoring invalid update field: %s", field)
		}
	}

	if len(setParts) == 0 {
		return s.Get(ctx, tenantID, id)
	}

	// Add updated timestamp
	setParts = append(setParts, "updated = CURRENT_TIMESTAMP")

	setClause := setParts[0]
	for i := 1; i < len(setParts); i++ {
		setClause += ", " + setParts[i]
	}

	query := fmt.Sprintf(`
		UPDATE policies 
		SET %s
		WHERE tenant_id = $1 AND policy_id = $2
		RETURNING policy_id, tenant_id, policy_name, policy_description, policy_type, 
		          policy_effect, COALESCE(policy_actions, '{}') as policy_actions, 
		          COALESCE(policy_resources, '{}') as policy_resources,
		          COALESCE(policy_conditions, '{}') as policy_conditions, 
		          policy_priority, policy_enabled, owner_id, created, updated
	`, setClause)

	var policy Policy
	err = s.db.Pool().QueryRow(ctx, query, args...).Scan(
		&policy.ID,
		&policy.TenantID,
		&policy.Name,
		&policy.Description,
		&policy.Type,
		&policy.Effect,
		&policy.Actions,
		&policy.Resources,
		&policy.Conditions,
		&policy.Priority,
		&policy.Enabled,
		&policy.OwnerID,
		&policy.Created,
		&policy.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("policy not found")
		}
		s.logger.Errorf("Failed to update policy: %v", err)
		return nil, err
	}

	return &policy, nil
}

// Delete deletes a policy
func (s *Service) Delete(ctx context.Context, tenantID, id string) error {
	s.logger.Infof("Deleting policy with ID: %s", id)

	// Check if policy exists
	_, err := s.Get(ctx, tenantID, id)
	if err != nil {
		return err
	}

	// Check if policy is being used by other resources
	// Check repos
	var repoCount int
	err = s.db.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM repos WHERE tenant_id = $1 AND $2 = ANY(policy_ids)",
		tenantID, id).Scan(&repoCount)
	if err != nil {
		return fmt.Errorf("failed to check for repos using this policy: %w", err)
	}
	if repoCount > 0 {
		return errors.New("cannot delete policy that is being used by repositories")
	}

	// Check mappings
	var mappingCount int
	err = s.db.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM mappings WHERE tenant_id = $1 AND $2 = ANY(policy_ids)",
		tenantID, id).Scan(&mappingCount)
	if err != nil {
		return fmt.Errorf("failed to check for mappings using this policy: %w", err)
	}
	if mappingCount > 0 {
		return errors.New("cannot delete policy that is being used by mappings")
	}

	// Check relationships
	var relationshipCount int
	err = s.db.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM relationships WHERE tenant_id = $1 AND $2 = ANY(policy_ids)",
		tenantID, id).Scan(&relationshipCount)
	if err != nil {
		return fmt.Errorf("failed to check for relationships using this policy: %w", err)
	}
	if relationshipCount > 0 {
		return errors.New("cannot delete policy that is being used by relationships")
	}

	// Delete policy
	result, err := s.db.Pool().Exec(ctx, "DELETE FROM policies WHERE tenant_id = $1 AND policy_id = $2",
		tenantID, id)
	if err != nil {
		return fmt.Errorf("failed to delete policy: %w", err)
	}

	if result.RowsAffected() == 0 {
		return errors.New("policy not found")
	}

	return nil
}
