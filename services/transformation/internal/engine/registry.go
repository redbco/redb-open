package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// TransformationRegistry manages all available transformations
type TransformationRegistry struct {
	mu              sync.RWMutex
	transformations map[string]*TransformationRecord // by ID
	byName          map[string]*TransformationRecord // by name (tenant_id:name)
	functions       map[string]interface{}           // function implementations by name
	db              *DatabaseOps
	logger          *logger.Logger
}

// NewTransformationRegistry creates a new transformation registry
func NewTransformationRegistry(db *database.PostgreSQL, logger *logger.Logger) *TransformationRegistry {
	return &TransformationRegistry{
		transformations: make(map[string]*TransformationRecord),
		byName:          make(map[string]*TransformationRecord),
		functions:       make(map[string]interface{}),
		db:              NewDatabaseOps(db, logger),
		logger:          logger,
	}
}

// RegisterBuiltIn registers all built-in transformation functions
func (r *TransformationRegistry) RegisterBuiltIn() {
	r.mu.Lock()
	defer r.mu.Unlock()

	builtIns := GetBuiltInTransformations()
	for _, builtIn := range builtIns {
		r.functions[builtIn.Implementation] = builtIn.ExecuteFunc
		r.logger.Debugf("Registered built-in function: %s", builtIn.Name)
	}

	r.logger.Info("Built-in transformation functions registered")
}

// LoadFromDatabase loads transformation definitions from the database for a tenant
func (r *TransformationRegistry) LoadFromDatabase(ctx context.Context, tenantID string) error {
	r.logger.Infof("Loading transformations from database for tenant: %s", tenantID)

	transformations, err := r.db.ListTransformations(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("failed to list transformations: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, t := range transformations {
		r.transformations[t.ID] = t
		key := fmt.Sprintf("%s:%s", t.TenantID, t.Name)
		r.byName[key] = t
		r.logger.Debugf("Loaded transformation: %s (ID: %s)", t.Name, t.ID)
	}

	r.logger.Infof("Loaded %d transformations for tenant %s", len(transformations), tenantID)
	return nil
}

// GetTransformation retrieves a transformation by ID
func (r *TransformationRegistry) GetTransformation(transformationID string) (*TransformationRecord, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	t, exists := r.transformations[transformationID]
	if !exists {
		return nil, fmt.Errorf("transformation not found: %s", transformationID)
	}

	return t, nil
}

// GetTransformationByName retrieves a transformation by tenant ID and name
func (r *TransformationRegistry) GetTransformationByName(tenantID, name string) (*TransformationRecord, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", tenantID, name)
	t, exists := r.byName[key]
	if !exists {
		return nil, fmt.Errorf("transformation not found: %s:%s", tenantID, name)
	}

	return t, nil
}

// GetFunction retrieves a transformation function by implementation name
func (r *TransformationRegistry) GetFunction(implementation string) (interface{}, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	fn, exists := r.functions[implementation]
	if !exists {
		return nil, fmt.Errorf("function not found: %s", implementation)
	}

	return fn, nil
}

// ValidateIO validates inputs and outputs against transformation definitions
func (r *TransformationRegistry) ValidateIO(transformationID string, inputs, outputs map[string]interface{}) error {
	transformation, err := r.GetTransformation(transformationID)
	if err != nil {
		return err
	}

	// Validate inputs
	for _, ioDef := range transformation.IODefinitions {
		if ioDef.IOType == "input" {
			if ioDef.IsMandatory {
				if _, exists := inputs[ioDef.Name]; !exists {
					return fmt.Errorf("mandatory input missing: %s", ioDef.Name)
				}
			}

			// TODO: Add type validation based on ioDef.DataType
			// TODO: Add validation rules check based on ioDef.ValidationRules
		}
	}

	// Validate outputs
	for _, ioDef := range transformation.IODefinitions {
		if ioDef.IOType == "output" {
			if ioDef.IsMandatory {
				if _, exists := outputs[ioDef.Name]; !exists {
					return fmt.Errorf("mandatory output missing: %s", ioDef.Name)
				}
			}
		}
	}

	return nil
}

// RegisterTransformation adds a new transformation to the registry
func (r *TransformationRegistry) RegisterTransformation(t *TransformationRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.transformations[t.ID] = t
	key := fmt.Sprintf("%s:%s", t.TenantID, t.Name)
	r.byName[key] = t
	r.logger.Debugf("Registered transformation: %s (ID: %s)", t.Name, t.ID)
}

// UnregisterTransformation removes a transformation from the registry
func (r *TransformationRegistry) UnregisterTransformation(transformationID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	t, exists := r.transformations[transformationID]
	if !exists {
		return
	}

	key := fmt.Sprintf("%s:%s", t.TenantID, t.Name)
	delete(r.transformations, transformationID)
	delete(r.byName, key)
	r.logger.Debugf("Unregistered transformation: %s (ID: %s)", t.Name, t.ID)
}

// ListTransformations returns all transformations in the registry
func (r *TransformationRegistry) ListTransformations() []*TransformationRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*TransformationRecord, 0, len(r.transformations))
	for _, t := range r.transformations {
		result = append(result, t)
	}

	return result
}

// Reload reloads all transformations from the database
func (r *TransformationRegistry) Reload(ctx context.Context, tenantID string) error {
	r.logger.Infof("Reloading transformations from database for tenant: %s", tenantID)

	// Clear existing transformations for this tenant
	r.mu.Lock()
	for id, t := range r.transformations {
		if t.TenantID == tenantID {
			key := fmt.Sprintf("%s:%s", t.TenantID, t.Name)
			delete(r.transformations, id)
			delete(r.byName, key)
		}
	}
	r.mu.Unlock()

	// Reload from database
	return r.LoadFromDatabase(ctx, tenantID)
}
