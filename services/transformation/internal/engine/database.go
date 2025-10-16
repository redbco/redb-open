package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// DatabaseOps handles database operations for transformations
type DatabaseOps struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewDatabaseOps creates a new DatabaseOps instance
func NewDatabaseOps(db *database.PostgreSQL, logger *logger.Logger) *DatabaseOps {
	return &DatabaseOps{
		db:     db,
		logger: logger,
	}
}

// TransformationRecord represents a transformation in the database
type TransformationRecord struct {
	ID             string
	TenantID       string
	Name           string
	Description    string
	Type           string
	Version        string
	Function       string
	Cardinality    string
	RequiresInput  bool
	ProducesOutput bool
	Implementation string
	Metadata       map[string]interface{}
	Enabled        bool
	OwnerID        string
	IODefinitions  []IODefinitionRecord
}

// IODefinitionRecord represents an I/O definition in the database
type IODefinitionRecord struct {
	ID               string
	TransformationID string
	IOType           string
	Name             string
	DataType         string
	IsMandatory      bool
	IsArray          bool
	DefaultValue     interface{}
	Description      string
	ValidationRules  map[string]interface{}
}

// GetTransformation retrieves a transformation by ID
func (db *DatabaseOps) GetTransformation(ctx context.Context, transformationID string) (*TransformationRecord, error) {
	query := `
		SELECT transformation_id, tenant_id, transformation_name, transformation_description,
		       transformation_type, transformation_version, transformation_function,
		       transformation_cardinality, requires_input, produces_output,
		       transformation_implementation, transformation_metadata, transformation_enabled, owner_id
		FROM transformations
		WHERE transformation_id = $1
	`

	var record TransformationRecord
	var metadataJSON []byte

	err := db.db.Pool().QueryRow(ctx, query, transformationID).Scan(
		&record.ID,
		&record.TenantID,
		&record.Name,
		&record.Description,
		&record.Type,
		&record.Version,
		&record.Function,
		&record.Cardinality,
		&record.RequiresInput,
		&record.ProducesOutput,
		&record.Implementation,
		&metadataJSON,
		&record.Enabled,
		&record.OwnerID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get transformation: %w", err)
	}

	// Parse metadata JSON
	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &record.Metadata); err != nil {
			return nil, fmt.Errorf("failed to parse metadata: %w", err)
		}
	}

	// Get I/O definitions
	record.IODefinitions, err = db.GetIODefinitions(ctx, transformationID)
	if err != nil {
		return nil, fmt.Errorf("failed to get I/O definitions: %w", err)
	}

	return &record, nil
}

// GetTransformationByName retrieves a transformation by tenant ID and name
func (db *DatabaseOps) GetTransformationByName(ctx context.Context, tenantID, name string) (*TransformationRecord, error) {
	query := `
		SELECT transformation_id, tenant_id, transformation_name, transformation_description,
		       transformation_type, transformation_version, transformation_function,
		       transformation_cardinality, requires_input, produces_output,
		       transformation_implementation, transformation_metadata, transformation_enabled, owner_id
		FROM transformations
		WHERE tenant_id = $1 AND transformation_name = $2
	`

	var record TransformationRecord
	var metadataJSON []byte

	err := db.db.Pool().QueryRow(ctx, query, tenantID, name).Scan(
		&record.ID,
		&record.TenantID,
		&record.Name,
		&record.Description,
		&record.Type,
		&record.Version,
		&record.Function,
		&record.Cardinality,
		&record.RequiresInput,
		&record.ProducesOutput,
		&record.Implementation,
		&metadataJSON,
		&record.Enabled,
		&record.OwnerID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get transformation by name: %w", err)
	}

	// Parse metadata JSON
	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &record.Metadata); err != nil {
			return nil, fmt.Errorf("failed to parse metadata: %w", err)
		}
	}

	// Get I/O definitions
	record.IODefinitions, err = db.GetIODefinitions(ctx, record.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get I/O definitions: %w", err)
	}

	return &record, nil
}

// CreateTransformation creates a new transformation
func (db *DatabaseOps) CreateTransformation(ctx context.Context, record *TransformationRecord) (string, error) {
	metadataJSON, err := json.Marshal(record.Metadata)
	if err != nil {
		return "", fmt.Errorf("failed to marshal metadata: %w", err)
	}

	query := `
		INSERT INTO transformations (
			tenant_id, transformation_name, transformation_description,
			transformation_type, transformation_version, transformation_function,
			transformation_cardinality, requires_input, produces_output,
			transformation_implementation, transformation_metadata, transformation_enabled, owner_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		RETURNING transformation_id
	`

	var transformationID string
	err = db.db.Pool().QueryRow(ctx, query,
		record.TenantID,
		record.Name,
		record.Description,
		record.Type,
		record.Version,
		record.Function,
		record.Cardinality,
		record.RequiresInput,
		record.ProducesOutput,
		record.Implementation,
		metadataJSON,
		record.Enabled,
		record.OwnerID,
	).Scan(&transformationID)

	if err != nil {
		return "", fmt.Errorf("failed to create transformation: %w", err)
	}

	// Create I/O definitions
	for _, ioDef := range record.IODefinitions {
		ioDef.TransformationID = transformationID
		if err := db.CreateIODefinition(ctx, &ioDef); err != nil {
			return "", fmt.Errorf("failed to create I/O definition: %w", err)
		}
	}

	return transformationID, nil
}

// GetIODefinitions retrieves I/O definitions for a transformation
func (db *DatabaseOps) GetIODefinitions(ctx context.Context, transformationID string) ([]IODefinitionRecord, error) {
	query := `
		SELECT io_id, transformation_id, io_type, io_name, io_data_type,
		       is_mandatory, is_array, default_value, description, validation_rules
		FROM transformation_io_definitions
		WHERE transformation_id = $1
		ORDER BY io_type, io_name
	`

	rows, err := db.db.Pool().Query(ctx, query, transformationID)
	if err != nil {
		return nil, fmt.Errorf("failed to query I/O definitions: %w", err)
	}
	defer rows.Close()

	var definitions []IODefinitionRecord
	for rows.Next() {
		var def IODefinitionRecord
		var defaultValueJSON []byte
		var validationRulesJSON []byte

		err := rows.Scan(
			&def.ID,
			&def.TransformationID,
			&def.IOType,
			&def.Name,
			&def.DataType,
			&def.IsMandatory,
			&def.IsArray,
			&defaultValueJSON,
			&def.Description,
			&validationRulesJSON,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan I/O definition: %w", err)
		}

		// Parse default value
		if len(defaultValueJSON) > 0 {
			if err := json.Unmarshal(defaultValueJSON, &def.DefaultValue); err != nil {
				db.logger.Warnf("Failed to parse default value: %v", err)
			}
		}

		// Parse validation rules
		if len(validationRulesJSON) > 0 {
			if err := json.Unmarshal(validationRulesJSON, &def.ValidationRules); err != nil {
				db.logger.Warnf("Failed to parse validation rules: %v", err)
			}
		}

		definitions = append(definitions, def)
	}

	return definitions, rows.Err()
}

// CreateIODefinition creates a new I/O definition
func (db *DatabaseOps) CreateIODefinition(ctx context.Context, def *IODefinitionRecord) error {
	var defaultValueJSON []byte
	var err error
	if def.DefaultValue != nil {
		defaultValueJSON, err = json.Marshal(def.DefaultValue)
		if err != nil {
			return fmt.Errorf("failed to marshal default value: %w", err)
		}
	}

	validationRulesJSON, err := json.Marshal(def.ValidationRules)
	if err != nil {
		return fmt.Errorf("failed to marshal validation rules: %w", err)
	}

	query := `
		INSERT INTO transformation_io_definitions (
			transformation_id, io_type, io_name, io_data_type,
			is_mandatory, is_array, default_value, description, validation_rules
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING io_id
	`

	return db.db.Pool().QueryRow(ctx, query,
		def.TransformationID,
		def.IOType,
		def.Name,
		def.DataType,
		def.IsMandatory,
		def.IsArray,
		defaultValueJSON,
		def.Description,
		validationRulesJSON,
	).Scan(&def.ID)
}

// ListTransformations retrieves all transformations for a tenant
func (db *DatabaseOps) ListTransformations(ctx context.Context, tenantID string) ([]*TransformationRecord, error) {
	query := `
		SELECT transformation_id, tenant_id, transformation_name, transformation_description,
		       transformation_type, transformation_version, transformation_function,
		       transformation_cardinality, requires_input, produces_output,
		       transformation_implementation, transformation_metadata, transformation_enabled, owner_id
		FROM transformations
		WHERE tenant_id = $1
		ORDER BY transformation_name
	`

	rows, err := db.db.Pool().Query(ctx, query, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to list transformations: %w", err)
	}
	defer rows.Close()

	var records []*TransformationRecord
	for rows.Next() {
		var record TransformationRecord
		var metadataJSON []byte

		err := rows.Scan(
			&record.ID,
			&record.TenantID,
			&record.Name,
			&record.Description,
			&record.Type,
			&record.Version,
			&record.Function,
			&record.Cardinality,
			&record.RequiresInput,
			&record.ProducesOutput,
			&record.Implementation,
			&metadataJSON,
			&record.Enabled,
			&record.OwnerID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transformation: %w", err)
		}

		// Parse metadata JSON
		if len(metadataJSON) > 0 {
			if err := json.Unmarshal(metadataJSON, &record.Metadata); err != nil {
				db.logger.Warnf("Failed to parse metadata: %v", err)
			}
		}

		// Get I/O definitions
		record.IODefinitions, err = db.GetIODefinitions(ctx, record.ID)
		if err != nil {
			db.logger.Warnf("Failed to get I/O definitions for %s: %v", record.ID, err)
		}

		records = append(records, &record)
	}

	return records, rows.Err()
}

// SeedBuiltInTransformations seeds the database with built-in transformations
func (db *DatabaseOps) SeedBuiltInTransformations(ctx context.Context, tenantID, ownerID string) error {
	db.logger.Info("Seeding built-in transformations...")

	builtIns := GetBuiltInTransformations()
	for _, builtIn := range builtIns {
		// Check if transformation already exists
		existing, err := db.GetTransformationByName(ctx, tenantID, builtIn.Name)
		if err == nil && existing != nil {
			db.logger.Debugf("Transformation %s already exists, skipping", builtIn.Name)
			continue
		}

		// Convert to database record
		metadata := make(map[string]interface{})
		record := &TransformationRecord{
			TenantID:       tenantID,
			Name:           builtIn.Name,
			Description:    builtIn.Description,
			Type:           builtIn.Type,
			Version:        "1.0.0",
			Function:       builtIn.Implementation,
			Cardinality:    builtIn.Cardinality,
			RequiresInput:  builtIn.RequiresInput,
			ProducesOutput: builtIn.ProducesOutput,
			Implementation: builtIn.Implementation,
			Metadata:       metadata,
			Enabled:        true,
			OwnerID:        ownerID,
		}

		// Convert I/O definitions
		for _, ioDef := range builtIn.IODefinitions {
			record.IODefinitions = append(record.IODefinitions, IODefinitionRecord{
				IOType:          ioDef.IOType,
				Name:            ioDef.Name,
				DataType:        ioDef.DataType,
				IsMandatory:     ioDef.IsMandatory,
				IsArray:         ioDef.IsArray,
				DefaultValue:    ioDef.DefaultValue,
				Description:     ioDef.Description,
				ValidationRules: ioDef.ValidationRules,
			})
		}

		// Create transformation
		transformationID, err := db.CreateTransformation(ctx, record)
		if err != nil {
			db.logger.Warnf("Failed to seed transformation %s: %v", builtIn.Name, err)
			continue
		}

		db.logger.Infof("Seeded transformation %s (ID: %s)", builtIn.Name, transformationID)
	}

	db.logger.Info("Built-in transformations seeded successfully")
	return nil
}
