package mapping

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// DatabaseSchema represents the structure of a database schema
type DatabaseSchema struct {
	Tables map[string]TableSchema `json:"tables"`
}

// TableSchema represents the structure of a table
type TableSchema struct {
	Columns map[string]ColumnSchema `json:"columns"`
}

// ColumnSchema represents the structure of a column
type ColumnSchema struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// ValidationResult holds the result of validation
type ValidationResult struct {
	Valid   bool
	Message string
}

// ValidateSourceColumn validates that a source column exists in the database schema
func (s *Service) ValidateSourceColumn(ctx context.Context, databaseName, tableName, columnName, workspaceID string) (*ValidationResult, error) {
	// Get the database schema
	schema, err := s.getDatabaseSchema(ctx, databaseName, workspaceID)
	if err != nil {
		return &ValidationResult{
			Valid:   false,
			Message: fmt.Sprintf("failed to retrieve database schema: %v", err),
		}, nil
	}

	// Check if table exists
	table, exists := schema.Tables[tableName]
	if !exists {
		return &ValidationResult{
			Valid:   false,
			Message: fmt.Sprintf("table '%s' not found in database '%s'", tableName, databaseName),
		}, nil
	}

	// Check if column exists
	_, exists = table.Columns[columnName]
	if !exists {
		return &ValidationResult{
			Valid:   false,
			Message: fmt.Sprintf("column '%s' not found in table '%s.%s'", columnName, databaseName, tableName),
		}, nil
	}

	return &ValidationResult{
		Valid:   true,
		Message: "source column validated successfully",
	}, nil
}

// ValidateTargetColumn validates that a target column exists in the database schema
func (s *Service) ValidateTargetColumn(ctx context.Context, databaseName, tableName, columnName, workspaceID string) (*ValidationResult, error) {
	// Get the database schema
	schema, err := s.getDatabaseSchema(ctx, databaseName, workspaceID)
	if err != nil {
		return &ValidationResult{
			Valid:   false,
			Message: fmt.Sprintf("failed to retrieve database schema: %v", err),
		}, nil
	}

	// Check if table exists
	table, exists := schema.Tables[tableName]
	if !exists {
		return &ValidationResult{
			Valid:   false,
			Message: fmt.Sprintf("table '%s' not found in database '%s'", tableName, databaseName),
		}, nil
	}

	// Check if column exists
	_, exists = table.Columns[columnName]
	if !exists {
		return &ValidationResult{
			Valid:   false,
			Message: fmt.Sprintf("column '%s' not found in table '%s.%s'", columnName, databaseName, tableName),
		}, nil
	}

	return &ValidationResult{
		Valid:   true,
		Message: "target column validated successfully",
	}, nil
}

// CheckColumnNotMapped checks if a column is already mapped in the same mapping
func (s *Service) CheckColumnNotMapped(ctx context.Context, mappingID, sourceColumn, excludeRuleID string) (*ValidationResult, error) {
	query := `
		SELECT COUNT(*) 
		FROM mapping_rules mr
		JOIN mapping_rule_mappings mrm ON mr.mapping_rule_id = mrm.mapping_rule_id
		WHERE mrm.mapping_id = $1 
		  AND mr.mapping_rule_source = $2
		  AND mr.mapping_rule_id != $3
	`

	var count int
	err := s.db.Pool().QueryRow(ctx, query, mappingID, sourceColumn, excludeRuleID).Scan(&count)
	if err != nil {
		return &ValidationResult{
			Valid:   false,
			Message: fmt.Sprintf("failed to check if column is already mapped: %v", err),
		}, nil
	}

	if count > 0 {
		return &ValidationResult{
			Valid:   false,
			Message: fmt.Sprintf("column '%s' is already mapped in this mapping", sourceColumn),
		}, nil
	}

	return &ValidationResult{
		Valid:   true,
		Message: "column is not already mapped",
	}, nil
}

// getDatabaseSchema retrieves and parses the database schema from the databases table
func (s *Service) getDatabaseSchema(ctx context.Context, databaseName, workspaceID string) (*DatabaseSchema, error) {
	query := `
		SELECT database_schema, database_tables 
		FROM databases 
		WHERE database_name = $1 AND workspace_id = $2
	`

	var schemaJSON, tablesJSON []byte
	err := s.db.Pool().QueryRow(ctx, query, databaseName, workspaceID).Scan(&schemaJSON, &tablesJSON)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("database '%s' not found in workspace", databaseName)
		}
		return nil, fmt.Errorf("failed to query database schema: %w", err)
	}

	// Parse the schema JSON
	var schema DatabaseSchema
	if len(schemaJSON) > 0 {
		if err := json.Unmarshal(schemaJSON, &schema); err != nil {
			// Try parsing database_tables instead
			if len(tablesJSON) > 0 {
				if err := json.Unmarshal(tablesJSON, &schema); err != nil {
					return nil, fmt.Errorf("failed to parse database schema: %w", err)
				}
			} else {
				return nil, fmt.Errorf("failed to parse database schema: %w", err)
			}
		}
	} else if len(tablesJSON) > 0 {
		// Try using database_tables if database_schema is empty
		if err := json.Unmarshal(tablesJSON, &schema); err != nil {
			return nil, fmt.Errorf("failed to parse database tables: %w", err)
		}
	} else {
		return nil, fmt.Errorf("no schema information available for database '%s'", databaseName)
	}

	return &schema, nil
}

// ValidateTypeCompatibility checks if source and target columns have compatible types
func (s *Service) ValidateTypeCompatibility(ctx context.Context, sourceDatabaseName, sourceTableName, sourceColumnName, targetDatabaseName, targetTableName, targetColumnName, workspaceID string) (*ValidationResult, error) {
	// Get source schema
	sourceSchema, err := s.getDatabaseSchema(ctx, sourceDatabaseName, workspaceID)
	if err != nil {
		return &ValidationResult{
			Valid:   false,
			Message: fmt.Sprintf("failed to retrieve source database schema: %v", err),
		}, nil
	}

	// Get target schema
	targetSchema, err := s.getDatabaseSchema(ctx, targetDatabaseName, workspaceID)
	if err != nil {
		return &ValidationResult{
			Valid:   false,
			Message: fmt.Sprintf("failed to retrieve target database schema: %v", err),
		}, nil
	}

	// Get source column
	sourceTable, exists := sourceSchema.Tables[sourceTableName]
	if !exists {
		return &ValidationResult{
			Valid:   false,
			Message: fmt.Sprintf("source table '%s' not found", sourceTableName),
		}, nil
	}

	sourceColumn, exists := sourceTable.Columns[sourceColumnName]
	if !exists {
		return &ValidationResult{
			Valid:   false,
			Message: fmt.Sprintf("source column '%s' not found", sourceColumnName),
		}, nil
	}

	// Get target column
	targetTable, exists := targetSchema.Tables[targetTableName]
	if !exists {
		return &ValidationResult{
			Valid:   false,
			Message: fmt.Sprintf("target table '%s' not found", targetTableName),
		}, nil
	}

	targetColumn, exists := targetTable.Columns[targetColumnName]
	if !exists {
		return &ValidationResult{
			Valid:   false,
			Message: fmt.Sprintf("target column '%s' not found", targetColumnName),
		}, nil
	}

	// Check type compatibility (this is a basic check, can be enhanced)
	compatible := s.areTypesCompatible(sourceColumn.Type, targetColumn.Type)
	if !compatible {
		return &ValidationResult{
			Valid:   false,
			Message: fmt.Sprintf("column types are not compatible: source '%s' (%s) -> target '%s' (%s)", sourceColumnName, sourceColumn.Type, targetColumnName, targetColumn.Type),
		}, nil
	}

	return &ValidationResult{
		Valid:   true,
		Message: "column types are compatible",
	}, nil
}

// areTypesCompatible checks if two column types are compatible
// This is a basic implementation that can be enhanced with more sophisticated type matching
func (s *Service) areTypesCompatible(sourceType, targetType string) bool {
	// Basic type compatibility check
	// TODO: Enhance this with more sophisticated type matching logic

	// Exact match
	if sourceType == targetType {
		return true
	}

	// Compatible string types
	stringTypes := map[string]bool{
		"varchar": true, "char": true, "text": true, "string": true,
		"VARCHAR": true, "CHAR": true, "TEXT": true, "STRING": true,
	}
	if stringTypes[sourceType] && stringTypes[targetType] {
		return true
	}

	// Compatible numeric types
	numericTypes := map[string]bool{
		"int": true, "integer": true, "bigint": true, "smallint": true,
		"INT": true, "INTEGER": true, "BIGINT": true, "SMALLINT": true,
		"float": true, "double": true, "decimal": true, "numeric": true,
		"FLOAT": true, "DOUBLE": true, "DECIMAL": true, "NUMERIC": true,
	}
	if numericTypes[sourceType] && numericTypes[targetType] {
		return true
	}

	// Compatible date/time types
	dateTimeTypes := map[string]bool{
		"date": true, "time": true, "datetime": true, "timestamp": true,
		"DATE": true, "TIME": true, "DATETIME": true, "TIMESTAMP": true,
	}
	if dateTimeTypes[sourceType] && dateTimeTypes[targetType] {
		return true
	}

	// Compatible boolean types
	booleanTypes := map[string]bool{
		"bool": true, "boolean": true, "BOOL": true, "BOOLEAN": true,
	}
	if booleanTypes[sourceType] && booleanTypes[targetType] {
		return true
	}

	// If not explicitly compatible, return false
	return false
}
