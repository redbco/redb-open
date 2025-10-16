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
// NOTE: Updated for new workflow-based schema - checks metadata for source_identifier
func (s *Service) CheckColumnNotMapped(ctx context.Context, mappingID, sourceColumn, excludeRuleID string) (*ValidationResult, error) {
	query := `
		SELECT COUNT(*) 
		FROM mapping_rules mr
		JOIN mapping_rule_mappings mrm ON mr.mapping_rule_id = mrm.mapping_rule_id
		WHERE mrm.mapping_id = $1 
		  AND mr.mapping_rule_metadata->>'source_identifier' = $2
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
// Accepts either database name or database ID (starting with 'db_')
func (s *Service) getDatabaseSchema(ctx context.Context, databaseNameOrID, workspaceID string) (*DatabaseSchema, error) {
	var query string
	var args []interface{}

	// Check if it's a database ID (starts with 'db_') or a database name
	if len(databaseNameOrID) > 3 && databaseNameOrID[:3] == "db_" {
		// It's a database ID
		query = `
			SELECT database_schema, database_tables 
			FROM databases 
			WHERE database_id = $1 AND workspace_id = $2
		`
		args = []interface{}{databaseNameOrID, workspaceID}
	} else {
		// It's a database name
		query = `
			SELECT database_schema, database_tables 
			FROM databases 
			WHERE database_name = $1 AND workspace_id = $2
		`
		args = []interface{}{databaseNameOrID, workspaceID}
	}

	var schemaJSON, tablesJSON []byte
	err := s.db.Pool().QueryRow(ctx, query, args...).Scan(&schemaJSON, &tablesJSON)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("database '%s' not found in workspace", databaseNameOrID)
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
		return nil, fmt.Errorf("no schema information available for database '%s'", databaseNameOrID)
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

// MappingValidationResult holds the complete validation result for a mapping
type MappingValidationResult struct {
	IsValid  bool
	Errors   []string
	Warnings []string
}

// ValidateMappingComplete performs complete validation of a mapping
func (s *Service) ValidateMappingComplete(ctx context.Context, mappingID, workspaceID, tenantID string) (*MappingValidationResult, error) {
	result := &MappingValidationResult{
		IsValid:  true,
		Errors:   []string{},
		Warnings: []string{},
	}

	// Get the mapping details
	mapping, err := s.GetByID(ctx, mappingID)
	if err != nil {
		return nil, fmt.Errorf("failed to get mapping: %w", err)
	}

	// Get mapping rules with their workflows
	rules, err := s.GetRulesByMappingID(ctx, mappingID)
	if err != nil {
		return nil, fmt.Errorf("failed to get mapping rules: %w", err)
	}

	// Validate target consistency - all rules must target the same database and table
	consistencyErrors, consistencyWarnings, err := s.ValidateTargetConsistency(ctx, mapping, rules)
	if err != nil {
		return nil, fmt.Errorf("failed to validate target consistency: %w", err)
	}
	result.Errors = append(result.Errors, consistencyErrors...)
	result.Warnings = append(result.Warnings, consistencyWarnings...)

	// Validate target coverage
	coverageErrors, coverageWarnings, err := s.ValidateTargetCoverage(ctx, mapping, rules, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("failed to validate target coverage: %w", err)
	}
	result.Errors = append(result.Errors, coverageErrors...)
	result.Warnings = append(result.Warnings, coverageWarnings...)

	// Validate transformation workflows
	workflowErrors, workflowWarnings, err := s.ValidateTransformationWorkflows(ctx, rules)
	if err != nil {
		return nil, fmt.Errorf("failed to validate transformation workflows: %w", err)
	}
	result.Errors = append(result.Errors, workflowErrors...)
	result.Warnings = append(result.Warnings, workflowWarnings...)

	// Validate data type compatibility
	typeErrors, typeWarnings, err := s.ValidateDataTypeCompatibility(ctx, mapping, rules, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("failed to validate data type compatibility: %w", err)
	}
	result.Errors = append(result.Errors, typeErrors...)
	result.Warnings = append(result.Warnings, typeWarnings...)

	// Set overall validity
	if len(result.Errors) > 0 {
		result.IsValid = false
	}

	return result, nil
}

// ValidateTargetConsistency validates that all mapping rules target the same database and table
func (s *Service) ValidateTargetConsistency(ctx context.Context, mapping *Mapping, rules []*Rule) ([]string, []string, error) {
	errors := []string{}
	warnings := []string{}

	// Only validate table mappings for now
	if mapping.MappingType != "table" {
		return errors, warnings, nil
	}

	// Need at least one rule to validate
	if len(rules) == 0 {
		warnings = append(warnings, "No mapping rules defined for this mapping")
		return errors, warnings, nil
	}

	// Track target database and table from first valid rule
	var expectedTargetDB, expectedTargetTable string
	firstRuleSet := false

	// Check all rules for target consistency
	for _, rule := range rules {
		// Extract target identifier from metadata
		targetIdentifier, ok := rule.Metadata["target_identifier"].(string)
		if !ok || targetIdentifier == "" {
			warnings = append(warnings, fmt.Sprintf("Rule '%s': Missing target identifier in metadata", rule.Name))
			continue
		}

		// Parse target identifier
		targetInfo, err := s.parseIdentifier(targetIdentifier)
		if err != nil {
			errors = append(errors, fmt.Sprintf("Rule '%s': Failed to parse target identifier: %v", rule.Name, err))
			continue
		}

		// Set expected target on first valid rule
		if !firstRuleSet {
			expectedTargetDB = targetInfo.DatabaseName
			expectedTargetTable = targetInfo.TableName
			firstRuleSet = true
			continue
		}

		// Check consistency with expected target
		if targetInfo.DatabaseName != expectedTargetDB || targetInfo.TableName != expectedTargetTable {
			errors = append(errors, fmt.Sprintf(
				"Rule '%s': Inconsistent target. Expected '%s.%s' but found '%s.%s'",
				rule.Name,
				expectedTargetDB,
				expectedTargetTable,
				targetInfo.DatabaseName,
				targetInfo.TableName,
			))
		}
	}

	return errors, warnings, nil
}

// ValidateTargetCoverage validates that all required target columns have mapping rules
func (s *Service) ValidateTargetCoverage(ctx context.Context, mapping *Mapping, rules []*Rule, workspaceID string) ([]string, []string, error) {
	errors := []string{}
	warnings := []string{}

	// Parse the mapping type to determine target database and table
	// For table mappings, we need to check the target table columns
	if mapping.MappingType != "table" {
		// Only validate table mappings for now
		return errors, warnings, nil
	}

	// Extract target database and table from mapping rules
	// We'll look at the first rule to get the target info
	if len(rules) == 0 {
		warnings = append(warnings, "No mapping rules defined for this mapping")
		return errors, warnings, nil
	}

	// Get target database and table from the first rule's target identifier
	// Format: db://database_name.table_name.column_name
	// Extract from metadata (backward compatibility)
	targetIdentifier, ok := rules[0].Metadata["target_identifier"].(string)
	if !ok || targetIdentifier == "" {
		errors = append(errors, "Failed to get target identifier from rule metadata")
		return errors, warnings, nil
	}
	targetInfo, err := s.parseIdentifier(targetIdentifier)
	if err != nil {
		errors = append(errors, fmt.Sprintf("Failed to parse target identifier: %v", err))
		return errors, warnings, nil
	}

	// Get the target table schema
	schema, err := s.getDatabaseSchema(ctx, targetInfo.DatabaseName, workspaceID)
	if err != nil {
		errors = append(errors, fmt.Sprintf("Failed to retrieve target database schema: %v", err))
		return errors, warnings, nil
	}

	targetTable, exists := schema.Tables[targetInfo.TableName]
	if !exists {
		errors = append(errors, fmt.Sprintf("Target table '%s' not found in database '%s'", targetInfo.TableName, targetInfo.DatabaseName))
		return errors, warnings, nil
	}

	// Build a set of target columns covered by mapping rules
	coveredColumns := make(map[string]bool)
	for _, rule := range rules {
		// Extract target identifier from metadata
		ruleTargetIdentifier, ok := rule.Metadata["target_identifier"].(string)
		if !ok || ruleTargetIdentifier == "" {
			continue
		}
		ruleTargetInfo, err := s.parseIdentifier(ruleTargetIdentifier)
		if err != nil {
			continue
		}
		coveredColumns[ruleTargetInfo.ColumnName] = true
	}

	// Check each target column
	for columnName, column := range targetTable.Columns {
		if !coveredColumns[columnName] {
			if !column.Nullable {
				errors = append(errors, fmt.Sprintf("Required target column '%s' is not mapped", columnName))
			} else {
				warnings = append(warnings, fmt.Sprintf("Optional target column '%s' is not mapped", columnName))
			}
		}
	}

	return errors, warnings, nil
}

// ValidateTransformationWorkflows validates the transformation workflows in mapping rules
func (s *Service) ValidateTransformationWorkflows(ctx context.Context, rules []*Rule) ([]string, []string, error) {
	errors := []string{}
	warnings := []string{}

	for _, rule := range rules {
		// For simple workflow type, no complex validation needed
		if rule.Metadata != nil {
			workflowType, ok := rule.Metadata["workflow_type"].(string)
			if ok && workflowType == "workflow" {
				// Load workflow nodes and edges from database
				nodes, edges, err := s.getWorkflowNodesAndEdges(ctx, rule.ID)
				if err != nil {
					errors = append(errors, fmt.Sprintf("Failed to load workflow for rule '%s': %v", rule.Name, err))
					continue
				}

				// Validate the workflow structure
				workflowErrors := s.validateWorkflowStructure(nodes, edges)
				for _, err := range workflowErrors {
					errors = append(errors, fmt.Sprintf("Rule '%s': %s", rule.Name, err))
				}
			}
		}

		// Check if transformation is valid (extract from metadata)
		if transformationName, ok := rule.Metadata["transformation_name"].(string); ok && transformationName != "" {
			// For now, we'll skip transformation validation as transformations are being refactored
			// TODO: Re-implement transformation validation with new workflow system
			_ = transformationName
		}
	}

	return errors, warnings, nil
}

// ValidateDataTypeCompatibility validates data type compatibility
func (s *Service) ValidateDataTypeCompatibility(ctx context.Context, mapping *Mapping, rules []*Rule, workspaceID string) ([]string, []string, error) {
	errors := []string{}
	warnings := []string{}

	for _, rule := range rules {
		// Parse source and target identifiers (from metadata)
		sourceIdentifier, ok := rule.Metadata["source_identifier"].(string)
		if !ok || sourceIdentifier == "" {
			errors = append(errors, fmt.Sprintf("Rule '%s': No source identifier in metadata", rule.Name))
			continue
		}
		sourceInfo, err := s.parseIdentifier(sourceIdentifier)
		if err != nil {
			errors = append(errors, fmt.Sprintf("Rule '%s': Failed to parse source identifier: %v", rule.Name, err))
			continue
		}

		targetIdentifier, ok := rule.Metadata["target_identifier"].(string)
		if !ok || targetIdentifier == "" {
			errors = append(errors, fmt.Sprintf("Rule '%s': No target identifier in metadata", rule.Name))
			continue
		}
		targetInfo, err := s.parseIdentifier(targetIdentifier)
		if err != nil {
			errors = append(errors, fmt.Sprintf("Rule '%s': Failed to parse target identifier: %v", rule.Name, err))
			continue
		}

		// Get source schema
		sourceSchema, err := s.getDatabaseSchema(ctx, sourceInfo.DatabaseName, workspaceID)
		if err != nil {
			errors = append(errors, fmt.Sprintf("Rule '%s': Failed to retrieve source schema: %v", rule.Name, err))
			continue
		}

		// Get target schema
		targetSchema, err := s.getDatabaseSchema(ctx, targetInfo.DatabaseName, workspaceID)
		if err != nil {
			errors = append(errors, fmt.Sprintf("Rule '%s': Failed to retrieve target schema: %v", rule.Name, err))
			continue
		}

		// Get source column type
		sourceTable, exists := sourceSchema.Tables[sourceInfo.TableName]
		if !exists {
			errors = append(errors, fmt.Sprintf("Rule '%s': Source table '%s' not found", rule.Name, sourceInfo.TableName))
			continue
		}

		sourceColumn, exists := sourceTable.Columns[sourceInfo.ColumnName]
		if !exists {
			errors = append(errors, fmt.Sprintf("Rule '%s': Source column '%s' not found", rule.Name, sourceInfo.ColumnName))
			continue
		}

		// Get target column type
		targetTable, exists := targetSchema.Tables[targetInfo.TableName]
		if !exists {
			errors = append(errors, fmt.Sprintf("Rule '%s': Target table '%s' not found", rule.Name, targetInfo.TableName))
			continue
		}

		targetColumn, exists := targetTable.Columns[targetInfo.ColumnName]
		if !exists {
			errors = append(errors, fmt.Sprintf("Rule '%s': Target column '%s' not found", rule.Name, targetInfo.ColumnName))
			continue
		}

		// Determine final output type
		finalType := sourceColumn.Type
		// Check if there's a transformation (from metadata)
		if transformationName, ok := rule.Metadata["transformation_name"].(string); ok && transformationName != "" {
			// If there's a transformation, we would need to get its output type
			// For now, we'll use a simplified approach
			// TODO: Get actual transformation output type from new workflow system
			_ = transformationName
		}

		// Check type compatibility
		if !s.areTypesCompatible(finalType, targetColumn.Type) {
			errors = append(errors, fmt.Sprintf("Rule '%s': Incompatible types - source/transformation type '%s' cannot be assigned to target type '%s'", rule.Name, finalType, targetColumn.Type))
		}
	}

	return errors, warnings, nil
}

// IdentifierInfo holds parsed identifier information
type IdentifierInfo struct {
	DatabaseName string
	TableName    string
	ColumnName   string
}

// parseIdentifier parses a database identifier in the format "db://database_name.table_name.column_name"
func (s *Service) parseIdentifier(identifier string) (*IdentifierInfo, error) {
	// Remove the prefix if present
	cleanIdentifier := identifier
	if len(identifier) > 5 && identifier[:5] == "db://" {
		cleanIdentifier = identifier[5:]
	} else if len(identifier) > 6 && identifier[:6] == "@db://" {
		cleanIdentifier = identifier[6:]
	}

	// Split by dots - parse the identifier
	parts := make([]string, 0)
	current := ""
	for _, char := range cleanIdentifier {
		if char == '.' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(char)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}

	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid identifier format: expected 'database.table.column', got '%s'", identifier)
	}

	return &IdentifierInfo{
		DatabaseName: parts[0],
		TableName:    parts[1],
		ColumnName:   parts[2],
	}, nil
}

// WorkflowNode represents a workflow node
type WorkflowNode struct {
	NodeID           string
	NodeType         string
	TransformationID string
	NodeConfig       map[string]interface{}
	NodeOrder        int
}

// WorkflowEdge represents a workflow edge
type WorkflowEdge struct {
	EdgeID           string
	SourceNodeID     string
	SourceOutputName string
	TargetNodeID     string
	TargetInputName  string
}

// getWorkflowNodesAndEdges retrieves workflow nodes and edges for a mapping rule
func (s *Service) getWorkflowNodesAndEdges(ctx context.Context, mappingRuleID string) ([]*WorkflowNode, []*WorkflowEdge, error) {
	nodes := []*WorkflowNode{}
	edges := []*WorkflowEdge{}

	// Query workflow nodes
	nodesQuery := `
		SELECT node_id, node_type, transformation_id, node_config, node_order
		FROM transformation_workflow_nodes
		WHERE mapping_rule_id = $1
		ORDER BY node_order
	`
	nodesRows, err := s.db.Pool().Query(ctx, nodesQuery, mappingRuleID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query workflow nodes: %w", err)
	}
	defer nodesRows.Close()

	for nodesRows.Next() {
		node := &WorkflowNode{}
		var transformationID *string
		err := nodesRows.Scan(&node.NodeID, &node.NodeType, &transformationID, &node.NodeConfig, &node.NodeOrder)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan workflow node: %w", err)
		}
		if transformationID != nil {
			node.TransformationID = *transformationID
		}
		nodes = append(nodes, node)
	}

	// Query workflow edges
	edgesQuery := `
		SELECT edge_id, source_node_id, source_output_name, target_node_id, target_input_name
		FROM transformation_workflow_edges
		WHERE mapping_rule_id = $1
	`
	edgesRows, err := s.db.Pool().Query(ctx, edgesQuery, mappingRuleID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query workflow edges: %w", err)
	}
	defer edgesRows.Close()

	for edgesRows.Next() {
		edge := &WorkflowEdge{}
		err := edgesRows.Scan(&edge.EdgeID, &edge.SourceNodeID, &edge.SourceOutputName, &edge.TargetNodeID, &edge.TargetInputName)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan workflow edge: %w", err)
		}
		edges = append(edges, edge)
	}

	return nodes, edges, nil
}

// validateWorkflowStructure validates the structure of a workflow
func (s *Service) validateWorkflowStructure(nodes []*WorkflowNode, edges []*WorkflowEdge) []string {
	errors := []string{}

	if len(nodes) == 0 {
		errors = append(errors, "Workflow has no nodes")
		return errors
	}

	// Build node map for quick lookup
	nodeMap := make(map[string]*WorkflowNode)
	for _, node := range nodes {
		nodeMap[node.NodeID] = node
	}

	// Validate edges
	for _, edge := range edges {
		if _, exists := nodeMap[edge.SourceNodeID]; !exists {
			errors = append(errors, fmt.Sprintf("Edge references non-existent source node: %s", edge.SourceNodeID))
		}
		if _, exists := nodeMap[edge.TargetNodeID]; !exists {
			errors = append(errors, fmt.Sprintf("Edge references non-existent target node: %s", edge.TargetNodeID))
		}
	}

	// Check for at least one source and one target node
	hasSource := false
	hasTarget := false
	for _, node := range nodes {
		if node.NodeType == "source" {
			hasSource = true
		}
		if node.NodeType == "target" {
			hasTarget = true
		}
	}

	if !hasSource {
		errors = append(errors, "Workflow has no source node")
	}
	if !hasTarget {
		errors = append(errors, "Workflow has no target node")
	}

	return errors
}
