package generators

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// Neo4jGenerator implements Cypher generation for Neo4j graph database
type Neo4jGenerator struct {
	BaseGenerator
}

// NewNeo4jGenerator creates a new Neo4j generator
func NewNeo4jGenerator() *Neo4jGenerator {
	return &Neo4jGenerator{}
}

// Override BaseGenerator methods to provide Neo4j-specific implementations

// Structural organization (Neo4j uses databases in 4.0+)
func (g *Neo4jGenerator) GenerateCreateDatabaseSQL(database unifiedmodel.Database) (string, error) {
	if database.Name == "" {
		return "", fmt.Errorf("database name cannot be empty")
	}

	// Neo4j 4.0+ supports multiple databases
	return fmt.Sprintf("CREATE DATABASE %s;", database.Name), nil
}

// Graph-specific objects (Neo4j primary focus)
func (g *Neo4jGenerator) GenerateCreateGraphSQL(graph unifiedmodel.Graph) (string, error) {
	if graph.Name == "" {
		return "", fmt.Errorf("graph name cannot be empty")
	}

	// Neo4j Graph Data Science (GDS) library graph creation
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CALL gds.graph.project('%s', ", graph.Name))

	// Add node labels
	if len(graph.NodeLabels) > 0 {
		var labels []string
		for label := range graph.NodeLabels {
			labels = append(labels, fmt.Sprintf("'%s'", label))
		}
		sb.WriteString(fmt.Sprintf("[%s], ", strings.Join(labels, ", ")))
	} else {
		sb.WriteString("'*', ") // All node labels
	}

	// Add relationship types
	if len(graph.RelTypes) > 0 {
		var relTypes []string
		for relType := range graph.RelTypes {
			relTypes = append(relTypes, fmt.Sprintf("'%s'", relType))
		}
		sb.WriteString(fmt.Sprintf("[%s]", strings.Join(relTypes, ", ")))
	} else {
		sb.WriteString("'*'") // All relationship types
	}

	// Add configuration if present (from options or default)
	sb.WriteString(", {}")

	sb.WriteString(");")

	return sb.String(), nil
}

func (g *Neo4jGenerator) GenerateCreateNodeSQL(node unifiedmodel.Node) (string, error) {
	if node.Label == "" {
		return "", fmt.Errorf("node label cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString("CREATE (")

	// Add node variable (default to 'n')
	sb.WriteString("n")

	// Add label
	sb.WriteString(fmt.Sprintf(":%s", node.Label))

	// Add properties
	if len(node.Properties) > 0 {
		sb.WriteString(" {")
		var props []string
		for key, prop := range node.Properties {
			// Use property name and type to create a default value
			defaultValue := g.getDefaultValueForType(prop.Type)
			props = append(props, fmt.Sprintf("%s: %v", key, g.formatPropertyValue(defaultValue)))
		}
		sb.WriteString(strings.Join(props, ", "))
		sb.WriteString("}")
	}

	sb.WriteString(");")

	return sb.String(), nil
}

func (g *Neo4jGenerator) GenerateCreateRelationshipSQL(relationship unifiedmodel.Relationship) (string, error) {
	if relationship.Type == "" {
		return "", fmt.Errorf("relationship type cannot be empty")
	}

	var sb strings.Builder

	// Create relationship between nodes using labels
	fromLabel := relationship.FromLabel
	toLabel := relationship.ToLabel
	if fromLabel == "" {
		fromLabel = "Node"
	}
	if toLabel == "" {
		toLabel = "Node"
	}

	sb.WriteString(fmt.Sprintf("MATCH (a:%s), (b:%s) ", fromLabel, toLabel))
	sb.WriteString(fmt.Sprintf("CREATE (a)-[r:%s", relationship.Type))

	// Add relationship properties
	if len(relationship.Properties) > 0 {
		sb.WriteString(" {")
		var props []string
		for key, prop := range relationship.Properties {
			// Use property name and type to create a default value
			defaultValue := g.getDefaultValueForType(prop.Type)
			props = append(props, fmt.Sprintf("%s: %v", key, g.formatPropertyValue(defaultValue)))
		}
		sb.WriteString(strings.Join(props, ", "))
		sb.WriteString("}")
	}

	sb.WriteString("]->(b);")

	return sb.String(), nil
}

// Integrity, performance and identity objects
func (g *Neo4jGenerator) GenerateCreateIndexSQL(index unifiedmodel.Index) (string, error) {
	if index.Name == "" {
		return "", fmt.Errorf("index name cannot be empty")
	}

	var sb strings.Builder

	// Determine index type
	indexType := "INDEX"
	if indexTypeStr, ok := index.Options["type"].(string); ok {
		switch strings.ToUpper(indexTypeStr) {
		case "FULLTEXT":
			indexType = "FULLTEXT INDEX"
		case "TEXT":
			indexType = "TEXT INDEX"
		case "LOOKUP":
			indexType = "LOOKUP INDEX"
		case "POINT":
			indexType = "POINT INDEX"
		case "RANGE":
			indexType = "RANGE INDEX"
		}
	}

	sb.WriteString(fmt.Sprintf("CREATE %s %s IF NOT EXISTS ", indexType, index.Name))

	// Determine if this is for nodes or relationships
	isRelationshipIndex := false
	if relIndex, ok := index.Options["relationship"].(bool); ok && relIndex {
		isRelationshipIndex = true
	}

	if isRelationshipIndex {
		// Relationship index
		relType := "REL_TYPE"
		if len(index.Fields) > 0 {
			relType = index.Fields[0]
		}
		sb.WriteString(fmt.Sprintf("FOR ()-[r:%s]-() ON (", relType))

		// Add properties
		if len(index.Columns) > 0 {
			var props []string
			for _, col := range index.Columns {
				props = append(props, fmt.Sprintf("r.%s", col))
			}
			sb.WriteString(strings.Join(props, ", "))
		} else if index.Expression != "" {
			sb.WriteString(index.Expression)
		}
	} else {
		// Node index
		nodeLabel := "Node"
		if len(index.Fields) > 0 {
			nodeLabel = index.Fields[0]
		}
		sb.WriteString(fmt.Sprintf("FOR (n:%s) ON (", nodeLabel))

		// Add properties
		if len(index.Columns) > 0 {
			var props []string
			for _, col := range index.Columns {
				props = append(props, fmt.Sprintf("n.%s", col))
			}
			sb.WriteString(strings.Join(props, ", "))
		} else if index.Expression != "" {
			sb.WriteString(index.Expression)
		}
	}

	sb.WriteString(");")

	return sb.String(), nil
}

func (g *Neo4jGenerator) GenerateCreateConstraintSQL(constraint unifiedmodel.Constraint) (string, error) {
	if constraint.Name == "" {
		return "", fmt.Errorf("constraint name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE CONSTRAINT %s IF NOT EXISTS ", constraint.Name))

	// Determine constraint target (node or relationship)
	isRelationshipConstraint := false
	if relConstraint, ok := constraint.Options["relationship"].(bool); ok && relConstraint {
		isRelationshipConstraint = true
	}

	if isRelationshipConstraint {
		// Relationship constraint
		relType := "REL_TYPE"
		if len(constraint.Columns) > 0 {
			relType = constraint.Columns[0]
		}
		sb.WriteString(fmt.Sprintf("FOR ()-[r:%s]-() ", relType))
	} else {
		// Node constraint
		nodeLabel := "Node"
		if len(constraint.Columns) > 0 {
			nodeLabel = constraint.Columns[0]
		}
		sb.WriteString(fmt.Sprintf("FOR (n:%s) ", nodeLabel))
	}

	// Add constraint type
	switch constraint.Type {
	case unifiedmodel.ConstraintTypeUnique:
		if isRelationshipConstraint {
			sb.WriteString("REQUIRE r.")
		} else {
			sb.WriteString("REQUIRE n.")
		}
		if len(constraint.Columns) > 1 {
			var props []string
			for i := 1; i < len(constraint.Columns); i++ {
				props = append(props, constraint.Columns[i])
			}
			sb.WriteString(strings.Join(props, ", "))
		}
		sb.WriteString(" IS UNIQUE")

	case unifiedmodel.ConstraintTypeNotNull:
		if isRelationshipConstraint {
			sb.WriteString("REQUIRE r.")
		} else {
			sb.WriteString("REQUIRE n.")
		}
		if len(constraint.Columns) > 1 {
			var props []string
			for i := 1; i < len(constraint.Columns); i++ {
				props = append(props, constraint.Columns[i])
			}
			sb.WriteString(strings.Join(props, " IS NOT NULL AND "))
		}
		sb.WriteString(" IS NOT NULL")

	case unifiedmodel.ConstraintTypeCheck:
		if constraint.Expression != "" {
			sb.WriteString(fmt.Sprintf("REQUIRE %s", constraint.Expression))
		}

	default:
		return "", fmt.Errorf("unsupported constraint type: %s", constraint.Type)
	}

	sb.WriteString(";")

	return sb.String(), nil
}

// Executable code objects
func (g *Neo4jGenerator) GenerateCreateFunctionSQL(fn unifiedmodel.Function) (string, error) {
	if fn.Name == "" {
		return "", fmt.Errorf("function name cannot be empty")
	}

	// Neo4j user-defined functions (UDF) - requires Java/Scala implementation
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("// User-defined function: %s\n", fn.Name))
	sb.WriteString("// This requires a Java/Scala implementation and deployment\n")
	sb.WriteString(fmt.Sprintf("// Function signature: %s(", fn.Name))

	// Add arguments
	var argDefs []string
	for _, arg := range fn.Arguments {
		argDefs = append(argDefs, fmt.Sprintf("%s: %s", arg.Name, g.mapDataType(arg.Type)))
	}
	sb.WriteString(strings.Join(argDefs, ", "))
	sb.WriteString(")")

	// Add return type
	if fn.Returns != "" {
		sb.WriteString(fmt.Sprintf(" -> %s", g.mapDataType(fn.Returns)))
	}

	sb.WriteString("\n// Implementation needed in Java/Scala")

	return sb.String(), nil
}

func (g *Neo4jGenerator) GenerateCreateProcedureSQL(procedure unifiedmodel.Procedure) (string, error) {
	if procedure.Name == "" {
		return "", fmt.Errorf("procedure name cannot be empty")
	}

	// Neo4j user-defined procedures - requires Java/Scala implementation
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("// User-defined procedure: %s\n", procedure.Name))
	sb.WriteString("// This requires a Java/Scala implementation and deployment\n")
	sb.WriteString(fmt.Sprintf("// Procedure signature: %s(", procedure.Name))

	// Add arguments
	var argDefs []string
	for _, arg := range procedure.Arguments {
		argDefs = append(argDefs, fmt.Sprintf("%s: %s", arg.Name, g.mapDataType(arg.Type)))
	}
	sb.WriteString(strings.Join(argDefs, ", "))
	sb.WriteString(")")

	sb.WriteString("\n// Implementation needed in Java/Scala")

	return sb.String(), nil
}

// Security and access control
func (g *Neo4jGenerator) GenerateCreateUserSQL(user unifiedmodel.DBUser) (string, error) {
	if user.Name == "" {
		return "", fmt.Errorf("user name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE USER %s", user.Name))

	// Add password if specified in options
	if password, ok := user.Options["password"].(string); ok && password != "" {
		sb.WriteString(fmt.Sprintf(" SET PASSWORD '%s'", password))
	}

	// Add password change requirement
	if requireChange, ok := user.Options["require_password_change"].(bool); ok && requireChange {
		sb.WriteString(" SET PASSWORD CHANGE REQUIRED")
	} else {
		sb.WriteString(" SET PASSWORD CHANGE NOT REQUIRED")
	}

	// Add status
	if suspended, ok := user.Options["suspended"].(bool); ok && suspended {
		sb.WriteString(" SET STATUS SUSPENDED")
	} else {
		sb.WriteString(" SET STATUS ACTIVE")
	}

	sb.WriteString(";")

	return sb.String(), nil
}

func (g *Neo4jGenerator) GenerateCreateRoleSQL(role unifiedmodel.DBRole) (string, error) {
	if role.Name == "" {
		return "", fmt.Errorf("role name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE ROLE %s", role.Name))

	// Add IF NOT EXISTS
	sb.WriteString(" IF NOT EXISTS")

	// Add AS COPY OF if specified (Neo4j doesn't have this feature, but we can comment it)
	// Neo4j roles don't support copying from other roles directly

	sb.WriteString(";")

	return sb.String(), nil
}

func (g *Neo4jGenerator) GenerateCreateGrantSQL(grant unifiedmodel.Grant) (string, error) {
	if grant.Principal == "" {
		return "", fmt.Errorf("grant principal cannot be empty")
	}
	if grant.Privilege == "" {
		return "", fmt.Errorf("grant privilege cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("GRANT %s", grant.Privilege))

	// Add object specification
	if grant.Object != "" {
		sb.WriteString(fmt.Sprintf(" ON %s", grant.Object))
	} else {
		sb.WriteString(" ON GRAPH *") // Default to all graphs
	}

	sb.WriteString(fmt.Sprintf(" TO %s", grant.Principal))

	sb.WriteString(";")

	return sb.String(), nil
}

// High-level generation methods
func (g *Neo4jGenerator) GenerateSchema(model *unifiedmodel.UnifiedModel) (string, []string, error) {
	if model == nil {
		return "", nil, fmt.Errorf("unified model cannot be nil")
	}

	var statements []string
	var warnings []string

	// Generate in dependency order for Neo4j

	// 1. Databases first (Neo4j 4.0+)
	for _, database := range model.Databases {
		stmt, err := g.GenerateCreateDatabaseSQL(database)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate database %s: %v", database.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 2. Graphs (GDS library)
	for _, graph := range model.Graphs {
		stmt, err := g.GenerateCreateGraphSQL(graph)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate graph %s: %v", graph.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 3. Nodes (create sample nodes)
	for _, node := range model.Nodes {
		stmt, err := g.GenerateCreateNodeSQL(node)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate node %s: %v", node.Label, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 4. Relationships (depend on nodes)
	for _, relationship := range model.Relationships {
		stmt, err := g.GenerateCreateRelationshipSQL(relationship)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate relationship %s: %v", relationship.Type, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 5. Constraints (integrity rules)
	for _, constraint := range model.Constraints {
		stmt, err := g.GenerateCreateConstraintSQL(constraint)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate constraint %s: %v", constraint.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 6. Indexes (performance optimization)
	for _, index := range model.Indexes {
		stmt, err := g.GenerateCreateIndexSQL(index)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate index %s: %v", index.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 7. Functions and Procedures (UDFs/UDPs)
	for _, fn := range model.Functions {
		stmt, err := g.GenerateCreateFunctionSQL(fn)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate function %s: %v", fn.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	for _, proc := range model.Procedures {
		stmt, err := g.GenerateCreateProcedureSQL(proc)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate procedure %s: %v", proc.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 8. Roles
	for _, role := range model.Roles {
		stmt, err := g.GenerateCreateRoleSQL(role)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate role %s: %v", role.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 9. Users
	for _, user := range model.Users {
		stmt, err := g.GenerateCreateUserSQL(user)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate user %s: %v", user.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 10. Grants (should be last)
	for _, grant := range model.Grants {
		stmt, err := g.GenerateCreateGrantSQL(grant)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate grant for %s: %v", grant.Principal, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// Combine all statements
	fullScript := strings.Join(statements, "\n\n")

	return fullScript, warnings, nil
}

func (g *Neo4jGenerator) GenerateCreateStatements(model *unifiedmodel.UnifiedModel) ([]string, error) {
	if model == nil {
		return nil, fmt.Errorf("unified model cannot be nil")
	}

	var statements []string

	// Generate in dependency order (same as GenerateSchema but with error propagation)

	// 1. Databases first
	for _, database := range model.Databases {
		stmt, err := g.GenerateCreateDatabaseSQL(database)
		if err != nil {
			return nil, fmt.Errorf("failed to generate database %s: %w", database.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 2. Graphs
	for _, graph := range model.Graphs {
		stmt, err := g.GenerateCreateGraphSQL(graph)
		if err != nil {
			return nil, fmt.Errorf("failed to generate graph %s: %w", graph.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 3. Nodes
	for _, node := range model.Nodes {
		stmt, err := g.GenerateCreateNodeSQL(node)
		if err != nil {
			return nil, fmt.Errorf("failed to generate node %s: %w", node.Label, err)
		}
		statements = append(statements, stmt)
	}

	// 4. Relationships
	for _, relationship := range model.Relationships {
		stmt, err := g.GenerateCreateRelationshipSQL(relationship)
		if err != nil {
			return nil, fmt.Errorf("failed to generate relationship %s: %w", relationship.Type, err)
		}
		statements = append(statements, stmt)
	}

	// 5. Constraints
	for _, constraint := range model.Constraints {
		stmt, err := g.GenerateCreateConstraintSQL(constraint)
		if err != nil {
			return nil, fmt.Errorf("failed to generate constraint %s: %w", constraint.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 6. Indexes
	for _, index := range model.Indexes {
		stmt, err := g.GenerateCreateIndexSQL(index)
		if err != nil {
			return nil, fmt.Errorf("failed to generate index %s: %w", index.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 7. Functions
	for _, fn := range model.Functions {
		stmt, err := g.GenerateCreateFunctionSQL(fn)
		if err != nil {
			return nil, fmt.Errorf("failed to generate function %s: %w", fn.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 8. Procedures
	for _, proc := range model.Procedures {
		stmt, err := g.GenerateCreateProcedureSQL(proc)
		if err != nil {
			return nil, fmt.Errorf("failed to generate procedure %s: %w", proc.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 9. Roles
	for _, role := range model.Roles {
		stmt, err := g.GenerateCreateRoleSQL(role)
		if err != nil {
			return nil, fmt.Errorf("failed to generate role %s: %w", role.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 10. Users
	for _, user := range model.Users {
		stmt, err := g.GenerateCreateUserSQL(user)
		if err != nil {
			return nil, fmt.Errorf("failed to generate user %s: %w", user.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 11. Grants
	for _, grant := range model.Grants {
		stmt, err := g.GenerateCreateGrantSQL(grant)
		if err != nil {
			return nil, fmt.Errorf("failed to generate grant for %s: %w", grant.Principal, err)
		}
		statements = append(statements, stmt)
	}

	return statements, nil
}

// Helper methods

func (g *Neo4jGenerator) formatPropertyValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "\\'"))
	case int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", v)
	case bool:
		return fmt.Sprintf("%t", v)
	case []interface{}:
		var items []string
		for _, item := range v {
			items = append(items, g.formatPropertyValue(item))
		}
		return fmt.Sprintf("[%s]", strings.Join(items, ", "))
	case map[string]interface{}:
		var props []string
		for key, val := range v {
			props = append(props, fmt.Sprintf("%s: %s", key, g.formatPropertyValue(val)))
		}
		return fmt.Sprintf("{%s}", strings.Join(props, ", "))
	default:
		return fmt.Sprintf("'%v'", v)
	}
}

func (g *Neo4jGenerator) mapDataType(dataType string) string {
	// Map common data types to Neo4j/Cypher equivalents
	switch strings.ToLower(dataType) {
	case "int", "integer":
		return "INTEGER"
	case "bigint", "long":
		return "INTEGER"
	case "float", "real":
		return "FLOAT"
	case "double":
		return "FLOAT"
	case "string", "varchar", "text":
		return "STRING"
	case "boolean", "bool":
		return "BOOLEAN"
	case "date":
		return "DATE"
	case "time":
		return "TIME"
	case "datetime", "timestamp":
		return "DATETIME"
	case "duration":
		return "DURATION"
	case "point":
		return "POINT"
	case "list", "array":
		return "LIST"
	case "map", "object":
		return "MAP"
	case "any":
		return "ANY"
	default:
		// Return as-is for Neo4j-specific types
		return strings.ToUpper(dataType)
	}
}

func (g *Neo4jGenerator) getDefaultValueForType(dataType string) interface{} {
	// Return default values for different data types
	switch strings.ToLower(dataType) {
	case "int", "integer", "bigint", "long":
		return 0
	case "float", "real", "double":
		return 0.0
	case "string", "varchar", "text":
		return "example"
	case "boolean", "bool":
		return true
	case "date":
		return "2023-01-01"
	case "time":
		return "12:00:00"
	case "datetime", "timestamp":
		return "2023-01-01T12:00:00"
	case "list", "array":
		return []interface{}{}
	case "map", "object":
		return map[string]interface{}{}
	default:
		return "value"
	}
}
