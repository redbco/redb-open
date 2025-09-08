package generators

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// CassandraGenerator implements CQL generation for Apache Cassandra
type CassandraGenerator struct {
	BaseGenerator
}

// NewCassandraGenerator creates a new Cassandra generator
func NewCassandraGenerator() *CassandraGenerator {
	return &CassandraGenerator{}
}

// Override BaseGenerator methods to provide Cassandra-specific implementations

// Structural organization (Cassandra uses keyspaces)
func (g *CassandraGenerator) GenerateCreateDatabaseSQL(database unifiedmodel.Database) (string, error) {
	if database.Name == "" {
		return "", fmt.Errorf("database name cannot be empty")
	}

	// Cassandra uses keyspaces instead of databases
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE KEYSPACE %s", database.Name))

	// Add replication strategy (default to SimpleStrategy)
	replicationStrategy := "SimpleStrategy"
	replicationFactor := "1"

	if strategy, ok := database.Options["replication_strategy"].(string); ok && strategy != "" {
		replicationStrategy = strategy
	}
	if factor, ok := database.Options["replication_factor"].(string); ok && factor != "" {
		replicationFactor = factor
	}

	sb.WriteString(" WITH REPLICATION = {")
	sb.WriteString(fmt.Sprintf("'class': '%s'", replicationStrategy))

	switch replicationStrategy {
	case "SimpleStrategy":
		sb.WriteString(fmt.Sprintf(", 'replication_factor': %s", replicationFactor))
	case "NetworkTopologyStrategy":
		// Add datacenter replication factors from options
		if dcFactors, ok := database.Options["datacenter_factors"].(map[string]interface{}); ok {
			for dc, factor := range dcFactors {
				sb.WriteString(fmt.Sprintf(", '%s': %v", dc, factor))
			}
		} else {
			sb.WriteString(", 'datacenter1': 1") // Default datacenter
		}
	}

	sb.WriteString("}")

	// Add durable writes option
	if durableWrites, ok := database.Options["durable_writes"].(bool); ok {
		sb.WriteString(fmt.Sprintf(" AND DURABLE_WRITES = %t", durableWrites))
	} else {
		sb.WriteString(" AND DURABLE_WRITES = true") // Default
	}

	sb.WriteString(";")

	return sb.String(), nil
}

// Primary Data Containers (Cassandra tables)
func (g *CassandraGenerator) GenerateCreateTableSQL(table unifiedmodel.Table) (string, error) {
	if table.Name == "" {
		return "", fmt.Errorf("table name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (", table.Name))

	// Add columns
	var columnDefs []string
	for _, col := range table.Columns {
		colDef, err := g.generateColumnDefinition(col)
		if err != nil {
			return "", fmt.Errorf("failed to generate column definition for %s: %w", col.Name, err)
		}
		columnDefs = append(columnDefs, colDef)
	}

	if len(columnDefs) == 0 {
		return "", fmt.Errorf("table must have at least one column")
	}

	sb.WriteString(strings.Join(columnDefs, ", "))

	// Add primary key
	primaryKey, err := g.generatePrimaryKeyDefinition(table)
	if err != nil {
		return "", fmt.Errorf("failed to generate primary key: %w", err)
	}
	if primaryKey != "" {
		sb.WriteString(", ")
		sb.WriteString(primaryKey)
	}

	sb.WriteString(")")

	// Add table options
	tableOptions := g.generateTableOptions(table)
	if tableOptions != "" {
		sb.WriteString(" WITH ")
		sb.WriteString(tableOptions)
	}

	sb.WriteString(";")

	return sb.String(), nil
}

// Integrity, performance and identity objects
func (g *CassandraGenerator) GenerateCreateIndexSQL(index unifiedmodel.Index) (string, error) {
	if index.Name == "" {
		return "", fmt.Errorf("index name cannot be empty")
	}

	var sb strings.Builder

	// Determine index type
	if index.Type == "SASI" {
		sb.WriteString("CREATE CUSTOM INDEX ")
	} else {
		sb.WriteString("CREATE INDEX ")
	}

	sb.WriteString(index.Name)

	// Add table name (should be provided in options)
	tableName := "unknown_table"
	if table, ok := index.Options["table"].(string); ok && table != "" {
		tableName = table
	}

	sb.WriteString(fmt.Sprintf(" ON %s", tableName))

	// Add indexed column or expression
	if index.Expression != "" {
		sb.WriteString(fmt.Sprintf(" (%s)", index.Expression))
	} else if len(index.Columns) > 0 {
		if len(index.Columns) == 1 {
			sb.WriteString(fmt.Sprintf(" (%s)", index.Columns[0]))
		} else {
			// Cassandra doesn't support multi-column indexes directly
			return "", fmt.Errorf("cassandra doesn't support multi-column indexes")
		}
	} else if len(index.Fields) > 0 {
		if len(index.Fields) == 1 {
			sb.WriteString(fmt.Sprintf(" (%s)", index.Fields[0]))
		} else {
			return "", fmt.Errorf("cassandra doesn't support multi-column indexes")
		}
	} else {
		return "", fmt.Errorf("index must have a column, field, or expression")
	}

	// Add SASI options if it's a SASI index
	if index.Type == "SASI" {
		sb.WriteString(" USING 'org.apache.cassandra.index.sasi.SASIIndex'")
		if sasiOptions, ok := index.Options["sasi_options"].(map[string]interface{}); ok {
			sb.WriteString(" WITH OPTIONS = {")
			var opts []string
			for key, value := range sasiOptions {
				opts = append(opts, fmt.Sprintf("'%s': '%v'", key, value))
			}
			sb.WriteString(strings.Join(opts, ", "))
			sb.WriteString("}")
		}
	}

	sb.WriteString(";")

	return sb.String(), nil
}

// Data types and custom objects
func (g *CassandraGenerator) GenerateCreateTypeSQL(dataType unifiedmodel.Type) (string, error) {
	if dataType.Name == "" {
		return "", fmt.Errorf("type name cannot be empty")
	}

	// Cassandra only supports user-defined types (UDTs)
	if dataType.Category != "composite" {
		return "", fmt.Errorf("cassandra only supports composite (UDT) types")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TYPE %s (", dataType.Name))

	// Add fields from definition
	if fields, ok := dataType.Definition["fields"].([]interface{}); ok && len(fields) > 0 {
		var fieldDefs []string
		for _, field := range fields {
			if fieldMap, ok := field.(map[string]interface{}); ok {
				name, _ := fieldMap["name"].(string)
				fieldType, _ := fieldMap["type"].(string)
				if name != "" && fieldType != "" {
					fieldDefs = append(fieldDefs, fmt.Sprintf("%s %s", name, g.mapDataType(fieldType)))
				}
			}
		}
		sb.WriteString(strings.Join(fieldDefs, ", "))
	} else {
		return "", fmt.Errorf("UDT must have fields defined")
	}

	sb.WriteString(");")

	return sb.String(), nil
}

// Executable code objects
func (g *CassandraGenerator) GenerateCreateFunctionSQL(fn unifiedmodel.Function) (string, error) {
	if fn.Name == "" {
		return "", fmt.Errorf("function name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE FUNCTION %s(", fn.Name))

	// Add parameters
	var paramDefs []string
	for _, arg := range fn.Arguments {
		paramDef := fmt.Sprintf("%s %s", arg.Name, g.mapDataType(arg.Type))
		paramDefs = append(paramDefs, paramDef)
	}
	sb.WriteString(strings.Join(paramDefs, ", "))
	sb.WriteString(")")

	// Add return type
	if fn.Returns != "" {
		sb.WriteString(fmt.Sprintf(" RETURNS %s", g.mapDataType(fn.Returns)))
	} else {
		sb.WriteString(" RETURNS text") // Default return type
	}

	// Add language
	language := "java"
	if lang, ok := fn.Options["language"].(string); ok && lang != "" {
		language = lang
	}
	sb.WriteString(fmt.Sprintf(" LANGUAGE %s", language))

	// Add function body
	sb.WriteString(" AS $$")
	if fn.Definition != "" {
		sb.WriteString(fn.Definition)
	} else {
		sb.WriteString("// Function implementation goes here")
	}
	sb.WriteString("$$;")

	return sb.String(), nil
}

func (g *CassandraGenerator) GenerateCreateTriggerSQL(trigger unifiedmodel.Trigger) (string, error) {
	if trigger.Name == "" {
		return "", fmt.Errorf("trigger name cannot be empty")
	}
	if trigger.Table == "" {
		return "", fmt.Errorf("trigger table cannot be empty")
	}

	// Cassandra triggers are different from traditional SQL triggers
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TRIGGER %s ON %s", trigger.Name, trigger.Table))

	// Add trigger class
	triggerClass := "org.apache.cassandra.triggers.AuditTrigger"
	if class, ok := trigger.Options["class"].(string); ok && class != "" {
		triggerClass = class
	}
	sb.WriteString(fmt.Sprintf(" USING '%s'", triggerClass))

	sb.WriteString(";")

	return sb.String(), nil
}

// Security and access control
func (g *CassandraGenerator) GenerateCreateUserSQL(user unifiedmodel.DBUser) (string, error) {
	if user.Name == "" {
		return "", fmt.Errorf("user name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE USER %s", user.Name))

	// Add password if specified in options
	if password, ok := user.Options["password"].(string); ok && password != "" {
		sb.WriteString(fmt.Sprintf(" WITH PASSWORD '%s'", password))
	}

	// Add superuser status
	if superuser, ok := user.Options["superuser"].(bool); ok && superuser {
		sb.WriteString(" SUPERUSER")
	} else {
		sb.WriteString(" NOSUPERUSER")
	}

	sb.WriteString(";")

	return sb.String(), nil
}

func (g *CassandraGenerator) GenerateCreateRoleSQL(role unifiedmodel.DBRole) (string, error) {
	if role.Name == "" {
		return "", fmt.Errorf("role name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE ROLE %s", role.Name))

	// Cassandra roles are simpler - just create the role
	// Additional options would need to be set via separate ALTER ROLE statements

	sb.WriteString(";")

	return sb.String(), nil
}

func (g *CassandraGenerator) GenerateCreateGrantSQL(grant unifiedmodel.Grant) (string, error) {
	if grant.Principal == "" {
		return "", fmt.Errorf("grant principal cannot be empty")
	}
	if grant.Privilege == "" {
		return "", fmt.Errorf("grant privilege cannot be empty")
	}
	if grant.Object == "" {
		return "", fmt.Errorf("grant object cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("GRANT %s", grant.Privilege))

	// Add ON clause
	sb.WriteString(fmt.Sprintf(" ON %s", grant.Object))

	// Add TO clause
	sb.WriteString(fmt.Sprintf(" TO %s", grant.Principal))

	sb.WriteString(";")

	return sb.String(), nil
}

// Cassandra-specific objects
func (g *CassandraGenerator) GenerateCreateMaterializedViewSQL(mv unifiedmodel.MaterializedView) (string, error) {
	if mv.Name == "" {
		return "", fmt.Errorf("materialized view name cannot be empty")
	}
	if mv.Definition == "" {
		return "", fmt.Errorf("materialized view definition cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE MATERIALIZED VIEW %s AS %s", mv.Name, mv.Definition))

	// Cassandra materialized views require a primary key to be specified in the SELECT
	// The primary key is part of the definition, not separate options

	// Add table options from storage if available
	if len(mv.Storage) > 0 {
		sb.WriteString(" WITH ")
		var opts []string
		for key, value := range mv.Storage {
			opts = append(opts, fmt.Sprintf("%s = %v", key, value))
		}
		sb.WriteString(strings.Join(opts, " AND "))
	}

	sb.WriteString(";")

	return sb.String(), nil
}

// High-level generation methods
func (g *CassandraGenerator) GenerateSchema(model *unifiedmodel.UnifiedModel) (string, []string, error) {
	if model == nil {
		return "", nil, fmt.Errorf("unified model cannot be nil")
	}

	var statements []string
	var warnings []string

	// Generate in dependency order for Cassandra

	// 1. Keyspaces (Cassandra's databases)
	for _, database := range model.Databases {
		stmt, err := g.GenerateCreateDatabaseSQL(database)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate keyspace %s: %v", database.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 2. User-defined types (UDTs)
	for _, dataType := range model.Types {
		if dataType.Category == "composite" {
			stmt, err := g.GenerateCreateTypeSQL(dataType)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("Failed to generate UDT %s: %v", dataType.Name, err))
				continue
			}
			statements = append(statements, stmt)
		}
	}

	// 3. Tables (core data structures)
	for _, table := range model.Tables {
		stmt, err := g.GenerateCreateTableSQL(table)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate table %s: %v", table.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 4. Materialized Views (depend on tables)
	for _, mv := range model.MaterializedViews {
		stmt, err := g.GenerateCreateMaterializedViewSQL(mv)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate materialized view %s: %v", mv.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 5. Indexes (depend on tables)
	for _, index := range model.Indexes {
		stmt, err := g.GenerateCreateIndexSQL(index)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate index %s: %v", index.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 6. Functions (UDFs)
	for _, fn := range model.Functions {
		stmt, err := g.GenerateCreateFunctionSQL(fn)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate function %s: %v", fn.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 7. Triggers
	for _, trigger := range model.Triggers {
		stmt, err := g.GenerateCreateTriggerSQL(trigger)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate trigger %s: %v", trigger.Name, err))
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

func (g *CassandraGenerator) GenerateCreateStatements(model *unifiedmodel.UnifiedModel) ([]string, error) {
	if model == nil {
		return nil, fmt.Errorf("unified model cannot be nil")
	}

	var statements []string

	// Generate in dependency order (same as GenerateSchema but with error propagation)

	// 1. Keyspaces
	for _, database := range model.Databases {
		stmt, err := g.GenerateCreateDatabaseSQL(database)
		if err != nil {
			return nil, fmt.Errorf("failed to generate keyspace %s: %w", database.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 2. UDTs
	for _, dataType := range model.Types {
		if dataType.Category == "composite" {
			stmt, err := g.GenerateCreateTypeSQL(dataType)
			if err != nil {
				return nil, fmt.Errorf("failed to generate UDT %s: %w", dataType.Name, err)
			}
			statements = append(statements, stmt)
		}
	}

	// 3. Tables
	for _, table := range model.Tables {
		stmt, err := g.GenerateCreateTableSQL(table)
		if err != nil {
			return nil, fmt.Errorf("failed to generate table %s: %w", table.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 4. Materialized Views
	for _, mv := range model.MaterializedViews {
		stmt, err := g.GenerateCreateMaterializedViewSQL(mv)
		if err != nil {
			return nil, fmt.Errorf("failed to generate materialized view %s: %w", mv.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 5. Indexes
	for _, index := range model.Indexes {
		stmt, err := g.GenerateCreateIndexSQL(index)
		if err != nil {
			return nil, fmt.Errorf("failed to generate index %s: %w", index.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 6. Functions
	for _, fn := range model.Functions {
		stmt, err := g.GenerateCreateFunctionSQL(fn)
		if err != nil {
			return nil, fmt.Errorf("failed to generate function %s: %w", fn.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 7. Triggers
	for _, trigger := range model.Triggers {
		stmt, err := g.GenerateCreateTriggerSQL(trigger)
		if err != nil {
			return nil, fmt.Errorf("failed to generate trigger %s: %w", trigger.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 8. Roles
	for _, role := range model.Roles {
		stmt, err := g.GenerateCreateRoleSQL(role)
		if err != nil {
			return nil, fmt.Errorf("failed to generate role %s: %w", role.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 9. Users
	for _, user := range model.Users {
		stmt, err := g.GenerateCreateUserSQL(user)
		if err != nil {
			return nil, fmt.Errorf("failed to generate user %s: %w", user.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 10. Grants
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

func (g *CassandraGenerator) generateColumnDefinition(col unifiedmodel.Column) (string, error) {
	if col.Name == "" {
		return "", fmt.Errorf("column name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%s %s", col.Name, g.mapDataType(col.DataType)))

	// Cassandra doesn't have NOT NULL constraints like traditional SQL
	// Primary key columns are implicitly NOT NULL

	// Add STATIC modifier if specified
	if static, ok := col.Options["static"].(bool); ok && static {
		sb.WriteString(" STATIC")
	}

	return sb.String(), nil
}

func (g *CassandraGenerator) generatePrimaryKeyDefinition(table unifiedmodel.Table) (string, error) {
	// Find primary key constraint or use primary key columns
	var partitionKeys []string
	var clusteringKeys []string

	// Look for primary key constraint
	for _, constraint := range table.Constraints {
		if constraint.Type == unifiedmodel.ConstraintTypePrimaryKey {
			if len(constraint.Columns) == 0 {
				return "", fmt.Errorf("primary key constraint must have columns")
			}

			// First column is partition key, rest are clustering keys
			partitionKeys = append(partitionKeys, constraint.Columns[0])
			if len(constraint.Columns) > 1 {
				clusteringKeys = constraint.Columns[1:]
			}
			break
		}
	}

	// If no constraint found, look for primary key columns
	if len(partitionKeys) == 0 {
		for _, col := range table.Columns {
			if col.IsPrimaryKey {
				partitionKeys = append(partitionKeys, col.Name)
				break // Only first primary key column becomes partition key
			}
		}
	}

	if len(partitionKeys) == 0 {
		return "", fmt.Errorf("table must have a primary key")
	}

	var sb strings.Builder
	sb.WriteString("PRIMARY KEY (")

	// Add partition key
	if len(partitionKeys) == 1 {
		sb.WriteString(partitionKeys[0])
	} else {
		sb.WriteString(fmt.Sprintf("(%s)", strings.Join(partitionKeys, ", ")))
	}

	// Add clustering keys
	if len(clusteringKeys) > 0 {
		sb.WriteString(fmt.Sprintf(", %s", strings.Join(clusteringKeys, ", ")))
	}

	sb.WriteString(")")

	return sb.String(), nil
}

func (g *CassandraGenerator) generateTableOptions(table unifiedmodel.Table) string {
	var options []string

	// Add clustering order
	if clusteringOrder, ok := table.Options["clustering_order"].(string); ok && clusteringOrder != "" {
		options = append(options, fmt.Sprintf("CLUSTERING ORDER BY (%s)", clusteringOrder))
	}

	// Add compaction strategy
	if compaction, ok := table.Options["compaction"].(map[string]interface{}); ok {
		compactionStr := "compaction = {"
		var compactionOpts []string
		for key, value := range compaction {
			compactionOpts = append(compactionOpts, fmt.Sprintf("'%s': '%v'", key, value))
		}
		compactionStr += strings.Join(compactionOpts, ", ") + "}"
		options = append(options, compactionStr)
	} else {
		// Default compaction strategy
		options = append(options, "compaction = {'class': 'SizeTieredCompactionStrategy'}")
	}

	// Add compression
	if compression, ok := table.Options["compression"].(map[string]interface{}); ok {
		compressionStr := "compression = {"
		var compressionOpts []string
		for key, value := range compression {
			compressionOpts = append(compressionOpts, fmt.Sprintf("'%s': '%v'", key, value))
		}
		compressionStr += strings.Join(compressionOpts, ", ") + "}"
		options = append(options, compressionStr)
	}

	// Add gc_grace_seconds
	if gcGrace, ok := table.Options["gc_grace_seconds"].(int); ok {
		options = append(options, fmt.Sprintf("gc_grace_seconds = %d", gcGrace))
	}

	// Add bloom_filter_fp_chance
	if bloomFilter, ok := table.Options["bloom_filter_fp_chance"].(float64); ok {
		options = append(options, fmt.Sprintf("bloom_filter_fp_chance = %f", bloomFilter))
	}

	// Add comment
	if table.Comment != "" {
		options = append(options, fmt.Sprintf("comment = '%s'", strings.ReplaceAll(table.Comment, "'", "''")))
	}

	return strings.Join(options, " AND ")
}

func (g *CassandraGenerator) mapDataType(dataType string) string {
	// Map common data types to Cassandra equivalents
	switch strings.ToLower(dataType) {
	case "int", "integer":
		return "int"
	case "bigint", "long":
		return "bigint"
	case "smallint", "short":
		return "smallint"
	case "tinyint":
		return "tinyint"
	case "varchar", "string":
		return "text"
	case "text":
		return "text"
	case "char":
		return "ascii"
	case "boolean", "bool":
		return "boolean"
	case "decimal", "numeric":
		return "decimal"
	case "float", "real":
		return "float"
	case "double":
		return "double"
	case "date":
		return "date"
	case "time":
		return "time"
	case "datetime", "timestamp":
		return "timestamp"
	case "uuid":
		return "uuid"
	case "timeuuid":
		return "timeuuid"
	case "inet":
		return "inet"
	case "counter":
		return "counter"
	case "blob", "binary":
		return "blob"
	case "varint":
		return "varint"
	case "duration":
		return "duration"
	case "list":
		return "list<text>" // Default list type
	case "set":
		return "set<text>" // Default set type
	case "map":
		return "map<text, text>" // Default map type
	case "tuple":
		return "tuple<text>" // Default tuple type
	case "frozen":
		return "frozen<text>" // Default frozen type
	default:
		// Return as-is for Cassandra-specific types or custom UDTs
		return dataType
	}
}
