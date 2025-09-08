package generators

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

type PostgresGenerator struct {
	BaseGenerator
}

// Override BaseGenerator methods to provide PostgreSQL-specific implementations

// Structural organization
func (pg *PostgresGenerator) GenerateCreateDatabaseSQL(database unifiedmodel.Database) (string, error) {
	if database.Name == "" {
		return "", fmt.Errorf("database name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE DATABASE %s", database.Name))

	if database.Owner != "" {
		sb.WriteString(fmt.Sprintf(" OWNER %s", database.Owner))
	}

	sb.WriteString(";")

	if database.Comment != "" {
		sb.WriteString(fmt.Sprintf("\nCOMMENT ON DATABASE %s IS '%s';", database.Name, strings.ReplaceAll(database.Comment, "'", "''")))
	}

	return sb.String(), nil
}

func (pg *PostgresGenerator) GenerateCreateSchemaSQL(schema unifiedmodel.Schema) (string, error) {
	if schema.Name == "" {
		return "", fmt.Errorf("schema name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE SCHEMA %s", schema.Name))

	if schema.Owner != "" {
		sb.WriteString(fmt.Sprintf(" AUTHORIZATION %s", schema.Owner))
	}

	sb.WriteString(";")

	if schema.Comment != "" {
		sb.WriteString(fmt.Sprintf("\nCOMMENT ON SCHEMA %s IS '%s';", schema.Name, strings.ReplaceAll(schema.Comment, "'", "''")))
	}

	return sb.String(), nil
}

// Virtual Data Containers
func (pg *PostgresGenerator) GenerateCreateViewSQL(view unifiedmodel.View) (string, error) {
	if view.Name == "" {
		return "", fmt.Errorf("view name cannot be empty")
	}
	if view.Definition == "" {
		return "", fmt.Errorf("view definition cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE VIEW %s AS\n%s;", view.Name, view.Definition))

	if view.Comment != "" {
		sb.WriteString(fmt.Sprintf("\nCOMMENT ON VIEW %s IS '%s';", view.Name, strings.ReplaceAll(view.Comment, "'", "''")))
	}

	return sb.String(), nil
}

func (pg *PostgresGenerator) GenerateCreateMaterializedViewSQL(matView unifiedmodel.MaterializedView) (string, error) {
	if matView.Name == "" {
		return "", fmt.Errorf("materialized view name cannot be empty")
	}
	if matView.Definition == "" {
		return "", fmt.Errorf("materialized view definition cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE MATERIALIZED VIEW %s AS\n%s", matView.Name, matView.Definition))

	// Add refresh options
	if matView.RefreshMode == "immediate" {
		sb.WriteString(" WITH DATA")
	} else {
		sb.WriteString(" WITH NO DATA")
	}

	sb.WriteString(";")

	return sb.String(), nil
}

func (pg *PostgresGenerator) GenerateCreateForeignTableSQL(foreignTable unifiedmodel.ForeignTable) (string, error) {
	if foreignTable.Name == "" {
		return "", fmt.Errorf("foreign table name cannot be empty")
	}
	if foreignTable.Server == "" {
		return "", fmt.Errorf("foreign table server cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE FOREIGN TABLE %s (", foreignTable.Name))

	// Add columns
	var columnDefs []string
	for _, col := range foreignTable.Columns {
		colDef, err := pg.generateColumnDefinition(col)
		if err != nil {
			return "", fmt.Errorf("failed to generate column definition for %s: %w", col.Name, err)
		}
		columnDefs = append(columnDefs, colDef)
	}

	if len(columnDefs) == 0 {
		return "", fmt.Errorf("foreign table must have at least one column")
	}

	sb.WriteString(strings.Join(columnDefs, ", "))
	sb.WriteString(fmt.Sprintf(") SERVER %s", foreignTable.Server))

	// Add options
	if len(foreignTable.Options) > 0 {
		sb.WriteString(" OPTIONS (")
		var opts []string
		for key, value := range foreignTable.Options {
			opts = append(opts, fmt.Sprintf("%s '%v'", key, value))
		}
		sb.WriteString(strings.Join(opts, ", "))
		sb.WriteString(")")
	}

	sb.WriteString(";")

	return sb.String(), nil
}

// Structural definition objects
func (pg *PostgresGenerator) GenerateCreateTypeSQL(dataType unifiedmodel.Type) (string, error) {
	if dataType.Name == "" {
		return "", fmt.Errorf("type name cannot be empty")
	}

	var sb strings.Builder

	switch dataType.Category {
	case "enum":
		// Create ENUM type
		if def, ok := dataType.Definition["values"].([]interface{}); ok {
			sb.WriteString(fmt.Sprintf("CREATE TYPE %s AS ENUM (", dataType.Name))
			var values []string
			for _, v := range def {
				values = append(values, fmt.Sprintf("'%s'", v))
			}
			sb.WriteString(strings.Join(values, ", "))
			sb.WriteString(");")
		} else {
			return "", fmt.Errorf("enum type must have values defined")
		}

	case "composite":
		// Create composite type
		if def, ok := dataType.Definition["fields"].([]interface{}); ok {
			sb.WriteString(fmt.Sprintf("CREATE TYPE %s AS (", dataType.Name))
			var fields []string
			for _, f := range def {
				if field, ok := f.(map[string]interface{}); ok {
					name := field["name"].(string)
					fieldType := field["type"].(string)
					fields = append(fields, fmt.Sprintf("%s %s", name, fieldType))
				}
			}
			sb.WriteString(strings.Join(fields, ", "))
			sb.WriteString(");")
		} else {
			return "", fmt.Errorf("composite type must have fields defined")
		}

	case "domain":
		// Create domain type
		if baseType, ok := dataType.Definition["base_type"].(string); ok {
			sb.WriteString(fmt.Sprintf("CREATE DOMAIN %s AS %s", dataType.Name, baseType))

			if constraint, ok := dataType.Definition["constraint"].(string); ok {
				sb.WriteString(fmt.Sprintf(" CHECK (%s)", constraint))
			}

			sb.WriteString(";")
		} else {
			return "", fmt.Errorf("domain type must have base_type defined")
		}

	default:
		return "", fmt.Errorf("unsupported type category: %s", dataType.Category)
	}

	return sb.String(), nil
}

// Security and access control
func (pg *PostgresGenerator) GenerateCreateUserSQL(user unifiedmodel.DBUser) (string, error) {
	if user.Name == "" {
		return "", fmt.Errorf("user name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE USER %s", user.Name))

	// Add roles
	if len(user.Roles) > 0 {
		sb.WriteString(fmt.Sprintf(" IN ROLE %s", strings.Join(user.Roles, ", ")))
	}

	sb.WriteString(";")

	return sb.String(), nil
}

func (pg *PostgresGenerator) GenerateCreateRoleSQL(role unifiedmodel.DBRole) (string, error) {
	if role.Name == "" {
		return "", fmt.Errorf("role name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE ROLE %s", role.Name))

	// Add parent roles
	if len(role.ParentRoles) > 0 {
		sb.WriteString(fmt.Sprintf(" IN ROLE %s", strings.Join(role.ParentRoles, ", ")))
	}

	sb.WriteString(";")

	return sb.String(), nil
}

func (pg *PostgresGenerator) GenerateCreateGrantSQL(grant unifiedmodel.Grant) (string, error) {
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

	// Add columns if specified
	if len(grant.Columns) > 0 {
		sb.WriteString(fmt.Sprintf(" (%s)", strings.Join(grant.Columns, ", ")))
	}

	sb.WriteString(fmt.Sprintf(" ON %s TO %s", grant.Object, grant.Principal))
	sb.WriteString(";")

	return sb.String(), nil
}

// Physical storage and placement
func (pg *PostgresGenerator) GenerateCreateTablespaceSQL(tablespace unifiedmodel.Tablespace) (string, error) {
	if tablespace.Name == "" {
		return "", fmt.Errorf("tablespace name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLESPACE %s", tablespace.Name))

	// Add location if specified in options
	if location, ok := tablespace.Options["location"].(string); ok {
		sb.WriteString(fmt.Sprintf(" LOCATION '%s'", location))
	}

	sb.WriteString(";")

	return sb.String(), nil
}

// Connectivity and integration
func (pg *PostgresGenerator) GenerateCreateServerSQL(server unifiedmodel.Server) (string, error) {
	if server.Name == "" {
		return "", fmt.Errorf("server name cannot be empty")
	}
	if server.Type == "" {
		return "", fmt.Errorf("server type cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE SERVER %s TYPE '%s' FOREIGN DATA WRAPPER %s", server.Name, server.Type, server.Type))

	// Add options
	if len(server.Options) > 0 {
		sb.WriteString(" OPTIONS (")
		var opts []string
		for key, value := range server.Options {
			opts = append(opts, fmt.Sprintf("%s '%v'", key, value))
		}
		sb.WriteString(strings.Join(opts, ", "))
		sb.WriteString(")")
	}

	sb.WriteString(";")

	return sb.String(), nil
}

func (pg *PostgresGenerator) GenerateCreateForeignDataWrapperSQL(fdw unifiedmodel.ForeignDataWrapper) (string, error) {
	if fdw.Name == "" {
		return "", fmt.Errorf("foreign data wrapper name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE FOREIGN DATA WRAPPER %s", fdw.Name))

	if fdw.Handler != "" {
		sb.WriteString(fmt.Sprintf(" HANDLER %s", fdw.Handler))
	}

	// Add options
	if len(fdw.Options) > 0 {
		sb.WriteString(" OPTIONS (")
		var opts []string
		for key, value := range fdw.Options {
			opts = append(opts, fmt.Sprintf("%s '%v'", key, value))
		}
		sb.WriteString(strings.Join(opts, ", "))
		sb.WriteString(")")
	}

	sb.WriteString(";")

	return sb.String(), nil
}

func (pg *PostgresGenerator) GenerateCreateUserMappingSQL(userMapping unifiedmodel.UserMapping) (string, error) {
	if userMapping.User == "" {
		return "", fmt.Errorf("user mapping user cannot be empty")
	}
	if userMapping.Server == "" {
		return "", fmt.Errorf("user mapping server cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE USER MAPPING FOR %s SERVER %s", userMapping.User, userMapping.Server))

	// Add options
	if len(userMapping.Options) > 0 {
		sb.WriteString(" OPTIONS (")
		var opts []string
		for key, value := range userMapping.Options {
			opts = append(opts, fmt.Sprintf("%s '%v'", key, value))
		}
		sb.WriteString(strings.Join(opts, ", "))
		sb.WriteString(")")
	}

	sb.WriteString(";")

	return sb.String(), nil
}

// Extensions and customization
func (pg *PostgresGenerator) GenerateCreateExtensionSQL(extension unifiedmodel.Extension) (string, error) {
	if extension.Name == "" {
		return "", fmt.Errorf("extension name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s", extension.Name))

	if extension.Version != "" {
		sb.WriteString(fmt.Sprintf(" VERSION '%s'", extension.Version))
	}

	sb.WriteString(";")

	return sb.String(), nil
}

// Replication & distribution
func (pg *PostgresGenerator) GenerateCreatePublicationSQL(publication unifiedmodel.Publication) (string, error) {
	if publication.Name == "" {
		return "", fmt.Errorf("publication name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE PUBLICATION %s", publication.Name))

	if len(publication.Objects) > 0 {
		sb.WriteString(fmt.Sprintf(" FOR TABLE %s", strings.Join(publication.Objects, ", ")))
	} else {
		sb.WriteString(" FOR ALL TABLES")
	}

	sb.WriteString(";")

	return sb.String(), nil
}

func (pg *PostgresGenerator) GenerateCreateSubscriptionSQL(subscription unifiedmodel.Subscription) (string, error) {
	if subscription.Name == "" {
		return "", fmt.Errorf("subscription name cannot be empty")
	}
	if subscription.Source == "" {
		return "", fmt.Errorf("subscription source cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION %s",
		subscription.Name, subscription.Source, subscription.Name))

	sb.WriteString(";")

	return sb.String(), nil
}

func (pg *PostgresGenerator) GenerateCreateReplicationSlotSQL(replicationSlot unifiedmodel.ReplicationSlot) (string, error) {
	if replicationSlot.Name == "" {
		return "", fmt.Errorf("replication slot name cannot be empty")
	}

	var sb strings.Builder
	slotType := "logical"
	if replicationSlot.Type != "" {
		slotType = replicationSlot.Type
	}

	sb.WriteString(fmt.Sprintf("SELECT pg_create_%s_replication_slot('%s', 'pgoutput');", slotType, replicationSlot.Name))

	return sb.String(), nil
}

// GenerateCreateTableSQL implements StatementGenerator interface
func (pg *PostgresGenerator) GenerateCreateTableSQL(table unifiedmodel.Table) (string, error) {
	var sb strings.Builder

	// Start CREATE TABLE statement
	tableName := table.Name
	if tableName == "" {
		return "", fmt.Errorf("table name cannot be empty")
	}

	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (", tableName))

	// Add columns
	var columnDefs []string
	for _, col := range table.Columns {
		colDef, err := pg.generateColumnDefinition(col)
		if err != nil {
			return "", fmt.Errorf("failed to generate column definition for %s: %w", col.Name, err)
		}
		columnDefs = append(columnDefs, colDef)
	}

	if len(columnDefs) == 0 {
		return "", fmt.Errorf("table must have at least one column")
	}

	sb.WriteString(strings.Join(columnDefs, ", "))

	// Add table-level constraints
	for _, constraint := range table.Constraints {
		constraintDef, err := pg.generateConstraintDefinition(constraint)
		if err != nil {
			return "", fmt.Errorf("failed to generate constraint definition for %s: %w", constraint.Name, err)
		}
		if constraintDef != "" {
			sb.WriteString(", ")
			sb.WriteString(constraintDef)
		}
	}

	sb.WriteString(");")

	// Add table comment if present
	if table.Comment != "" {
		sb.WriteString(fmt.Sprintf("\nCOMMENT ON TABLE %s IS '%s';", tableName, strings.ReplaceAll(table.Comment, "'", "''")))
	}

	return sb.String(), nil
}

// GenerateCreateFunctionSQL implements StatementGenerator interface
func (pg *PostgresGenerator) GenerateCreateFunctionSQL(fn unifiedmodel.Function) (string, error) {
	if fn.Name == "" {
		return "", fmt.Errorf("function name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE OR REPLACE FUNCTION %s(", fn.Name))

	// Add arguments
	var argDefs []string
	for _, arg := range fn.Arguments {
		argDef := fmt.Sprintf("%s %s", arg.Name, arg.Type)
		argDefs = append(argDefs, argDef)
	}
	sb.WriteString(strings.Join(argDefs, ", "))
	sb.WriteString(")")

	// Add return type
	if fn.Returns != "" {
		sb.WriteString(fmt.Sprintf(" RETURNS %s", fn.Returns))
	}

	// Add language
	if fn.Language != "" {
		sb.WriteString(fmt.Sprintf(" LANGUAGE %s", fn.Language))
	} else {
		sb.WriteString(" LANGUAGE plpgsql") // Default to plpgsql
	}

	// Add function body
	sb.WriteString(" AS $$\n")
	sb.WriteString(fn.Definition)
	sb.WriteString("\n$$;")

	return sb.String(), nil
}

// GenerateCreateTriggerSQL implements StatementGenerator interface
func (pg *PostgresGenerator) GenerateCreateTriggerSQL(trigger unifiedmodel.Trigger) (string, error) {
	if trigger.Name == "" {
		return "", fmt.Errorf("trigger name cannot be empty")
	}
	if trigger.Table == "" {
		return "", fmt.Errorf("trigger table cannot be empty")
	}
	if trigger.Procedure == "" {
		return "", fmt.Errorf("trigger procedure cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TRIGGER %s", trigger.Name))

	// Add timing
	if trigger.Timing != "" {
		sb.WriteString(fmt.Sprintf(" %s", strings.ToUpper(trigger.Timing)))
	} else {
		sb.WriteString(" BEFORE") // Default timing
	}

	// Add events
	if len(trigger.Events) > 0 {
		events := make([]string, len(trigger.Events))
		for i, event := range trigger.Events {
			events[i] = strings.ToUpper(event)
		}
		sb.WriteString(fmt.Sprintf(" %s", strings.Join(events, " OR ")))
	} else {
		sb.WriteString(" INSERT OR UPDATE OR DELETE") // Default events
	}

	// Add table
	sb.WriteString(fmt.Sprintf(" ON %s", trigger.Table))

	// Add procedure
	sb.WriteString(fmt.Sprintf(" EXECUTE FUNCTION %s();", trigger.Procedure))

	return sb.String(), nil
}

// GenerateCreateSequenceSQL implements StatementGenerator interface
func (pg *PostgresGenerator) GenerateCreateSequenceSQL(seq unifiedmodel.Sequence) (string, error) {
	if seq.Name == "" {
		return "", fmt.Errorf("sequence name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE SEQUENCE %s", seq.Name))

	// Add start value
	if seq.Start != 0 {
		sb.WriteString(fmt.Sprintf(" START WITH %d", seq.Start))
	}

	// Add increment
	if seq.Increment != 0 {
		sb.WriteString(fmt.Sprintf(" INCREMENT BY %d", seq.Increment))
	}

	// Add min value
	if seq.Min != nil {
		sb.WriteString(fmt.Sprintf(" MINVALUE %d", *seq.Min))
	}

	// Add max value
	if seq.Max != nil {
		sb.WriteString(fmt.Sprintf(" MAXVALUE %d", *seq.Max))
	}

	// Add cache
	if seq.Cache != nil {
		sb.WriteString(fmt.Sprintf(" CACHE %d", *seq.Cache))
	}

	// Add cycle
	if seq.Cycle {
		sb.WriteString(" CYCLE")
	} else {
		sb.WriteString(" NO CYCLE")
	}

	sb.WriteString(";")

	return sb.String(), nil
}

// GenerateSchema implements StatementGenerator interface
func (pg *PostgresGenerator) GenerateSchema(model *unifiedmodel.UnifiedModel) (string, []string, error) {
	if model == nil {
		return "", nil, fmt.Errorf("unified model cannot be nil")
	}

	var statements []string
	var warnings []string

	// Generate in dependency order

	// 1. Extensions first (needed for types and other objects)
	for _, extension := range model.Extensions {
		stmt, err := pg.GenerateCreateExtensionSQL(extension)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate extension %s: %v", extension.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 2. Tablespaces (needed for tables)
	for _, tablespace := range model.Tablespaces {
		stmt, err := pg.GenerateCreateTablespaceSQL(tablespace)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate tablespace %s: %v", tablespace.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 3. Schemas
	for _, schema := range model.Schemas {
		stmt, err := pg.GenerateCreateSchemaSQL(schema)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate schema %s: %v", schema.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 4. Types (needed for tables and functions)
	for _, dataType := range model.Types {
		stmt, err := pg.GenerateCreateTypeSQL(dataType)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate type %s: %v", dataType.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 5. Sequences (needed for tables with auto-increment)
	for _, seq := range model.Sequences {
		stmt, err := pg.GenerateCreateSequenceSQL(seq)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate sequence %s: %v", seq.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 6. Foreign Data Wrappers and Servers (needed for foreign tables)
	for _, fdw := range model.ForeignDataWrappers {
		stmt, err := pg.GenerateCreateForeignDataWrapperSQL(fdw)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate foreign data wrapper %s: %v", fdw.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	for _, server := range model.Servers {
		stmt, err := pg.GenerateCreateServerSQL(server)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate server %s: %v", server.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 7. Users and Roles (needed for grants)
	for _, user := range model.Users {
		stmt, err := pg.GenerateCreateUserSQL(user)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate user %s: %v", user.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	for _, role := range model.Roles {
		stmt, err := pg.GenerateCreateRoleSQL(role)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate role %s: %v", role.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 8. Tables (core data structures)
	for _, table := range model.Tables {
		stmt, err := pg.GenerateCreateTableSQL(table)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate table %s: %v", table.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 9. Foreign Tables
	for _, foreignTable := range model.ForeignTables {
		stmt, err := pg.GenerateCreateForeignTableSQL(foreignTable)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate foreign table %s: %v", foreignTable.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 10. Views (depend on tables)
	for _, view := range model.Views {
		stmt, err := pg.GenerateCreateViewSQL(view)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate view %s: %v", view.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	for _, matView := range model.MaterializedViews {
		stmt, err := pg.GenerateCreateMaterializedViewSQL(matView)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate materialized view %s: %v", matView.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 11. Indexes (depend on tables/views)
	for _, index := range model.Indexes {
		stmt, err := pg.generateIndexStatement(index)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate index %s: %v", index.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 12. Functions and Procedures (can reference tables)
	for _, fn := range model.Functions {
		stmt, err := pg.GenerateCreateFunctionSQL(fn)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate function %s: %v", fn.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 13. Triggers (depend on functions and tables)
	for _, trigger := range model.Triggers {
		stmt, err := pg.GenerateCreateTriggerSQL(trigger)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate trigger %s: %v", trigger.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 14. User Mappings (depend on users and servers)
	for _, userMapping := range model.UserMappings {
		stmt, err := pg.GenerateCreateUserMappingSQL(userMapping)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate user mapping for %s: %v", userMapping.User, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 15. Publications and Subscriptions (depend on tables)
	for _, publication := range model.Publications {
		stmt, err := pg.GenerateCreatePublicationSQL(publication)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate publication %s: %v", publication.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	for _, subscription := range model.Subscriptions {
		stmt, err := pg.GenerateCreateSubscriptionSQL(subscription)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate subscription %s: %v", subscription.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 16. Replication Slots
	for _, replicationSlot := range model.ReplicationSlots {
		stmt, err := pg.GenerateCreateReplicationSlotSQL(replicationSlot)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate replication slot %s: %v", replicationSlot.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 17. Grants (should be last)
	for _, grant := range model.Grants {
		stmt, err := pg.GenerateCreateGrantSQL(grant)
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

// GenerateCreateStatements implements StatementGenerator interface
func (pg *PostgresGenerator) GenerateCreateStatements(model *unifiedmodel.UnifiedModel) ([]string, error) {
	if model == nil {
		return nil, fmt.Errorf("unified model cannot be nil")
	}

	var statements []string

	// Generate in dependency order (same as GenerateSchema but with error propagation)

	// 1. Extensions first
	for _, extension := range model.Extensions {
		stmt, err := pg.GenerateCreateExtensionSQL(extension)
		if err != nil {
			return nil, fmt.Errorf("failed to generate extension %s: %w", extension.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 2. Tablespaces
	for _, tablespace := range model.Tablespaces {
		stmt, err := pg.GenerateCreateTablespaceSQL(tablespace)
		if err != nil {
			return nil, fmt.Errorf("failed to generate tablespace %s: %w", tablespace.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 3. Schemas
	for _, schema := range model.Schemas {
		stmt, err := pg.GenerateCreateSchemaSQL(schema)
		if err != nil {
			return nil, fmt.Errorf("failed to generate schema %s: %w", schema.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 4. Types
	for _, dataType := range model.Types {
		stmt, err := pg.GenerateCreateTypeSQL(dataType)
		if err != nil {
			return nil, fmt.Errorf("failed to generate type %s: %w", dataType.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 5. Sequences
	for _, seq := range model.Sequences {
		stmt, err := pg.GenerateCreateSequenceSQL(seq)
		if err != nil {
			return nil, fmt.Errorf("failed to generate sequence %s: %w", seq.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 6. Foreign Data Wrappers and Servers
	for _, fdw := range model.ForeignDataWrappers {
		stmt, err := pg.GenerateCreateForeignDataWrapperSQL(fdw)
		if err != nil {
			return nil, fmt.Errorf("failed to generate foreign data wrapper %s: %w", fdw.Name, err)
		}
		statements = append(statements, stmt)
	}

	for _, server := range model.Servers {
		stmt, err := pg.GenerateCreateServerSQL(server)
		if err != nil {
			return nil, fmt.Errorf("failed to generate server %s: %w", server.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 7. Users and Roles
	for _, user := range model.Users {
		stmt, err := pg.GenerateCreateUserSQL(user)
		if err != nil {
			return nil, fmt.Errorf("failed to generate user %s: %w", user.Name, err)
		}
		statements = append(statements, stmt)
	}

	for _, role := range model.Roles {
		stmt, err := pg.GenerateCreateRoleSQL(role)
		if err != nil {
			return nil, fmt.Errorf("failed to generate role %s: %w", role.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 8. Tables
	for _, table := range model.Tables {
		stmt, err := pg.GenerateCreateTableSQL(table)
		if err != nil {
			return nil, fmt.Errorf("failed to generate table %s: %w", table.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 9. Foreign Tables
	for _, foreignTable := range model.ForeignTables {
		stmt, err := pg.GenerateCreateForeignTableSQL(foreignTable)
		if err != nil {
			return nil, fmt.Errorf("failed to generate foreign table %s: %w", foreignTable.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 10. Views
	for _, view := range model.Views {
		stmt, err := pg.GenerateCreateViewSQL(view)
		if err != nil {
			return nil, fmt.Errorf("failed to generate view %s: %w", view.Name, err)
		}
		statements = append(statements, stmt)
	}

	for _, matView := range model.MaterializedViews {
		stmt, err := pg.GenerateCreateMaterializedViewSQL(matView)
		if err != nil {
			return nil, fmt.Errorf("failed to generate materialized view %s: %w", matView.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 11. Indexes
	for _, index := range model.Indexes {
		stmt, err := pg.generateIndexStatement(index)
		if err != nil {
			return nil, fmt.Errorf("failed to generate index %s: %w", index.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 12. Functions
	for _, fn := range model.Functions {
		stmt, err := pg.GenerateCreateFunctionSQL(fn)
		if err != nil {
			return nil, fmt.Errorf("failed to generate function %s: %w", fn.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 13. Triggers
	for _, trigger := range model.Triggers {
		stmt, err := pg.GenerateCreateTriggerSQL(trigger)
		if err != nil {
			return nil, fmt.Errorf("failed to generate trigger %s: %w", trigger.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 14. User Mappings
	for _, userMapping := range model.UserMappings {
		stmt, err := pg.GenerateCreateUserMappingSQL(userMapping)
		if err != nil {
			return nil, fmt.Errorf("failed to generate user mapping for %s: %w", userMapping.User, err)
		}
		statements = append(statements, stmt)
	}

	// 15. Publications and Subscriptions
	for _, publication := range model.Publications {
		stmt, err := pg.GenerateCreatePublicationSQL(publication)
		if err != nil {
			return nil, fmt.Errorf("failed to generate publication %s: %w", publication.Name, err)
		}
		statements = append(statements, stmt)
	}

	for _, subscription := range model.Subscriptions {
		stmt, err := pg.GenerateCreateSubscriptionSQL(subscription)
		if err != nil {
			return nil, fmt.Errorf("failed to generate subscription %s: %w", subscription.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 16. Replication Slots
	for _, replicationSlot := range model.ReplicationSlots {
		stmt, err := pg.GenerateCreateReplicationSlotSQL(replicationSlot)
		if err != nil {
			return nil, fmt.Errorf("failed to generate replication slot %s: %w", replicationSlot.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 17. Grants (should be last)
	for _, grant := range model.Grants {
		stmt, err := pg.GenerateCreateGrantSQL(grant)
		if err != nil {
			return nil, fmt.Errorf("failed to generate grant for %s: %w", grant.Principal, err)
		}
		statements = append(statements, stmt)
	}

	return statements, nil
}

// Helper methods

func (pg *PostgresGenerator) generateColumnDefinition(col unifiedmodel.Column) (string, error) {
	if col.Name == "" {
		return "", fmt.Errorf("column name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%s %s", col.Name, pg.mapDataType(col.DataType)))

	// Add NOT NULL constraint
	if !col.Nullable {
		sb.WriteString(" NOT NULL")
	}

	// Add DEFAULT value
	if col.Default != "" {
		sb.WriteString(fmt.Sprintf(" DEFAULT %s", col.Default))
	}

	// Add AUTO INCREMENT (SERIAL in PostgreSQL)
	if col.AutoIncrement {
		// For PostgreSQL, we should use SERIAL types instead of adding AUTO_INCREMENT
		// This is handled in mapDataType
	}

	return sb.String(), nil
}

func (pg *PostgresGenerator) generateConstraintDefinition(constraint unifiedmodel.Constraint) (string, error) {
	switch constraint.Type {
	case unifiedmodel.ConstraintTypePrimaryKey:
		if len(constraint.Columns) == 0 {
			return "", fmt.Errorf("primary key constraint must have columns")
		}
		return fmt.Sprintf("CONSTRAINT %s PRIMARY KEY (%s)", constraint.Name, strings.Join(constraint.Columns, ", ")), nil

	case unifiedmodel.ConstraintTypeForeignKey:
		if len(constraint.Columns) == 0 {
			return "", fmt.Errorf("foreign key constraint must have columns")
		}
		if constraint.Reference.Table == "" || len(constraint.Reference.Columns) == 0 {
			return "", fmt.Errorf("foreign key constraint must have reference table and columns")
		}

		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
			constraint.Name,
			strings.Join(constraint.Columns, ", "),
			constraint.Reference.Table,
			strings.Join(constraint.Reference.Columns, ", ")))

		if constraint.Reference.OnUpdate != "" {
			sb.WriteString(fmt.Sprintf(" ON UPDATE %s", constraint.Reference.OnUpdate))
		}
		if constraint.Reference.OnDelete != "" {
			sb.WriteString(fmt.Sprintf(" ON DELETE %s", constraint.Reference.OnDelete))
		}

		return sb.String(), nil

	case unifiedmodel.ConstraintTypeUnique:
		if len(constraint.Columns) == 0 {
			return "", fmt.Errorf("unique constraint must have columns")
		}
		return fmt.Sprintf("CONSTRAINT %s UNIQUE (%s)", constraint.Name, strings.Join(constraint.Columns, ", ")), nil

	case unifiedmodel.ConstraintTypeCheck:
		if constraint.Expression == "" {
			return "", fmt.Errorf("check constraint must have expression")
		}
		return fmt.Sprintf("CONSTRAINT %s CHECK (%s)", constraint.Name, constraint.Expression), nil

	default:
		return "", nil // Skip unsupported constraint types
	}
}

func (pg *PostgresGenerator) generateIndexStatement(index unifiedmodel.Index) (string, error) {
	if index.Name == "" {
		return "", fmt.Errorf("index name cannot be empty")
	}

	var sb strings.Builder

	// Add UNIQUE if specified
	if index.Unique {
		sb.WriteString("CREATE UNIQUE INDEX ")
	} else {
		sb.WriteString("CREATE INDEX ")
	}

	sb.WriteString(fmt.Sprintf("%s ON ", index.Name))

	// Determine table name from columns or fields
	// This is a simplified approach - in practice, you might need more context
	var tableName string
	if len(index.Columns) > 0 {
		// For now, we'll need the table name to be provided somehow
		// This is a limitation of the current design
		tableName = "unknown_table" // This should be resolved by the caller
	}

	sb.WriteString(fmt.Sprintf("%s ", tableName))

	// Add index method if specified
	if index.Type != "" {
		sb.WriteString(fmt.Sprintf("USING %s ", strings.ToUpper(string(index.Type))))
	}

	// Add columns or expression
	if index.Expression != "" {
		sb.WriteString(fmt.Sprintf("(%s)", index.Expression))
	} else if len(index.Columns) > 0 {
		sb.WriteString(fmt.Sprintf("(%s)", strings.Join(index.Columns, ", ")))
	} else if len(index.Fields) > 0 {
		sb.WriteString(fmt.Sprintf("(%s)", strings.Join(index.Fields, ", ")))
	} else {
		return "", fmt.Errorf("index must have columns, fields, or expression")
	}

	// Add partial index predicate
	if index.Predicate != "" {
		sb.WriteString(fmt.Sprintf(" WHERE %s", index.Predicate))
	}

	sb.WriteString(";")

	return sb.String(), nil
}

func (pg *PostgresGenerator) mapDataType(dataType string) string {
	// Map common data types to PostgreSQL equivalents
	switch strings.ToLower(dataType) {
	case "int", "integer":
		return "INTEGER"
	case "bigint", "long":
		return "BIGINT"
	case "smallint", "short":
		return "SMALLINT"
	case "varchar", "string":
		return "VARCHAR(255)" // Default length
	case "text":
		return "TEXT"
	case "char":
		return "CHAR(1)" // Default length
	case "boolean", "bool":
		return "BOOLEAN"
	case "decimal", "numeric":
		return "NUMERIC"
	case "float", "real":
		return "REAL"
	case "double":
		return "DOUBLE PRECISION"
	case "date":
		return "DATE"
	case "time":
		return "TIME"
	case "datetime", "timestamp":
		return "TIMESTAMP"
	case "timestamptz":
		return "TIMESTAMP WITH TIME ZONE"
	case "json":
		return "JSON"
	case "jsonb":
		return "JSONB"
	case "uuid":
		return "UUID"
	case "bytea", "binary":
		return "BYTEA"
	default:
		// Return as-is for PostgreSQL-specific types
		return dataType
	}
}
