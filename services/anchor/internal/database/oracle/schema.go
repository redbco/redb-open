package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// DiscoverSchema fetches the current schema of an Oracle database and returns a UnifiedModel
func DiscoverSchema(db *sql.DB) (*unifiedmodel.UnifiedModel, error) {
	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.Oracle,
		Tables:       make(map[string]unifiedmodel.Table),
		Schemas:      make(map[string]unifiedmodel.Schema),
		Functions:    make(map[string]unifiedmodel.Function),
		Procedures:   make(map[string]unifiedmodel.Procedure),
		Triggers:     make(map[string]unifiedmodel.Trigger),
		Sequences:    make(map[string]unifiedmodel.Sequence),
		Types:        make(map[string]unifiedmodel.Type),
		Packages:     make(map[string]unifiedmodel.Package),
		Indexes:      make(map[string]unifiedmodel.Index),
	}

	// Get tables and their columns
	if err := discoverTablesUnified(db, um); err != nil {
		return nil, fmt.Errorf("error discovering tables: %v", err)
	}

	// Get user-defined types
	if err := discoverTypesUnified(db, um); err != nil {
		return nil, fmt.Errorf("error discovering types: %v", err)
	}

	// Get schemas (users in Oracle)
	if err := discoverSchemasUnified(db, um); err != nil {
		return nil, fmt.Errorf("error getting schemas: %v", err)
	}

	// Get functions
	if err := discoverFunctionsUnified(db, um); err != nil {
		return nil, fmt.Errorf("error getting functions: %v", err)
	}

	// Get procedures
	if err := discoverProceduresUnified(db, um); err != nil {
		return nil, fmt.Errorf("error getting procedures: %v", err)
	}

	// Get triggers
	if err := discoverTriggersUnified(db, um); err != nil {
		return nil, fmt.Errorf("error getting triggers: %v", err)
	}

	// Get sequences
	if err := discoverSequencesUnified(db, um); err != nil {
		return nil, fmt.Errorf("error getting sequences: %v", err)
	}

	// Get packages
	if err := discoverPackagesUnified(db, um); err != nil {
		return nil, fmt.Errorf("error getting packages: %v", err)
	}

	return um, nil
}

// CreateStructure creates database objects from a UnifiedModel
func CreateStructure(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	if um == nil {
		return fmt.Errorf("unified model cannot be nil")
	}

	// Start a transaction
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	// Create user-defined types first
	for _, typeInfo := range um.Types {
		if err := createTypeFromUnified(tx, typeInfo); err != nil {
			return fmt.Errorf("error creating type %s: %v", typeInfo.Name, err)
		}
	}

	// Create sequences
	for _, sequence := range um.Sequences {
		if err := createSequenceFromUnified(tx, sequence); err != nil {
			return fmt.Errorf("error creating sequence %s: %v", sequence.Name, err)
		}
	}

	// Create tables (with topological sorting for dependencies)
	if err := createTablesFromUnified(tx, um.Tables); err != nil {
		return fmt.Errorf("error creating tables: %v", err)
	}

	// Create functions
	for _, function := range um.Functions {
		if err := createFunctionFromUnified(tx, function); err != nil {
			return fmt.Errorf("error creating function %s: %v", function.Name, err)
		}
	}

	// Create procedures
	for _, procedure := range um.Procedures {
		if err := createProcedureFromUnified(tx, procedure); err != nil {
			return fmt.Errorf("error creating procedure %s: %v", procedure.Name, err)
		}
	}

	// Create packages
	for _, pkg := range um.Packages {
		if err := createPackageFromUnified(tx, pkg); err != nil {
			return fmt.Errorf("error creating package %s: %v", pkg.Name, err)
		}
	}

	// Create triggers
	for _, trigger := range um.Triggers {
		if err := createTriggerFromUnified(tx, trigger); err != nil {
			return fmt.Errorf("error creating trigger %s: %v", trigger.Name, err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	return nil
}

// discoverTablesUnified discovers Oracle tables directly into UnifiedModel
func discoverTablesUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			t.OWNER,
			t.TABLE_NAME,
			c.COLUMN_NAME,
			c.DATA_TYPE,
			c.DATA_LENGTH,
			c.DATA_PRECISION,
			c.DATA_SCALE,
			CASE WHEN c.NULLABLE = 'Y' THEN 1 ELSE 0 END AS IS_NULLABLE,
			c.DATA_DEFAULT,
			CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY,
			c.COLUMN_ID
		FROM ALL_TABLES t
		INNER JOIN ALL_TAB_COLUMNS c ON t.OWNER = c.OWNER AND t.TABLE_NAME = c.TABLE_NAME
		LEFT JOIN (
			SELECT DISTINCT cc.OWNER, cc.TABLE_NAME, cc.COLUMN_NAME
			FROM ALL_CONS_COLUMNS cc
			INNER JOIN ALL_CONSTRAINTS ac ON cc.OWNER = ac.OWNER 
				AND cc.TABLE_NAME = ac.TABLE_NAME 
				AND cc.CONSTRAINT_NAME = ac.CONSTRAINT_NAME
			WHERE ac.CONSTRAINT_TYPE = 'P'
		) pk ON t.OWNER = pk.OWNER AND t.TABLE_NAME = pk.TABLE_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME
		WHERE t.OWNER NOT IN ('SYS', 'SYSTEM', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'OUTLN', 'WMSYS')
		ORDER BY t.OWNER, t.TABLE_NAME, c.COLUMN_ID
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying tables: %v", err)
	}
	defer rows.Close()

	tables := make(map[string]*unifiedmodel.Table)

	for rows.Next() {
		var owner, tableName, columnName, dataType, dataDefault string
		var dataLength, dataPrecision, dataScale, columnID int
		var isNullable, isPrimaryKey bool

		err := rows.Scan(&owner, &tableName, &columnName, &dataType,
			&dataLength, &dataPrecision, &dataScale, &isNullable, &dataDefault, &isPrimaryKey, &columnID)
		if err != nil {
			return fmt.Errorf("error scanning table row: %v", err)
		}

		fullTableName := owner + "." + tableName

		// Create table if it doesn't exist
		if tables[fullTableName] == nil {
			tables[fullTableName] = &unifiedmodel.Table{
				Name:        tableName,
				Columns:     make(map[string]unifiedmodel.Column),
				Indexes:     make(map[string]unifiedmodel.Index),
				Constraints: make(map[string]unifiedmodel.Constraint),
				Options: map[string]interface{}{
					"owner": owner,
				},
			}
		}

		// Add column to table
		column := unifiedmodel.Column{
			Name:         columnName,
			DataType:     dataType,
			Nullable:     isNullable,
			Default:      dataDefault,
			IsPrimaryKey: isPrimaryKey,
			Options: map[string]interface{}{
				"length":    dataLength,
				"precision": dataPrecision,
				"scale":     dataScale,
				"column_id": columnID,
			},
		}

		tables[fullTableName].Columns[columnName] = column
	}

	// Discover indexes and constraints for each table
	for _, table := range tables {
		owner := table.Options["owner"].(string)
		if err := discoverTableIndexesUnified(db, owner, table.Name, table); err != nil {
			return fmt.Errorf("error discovering indexes for table %s.%s: %v", owner, table.Name, err)
		}
		if err := discoverTableConstraintsUnified(db, owner, table.Name, table); err != nil {
			return fmt.Errorf("error discovering constraints for table %s.%s: %v", owner, table.Name, err)
		}
	}

	// Add tables to unified model
	for _, table := range tables {
		um.Tables[table.Name] = *table
	}

	return nil
}

// discoverTableIndexesUnified discovers Oracle indexes for a specific table
func discoverTableIndexesUnified(db *sql.DB, owner, tableName string, table *unifiedmodel.Table) error {
	query := `
		SELECT 
			i.INDEX_NAME,
			i.UNIQUENESS,
			ic.COLUMN_NAME,
			ic.COLUMN_POSITION
		FROM ALL_INDEXES i
		INNER JOIN ALL_IND_COLUMNS ic ON i.OWNER = ic.INDEX_OWNER AND i.INDEX_NAME = ic.INDEX_NAME
		WHERE i.TABLE_OWNER = ? AND i.TABLE_NAME = ?
		ORDER BY i.INDEX_NAME, ic.COLUMN_POSITION
	`

	rows, err := db.Query(query, owner, tableName)
	if err != nil {
		return fmt.Errorf("error querying indexes: %v", err)
	}
	defer rows.Close()

	indexes := make(map[string]*unifiedmodel.Index)

	for rows.Next() {
		var indexName, uniqueness, columnName string
		var columnPosition int

		if err := rows.Scan(&indexName, &uniqueness, &columnName, &columnPosition); err != nil {
			return fmt.Errorf("error scanning index row: %v", err)
		}

		if indexes[indexName] == nil {
			indexes[indexName] = &unifiedmodel.Index{
				Name:    indexName,
				Columns: []string{},
				Unique:  uniqueness == "UNIQUE",
			}
		}

		indexes[indexName].Columns = append(indexes[indexName].Columns, columnName)
	}

	// Add indexes to table
	for _, index := range indexes {
		table.Indexes[index.Name] = *index
	}

	return nil
}

// discoverTableConstraintsUnified discovers Oracle constraints for a specific table
func discoverTableConstraintsUnified(db *sql.DB, owner, tableName string, table *unifiedmodel.Table) error {
	query := `
		SELECT 
			ac.CONSTRAINT_NAME,
			ac.CONSTRAINT_TYPE,
			cc.COLUMN_NAME,
			ac.R_OWNER,
			ac.R_CONSTRAINT_NAME,
			ac.SEARCH_CONDITION
		FROM ALL_CONSTRAINTS ac
		LEFT JOIN ALL_CONS_COLUMNS cc ON ac.OWNER = cc.OWNER 
			AND ac.TABLE_NAME = cc.TABLE_NAME 
			AND ac.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
		WHERE ac.OWNER = ? AND ac.TABLE_NAME = ?
		ORDER BY ac.CONSTRAINT_NAME, cc.POSITION
	`

	rows, err := db.Query(query, owner, tableName)
	if err != nil {
		return fmt.Errorf("error querying constraints: %v", err)
	}
	defer rows.Close()

	constraints := make(map[string]*unifiedmodel.Constraint)

	for rows.Next() {
		var constraintName, constraintType, columnName, rOwner, rConstraintName, searchCondition string

		if err := rows.Scan(&constraintName, &constraintType, &columnName, &rOwner, &rConstraintName, &searchCondition); err != nil {
			return fmt.Errorf("error scanning constraint row: %v", err)
		}

		if constraints[constraintName] == nil {
			var cType unifiedmodel.ConstraintType
			switch constraintType {
			case "P":
				cType = unifiedmodel.ConstraintTypePrimaryKey
			case "R":
				cType = unifiedmodel.ConstraintTypeForeignKey
			case "U":
				cType = unifiedmodel.ConstraintTypeUnique
			case "C":
				cType = unifiedmodel.ConstraintTypeCheck
			default:
				cType = unifiedmodel.ConstraintTypeCheck
			}

			constraints[constraintName] = &unifiedmodel.Constraint{
				Name:    constraintName,
				Type:    cType,
				Columns: []string{},
				Options: map[string]interface{}{
					"search_condition": searchCondition,
				},
			}

			// Set foreign key reference if applicable
			if constraintType == "R" && rConstraintName != "" {
				// Get referenced table and column
				refQuery := `
					SELECT ac.TABLE_NAME, cc.COLUMN_NAME
					FROM ALL_CONSTRAINTS ac
					INNER JOIN ALL_CONS_COLUMNS cc ON ac.OWNER = cc.OWNER 
						AND ac.TABLE_NAME = cc.TABLE_NAME 
						AND ac.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
					WHERE ac.OWNER = ? AND ac.CONSTRAINT_NAME = ?
				`
				var refTable, refColumn string
				if err := db.QueryRow(refQuery, rOwner, rConstraintName).Scan(&refTable, &refColumn); err == nil {
					constraints[constraintName].Reference = unifiedmodel.Reference{
						Table:   refTable,
						Columns: []string{refColumn},
					}
				}
			}
		}

		if columnName != "" {
			constraints[constraintName].Columns = append(constraints[constraintName].Columns, columnName)
		}
	}

	// Add constraints to table
	for _, constraint := range constraints {
		table.Constraints[constraint.Name] = *constraint
	}

	return nil
}

// discoverTypesUnified discovers Oracle user-defined types directly into UnifiedModel
func discoverTypesUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			t.OWNER,
			t.TYPE_NAME,
			t.TYPECODE,
			COALESCE(tc.COMMENTS, '') AS COMMENTS
		FROM ALL_TYPES t
		LEFT JOIN ALL_TYPE_COMMENTS tc ON t.OWNER = tc.OWNER AND t.TYPE_NAME = tc.TYPE_NAME
		WHERE t.OWNER NOT IN ('SYS', 'SYSTEM', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'OUTLN', 'WMSYS')
		ORDER BY t.OWNER, t.TYPE_NAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying types: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var owner, typeName, typeCode, comments string
		if err := rows.Scan(&owner, &typeName, &typeCode, &comments); err != nil {
			return fmt.Errorf("error scanning type row: %v", err)
		}

		um.Types[typeName] = unifiedmodel.Type{
			Name:     typeName,
			Category: "user-defined",
			Definition: map[string]any{
				"owner":     owner,
				"type_code": typeCode,
				"comments":  comments,
			},
		}
	}

	return nil
}

// discoverSchemasUnified discovers Oracle schemas (users) directly into UnifiedModel
func discoverSchemasUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			USERNAME,
			CREATED,
			ACCOUNT_STATUS
		FROM ALL_USERS
		WHERE USERNAME NOT IN ('SYS', 'SYSTEM', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'OUTLN', 'WMSYS')
		ORDER BY USERNAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying schemas: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var username, created, accountStatus string
		if err := rows.Scan(&username, &created, &accountStatus); err != nil {
			return fmt.Errorf("error scanning schema row: %v", err)
		}

		um.Schemas[username] = unifiedmodel.Schema{
			Name: username,
			Options: map[string]interface{}{
				"created":        created,
				"account_status": accountStatus,
			},
		}
	}

	return nil
}

// discoverFunctionsUnified discovers Oracle functions directly into UnifiedModel
func discoverFunctionsUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			p.OWNER,
			p.OBJECT_NAME,
			s.TEXT
		FROM ALL_PROCEDURES p
		LEFT JOIN ALL_SOURCE s ON p.OWNER = s.OWNER 
			AND p.OBJECT_NAME = s.NAME 
			AND s.TYPE = 'FUNCTION'
		WHERE p.PROCEDURE_NAME IS NULL 
			AND p.OBJECT_TYPE = 'FUNCTION'
			AND p.OWNER NOT IN ('SYS', 'SYSTEM', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'OUTLN', 'WMSYS')
		ORDER BY p.OWNER, p.OBJECT_NAME, s.LINE
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying functions: %v", err)
	}
	defer rows.Close()

	functions := make(map[string]*unifiedmodel.Function)

	for rows.Next() {
		var owner, objectName, text string
		if err := rows.Scan(&owner, &objectName, &text); err != nil {
			return fmt.Errorf("error scanning function row: %v", err)
		}

		if functions[objectName] == nil {
			functions[objectName] = &unifiedmodel.Function{
				Name:       objectName,
				Language:   "plsql",
				Definition: "",
				Options: map[string]interface{}{
					"owner": owner,
				},
			}
		}

		functions[objectName].Definition += text
	}

	// Add functions to unified model
	for _, function := range functions {
		um.Functions[function.Name] = *function
	}

	return nil
}

// discoverProceduresUnified discovers Oracle procedures directly into UnifiedModel
func discoverProceduresUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			p.OWNER,
			p.OBJECT_NAME,
			s.TEXT
		FROM ALL_PROCEDURES p
		LEFT JOIN ALL_SOURCE s ON p.OWNER = s.OWNER 
			AND p.OBJECT_NAME = s.NAME 
			AND s.TYPE = 'PROCEDURE'
		WHERE p.PROCEDURE_NAME IS NULL 
			AND p.OBJECT_TYPE = 'PROCEDURE'
			AND p.OWNER NOT IN ('SYS', 'SYSTEM', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'OUTLN', 'WMSYS')
		ORDER BY p.OWNER, p.OBJECT_NAME, s.LINE
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying procedures: %v", err)
	}
	defer rows.Close()

	procedures := make(map[string]*unifiedmodel.Procedure)

	for rows.Next() {
		var owner, objectName, text string
		if err := rows.Scan(&owner, &objectName, &text); err != nil {
			return fmt.Errorf("error scanning procedure row: %v", err)
		}

		if procedures[objectName] == nil {
			procedures[objectName] = &unifiedmodel.Procedure{
				Name:       objectName,
				Language:   "plsql",
				Definition: "",
				Options: map[string]interface{}{
					"owner": owner,
				},
			}
		}

		procedures[objectName].Definition += text
	}

	// Add procedures to unified model
	for _, procedure := range procedures {
		um.Procedures[procedure.Name] = *procedure
	}

	return nil
}

// discoverTriggersUnified discovers Oracle triggers directly into UnifiedModel
func discoverTriggersUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			t.OWNER,
			t.TRIGGER_NAME,
			t.TABLE_OWNER,
			t.TABLE_NAME,
			t.TRIGGERING_EVENT,
			t.TRIGGER_TYPE,
			t.TRIGGER_BODY
		FROM ALL_TRIGGERS t
		WHERE t.OWNER NOT IN ('SYS', 'SYSTEM', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'OUTLN', 'WMSYS')
		ORDER BY t.OWNER, t.TRIGGER_NAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying triggers: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var owner, triggerName, tableOwner, tableName, triggeringEvent, triggerType, triggerBody string
		if err := rows.Scan(&owner, &triggerName, &tableOwner, &tableName, &triggeringEvent, &triggerType, &triggerBody); err != nil {
			return fmt.Errorf("error scanning trigger row: %v", err)
		}

		um.Triggers[triggerName] = unifiedmodel.Trigger{
			Name:  triggerName,
			Table: tableName,
			Options: map[string]interface{}{
				"owner":            owner,
				"table_owner":      tableOwner,
				"triggering_event": triggeringEvent,
				"trigger_type":     triggerType,
				"definition":       triggerBody,
			},
		}
	}

	return nil
}

// discoverSequencesUnified discovers Oracle sequences directly into UnifiedModel
func discoverSequencesUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			s.SEQUENCE_OWNER,
			s.SEQUENCE_NAME,
			s.MIN_VALUE,
			s.MAX_VALUE,
			s.INCREMENT_BY,
			s.LAST_NUMBER
		FROM ALL_SEQUENCES s
		WHERE s.SEQUENCE_OWNER NOT IN ('SYS', 'SYSTEM', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'OUTLN', 'WMSYS')
		ORDER BY s.SEQUENCE_OWNER, s.SEQUENCE_NAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying sequences: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var owner, sequenceName string
		var minValue, maxValue, incrementBy, lastNumber int64
		if err := rows.Scan(&owner, &sequenceName, &minValue, &maxValue, &incrementBy, &lastNumber); err != nil {
			return fmt.Errorf("error scanning sequence row: %v", err)
		}

		um.Sequences[sequenceName] = unifiedmodel.Sequence{
			Name:      sequenceName,
			Start:     lastNumber,
			Increment: incrementBy,
			Options: map[string]interface{}{
				"owner":     owner,
				"min_value": minValue,
				"max_value": maxValue,
			},
		}
	}

	return nil
}

// discoverPackagesUnified discovers Oracle packages directly into UnifiedModel
func discoverPackagesUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			p.OWNER,
			p.OBJECT_NAME,
			p.STATUS,
			p.CREATED,
			p.LAST_DDL_TIME
		FROM ALL_OBJECTS p
		WHERE p.OBJECT_TYPE = 'PACKAGE'
			AND p.OWNER NOT IN ('SYS', 'SYSTEM', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'OUTLN', 'WMSYS')
		ORDER BY p.OWNER, p.OBJECT_NAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying packages: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var owner, objectName, status, created, lastDDLTime string
		if err := rows.Scan(&owner, &objectName, &status, &created, &lastDDLTime); err != nil {
			return fmt.Errorf("error scanning package row: %v", err)
		}

		um.Packages[objectName] = unifiedmodel.Package{
			Name: objectName,
			Options: map[string]interface{}{
				"owner":         owner,
				"status":        status,
				"created":       created,
				"last_ddl_time": lastDDLTime,
			},
		}
	}

	return nil
}

// Helper function to quote Oracle identifiers
func QuoteIdentifier(name string) string {
	return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
}

// createTypeFromUnified creates an Oracle user-defined type from UnifiedModel Type
func createTypeFromUnified(tx *sql.Tx, typeInfo unifiedmodel.Type) error {
	if typeInfo.Name == "" {
		return fmt.Errorf("type name cannot be empty")
	}

	// Oracle type creation requires specific DDL based on type category
	// This is a simplified implementation - real Oracle types are complex
	query := fmt.Sprintf("CREATE OR REPLACE TYPE %s AS OBJECT (id NUMBER)", QuoteIdentifier(typeInfo.Name))

	_, err := tx.Exec(query)
	if err != nil {
		return fmt.Errorf("error creating type: %v", err)
	}

	return nil
}

// createSequenceFromUnified creates an Oracle sequence from UnifiedModel Sequence
func createSequenceFromUnified(tx *sql.Tx, sequence unifiedmodel.Sequence) error {
	if sequence.Name == "" {
		return fmt.Errorf("sequence name cannot be empty")
	}

	query := fmt.Sprintf("CREATE SEQUENCE %s START WITH %d INCREMENT BY %d",
		QuoteIdentifier(sequence.Name), sequence.Start, sequence.Increment)

	// Add min/max values if specified in options
	if sequence.Options != nil {
		if minVal, ok := sequence.Options["min_value"].(int64); ok {
			query += fmt.Sprintf(" MINVALUE %d", minVal)
		}
		if maxVal, ok := sequence.Options["max_value"].(int64); ok {
			query += fmt.Sprintf(" MAXVALUE %d", maxVal)
		}
	}

	_, err := tx.Exec(query)
	if err != nil {
		return fmt.Errorf("error creating sequence: %v", err)
	}

	return nil
}

// createTablesFromUnified creates Oracle tables from UnifiedModel Tables with dependency sorting
func createTablesFromUnified(tx *sql.Tx, tables map[string]unifiedmodel.Table) error {
	// Simple table creation without dependency sorting for now
	// In a full implementation, you would implement topological sorting
	for _, table := range tables {
		if err := createTableFromUnified(tx, table); err != nil {
			return fmt.Errorf("error creating table %s: %v", table.Name, err)
		}
	}

	// Add constraints after all tables are created
	for _, table := range tables {
		if err := addTableConstraintsFromUnified(tx, table); err != nil {
			return fmt.Errorf("error adding constraints to table %s: %v", table.Name, err)
		}
	}

	return nil
}

// createTableFromUnified creates an Oracle table from UnifiedModel Table
func createTableFromUnified(tx *sql.Tx, table unifiedmodel.Table) error {
	if table.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	var columns []string
	for _, column := range table.Columns {
		columnDef := QuoteIdentifier(column.Name) + " " + column.DataType

		if !column.Nullable {
			columnDef += " NOT NULL"
		}

		if column.Default != "" {
			columnDef += " DEFAULT " + column.Default
		}

		columns = append(columns, columnDef)
	}

	query := fmt.Sprintf("CREATE TABLE %s (%s)",
		QuoteIdentifier(table.Name), strings.Join(columns, ", "))

	_, err := tx.Exec(query)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	return nil
}

// addTableConstraintsFromUnified adds constraints to an Oracle table from UnifiedModel Table
func addTableConstraintsFromUnified(tx *sql.Tx, table unifiedmodel.Table) error {
	// Add primary key constraint
	var pkColumns []string
	for _, column := range table.Columns {
		if column.IsPrimaryKey {
			pkColumns = append(pkColumns, QuoteIdentifier(column.Name))
		}
	}

	if len(pkColumns) > 0 {
		pkName := fmt.Sprintf("PK_%s", table.Name)
		query := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)",
			QuoteIdentifier(table.Name), QuoteIdentifier(pkName), strings.Join(pkColumns, ", "))

		_, err := tx.Exec(query)
		if err != nil {
			return fmt.Errorf("error adding primary key: %v", err)
		}
	}

	// Add other constraints
	for _, constraint := range table.Constraints {
		if err := createConstraintFromUnified(tx, table.Name, constraint); err != nil {
			return fmt.Errorf("error creating constraint %s: %v", constraint.Name, err)
		}
	}

	return nil
}

// createConstraintFromUnified creates an Oracle constraint from UnifiedModel Constraint
func createConstraintFromUnified(tx *sql.Tx, tableName string, constraint unifiedmodel.Constraint) error {
	var query string

	switch constraint.Type {
	case unifiedmodel.ConstraintTypeForeignKey:
		if len(constraint.Columns) > 0 && constraint.Reference.Table != "" && len(constraint.Reference.Columns) > 0 {
			query = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
				QuoteIdentifier(tableName), QuoteIdentifier(constraint.Name),
				QuoteIdentifier(constraint.Columns[0]),
				QuoteIdentifier(constraint.Reference.Table),
				QuoteIdentifier(constraint.Reference.Columns[0]))
		}
	case unifiedmodel.ConstraintTypeUnique:
		query = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)",
			QuoteIdentifier(tableName), QuoteIdentifier(constraint.Name),
			strings.Join(QuoteStringSlice(constraint.Columns), ", "))
	case unifiedmodel.ConstraintTypeCheck:
		if constraint.Options != nil {
			if searchCondition, ok := constraint.Options["search_condition"].(string); ok && searchCondition != "" {
				query = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)",
					QuoteIdentifier(tableName), QuoteIdentifier(constraint.Name), searchCondition)
			}
		}
	}

	if query != "" {
		_, err := tx.Exec(query)
		if err != nil {
			return fmt.Errorf("error creating constraint: %v", err)
		}
	}

	return nil
}

// createFunctionFromUnified creates an Oracle function from UnifiedModel Function
func createFunctionFromUnified(tx *sql.Tx, function unifiedmodel.Function) error {
	if function.Name == "" || function.Definition == "" {
		return fmt.Errorf("function name and definition cannot be empty")
	}

	// Oracle functions require the full CREATE OR REPLACE FUNCTION statement in the definition
	_, err := tx.Exec(function.Definition)
	if err != nil {
		return fmt.Errorf("error creating function: %v", err)
	}

	return nil
}

// createProcedureFromUnified creates an Oracle procedure from UnifiedModel Procedure
func createProcedureFromUnified(tx *sql.Tx, procedure unifiedmodel.Procedure) error {
	if procedure.Name == "" || procedure.Definition == "" {
		return fmt.Errorf("procedure name and definition cannot be empty")
	}

	// Oracle procedures require the full CREATE OR REPLACE PROCEDURE statement in the definition
	_, err := tx.Exec(procedure.Definition)
	if err != nil {
		return fmt.Errorf("error creating procedure: %v", err)
	}

	return nil
}

// createPackageFromUnified creates an Oracle package from UnifiedModel Package
func createPackageFromUnified(tx *sql.Tx, pkg unifiedmodel.Package) error {
	if pkg.Name == "" {
		return fmt.Errorf("package name cannot be empty")
	}

	// Oracle packages require both specification and body
	// This is a simplified implementation
	specQuery := fmt.Sprintf("CREATE OR REPLACE PACKAGE %s AS END %s;",
		QuoteIdentifier(pkg.Name), QuoteIdentifier(pkg.Name))

	_, err := tx.Exec(specQuery)
	if err != nil {
		return fmt.Errorf("error creating package: %v", err)
	}

	return nil
}

// createTriggerFromUnified creates an Oracle trigger from UnifiedModel Trigger
func createTriggerFromUnified(tx *sql.Tx, trigger unifiedmodel.Trigger) error {
	if trigger.Name == "" || trigger.Table == "" {
		return fmt.Errorf("trigger name and table cannot be empty")
	}

	// Get trigger definition from options
	var definition string
	if trigger.Options != nil {
		if def, ok := trigger.Options["definition"].(string); ok {
			definition = def
		}
	}

	if definition == "" {
		return fmt.Errorf("trigger definition cannot be empty")
	}

	// Oracle triggers require the full CREATE OR REPLACE TRIGGER statement in the definition
	_, err := tx.Exec(definition)
	if err != nil {
		return fmt.Errorf("error creating trigger: %v", err)
	}

	return nil
}

// QuoteStringSlice quotes each string in a slice
func QuoteStringSlice(slice []string) []string {
	quoted := make([]string, len(slice))
	for i, s := range slice {
		quoted[i] = QuoteIdentifier(s)
	}
	return quoted
}
