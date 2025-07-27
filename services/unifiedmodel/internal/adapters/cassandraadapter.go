package adapters

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type CassandraIngester struct{}

func (c *CassandraIngester) IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error) {
	var cassSchema models.CassandraSchema
	if err := json.Unmarshal(rawSchema, &cassSchema); err != nil {
		return nil, nil, err
	}

	unifiedModel := &models.UnifiedModel{
		SchemaType: "unified",
	}
	warnings := []string{}

	// Convert keyspaces to schemas
	for _, keyspace := range cassSchema.Keyspaces {
		unifiedModel.Schemas = append(unifiedModel.Schemas, models.Schema{
			Name: keyspace.Name,
		})
	}

	// Convert tables
	for _, table := range cassSchema.Tables {
		unifiedTable := models.Table{
			Name:      table.Name,
			Schema:    table.Keyspace,
			TableType: "standard",
		}

		// Convert columns
		for _, col := range table.Columns {
			unifiedCol := models.Column{
				Name:         col.Name,
				IsNullable:   col.IsNullable,
				IsPrimaryKey: col.IsPrimary,
				DataType: models.DataType{
					Name:         col.DataType,
					TypeCategory: getCassandraTypeCategory(col.DataType),
					BaseType:     getCassandraBaseType(col.DataType),
					IsArray:      strings.HasPrefix(col.DataType, "list<") || strings.HasPrefix(col.DataType, "set<"),
					Length:       getCassandraTypeLength(col.DataType),
					Precision:    getCassandraTypePrecision(col.DataType),
					Scale:        getCassandraTypeScale(col.DataType),
				},
			}
			unifiedTable.Columns = append(unifiedTable.Columns, unifiedCol)
		}

		// Add primary key constraint
		if len(table.PrimaryKey) > 0 {
			unifiedTable.Constraints = append(unifiedTable.Constraints, models.Constraint{
				Type:    "PRIMARY KEY",
				Name:    fmt.Sprintf("pk_%s", table.Name),
				Table:   table.Name,
				Columns: table.PrimaryKey,
			})
		}

		unifiedModel.Tables = append(unifiedModel.Tables, unifiedTable)
	}

	// Convert user-defined types
	for _, udt := range cassSchema.Types {
		unifiedType := models.CompositeType{
			Name:   udt.Name,
			Schema: udt.Keyspace,
		}

		for _, field := range udt.Fields {
			unifiedField := models.CompositeField{
				Name: field.Name,
				DataType: models.DataType{
					Name:         field.DataType,
					TypeCategory: getCassandraTypeCategory(field.DataType),
					BaseType:     getCassandraBaseType(field.DataType),
				},
			}
			unifiedType.Fields = append(unifiedType.Fields, unifiedField)
		}

		unifiedModel.CompositeTypes = append(unifiedModel.CompositeTypes, unifiedType)
	}

	// Convert materialized views
	for _, view := range cassSchema.MaterializedViews {
		unifiedView := models.Table{
			Name:      view.Name,
			Schema:    view.Keyspace,
			TableType: "materialized",
			ViewDefinition: fmt.Sprintf("SELECT %s FROM %s.%s%s",
				strings.Join(view.Columns, ", "),
				view.Keyspace,
				view.BaseTable,
				view.WhereClause),
		}
		unifiedModel.Tables = append(unifiedModel.Tables, unifiedView)
	}

	return unifiedModel, warnings, nil
}

type CassandraExporter struct{}

func (c *CassandraExporter) ExportSchema(model *models.UnifiedModel) (interface{}, []string, error) {
	cassSchema := models.CassandraSchema{
		SchemaType: "cassandra",
	}
	warnings := []string{}

	// Convert schemas to keyspaces
	for _, schema := range model.Schemas {
		cassSchema.Keyspaces = append(cassSchema.Keyspaces, models.KeyspaceInfo{
			Name:                schema.Name,
			ReplicationStrategy: "SimpleStrategy",
			ReplicationOptions:  map[string]string{"replication_factor": "1"},
			DurableWrites:       true,
		})
	}

	// Convert tables
	for _, table := range model.Tables {
		if table.TableType == "materialized" {
			// Handle materialized views
			view := models.CassandraView{
				Name:        table.Name,
				Keyspace:    table.Schema,
				BaseTable:   extractBaseTableFromView(table.ViewDefinition),
				Columns:     extractColumnsFromView(table.ViewDefinition),
				WhereClause: extractWhereClauseFromView(table.ViewDefinition),
			}
			cassSchema.MaterializedViews = append(cassSchema.MaterializedViews, view)
			continue
		}

		cassTable := models.CassandraTable{
			Name:     table.Name,
			Keyspace: table.Schema,
		}

		// Convert columns
		for _, col := range table.Columns {
			cassCol := models.CassandraColumn{
				Name:       col.Name,
				IsNullable: col.IsNullable,
				IsPrimary:  col.IsPrimaryKey,
				DataType:   convertToCassandraType(col.DataType),
			}
			cassTable.Columns = append(cassTable.Columns, cassCol)
		}

		// Extract primary key and clustering columns from constraints
		for _, constraint := range table.Constraints {
			if constraint.Type == "PRIMARY KEY" {
				cassTable.PrimaryKey = constraint.Columns
			}
		}

		cassSchema.Tables = append(cassSchema.Tables, cassTable)
	}

	// Convert composite types
	for _, composite := range model.CompositeTypes {
		cassType := models.CassandraType{
			Keyspace: composite.Schema,
			Name:     composite.Name,
		}

		for _, field := range composite.Fields {
			cassField := models.CassandraTypeField{
				Name:     field.Name,
				DataType: convertToCassandraType(field.DataType),
			}
			cassType.Fields = append(cassType.Fields, cassField)
		}

		cassSchema.Types = append(cassSchema.Types, cassType)
	}

	return cassSchema, warnings, nil
}

// Helper functions for type conversion
func getCassandraTypeCategory(dataType string) string {
	if strings.HasPrefix(dataType, "list<") || strings.HasPrefix(dataType, "set<") {
		return "array"
	}
	if strings.HasPrefix(dataType, "frozen<") {
		return "composite"
	}
	return "basic"
}

func getCassandraBaseType(dataType string) string {
	// Remove collection type prefixes
	dataType = strings.TrimPrefix(dataType, "list<")
	dataType = strings.TrimPrefix(dataType, "set<")
	dataType = strings.TrimPrefix(dataType, "frozen<")
	dataType = strings.TrimSuffix(dataType, ">")
	return dataType
}

func getCassandraTypeLength(dataType string) int {
	// Extract length from types like varchar(n)
	if strings.Contains(dataType, "(") {
		parts := strings.Split(dataType, "(")
		if len(parts) > 1 {
			lengthStr := strings.TrimSuffix(parts[1], ")")
			length, _ := strconv.Atoi(lengthStr)
			return length
		}
	}
	return 0
}

func getCassandraTypePrecision(dataType string) int {
	// Extract precision from types like decimal(p,s)
	if strings.Contains(dataType, "(") {
		parts := strings.Split(dataType, "(")
		if len(parts) > 1 {
			precisionStr := strings.Split(parts[1], ",")[0]
			precision, _ := strconv.Atoi(precisionStr)
			return precision
		}
	}
	return 0
}

func getCassandraTypeScale(dataType string) int {
	// Extract scale from types like decimal(p,s)
	if strings.Contains(dataType, "(") {
		parts := strings.Split(dataType, "(")
		if len(parts) > 1 {
			scaleParts := strings.Split(parts[1], ",")
			if len(scaleParts) > 1 {
				scaleStr := strings.TrimSuffix(scaleParts[1], ")")
				scale, _ := strconv.Atoi(scaleStr)
				return scale
			}
		}
	}
	return 0
}

func convertToCassandraType(dt models.DataType) string {
	if dt.IsArray {
		return fmt.Sprintf("list<%s>", dt.BaseType)
	}
	if dt.IsComposite {
		return fmt.Sprintf("frozen<%s>", dt.Name)
	}
	return dt.Name
}

func extractBaseTableFromView(viewDef string) string {
	// Simple extraction - assumes format "SELECT ... FROM keyspace.table WHERE ..."
	parts := strings.Split(viewDef, " FROM ")
	if len(parts) > 1 {
		tablePart := strings.Split(parts[1], " WHERE ")[0]
		return tablePart
	}
	return ""
}

func extractColumnsFromView(viewDef string) []string {
	// Simple extraction - assumes format "SELECT col1, col2, ... FROM ..."
	parts := strings.Split(viewDef, " SELECT ")
	if len(parts) > 1 {
		colPart := strings.Split(parts[1], " FROM ")[0]
		return strings.Split(colPart, ", ")
	}
	return []string{}
}

func extractWhereClauseFromView(viewDef string) string {
	// Simple extraction - assumes format "... WHERE condition"
	parts := strings.Split(viewDef, " WHERE ")
	if len(parts) > 1 {
		return parts[1]
	}
	return ""
}
