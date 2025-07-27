package adapters

import (
	"encoding/json"
	"fmt"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type ClickhouseIngester struct{}

func (c *ClickhouseIngester) IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error) {
	var chSchema models.ClickhouseSchema
	if err := json.Unmarshal(rawSchema, &chSchema); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal Clickhouse schema: %w", err)
	}

	unifiedModel := &models.UnifiedModel{
		SchemaType: "unified",
	}
	warnings := []string{}

	// Convert databases to schemas
	for _, db := range chSchema.Databases {
		unifiedModel.Schemas = append(unifiedModel.Schemas, models.Schema{
			Name:    db.Name,
			Comment: db.Comment,
		})
	}

	// Convert tables
	for _, table := range chSchema.Tables {
		unifiedTable := models.Table{
			Name:      table.Name,
			Schema:    table.Database,
			TableType: "standard",
			Comment:   table.Comment,
		}

		// Convert columns
		for _, col := range table.Columns {
			unifiedCol := models.Column{
				Name:       col.Name,
				DataType:   models.DataType{Name: col.DataType},
				IsNullable: col.IsNullable,
				Comment:    col.Comment,
			}

			if col.DefaultValue != nil {
				unifiedCol.DefaultValue = col.DefaultValue
			}

			unifiedTable.Columns = append(unifiedTable.Columns, unifiedCol)
		}

		// Add primary key constraint if any column is marked as primary key
		pkColumns := make([]string, 0)
		for _, col := range table.Columns {
			if col.IsPrimaryKey {
				pkColumns = append(pkColumns, col.Name)
			}
		}
		if len(pkColumns) > 0 {
			unifiedTable.Constraints = append(unifiedTable.Constraints, models.Constraint{
				Type:    "PRIMARY KEY",
				Columns: pkColumns,
			})
		}

		unifiedModel.Tables = append(unifiedModel.Tables, unifiedTable)
	}

	// Convert views
	for _, view := range chSchema.Views {
		unifiedTable := models.Table{
			Name:      view.Name,
			Schema:    view.Database,
			TableType: "view",
			Comment:   view.Comment,
		}
		unifiedModel.Tables = append(unifiedModel.Tables, unifiedTable)
	}

	// Convert functions
	for _, function := range chSchema.Functions {
		unifiedModel.Functions = append(unifiedModel.Functions, models.Function{
			Name:       function.Name,
			Schema:     "default",
			Arguments:  function.Arguments,
			ReturnType: function.ReturnType,
			Definition: function.Definition,
		})
	}

	return unifiedModel, warnings, nil
}

type ClickhouseExporter struct{}

func (c *ClickhouseExporter) ExportSchema(model *models.UnifiedModel) (interface{}, []string, error) {
	chSchema := models.ClickhouseSchema{
		SchemaType: "clickhouse",
	}
	warnings := []string{}

	// Convert schemas to databases
	for _, schema := range model.Schemas {
		chSchema.Databases = append(chSchema.Databases, models.Database{
			Name:    schema.Name,
			Comment: schema.Comment,
		})
	}

	// Convert tables
	for _, table := range model.Tables {
		if table.TableType == "view" {
			// Handle views separately
			continue
		}

		chTable := models.ClickhouseTable{
			Name:     table.Name,
			Database: table.Schema,
			Engine:   "MergeTree", // Default engine
			Comment:  table.Comment,
		}

		// Convert columns
		for _, col := range table.Columns {
			chCol := models.ClickhouseColumn{
				Name:       col.Name,
				DataType:   col.DataType.Name,
				IsNullable: col.IsNullable,
				Comment:    col.Comment,
			}

			if col.DefaultValue != nil {
				chCol.DefaultValue = col.DefaultValue
			}

			chTable.Columns = append(chTable.Columns, chCol)
		}

		// Set primary key columns as order by
		for _, constraint := range table.Constraints {
			if constraint.Type == "PRIMARY KEY" {
				chTable.OrderBy = constraint.Columns
				break
			}
		}

		chSchema.Tables = append(chSchema.Tables, chTable)
	}

	// Convert views
	for _, table := range model.Tables {
		if table.TableType == "view" {
			chView := models.View{
				Name:     table.Name,
				Database: table.Schema,
				Engine:   "View",
				Comment:  table.Comment,
			}
			chSchema.Views = append(chSchema.Views, chView)
		}
	}

	// Convert functions
	for _, function := range model.Functions {
		chSchema.Functions = append(chSchema.Functions, models.Function{
			Name:       function.Name,
			Schema:     function.Schema,
			Arguments:  function.Arguments,
			ReturnType: function.ReturnType,
			Definition: function.Definition,
		})
	}

	return chSchema, warnings, nil
}
