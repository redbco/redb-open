package adapters

import (
	"encoding/json"
	"fmt"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type SnowflakeIngester struct{}

func (s *SnowflakeIngester) IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error) {
	var snowflakeSchema models.SnowflakeSchema
	if err := json.Unmarshal(rawSchema, &snowflakeSchema); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal Snowflake schema: %w", err)
	}

	unifiedModel := &models.UnifiedModel{
		SchemaType: "unified",
	}
	warnings := []string{}

	// Convert schemas
	unifiedModel.Schemas = snowflakeSchema.Schemas

	// Convert tables
	unifiedModel.Tables = snowflakeSchema.Tables

	// Convert functions
	unifiedModel.Functions = snowflakeSchema.Functions

	// Convert sequences
	unifiedModel.Sequences = snowflakeSchema.Sequences

	// Convert views to tables with TableType "view"
	for _, view := range snowflakeSchema.Views {
		viewTable := models.Table{
			Name:      view.Name,
			Schema:    view.Schema,
			TableType: "view",
			Comment:   view.Comment,
			Owner:     view.Owner,
		}

		// Add a placeholder column for the view definition
		defColumn := models.Column{
			Name: "definition",
			DataType: models.DataType{
				Name:         "text",
				TypeCategory: "basic",
			},
			IsNullable: false,
			Comment:    "View definition",
		}
		viewTable.Columns = append(viewTable.Columns, defColumn)

		unifiedModel.Tables = append(unifiedModel.Tables, viewTable)
	}

	// Add Snowflake-specific extensions
	unifiedModel.Extensions = append(unifiedModel.Extensions, models.Extension{
		Name:        "snowflake",
		Schema:      "public",
		Version:     "1.0",
		Description: "Snowflake-specific extensions",
	})

	return unifiedModel, warnings, nil
}

type SnowflakeExporter struct{}

func (s *SnowflakeExporter) ExportSchema(model *models.UnifiedModel) (interface{}, []string, error) {
	snowflakeSchema := models.SnowflakeSchema{
		SchemaType: "snowflake",
	}
	warnings := []string{}

	// Convert schemas
	snowflakeSchema.Schemas = model.Schemas

	// Convert tables (excluding views)
	for _, table := range model.Tables {
		if table.TableType == "view" {
			// Handle views separately
			view := models.SnowflakeView{
				Name:           table.Name,
				Schema:         table.Schema,
				Database:       "default", // Default database
				Definition:     "",        // Would need to extract from columns
				IsMaterialized: false,
				Owner:          table.Owner,
				Comment:        table.Comment,
			}

			// Try to extract view definition from columns
			for _, col := range table.Columns {
				if col.Name == "definition" {
					view.Definition = col.Comment
					break
				}
			}

			snowflakeSchema.Views = append(snowflakeSchema.Views, view)
		} else {
			snowflakeSchema.Tables = append(snowflakeSchema.Tables, table)
		}
	}

	// Convert functions
	snowflakeSchema.Functions = model.Functions

	// Convert sequences
	snowflakeSchema.Sequences = model.Sequences

	// Add default warehouse if none exists
	if len(snowflakeSchema.Warehouses) == 0 {
		snowflakeSchema.Warehouses = append(snowflakeSchema.Warehouses, models.SnowflakeWarehouse{
			Name:            "COMPUTE_WH",
			Size:            "X-Small",
			MinClusterCount: 1,
			MaxClusterCount: 1,
			AutoSuspend:     60,
			AutoResume:      true,
			State:           "STARTED",
		})
	}

	return snowflakeSchema, warnings, nil
}
