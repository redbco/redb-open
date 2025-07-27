package adapters

import (
	"encoding/json"
	"fmt"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

// RedisIngester converts Redis schema to UnifiedModel
type RedisIngester struct{}

// IngestSchema converts a Redis schema to a UnifiedModel
func (r *RedisIngester) IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error) {
	var redisSchema models.RedisSchema
	if err := json.Unmarshal(rawSchema, &redisSchema); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal Redis schema: %w", err)
	}

	unifiedModel := &models.UnifiedModel{
		SchemaType: "unified",
	}
	warnings := []string{}

	// Convert Redis keys to tables
	for _, key := range redisSchema.Keys {
		// Create a table for each Redis key
		table := models.Table{
			Name:      key.Name,
			Schema:    "redis",
			TableType: key.Type,
			Comment:   fmt.Sprintf("Redis key of type %s", key.Type),
		}

		// Add columns based on the key type
		switch key.Type {
		case "string":
			// For string type, add a single value column
			table.Columns = append(table.Columns, models.Column{
				Name: "value",
				DataType: models.DataType{
					Name:         "text",
					TypeCategory: "basic",
				},
				IsNullable: false,
				Comment:    "String value",
			})
		case "list":
			// For list type, add an index and value column
			table.Columns = append(table.Columns, models.Column{
				Name: "index",
				DataType: models.DataType{
					Name:         "integer",
					TypeCategory: "basic",
				},
				IsNullable: false,
				Comment:    "List index",
			})
			table.Columns = append(table.Columns, models.Column{
				Name: "value",
				DataType: models.DataType{
					Name:         "text",
					TypeCategory: "basic",
				},
				IsNullable: false,
				Comment:    "List value",
			})
		case "set":
			// For set type, add a value column
			table.Columns = append(table.Columns, models.Column{
				Name: "value",
				DataType: models.DataType{
					Name:         "text",
					TypeCategory: "basic",
				},
				IsNullable: false,
				Comment:    "Set member",
			})
		case "hash":
			// For hash type, add a field and value column
			table.Columns = append(table.Columns, models.Column{
				Name: "field",
				DataType: models.DataType{
					Name:         "text",
					TypeCategory: "basic",
				},
				IsNullable: false,
				Comment:    "Hash field",
			})
			table.Columns = append(table.Columns, models.Column{
				Name: "value",
				DataType: models.DataType{
					Name:         "text",
					TypeCategory: "basic",
				},
				IsNullable: false,
				Comment:    "Hash value",
			})
		case "zset":
			// For sorted set type, add a score and value column
			table.Columns = append(table.Columns, models.Column{
				Name: "score",
				DataType: models.DataType{
					Name:         "double",
					TypeCategory: "basic",
				},
				IsNullable: false,
				Comment:    "Sorted set score",
			})
			table.Columns = append(table.Columns, models.Column{
				Name: "value",
				DataType: models.DataType{
					Name:         "text",
					TypeCategory: "basic",
				},
				IsNullable: false,
				Comment:    "Sorted set member",
			})
		}

		// Add metadata columns
		table.Columns = append(table.Columns, models.Column{
			Name: "ttl",
			DataType: models.DataType{
				Name:         "bigint",
				TypeCategory: "basic",
			},
			IsNullable: true,
			Comment:    "Time to live in seconds",
		})

		unifiedModel.Tables = append(unifiedModel.Tables, table)
	}

	// Convert Redis streams to tables
	for _, stream := range redisSchema.Streams {
		// Create a table for each Redis stream
		table := models.Table{
			Name:      stream.Name,
			Schema:    "redis",
			TableType: "stream",
			Comment:   "Redis stream",
		}

		// Add columns for stream entries
		table.Columns = append(table.Columns, models.Column{
			Name: "id",
			DataType: models.DataType{
				Name:         "text",
				TypeCategory: "basic",
			},
			IsNullable: false,
			Comment:    "Stream entry ID",
		})
		table.Columns = append(table.Columns, models.Column{
			Name: "field",
			DataType: models.DataType{
				Name:         "text",
				TypeCategory: "basic",
			},
			IsNullable: false,
			Comment:    "Stream field",
		})
		table.Columns = append(table.Columns, models.Column{
			Name: "value",
			DataType: models.DataType{
				Name:         "text",
				TypeCategory: "basic",
			},
			IsNullable: false,
			Comment:    "Stream value",
		})

		unifiedModel.Tables = append(unifiedModel.Tables, table)
	}

	// Convert Redis modules to extensions
	for _, module := range redisSchema.Modules {
		unifiedModel.Extensions = append(unifiedModel.Extensions, models.Extension{
			Name:        module.Name,
			Schema:      "redis",
			Version:     "1.0",
			Description: fmt.Sprintf("Redis module: %s", module.Name),
		})
	}

	// Convert Redis functions to functions
	for _, function := range redisSchema.Functions {
		// Parse the arguments string into a list of parameters
		var params []models.FunctionParameter
		if function.Arguments != "" {
			// Simple parsing - in a real implementation, this would be more sophisticated
			params = append(params, models.FunctionParameter{
				Name:     "arg",
				DataType: "text",
			})
		}

		unifiedModel.Functions = append(unifiedModel.Functions, models.Function{
			Name:       function.Name,
			Schema:     "redis",
			Arguments:  params,
			ReturnType: function.ReturnType,
			Definition: function.Body,
		})
	}

	// Add Redis-specific extensions
	unifiedModel.Extensions = append(unifiedModel.Extensions, models.Extension{
		Name:        "redis",
		Schema:      "redis",
		Version:     "1.0",
		Description: "Redis-specific extensions",
	})

	return unifiedModel, warnings, nil
}

// RedisExporter converts UnifiedModel to Redis schema
type RedisExporter struct{}

// ExportSchema converts a UnifiedModel to a Redis schema
func (r *RedisExporter) ExportSchema(model *models.UnifiedModel) (interface{}, []string, error) {
	redisSchema := models.RedisSchema{
		SchemaType: "redis",
	}
	warnings := []string{}

	// Convert tables to Redis keys
	for _, table := range model.Tables {
		if table.Schema != "redis" {
			warnings = append(warnings, fmt.Sprintf("Skipping table %s.%s: not a Redis table", table.Schema, table.Name))
			continue
		}

		// Create a Redis key based on the table
		key := models.RedisKey{
			Name: table.Name,
			Type: table.TableType,
			TTL:  -1, // Default TTL
		}

		// Try to find TTL column
		for _, col := range table.Columns {
			if col.Name == "ttl" {
				// TTL found, but we can't determine the actual value from the schema
				key.TTL = 0
				break
			}
		}

		redisSchema.Keys = append(redisSchema.Keys, key)
	}

	// Convert extensions to Redis modules
	for _, ext := range model.Extensions {
		if ext.Schema == "redis" && ext.Name != "redis" {
			redisSchema.Modules = append(redisSchema.Modules, models.RedisModule{
				Name: ext.Name,
			})
		}
	}

	// Convert functions to Redis functions
	for _, fn := range model.Functions {
		if fn.Schema == "redis" {
			// Convert function parameters to a string representation
			argsStr := ""
			if len(fn.Arguments) > 0 {
				argsStr = fn.Arguments[0].DataType
			}

			redisSchema.Functions = append(redisSchema.Functions, models.RedisFunction{
				Name:       fn.Name,
				Library:    "default",
				Arguments:  argsStr,
				ReturnType: fn.ReturnType,
				Body:       fn.Definition,
			})
		}
	}

	return redisSchema, warnings, nil
}
