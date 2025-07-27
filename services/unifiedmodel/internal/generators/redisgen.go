package generators

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

// RedisGenerator generates Redis commands from a UnifiedModel
type RedisGenerator struct {
	BaseGenerator
}

// GenerateCreateStatements generates Redis commands to create the schema
func (r *RedisGenerator) GenerateCreateStatements(schema interface{}) ([]string, error) {
	redisSchema, ok := schema.(*models.RedisSchema)
	if !ok {
		return nil, fmt.Errorf("expected RedisSchema, got %T", schema)
	}

	var statements []string

	// Generate commands for keys
	for _, key := range redisSchema.Keys {
		// For each key, we'll generate a comment and a sample command
		comment := fmt.Sprintf("-- Key: %s (Type: %s)", key.Name, key.Type)
		statements = append(statements, comment)

		// Generate a sample command based on the key type
		var cmd string
		switch key.Type {
		case "string":
			cmd = fmt.Sprintf("SET %s \"value\"", key.Name)
		case "list":
			cmd = fmt.Sprintf("RPUSH %s \"value1\" \"value2\"", key.Name)
		case "set":
			cmd = fmt.Sprintf("SADD %s \"member1\" \"member2\"", key.Name)
		case "hash":
			cmd = fmt.Sprintf("HSET %s field1 \"value1\" field2 \"value2\"", key.Name)
		case "zset":
			cmd = fmt.Sprintf("ZADD %s 1.0 \"member1\" 2.0 \"member2\"", key.Name)
		case "stream":
			cmd = fmt.Sprintf("XADD %s * field1 \"value1\" field2 \"value2\"", key.Name)
		default:
			cmd = fmt.Sprintf("-- Unknown key type: %s", key.Type)
		}

		// Add TTL if specified
		if key.TTL > 0 {
			cmd = fmt.Sprintf("%s\nEXPIRE %s %d", cmd, key.Name, key.TTL)
		}

		statements = append(statements, cmd)
		statements = append(statements, "") // Empty line for readability
	}

	// Generate commands for streams
	for _, stream := range redisSchema.Streams {
		comment := fmt.Sprintf("-- Stream: %s (Length: %d, Groups: %d)",
			stream.Name, stream.Length, stream.Groups)
		statements = append(statements, comment)

		// Add a sample entry to the stream
		cmd := fmt.Sprintf("XADD %s * field1 \"value1\" field2 \"value2\"", stream.Name)
		statements = append(statements, cmd)

		// If the stream has groups, add a sample consumer group
		if stream.Groups > 0 {
			groupCmd := fmt.Sprintf("XGROUP CREATE %s mygroup $ MKSTREAM", stream.Name)
			statements = append(statements, groupCmd)
		}

		statements = append(statements, "") // Empty line for readability
	}

	// Generate commands for functions
	for _, function := range redisSchema.Functions {
		comment := fmt.Sprintf("-- Function: %s (Library: %s)", function.Name, function.Library)
		statements = append(statements, comment)

		// Create a function using FUNCTION LOAD
		// Note: This is a simplified example, actual function loading would be more complex
		cmd := fmt.Sprintf("FUNCTION LOAD %s %s", function.Library, function.Body)
		statements = append(statements, cmd)
		statements = append(statements, "") // Empty line for readability
	}

	// Generate commands for modules
	for _, module := range redisSchema.Modules {
		comment := fmt.Sprintf("-- Module: %s", module.Name)
		statements = append(statements, comment)

		// Note: Module loading is typically done at server startup
		// This is just a comment for documentation
		cmd := fmt.Sprintf("-- Module %s would be loaded at server startup", module.Name)
		statements = append(statements, cmd)
		statements = append(statements, "") // Empty line for readability
	}

	// Generate commands for keyspaces
	for _, keyspace := range redisSchema.KeySpaces {
		comment := fmt.Sprintf("-- Keyspace: %d (Keys: %d, Expires: %d, AvgTTL: %d)",
			keyspace.ID, keyspace.Keys, keyspace.Expires, keyspace.AvgTTL)
		statements = append(statements, comment)

		// Select the database
		cmd := fmt.Sprintf("SELECT %d", keyspace.ID)
		statements = append(statements, cmd)
		statements = append(statements, "") // Empty line for readability
	}

	return statements, nil
}

// GenerateSchema generates a complete Redis schema from a UnifiedModel
func (r *RedisGenerator) GenerateSchema(model *models.UnifiedModel) (string, []string, error) {
	var sb strings.Builder
	warnings := []string{}

	// Add header comment
	sb.WriteString("-- Redis Schema Generated from UnifiedModel\n")
	sb.WriteString("-- This is a representation of the Redis data structure\n\n")

	// Process tables (which represent Redis keys)
	for _, table := range model.Tables {
		if table.Schema != "redis" {
			warnings = append(warnings, fmt.Sprintf("Skipping table %s.%s: not a Redis table",
				table.Schema, table.Name))
			continue
		}

		// Add comment for the table
		sb.WriteString(fmt.Sprintf("-- %s: %s\n", table.TableType, table.Name))
		if table.Comment != "" {
			sb.WriteString(fmt.Sprintf("-- %s\n", table.Comment))
		}

		// Generate sample command based on table type
		switch table.TableType {
		case "string":
			sb.WriteString(fmt.Sprintf("SET %s \"value\"\n", table.Name))
		case "list":
			sb.WriteString(fmt.Sprintf("RPUSH %s \"value1\" \"value2\"\n", table.Name))
		case "set":
			sb.WriteString(fmt.Sprintf("SADD %s \"member1\" \"member2\"\n", table.Name))
		case "hash":
			sb.WriteString(fmt.Sprintf("HSET %s field1 \"value1\" field2 \"value2\"\n", table.Name))
		case "zset":
			sb.WriteString(fmt.Sprintf("ZADD %s 1.0 \"member1\" 2.0 \"member2\"\n", table.Name))
		case "stream":
			sb.WriteString(fmt.Sprintf("XADD %s * field1 \"value1\" field2 \"value2\"\n", table.Name))

			// Add consumer group if this is a stream
			sb.WriteString(fmt.Sprintf("XGROUP CREATE %s mygroup $ MKSTREAM\n", table.Name))
		default:
			sb.WriteString(fmt.Sprintf("-- Unknown table type: %s\n", table.TableType))
		}

		// Check for TTL column
		for _, col := range table.Columns {
			if col.Name == "ttl" {
				sb.WriteString(fmt.Sprintf("EXPIRE %s 3600\n", table.Name))
				break
			}
		}

		sb.WriteString("\n") // Empty line for readability
	}

	// Process functions
	for _, fn := range model.Functions {
		if fn.Schema != "redis" {
			continue
		}

		sb.WriteString(fmt.Sprintf("-- Function: %s\n", fn.Name))
		sb.WriteString(fmt.Sprintf("-- Arguments: %v\n", fn.Arguments))
		sb.WriteString(fmt.Sprintf("-- Return type: %s\n", fn.ReturnType))

		// Add function definition
		sb.WriteString(fmt.Sprintf("FUNCTION LOAD %s\n", fn.Definition))
		sb.WriteString("\n")
	}

	// Process extensions (which represent Redis modules)
	for _, ext := range model.Extensions {
		if ext.Schema != "redis" || ext.Name == "redis" {
			continue
		}

		sb.WriteString(fmt.Sprintf("-- Module: %s\n", ext.Name))
		sb.WriteString(fmt.Sprintf("-- Version: %s\n", ext.Version))
		if ext.Description != "" {
			sb.WriteString(fmt.Sprintf("-- %s\n", ext.Description))
		}
		sb.WriteString("-- Note: Modules are typically loaded at server startup\n\n")
	}

	return sb.String(), warnings, nil
}

// GenerateCreateTableSQL implements StatementGenerator interface
func (r *RedisGenerator) GenerateCreateTableSQL(table models.Table) (string, error) {
	return r.BaseGenerator.GenerateCreateTableSQL(table)
}

// GenerateCreateFunctionSQL implements StatementGenerator interface
func (r *RedisGenerator) GenerateCreateFunctionSQL(fn models.Function) (string, error) {
	return r.BaseGenerator.GenerateCreateFunctionSQL(fn)
}

// GenerateCreateTriggerSQL implements StatementGenerator interface
func (r *RedisGenerator) GenerateCreateTriggerSQL(trigger models.Trigger) (string, error) {
	return r.BaseGenerator.GenerateCreateTriggerSQL(trigger)
}

// GenerateCreateSequenceSQL implements StatementGenerator interface
func (r *RedisGenerator) GenerateCreateSequenceSQL(seq models.Sequence) (string, error) {
	return r.BaseGenerator.GenerateCreateSequenceSQL(seq)
}
