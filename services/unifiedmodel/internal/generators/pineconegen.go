package generators

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

// PineconeGenerator implements the StatementGenerator interface for Pinecone vector database
type PineconeGenerator struct {
	BaseGenerator
}

// GenerateCreateTableSQL generates SQL for creating a table in Pinecone
func (g *PineconeGenerator) GenerateCreateTableSQL(table models.Table) (string, error) {
	var sb strings.Builder

	// Pinecone doesn't use traditional tables, but collections/indexes
	sb.WriteString(fmt.Sprintf("-- Create collection/index: %s\n", table.Name))
	sb.WriteString("-- Note: Pinecone doesn't use traditional tables\n")
	sb.WriteString(fmt.Sprintf("-- This would be a collection with %d dimensions\n", len(table.Columns)))

	// Add metadata fields
	if len(table.Columns) > 0 {
		sb.WriteString("-- Metadata fields:\n")
		for _, col := range table.Columns {
			sb.WriteString(fmt.Sprintf("--   - %s: %s\n", col.Name, col.DataType.Name))
		}
	}

	return sb.String(), nil
}

// GenerateCreateFunctionSQL generates SQL for creating a function in Pinecone
func (g *PineconeGenerator) GenerateCreateFunctionSQL(fn models.Function) (string, error) {
	var sb strings.Builder

	// Pinecone doesn't support functions directly
	sb.WriteString(fmt.Sprintf("-- Function: %s\n", fn.Name))
	sb.WriteString("-- Note: Pinecone doesn't support functions directly\n")
	sb.WriteString("-- Consider using application-level functions instead\n")

	return sb.String(), nil
}

// GenerateCreateTriggerSQL generates SQL for creating a trigger in Pinecone
func (g *PineconeGenerator) GenerateCreateTriggerSQL(trigger models.Trigger) (string, error) {
	var sb strings.Builder

	// Pinecone doesn't support triggers directly
	sb.WriteString(fmt.Sprintf("-- Trigger: %s\n", trigger.Name))
	sb.WriteString("-- Note: Pinecone doesn't support triggers directly\n")
	sb.WriteString("-- Consider using application-level triggers instead\n")

	return sb.String(), nil
}

// GenerateCreateSequenceSQL generates SQL for creating a sequence in Pinecone
func (g *PineconeGenerator) GenerateCreateSequenceSQL(seq models.Sequence) (string, error) {
	var sb strings.Builder

	// Pinecone doesn't support sequences directly
	sb.WriteString(fmt.Sprintf("-- Sequence: %s\n", seq.Name))
	sb.WriteString("-- Note: Pinecone doesn't support sequences directly\n")
	sb.WriteString("-- Consider using application-level sequence generation\n")

	return sb.String(), nil
}

// GenerateSchema generates a complete schema for Pinecone
func (g *PineconeGenerator) GenerateSchema(model *models.UnifiedModel) (string, []string, error) {
	var sb strings.Builder
	warnings := []string{}

	// Add header comment
	sb.WriteString("-- Pinecone Schema Generated from UnifiedModel\n\n")

	// Process tables (which represent collections/indexes in Pinecone)
	for _, table := range model.Tables {
		tableSQL, err := g.GenerateCreateTableSQL(table)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Error generating table %s.%s: %v", table.Schema, table.Name, err))
			continue
		}
		sb.WriteString(tableSQL)
		sb.WriteString("\n\n")
	}

	// Process functions
	for _, fn := range model.Functions {
		fnSQL, err := g.GenerateCreateFunctionSQL(fn)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Error generating function %s.%s: %v", fn.Schema, fn.Name, err))
			continue
		}
		sb.WriteString(fnSQL)
		sb.WriteString("\n\n")
	}

	// Process triggers
	for _, trigger := range model.Triggers {
		triggerSQL, err := g.GenerateCreateTriggerSQL(trigger)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Error generating trigger %s.%s: %v", trigger.Schema, trigger.Name, err))
			continue
		}
		sb.WriteString(triggerSQL)
		sb.WriteString("\n\n")
	}

	// Process sequences
	for _, seq := range model.Sequences {
		seqSQL, err := g.GenerateCreateSequenceSQL(seq)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Error generating sequence %s.%s: %v", seq.Schema, seq.Name, err))
			continue
		}
		sb.WriteString(seqSQL)
		sb.WriteString("\n\n")
	}

	return sb.String(), warnings, nil
}

// GenerateCreateStatements generates Pinecone-specific statements
func (g *PineconeGenerator) GenerateCreateStatements(schema interface{}) ([]string, error) {
	var statements []string

	// Add a sample statement for creating a Pinecone index
	statements = append(statements, "-- Create a Pinecone index")
	statements = append(statements, "CREATE INDEX my_index WITH DIMENSION=1536 METRIC=cosine")

	return statements, nil
}
