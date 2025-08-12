package adapters

import (
	"encoding/json"
	"fmt"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

// ChromaIngester ingests Chroma vector database schema into the unified model
type ChromaIngester struct{}

func (c *ChromaIngester) IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error) {
	var chromaSchema models.ChromaSchema
	if err := json.Unmarshal(rawSchema, &chromaSchema); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal Chroma schema: %w", err)
	}

	unified := &models.UnifiedModel{SchemaType: "unified"}
	warnings := []string{}

	// Convert collections to unified tables
	for _, coll := range chromaSchema.Collections {
		table := models.Table{
			Name:      coll.Name,
			Schema:    "default",
			TableType: "vector",
			Comment:   fmt.Sprintf("Chroma collection (id=%s)", coll.ID),
		}

		// Vector column with dimension if known
		vecCol := models.Column{
			Name: "vector",
			DataType: models.DataType{
				Name:         "vector",
				TypeCategory: "vector",
				Length:       coll.Dimension,
				Modifiers:    []string{coll.DistanceFunction},
			},
			IsNullable: false,
			Comment:    "Embedding vector",
		}
		table.Columns = append(table.Columns, vecCol)

		// Add metadata as a generic JSON column to reflect presence of metadata fields
		if len(coll.Metadata) > 0 {
			table.Columns = append(table.Columns, models.Column{
				Name: "metadata",
				DataType: models.DataType{
					Name:         "jsonb",
					TypeCategory: "basic",
				},
				IsNullable: true,
				Comment:    "Document metadata",
			})
		}

		unified.Tables = append(unified.Tables, table)
	}

	return unified, warnings, nil
}
