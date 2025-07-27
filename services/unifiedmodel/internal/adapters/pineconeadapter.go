package adapters

import (
	"encoding/json"
	"fmt"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type PineconeIngester struct{}

func (p *PineconeIngester) IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error) {
	var pineconeSchema models.PineconeSchema
	if err := json.Unmarshal(rawSchema, &pineconeSchema); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal Pinecone schema: %w", err)
	}

	unifiedModel := &models.UnifiedModel{
		SchemaType: "unified",
	}
	warnings := []string{}

	// Convert indexes to tables
	for _, index := range pineconeSchema.Indexes {
		unifiedTable := models.Table{
			Name:      index.Name,
			Schema:    "default",
			TableType: "vector", // Special type for vector databases
			Comment:   fmt.Sprintf("Pinecone index with dimension %d, metric %s", index.Dimension, index.Metric),
		}

		// Add vector dimension as a special column
		dimensionCol := models.Column{
			Name: "vector",
			DataType: models.DataType{
				Name:         "vector",
				TypeCategory: "vector",
				Length:       index.Dimension,
				Modifiers:    []string{index.Metric},
			},
			IsNullable: false,
			Comment:    fmt.Sprintf("Vector column with dimension %d using %s metric", index.Dimension, index.Metric),
		}
		unifiedTable.Columns = append(unifiedTable.Columns, dimensionCol)

		// Add metadata columns if they exist
		if len(index.MetadataConfig.Indexed) > 0 {
			for _, metadataField := range index.MetadataConfig.Indexed {
				metadataCol := models.Column{
					Name: metadataField,
					DataType: models.DataType{
						Name:         "jsonb",
						TypeCategory: "basic",
					},
					IsNullable: true,
					Comment:    "Metadata field for vector search",
				}
				unifiedTable.Columns = append(unifiedTable.Columns, metadataCol)
			}
		}

		unifiedModel.Tables = append(unifiedModel.Tables, unifiedTable)
	}

	return unifiedModel, warnings, nil
}

type PineconeExporter struct{}

func (p *PineconeExporter) ExportSchema(model *models.UnifiedModel) (interface{}, []string, error) {
	pineconeSchema := models.PineconeSchema{
		SchemaType: "pinecone",
	}
	warnings := []string{}

	// Convert tables to indexes
	for _, table := range model.Tables {
		if table.TableType != "vector" {
			warnings = append(warnings, fmt.Sprintf("Skipping non-vector table: %s", table.Name))
			continue
		}

		// Find the vector column
		var dimension int
		var metric string
		var metadataFields []string

		for _, col := range table.Columns {
			if col.DataType.TypeCategory == "vector" {
				dimension = col.DataType.Length
				if len(col.DataType.Modifiers) > 0 {
					metric = col.DataType.Modifiers[0]
				} else {
					metric = "cosine" // Default metric
				}
			} else if col.DataType.Name == "jsonb" {
				metadataFields = append(metadataFields, col.Name)
			}
		}

		if dimension == 0 {
			warnings = append(warnings, fmt.Sprintf("No vector column found in table: %s", table.Name))
			continue
		}

		pineconeIndex := models.PineconeIndex{
			Name:      table.Name,
			Dimension: dimension,
			Metric:    metric,
			Pods:      1, // Default values
			Replicas:  1,
			PodType:   "p1.x1",
			Shards:    1,
			Status:    "Ready",
			MetadataConfig: models.MetadataConfig{
				Indexed: metadataFields,
			},
		}

		pineconeSchema.Indexes = append(pineconeSchema.Indexes, pineconeIndex)
	}

	return pineconeSchema, warnings, nil
}
