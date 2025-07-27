package translator

import (
	"encoding/json"
	"fmt"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type SchemaTranslator struct {
	ingesters map[string]SchemaIngester
	exporters map[string]SchemaExporter
}

type SchemaIngester interface {
	IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error)
}

type SchemaExporter interface {
	ExportSchema(model *models.UnifiedModel) (interface{}, []string, error)
}

// TranslationResponse is the response from the translator
type TranslationResponse struct {
	ConvertedStructure interface{} `json:"convertedStructure"`
	Warnings           []string    `json:"warnings"`
}

func NewSchemaTranslator() *SchemaTranslator {
	return &SchemaTranslator{
		ingesters: make(map[string]SchemaIngester),
		exporters: make(map[string]SchemaExporter),
	}
}

func (st *SchemaTranslator) RegisterIngester(dbType string, ingester SchemaIngester) {
	st.ingesters[dbType] = ingester
}

func (st *SchemaTranslator) RegisterExporter(dbType string, exporter SchemaExporter) {
	st.exporters[dbType] = exporter
}

func (st *SchemaTranslator) Translate(sourceType, targetType string, sourceSchema json.RawMessage) (*TranslationResponse, error) {
	// Get ingester for source type
	ingester, ok := st.ingesters[sourceType]
	if !ok {
		return nil, fmt.Errorf("unsupported source database type: %s", sourceType)
	}

	// Convert source schema to common model
	commonModel, ingestWarnings, err := ingester.IngestSchema(sourceSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to ingest schema: %v", err)
	}

	// If target is "unified", return the unified model directly
	if targetType == "unified" {
		// Add SchemaType to the unified model
		unifiedWithType := struct {
			SchemaType string `json:"schemaType"`
			*models.UnifiedModel
		}{
			SchemaType:   "unified",
			UnifiedModel: commonModel,
		}

		return &TranslationResponse{
			ConvertedStructure: unifiedWithType,
			Warnings:           ingestWarnings,
		}, nil
	}

	// Get exporter for target type
	exporter, ok := st.exporters[targetType]
	if !ok {
		return nil, fmt.Errorf("unsupported target database type: %s", targetType)
	}

	// Convert common model to target schema
	targetSchema, exportWarnings, err := exporter.ExportSchema(commonModel)
	if err != nil {
		return nil, fmt.Errorf("failed to export schema: %v", err)
	}

	// Combine warnings
	warnings := append(ingestWarnings, exportWarnings...)

	return &TranslationResponse{
		ConvertedStructure: targetSchema,
		Warnings:           warnings,
	}, nil
}
