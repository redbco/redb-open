package adapters

import (
	"encoding/json"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

// SchemaIngester defines the interface for ingesting database schemas
type SchemaIngester interface {
	// IngestSchema converts a raw JSON schema into a unified model
	IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error)
}

// SchemaExporter defines the interface for exporting unified models to database schemas
type SchemaExporter interface {
	// ExportSchema converts a unified model into a database-specific schema
	ExportSchema(model *models.UnifiedModel) (interface{}, []string, error)
}
