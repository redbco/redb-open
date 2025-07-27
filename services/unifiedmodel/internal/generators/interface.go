// internal/generators/interface.go
package generators

import (
	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type StatementGenerator interface {
	GenerateCreateTableSQL(table models.Table) (string, error)
	GenerateCreateFunctionSQL(fn models.Function) (string, error)
	GenerateCreateTriggerSQL(trigger models.Trigger) (string, error)
	GenerateCreateSequenceSQL(seq models.Sequence) (string, error)
	GenerateSchema(model *models.UnifiedModel) (string, []string, error)
	GenerateCreateStatements(schema interface{}) ([]string, error)
}
