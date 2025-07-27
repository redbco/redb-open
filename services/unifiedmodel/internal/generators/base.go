// internal/generators/base.go
package generators

import (
	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

// BaseGenerator provides a default implementation for the StatementGenerator interface
type BaseGenerator struct{}

func (b *BaseGenerator) GenerateCreateTableSQL(table models.Table) (string, error) {
	return "", nil
}

func (b *BaseGenerator) GenerateCreateFunctionSQL(fn models.Function) (string, error) {
	return "", nil
}

func (b *BaseGenerator) GenerateCreateTriggerSQL(trigger models.Trigger) (string, error) {
	return "", nil
}

func (b *BaseGenerator) GenerateCreateSequenceSQL(seq models.Sequence) (string, error) {
	return "", nil
}

func (b *BaseGenerator) GenerateSchema(model *models.UnifiedModel) (string, []string, error) {
	return "", nil, nil
}

func (b *BaseGenerator) GenerateCreateStatements(schema interface{}) ([]string, error) {
	return nil, nil
}
