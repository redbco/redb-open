package translator

import (
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/core"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/cross_paradigm"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/same_paradigm"
)

// TranslatorFactory creates and configures translator instances
type TranslatorFactory struct{}

// NewTranslatorFactory creates a new translator factory
func NewTranslatorFactory() *TranslatorFactory {
	return &TranslatorFactory{}
}

// CreateUnifiedTranslator creates a fully configured unified translator
func (tf *TranslatorFactory) CreateUnifiedTranslator() core.UnifiedTranslator {
	// Create same-paradigm translator
	sameParadigmTranslator := same_paradigm.NewSameParadigmTranslator()

	// Create cross-paradigm translator
	crossParadigmTranslator := cross_paradigm.NewCrossParadigmTranslator()

	// Create and return the unified translator
	return core.NewUnifiedTranslator(sameParadigmTranslator, crossParadigmTranslator)
}

// CreateSameParadigmTranslator creates a same-paradigm translator
func (tf *TranslatorFactory) CreateSameParadigmTranslator() *same_paradigm.SameParadigmTranslatorImpl {
	return same_paradigm.NewSameParadigmTranslator()
}

// CreateCrossParadigmTranslator creates a cross-paradigm translator
func (tf *TranslatorFactory) CreateCrossParadigmTranslator() *cross_paradigm.CrossParadigmTranslatorImpl {
	return cross_paradigm.NewCrossParadigmTranslator()
}

// CreateParadigmAnalyzer creates a paradigm analyzer
func (tf *TranslatorFactory) CreateParadigmAnalyzer() *core.ParadigmAnalyzer {
	return core.NewParadigmAnalyzer()
}

// CreateObjectMapper creates an object mapper for same-paradigm translations
func (tf *TranslatorFactory) CreateObjectMapper() *same_paradigm.ObjectMapper {
	return same_paradigm.NewObjectMapper()
}

// CreateCapabilityFilter creates a capability filter
func (tf *TranslatorFactory) CreateCapabilityFilter() *same_paradigm.CapabilityFilter {
	return same_paradigm.NewCapabilityFilter()
}

// CreateEnrichmentAnalyzer creates an enrichment analyzer for cross-paradigm translations
func (tf *TranslatorFactory) CreateEnrichmentAnalyzer() *cross_paradigm.EnrichmentAnalyzer {
	return cross_paradigm.NewEnrichmentAnalyzer()
}

// CreateStructureTransformer creates a structure transformer for cross-paradigm translations
func (tf *TranslatorFactory) CreateStructureTransformer() *cross_paradigm.StructureTransformer {
	return cross_paradigm.NewStructureTransformer()
}

// CreateRelationshipMapper creates a relationship mapper for cross-paradigm translations
func (tf *TranslatorFactory) CreateRelationshipMapper() *cross_paradigm.RelationshipMapper {
	return cross_paradigm.NewRelationshipMapper()
}

// Default factory instance for convenience
var DefaultFactory = NewTranslatorFactory()

// Convenience functions using the default factory

// NewUnifiedTranslator creates a unified translator using the default factory
func NewUnifiedTranslator() core.UnifiedTranslator {
	return DefaultFactory.CreateUnifiedTranslator()
}

// NewSameParadigmTranslator creates a same-paradigm translator using the default factory
func NewSameParadigmTranslator() *same_paradigm.SameParadigmTranslatorImpl {
	return DefaultFactory.CreateSameParadigmTranslator()
}

// NewCrossParadigmTranslator creates a cross-paradigm translator using the default factory
func NewCrossParadigmTranslator() *cross_paradigm.CrossParadigmTranslatorImpl {
	return DefaultFactory.CreateCrossParadigmTranslator()
}

// NewParadigmAnalyzer creates a paradigm analyzer using the default factory
func NewParadigmAnalyzer() *core.ParadigmAnalyzer {
	return DefaultFactory.CreateParadigmAnalyzer()
}
