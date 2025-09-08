package core

import (
	"fmt"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// ParadigmAnalyzer analyzes database paradigms and determines conversion approach
type ParadigmAnalyzer struct {
	conversionUtils *unifiedmodel.ConversionUtils
}

// NewParadigmAnalyzer creates a new paradigm analyzer
func NewParadigmAnalyzer() *ParadigmAnalyzer {
	return &ParadigmAnalyzer{
		conversionUtils: unifiedmodel.NewConversionUtils(),
	}
}

// AnalyzeParadigms analyzes source and target paradigms for conversion
func (pa *ParadigmAnalyzer) AnalyzeParadigms(sourceDB, targetDB dbcapabilities.DatabaseType) (*ParadigmAnalysisResult, error) {
	// Get database capabilities
	sourceCapability, sourceExists := dbcapabilities.Get(sourceDB)
	if !sourceExists {
		return nil, fmt.Errorf("unknown source database: %s", sourceDB)
	}

	targetCapability, targetExists := dbcapabilities.Get(targetDB)
	if !targetExists {
		return nil, fmt.Errorf("unknown target database: %s", targetDB)
	}

	// Perform quick conversion analysis
	analysis, err := pa.conversionUtils.QuickConversionAnalysis(sourceDB, targetDB)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze conversion: %w", err)
	}

	// Determine conversion approach
	approach := pa.determineConversionApproach(sourceCapability.Paradigms, targetCapability.Paradigms)

	// Get required enrichment types
	enrichmentTypes := pa.getRequiredEnrichmentTypes(sourceCapability.Paradigms, targetCapability.Paradigms, pa.mapParadigmCompatibility(analysis.ParadigmCompatibility))

	// Generate conversion strategies
	strategies := pa.getConversionStrategies(sourceCapability.Paradigms, targetCapability.Paradigms)

	// Determine paradigm compatibility directly from paradigms
	paradigmCompatibility := pa.determineParadigmCompatibility(sourceCapability.Paradigms, targetCapability.Paradigms)

	result := &ParadigmAnalysisResult{
		SourceDatabase:          sourceDB,
		TargetDatabase:          targetDB,
		SourceParadigms:         sourceCapability.Paradigms,
		TargetParadigms:         targetCapability.Paradigms,
		ParadigmCompatibility:   paradigmCompatibility,
		ConversionApproach:      approach,
		TranslationComplexity:   pa.mapTranslationComplexity(analysis.ConversionComplexity),
		RequiresEnrichment:      analysis.RequiresEnrichment,
		RequiredEnrichmentTypes: enrichmentTypes,
		AvailableStrategies:     strategies,
		RecommendedStrategy:     pa.getRecommendedStrategy(strategies, approach),
		EstimatedSuccessRate:    0.85, // Default success rate - will be calculated properly
		UnsupportedFeatures:     pa.mapUnsupportedFeatures(analysis.UnsupportedFeatures),
		Recommendations:         analysis.Recommendations,
	}

	return result, nil
}

// ParadigmAnalysisResult contains the results of paradigm analysis
type ParadigmAnalysisResult struct {
	SourceDatabase          dbcapabilities.DatabaseType   `json:"source_database"`
	TargetDatabase          dbcapabilities.DatabaseType   `json:"target_database"`
	SourceParadigms         []dbcapabilities.DataParadigm `json:"source_paradigms"`
	TargetParadigms         []dbcapabilities.DataParadigm `json:"target_paradigms"`
	ParadigmCompatibility   ParadigmCompatibility         `json:"paradigm_compatibility"`
	ConversionApproach      ConversionApproach            `json:"conversion_approach"`
	TranslationComplexity   TranslationComplexity         `json:"translation_complexity"`
	RequiresEnrichment      bool                          `json:"requires_enrichment"`
	RequiredEnrichmentTypes []EnrichmentType              `json:"required_enrichment_types,omitempty"`
	AvailableStrategies     []ConversionStrategy          `json:"available_strategies"`
	RecommendedStrategy     ConversionStrategy            `json:"recommended_strategy"`
	EstimatedSuccessRate    float64                       `json:"estimated_success_rate"`
	UnsupportedFeatures     []string                      `json:"unsupported_features"`
	Recommendations         []string                      `json:"recommendations"`
}

// ConversionApproach defines the high-level approach for conversion
type ConversionApproach string

const (
	ConversionApproachSameParadigm  ConversionApproach = "same_paradigm"
	ConversionApproachCrossParadigm ConversionApproach = "cross_paradigm"
	ConversionApproachMultiStep     ConversionApproach = "multi_step"
	ConversionApproachImpossible    ConversionApproach = "impossible"
)

// determineConversionApproach determines the conversion approach based on paradigms
func (pa *ParadigmAnalyzer) determineConversionApproach(sourceParadigms, targetParadigms []dbcapabilities.DataParadigm) ConversionApproach {
	// Check for identical paradigms
	if pa.hasIdenticalParadigms(sourceParadigms, targetParadigms) {
		return ConversionApproachSameParadigm
	}

	// Check for overlapping paradigms
	if pa.hasOverlappingParadigms(sourceParadigms, targetParadigms) {
		return ConversionApproachSameParadigm
	}

	// Check if cross-paradigm conversion is possible
	if pa.isCrossParadigmConversionPossible(sourceParadigms, targetParadigms) {
		return ConversionApproachCrossParadigm
	}

	// Check if multi-step conversion is needed
	if pa.isMultiStepConversionPossible(sourceParadigms, targetParadigms) {
		return ConversionApproachMultiStep
	}

	return ConversionApproachImpossible
}

// hasIdenticalParadigms checks if source and target have identical paradigms
func (pa *ParadigmAnalyzer) hasIdenticalParadigms(source, target []dbcapabilities.DataParadigm) bool {
	if len(source) != len(target) {
		return false
	}

	sourceMap := make(map[dbcapabilities.DataParadigm]bool)
	for _, paradigm := range source {
		sourceMap[paradigm] = true
	}

	for _, paradigm := range target {
		if !sourceMap[paradigm] {
			return false
		}
	}

	return true
}

// hasOverlappingParadigms checks if source and target have overlapping paradigms
func (pa *ParadigmAnalyzer) hasOverlappingParadigms(source, target []dbcapabilities.DataParadigm) bool {
	sourceMap := make(map[dbcapabilities.DataParadigm]bool)
	for _, paradigm := range source {
		sourceMap[paradigm] = true
	}

	for _, paradigm := range target {
		if sourceMap[paradigm] {
			return true
		}
	}

	return false
}

// isCrossParadigmConversionPossible checks if cross-paradigm conversion is feasible
func (pa *ParadigmAnalyzer) isCrossParadigmConversionPossible(source, target []dbcapabilities.DataParadigm) bool {
	// Define paradigm conversion compatibility matrix
	compatibleConversions := map[dbcapabilities.DataParadigm][]dbcapabilities.DataParadigm{
		dbcapabilities.ParadigmRelational: {
			dbcapabilities.ParadigmDocument,
			dbcapabilities.ParadigmGraph,
			dbcapabilities.ParadigmKeyValue,
			dbcapabilities.ParadigmColumnar,
			dbcapabilities.ParadigmWideColumn,
		},
		dbcapabilities.ParadigmDocument: {
			dbcapabilities.ParadigmRelational,
			dbcapabilities.ParadigmGraph,
			dbcapabilities.ParadigmKeyValue,
			dbcapabilities.ParadigmSearchIndex,
			dbcapabilities.ParadigmVector,
		},
		dbcapabilities.ParadigmGraph: {
			dbcapabilities.ParadigmRelational,
			dbcapabilities.ParadigmDocument,
			dbcapabilities.ParadigmKeyValue,
		},
		dbcapabilities.ParadigmKeyValue: {
			dbcapabilities.ParadigmRelational,
			dbcapabilities.ParadigmDocument,
			dbcapabilities.ParadigmWideColumn,
		},
		dbcapabilities.ParadigmColumnar: {
			dbcapabilities.ParadigmRelational,
			dbcapabilities.ParadigmWideColumn,
			dbcapabilities.ParadigmTimeSeries,
		},
		dbcapabilities.ParadigmWideColumn: {
			dbcapabilities.ParadigmRelational,
			dbcapabilities.ParadigmKeyValue,
			dbcapabilities.ParadigmColumnar,
		},
		dbcapabilities.ParadigmSearchIndex: {
			dbcapabilities.ParadigmDocument,
			dbcapabilities.ParadigmKeyValue,
		},
		dbcapabilities.ParadigmVector: {
			dbcapabilities.ParadigmDocument,
			dbcapabilities.ParadigmSearchIndex,
		},
		dbcapabilities.ParadigmTimeSeries: {
			dbcapabilities.ParadigmColumnar,
			dbcapabilities.ParadigmWideColumn,
		},
	}

	// Check if any source paradigm can convert to any target paradigm
	for _, sourceParadigm := range source {
		if compatibleTargets, exists := compatibleConversions[sourceParadigm]; exists {
			for _, targetParadigm := range target {
				for _, compatibleTarget := range compatibleTargets {
					if targetParadigm == compatibleTarget {
						return true
					}
				}
			}
		}
	}

	return false
}

// isMultiStepConversionPossible checks if multi-step conversion is feasible
func (pa *ParadigmAnalyzer) isMultiStepConversionPossible(source, target []dbcapabilities.DataParadigm) bool {
	// For now, assume multi-step is possible through relational paradigm
	// This is a simplified implementation - could be enhanced with actual path finding
	hasRelational := true // Simplified assumption

	if hasRelational {
		// Check if source can convert to relational and relational can convert to target
		sourceToRelational := pa.isCrossParadigmConversionPossible(source, []dbcapabilities.DataParadigm{dbcapabilities.ParadigmRelational})
		relationalToTarget := pa.isCrossParadigmConversionPossible([]dbcapabilities.DataParadigm{dbcapabilities.ParadigmRelational}, target)
		return sourceToRelational && relationalToTarget
	}

	return false
}

// getRequiredEnrichmentTypes determines what enrichment data is needed
func (pa *ParadigmAnalyzer) getRequiredEnrichmentTypes(source, target []dbcapabilities.DataParadigm, compatibility ParadigmCompatibility) []EnrichmentType {
	var enrichmentTypes []EnrichmentType

	// Always useful for cross-paradigm conversions
	if compatibility == ParadigmCompatibilityPartial || compatibility == ParadigmCompatibilityIncompatible {
		enrichmentTypes = append(enrichmentTypes, EnrichmentTypeDataClassification)
		enrichmentTypes = append(enrichmentTypes, EnrichmentTypeRelationships)
	}

	// Specific paradigm combinations
	for _, sourceParadigm := range source {
		for _, targetParadigm := range target {
			switch {
			case sourceParadigm == dbcapabilities.ParadigmRelational && targetParadigm == dbcapabilities.ParadigmDocument:
				enrichmentTypes = append(enrichmentTypes, EnrichmentTypeAccessPatterns)
			case sourceParadigm == dbcapabilities.ParadigmRelational && targetParadigm == dbcapabilities.ParadigmGraph:
				enrichmentTypes = append(enrichmentTypes, EnrichmentTypeBusinessRules)
			case targetParadigm == dbcapabilities.ParadigmVector:
				enrichmentTypes = append(enrichmentTypes, EnrichmentTypeDataFlow)
			}
		}
	}

	// Remove duplicates
	return pa.removeDuplicateEnrichmentTypes(enrichmentTypes)
}

// getConversionStrategies determines available conversion strategies
func (pa *ParadigmAnalyzer) getConversionStrategies(source, target []dbcapabilities.DataParadigm) []ConversionStrategy {
	var strategies []ConversionStrategy

	// Same paradigm strategies
	if pa.hasOverlappingParadigms(source, target) {
		strategies = append(strategies, ConversionStrategyDirect)
		strategies = append(strategies, ConversionStrategyTransform)
	}

	// Cross-paradigm strategies
	for _, sourceParadigm := range source {
		for _, targetParadigm := range target {
			switch {
			case sourceParadigm == dbcapabilities.ParadigmRelational && targetParadigm == dbcapabilities.ParadigmDocument:
				strategies = append(strategies, ConversionStrategyDenormalization)
			case sourceParadigm == dbcapabilities.ParadigmDocument && targetParadigm == dbcapabilities.ParadigmRelational:
				strategies = append(strategies, ConversionStrategyNormalization)
			case sourceParadigm == dbcapabilities.ParadigmRelational && targetParadigm == dbcapabilities.ParadigmGraph:
				strategies = append(strategies, ConversionStrategyDecomposition)
			case sourceParadigm == dbcapabilities.ParadigmGraph && targetParadigm == dbcapabilities.ParadigmRelational:
				strategies = append(strategies, ConversionStrategyAggregation)
			case targetParadigm == dbcapabilities.ParadigmVector:
				strategies = append(strategies, ConversionStrategyDecomposition)
			}
		}
	}

	// Always available as fallback
	strategies = append(strategies, ConversionStrategyHybrid)

	return pa.removeDuplicateStrategies(strategies)
}

// getRecommendedStrategy selects the best strategy for the conversion
func (pa *ParadigmAnalyzer) getRecommendedStrategy(strategies []ConversionStrategy, approach ConversionApproach) ConversionStrategy {
	if len(strategies) == 0 {
		return ConversionStrategyHybrid
	}

	// Prefer direct conversion for same paradigm
	if approach == ConversionApproachSameParadigm {
		for _, strategy := range strategies {
			if strategy == ConversionStrategyDirect {
				return strategy
			}
		}
		for _, strategy := range strategies {
			if strategy == ConversionStrategyTransform {
				return strategy
			}
		}
	}

	// For cross-paradigm, prefer specific strategies over hybrid
	if approach == ConversionApproachCrossParadigm {
		for _, strategy := range strategies {
			if strategy != ConversionStrategyHybrid {
				return strategy
			}
		}
	}

	// Default to first available strategy
	return strategies[0]
}

// Helper functions for mapping between types

func (pa *ParadigmAnalyzer) determineParadigmCompatibility(sourceParadigms, targetParadigms []dbcapabilities.DataParadigm) ParadigmCompatibility {
	// Check for exact paradigm matches
	commonParadigms := 0
	for _, sourceParadigm := range sourceParadigms {
		for _, targetParadigm := range targetParadigms {
			if sourceParadigm == targetParadigm {
				commonParadigms++
				break
			}
		}
	}

	// Determine compatibility based on common paradigms
	if commonParadigms == len(sourceParadigms) && commonParadigms == len(targetParadigms) {
		return ParadigmCompatibilityIdentical
	} else if commonParadigms > 0 {
		return ParadigmCompatibilityCompatible
	} else if len(sourceParadigms) == 1 && len(targetParadigms) == 1 {
		// Single paradigm to single paradigm - check if conversion is possible
		source := sourceParadigms[0]
		target := targetParadigms[0]

		// Define paradigm conversion compatibility
		if (source == dbcapabilities.ParadigmRelational && target == dbcapabilities.ParadigmDocument) ||
			(source == dbcapabilities.ParadigmRelational && target == dbcapabilities.ParadigmGraph) ||
			(source == dbcapabilities.ParadigmDocument && target == dbcapabilities.ParadigmRelational) ||
			(source == dbcapabilities.ParadigmGraph && target == dbcapabilities.ParadigmRelational) {
			return ParadigmCompatibilityPartial
		}
	}

	return ParadigmCompatibilityIncompatible
}

func (pa *ParadigmAnalyzer) mapParadigmCompatibility(compatibility unifiedmodel.ParadigmCompatibility) ParadigmCompatibility {
	switch compatibility {
	case unifiedmodel.ParadigmCompatibilityIdentical:
		return ParadigmCompatibilityIdentical
	case unifiedmodel.ParadigmCompatibilityCompatible:
		return ParadigmCompatibilityCompatible
	case unifiedmodel.ParadigmCompatibilityPartial:
		return ParadigmCompatibilityPartial
	case unifiedmodel.ParadigmCompatibilityIncompatible:
		return ParadigmCompatibilityIncompatible
	default:
		return ParadigmCompatibilityIncompatible
	}
}

func (pa *ParadigmAnalyzer) mapTranslationComplexity(complexity unifiedmodel.ConversionComplexity) TranslationComplexity {
	switch complexity {
	case unifiedmodel.ConversionComplexityTrivial:
		return TranslationComplexityTrivial
	case unifiedmodel.ConversionComplexitySimple:
		return TranslationComplexitySimple
	case unifiedmodel.ConversionComplexityModerate:
		return TranslationComplexityModerate
	case unifiedmodel.ConversionComplexityComplex:
		return TranslationComplexityComplex
	case unifiedmodel.ConversionComplexityImpossible:
		return TranslationComplexityImpossible
	default:
		return TranslationComplexityComplex
	}
}

func (pa *ParadigmAnalyzer) mapUnsupportedFeatures(features []string) []string {
	// Direct mapping for now - could be enhanced with more detailed feature analysis
	return features
}

// Utility functions

func (pa *ParadigmAnalyzer) removeDuplicateEnrichmentTypes(types []EnrichmentType) []EnrichmentType {
	seen := make(map[EnrichmentType]bool)
	result := []EnrichmentType{}

	for _, t := range types {
		if !seen[t] {
			seen[t] = true
			result = append(result, t)
		}
	}

	return result
}

func (pa *ParadigmAnalyzer) removeDuplicateStrategies(strategies []ConversionStrategy) []ConversionStrategy {
	seen := make(map[ConversionStrategy]bool)
	result := []ConversionStrategy{}

	for _, strategy := range strategies {
		if !seen[strategy] {
			seen[strategy] = true
			result = append(result, strategy)
		}
	}

	return result
}
