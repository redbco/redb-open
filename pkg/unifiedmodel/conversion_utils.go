package unifiedmodel

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// ConversionUtils provides utility functions for the conversion framework
type ConversionUtils struct {
	engine *ConversionEngine
}

// NewConversionUtils creates a new ConversionUtils instance
func NewConversionUtils() *ConversionUtils {
	return &ConversionUtils{
		engine: NewConversionEngine(),
	}
}

// QuickConversionAnalysis provides a quick analysis of conversion feasibility
func (cu *ConversionUtils) QuickConversionAnalysis(source, target dbcapabilities.DatabaseType) (*ConversionAnalysis, error) {
	// Get paradigms
	sourceCapability, sourceExists := dbcapabilities.Get(source)
	targetCapability, targetExists := dbcapabilities.Get(target)

	if !sourceExists {
		return nil, fmt.Errorf("unknown source database: %s", source)
	}
	if !targetExists {
		return nil, fmt.Errorf("unknown target database: %s", target)
	}

	analysis := &ConversionAnalysis{
		SourceDatabase:  source,
		TargetDatabase:  target,
		SourceParadigms: sourceCapability.Paradigms,
		TargetParadigms: targetCapability.Paradigms,
	}

	// Check paradigm compatibility
	analysis.ParadigmCompatibility = cu.assessParadigmCompatibility(sourceCapability.Paradigms, targetCapability.Paradigms)

	// Get conversion complexity
	analysis.ConversionComplexity = GetConversionComplexity(source, target)

	// Check if conversion is supported
	analysis.ConversionSupported = analysis.ConversionComplexity != ConversionComplexityImpossible

	// Get requirements
	analysis.RequiresUserInput = RequiresUserInteraction(source, target)
	analysis.RequiresEnrichment = RequiresEnrichmentData(source, target)

	// Get unsupported features
	analysis.UnsupportedFeatures = GetUnsupportedFeatures(source, target)

	// Get conversion strategies
	analysis.AvailableStrategies = GetConversionStrategies(source, target)

	// Generate recommendations
	analysis.Recommendations = cu.generateRecommendations(analysis)

	return analysis, nil
}

// ConversionAnalysis provides a summary of conversion feasibility
type ConversionAnalysis struct {
	SourceDatabase        dbcapabilities.DatabaseType   `json:"source_database"`
	TargetDatabase        dbcapabilities.DatabaseType   `json:"target_database"`
	SourceParadigms       []dbcapabilities.DataParadigm `json:"source_paradigms"`
	TargetParadigms       []dbcapabilities.DataParadigm `json:"target_paradigms"`
	ParadigmCompatibility ParadigmCompatibility         `json:"paradigm_compatibility"`
	ConversionComplexity  ConversionComplexity          `json:"conversion_complexity"`
	ConversionSupported   bool                          `json:"conversion_supported"`
	RequiresUserInput     bool                          `json:"requires_user_input"`
	RequiresEnrichment    bool                          `json:"requires_enrichment"`
	UnsupportedFeatures   []string                      `json:"unsupported_features"`
	AvailableStrategies   []ConversionStrategy          `json:"available_strategies"`
	Recommendations       []string                      `json:"recommendations"`
}

// assessParadigmCompatibility determines compatibility between paradigm sets
func (cu *ConversionUtils) assessParadigmCompatibility(source, target []dbcapabilities.DataParadigm) ParadigmCompatibility {
	// Check for identical paradigms
	if cu.paradigmSetsEqual(source, target) {
		return ParadigmCompatibilityIdentical
	}

	// Check for overlap
	overlap := cu.paradigmOverlap(source, target)
	if len(overlap) > 0 {
		if len(overlap) == len(source) || len(overlap) == len(target) {
			return ParadigmCompatibilityCompatible
		}
		return ParadigmCompatibilityPartial
	}

	return ParadigmCompatibilityIncompatible
}

// paradigmSetsEqual checks if two paradigm sets are identical
func (cu *ConversionUtils) paradigmSetsEqual(a, b []dbcapabilities.DataParadigm) bool {
	if len(a) != len(b) {
		return false
	}

	aMap := make(map[dbcapabilities.DataParadigm]bool)
	for _, paradigm := range a {
		aMap[paradigm] = true
	}

	for _, paradigm := range b {
		if !aMap[paradigm] {
			return false
		}
	}

	return true
}

// paradigmOverlap finds common paradigms between two sets
func (cu *ConversionUtils) paradigmOverlap(a, b []dbcapabilities.DataParadigm) []dbcapabilities.DataParadigm {
	aMap := make(map[dbcapabilities.DataParadigm]bool)
	for _, paradigm := range a {
		aMap[paradigm] = true
	}

	var overlap []dbcapabilities.DataParadigm
	for _, paradigm := range b {
		if aMap[paradigm] {
			overlap = append(overlap, paradigm)
		}
	}

	return overlap
}

// generateRecommendations generates conversion recommendations
func (cu *ConversionUtils) generateRecommendations(analysis *ConversionAnalysis) []string {
	var recommendations []string

	switch analysis.ConversionComplexity {
	case ConversionComplexityTrivial:
		recommendations = append(recommendations, "This conversion is straightforward and can be automated")

	case ConversionComplexitySimple:
		recommendations = append(recommendations, "This conversion requires minimal configuration")
		if analysis.RequiresUserInput {
			recommendations = append(recommendations, "Some user decisions may be needed for optimal results")
		}

	case ConversionComplexityModerate:
		recommendations = append(recommendations, "This cross-paradigm conversion requires careful planning")
		if analysis.RequiresEnrichment {
			recommendations = append(recommendations, "Enrichment data will significantly improve conversion quality")
		}

	case ConversionComplexityComplex:
		recommendations = append(recommendations, "This conversion is complex and requires significant user input")
		recommendations = append(recommendations, "Consider breaking the conversion into phases")
		if len(analysis.UnsupportedFeatures) > 0 {
			recommendations = append(recommendations, "Plan for alternative implementations of unsupported features")
		}

	case ConversionComplexityImpossible:
		recommendations = append(recommendations, "Direct conversion is not supported")
		recommendations = append(recommendations, "Consider using an intermediate database or manual migration")
	}

	// Paradigm-specific recommendations
	switch analysis.ParadigmCompatibility {
	case ParadigmCompatibilityIncompatible:
		recommendations = append(recommendations, "Consider the fundamental differences between paradigms")
		recommendations = append(recommendations, "Data modeling will need to be completely rethought")

	case ParadigmCompatibilityPartial:
		recommendations = append(recommendations, "Focus on the compatible aspects first")
		recommendations = append(recommendations, "Plan for paradigm-specific features separately")
	}

	return recommendations
}

// GetConversionPath finds the best conversion path between databases
func (cu *ConversionUtils) GetConversionPath(source, target dbcapabilities.DatabaseType) (*ConversionPath, error) {
	// Direct conversion
	if IsConversionPossible(source, target) {
		complexity := GetConversionComplexity(source, target)
		return &ConversionPath{
			Steps: []ConversionStep{
				{
					From:       source,
					To:         target,
					Complexity: complexity,
					Direct:     true,
				},
			},
			TotalComplexity: complexity,
			Recommended:     complexity != ConversionComplexityImpossible,
		}, nil
	}

	// Try to find intermediate path
	intermediatePath := cu.findIntermediatePath(source, target)
	if intermediatePath != nil {
		return intermediatePath, nil
	}

	return nil, fmt.Errorf("no conversion path found from %s to %s", source, target)
}

// ConversionPath represents a conversion path between databases
type ConversionPath struct {
	Steps           []ConversionStep     `json:"steps"`
	TotalComplexity ConversionComplexity `json:"total_complexity"`
	Recommended     bool                 `json:"recommended"`
	EstimatedTime   string               `json:"estimated_time,omitempty"`
	Notes           []string             `json:"notes,omitempty"`
}

// ConversionStep represents a single step in a conversion path
type ConversionStep struct {
	From       dbcapabilities.DatabaseType `json:"from"`
	To         dbcapabilities.DatabaseType `json:"to"`
	Complexity ConversionComplexity        `json:"complexity"`
	Direct     bool                        `json:"direct"`
	Reason     string                      `json:"reason,omitempty"`
}

// findIntermediatePath attempts to find a conversion path through intermediate databases
func (cu *ConversionUtils) findIntermediatePath(source, target dbcapabilities.DatabaseType) *ConversionPath {
	// Get all database types
	allDatabases := dbcapabilities.IDs()

	// Try single intermediate step
	for _, intermediate := range allDatabases {
		if intermediate == source || intermediate == target {
			continue
		}

		if IsConversionPossible(source, intermediate) && IsConversionPossible(intermediate, target) {
			sourceComplexity := GetConversionComplexity(source, intermediate)
			targetComplexity := GetConversionComplexity(intermediate, target)

			// Only suggest if both steps are reasonable
			if sourceComplexity != ConversionComplexityImpossible &&
				targetComplexity != ConversionComplexityImpossible &&
				sourceComplexity != ConversionComplexityComplex &&
				targetComplexity != ConversionComplexityComplex {

				totalComplexity := cu.combineComplexity(sourceComplexity, targetComplexity)

				return &ConversionPath{
					Steps: []ConversionStep{
						{
							From:       source,
							To:         intermediate,
							Complexity: sourceComplexity,
							Direct:     false,
							Reason:     "Intermediate step for better compatibility",
						},
						{
							From:       intermediate,
							To:         target,
							Complexity: targetComplexity,
							Direct:     false,
							Reason:     "Final conversion step",
						},
					},
					TotalComplexity: totalComplexity,
					Recommended:     totalComplexity != ConversionComplexityImpossible,
					Notes: []string{
						fmt.Sprintf("Using %s as intermediate database", intermediate),
						"Two-step conversion may preserve more data fidelity",
					},
				}
			}
		}
	}

	return nil
}

// combineComplexity combines two complexity levels
func (cu *ConversionUtils) combineComplexity(a, b ConversionComplexity) ConversionComplexity {
	complexityOrder := map[ConversionComplexity]int{
		ConversionComplexityTrivial:    1,
		ConversionComplexitySimple:     2,
		ConversionComplexityModerate:   3,
		ConversionComplexityComplex:    4,
		ConversionComplexityImpossible: 5,
	}

	aLevel := complexityOrder[a]
	bLevel := complexityOrder[b]

	// Take the higher complexity level and add one level for multi-step
	maxLevel := aLevel
	if bLevel > aLevel {
		maxLevel = bLevel
	}

	// Add complexity for multi-step process
	maxLevel++

	for complexity, level := range complexityOrder {
		if level == maxLevel {
			return complexity
		}
	}

	return ConversionComplexityImpossible
}

// FormatConversionSummary creates a human-readable conversion summary
func (cu *ConversionUtils) FormatConversionSummary(analysis *ConversionAnalysis) string {
	var summary strings.Builder

	summary.WriteString(fmt.Sprintf("Conversion from %s to %s:\n", analysis.SourceDatabase, analysis.TargetDatabase))
	summary.WriteString(fmt.Sprintf("  Complexity: %s\n", analysis.ConversionComplexity))
	summary.WriteString(fmt.Sprintf("  Paradigm Compatibility: %s\n", analysis.ParadigmCompatibility))
	summary.WriteString(fmt.Sprintf("  Supported: %t\n", analysis.ConversionSupported))

	if analysis.RequiresUserInput {
		summary.WriteString("  ⚠️  Requires user input\n")
	}

	if analysis.RequiresEnrichment {
		summary.WriteString("  📊 Enrichment data recommended\n")
	}

	if len(analysis.UnsupportedFeatures) > 0 {
		summary.WriteString("  ❌ Unsupported features:\n")
		for _, feature := range analysis.UnsupportedFeatures {
			summary.WriteString(fmt.Sprintf("     - %s\n", feature))
		}
	}

	if len(analysis.Recommendations) > 0 {
		summary.WriteString("  💡 Recommendations:\n")
		for _, rec := range analysis.Recommendations {
			summary.WriteString(fmt.Sprintf("     - %s\n", rec))
		}
	}

	return summary.String()
}

// ValidateConversionRequest validates a conversion request
func (cu *ConversionUtils) ValidateConversionRequest(request ConversionRequest) []ValidationError {
	var errors []ValidationError

	if request.SourceSchema == nil {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorCritical,
			Field:   "source_schema",
			Message: "Source schema is required",
		})
	}

	if request.SourceDatabase == "" {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorCritical,
			Field:   "source_database",
			Message: "Source database type is required",
		})
	}

	if request.TargetDatabase == "" {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorCritical,
			Field:   "target_database",
			Message: "Target database type is required",
		})
	}

	if request.SourceDatabase == request.TargetDatabase {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorCritical,
			Field:   "target_database",
			Message: "Source and target databases cannot be the same",
		})
	}

	// Check if databases are supported
	if _, exists := dbcapabilities.Get(request.SourceDatabase); !exists {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorCritical,
			Field:   "source_database",
			Message: fmt.Sprintf("Unsupported source database: %s", request.SourceDatabase),
		})
	}

	if _, exists := dbcapabilities.Get(request.TargetDatabase); !exists {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorCritical,
			Field:   "target_database",
			Message: fmt.Sprintf("Unsupported target database: %s", request.TargetDatabase),
		})
	}

	// Check conversion feasibility
	if !IsConversionPossible(request.SourceDatabase, request.TargetDatabase) {
		errors = append(errors, ValidationError{
			Type:       ValidationErrorWarning,
			Field:      "conversion",
			Message:    fmt.Sprintf("Direct conversion from %s to %s may not be possible", request.SourceDatabase, request.TargetDatabase),
			Suggestion: "Consider using an intermediate database or manual migration",
		})
	}

	return errors
}

// GenerateConversionMatrix dynamically generates a conversion matrix between two databases
// based on their feature differences and paradigm compatibility.
func (cu *ConversionUtils) GenerateConversionMatrix(sourceDB, targetDB dbcapabilities.DatabaseType) (*ConversionMatrix, error) {
	// Always use dynamic generation - no pre-defined matrices

	// Get database features
	sourceFeatures, sourceExists := DatabaseFeatureRegistry[sourceDB]
	if !sourceExists {
		return nil, fmt.Errorf("source database %s not found in feature matrix", sourceDB)
	}

	targetFeatures, targetExists := DatabaseFeatureRegistry[targetDB]
	if !targetExists {
		return nil, fmt.Errorf("target database %s not found in feature matrix", targetDB)
	}

	// Determine paradigm compatibility
	paradigmCompatibility := cu.assessParadigmCompatibility(sourceFeatures.Paradigms, targetFeatures.Paradigms)

	// Determine conversion complexity based on paradigm compatibility and feature differences
	complexity := cu.assessConversionComplexity(sourceFeatures, targetFeatures, paradigmCompatibility)

	// Generate object conversion rules
	objectConversions := cu.generateObjectConversions(sourceFeatures, targetFeatures)

	// Determine if user input or enrichment is required
	requiresUserInput := complexity >= ConversionComplexityComplex || paradigmCompatibility <= ParadigmCompatibilityPartial
	requiresEnrichment := paradigmCompatibility <= ParadigmCompatibilityPartial

	// Get conversion strategies based on paradigms
	strategies := cu.getConversionStrategies(sourceFeatures.Paradigms, targetFeatures.Paradigms)

	// Estimate success rate and duration
	successRate := cu.estimateSuccessRate(complexity, paradigmCompatibility)
	duration := cu.estimateDuration(complexity, len(objectConversions))

	return &ConversionMatrix{
		SourceDatabase:        sourceDB,
		TargetDatabase:        targetDB,
		ConversionComplexity:  complexity,
		ParadigmCompatibility: paradigmCompatibility,
		RequiresUserInput:     requiresUserInput,
		RequiresEnrichment:    requiresEnrichment,
		ObjectConversions:     objectConversions,
		ConversionStrategies:  strategies,
		EstimatedDuration:     duration,
		SuccessRate:           successRate,
	}, nil
}

// assessConversionComplexity determines the overall complexity of conversion
func (cu *ConversionUtils) assessConversionComplexity(sourceFeatures, targetFeatures DatabaseFeatureSupport, paradigmCompatibility ParadigmCompatibility) ConversionComplexity {
	switch paradigmCompatibility {
	case ParadigmCompatibilityIdentical:
		// Same paradigm - complexity depends on feature differences
		unsupportedCount := cu.countUnsupportedObjects(sourceFeatures, targetFeatures)
		if unsupportedCount == 0 {
			return ConversionComplexityTrivial
		} else if unsupportedCount <= 3 {
			return ConversionComplexitySimple
		} else {
			return ConversionComplexityModerate
		}
	case ParadigmCompatibilityCompatible:
		return ConversionComplexitySimple
	case ParadigmCompatibilityPartial:
		return ConversionComplexityModerate
	case ParadigmCompatibilityIncompatible:
		return ConversionComplexityComplex
	default:
		return ConversionComplexityComplex
	}
}

// generateObjectConversions creates conversion rules for all supported objects
func (cu *ConversionUtils) generateObjectConversions(sourceFeatures, targetFeatures DatabaseFeatureSupport) map[ObjectType]ObjectConversionRule {
	conversions := make(map[ObjectType]ObjectConversionRule)

	for objType, sourceSupport := range sourceFeatures.SupportedObjects {
		if !sourceSupport.Supported {
			continue // Skip unsupported source objects
		}

		if targetSupport, exists := targetFeatures.SupportedObjects[objType]; exists {
			if targetSupport.Supported {
				// Direct conversion possible
				conversions[objType] = DirectConversion(objType, objType)
			} else {
				// Target doesn't support this object - suggest alternatives
				if len(targetSupport.Alternatives) > 0 {
					conversions[objType] = EmulatedConversion(objType, targetSupport.Alternatives, targetSupport.Notes)
				} else {
					conversions[objType] = DroppedConversion(objType, targetSupport.Notes)
				}
			}
		} else {
			// Object type not defined in target - drop it
			conversions[objType] = DroppedConversion(objType, "Object type not supported in target database")
		}
	}

	return conversions
}

// countUnsupportedObjects counts how many source objects are unsupported in target
func (cu *ConversionUtils) countUnsupportedObjects(sourceFeatures, targetFeatures DatabaseFeatureSupport) int {
	count := 0
	for objType, sourceSupport := range sourceFeatures.SupportedObjects {
		if !sourceSupport.Supported {
			continue
		}
		if targetSupport, exists := targetFeatures.SupportedObjects[objType]; exists {
			if !targetSupport.Supported {
				count++
			}
		} else {
			count++
		}
	}
	return count
}

// getConversionStrategies determines appropriate strategies based on paradigms using dynamic generation
func (cu *ConversionUtils) getConversionStrategies(sourceParadigms, targetParadigms []dbcapabilities.DataParadigm) []ConversionStrategy {
	// Handle single paradigm conversions
	if len(sourceParadigms) == 1 && len(targetParadigms) == 1 {
		return cu.getSingleParadigmStrategy(sourceParadigms[0], targetParadigms[0])
	}

	// Handle multi-paradigm databases
	strategies := []ConversionStrategy{}
	for _, sp := range sourceParadigms {
		for _, tp := range targetParadigms {
			if sp == tp {
				// Same paradigm - direct conversion
				strategies = append(strategies, StrategyDirect)
			} else {
				// Different paradigms - get conversion strategy
				strategy := cu.getSingleParadigmStrategy(sp, tp)
				strategies = append(strategies, strategy...)
			}
		}
	}

	// Remove duplicates and return
	return cu.removeDuplicateStrategies(strategies)
}

// getSingleParadigmStrategy determines strategy for converting between two specific paradigms
func (cu *ConversionUtils) getSingleParadigmStrategy(source, target dbcapabilities.DataParadigm) []ConversionStrategy {
	// Same paradigm
	if source == target {
		return []ConversionStrategy{StrategyDirect}
	}

	// Relational conversions
	switch source {
	case dbcapabilities.ParadigmRelational:
		switch target {
		case dbcapabilities.ParadigmDocument:
			return []ConversionStrategy{StrategyDenormalization}
		case dbcapabilities.ParadigmGraph:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmKeyValue:
			return []ConversionStrategy{StrategyDenormalization}
		case dbcapabilities.ParadigmColumnar:
			return []ConversionStrategy{StrategyDenormalization}
		case dbcapabilities.ParadigmWideColumn:
			return []ConversionStrategy{StrategyDenormalization}
		case dbcapabilities.ParadigmSearchIndex:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmVector:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmTimeSeries:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmObjectStore:
			return []ConversionStrategy{StrategyAggregation}
		}

	case dbcapabilities.ParadigmDocument:
		switch target {
		case dbcapabilities.ParadigmRelational:
			return []ConversionStrategy{StrategyNormalization}
		case dbcapabilities.ParadigmGraph:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmKeyValue:
			return []ConversionStrategy{StrategyDirect}
		case dbcapabilities.ParadigmColumnar:
			return []ConversionStrategy{StrategyNormalization}
		case dbcapabilities.ParadigmWideColumn:
			return []ConversionStrategy{StrategyDenormalization}
		case dbcapabilities.ParadigmSearchIndex:
			return []ConversionStrategy{StrategyDirect}
		case dbcapabilities.ParadigmVector:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmTimeSeries:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmObjectStore:
			return []ConversionStrategy{StrategyAggregation}
		}

	case dbcapabilities.ParadigmGraph:
		switch target {
		case dbcapabilities.ParadigmRelational:
			return []ConversionStrategy{StrategyAggregation}
		case dbcapabilities.ParadigmDocument:
			return []ConversionStrategy{StrategyAggregation}
		case dbcapabilities.ParadigmKeyValue:
			return []ConversionStrategy{StrategyAggregation}
		case dbcapabilities.ParadigmColumnar:
			return []ConversionStrategy{StrategyAggregation}
		case dbcapabilities.ParadigmWideColumn:
			return []ConversionStrategy{StrategyAggregation}
		case dbcapabilities.ParadigmSearchIndex:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmVector:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmTimeSeries:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmObjectStore:
			return []ConversionStrategy{StrategyAggregation}
		}

	case dbcapabilities.ParadigmKeyValue:
		switch target {
		case dbcapabilities.ParadigmRelational:
			return []ConversionStrategy{StrategyNormalization}
		case dbcapabilities.ParadigmDocument:
			return []ConversionStrategy{StrategyDirect}
		case dbcapabilities.ParadigmGraph:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmColumnar:
			return []ConversionStrategy{StrategyNormalization}
		case dbcapabilities.ParadigmWideColumn:
			return []ConversionStrategy{StrategyDirect}
		case dbcapabilities.ParadigmSearchIndex:
			return []ConversionStrategy{StrategyDirect}
		case dbcapabilities.ParadigmVector:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmTimeSeries:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmObjectStore:
			return []ConversionStrategy{StrategyAggregation}
		}

	case dbcapabilities.ParadigmColumnar:
		switch target {
		case dbcapabilities.ParadigmRelational:
			return []ConversionStrategy{StrategyNormalization}
		case dbcapabilities.ParadigmDocument:
			return []ConversionStrategy{StrategyDenormalization}
		case dbcapabilities.ParadigmGraph:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmKeyValue:
			return []ConversionStrategy{StrategyDenormalization}
		case dbcapabilities.ParadigmWideColumn:
			return []ConversionStrategy{StrategyDirect}
		case dbcapabilities.ParadigmSearchIndex:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmVector:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmTimeSeries:
			return []ConversionStrategy{StrategyDirect}
		case dbcapabilities.ParadigmObjectStore:
			return []ConversionStrategy{StrategyAggregation}
		}

	case dbcapabilities.ParadigmWideColumn:
		switch target {
		case dbcapabilities.ParadigmRelational:
			return []ConversionStrategy{StrategyNormalization}
		case dbcapabilities.ParadigmDocument:
			return []ConversionStrategy{StrategyAggregation}
		case dbcapabilities.ParadigmGraph:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmKeyValue:
			return []ConversionStrategy{StrategyDirect}
		case dbcapabilities.ParadigmColumnar:
			return []ConversionStrategy{StrategyDirect}
		case dbcapabilities.ParadigmSearchIndex:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmVector:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmTimeSeries:
			return []ConversionStrategy{StrategyDirect}
		case dbcapabilities.ParadigmObjectStore:
			return []ConversionStrategy{StrategyAggregation}
		}

	case dbcapabilities.ParadigmSearchIndex:
		switch target {
		case dbcapabilities.ParadigmRelational:
			return []ConversionStrategy{StrategyNormalization}
		case dbcapabilities.ParadigmDocument:
			return []ConversionStrategy{StrategyDirect}
		case dbcapabilities.ParadigmGraph:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmKeyValue:
			return []ConversionStrategy{StrategyDirect}
		case dbcapabilities.ParadigmColumnar:
			return []ConversionStrategy{StrategyNormalization}
		case dbcapabilities.ParadigmWideColumn:
			return []ConversionStrategy{StrategyDenormalization}
		case dbcapabilities.ParadigmVector:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmTimeSeries:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmObjectStore:
			return []ConversionStrategy{StrategyAggregation}
		}

	case dbcapabilities.ParadigmVector:
		switch target {
		case dbcapabilities.ParadigmRelational:
			return []ConversionStrategy{StrategyAggregation}
		case dbcapabilities.ParadigmDocument:
			return []ConversionStrategy{StrategyAggregation}
		case dbcapabilities.ParadigmGraph:
			return []ConversionStrategy{StrategyAggregation}
		case dbcapabilities.ParadigmKeyValue:
			return []ConversionStrategy{StrategyAggregation}
		case dbcapabilities.ParadigmColumnar:
			return []ConversionStrategy{StrategyAggregation}
		case dbcapabilities.ParadigmWideColumn:
			return []ConversionStrategy{StrategyAggregation}
		case dbcapabilities.ParadigmSearchIndex:
			return []ConversionStrategy{StrategyDirect}
		case dbcapabilities.ParadigmTimeSeries:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmObjectStore:
			return []ConversionStrategy{StrategyAggregation}
		}

	case dbcapabilities.ParadigmTimeSeries:
		switch target {
		case dbcapabilities.ParadigmRelational:
			return []ConversionStrategy{StrategyNormalization}
		case dbcapabilities.ParadigmDocument:
			return []ConversionStrategy{StrategyAggregation}
		case dbcapabilities.ParadigmGraph:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmKeyValue:
			return []ConversionStrategy{StrategyAggregation}
		case dbcapabilities.ParadigmColumnar:
			return []ConversionStrategy{StrategyDirect}
		case dbcapabilities.ParadigmWideColumn:
			return []ConversionStrategy{StrategyDirect}
		case dbcapabilities.ParadigmSearchIndex:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmVector:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmObjectStore:
			return []ConversionStrategy{StrategyAggregation}
		}

	case dbcapabilities.ParadigmObjectStore:
		switch target {
		case dbcapabilities.ParadigmRelational:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmDocument:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmGraph:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmKeyValue:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmColumnar:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmWideColumn:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmSearchIndex:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmVector:
			return []ConversionStrategy{StrategyDecomposition}
		case dbcapabilities.ParadigmTimeSeries:
			return []ConversionStrategy{StrategyDecomposition}
		}
	}

	// Default fallback
	return []ConversionStrategy{StrategyHybrid}
}

// removeDuplicateStrategies removes duplicate strategies from a slice
func (cu *ConversionUtils) removeDuplicateStrategies(strategies []ConversionStrategy) []ConversionStrategy {
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

// estimateSuccessRate provides a realistic success rate estimate
func (cu *ConversionUtils) estimateSuccessRate(complexity ConversionComplexity, paradigmCompatibility ParadigmCompatibility) float64 {
	baseRate := 0.95

	// Adjust for complexity
	switch complexity {
	case ConversionComplexityTrivial:
		baseRate = 0.99
	case ConversionComplexitySimple:
		baseRate = 0.95
	case ConversionComplexityModerate:
		baseRate = 0.85
	case ConversionComplexityComplex:
		baseRate = 0.70
	case ConversionComplexityImpossible:
		baseRate = 0.10
	}

	// Adjust for paradigm compatibility
	switch paradigmCompatibility {
	case ParadigmCompatibilityIdentical:
		// No adjustment
	case ParadigmCompatibilityCompatible:
		baseRate *= 0.95
	case ParadigmCompatibilityPartial:
		baseRate *= 0.80
	case ParadigmCompatibilityIncompatible:
		baseRate *= 0.60
	}

	return baseRate
}

// estimateDuration provides time estimates based on complexity
func (cu *ConversionUtils) estimateDuration(complexity ConversionComplexity, objectCount int) string {
	switch complexity {
	case ConversionComplexityTrivial:
		return "seconds"
	case ConversionComplexitySimple:
		return "minutes"
	case ConversionComplexityModerate:
		if objectCount > 50 {
			return "hours"
		}
		return "minutes"
	case ConversionComplexityComplex:
		return "hours to days"
	case ConversionComplexityImpossible:
		return "not feasible"
	default:
		return "unknown"
	}
}
