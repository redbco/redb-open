package engine

import (
	"fmt"
)

// inferCardinality infers the cardinality type based on source and target item counts
func inferCardinality(sourceCount, targetCount int) string {
	// Generator: no source, one or more targets
	if sourceCount == 0 && targetCount > 0 {
		return "generator"
	}

	// Sink: one or more sources, no target
	if sourceCount > 0 && targetCount == 0 {
		return "sink"
	}

	// One-to-one: single source, single target
	if sourceCount == 1 && targetCount == 1 {
		return "one-to-one"
	}

	// One-to-many: single source, multiple targets
	if sourceCount == 1 && targetCount > 1 {
		return "one-to-many"
	}

	// Many-to-one: multiple sources, single target
	if sourceCount > 1 && targetCount == 1 {
		return "many-to-one"
	}

	// Many-to-many: multiple sources, multiple targets
	if sourceCount > 1 && targetCount > 1 {
		return "many-to-many"
	}

	// Edge case: no sources and no targets
	return "invalid"
}

// validateCardinality validates that the specified cardinality matches the actual item counts
func validateCardinality(cardinality string, sourceCount, targetCount int) error {
	switch cardinality {
	case "one-to-one":
		if sourceCount != 1 || targetCount != 1 {
			return fmt.Errorf("one-to-one cardinality requires exactly 1 source and 1 target, got %d source(s) and %d target(s)", sourceCount, targetCount)
		}
	case "one-to-many":
		if sourceCount != 1 || targetCount < 2 {
			return fmt.Errorf("one-to-many cardinality requires exactly 1 source and 2+ targets, got %d source(s) and %d target(s)", sourceCount, targetCount)
		}
	case "many-to-one":
		if sourceCount < 2 || targetCount != 1 {
			return fmt.Errorf("many-to-one cardinality requires 2+ sources and exactly 1 target, got %d source(s) and %d target(s)", sourceCount, targetCount)
		}
	case "many-to-many":
		if sourceCount < 2 || targetCount < 2 {
			return fmt.Errorf("many-to-many cardinality requires 2+ sources and 2+ targets, got %d source(s) and %d target(s)", sourceCount, targetCount)
		}
	case "generator":
		if sourceCount != 0 || targetCount < 1 {
			return fmt.Errorf("generator cardinality requires 0 sources and 1+ targets, got %d source(s) and %d target(s)", sourceCount, targetCount)
		}
	case "sink":
		if sourceCount < 1 || targetCount != 0 {
			return fmt.Errorf("sink cardinality requires 1+ sources and 0 targets, got %d source(s) and %d target(s)", sourceCount, targetCount)
		}
	default:
		return fmt.Errorf("unknown cardinality type: %s", cardinality)
	}
	return nil
}

// validateTransformationCardinality validates that a transformation supports the specified cardinality
func validateTransformationCardinality(transformationType, ruleCardinality string) error {
	// Define which cardinalities each transformation type supports
	supportedCardinalities := map[string][]string{
		"generator":   {"generator"},
		"sink":        {"sink"},
		"passthrough": {"one-to-one", "one-to-many"},
		"aggregation": {"many-to-one"},
		"merge":       {"many-to-one"},
		"split":       {"one-to-many"},
		"fanout":      {"one-to-many", "many-to-many"},
	}

	// Get supported cardinalities for this transformation type
	supported, exists := supportedCardinalities[transformationType]
	if !exists {
		// If transformation type is not in our map, allow any cardinality
		// This is for custom or future transformation types
		return nil
	}

	// Check if the rule cardinality is supported
	for _, c := range supported {
		if c == ruleCardinality {
			return nil
		}
	}

	return fmt.Errorf("transformation type '%s' does not support cardinality '%s' (supported: %v)",
		transformationType, ruleCardinality, supported)
}

// validateFilterExpression validates the structure of a filter expression
func validateFilterExpression(filterType string, expression map[string]interface{}) error {
	if len(expression) == 0 {
		return fmt.Errorf("filter expression cannot be empty")
	}

	switch filterType {
	case "where":
		// WHERE filters should have: field, operator, value
		if _, ok := expression["field"]; !ok {
			return fmt.Errorf("WHERE filter must specify 'field'")
		}
		if _, ok := expression["operator"]; !ok {
			return fmt.Errorf("WHERE filter must specify 'operator'")
		}
		// Value can be optional for operators like IS NULL, IS NOT NULL

	case "limit":
		// LIMIT filters should have: count
		if _, ok := expression["count"]; !ok {
			return fmt.Errorf("LIMIT filter must specify 'count'")
		}

	case "order_by":
		// ORDER BY filters should have: field, direction
		if _, ok := expression["field"]; !ok {
			return fmt.Errorf("ORDER_BY filter must specify 'field'")
		}
		if _, ok := expression["direction"]; !ok {
			return fmt.Errorf("ORDER_BY filter must specify 'direction'")
		}

	case "custom":
		// Custom filters can have any structure
		// Just ensure it's not empty

	default:
		return fmt.Errorf("unknown filter type: %s", filterType)
	}

	return nil
}

// validateFilterOperator validates the operator used to combine multiple filters
func validateFilterOperator(operator string) error {
	validOperators := []string{"AND", "OR"}

	for _, op := range validOperators {
		if operator == op {
			return nil
		}
	}

	return fmt.Errorf("invalid filter operator '%s': must be one of %v", operator, validOperators)
}
