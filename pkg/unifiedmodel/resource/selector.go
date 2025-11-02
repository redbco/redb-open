package resource

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// JSONPathEvaluator provides JSON path evaluation capabilities
type JSONPathEvaluator struct {
	compiled *compiledJSONPath
}

// compiledJSONPath represents a parsed JSONPath expression
type compiledJSONPath struct {
	expression string
	segments   []jsonPathSegment
}

// jsonPathSegment represents one part of a JSONPath
type jsonPathSegment struct {
	typ        jsonPathSegmentType
	key        string
	index      int
	isWildcard bool
	filter     string
}

type jsonPathSegmentType int

const (
	jsonPathRoot jsonPathSegmentType = iota
	jsonPathChild
	jsonPathArrayIndex
	jsonPathArrayAll
	jsonPathFilter
)

// CompileJSONPath compiles a JSONPath expression for efficient evaluation
func CompileJSONPath(expression string) (*JSONPathEvaluator, error) {
	if expression == "" {
		return nil, fmt.Errorf("empty JSONPath expression")
	}

	// Parse the JSONPath expression
	compiled, err := parseJSONPath(expression)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSONPath: %w", err)
	}

	return &JSONPathEvaluator{
		compiled: compiled,
	}, nil
}

// parseJSONPath parses a JSONPath expression into segments
func parseJSONPath(expr string) (*compiledJSONPath, error) {
	compiled := &compiledJSONPath{
		expression: expr,
		segments:   []jsonPathSegment{},
	}

	// Must start with $ for root
	if !strings.HasPrefix(expr, "$") {
		return nil, fmt.Errorf("JSONPath must start with $")
	}

	// Remove the $ prefix
	expr = expr[1:]

	// If empty after removing $, it's just the root
	if expr == "" {
		compiled.segments = append(compiled.segments, jsonPathSegment{typ: jsonPathRoot})
		return compiled, nil
	}

	// Parse the rest of the path
	parts := strings.Split(expr, ".")
	for _, part := range parts {
		if part == "" {
			continue
		}

		// Check for array notation: field[index] or field[*]
		if strings.Contains(part, "[") {
			seg, err := parseArrayNotation(part)
			if err != nil {
				return nil, err
			}
			compiled.segments = append(compiled.segments, seg...)
		} else {
			// Simple field access
			compiled.segments = append(compiled.segments, jsonPathSegment{
				typ: jsonPathChild,
				key: part,
			})
		}
	}

	return compiled, nil
}

// parseArrayNotation parses array access like field[0] or field[*]
func parseArrayNotation(part string) ([]jsonPathSegment, error) {
	// Extract field name and array part
	bracketIdx := strings.Index(part, "[")
	fieldName := part[:bracketIdx]
	arrayPart := part[bracketIdx:]

	segments := []jsonPathSegment{}

	// Add field segment if present
	if fieldName != "" {
		segments = append(segments, jsonPathSegment{
			typ: jsonPathChild,
			key: fieldName,
		})
	}

	// Parse array access
	re := regexp.MustCompile(`\[([^\]]+)\]`)
	matches := re.FindAllStringSubmatch(arrayPart, -1)

	for _, match := range matches {
		if len(match) < 2 {
			continue
		}

		indexStr := match[1]
		if indexStr == "*" {
			// Wildcard - all elements
			segments = append(segments, jsonPathSegment{
				typ:        jsonPathArrayAll,
				isWildcard: true,
			})
		} else if strings.HasPrefix(indexStr, "?") {
			// Filter expression
			segments = append(segments, jsonPathSegment{
				typ:    jsonPathFilter,
				filter: indexStr,
			})
		} else {
			// Numeric index
			index, err := strconv.Atoi(indexStr)
			if err != nil {
				return nil, fmt.Errorf("invalid array index: %s", indexStr)
			}
			segments = append(segments, jsonPathSegment{
				typ:   jsonPathArrayIndex,
				index: index,
			})
		}
	}

	return segments, nil
}

// Evaluate evaluates the JSONPath against JSON data
func (e *JSONPathEvaluator) Evaluate(data []byte) (interface{}, error) {
	if e.compiled == nil {
		return nil, fmt.Errorf("JSONPath not compiled")
	}

	// Parse JSON data
	var jsonData interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Evaluate path segments
	result := jsonData
	for _, seg := range e.compiled.segments {
		var err error
		result, err = e.evaluateSegment(result, seg)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// EvaluateString is a convenience method that returns the result as a string
func (e *JSONPathEvaluator) EvaluateString(data []byte) (string, error) {
	result, err := e.Evaluate(data)
	if err != nil {
		return "", err
	}

	// Convert result to string
	switch v := result.(type) {
	case string:
		return v, nil
	case nil:
		return "", nil
	default:
		// Marshal back to JSON string
		bytes, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("failed to marshal result: %w", err)
		}
		return string(bytes), nil
	}
}

// evaluateSegment evaluates a single path segment
func (e *JSONPathEvaluator) evaluateSegment(data interface{}, seg jsonPathSegment) (interface{}, error) {
	switch seg.typ {
	case jsonPathRoot:
		return data, nil

	case jsonPathChild:
		// Access object field
		obj, ok := data.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("cannot access field '%s' on non-object", seg.key)
		}
		value, exists := obj[seg.key]
		if !exists {
			return nil, nil // Field doesn't exist, return nil
		}
		return value, nil

	case jsonPathArrayIndex:
		// Access array element by index
		arr, ok := data.([]interface{})
		if !ok {
			return nil, fmt.Errorf("cannot access array index on non-array")
		}
		if seg.index < 0 || seg.index >= len(arr) {
			return nil, nil // Index out of bounds, return nil
		}
		return arr[seg.index], nil

	case jsonPathArrayAll:
		// Return all array elements
		arr, ok := data.([]interface{})
		if !ok {
			return nil, fmt.Errorf("cannot access array elements on non-array")
		}
		return arr, nil

	case jsonPathFilter:
		// Filter array elements (simplified - just return all for now)
		arr, ok := data.([]interface{})
		if !ok {
			return nil, fmt.Errorf("cannot filter non-array")
		}
		// TODO: Implement actual filter logic
		return arr, nil

	default:
		return nil, fmt.Errorf("unknown segment type")
	}
}

// Expression returns the original JSONPath expression
func (e *JSONPathEvaluator) Expression() string {
	if e.compiled == nil {
		return ""
	}
	return e.compiled.expression
}

// XPathEvaluator provides XPath evaluation capabilities for XML data
type XPathEvaluator struct {
	expression string
}

// CompileXPath compiles an XPath expression (basic implementation)
func CompileXPath(expression string) (*XPathEvaluator, error) {
	if expression == "" {
		return nil, fmt.Errorf("empty XPath expression")
	}

	// Basic validation - XPath should start with / or //
	if !strings.HasPrefix(expression, "/") {
		return nil, fmt.Errorf("XPath must start with / or //")
	}

	return &XPathEvaluator{
		expression: expression,
	}, nil
}

// Evaluate evaluates the XPath against XML data (stub implementation)
func (x *XPathEvaluator) Evaluate(data []byte) (interface{}, error) {
	// TODO: Implement actual XPath evaluation using an XML library
	// This is a placeholder that would need integration with an XML/XPath library
	return nil, fmt.Errorf("XPath evaluation not yet implemented - requires XML library integration")
}

// Expression returns the XPath expression
func (x *XPathEvaluator) Expression() string {
	return x.expression
}

// CompileSelector compiles a selector based on its type
func CompileSelector(selector *Selector) error {
	if selector == nil {
		return fmt.Errorf("selector is nil")
	}

	if selector.Compiled != nil {
		// Already compiled
		return nil
	}

	switch selector.Type {
	case SelectorJSONPath:
		evaluator, err := CompileJSONPath(selector.Expression)
		if err != nil {
			return fmt.Errorf("failed to compile JSONPath: %w", err)
		}
		selector.Compiled = evaluator

	case SelectorXPath:
		evaluator, err := CompileXPath(selector.Expression)
		if err != nil {
			return fmt.Errorf("failed to compile XPath: %w", err)
		}
		selector.Compiled = evaluator

	case SelectorIndex:
		// Parse index
		index, err := strconv.Atoi(selector.Expression)
		if err != nil {
			return fmt.Errorf("invalid index: %s", selector.Expression)
		}
		selector.Compiled = index

	case SelectorKey:
		// No compilation needed for simple keys
		selector.Compiled = selector.Expression

	case SelectorWildcard:
		// No compilation needed for wildcard
		selector.Compiled = "*"

	case SelectorRegex:
		// Compile regex
		re, err := regexp.Compile(selector.Expression)
		if err != nil {
			return fmt.Errorf("failed to compile regex: %w", err)
		}
		selector.Compiled = re

	default:
		return fmt.Errorf("unsupported selector type: %s", selector.Type)
	}

	return nil
}

// EvaluateSelector evaluates a compiled selector against data
func EvaluateSelector(selector *Selector, data []byte) (interface{}, error) {
	if selector == nil {
		return nil, fmt.Errorf("selector is nil")
	}

	// Ensure selector is compiled
	if selector.Compiled == nil {
		if err := CompileSelector(selector); err != nil {
			return nil, err
		}
	}

	switch selector.Type {
	case SelectorJSONPath:
		evaluator, ok := selector.Compiled.(*JSONPathEvaluator)
		if !ok {
			return nil, fmt.Errorf("invalid compiled JSONPath evaluator")
		}
		return evaluator.Evaluate(data)

	case SelectorXPath:
		evaluator, ok := selector.Compiled.(*XPathEvaluator)
		if !ok {
			return nil, fmt.Errorf("invalid compiled XPath evaluator")
		}
		return evaluator.Evaluate(data)

	case SelectorIndex:
		index, ok := selector.Compiled.(int)
		if !ok {
			return nil, fmt.Errorf("invalid compiled index")
		}
		// Parse as JSON array
		var arr []interface{}
		if err := json.Unmarshal(data, &arr); err != nil {
			return nil, fmt.Errorf("data is not a JSON array: %w", err)
		}
		if index < 0 || index >= len(arr) {
			return nil, fmt.Errorf("index %d out of bounds", index)
		}
		return arr[index], nil

	case SelectorKey:
		key, ok := selector.Compiled.(string)
		if !ok {
			return nil, fmt.Errorf("invalid compiled key")
		}
		// Parse as JSON object
		var obj map[string]interface{}
		if err := json.Unmarshal(data, &obj); err != nil {
			return nil, fmt.Errorf("data is not a JSON object: %w", err)
		}
		return obj[key], nil

	case SelectorWildcard:
		// Return all data
		var result interface{}
		if err := json.Unmarshal(data, &result); err != nil {
			return nil, fmt.Errorf("failed to parse JSON: %w", err)
		}
		return result, nil

	default:
		return nil, fmt.Errorf("unsupported selector type: %s", selector.Type)
	}
}
