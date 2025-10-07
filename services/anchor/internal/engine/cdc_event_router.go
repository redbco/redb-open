package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/logger"
)

// CDCEventRouter handles database-agnostic routing of CDC events from source to target.
// It orchestrates the flow: source event -> parsing -> transformation -> target application.
type CDCEventRouter struct {
	sourceAdapter  adapter.Connection
	targetAdapter  adapter.Connection
	transformRules []adapter.TransformationRule
	logger         *logger.Logger
	stats          *adapter.CDCStatistics
}

// NewCDCEventRouter creates a new CDC event router.
func NewCDCEventRouter(
	sourceAdapter adapter.Connection,
	targetAdapter adapter.Connection,
	mappingRulesJSON []byte,
	logger *logger.Logger,
) (*CDCEventRouter, error) {
	router := &CDCEventRouter{
		sourceAdapter: sourceAdapter,
		targetAdapter: targetAdapter,
		logger:        logger,
		stats:         adapter.NewCDCStatistics(),
	}

	// Parse mapping rules if provided
	if len(mappingRulesJSON) > 0 {
		if err := router.parseMappingRules(mappingRulesJSON); err != nil {
			return nil, fmt.Errorf("failed to parse mapping rules: %v", err)
		}
	}

	return router, nil
}

// RouteEvent processes a CDC event from source format to target application.
// This is the main entry point for CDC event processing.
func (r *CDCEventRouter) RouteEvent(ctx context.Context, rawEvent map[string]interface{}) error {
	startTime := time.Now()

	// Step 1: Parse raw event to standardized CDCEvent using source adapter
	event, err := r.sourceAdapter.ReplicationOperations().ParseEvent(ctx, rawEvent)
	if err != nil {
		r.stats.RecordFailure()
		if r.logger != nil {
			r.logger.Error("Failed to parse CDC event: %v", err)
		}
		return fmt.Errorf("parse event failed: %w", err)
	}

	// Step 2: Apply transformations if rules are configured
	if len(r.transformRules) > 0 {
		transformedData, err := r.applyTransformations(ctx, event.Data)
		if err != nil {
			r.stats.RecordFailure()
			if r.logger != nil {
				r.logger.Error("Failed to apply transformations: %v", err)
			}
			return fmt.Errorf("transformation failed: %w", err)
		}
		event.Data = transformedData

		// Also transform old data if present (for UPDATE/DELETE)
		if len(event.OldData) > 0 {
			transformedOldData, err := r.applyTransformations(ctx, event.OldData)
			if err != nil {
				// Log warning but don't fail - old data transformation is less critical
				if r.logger != nil {
					r.logger.Warn("Failed to transform old_data: %v", err)
				}
			} else {
				event.OldData = transformedOldData
			}
		}
	}

	// Step 3: Map table name if specified in transformation rules
	if targetTable := r.getTargetTableName(event.TableName); targetTable != "" {
		event.TableName = targetTable
	}

	// Step 4: Apply event to target database using target adapter
	if err := r.targetAdapter.ReplicationOperations().ApplyCDCEvent(ctx, event); err != nil {
		r.stats.RecordFailure()
		if r.logger != nil {
			r.logger.Error("Failed to apply CDC event to target: %v", err)
		}
		return fmt.Errorf("apply event failed: %w", err)
	}

	// Step 5: Record successful event processing
	latency := time.Since(startTime)
	r.stats.RecordEvent(event, latency)

	if r.logger != nil {
		r.logger.Debug("Successfully processed CDC event: %s on %s (latency: %v)",
			event.Operation, event.TableName, latency)
	}

	return nil
}

// CreateEventHandler creates a function that can be used as an event callback.
// This wraps RouteEvent in a function signature compatible with replication sources.
func (r *CDCEventRouter) CreateEventHandler() func(map[string]interface{}) error {
	return func(rawEvent map[string]interface{}) error {
		// Use a background context for CDC operations - they run indefinitely
		// and should not be tied to any specific RPC request context
		ctx := context.Background()
		return r.RouteEvent(ctx, rawEvent)
	}
}

// applyTransformations applies transformation rules to event data.
func (r *CDCEventRouter) applyTransformations(ctx context.Context, data map[string]interface{}) (map[string]interface{}, error) {
	if len(r.transformRules) == 0 {
		return data, nil
	}

	// Use target adapter's transform capabilities
	// This allows database-specific transformation optimizations
	return r.targetAdapter.ReplicationOperations().TransformData(ctx, data, r.transformRules)
}

// getTargetTableName returns the target table name from transformation rules.
func (r *CDCEventRouter) getTargetTableName(sourceTable string) string {
	for _, rule := range r.transformRules {
		if rule.SourceTable == sourceTable && rule.TargetTable != "" {
			return rule.TargetTable
		}
	}
	return "" // No mapping found - use source table name
}

// parseMappingRules parses JSON mapping rules into TransformationRules.
func (r *CDCEventRouter) parseMappingRules(mappingRulesJSON []byte) error {
	// Try parsing as array of transformation rules
	var rules []map[string]interface{}
	if err := json.Unmarshal(mappingRulesJSON, &rules); err != nil {
		return err
	}

	r.transformRules = make([]adapter.TransformationRule, 0, len(rules))

	for _, ruleMap := range rules {
		rule := adapter.TransformationRule{}

		// Extract source column
		if sourceCol, ok := ruleMap["source_column"].(string); ok {
			rule.SourceColumn = sourceCol
		}

		// Extract target column
		if targetCol, ok := ruleMap["target_column"].(string); ok {
			rule.TargetColumn = targetCol
		}

		// Extract transformation type (default to "direct")
		if transformType, ok := ruleMap["transformation_type"].(string); ok {
			rule.TransformationType = transformType
		} else {
			rule.TransformationType = adapter.TransformDirect
		}

		// Extract source and target tables (optional)
		if sourceTable, ok := ruleMap["source_table"].(string); ok {
			rule.SourceTable = sourceTable
		}
		if targetTable, ok := ruleMap["target_table"].(string); ok {
			rule.TargetTable = targetTable
		}

		// Extract transformation parameters (optional)
		if params, ok := ruleMap["parameters"].(map[string]interface{}); ok {
			rule.Parameters = params
		}

		// Only add rule if it has at least source and target columns
		if rule.SourceColumn != "" && rule.TargetColumn != "" {
			r.transformRules = append(r.transformRules, rule)
		}
	}

	return nil
}

// GetStatistics returns the current CDC statistics.
func (r *CDCEventRouter) GetStatistics() *adapter.CDCStatistics {
	return r.stats
}

// Reset resets the router statistics.
func (r *CDCEventRouter) Reset() {
	r.stats = adapter.NewCDCStatistics()
}
