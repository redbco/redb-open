package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	transformationv1 "github.com/redbco/redb-open/api/proto/transformation/v1"
	unifiedmodelv1 "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/pkg/unifiedmodel/resource"
	"github.com/redbco/redb-open/services/core/internal/services/database"
	"github.com/redbco/redb-open/services/core/internal/services/mapping"
	"github.com/redbco/redb-open/services/core/internal/services/workspace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ============================================================================
// MappingService gRPC handlers
// ============================================================================

func (s *Server) ListMappings(ctx context.Context, req *corev1.ListMappingsRequest) (*corev1.ListMappingsResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// List mappings for the tenant and workspace
	mappings, err := mappingService.List(ctx, req.TenantId, workspaceID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to list mappings: %v", err)
	}

	// Convert to protobuf format
	protoMappings := make([]*corev1.Mapping, len(mappings))
	for i, m := range mappings {
		protoMapping, err := s.mappingToProtoWithContext(ctx, m)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to convert mapping: %v", err)
		}
		protoMappings[i] = protoMapping
	}

	return &corev1.ListMappingsResponse{
		Mappings: protoMappings,
	}, nil
}

func (s *Server) ShowMapping(ctx context.Context, req *corev1.ShowMappingRequest) (*corev1.ShowMappingResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Get the mapping by name
	m, err := mappingService.Get(ctx, req.TenantId, workspaceID, req.MappingName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "mapping not found: %v", err)
	}

	// Get filters for this mapping
	filters, err := mappingService.GetMappingFilters(ctx, m.ID)
	if err != nil {
		s.engine.logger.Warnf("Failed to get filters for mapping %s: %v", req.MappingName, err)
		filters = []*mapping.MappingFilter{}
	}
	m.Filters = filters

	// Get mapping rules for this mapping
	mappingRules, err := mappingService.GetMappingRulesForMapping(ctx, req.TenantId, workspaceID, req.MappingName)
	if err != nil {
		s.engine.logger.Warnf("Failed to get mapping rules for mapping %s: %v", req.MappingName, err)
		mappingRules = []*mapping.Rule{}
	}

	// Load source/target items for each rule
	for _, rule := range mappingRules {
		// Load source items
		sourceItems, err := mappingService.GetRuleSourceItems(ctx, rule.ID)
		if err != nil {
			s.engine.logger.Warnf("Failed to get source items for rule %s: %v", rule.Name, err)
		} else {
			rule.SourceItems = sourceItems
		}

		// Load target items
		targetItems, err := mappingService.GetRuleTargetItems(ctx, rule.ID)
		if err != nil {
			s.engine.logger.Warnf("Failed to get target items for rule %s: %v", rule.Name, err)
		} else {
			rule.TargetItems = targetItems
		}
	}

	// Fetch all container items for source and target containers
	// This allows the dashboard to show which columns are unmapped
	var sourceContainerItems []*mapping.ResourceItem
	var targetContainerItems []*mapping.ResourceItem

	if m.SourceContainerID != nil && *m.SourceContainerID != "" {
		items, err := mappingService.GetContainerItems(ctx, *m.SourceContainerID)
		if err != nil {
			s.engine.logger.Warnf("Failed to get source container items for mapping %s: %v", req.MappingName, err)
		} else {
			sourceContainerItems = items
		}
	}

	if m.TargetContainerID != nil && *m.TargetContainerID != "" {
		items, err := mappingService.GetContainerItems(ctx, *m.TargetContainerID)
		if err != nil {
			s.engine.logger.Warnf("Failed to get target container items for mapping %s: %v", req.MappingName, err)
		} else {
			targetContainerItems = items
		}
	}

	// Add container items to mapping object for serialization
	if len(sourceContainerItems) > 0 || len(targetContainerItems) > 0 {
		if m.MappingObject == nil {
			m.MappingObject = make(map[string]interface{})
		}

		// Convert source container items to JSON-friendly format
		if len(sourceContainerItems) > 0 {
			sourceItemsData := make([]map[string]interface{}, len(sourceContainerItems))
			for i, item := range sourceContainerItems {
				sourceItemsData[i] = map[string]interface{}{
					"item_id":                   item.ItemID,
					"container_id":              item.ContainerID,
					"resource_uri":              item.ResourceURI,
					"item_type":                 item.ItemType,
					"item_name":                 item.ItemName,
					"item_display_name":         item.ItemDisplayName,
					"item_path":                 item.ItemPath,
					"data_type":                 item.DataType,
					"unified_data_type":         item.UnifiedDataType,
					"is_nullable":               item.IsNullable,
					"is_primary_key":            item.IsPrimaryKey,
					"is_unique":                 item.IsUnique,
					"is_indexed":                item.IsIndexed,
					"is_required":               item.IsRequired,
					"is_array":                  item.IsArray,
					"array_dimensions":          item.ArrayDimensions,
					"default_value":             item.DefaultValue,
					"max_length":                item.MaxLength,
					"precision":                 item.Precision,
					"scale":                     item.Scale,
					"description":               item.Description,
					"is_privileged":             item.IsPrivileged,
					"privileged_classification": item.PrivilegedClassification,
					"detection_confidence":      item.DetectionConfidence,
					"detection_method":          item.DetectionMethod,
				}
			}
			m.MappingObject["source_container_items"] = sourceItemsData
		}

		// Convert target container items to JSON-friendly format
		if len(targetContainerItems) > 0 {
			targetItemsData := make([]map[string]interface{}, len(targetContainerItems))
			for i, item := range targetContainerItems {
				targetItemsData[i] = map[string]interface{}{
					"item_id":                   item.ItemID,
					"container_id":              item.ContainerID,
					"resource_uri":              item.ResourceURI,
					"item_type":                 item.ItemType,
					"item_name":                 item.ItemName,
					"item_display_name":         item.ItemDisplayName,
					"item_path":                 item.ItemPath,
					"data_type":                 item.DataType,
					"unified_data_type":         item.UnifiedDataType,
					"is_nullable":               item.IsNullable,
					"is_primary_key":            item.IsPrimaryKey,
					"is_unique":                 item.IsUnique,
					"is_indexed":                item.IsIndexed,
					"is_required":               item.IsRequired,
					"is_array":                  item.IsArray,
					"array_dimensions":          item.ArrayDimensions,
					"default_value":             item.DefaultValue,
					"max_length":                item.MaxLength,
					"precision":                 item.Precision,
					"scale":                     item.Scale,
					"description":               item.Description,
					"is_privileged":             item.IsPrivileged,
					"privileged_classification": item.PrivilegedClassification,
					"detection_confidence":      item.DetectionConfidence,
					"detection_method":          item.DetectionMethod,
				}
			}
			m.MappingObject["target_container_items"] = targetItemsData
		}
	}

	// Convert to protobuf format
	protoMapping, err := s.mappingToProtoWithContext(ctx, m)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert mapping: %v", err)
	}

	// Convert mapping rules to protobuf format
	protoMappingRules := make([]*corev1.MappingRule, len(mappingRules))
	for i, rule := range mappingRules {
		protoRule, err := s.mappingRuleToProtoWithItems(rule)
		if err != nil {
			s.engine.logger.Warnf("Failed to convert mapping rule: %v", err)
			continue
		}

		// Populate cardinality and item URIs from loaded data
		protoRule.MappingRuleCardinality = rule.Cardinality
		protoRule.SourceItemUris = make([]string, len(rule.SourceItems))
		for j, item := range rule.SourceItems {
			protoRule.SourceItemUris[j] = item.ResourceURI
		}
		protoRule.TargetItemUris = make([]string, len(rule.TargetItems))
		for j, item := range rule.TargetItems {
			protoRule.TargetItemUris[j] = item.ResourceURI
		}

		protoMappingRules[i] = protoRule
	}

	protoMapping.MappingRules = protoMappingRules

	return &corev1.ShowMappingResponse{
		Mapping: protoMapping,
	}, nil
}

func (s *Server) AddMapping(ctx context.Context, req *corev1.AddMappingRequest) (*corev1.AddMappingResponse, error) {
	defer s.trackOperation()()

	// Validate scope
	if req.Scope != "database" && req.Scope != "table" {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.InvalidArgument, "invalid scope '%s': must be 'database' or 'table'", req.Scope)
	}

	// Check if target is MCP resource
	isMCPTarget := strings.HasPrefix(req.Target, "mcp://")

	// Parse source
	sourceDB, sourceTable, err := s.parseSourceTarget(req.Source)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.InvalidArgument, "invalid source format: %v", err)
	}

	// If MCP target, handle differently
	if isMCPTarget {
		mcpResourceName := strings.TrimPrefix(req.Target, "mcp://")
		if mcpResourceName == "" {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.InvalidArgument, "invalid MCP target format: expected 'mcp://resource_name'")
		}
		return s.addMCPMapping(ctx, req, sourceDB, sourceTable, mcpResourceName)
	}

	// Parse database target
	targetDB, targetTable, err := s.parseSourceTarget(req.Target)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.InvalidArgument, "invalid target format: %v", err)
	}

	// Validate scope-specific requirements
	if req.Scope == "table" {
		if sourceTable == "" || targetTable == "" {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.InvalidArgument, "table scope requires both source and target to include table names (format: database.table)")
		}
	}

	// Route to appropriate handler based on scope
	switch req.Scope {
	case "database":
		return s.addDatabaseMappingUnified(ctx, req, sourceDB, targetDB, req.GenerateRules)
	case "table":
		return s.addTableMappingUnified(ctx, req, sourceDB, sourceTable, targetDB, targetTable, req.GenerateRules)
	default:
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.InvalidArgument, "unsupported scope: %s", req.Scope)
	}
}

func (s *Server) AddTableMapping(ctx context.Context, req *corev1.AddTableMappingRequest) (*corev1.AddMappingResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get database service to validate and fetch database schemas
	databaseService := database.NewService(s.engine.db, s.engine.logger)

	// Validate source database exists and belongs to the tenant/workspace
	sourceDB, err := databaseService.Get(ctx, req.TenantId, workspaceID, req.MappingSourceDatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "source database not found: %v", err)
	}

	// Validate target database exists and belongs to the tenant/workspace
	targetDB, err := databaseService.Get(ctx, req.TenantId, workspaceID, req.MappingTargetDatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "target database not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Build resource URIs and mapping type
	sourceType := "table"
	targetType := "table"
	sourceIdentifier := s.buildResourceURI("table", sourceDB.ID, req.MappingSourceTableName, "")
	targetIdentifier := s.buildResourceURI("table", targetDB.ID, req.MappingTargetTableName, "")
	mappingType := s.buildMappingType(sourceType, targetType)

	// Build mapping object with human-readable names
	mappingObject := map[string]interface{}{
		"source_database_name": sourceDB.Name,
		"source_database_id":   sourceDB.ID,
		"source_table_name":    req.MappingSourceTableName,
		"target_database_name": targetDB.Name,
		"target_database_id":   targetDB.ID,
		"target_table_name":    req.MappingTargetTableName,
	}

	// Create the mapping
	createdMapping, err := mappingService.Create(ctx, req.TenantId, workspaceID, mappingType, req.MappingName, req.MappingDescription, req.OwnerId,
		sourceType, targetType, sourceIdentifier, targetIdentifier, mappingObject)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create mapping: %v", err)
	}

	// Get unified model client
	umClient := s.engine.GetUnifiedModelClient()
	if umClient == nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "unified model service not available")
	}

	// Convert source database schema to UnifiedModel
	var sourceUM *unifiedmodelv1.UnifiedModel
	var sourceEnrichment *unifiedmodelv1.UnifiedModelEnrichment

	if sourceDB.Schema != "" {
		var err error
		sourceUM, err = s.convertDatabaseSchemaToUnifiedModel(sourceDB.Schema)
		if err != nil {
			s.engine.logger.Warnf("Failed to convert source database schema: %v", err)
		} else {
			// Filter to only include the requested source table
			sourceUM = s.filterUnifiedModelForTable(sourceUM, req.MappingSourceTableName)
			s.engine.logger.Infof("Filtered source schema to include only table: %s", req.MappingSourceTableName)
		}
	}

	// Convert source enrichment data
	if sourceDB.Tables != "" {
		var err error
		sourceEnrichment, err = s.convertEnrichedDataToUnifiedModelEnrichment(sourceDB.Tables, sourceDB.ID)
		if err != nil {
			s.engine.logger.Warnf("Failed to convert source enrichment data: %v", err)
		} else {
			// Filter to only include the requested source table
			sourceEnrichment = s.filterUnifiedModelEnrichmentForTable(sourceEnrichment, req.MappingSourceTableName)
		}
	}

	// Convert target database schema to UnifiedModel
	var targetUM *unifiedmodelv1.UnifiedModel
	var targetEnrichment *unifiedmodelv1.UnifiedModelEnrichment

	if targetDB.Schema != "" {
		var err error
		targetUM, err = s.convertDatabaseSchemaToUnifiedModel(targetDB.Schema)
		if err != nil {
			s.engine.logger.Warnf("Failed to convert target database schema: %v", err)
		} else {
			// Filter to only include the requested target table
			targetUM = s.filterUnifiedModelForTable(targetUM, req.MappingTargetTableName)
			s.engine.logger.Infof("Filtered target schema to include only table: %s", req.MappingTargetTableName)
		}
	}

	// Convert target enrichment data
	if targetDB.Tables != "" {
		var err error
		targetEnrichment, err = s.convertEnrichedDataToUnifiedModelEnrichment(targetDB.Tables, targetDB.ID)
		if err != nil {
			s.engine.logger.Warnf("Failed to convert target enrichment data: %v", err)
		} else {
			// Filter to only include the requested target table
			targetEnrichment = s.filterUnifiedModelEnrichmentForTable(targetEnrichment, req.MappingTargetTableName)
		}
	}

	// Use unified model service to match schemas
	if sourceUM != nil && targetUM != nil {
		matchReq := &unifiedmodelv1.MatchUnifiedModelsEnrichedRequest{
			SourceUnifiedModel: sourceUM,
			SourceEnrichment:   sourceEnrichment,
			TargetUnifiedModel: targetUM,
			TargetEnrichment:   targetEnrichment,
			Options: &unifiedmodelv1.MatchOptions{
				NameSimilarityThreshold:  0.3, // Lower threshold to allow more matches
				PoorMatchThreshold:       0.2,
				NameWeight:               0.4,
				TypeWeight:               0.3,
				ClassificationWeight:     0.2,
				PrivilegedDataWeight:     0.1,
				TableStructureWeight:     0.3,
				EnableCrossTableMatching: false,
			},
		}

		s.engine.logger.Infof("Calling MatchUnifiedModelsEnriched with source table %s and target table %s", req.MappingSourceTableName, req.MappingTargetTableName)

		matchResp, err := umClient.MatchUnifiedModelsEnriched(ctx, matchReq)
		s.engine.logger.Infof("Match response: %v", matchResp)
		if err != nil {
			s.engine.logger.Warnf("Failed to match schemas using unified model service: %v", err)
		} else {
			// Create mapping rules for matched columns
			s.engine.logger.Infof("Creating mapping rules for matched columns: %v", matchResp.TableMatches)
			for _, tableMatch := range matchResp.TableMatches {
				for _, columnMatch := range tableMatch.ColumnMatches {
					if columnMatch.Score >= 0.5 && !columnMatch.IsPoorMatch && !columnMatch.IsUnmatched {
						// Create mapping rule for this column match
						baseRuleName := fmt.Sprintf("%s_%s_to_%s_%s",
							tableMatch.SourceTable, columnMatch.SourceColumn,
							tableMatch.TargetTable, columnMatch.TargetColumn)

						// Find an available rule name by incrementing the number if needed
						ruleName := baseRuleName
						counter := 1
						for {
							existingRule, err := mappingService.GetMappingRuleByName(ctx, req.TenantId, workspaceID, ruleName)
							if err != nil {
								break // Use the current name if we can't check
							}
							if existingRule == nil {
								break // Name is available
							}
							// Name exists, try with incremented counter
							ruleName = fmt.Sprintf("%s_%d", baseRuleName, counter)
							counter++
						}

						// Create metadata based on the match
						metadata := map[string]interface{}{
							"source_table":         tableMatch.SourceTable,
							"source_column":        columnMatch.SourceColumn,
							"source_database_name": sourceDB.Name,
							"source_database_id":   sourceDB.ID,
							"target_table":         tableMatch.TargetTable,
							"target_column":        columnMatch.TargetColumn,
							"target_database_name": targetDB.Name,
							"target_database_id":   targetDB.ID,
							"match_score":          columnMatch.Score,
							"type_compatible":      columnMatch.IsTypeCompatible,
							"match_type":           "auto_generated",
							"generated_at":         time.Now().Format(time.RFC3339),
						}

						// Create empty transformation options (as requested)
						transformationOptions := map[string]interface{}{}

						// Build proper resource URIs
						sourceURI := s.buildResourceURI("column", sourceDB.ID, tableMatch.SourceTable, columnMatch.SourceColumn)
						targetURI := s.buildResourceURI("column", targetDB.ID, tableMatch.TargetTable, columnMatch.TargetColumn)

						// Create the mapping rule
						_, err = mappingService.CreateMappingRule(ctx, req.TenantId, workspaceID, ruleName,
							fmt.Sprintf("Auto-generated rule for %s.%s.%s -> %s.%s.%s",
								req.MappingSourceDatabaseName, tableMatch.SourceTable, columnMatch.SourceColumn,
								req.MappingTargetDatabaseName, tableMatch.TargetTable, columnMatch.TargetColumn),
							sourceURI,
							targetURI,
							"direct_mapping", // Default transformation
							transformationOptions,
							metadata,
							req.OwnerId)

						if err != nil {
							s.engine.logger.Warnf("Failed to create mapping rule %s: %v", ruleName, err)
							continue
						}

						// Attach the mapping rule to the mapping
						err = mappingService.AttachMappingRule(ctx, req.TenantId, workspaceID, req.MappingName, ruleName, nil)
						if err != nil {
							s.engine.logger.Warnf("Failed to attach mapping rule %s to mapping: %v", ruleName, err)
						}
					}
				}
			}
		}
	}

	// Refresh the mapping to get the updated mapping rule count
	updatedMapping, err := mappingService.Get(ctx, req.TenantId, workspaceID, req.MappingName)
	if err != nil {
		s.engine.logger.Warnf("Failed to refresh mapping data: %v", err)
		// Use the original mapping if refresh fails
		updatedMapping = createdMapping
	}

	// Convert to protobuf format
	protoMapping, err := s.mappingToProto(updatedMapping)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert mapping: %v", err)
	}

	return &corev1.AddMappingResponse{
		Message: "Table mapping created successfully",
		Success: true,
		Mapping: protoMapping,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) AddTableMappingWithDeploy(ctx context.Context, req *corev1.AddTableMappingWithDeployRequest) (*corev1.AddTableMappingWithDeployResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get database service
	databaseService := database.NewService(s.engine.db, s.engine.logger)

	// Validate source database exists
	sourceDB, err := databaseService.Get(ctx, req.TenantId, workspaceID, req.SourceDatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "source database not found: %v", err)
	}

	// Validate target database exists
	targetDB, err := databaseService.Get(ctx, req.TenantId, workspaceID, req.TargetDatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "target database not found: %v", err)
	}

	// Get source database schema as UnifiedModel
	if sourceDB.Schema == "" {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.FailedPrecondition, "source database schema not available")
	}

	var sourceSchema unifiedmodel.UnifiedModel
	if err := json.Unmarshal([]byte(sourceDB.Schema), &sourceSchema); err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to parse source database schema: %v", err)
	}

	// Check if source table exists in schema
	_, exists := sourceSchema.Tables[req.SourceTableName]
	if !exists {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "source table '%s' not found in database '%s'", req.SourceTableName, req.SourceDatabaseName)
	}

	// Check if target table already exists via anchor service
	tableExists, err := s.checkTableExists(ctx, targetDB.ID, req.TargetTableName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to check if target table exists: %v", err)
	}
	if tableExists {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.AlreadyExists, "target table '%s' already exists in database '%s'", req.TargetTableName, req.TargetDatabaseName)
	}

	// Extract table with dependent types
	filteredSchema, deployedTypes := s.extractTableWithTypes(&sourceSchema, req.SourceTableName)

	// If target database is a different type, convert schema
	var schemaToDeply *unifiedmodel.UnifiedModel
	if sourceDB.Type != targetDB.Type {
		s.engine.logger.Infof("Converting schema from %s to %s", sourceDB.Type, targetDB.Type)

		schemaJSON, err := json.Marshal(filteredSchema)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to serialize filtered schema: %v", err)
		}

		convertedSchemaStr, _, err := s.convertSchemaViaUnifiedModel(ctx, string(schemaJSON), sourceDB.Type, targetDB.Type)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to convert schema: %v", err)
		}

		var convertedSchema unifiedmodel.UnifiedModel
		if err := json.Unmarshal([]byte(convertedSchemaStr), &convertedSchema); err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to parse converted schema: %v", err)
		}

		schemaToDeply = &convertedSchema
	} else {
		schemaToDeply = filteredSchema
	}

	// Rename table if target name is different
	if req.TargetTableName != req.SourceTableName {
		if table, exists := schemaToDeply.Tables[req.SourceTableName]; exists {
			delete(schemaToDeply.Tables, req.SourceTableName)
			table.Name = req.TargetTableName
			schemaToDeply.Tables[req.TargetTableName] = table
		}
	}

	// Deploy table schema to target database
	deploySchemaJSON, err := json.Marshal(schemaToDeply)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to serialize deployment schema: %v", err)
	}

	err = s.deploySchemaToDatabase(ctx, targetDB.ID, string(deploySchemaJSON), nil)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to deploy table schema: %v", err)
	}

	s.engine.logger.Infof("Successfully deployed table '%s' to database '%s'", req.TargetTableName, req.TargetDatabaseName)

	// Immediately refresh the target database discovery to populate resource_containers and resource_items
	// This ensures the target table's metadata is available for mapping rule generation
	err = s.refreshDatabaseDiscovery(ctx, targetDB.ID)
	if err != nil {
		s.engine.logger.Warnf("Failed to refresh database discovery after deployment: %v (proceeding with mapping creation)", err)
		// Don't fail the entire operation - mapping can still be created, just without auto-generated rules
	} else {
		s.engine.logger.Infof("Successfully refreshed discovery for database '%s' after table deployment", req.TargetDatabaseName)
	}

	// Now create the mapping using the existing AddTableMapping logic
	mappingReq := &corev1.AddTableMappingRequest{
		TenantId:                  req.TenantId,
		WorkspaceName:             req.WorkspaceName,
		OwnerId:                   req.OwnerId,
		MappingName:               req.MappingName,
		MappingDescription:        req.MappingDescription,
		MappingSourceDatabaseName: req.SourceDatabaseName,
		MappingSourceTableName:    req.SourceTableName,
		MappingTargetDatabaseName: req.TargetDatabaseName,
		MappingTargetTableName:    req.TargetTableName,
		PolicyId:                  req.PolicyId,
	}

	mappingResp, err := s.AddTableMapping(ctx, mappingReq)
	if err != nil {
		s.engine.logger.Errorf("Failed to create mapping after successful deployment: %v", err)
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "table deployed successfully but failed to create mapping: %v (table remains in target database)", err)
	}

	return &corev1.AddTableMappingWithDeployResponse{
		Message:       "Table deployed and mapping created successfully",
		Success:       true,
		Status:        string(commonv1.Status_STATUS_SUCCESS),
		Mapping:       mappingResp.Mapping,
		TypesDeployed: deployedTypes,
	}, nil
}

func (s *Server) AddDatabaseMapping(ctx context.Context, req *corev1.AddDatabaseMappingRequest) (*corev1.AddMappingResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get database service to validate and fetch database schemas
	databaseService := database.NewService(s.engine.db, s.engine.logger)

	// Validate source database exists and belongs to the tenant/workspace
	sourceDB, err := databaseService.Get(ctx, req.TenantId, workspaceID, req.MappingSourceDatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "source database not found: %v", err)
	}

	// Validate target database exists and belongs to the tenant/workspace
	targetDB, err := databaseService.Get(ctx, req.TenantId, workspaceID, req.MappingTargetDatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "target database not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Build resource URIs and mapping type
	sourceType := "database"
	targetType := "database"
	sourceIdentifier := s.buildResourceURI("database", sourceDB.ID, "", "")
	targetIdentifier := s.buildResourceURI("database", targetDB.ID, "", "")
	mappingType := s.buildMappingType(sourceType, targetType)

	// Build mapping object with human-readable names
	mappingObject := map[string]interface{}{
		"source_database_name": sourceDB.Name,
		"source_database_id":   sourceDB.ID,
		"target_database_name": targetDB.Name,
		"target_database_id":   targetDB.ID,
	}

	// Create the mapping
	createdMapping, err := mappingService.Create(ctx, req.TenantId, workspaceID, mappingType, req.MappingName, req.MappingDescription, req.OwnerId,
		sourceType, targetType, sourceIdentifier, targetIdentifier, mappingObject)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create mapping: %v", err)
	}

	// Get unified model client
	umClient := s.engine.GetUnifiedModelClient()
	if umClient == nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "unified model service not available")
	}

	// Convert source database schema to UnifiedModel
	var sourceUM *unifiedmodelv1.UnifiedModel
	var sourceEnrichment *unifiedmodelv1.UnifiedModelEnrichment

	if sourceDB.Schema != "" {
		var err error
		sourceUM, err = s.convertDatabaseSchemaToUnifiedModel(sourceDB.Schema)
		if err != nil {
			s.engine.logger.Warnf("Failed to convert source database schema: %v", err)
		} else {
			s.engine.logger.Infof("Converted source database schema with %d tables", len(sourceUM.Tables))
		}
	}

	// Convert source enrichment data
	if sourceDB.Tables != "" {
		var err error
		sourceEnrichment, err = s.convertEnrichedDataToUnifiedModelEnrichment(sourceDB.Tables, sourceDB.ID)
		if err != nil {
			s.engine.logger.Warnf("Failed to convert source enrichment data: %v", err)
		} else {
			s.engine.logger.Infof("Converted source enrichment data with %d table enrichments", len(sourceEnrichment.TableEnrichments))
		}
	}

	// Convert target database schema to UnifiedModel
	var targetUM *unifiedmodelv1.UnifiedModel
	var targetEnrichment *unifiedmodelv1.UnifiedModelEnrichment

	if targetDB.Schema != "" {
		var err error
		targetUM, err = s.convertDatabaseSchemaToUnifiedModel(targetDB.Schema)
		if err != nil {
			s.engine.logger.Warnf("Failed to convert target database schema: %v", err)
		} else {
			s.engine.logger.Infof("Converted target database schema with %d tables", len(targetUM.Tables))
		}
	}

	// Convert target enrichment data
	if targetDB.Tables != "" {
		var err error
		targetEnrichment, err = s.convertEnrichedDataToUnifiedModelEnrichment(targetDB.Tables, targetDB.ID)
		if err != nil {
			s.engine.logger.Warnf("Failed to convert target enrichment data: %v", err)
		} else {
			s.engine.logger.Infof("Converted target enrichment data with %d table enrichments", len(targetEnrichment.TableEnrichments))
		}
	}

	// Use unified model service to match schemas
	if sourceUM != nil && targetUM != nil {
		matchReq := &unifiedmodelv1.MatchUnifiedModelsEnrichedRequest{
			SourceUnifiedModel: sourceUM,
			SourceEnrichment:   sourceEnrichment,
			TargetUnifiedModel: targetUM,
			TargetEnrichment:   targetEnrichment,
			Options: &unifiedmodelv1.MatchOptions{
				NameSimilarityThreshold:  0.3, // Lower threshold to allow more matches
				PoorMatchThreshold:       0.2,
				NameWeight:               0.4,
				TypeWeight:               0.3,
				ClassificationWeight:     0.2,
				PrivilegedDataWeight:     0.1,
				TableStructureWeight:     0.3,
				EnableCrossTableMatching: false,
			},
		}

		s.engine.logger.Infof("Calling MatchUnifiedModelsEnriched with %d source tables and %d target tables", len(sourceUM.Tables), len(targetUM.Tables))

		matchResp, err := umClient.MatchUnifiedModelsEnriched(ctx, matchReq)
		s.engine.logger.Infof("Match response: %v", matchResp)
		if err != nil {
			s.engine.logger.Warnf("Failed to match schemas using unified model service: %v", err)
		} else {
			// Create mapping rules for matched columns
			s.engine.logger.Infof("Creating mapping rules for matched columns: %v", matchResp.TableMatches)
			for _, tableMatch := range matchResp.TableMatches {
				for _, columnMatch := range tableMatch.ColumnMatches {
					if columnMatch.Score >= 0.5 && !columnMatch.IsPoorMatch && !columnMatch.IsUnmatched {
						// Create mapping rule for this column match
						baseRuleName := fmt.Sprintf("%s_%s_to_%s_%s",
							tableMatch.SourceTable, columnMatch.SourceColumn,
							tableMatch.TargetTable, columnMatch.TargetColumn)

						// Find an available rule name by incrementing the number if needed
						ruleName := baseRuleName
						counter := 1
						for {
							existingRule, err := mappingService.GetMappingRuleByName(ctx, req.TenantId, workspaceID, ruleName)
							if err != nil {
								break // Use the current name if we can't check
							}
							if existingRule == nil {
								break // Name is available
							}
							// Name exists, try with incremented counter
							ruleName = fmt.Sprintf("%s_%d", baseRuleName, counter)
							counter++
						}

						// Create metadata based on the match
						metadata := map[string]interface{}{
							"source_table":         tableMatch.SourceTable,
							"source_column":        columnMatch.SourceColumn,
							"source_database_name": sourceDB.Name,
							"source_database_id":   sourceDB.ID,
							"target_table":         tableMatch.TargetTable,
							"target_column":        columnMatch.TargetColumn,
							"target_database_name": targetDB.Name,
							"target_database_id":   targetDB.ID,
							"match_score":          columnMatch.Score,
							"type_compatible":      columnMatch.IsTypeCompatible,
							"match_type":           "auto_generated",
							"generated_at":         time.Now().Format(time.RFC3339),
						}

						// Create empty transformation options (as requested)
						transformationOptions := map[string]interface{}{}

						// Build proper resource URIs
						sourceURI := s.buildResourceURI("column", sourceDB.ID, tableMatch.SourceTable, columnMatch.SourceColumn)
						targetURI := s.buildResourceURI("column", targetDB.ID, tableMatch.TargetTable, columnMatch.TargetColumn)

						// Create the mapping rule
						_, err = mappingService.CreateMappingRule(ctx, req.TenantId, workspaceID, ruleName,
							fmt.Sprintf("Auto-generated rule for %s.%s.%s -> %s.%s.%s",
								req.MappingSourceDatabaseName, tableMatch.SourceTable, columnMatch.SourceColumn,
								req.MappingTargetDatabaseName, tableMatch.TargetTable, columnMatch.TargetColumn),
							sourceURI,
							targetURI,
							"direct_mapping", // Default transformation
							transformationOptions,
							metadata,
							req.OwnerId)

						if err != nil {
							s.engine.logger.Warnf("Failed to create mapping rule %s: %v", ruleName, err)
							continue
						}

						// Attach the mapping rule to the mapping
						err = mappingService.AttachMappingRule(ctx, req.TenantId, workspaceID, req.MappingName, ruleName, nil)
						if err != nil {
							s.engine.logger.Warnf("Failed to attach mapping rule %s to mapping: %v", ruleName, err)
						}
					}
				}
			}
		}
	}

	// Refresh the mapping to get the updated mapping rule count
	updatedMapping, err := mappingService.Get(ctx, req.TenantId, workspaceID, req.MappingName)
	if err != nil {
		s.engine.logger.Warnf("Failed to refresh mapping data: %v", err)
		// Use the original mapping if refresh fails
		updatedMapping = createdMapping
	}

	// Convert to protobuf format
	protoMapping, err := s.mappingToProto(updatedMapping)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert mapping: %v", err)
	}

	return &corev1.AddMappingResponse{
		Message: "Database mapping created successfully",
		Success: true,
		Mapping: protoMapping,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) AddEmptyMapping(ctx context.Context, req *corev1.AddEmptyMappingRequest) (*corev1.AddMappingResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Create the mapping with empty/undefined type information
	// These can be filled in later when the mapping is fully defined
	createdMapping, err := mappingService.Create(ctx, req.TenantId, workspaceID, "undefined", req.MappingName, req.MappingDescription, req.OwnerId,
		"", "", "", "", map[string]interface{}{})
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create mapping: %v", err)
	}

	// Convert to protobuf format
	protoMapping, err := s.mappingToProto(createdMapping)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert mapping: %v", err)
	}

	return &corev1.AddMappingResponse{
		Message: "Mapping created successfully",
		Success: true,
		Mapping: protoMapping,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) ModifyMapping(ctx context.Context, req *corev1.ModifyMappingRequest) (*corev1.ModifyMappingResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Build update map
	updates := make(map[string]interface{})
	if req.MappingNameNew != nil {
		updates["mapping_name"] = *req.MappingNameNew
	}
	if req.MappingDescription != nil {
		updates["mapping_description"] = *req.MappingDescription
	}
	if req.PolicyId != nil {
		updates["policy_ids"] = []string{*req.PolicyId}
	}

	// Update the mapping
	updatedMapping, err := mappingService.Update(ctx, req.TenantId, workspaceID, req.MappingName, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update mapping: %v", err)
	}

	// Convert to protobuf format
	protoMapping, err := s.mappingToProto(updatedMapping)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert mapping: %v", err)
	}

	return &corev1.ModifyMappingResponse{
		Message: "Mapping updated successfully",
		Success: true,
		Mapping: protoMapping,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DeleteMapping(ctx context.Context, req *corev1.DeleteMappingRequest) (*corev1.DeleteMappingResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Determine keep_rules value (default to false if not provided)
	keepRules := false
	if req.KeepRules != nil {
		keepRules = *req.KeepRules
	}

	// Delete the mapping
	err = mappingService.Delete(ctx, req.TenantId, workspaceID, req.MappingName, keepRules)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to delete mapping: %v", err)
	}

	return &corev1.DeleteMappingResponse{
		Message: "Mapping deleted successfully",
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) AttachMappingRule(ctx context.Context, req *corev1.AttachMappingRuleRequest) (*corev1.AttachMappingRuleResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Attach the mapping rule
	err = mappingService.AttachMappingRule(ctx, req.TenantId, workspaceID, req.MappingName, req.MappingRuleName, req.MappingRuleOrder)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to attach mapping rule: %v", err)
	}

	// Invalidate the mapping's validation status
	mappingObj, err := mappingService.GetByName(ctx, req.TenantId, workspaceID, req.MappingName)
	if err != nil {
		s.engine.logger.Warnf("Failed to get mapping for invalidation: %v", err)
	} else {
		if err := mappingService.InvalidateMapping(ctx, mappingObj.ID); err != nil {
			s.engine.logger.Warnf("Failed to invalidate mapping validation: %v", err)
		}
	}

	return &corev1.AttachMappingRuleResponse{
		Message: "Mapping rule attached successfully",
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DetachMappingRule(ctx context.Context, req *corev1.DetachMappingRuleRequest) (*corev1.DetachMappingRuleResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Detach the mapping rule
	err = mappingService.DetachMappingRule(ctx, req.TenantId, workspaceID, req.MappingName, req.MappingRuleName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to detach mapping rule: %v", err)
	}

	// Invalidate the mapping's validation status
	mappingObj, err := mappingService.GetByName(ctx, req.TenantId, workspaceID, req.MappingName)
	if err != nil {
		s.engine.logger.Warnf("Failed to get mapping for invalidation: %v", err)
	} else {
		if err := mappingService.InvalidateMapping(ctx, mappingObj.ID); err != nil {
			s.engine.logger.Warnf("Failed to invalidate mapping validation: %v", err)
		}
	}

	return &corev1.DetachMappingRuleResponse{
		Message: "Mapping rule detached successfully",
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) ListMappingRules(ctx context.Context, req *corev1.ListMappingRulesRequest) (*corev1.ListMappingRulesResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// List mapping rules for the tenant and workspace
	rules, err := mappingService.ListMappingRules(ctx, req.TenantId, workspaceID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to list mapping rules: %v", err)
	}

	// Convert to protobuf format
	protoRules := make([]*corev1.MappingRule, len(rules))
	for i, r := range rules {
		protoRule, err := s.mappingRuleToProto(r)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to convert mapping rule: %v", err)
		}
		protoRules[i] = protoRule
	}

	return &corev1.ListMappingRulesResponse{
		MappingRules: protoRules,
	}, nil
}

func (s *Server) ShowMappingRule(ctx context.Context, req *corev1.ShowMappingRuleRequest) (*corev1.ShowMappingRuleResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Get the mapping rule
	r, err := mappingService.GetMappingRuleByName(ctx, req.TenantId, workspaceID, req.MappingRuleName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "mapping rule not found: %v", err)
	}

	// Get mappings that use this rule
	mappings, err := mappingService.GetMappingsForRule(ctx, req.TenantId, workspaceID, req.MappingRuleName)
	if err != nil {
		s.engine.logger.Warnf("Failed to get mappings for mapping rule %s: %v", req.MappingRuleName, err)
		mappings = []*mapping.Mapping{}
	}

	// Convert to protobuf format
	protoRule, err := s.mappingRuleToProto(r)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert mapping rule: %v", err)
	}

	// Convert mappings to protobuf format (without their rules to avoid circular references)
	protoMappings := make([]*corev1.Mapping, len(mappings))
	for i, m := range mappings {
		protoMapping, err := s.mappingToProto(m)
		if err != nil {
			s.engine.logger.Warnf("Failed to convert mapping: %v", err)
			continue
		}
		// Clear the mapping rules to avoid circular references
		protoMapping.MappingRules = nil
		protoMappings[i] = protoMapping
	}

	protoRule.Mappings = protoMappings

	return &corev1.ShowMappingRuleResponse{
		MappingRule: protoRule,
	}, nil
}

func (s *Server) AddMappingRule(ctx context.Context, req *corev1.AddMappingRuleRequest) (*corev1.AddMappingRuleResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Convert transformation options to map[string]interface{}
	transformationOptions := make(map[string]interface{})
	if req.MappingRuleTransformationOptions != "" {
		if err := json.Unmarshal([]byte(req.MappingRuleTransformationOptions), &transformationOptions); err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.InvalidArgument, "failed to unmarshal transformation options: %v", err)
		}
	}

	// Convert metadata to map[string]interface{}
	metadata := make(map[string]interface{})
	if req.MappingRuleMetadata != "" {
		if err := json.Unmarshal([]byte(req.MappingRuleMetadata), &metadata); err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.InvalidArgument, "failed to unmarshal metadata: %v", err)
		}
	}

	// Determine source and target URIs from new or legacy fields
	var sourceURIs []string
	var targetURIs []string

	// Use new multi-item fields if provided, otherwise fall back to legacy single fields
	if len(req.SourceItemUris) > 0 {
		sourceURIs = req.SourceItemUris
	} else if req.MappingRuleSource != "" {
		sourceURIs = []string{req.MappingRuleSource}
	}

	if len(req.TargetItemUris) > 0 {
		targetURIs = req.TargetItemUris
	} else if req.MappingRuleTarget != "" {
		targetURIs = []string{req.MappingRuleTarget}
	}

	// Determine cardinality
	cardinality := req.MappingRuleCardinality
	if cardinality == "" {
		cardinality = inferCardinality(len(sourceURIs), len(targetURIs))
		s.engine.logger.Infof("Inferred cardinality: %s (sources: %d, targets: %d)", cardinality, len(sourceURIs), len(targetURIs))
	}

	// Validate cardinality
	if err := validateCardinality(cardinality, len(sourceURIs), len(targetURIs)); err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.InvalidArgument, "invalid cardinality: %v", err)
	}

	// Validate transformation if provided
	var transformationType string
	if req.MappingRuleTransformationName != "" {
		transformationName := req.MappingRuleTransformationName
		s.engine.logger.Infof("Validating transformation: %s", transformationName)

		// Get transformation client
		transformationClient, err := s.getTransformationClient()
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Unavailable, "failed to connect to transformation service: %v", err)
		}

		// Call GetTransformationMetadata to validate transformation exists
		metadataReq := &transformationv1.GetTransformationMetadataRequest{
			TransformationName: transformationName,
		}

		metadataResp, err := transformationClient.GetTransformationMetadata(ctx, metadataReq)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.InvalidArgument, "transformation '%s' does not exist or is invalid: %v", transformationName, err)
		}

		// Check if transformation was found
		if metadataResp.Status != commonv1.Status_STATUS_SUCCESS || metadataResp.Metadata == nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.InvalidArgument, "transformation '%s' does not exist: %s", transformationName, metadataResp.StatusMessage)
		}

		transformationType = metadataResp.Metadata.Type

		// Validate transformation supports the cardinality
		if err := validateTransformationCardinality(transformationType, cardinality); err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}

		s.engine.logger.Infof("Transformation '%s' validated successfully (type: %s, cardinality: %s)",
			transformationName, transformationType, cardinality)
	}

	// Resolve source URIs to item IDs
	sourceItemIDs := make([]string, len(sourceURIs))
	sourceOrders := make([]int, len(sourceURIs))
	for i, uri := range sourceURIs {
		item, err := mappingService.GetItemByURI(ctx, uri)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.NotFound, "source item not found for URI '%s': %v", uri, err)
		}
		sourceItemIDs[i] = item.ItemID
		sourceOrders[i] = i
	}

	// Resolve target URIs to item IDs
	targetItemIDs := make([]string, len(targetURIs))
	targetOrders := make([]int, len(targetURIs))
	for i, uri := range targetURIs {
		item, err := mappingService.GetItemByURI(ctx, uri)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.NotFound, "target item not found for URI '%s': %v", uri, err)
		}
		targetItemIDs[i] = item.ItemID
		targetOrders[i] = i
	}

	// Add metadata
	metadata["match_type"] = "user_defined"
	metadata["source_uris"] = sourceURIs
	metadata["target_uris"] = targetURIs

	// Create the mapping rule with cardinality
	// For backward compatibility, use the first source/target URI for the legacy fields
	legacySource := ""
	legacyTarget := ""
	if len(sourceURIs) > 0 {
		legacySource = sourceURIs[0]
	}
	if len(targetURIs) > 0 {
		legacyTarget = targetURIs[0]
	}

	createdRule, err := mappingService.CreateMappingRule(
		ctx, req.TenantId, workspaceID, req.MappingRuleName, req.MappingRuleDescription,
		legacySource, legacyTarget, req.MappingRuleTransformationName,
		transformationOptions, metadata, req.OwnerId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create mapping rule: %v", err)
	}

	// Update the rule's cardinality in the database
	if err := mappingService.UpdateMappingRuleCardinality(ctx, createdRule.ID, cardinality); err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to set cardinality: %v", err)
	}

	// Attach source items to the rule
	if len(sourceItemIDs) > 0 {
		if err := mappingService.AttachSourceItems(ctx, createdRule.ID, sourceItemIDs, sourceOrders); err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to attach source items: %v", err)
		}
	}

	// Attach target items to the rule
	if len(targetItemIDs) > 0 {
		if err := mappingService.AttachTargetItems(ctx, createdRule.ID, targetItemIDs, targetOrders); err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to attach target items: %v", err)
		}
	}

	// Convert to protobuf format
	protoRule, err := s.mappingRuleToProto(createdRule)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert mapping rule: %v", err)
	}

	// Populate the new fields in the proto
	protoRule.MappingRuleCardinality = cardinality
	protoRule.SourceItemUris = sourceURIs
	protoRule.TargetItemUris = targetURIs

	return &corev1.AddMappingRuleResponse{
		Message:     "Mapping rule created successfully",
		Success:     true,
		MappingRule: protoRule,
		Status:      commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) ModifyMappingRule(ctx context.Context, req *corev1.ModifyMappingRuleRequest) (*corev1.ModifyMappingRuleResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Get the existing mapping rule to extract information for validation
	existingRule, err := mappingService.GetMappingRuleByName(ctx, req.TenantId, workspaceID, req.MappingRuleName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "mapping rule not found: %v", err)
	}

	// Validate transformation if being changed
	if req.MappingRuleTransformationName != nil && *req.MappingRuleTransformationName != "" {
		transformationName := *req.MappingRuleTransformationName
		s.engine.logger.Infof("Validating transformation: %s", transformationName)

		// Get transformation client
		transformationClient, err := s.getTransformationClient()
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Unavailable, "failed to connect to transformation service: %v", err)
		}

		{
			// Call GetTransformationMetadata to validate transformation exists
			metadataReq := &transformationv1.GetTransformationMetadataRequest{
				TransformationName: transformationName,
			}

			metadataResp, err := transformationClient.GetTransformationMetadata(ctx, metadataReq)
			if err != nil {
				s.engine.IncrementErrors()
				return nil, status.Errorf(codes.InvalidArgument, "transformation '%s' does not exist or is invalid: %v", transformationName, err)
			}

			// Check if transformation was found (check Status field)
			if metadataResp.Status != commonv1.Status_STATUS_SUCCESS || metadataResp.Metadata == nil {
				s.engine.IncrementErrors()
				return nil, status.Errorf(codes.InvalidArgument, "transformation '%s' does not exist: %s", transformationName, metadataResp.StatusMessage)
			}

			// Validate transformation requirements based on type
			if metadataResp.Metadata != nil {
				transformationType := metadataResp.Metadata.Type

				// Determine final source and target (use existing if not being updated)
				// Extract from metadata
				var finalSource, finalTarget string
				if existingRule.Metadata != nil {
					if src, ok := existingRule.Metadata["source_resource_uri"].(string); ok {
						finalSource = src
					}
					if tgt, ok := existingRule.Metadata["target_resource_uri"].(string); ok {
						finalTarget = tgt
					}
				}

				if req.MappingRuleSource != nil && *req.MappingRuleSource != "" {
					finalSource = *req.MappingRuleSource
				}

				if req.MappingRuleTarget != nil && *req.MappingRuleTarget != "" {
					finalTarget = *req.MappingRuleTarget
				}

				// Validate based on transformation type
				switch transformationType {
				case "generator":
					// Generator transformations should not have a source
					if finalSource != "" {
						s.engine.IncrementErrors()
						return nil, status.Errorf(codes.InvalidArgument,
							"transformation '%s' is a generator type and should not have a source column", transformationName)
					}
					// Generator transformations must have a target
					if finalTarget == "" {
						s.engine.IncrementErrors()
						return nil, status.Errorf(codes.InvalidArgument,
							"transformation '%s' is a generator type and requires a target column", transformationName)
					}
				case "null_returning":
					// Null-returning transformations should not have a target
					if finalTarget != "" {
						s.engine.IncrementErrors()
						return nil, status.Errorf(codes.InvalidArgument,
							"transformation '%s' is a null-returning type and should not have a target column", transformationName)
					}
					// Null-returning transformations must have a source
					if finalSource == "" {
						s.engine.IncrementErrors()
						return nil, status.Errorf(codes.InvalidArgument,
							"transformation '%s' is a null-returning type and requires a source column", transformationName)
					}
				case "passthrough":
					// Passthrough transformations require both source and target
					if finalSource == "" {
						s.engine.IncrementErrors()
						return nil, status.Errorf(codes.InvalidArgument,
							"transformation '%s' is a passthrough type and requires a source column", transformationName)
					}
					if finalTarget == "" {
						s.engine.IncrementErrors()
						return nil, status.Errorf(codes.InvalidArgument,
							"transformation '%s' is a passthrough type and requires a target column", transformationName)
					}
				}

				s.engine.logger.Infof("Transformation '%s' validated successfully (type: %s)", transformationName, transformationType)
			}
		}
	}

	// Note: Column validation is deferred to actual resource URI resolution
	// during rule attachment and mapping execution. The resource URI system
	// handles validation through GetItemByURI calls.
	// Explicit column validation methods can be added later if needed.

	// Build update map - need to merge changes into metadata for new schema
	updates := make(map[string]interface{})

	// Handle simple field updates
	if req.MappingRuleNameNew != nil {
		updates["mapping_rule_name"] = *req.MappingRuleNameNew
	}
	if req.MappingRuleDescription != nil {
		updates["mapping_rule_description"] = *req.MappingRuleDescription
	}

	// Handle metadata updates - merge with existing metadata
	needsMetadataUpdate := false
	updatedMetadata := make(map[string]interface{})

	// Start with existing metadata
	if existingRule.Metadata != nil {
		for k, v := range existingRule.Metadata {
			updatedMetadata[k] = v
		}
	}

	// Update source URI in metadata if provided
	if req.MappingRuleSource != nil {
		updatedMetadata["source_resource_uri"] = *req.MappingRuleSource
		needsMetadataUpdate = true
	}

	// Update target URI in metadata if provided
	if req.MappingRuleTarget != nil {
		updatedMetadata["target_resource_uri"] = *req.MappingRuleTarget
		needsMetadataUpdate = true
	}

	// Update transformation name in metadata if provided
	if req.MappingRuleTransformationName != nil {
		updatedMetadata["transformation_name"] = *req.MappingRuleTransformationName
		needsMetadataUpdate = true
	}

	// Update transformation options in metadata if provided
	if req.MappingRuleTransformationOptions != nil {
		var transformationOptions map[string]interface{}
		if err := json.Unmarshal([]byte(*req.MappingRuleTransformationOptions), &transformationOptions); err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.InvalidArgument, "failed to unmarshal transformation options: %v", err)
		}
		updatedMetadata["transformation_options"] = transformationOptions
		needsMetadataUpdate = true
	}

	// Handle explicit metadata updates (merge with above changes)
	if req.MappingRuleMetadata != nil {
		var explicitMetadata map[string]interface{}
		if err := json.Unmarshal([]byte(*req.MappingRuleMetadata), &explicitMetadata); err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.InvalidArgument, "failed to unmarshal metadata: %v", err)
		}
		// Merge explicit metadata into updatedMetadata
		for k, v := range explicitMetadata {
			updatedMetadata[k] = v
		}
		needsMetadataUpdate = true
	}

	// Add metadata to updates if it changed
	if needsMetadataUpdate {
		updates["mapping_rule_metadata"] = updatedMetadata
	}

	// Update the mapping rule
	updatedRule, err := mappingService.ModifyMappingRule(ctx, req.TenantId, workspaceID, req.MappingRuleName, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update mapping rule: %v", err)
	}

	// Invalidate all mappings that use this rule
	mappings, err := mappingService.GetMappingsForRule(ctx, req.TenantId, workspaceID, req.MappingRuleName)
	if err != nil {
		s.engine.logger.Warnf("Failed to get mappings for rule invalidation: %v", err)
	} else {
		for _, mappingObj := range mappings {
			if err := mappingService.InvalidateMapping(ctx, mappingObj.ID); err != nil {
				s.engine.logger.Warnf("Failed to invalidate mapping %s: %v", mappingObj.Name, err)
			}
		}
	}

	// Convert to protobuf format
	protoRule, err := s.mappingRuleToProto(updatedRule)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert mapping rule: %v", err)
	}

	return &corev1.ModifyMappingRuleResponse{
		Message:     "Mapping rule updated successfully",
		Success:     true,
		MappingRule: protoRule,
		Status:      commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DeleteMappingRule(ctx context.Context, req *corev1.DeleteMappingRuleRequest) (*corev1.DeleteMappingRuleResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Get mappings using this rule before deletion (for invalidation)
	mappingsToInvalidate, err := mappingService.GetMappingsForRule(ctx, req.TenantId, workspaceID, req.MappingRuleName)
	if err != nil {
		s.engine.logger.Warnf("Failed to get mappings for rule invalidation: %v", err)
		mappingsToInvalidate = nil
	}

	// Delete the mapping rule
	err = mappingService.DeleteMappingRule(ctx, req.TenantId, workspaceID, req.MappingRuleName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to delete mapping rule: %v", err)
	}

	// Invalidate all mappings that used this rule
	for _, mappingObj := range mappingsToInvalidate {
		if err := mappingService.InvalidateMapping(ctx, mappingObj.ID); err != nil {
			s.engine.logger.Warnf("Failed to invalidate mapping %s: %v", mappingObj.Name, err)
		}
	}

	return &corev1.DeleteMappingRuleResponse{
		Message: "Mapping rule deleted successfully",
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// Helper functions for type conversion
func getString(m map[string]interface{}, key string) string {
	if val, ok := m[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func getBool(m map[string]interface{}, key string) bool {
	if val, ok := m[key]; ok {
		if b, ok := val.(bool); ok {
			return b
		}
	}
	return false
}

func getFloat64(m map[string]interface{}, key string) float64 {
	if val, ok := m[key]; ok {
		switch v := val.(type) {
		case float64:
			return v
		case float32:
			return float64(v)
		case int:
			return float64(v)
		case int32:
			return float64(v)
		case int64:
			return float64(v)
		}
	}
	return 0
}

// convertDatabaseSchemaToUnifiedModel converts stored JSON schema to UnifiedModel protobuf
func (s *Server) convertDatabaseSchemaToUnifiedModel(schemaJSON string) (*unifiedmodelv1.UnifiedModel, error) {
	if schemaJSON == "" {
		return nil, fmt.Errorf("schema is empty")
	}

	// Parse JSON into Go UnifiedModel
	var goUM unifiedmodel.UnifiedModel
	if err := json.Unmarshal([]byte(schemaJSON), &goUM); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema JSON: %w", err)
	}

	// Convert to protobuf
	pbUM := goUM.ToProto()
	return pbUM, nil
}

// filterUnifiedModelForTable creates a new UnifiedModel containing only the specified table
func (s *Server) filterUnifiedModelForTable(um *unifiedmodelv1.UnifiedModel, tableName string) *unifiedmodelv1.UnifiedModel {
	if um == nil {
		return nil
	}

	// Create a new UnifiedModel with only the specified table
	filteredUM := &unifiedmodelv1.UnifiedModel{
		DatabaseType: um.DatabaseType,
		Tables:       make(map[string]*unifiedmodelv1.Table),
		Schemas:      make(map[string]*unifiedmodelv1.Schema),
		Views:        make(map[string]*unifiedmodelv1.View),
		Functions:    make(map[string]*unifiedmodelv1.Function),
		Procedures:   make(map[string]*unifiedmodelv1.Procedure),
		Triggers:     make(map[string]*unifiedmodelv1.Trigger),
		Sequences:    make(map[string]*unifiedmodelv1.Sequence),
		Types:        make(map[string]*unifiedmodelv1.Type),
		Indexes:      make(map[string]*unifiedmodelv1.Index),
		Constraints:  make(map[string]*unifiedmodelv1.Constraint),
	}

	// Copy the specific table if it exists
	if table, exists := um.Tables[tableName]; exists {
		filteredUM.Tables[tableName] = table

		// Copy related schemas, types, etc. that might be referenced by this table
		// This is a simplified approach - in a more complete implementation,
		// we might want to trace dependencies more thoroughly

		// Copy all schemas (they're usually small and might be referenced)
		for name, schema := range um.Schemas {
			filteredUM.Schemas[name] = schema
		}

		// Copy all types (they might be referenced by columns)
		for name, umType := range um.Types {
			filteredUM.Types[name] = umType
		}
	}

	return filteredUM
}

// convertEnrichedDataToUnifiedModelEnrichment converts enriched table data to UnifiedModelEnrichment
func (s *Server) convertEnrichedDataToUnifiedModelEnrichment(enrichedDataJSON string, schemaID string) (*unifiedmodelv1.UnifiedModelEnrichment, error) {
	if enrichedDataJSON == "" {
		return nil, nil // No enrichment data available
	}

	var enrichedData map[string]interface{}
	if err := json.Unmarshal([]byte(enrichedDataJSON), &enrichedData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal enriched data: %w", err)
	}

	enrichment := &unifiedmodelv1.UnifiedModelEnrichment{
		SchemaId:          schemaID,
		EnrichmentVersion: "1.0.0",
		GeneratedAt:       time.Now().Unix(),
		GeneratedBy:       "core-service",
		TableEnrichments:  make(map[string]*unifiedmodelv1.TableEnrichment),
		ColumnEnrichments: make(map[string]*unifiedmodelv1.ColumnEnrichment),
	}

	// Extract tables from enriched data
	tablesData, ok := enrichedData["tables"]
	if !ok {
		return enrichment, nil
	}

	tableArray, ok := tablesData.([]interface{})
	if !ok {
		return enrichment, nil
	}

	// Convert each table's enrichment data
	for _, tableData := range tableArray {
		tableMap, ok := tableData.(map[string]interface{})
		if !ok {
			continue
		}

		tableName := getString(tableMap, "name")
		if tableName == "" {
			continue
		}

		// Create table enrichment
		tableEnrichment := &unifiedmodelv1.TableEnrichment{
			PrimaryCategory:          getString(tableMap, "primary_category"),
			ClassificationConfidence: getFloat64(tableMap, "classification_confidence"),
			AccessPattern:            getString(tableMap, "access_pattern"),
			EstimatedRows:            int64(getFloat64(tableMap, "estimated_rows")),
			HasPrivilegedData:        getBool(tableMap, "has_privileged_data"),
		}

		// Convert classification scores
		if scoresData, ok := tableMap["classification_scores"].([]interface{}); ok {
			for _, scoreData := range scoresData {
				if scoreMap, ok := scoreData.(map[string]interface{}); ok {
					score := &unifiedmodelv1.CategoryScore{
						Category: getString(scoreMap, "category"),
						Score:    getFloat64(scoreMap, "score"),
						Reason:   getString(scoreMap, "reason"),
					}
					tableEnrichment.ClassificationScores = append(tableEnrichment.ClassificationScores, score)
				}
			}
		}

		enrichment.TableEnrichments[tableName] = tableEnrichment

		// Convert column enrichments
		if columnsData, ok := tableMap["columns"].([]interface{}); ok {
			for _, colData := range columnsData {
				if colMap, ok := colData.(map[string]interface{}); ok {
					columnName := getString(colMap, "name")
					if columnName == "" {
						continue
					}

					columnKey := fmt.Sprintf("%s.%s", tableName, columnName)
					columnEnrichment := &unifiedmodelv1.ColumnEnrichment{
						IsPrivilegedData:      getBool(colMap, "is_privileged_data"),
						DataCategory:          getString(colMap, "data_category"),
						PrivilegedConfidence:  getFloat64(colMap, "privileged_confidence"),
						PrivilegedDescription: getString(colMap, "privileged_description"),
						RiskLevel:             getString(colMap, "risk_level"),
					}

					enrichment.ColumnEnrichments[columnKey] = columnEnrichment
				}
			}
		}
	}

	return enrichment, nil
}

// filterUnifiedModelEnrichmentForTable filters enrichment data to only include the specified table
func (s *Server) filterUnifiedModelEnrichmentForTable(enrichment *unifiedmodelv1.UnifiedModelEnrichment, tableName string) *unifiedmodelv1.UnifiedModelEnrichment {
	if enrichment == nil {
		return nil
	}

	filteredEnrichment := &unifiedmodelv1.UnifiedModelEnrichment{
		SchemaId:          enrichment.SchemaId,
		EnrichmentVersion: enrichment.EnrichmentVersion,
		GeneratedAt:       enrichment.GeneratedAt,
		GeneratedBy:       enrichment.GeneratedBy,
		TableEnrichments:  make(map[string]*unifiedmodelv1.TableEnrichment),
		ColumnEnrichments: make(map[string]*unifiedmodelv1.ColumnEnrichment),
	}

	// Copy table enrichment for the specific table
	if tableEnrichment, exists := enrichment.TableEnrichments[tableName]; exists {
		filteredEnrichment.TableEnrichments[tableName] = tableEnrichment
	}

	// Copy column enrichments for the specific table
	tablePrefix := tableName + "."
	for key, columnEnrichment := range enrichment.ColumnEnrichments {
		if len(key) > len(tablePrefix) && key[:len(tablePrefix)] == tablePrefix {
			filteredEnrichment.ColumnEnrichments[key] = columnEnrichment
		}
	}

	return filteredEnrichment
}

// ============================================================================
// Resource URI Builder Helper Functions
// ============================================================================

// buildResourceURI constructs a proper redb:// URI according to RESOURCE_ADDRESSING.md
// Note: Uses double slash format (redb://data) not single slash (redb:/data)
func (s *Server) buildResourceURI(scope, databaseID, tableName, columnName string) string {
	switch scope {
	case "database":
		return fmt.Sprintf("redb://data/database/%s", databaseID)
	case "table":
		if tableName == "" {
			s.engine.logger.Warnf("Table name is empty for table-scope resource URI")
			return fmt.Sprintf("redb://data/database/%s", databaseID)
		}
		return fmt.Sprintf("redb://data/database/%s/table/%s", databaseID, tableName)
	case "column":
		if tableName == "" || columnName == "" {
			s.engine.logger.Warnf("Table or column name is empty for column-scope resource URI")
			return fmt.Sprintf("redb://data/database/%s", databaseID)
		}
		return fmt.Sprintf("redb://data/database/%s/table/%s/column/%s", databaseID, tableName, columnName)
	default:
		s.engine.logger.Warnf("Unknown scope '%s' in buildResourceURI, defaulting to database", scope)
		return fmt.Sprintf("redb://data/database/%s", databaseID)
	}
}

// buildMCPResourceURI constructs a proper mcp:// URI
// For now, we use simple format: mcp://{resource_name}
// Future: mcp://{server_id}/resource/{resource_name}
func (s *Server) buildMCPResourceURI(mcpResourceName string) string {
	// Currently using simplified format without server_id
	return fmt.Sprintf("mcp://%s", mcpResourceName)
}

// buildMappingType constructs the mapping_type string based on source and target types
func (s *Server) buildMappingType(sourceType, targetType string) string {
	return fmt.Sprintf("%s-to-%s", sourceType, targetType)
}

// parseSourceTarget parses database[.table] format or redb:// URI format
// For URIs, it resolves database IDs to database names
func (s *Server) parseSourceTarget(input string) (database, table string, err error) {
	if input == "" {
		return "", "", fmt.Errorf("source/target cannot be empty")
	}

	// Check if input is a URI (redb://, mcp://, stream://, webhook://)
	if strings.Contains(input, "://") {
		// Parse as URI
		addr, err := resource.ParseResourceURI(input)
		if err != nil {
			return "", "", fmt.Errorf("failed to parse URI: %w", err)
		}

		// For database resources (redb://), extract database ID and table name
		if addr.Protocol == resource.ProtocolDatabase {
			// Resolve database ID to database name
			ctx := context.Background()
			databaseName, err := s.getDatabaseNameByID(ctx, addr.DatabaseID)
			if err != nil {
				return "", "", fmt.Errorf("failed to resolve database ID %s: %w", addr.DatabaseID, err)
			}

			// Extract table name if present
			tableName := ""
			if addr.ObjectType == resource.ObjectTypeTable {
				tableName = addr.ObjectName
			}

			return databaseName, tableName, nil
		}

		// For non-database resources (MCP, stream, webhook), return the URI as-is in the database field
		// The table field remains empty for these resources
		return input, "", nil
	}

	// Legacy format: database[.table]
	parts := strings.Split(input, ".")
	if len(parts) == 1 {
		// Only database name
		return parts[0], "", nil
	} else if len(parts) == 2 {
		// Database and table name
		return parts[0], parts[1], nil
	} else {
		return "", "", fmt.Errorf("invalid format '%s': expected 'database' or 'database.table'", input)
	}
}

// getDatabaseNameByID retrieves the database name from database ID
func (s *Server) getDatabaseNameByID(ctx context.Context, databaseID string) (string, error) {
	// Create database service
	databaseService := database.NewService(s.engine.db, s.engine.logger)

	// Get database by ID
	db, err := databaseService.GetByID(ctx, databaseID)
	if err != nil {
		return "", fmt.Errorf("failed to get database: %w", err)
	}

	return db.Name, nil
}

// addTableMappingUnified handles table-scoped mapping creation from unified request
func (s *Server) addTableMappingUnified(ctx context.Context, req *corev1.AddMappingRequest, sourceDB, sourceTable, targetDB, targetTable string, generateRules bool) (*corev1.AddMappingResponse, error) {
	// Convert to legacy AddTableMappingRequest format
	legacyReq := &corev1.AddTableMappingRequest{
		TenantId:                  req.TenantId,
		WorkspaceName:             req.WorkspaceName,
		MappingName:               req.MappingName,
		MappingDescription:        req.MappingDescription,
		MappingSourceDatabaseName: sourceDB,
		MappingSourceTableName:    sourceTable,
		MappingTargetDatabaseName: targetDB,
		MappingTargetTableName:    targetTable,
		OwnerId:                   req.OwnerId,
	}

	if req.PolicyId != nil {
		legacyReq.PolicyId = req.PolicyId
	}

	// Call existing AddTableMapping implementation
	// Note: AddTableMapping doesn't have generateRules parameter yet, we'll need to refactor it
	// For now, just call it - the refactoring will happen in a separate step
	return s.AddTableMapping(ctx, legacyReq)
}

// addDatabaseMappingUnified handles database-scoped mapping creation from unified request with enhanced matching
func (s *Server) addDatabaseMappingUnified(ctx context.Context, req *corev1.AddMappingRequest, sourceDB, targetDB string, generateRules bool) (*corev1.AddMappingResponse, error) {
	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get database service to validate and fetch database schemas
	databaseService := database.NewService(s.engine.db, s.engine.logger)

	// Validate source database exists and belongs to the tenant/workspace
	sourceDBObj, err := databaseService.Get(ctx, req.TenantId, workspaceID, sourceDB)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "source database not found: %v", err)
	}

	// Validate target database exists and belongs to the tenant/workspace
	targetDBObj, err := databaseService.Get(ctx, req.TenantId, workspaceID, targetDB)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "target database not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Build resource URIs and mapping type
	sourceType := "database"
	targetType := "database"
	sourceIdentifier := s.buildResourceURI("database", sourceDBObj.ID, "", "")
	targetIdentifier := s.buildResourceURI("database", targetDBObj.ID, "", "")
	mappingType := s.buildMappingType(sourceType, targetType)

	// Build mapping object with human-readable names
	mappingObject := map[string]interface{}{
		"source_database_name": sourceDBObj.Name,
		"source_database_id":   sourceDBObj.ID,
		"target_database_name": targetDBObj.Name,
		"target_database_id":   targetDBObj.ID,
	}

	// Create the mapping
	createdMapping, err := mappingService.Create(ctx, req.TenantId, workspaceID, mappingType, req.MappingName, req.MappingDescription, req.OwnerId,
		sourceType, targetType, sourceIdentifier, targetIdentifier, mappingObject)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create mapping: %v", err)
	}

	// Get unified model client
	umClient := s.engine.GetUnifiedModelClient()
	if umClient == nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "unified model service not available")
	}

	// Convert source database schema to UnifiedModel
	var sourceUM *unifiedmodelv1.UnifiedModel
	var sourceEnrichment *unifiedmodelv1.UnifiedModelEnrichment

	if sourceDBObj.Schema != "" {
		var err error
		sourceUM, err = s.convertDatabaseSchemaToUnifiedModel(sourceDBObj.Schema)
		if err != nil {
			s.engine.logger.Warnf("Failed to convert source database schema: %v", err)
		} else {
			s.engine.logger.Infof("Converted source database schema with %d tables", len(sourceUM.Tables))
		}
	}

	// Convert source enrichment data
	if sourceDBObj.Tables != "" {
		var err error
		sourceEnrichment, err = s.convertEnrichedDataToUnifiedModelEnrichment(sourceDBObj.Tables, sourceDBObj.ID)
		if err != nil {
			s.engine.logger.Warnf("Failed to convert source enrichment data: %v", err)
		} else {
			s.engine.logger.Infof("Converted source enrichment data with %d table enrichments", len(sourceEnrichment.TableEnrichments))
		}
	}

	// Convert target database schema to UnifiedModel
	var targetUM *unifiedmodelv1.UnifiedModel
	var targetEnrichment *unifiedmodelv1.UnifiedModelEnrichment

	if targetDBObj.Schema != "" {
		var err error
		targetUM, err = s.convertDatabaseSchemaToUnifiedModel(targetDBObj.Schema)
		if err != nil {
			s.engine.logger.Warnf("Failed to convert target database schema: %v", err)
		} else {
			s.engine.logger.Infof("Converted target database schema with %d tables", len(targetUM.Tables))
		}
	}

	// Convert target enrichment data
	if targetDBObj.Tables != "" {
		var err error
		targetEnrichment, err = s.convertEnrichedDataToUnifiedModelEnrichment(targetDBObj.Tables, targetDBObj.ID)
		if err != nil {
			s.engine.logger.Warnf("Failed to convert target enrichment data: %v", err)
		} else {
			s.engine.logger.Infof("Converted target enrichment data with %d table enrichments", len(targetEnrichment.TableEnrichments))
		}
	}

	// Perform enhanced database-to-database matching (only if generateRules is true)
	if generateRules && sourceUM != nil && targetUM != nil {
		// Create matching request with database-optimized options
		// For database-level mapping, we prioritize table name matching and structure
		matchReq := &unifiedmodelv1.MatchUnifiedModelsEnrichedRequest{
			SourceUnifiedModel: sourceUM,
			TargetUnifiedModel: targetUM,
			SourceEnrichment:   sourceEnrichment,
			TargetEnrichment:   targetEnrichment,
			Options: &unifiedmodelv1.MatchOptions{
				NameSimilarityThreshold:  0.2,   // Lower threshold to catch more table name similarities
				PoorMatchThreshold:       0.3,   // Lower threshold for poor matches
				NameWeight:               0.6,   // Higher weight for table name similarity
				TypeWeight:               0.15,  // Moderate weight for data types
				ClassificationWeight:     0.15,  // Moderate weight for table classification
				PrivilegedDataWeight:     0.05,  // Lower weight for privileged data
				TableStructureWeight:     0.05,  // Lower weight for structure
				EnableCrossTableMatching: false, // Disable cross-table matching for cleaner results
			},
		}

		// Call unified model service for matching
		s.engine.logger.Infof("Starting database-level matching with %d source tables and %d target tables",
			len(sourceUM.Tables), len(targetUM.Tables))

		matchResp, err := umClient.MatchUnifiedModelsEnriched(ctx, matchReq)
		if err != nil {
			s.engine.logger.Warnf("Failed to match unified models: %v", err)
		} else {
			// Process matching results and create mapping rules
			s.engine.logger.Infof("Database matching completed: found %d table matches for mapping %s (overall score: %.3f)",
				len(matchResp.TableMatches), req.MappingName, matchResp.OverallSimilarityScore)

			for _, tableMatch := range matchResp.TableMatches {
				s.engine.logger.Infof("Table match: %s -> %s (score: %.3f, %d/%d columns matched)",
					tableMatch.SourceTable, tableMatch.TargetTable, tableMatch.Score,
					tableMatch.MatchedColumns, tableMatch.TotalSourceColumns)

				// Create mapping rules for each column match within this table match
				for _, columnMatch := range tableMatch.ColumnMatches {
					ruleName := fmt.Sprintf("%s_%s_%s_to_%s_%s_%s",
						sourceDB, tableMatch.SourceTable, columnMatch.SourceColumn,
						targetDB, tableMatch.TargetTable, columnMatch.TargetColumn)

					// Create metadata for the mapping rule
					metadata := map[string]interface{}{
						"generated_at":         time.Now().UTC().Format(time.RFC3339),
						"match_score":          columnMatch.Score,
						"match_type":           "enriched_match",
						"source_column":        columnMatch.SourceColumn,
						"source_table":         tableMatch.SourceTable,
						"source_database_name": sourceDBObj.Name,
						"source_database_id":   sourceDBObj.ID,
						"target_column":        columnMatch.TargetColumn,
						"target_table":         tableMatch.TargetTable,
						"target_database_name": targetDBObj.Name,
						"target_database_id":   targetDBObj.ID,
						"type_compatible":      columnMatch.IsTypeCompatible,
						"table_match_score":    tableMatch.Score,
					}

					// Create empty transformation options
					transformationOptions := map[string]interface{}{}

					// Build proper resource URIs
					sourceURI := s.buildResourceURI("column", sourceDBObj.ID, tableMatch.SourceTable, columnMatch.SourceColumn)
					targetURI := s.buildResourceURI("column", targetDBObj.ID, tableMatch.TargetTable, columnMatch.TargetColumn)

					_, err = mappingService.CreateMappingRule(ctx, req.TenantId, workspaceID, ruleName,
						fmt.Sprintf("Auto-generated rule for %s.%s.%s -> %s.%s.%s",
							sourceDB, tableMatch.SourceTable, columnMatch.SourceColumn,
							targetDB, tableMatch.TargetTable, columnMatch.TargetColumn),
						sourceURI,
						targetURI,
						"direct_mapping", // Default transformation
						transformationOptions,
						metadata,
						req.OwnerId)

					if err != nil {
						s.engine.logger.Warnf("Failed to create mapping rule %s: %v", ruleName, err)
						continue
					}

					// Attach the mapping rule to the mapping
					err = mappingService.AttachMappingRule(ctx, req.TenantId, workspaceID, req.MappingName, ruleName, nil)
					if err != nil {
						s.engine.logger.Warnf("Failed to attach mapping rule %s to mapping: %v", ruleName, err)
					}
				}
			}

			// Log unmatched columns as warnings
			if len(matchResp.UnmatchedColumns) > 0 {
				s.engine.logger.Warnf("Found %d unmatched columns in database mapping %s", len(matchResp.UnmatchedColumns), req.MappingName)
				for _, unmatchedCol := range matchResp.UnmatchedColumns {
					s.engine.logger.Warnf("Unmatched column: %s.%s", unmatchedCol.SourceTable, unmatchedCol.SourceColumn)
				}
			}

			// Log overall warnings
			for _, warning := range matchResp.Warnings {
				s.engine.logger.Warnf("Matching warning: %s", warning)
			}
		}
	}

	// Refresh the mapping to get the updated mapping rule count
	updatedMapping, err := mappingService.Get(ctx, req.TenantId, workspaceID, req.MappingName)
	if err != nil {
		s.engine.logger.Warnf("Failed to refresh mapping data: %v", err)
		// Use the original mapping if refresh fails
		updatedMapping = createdMapping
	}

	// Convert to protobuf format
	protoMapping, err := s.mappingToProto(updatedMapping)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert mapping: %v", err)
	}

	return &corev1.AddMappingResponse{
		Message: "Database mapping created successfully",
		Success: true,
		Mapping: protoMapping,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// addMCPMapping creates a mapping from a database/table to an MCP resource
func (s *Server) addMCPMapping(ctx context.Context, req *corev1.AddMappingRequest, sourceDB, sourceTable, mcpResourceName string) (*corev1.AddMappingResponse, error) {
	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get database service to validate source database
	databaseService := database.NewService(s.engine.db, s.engine.logger)

	// Validate source database exists and belongs to the tenant/workspace
	sourceDBObj, err := databaseService.Get(ctx, req.TenantId, workspaceID, sourceDB)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "source database not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Build resource URIs and mapping type based on scope
	var sourceType, sourceIdentifier string
	var mappingObject map[string]interface{}

	if req.Scope == "table" && sourceTable != "" {
		sourceType = "table"
		sourceIdentifier = s.buildResourceURI("table", sourceDBObj.ID, sourceTable, "")
		mappingObject = map[string]interface{}{
			"source_database_name": sourceDBObj.Name,
			"source_database_id":   sourceDBObj.ID,
			"source_table_name":    sourceTable,
			"target_mcp_resource":  mcpResourceName,
		}
	} else {
		sourceType = "database"
		sourceIdentifier = s.buildResourceURI("database", sourceDBObj.ID, "", "")
		mappingObject = map[string]interface{}{
			"source_database_name": sourceDBObj.Name,
			"source_database_id":   sourceDBObj.ID,
			"target_mcp_resource":  mcpResourceName,
		}
	}

	targetType := "mcp-resource"
	targetIdentifier := s.buildMCPResourceURI(mcpResourceName)
	mappingType := s.buildMappingType(sourceType, targetType)

	// Create the mapping
	createdMapping, err := mappingService.Create(ctx, req.TenantId, workspaceID, mappingType, req.MappingName, req.MappingDescription, req.OwnerId,
		sourceType, targetType, sourceIdentifier, targetIdentifier, mappingObject)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create mapping: %v", err)
	}

	s.engine.logger.Infof("Created MCP mapping %s (ID: %s) from %s to %s", req.MappingName, createdMapping.ID, sourceIdentifier, targetIdentifier)

	// Auto-generate mapping rules for table-scope MCP mappings (only if generateRules is true)
	if req.GenerateRules && req.Scope == "table" && sourceTable != "" {
		err = s.autoGenerateMCPMappingRules(ctx, req.TenantId, workspaceID, createdMapping.ID, req.MappingName, sourceDB, sourceTable, mcpResourceName, req.OwnerId)
		if err != nil {
			s.engine.logger.Warnf("Failed to auto-generate mapping rules for MCP mapping: %v", err)
			// Don't fail the mapping creation, just log the warning
		}
	} else if req.Scope == "database" {
		s.engine.logger.Warnf("Database-scope MCP mappings are not supported for auto-rule generation. Please use table-scope mappings.")
	}

	// Convert to protobuf format
	protoMapping, err := s.mappingToProto(createdMapping)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert mapping: %v", err)
	}

	return &corev1.AddMappingResponse{
		Message: fmt.Sprintf("MCP mapping created successfully from %s to %s with %d auto-generated rules", sourceIdentifier, targetIdentifier, len(protoMapping.MappingRules)),
		Success: true,
		Mapping: protoMapping,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// autoGenerateMCPMappingRules creates mapping rules with direct_mapping for all columns in the source table
func (s *Server) autoGenerateMCPMappingRules(ctx context.Context, tenantID, workspaceID, mappingID, mappingName, sourceDatabaseName, sourceTableName, mcpResourceName, ownerID string) error {
	s.engine.logger.Infof("Auto-generating mapping rules for MCP mapping %s (source: %s.%s)", mappingName, sourceDatabaseName, sourceTableName)

	// Get database service
	databaseService := database.NewService(s.engine.db, s.engine.logger)

	// Get source database object to access schema
	sourceDBObj, err := databaseService.Get(ctx, tenantID, workspaceID, sourceDatabaseName)
	if err != nil {
		return fmt.Errorf("failed to get source database: %w", err)
	}

	// Convert database schema to UnifiedModel
	if sourceDBObj.Schema == "" {
		return fmt.Errorf("source database has no schema information")
	}

	sourceUM, err := s.convertDatabaseSchemaToUnifiedModel(sourceDBObj.Schema)
	if err != nil {
		return fmt.Errorf("failed to convert database schema: %w", err)
	}

	// Find the source table in the schema
	sourceTable, exists := sourceUM.Tables[sourceTableName]
	if !exists {
		return fmt.Errorf("table %s not found in source database schema", sourceTableName)
	}

	if len(sourceTable.Columns) == 0 {
		return fmt.Errorf("table %s has no columns", sourceTableName)
	}

	s.engine.logger.Infof("Found %d columns in table %s", len(sourceTable.Columns), sourceTableName)

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Generate virtual table name
	virtualTableName := fmt.Sprintf("mcp_virtual_%s", mappingName)

	// Create a mapping rule for each column
	ruleOrder := int32(0)
	rulesCreated := 0

	for columnName, column := range sourceTable.Columns {
		// Generate unique rule name
		baseRuleName := fmt.Sprintf("%s_%s_mcp_%s", sourceTableName, columnName, mcpResourceName)
		ruleName := baseRuleName

		// Check if rule name already exists and find a unique one
		counter := 1
		for {
			existingRule, err := mappingService.GetMappingRuleByName(ctx, tenantID, workspaceID, ruleName)
			if err != nil || existingRule == nil {
				break // Name is available
			}
			ruleName = fmt.Sprintf("%s_%d", baseRuleName, counter)
			counter++
		}

		// Build source and target URIs with correct format:
		// redb://data/database/{id}/table/{name}/column/{col}
		sourceURI := s.buildResourceURI("column", sourceDBObj.ID, sourceTableName, columnName)
		targetURI := fmt.Sprintf("mcp_virtual://%s.%s.%s", mappingName, virtualTableName, columnName)

		// Create metadata for the mapping rule (additional fields beyond source/target identifiers)
		metadata := map[string]interface{}{
			"source_table":         sourceTableName,
			"source_column":        columnName,
			"source_database_name": sourceDBObj.Name,
			"source_database_id":   sourceDBObj.ID,
			"target_table":         virtualTableName,
			"target_column":        columnName, // Same name by default
			"target_mcp_resource":  mcpResourceName,
			"match_type":           "auto_generated_mcp",
			"column_data_type":     column.DataType,
			"column_nullable":      column.Nullable,
			"is_primary_key":       column.IsPrimaryKey,
			"generated_at":         time.Now().UTC().Format(time.RFC3339),
		}

		// Create empty transformation options
		transformationOptions := map[string]interface{}{}

		// Create the mapping rule
		_, err := mappingService.CreateMappingRule(ctx, tenantID, workspaceID, ruleName, fmt.Sprintf("Auto-generated rule for %s.%s", sourceTableName, columnName), sourceURI, targetURI, "direct_mapping", transformationOptions, metadata, ownerID)
		if err != nil {
			s.engine.logger.Warnf("Failed to create mapping rule %s: %v", ruleName, err)
			continue
		}

		// Attach rule to mapping
		orderPtr := int64(ruleOrder)
		err = mappingService.AttachMappingRule(ctx, tenantID, workspaceID, mappingName, ruleName, &orderPtr)
		if err != nil {
			s.engine.logger.Warnf("Failed to attach rule %s to mapping: %v", ruleName, err)
			continue
		}

		s.engine.logger.Debugf("Created and attached mapping rule %s for column %s (order: %d)", ruleName, columnName, ruleOrder)
		ruleOrder++
		rulesCreated++
	}

	s.engine.logger.Infof("Auto-generated %d mapping rules for MCP mapping %s", rulesCreated, mappingName)

	if rulesCreated == 0 {
		return fmt.Errorf("failed to create any mapping rules")
	}

	return nil
}

// ValidateMapping validates a mapping
func (s *Server) ValidateMapping(ctx context.Context, req *corev1.ValidateMappingRequest) (*corev1.ValidateMappingResponse, error) {
	defer s.trackOperation()()

	// Validate input
	if req.TenantId == "" {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.InvalidArgument, "tenant_id is required")
	}
	if req.WorkspaceName == "" {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.InvalidArgument, "workspace_name is required")
	}
	if req.MappingName == "" {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.InvalidArgument, "mapping_name is required")
	}

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to get workspace: %v", err)
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Get mapping by name
	mappingObj, err := mappingService.GetByName(ctx, req.TenantId, workspaceID, req.MappingName)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to get mapping: %v", err)
		return nil, status.Errorf(codes.NotFound, "mapping not found: %v", err)
	}

	// Perform basic validation checks
	var errors []string
	var warnings []string
	isValid := true

	// Check if mapping has rules
	rules, err := mappingService.GetMappingRulesForMapping(ctx, req.TenantId, workspaceID, req.MappingName)
	if err != nil {
		s.engine.logger.Warnf("Failed to get mapping rules: %v", err)
		warnings = append(warnings, fmt.Sprintf("Could not verify mapping rules: %v", err))
	} else if len(rules) == 0 {
		warnings = append(warnings, "Mapping has no rules defined")
	}

	// Check if source and target are valid
	if mappingObj.SourceIdentifier == "" {
		errors = append(errors, "Mapping has no source identifier")
		isValid = false
	}
	if mappingObj.TargetIdentifier == "" {
		errors = append(errors, "Mapping has no target identifier")
		isValid = false
	}

	// Validate resource URIs by attempting to resolve them
	if mappingObj.SourceIdentifier != "" {
		_, err := mappingService.GetContainerByURI(ctx, mappingObj.SourceIdentifier)
		if err != nil {
			errors = append(errors, fmt.Sprintf("Invalid source resource URI: %v", err))
			isValid = false
		}
	}
	if mappingObj.TargetIdentifier != "" {
		_, err := mappingService.GetContainerByURI(ctx, mappingObj.TargetIdentifier)
		if err != nil {
			errors = append(errors, fmt.Sprintf("Invalid target resource URI: %v", err))
			isValid = false
		}
	}

	// Update validation status in database
	err = mappingService.UpdateValidationStatus(ctx, mappingObj.ID, isValid, errors, warnings)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to update validation status: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to update validation status: %v", err)
	}

	s.engine.logger.Infof("Mapping '%s' validated: valid=%v, errors=%d, warnings=%d", req.MappingName, isValid, len(errors), len(warnings))

	return &corev1.ValidateMappingResponse{
		IsValid:            isValid,
		ValidationErrors:   errors,
		ValidationWarnings: warnings,
		Status:             commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// AddStreamToTableMapping creates a mapping from a stream topic to a database table
func (s *Server) AddStreamToTableMapping(ctx context.Context, req *corev1.AddStreamToTableMappingRequest) (*corev1.AddMappingResponse, error) {
	defer s.trackOperation()()

	s.engine.logger.Infof("AddStreamToTableMapping request received for tenant: %s, workspace: %s, mapping: %s", req.TenantId, req.WorkspaceName, req.MappingName)

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Build stream URI for source
	sourceURI := mapping.BuildStreamURI(workspaceID, "stream", req.SourceIntegrationName, req.SourceTopicName)

	// Validate stream container exists
	sourceContainer, err := mappingService.GetStreamContainerByTopic(ctx, workspaceID, req.SourceIntegrationName, req.SourceTopicName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "source stream container not found: %v", err)
	}

	// Get database service to validate target database
	databaseService := database.NewService(s.engine.db, s.engine.logger)

	// Validate target database exists
	targetDB, err := databaseService.Get(ctx, req.TenantId, workspaceID, req.TargetDatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "target database not found: %v", err)
	}

	// Build target URI
	targetURI := s.buildResourceURI("table", targetDB.ID, req.TargetTableName, "")

	// Build mapping object
	mappingObject := map[string]interface{}{
		"source_integration_name": req.SourceIntegrationName,
		"source_topic_name":       req.SourceTopicName,
		"source_integration_id":   *sourceContainer.IntegrationID,
		"target_database_name":    targetDB.Name,
		"target_database_id":      targetDB.ID,
		"target_table_name":       req.TargetTableName,
	}

	// Create the mapping
	createdMapping, err := mappingService.Create(ctx, req.TenantId, workspaceID, "stream_to_table", req.MappingName, req.MappingDescription, req.OwnerId,
		"stream", "table", sourceURI, targetURI, mappingObject)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create mapping: %v", err)
	}

	// Store filters if provided
	if len(req.Filters) > 0 {
		for _, filter := range req.Filters {
			filterExpr := map[string]interface{}{}
			if filter.FilterExpression != nil {
				filterExpr = filter.FilterExpression.AsMap()
			}

			err := mappingService.CreateMappingFilter(ctx, createdMapping.ID, filter.FilterType, filterExpr, int(filter.FilterOrder), filter.FilterOperator)
			if err != nil {
				s.engine.logger.Warnf("Failed to create filter for mapping: %v", err)
			}
		}
	}

	// Get unified model client for automatic rule generation
	umClient := s.engine.GetUnifiedModelClient()
	if umClient != nil {
		// Get stream items (message fields)
		sourceItems, err := mappingService.GetItemsForContainer(ctx, sourceContainer.ContainerID)
		if err == nil && len(sourceItems) > 0 {
			// Get target table items (columns)
			targetContainer, err := mappingService.GetContainerByURI(ctx, targetURI)
			if err == nil {
				targetItems, err := mappingService.GetItemsForContainer(ctx, targetContainer.ContainerID)
				if err == nil && len(targetItems) > 0 {
					// Auto-generate mapping rules
					err = s.autoGenerateStreamMappingRules(ctx, req.TenantId, workspaceID, req.MappingName, req.OwnerId, sourceItems, targetItems)
					if err != nil {
						s.engine.logger.Warnf("Failed to auto-generate mapping rules: %v", err)
					}
				}
			}
		}
	}

	s.engine.logger.Infof("Successfully created stream-to-table mapping: %s", createdMapping.Name)

	return &corev1.AddMappingResponse{
		Message: "Stream-to-table mapping created successfully",
		Success: true,
		Mapping: &corev1.Mapping{
			TenantId:           createdMapping.TenantID,
			WorkspaceId:        createdMapping.WorkspaceID,
			MappingId:          createdMapping.ID,
			MappingName:        createdMapping.Name,
			MappingDescription: createdMapping.Description,
			MappingType:        createdMapping.MappingType,
			OwnerId:            createdMapping.OwnerID,
		},
		Status: commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// AddTableToStreamMapping creates a mapping from a database table to a stream topic
func (s *Server) AddTableToStreamMapping(ctx context.Context, req *corev1.AddTableToStreamMappingRequest) (*corev1.AddMappingResponse, error) {
	defer s.trackOperation()()

	s.engine.logger.Infof("AddTableToStreamMapping request received for tenant: %s, workspace: %s, mapping: %s", req.TenantId, req.WorkspaceName, req.MappingName)

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get database service
	databaseService := database.NewService(s.engine.db, s.engine.logger)

	// Validate source database exists
	sourceDB, err := databaseService.Get(ctx, req.TenantId, workspaceID, req.SourceDatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "source database not found: %v", err)
	}

	// Build source URI
	sourceURI := s.buildResourceURI("table", sourceDB.ID, req.SourceTableName, "")

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Build and validate target stream URI
	targetURI := mapping.BuildStreamURI(workspaceID, "stream", req.TargetIntegrationName, req.TargetTopicName)

	// Validate target stream container exists
	targetContainer, err := mappingService.GetStreamContainerByTopic(ctx, workspaceID, req.TargetIntegrationName, req.TargetTopicName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "target stream container not found: %v", err)
	}

	// Build mapping object
	mappingObject := map[string]interface{}{
		"source_database_name":    sourceDB.Name,
		"source_database_id":      sourceDB.ID,
		"source_table_name":       req.SourceTableName,
		"target_integration_name": req.TargetIntegrationName,
		"target_topic_name":       req.TargetTopicName,
		"target_integration_id":   *targetContainer.IntegrationID,
	}

	// Create the mapping
	createdMapping, err := mappingService.Create(ctx, req.TenantId, workspaceID, "table_to_stream", req.MappingName, req.MappingDescription, req.OwnerId,
		"table", "stream", sourceURI, targetURI, mappingObject)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create mapping: %v", err)
	}

	// Store filters if provided
	if len(req.Filters) > 0 {
		for _, filter := range req.Filters {
			filterExpr := map[string]interface{}{}
			if filter.FilterExpression != nil {
				filterExpr = filter.FilterExpression.AsMap()
			}

			err := mappingService.CreateMappingFilter(ctx, createdMapping.ID, filter.FilterType, filterExpr, int(filter.FilterOrder), filter.FilterOperator)
			if err != nil {
				s.engine.logger.Warnf("Failed to create filter for mapping: %v", err)
			}
		}
	}

	s.engine.logger.Infof("Successfully created table-to-stream mapping: %s", createdMapping.Name)

	return &corev1.AddMappingResponse{
		Message: "Table-to-stream mapping created successfully",
		Success: true,
		Mapping: &corev1.Mapping{
			TenantId:           createdMapping.TenantID,
			WorkspaceId:        createdMapping.WorkspaceID,
			MappingId:          createdMapping.ID,
			MappingName:        createdMapping.Name,
			MappingDescription: createdMapping.Description,
			MappingType:        createdMapping.MappingType,
			OwnerId:            createdMapping.OwnerID,
		},
		Status: commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// AddStreamToStreamMapping creates a mapping from one stream topic to another
func (s *Server) AddStreamToStreamMapping(ctx context.Context, req *corev1.AddStreamToStreamMappingRequest) (*corev1.AddMappingResponse, error) {
	defer s.trackOperation()()

	s.engine.logger.Infof("AddStreamToStreamMapping request received for tenant: %s, workspace: %s, mapping: %s", req.TenantId, req.WorkspaceName, req.MappingName)

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Build and validate source stream URI
	sourceURI := mapping.BuildStreamURI(workspaceID, "stream", req.SourceIntegrationName, req.SourceTopicName)
	sourceContainer, err := mappingService.GetStreamContainerByTopic(ctx, workspaceID, req.SourceIntegrationName, req.SourceTopicName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "source stream container not found: %v", err)
	}

	// Build and validate target stream URI
	targetURI := mapping.BuildStreamURI(workspaceID, "stream", req.TargetIntegrationName, req.TargetTopicName)
	targetContainer, err := mappingService.GetStreamContainerByTopic(ctx, workspaceID, req.TargetIntegrationName, req.TargetTopicName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "target stream container not found: %v", err)
	}

	// Build mapping object
	mappingObject := map[string]interface{}{
		"source_integration_name": req.SourceIntegrationName,
		"source_topic_name":       req.SourceTopicName,
		"source_integration_id":   *sourceContainer.IntegrationID,
		"target_integration_name": req.TargetIntegrationName,
		"target_topic_name":       req.TargetTopicName,
		"target_integration_id":   *targetContainer.IntegrationID,
	}

	// Create the mapping
	createdMapping, err := mappingService.Create(ctx, req.TenantId, workspaceID, "stream_to_stream", req.MappingName, req.MappingDescription, req.OwnerId,
		"stream", "stream", sourceURI, targetURI, mappingObject)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create mapping: %v", err)
	}

	// Store filters if provided
	if len(req.Filters) > 0 {
		for _, filter := range req.Filters {
			filterExpr := map[string]interface{}{}
			if filter.FilterExpression != nil {
				filterExpr = filter.FilterExpression.AsMap()
			}

			err := mappingService.CreateMappingFilter(ctx, createdMapping.ID, filter.FilterType, filterExpr, int(filter.FilterOrder), filter.FilterOperator)
			if err != nil {
				s.engine.logger.Warnf("Failed to create filter for mapping: %v", err)
			}
		}
	}

	s.engine.logger.Infof("Successfully created stream-to-stream mapping: %s", createdMapping.Name)

	return &corev1.AddMappingResponse{
		Message: "Stream-to-stream mapping created successfully",
		Success: true,
		Mapping: &corev1.Mapping{
			TenantId:           createdMapping.TenantID,
			WorkspaceId:        createdMapping.WorkspaceID,
			MappingId:          createdMapping.ID,
			MappingName:        createdMapping.Name,
			MappingDescription: createdMapping.Description,
			MappingType:        createdMapping.MappingType,
			OwnerId:            createdMapping.OwnerID,
		},
		Status: commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// autoGenerateStreamMappingRules generates mapping rules for stream-to-table mappings
func (s *Server) autoGenerateStreamMappingRules(ctx context.Context, tenantID, workspaceID, mappingName, ownerID string, sourceItems, targetItems []*mapping.ResourceItem) error {
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	s.engine.logger.Infof("Auto-generating mapping rules for stream mapping %s: %d source fields, %d target columns", mappingName, len(sourceItems), len(targetItems))

	// Create a map of target items by name for quick lookup
	targetItemMap := make(map[string]*mapping.ResourceItem)
	for _, item := range targetItems {
		targetItemMap[strings.ToLower(item.ItemName)] = item
	}

	rulesCreated := 0
	ruleOrder := 0

	// For each source item (message field), try to find a matching target item (column)
	for _, sourceItem := range sourceItems {
		sourceName := strings.ToLower(sourceItem.ItemName)
		targetItem, found := targetItemMap[sourceName]

		if !found {
			// Try without underscores or with common variations
			sourceName = strings.ReplaceAll(sourceName, "_", "")
			for targetName, item := range targetItemMap {
				if strings.ReplaceAll(targetName, "_", "") == sourceName {
					targetItem = item
					found = true
					break
				}
			}
		}

		if !found {
			s.engine.logger.Debugf("No matching target column for source field: %s", sourceItem.ItemName)
			continue
		}

		// Generate rule name
		ruleName := fmt.Sprintf("%s_to_%s", sourceItem.ItemName, targetItem.ItemName)

		// Build metadata
		metadata := map[string]interface{}{
			"source_field":  sourceItem.ItemName,
			"target_column": targetItem.ItemName,
			"match_type":    "auto_generated_stream",
			"generated_at":  time.Now().Format(time.RFC3339),
		}

		// Create mapping rule
		_, err := mappingService.CreateMappingRule(ctx, tenantID, workspaceID, ruleName, fmt.Sprintf("Auto-generated rule for %s -> %s", sourceItem.ItemName, targetItem.ItemName), sourceItem.ResourceURI, targetItem.ResourceURI, "direct_mapping", map[string]interface{}{}, metadata, ownerID)
		if err != nil {
			s.engine.logger.Warnf("Failed to create mapping rule %s: %v", ruleName, err)
			continue
		}

		// Attach rule to mapping
		orderPtr := int64(ruleOrder)
		err = mappingService.AttachMappingRule(ctx, tenantID, workspaceID, mappingName, ruleName, &orderPtr)
		if err != nil {
			s.engine.logger.Warnf("Failed to attach rule %s to mapping: %v", ruleName, err)
			continue
		}

		s.engine.logger.Debugf("Created and attached mapping rule %s (order: %d)", ruleName, ruleOrder)
		ruleOrder++
		rulesCreated++
	}

	s.engine.logger.Infof("Auto-generated %d mapping rules for stream mapping %s", rulesCreated, mappingName)

	if rulesCreated == 0 {
		return fmt.Errorf("failed to create any mapping rules")
	}

	return nil
}

// extractTableWithTypes extracts a single table and all its dependent user-defined types from a UnifiedModel
func (s *Server) extractTableWithTypes(um *unifiedmodel.UnifiedModel, tableName string) (*unifiedmodel.UnifiedModel, []string) {
	if um == nil {
		return nil, nil
	}

	// Get the table
	table, exists := um.Tables[tableName]
	if !exists {
		s.engine.logger.Warnf("Table %s not found in UnifiedModel", tableName)
		return nil, nil
	}

	// Create a new UnifiedModel with just this table
	filtered := &unifiedmodel.UnifiedModel{
		DatabaseType: um.DatabaseType,
		Tables:       map[string]unifiedmodel.Table{tableName: table},
		Types:        make(map[string]unifiedmodel.Type),
		Schemas:      make(map[string]unifiedmodel.Schema),
		Functions:    make(map[string]unifiedmodel.Function),
		Procedures:   make(map[string]unifiedmodel.Procedure),
		Triggers:     make(map[string]unifiedmodel.Trigger),
		Sequences:    make(map[string]unifiedmodel.Sequence),
		Views:        make(map[string]unifiedmodel.View),
		Extensions:   make(map[string]unifiedmodel.Extension),
	}

	// Collect all custom types referenced by the table columns
	typesReferenced := make(map[string]bool)
	deployedTypeNames := []string{}

	for _, column := range table.Columns {
		// Check if the data type is a user-defined type
		if s.isCustomType(column.DataType, um) {
			typeName := s.extractTypeName(column.DataType)
			if typeName != "" && !typesReferenced[typeName] {
				typesReferenced[typeName] = true
				// Add the type to the filtered model
				if typeDefinition, exists := um.Types[typeName]; exists {
					filtered.Types[typeName] = typeDefinition
					deployedTypeNames = append(deployedTypeNames, typeName)
					s.engine.logger.Infof("Including custom type '%s' for table '%s'", typeName, tableName)
				}
			}
		}
	}

	return filtered, deployedTypeNames
}

// isCustomType checks if a data type is a user-defined type
func (s *Server) isCustomType(dataType string, um *unifiedmodel.UnifiedModel) bool {
	// Remove array notation if present
	baseType := strings.TrimSuffix(dataType, "[]")

	// Check if it exists in the Types map
	if _, exists := um.Types[baseType]; exists {
		return true
	}

	// Check for PostgreSQL user-defined types (sometimes prefixed with schema)
	if strings.Contains(baseType, ".") {
		parts := strings.Split(baseType, ".")
		if len(parts) == 2 {
			if _, exists := um.Types[parts[1]]; exists {
				return true
			}
		}
	}

	return false
}

// extractTypeName extracts the base type name from a data type string
func (s *Server) extractTypeName(dataType string) string {
	// Remove array notation
	baseType := strings.TrimSuffix(dataType, "[]")

	// Handle schema-qualified types (e.g., public.my_enum)
	if strings.Contains(baseType, ".") {
		parts := strings.Split(baseType, ".")
		if len(parts) == 2 {
			return parts[1]
		}
	}

	return baseType
}

// checkTableExists checks if a table exists in the target database via anchor service
func (s *Server) checkTableExists(ctx context.Context, databaseID, tableName string) (bool, error) {
	// Get anchor client
	anchorClient := s.engine.GetAnchorClient()
	if anchorClient == nil {
		return false, fmt.Errorf("anchor service not available")
	}

	// Get database schema
	schemaReq := &anchorv1.GetDatabaseSchemaRequest{
		DatabaseId: databaseID,
	}

	schemaResp, err := anchorClient.GetDatabaseSchema(ctx, schemaReq)
	if err != nil {
		return false, fmt.Errorf("failed to get database schema: %w", err)
	}

	if !schemaResp.Success {
		return false, fmt.Errorf("failed to get database schema: %s", schemaResp.Message)
	}

	// Parse the schema
	var schema unifiedmodel.UnifiedModel
	if err := json.Unmarshal(schemaResp.Schema, &schema); err != nil {
		return false, fmt.Errorf("failed to parse database schema: %w", err)
	}

	// Check if table exists
	_, exists := schema.Tables[tableName]
	return exists, nil
}

// refreshDatabaseDiscovery triggers an immediate schema discovery for the specified database
// to populate resource_containers and resource_items tables
func (s *Server) refreshDatabaseDiscovery(ctx context.Context, databaseID string) error {
	// Get anchor client
	anchorClient := s.engine.GetAnchorClient()
	if anchorClient == nil {
		return fmt.Errorf("anchor service not available")
	}

	// Call RefreshDatabaseDiscovery RPC
	refreshReq := &anchorv1.RefreshDatabaseDiscoveryRequest{
		DatabaseId: databaseID,
	}

	refreshResp, err := anchorClient.RefreshDatabaseDiscovery(ctx, refreshReq)
	if err != nil {
		return fmt.Errorf("failed to refresh database discovery: %w", err)
	}

	if !refreshResp.Success {
		return fmt.Errorf("failed to refresh database discovery: %s", refreshResp.Message)
	}

	s.engine.logger.Infof("Refreshed discovery for database %s: %d containers, %d items created",
		databaseID, refreshResp.ContainersCreated, refreshResp.ItemsCreated)

	return nil
}
