package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	transformationv1 "github.com/redbco/redb-open/api/proto/transformation/v1"
	unifiedmodelv1 "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
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
		protoMapping, err := s.mappingToProto(m)
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

	// Get mapping rules for this mapping
	mappingRules, err := mappingService.GetMappingRulesForMapping(ctx, req.TenantId, workspaceID, req.MappingName)
	if err != nil {
		s.engine.logger.Warnf("Failed to get mapping rules for mapping %s: %v", req.MappingName, err)
		mappingRules = []*mapping.Rule{}
	}

	// Convert to protobuf format
	protoMapping, err := s.mappingToProto(m)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert mapping: %v", err)
	}

	// Convert mapping rules to protobuf format
	protoMappingRules := make([]*corev1.MappingRule, len(mappingRules))
	for i, rule := range mappingRules {
		protoRule, err := s.mappingRuleToProto(rule)
		if err != nil {
			s.engine.logger.Warnf("Failed to convert mapping rule: %v", err)
			continue
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
		return s.addDatabaseMappingUnified(ctx, req, sourceDB, targetDB)
	case "table":
		return s.addTableMappingUnified(ctx, req, sourceDB, sourceTable, targetDB, targetTable)
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

	// Create the mapping
	createdMapping, err := mappingService.Create(ctx, req.TenantId, workspaceID, "table", req.MappingName, req.MappingDescription, req.OwnerId)
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
							"source_table":    tableMatch.SourceTable,
							"source_column":   columnMatch.SourceColumn,
							"target_table":    tableMatch.TargetTable,
							"target_column":   columnMatch.TargetColumn,
							"match_score":     columnMatch.Score,
							"type_compatible": columnMatch.IsTypeCompatible,
							"match_type":      "auto_generated",
							"generated_at":    time.Now().Format(time.RFC3339),
						}

						// Create empty transformation options (as requested)
						transformationOptions := map[string]interface{}{}

						// Create the mapping rule
						_, err = mappingService.CreateMappingRule(ctx, req.TenantId, workspaceID, ruleName,
							fmt.Sprintf("Auto-generated rule for %s.%s.%s -> %s.%s.%s",
								req.MappingSourceDatabaseName, tableMatch.SourceTable, columnMatch.SourceColumn,
								req.MappingTargetDatabaseName, tableMatch.TargetTable, columnMatch.TargetColumn),
							fmt.Sprintf("db://%s.%s.%s", sourceDB.ID, tableMatch.SourceTable, columnMatch.SourceColumn),
							fmt.Sprintf("db://%s.%s.%s", targetDB.ID, tableMatch.TargetTable, columnMatch.TargetColumn),
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

	// Create the mapping
	createdMapping, err := mappingService.Create(ctx, req.TenantId, workspaceID, "database", req.MappingName, req.MappingDescription, req.OwnerId)
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
							"source_table":    tableMatch.SourceTable,
							"source_column":   columnMatch.SourceColumn,
							"target_table":    tableMatch.TargetTable,
							"target_column":   columnMatch.TargetColumn,
							"match_score":     columnMatch.Score,
							"type_compatible": columnMatch.IsTypeCompatible,
							"match_type":      "auto_generated",
							"generated_at":    time.Now().Format(time.RFC3339),
						}

						// Create empty transformation options (as requested)
						transformationOptions := map[string]interface{}{}

						// Create the mapping rule
						_, err = mappingService.CreateMappingRule(ctx, req.TenantId, workspaceID, ruleName,
							fmt.Sprintf("Auto-generated rule for %s.%s.%s -> %s.%s.%s",
								req.MappingSourceDatabaseName, tableMatch.SourceTable, columnMatch.SourceColumn,
								req.MappingTargetDatabaseName, tableMatch.TargetTable, columnMatch.TargetColumn),
							fmt.Sprintf("db://%s.%s.%s", sourceDB.ID, tableMatch.SourceTable, columnMatch.SourceColumn),
							fmt.Sprintf("db://%s.%s.%s", targetDB.ID, tableMatch.TargetTable, columnMatch.TargetColumn),
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

	// Create the mapping
	createdMapping, err := mappingService.Create(ctx, req.TenantId, workspaceID, "undefined", req.MappingName, req.MappingDescription, req.OwnerId)
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

	// Delete the mapping
	err = mappingService.Delete(ctx, req.TenantId, workspaceID, req.MappingName)
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

	// Validate transformation if provided
	if req.MappingRuleTransformationName != "" {
		transformationName := req.MappingRuleTransformationName
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

				// Validate based on transformation type
				switch transformationType {
				case "generator":
					// Generator transformations should not have a source
					if req.MappingRuleSource != "" {
						s.engine.IncrementErrors()
						return nil, status.Errorf(codes.InvalidArgument,
							"transformation '%s' is a generator type and should not have a source column", transformationName)
					}
					// Generator transformations must have a target
					if req.MappingRuleTarget == "" {
						s.engine.IncrementErrors()
						return nil, status.Errorf(codes.InvalidArgument,
							"transformation '%s' is a generator type and requires a target column", transformationName)
					}
				case "null_returning":
					// Null-returning transformations should not have a target
					if req.MappingRuleTarget != "" {
						s.engine.IncrementErrors()
						return nil, status.Errorf(codes.InvalidArgument,
							"transformation '%s' is a null-returning type and should not have a target column", transformationName)
					}
					// Null-returning transformations must have a source
					if req.MappingRuleSource == "" {
						s.engine.IncrementErrors()
						return nil, status.Errorf(codes.InvalidArgument,
							"transformation '%s' is a null-returning type and requires a source column", transformationName)
					}
				case "passthrough":
					// Passthrough transformations require both source and target
					if req.MappingRuleSource == "" {
						s.engine.IncrementErrors()
						return nil, status.Errorf(codes.InvalidArgument,
							"transformation '%s' is a passthrough type and requires a source column", transformationName)
					}
					if req.MappingRuleTarget == "" {
						s.engine.IncrementErrors()
						return nil, status.Errorf(codes.InvalidArgument,
							"transformation '%s' is a passthrough type and requires a target column", transformationName)
					}
				}

				s.engine.logger.Infof("Transformation '%s' validated successfully (type: %s)", transformationName, transformationType)
			}
		}
	}

	// Create the mapping rule
	createdRule, err := mappingService.CreateMappingRule(ctx, req.TenantId, workspaceID, req.MappingRuleName, req.MappingRuleDescription, req.MappingRuleSource, req.MappingRuleTarget, req.MappingRuleTransformationName, transformationOptions, metadata, req.OwnerId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create mapping rule: %v", err)
	}

	// Convert to protobuf format
	protoRule, err := s.mappingRuleToProto(createdRule)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert mapping rule: %v", err)
	}

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
				finalSource := existingRule.SourceIdentifier
				if req.MappingRuleSource != nil && *req.MappingRuleSource != "" {
					finalSource = *req.MappingRuleSource
				}

				finalTarget := existingRule.TargetIdentifier
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

	// Validate source column if being changed
	if req.MappingRuleSource != nil && *req.MappingRuleSource != "" {
		// Parse source to extract database, table, and column
		sourceParts := strings.Split(*req.MappingRuleSource, ".")
		if len(sourceParts) == 3 {
			databaseName := sourceParts[0]
			tableName := sourceParts[1]
			columnName := sourceParts[2]

			// Validate column exists
			validationResult, err := mappingService.ValidateSourceColumn(ctx, databaseName, tableName, columnName, workspaceID)
			if err != nil {
				s.engine.IncrementErrors()
				return nil, status.Errorf(codes.Internal, "failed to validate source column: %v", err)
			}
			if !validationResult.Valid {
				s.engine.IncrementErrors()
				return nil, status.Errorf(codes.InvalidArgument, "source column validation failed: %s", validationResult.Message)
			}
		}
	}

	// Validate target column if being changed
	if req.MappingRuleTarget != nil && *req.MappingRuleTarget != "" {
		// Parse target to extract database, table, and column
		targetParts := strings.Split(*req.MappingRuleTarget, ".")
		if len(targetParts) == 3 {
			databaseName := targetParts[0]
			tableName := targetParts[1]
			columnName := targetParts[2]

			// Validate column exists
			validationResult, err := mappingService.ValidateTargetColumn(ctx, databaseName, tableName, columnName, workspaceID)
			if err != nil {
				s.engine.IncrementErrors()
				return nil, status.Errorf(codes.Internal, "failed to validate target column: %v", err)
			}
			if !validationResult.Valid {
				s.engine.IncrementErrors()
				return nil, status.Errorf(codes.InvalidArgument, "target column validation failed: %s", validationResult.Message)
			}
		}
	}

	// Build update map
	updates := make(map[string]interface{})
	if req.MappingRuleNameNew != nil {
		updates["mapping_rule_name"] = *req.MappingRuleNameNew
	}
	if req.MappingRuleDescription != nil {
		updates["mapping_rule_description"] = *req.MappingRuleDescription
	}
	if req.MappingRuleSource != nil {
		updates["mapping_rule_source"] = *req.MappingRuleSource
	}
	if req.MappingRuleTarget != nil {
		updates["mapping_rule_target"] = *req.MappingRuleTarget
	}
	if req.MappingRuleTransformationName != nil {
		updates["mapping_rule_transformation_name"] = *req.MappingRuleTransformationName
	}
	if req.MappingRuleTransformationOptions != nil {
		updates["mapping_rule_transformation_options"] = *req.MappingRuleTransformationOptions
	}
	if req.MappingRuleMetadata != nil {
		var metadata map[string]interface{}
		if err := json.Unmarshal([]byte(*req.MappingRuleMetadata), &metadata); err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.InvalidArgument, "failed to unmarshal metadata: %v", err)
		}
		updates["mapping_rule_metadata"] = metadata
	}

	// Update the mapping rule
	updatedRule, err := mappingService.ModifyMappingRule(ctx, req.TenantId, workspaceID, req.MappingRuleName, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update mapping rule: %v", err)
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

	// Delete the mapping rule
	err = mappingService.DeleteMappingRule(ctx, req.TenantId, workspaceID, req.MappingRuleName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to delete mapping rule: %v", err)
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

// parseSourceTarget parses database[.table] format
func (s *Server) parseSourceTarget(input string) (database, table string, err error) {
	if input == "" {
		return "", "", fmt.Errorf("source/target cannot be empty")
	}

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

// addTableMappingUnified handles table-scoped mapping creation from unified request
func (s *Server) addTableMappingUnified(ctx context.Context, req *corev1.AddMappingRequest, sourceDB, sourceTable, targetDB, targetTable string) (*corev1.AddMappingResponse, error) {
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
	return s.AddTableMapping(ctx, legacyReq)
}

// addDatabaseMappingUnified handles database-scoped mapping creation from unified request with enhanced matching
func (s *Server) addDatabaseMappingUnified(ctx context.Context, req *corev1.AddMappingRequest, sourceDB, targetDB string) (*corev1.AddMappingResponse, error) {
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

	// Create the mapping
	createdMapping, err := mappingService.Create(ctx, req.TenantId, workspaceID, "database", req.MappingName, req.MappingDescription, req.OwnerId)
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

	// Perform enhanced database-to-database matching
	if sourceUM != nil && targetUM != nil {
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
						"generated_at":      time.Now().UTC().Format(time.RFC3339),
						"match_score":       columnMatch.Score,
						"match_type":        "enriched_match",
						"source_column":     columnMatch.SourceColumn,
						"source_table":      tableMatch.SourceTable,
						"target_column":     columnMatch.TargetColumn,
						"target_table":      tableMatch.TargetTable,
						"type_compatible":   columnMatch.IsTypeCompatible,
						"table_match_score": tableMatch.Score,
					}

					// Create empty transformation options
					transformationOptions := map[string]interface{}{}

					// Create the mapping rule
					_, err = mappingService.CreateMappingRule(ctx, req.TenantId, workspaceID, ruleName,
						fmt.Sprintf("Auto-generated rule for %s.%s.%s -> %s.%s.%s",
							sourceDB, tableMatch.SourceTable, columnMatch.SourceColumn,
							targetDB, tableMatch.TargetTable, columnMatch.TargetColumn),
						fmt.Sprintf("db://%s.%s.%s", sourceDBObj.ID, tableMatch.SourceTable, columnMatch.SourceColumn),
						fmt.Sprintf("db://%s.%s.%s", targetDBObj.ID, tableMatch.TargetTable, columnMatch.TargetColumn),
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
	_, err = databaseService.Get(ctx, req.TenantId, workspaceID, sourceDB)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "source database not found: %v", err)
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Build source identifier
	var sourceIdentifier string
	if req.Scope == "table" && sourceTable != "" {
		sourceIdentifier = fmt.Sprintf("%s.%s", sourceDB, sourceTable)
	} else {
		sourceIdentifier = sourceDB
	}

	// Create the mapping with MCP target type
	// Store mapping with source and target identifiers
	var query string
	var args []interface{}

	if req.PolicyId != nil && *req.PolicyId != "" {
		query = `
			INSERT INTO mappings (
				tenant_id, workspace_id, mapping_name, mapping_description,
				mapping_source_type, mapping_target_type,
				mapping_source_identifier, mapping_target_identifier,
				mapping_type, policy_ids, owner_id
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			RETURNING mapping_id, created, updated
		`
		args = []interface{}{
			req.TenantId,
			workspaceID,
			req.MappingName,
			req.MappingDescription,
			req.Scope,               // mapping_source_type
			"mcp",                   // mapping_target_type
			sourceIdentifier,        // mapping_source_identifier
			mcpResourceName,         // mapping_target_identifier
			req.Scope,               // mapping_type
			[]string{*req.PolicyId}, // policy_ids as array
			req.OwnerId,
		}
	} else {
		query = `
			INSERT INTO mappings (
				tenant_id, workspace_id, mapping_name, mapping_description,
				mapping_source_type, mapping_target_type,
				mapping_source_identifier, mapping_target_identifier,
				mapping_type, owner_id
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			RETURNING mapping_id, created, updated
		`
		args = []interface{}{
			req.TenantId,
			workspaceID,
			req.MappingName,
			req.MappingDescription,
			req.Scope,        // mapping_source_type
			"mcp",            // mapping_target_type
			sourceIdentifier, // mapping_source_identifier
			mcpResourceName,  // mapping_target_identifier
			req.Scope,        // mapping_type
			req.OwnerId,
		}
	}

	var mappingID string
	var created, updated time.Time
	err = s.engine.db.Pool().QueryRow(ctx, query, args...).Scan(&mappingID, &created, &updated)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to create MCP mapping: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to create MCP mapping: %v", err)
	}

	s.engine.logger.Infof("Created MCP mapping %s (ID: %s) from %s to mcp://%s", req.MappingName, mappingID, sourceIdentifier, mcpResourceName)

	// Get the created mapping
	createdMapping, err := mappingService.Get(ctx, req.TenantId, workspaceID, req.MappingName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to retrieve created mapping: %v", err)
	}

	// Convert to protobuf format
	protoMapping, err := s.mappingToProto(createdMapping)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert mapping: %v", err)
	}

	return &corev1.AddMappingResponse{
		Message: fmt.Sprintf("MCP mapping created successfully from %s to mcp://%s", sourceIdentifier, mcpResourceName),
		Success: true,
		Mapping: protoMapping,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}
