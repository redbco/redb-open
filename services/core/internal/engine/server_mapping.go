package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	unifiedmodelv1 "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
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

	// Prepare source and target table metadata for matching
	var sourceTables []*unifiedmodelv1.EnrichedTableMetadata
	var targetTables []*unifiedmodelv1.EnrichedTableMetadata

	// Parse source database enriched tables and find the specific source table
	if sourceDB.Tables != "" {
		var sourceEnrichedData map[string]interface{}
		if err := json.Unmarshal([]byte(sourceDB.Tables), &sourceEnrichedData); err != nil {
			s.engine.logger.Warnf("Failed to parse source database enriched tables: %v", err)
		} else {
			allSourceTables := s.convertEnrichedDataToTableMetadata(sourceEnrichedData)
			// Filter to only include the requested source table
			for _, table := range allSourceTables {
				if table.Name == req.MappingSourceTableName {
					sourceTables = append(sourceTables, table)
					s.engine.logger.Infof("Found source table: %s with %d columns", table.Name, len(table.Columns))
					break
				}
			}
			if len(sourceTables) == 0 {
				s.engine.logger.Warnf("Source table %s not found in database %s", req.MappingSourceTableName, req.MappingSourceDatabaseName)
			}
		}
	}

	// Parse target database enriched tables and find the specific target table
	if targetDB.Tables != "" {
		var targetEnrichedData map[string]interface{}
		if err := json.Unmarshal([]byte(targetDB.Tables), &targetEnrichedData); err != nil {
			s.engine.logger.Warnf("Failed to parse target database enriched tables: %v", err)
		} else {
			allTargetTables := s.convertEnrichedDataToTableMetadata(targetEnrichedData)
			// Filter to only include the requested target table
			for _, table := range allTargetTables {
				if table.Name == req.MappingTargetTableName {
					targetTables = append(targetTables, table)
					s.engine.logger.Infof("Found target table: %s with %d columns", table.Name, len(table.Columns))
					break
				}
			}
			if len(targetTables) == 0 {
				s.engine.logger.Warnf("Target table %s not found in database %s", req.MappingTargetTableName, req.MappingTargetDatabaseName)
			}
		}
	}

	// Use unified model service to match tables
	if len(sourceTables) > 0 && len(targetTables) > 0 {
		matchReq := &unifiedmodelv1.MatchTablesEnrichedRequest{
			SourceTables: sourceTables,
			TargetTables: targetTables,
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

		s.engine.logger.Infof("Calling MatchTablesEnriched with source table %s and target table %s", req.MappingSourceTableName, req.MappingTargetTableName)

		matchResp, err := umClient.MatchTablesEnriched(ctx, matchReq)
		s.engine.logger.Infof("Match response: %v", matchResp)
		if err != nil {
			s.engine.logger.Warnf("Failed to match tables using unified model service: %v", err)
		} else {
			// Create mapping rules for matched columns
			s.engine.logger.Infof("Creating mapping rules for matched columns: %v", matchResp.Matches)
			for _, tableMatch := range matchResp.Matches {
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

	// Prepare source and target table metadata for matching
	var sourceTables []*unifiedmodelv1.EnrichedTableMetadata
	var targetTables []*unifiedmodelv1.EnrichedTableMetadata

	// Parse source database enriched tables
	if sourceDB.Tables != "" {
		var sourceEnrichedData map[string]interface{}
		if err := json.Unmarshal([]byte(sourceDB.Tables), &sourceEnrichedData); err != nil {
			s.engine.logger.Warnf("Failed to parse source database enriched tables: %v", err)
		} else {
			sourceTables = s.convertEnrichedDataToTableMetadata(sourceEnrichedData)
			s.engine.logger.Infof("Converted %d source tables for matching", len(sourceTables))
			for i, table := range sourceTables {
				s.engine.logger.Infof("Source table %d: %s with %d columns", i, table.Name, len(table.Columns))
			}
		}
	}

	// Parse target database enriched tables
	if targetDB.Tables != "" {
		var targetEnrichedData map[string]interface{}
		if err := json.Unmarshal([]byte(targetDB.Tables), &targetEnrichedData); err != nil {
			s.engine.logger.Warnf("Failed to parse target database enriched tables: %v", err)
		} else {
			targetTables = s.convertEnrichedDataToTableMetadata(targetEnrichedData)
			s.engine.logger.Infof("Converted %d target tables for matching", len(targetTables))
			for i, table := range targetTables {
				s.engine.logger.Infof("Target table %d: %s with %d columns", i, table.Name, len(table.Columns))
			}
		}
	}

	// Use unified model service to match tables
	if len(sourceTables) > 0 && len(targetTables) > 0 {
		matchReq := &unifiedmodelv1.MatchTablesEnrichedRequest{
			SourceTables: sourceTables,
			TargetTables: targetTables,
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

		s.engine.logger.Infof("Calling MatchTablesEnriched with %d source tables and %d target tables", len(sourceTables), len(targetTables))

		matchResp, err := umClient.MatchTablesEnriched(ctx, matchReq)
		s.engine.logger.Infof("Match response: %v", matchResp)
		if err != nil {
			s.engine.logger.Warnf("Failed to match tables using unified model service: %v", err)
		} else {
			// Create mapping rules for matched columns
			s.engine.logger.Infof("Creating mapping rules for matched columns: %v", matchResp.Matches)
			for _, tableMatch := range matchResp.Matches {
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

// convertEnrichedDataToTableMetadata converts enriched table data to protobuf EnrichedTableMetadata
func (s *Server) convertEnrichedDataToTableMetadata(enrichedData map[string]interface{}) []*unifiedmodelv1.EnrichedTableMetadata {
	var tables []*unifiedmodelv1.EnrichedTableMetadata

	// Extract tables from enriched data
	tablesData, ok := enrichedData["tables"]
	if !ok {
		return tables
	}

	tableArray, ok := tablesData.([]interface{})
	if !ok {
		return tables
	}

	// Convert each table to EnrichedTableMetadata
	for _, tableData := range tableArray {
		tableMap, ok := tableData.(map[string]interface{})
		if !ok {
			continue
		}

		table := &unifiedmodelv1.EnrichedTableMetadata{
			Engine:     getString(tableMap, "engine"),
			Schema:     getString(tableMap, "schema"),
			Name:       getString(tableMap, "name"),
			TableType:  getString(tableMap, "table_type"),
			Properties: make(map[string]string),
			Columns:    []*unifiedmodelv1.EnrichedColumnMetadata{},
		}

		// Add classification information if available
		if primaryCategory := getString(tableMap, "primary_category"); primaryCategory != "" {
			table.PrimaryCategory = primaryCategory
		}

		if confidence := getFloat64(tableMap, "classification_confidence"); confidence > 0 {
			table.ClassificationConfidence = confidence
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
					table.ClassificationScores = append(table.ClassificationScores, score)
				}
			}
		}

		// Convert columns
		if columnsData, ok := tableMap["columns"].([]interface{}); ok {
			for _, colData := range columnsData {
				if colMap, ok := colData.(map[string]interface{}); ok {
					column := &unifiedmodelv1.EnrichedColumnMetadata{
						Name:            getString(colMap, "name"),
						Type:            getString(colMap, "type"),
						IsPrimaryKey:    getBool(colMap, "is_primary_key"),
						IsForeignKey:    getBool(colMap, "is_foreign_key"),
						IsNullable:      getBool(colMap, "is_nullable"),
						IsArray:         getBool(colMap, "is_array"),
						IsAutoIncrement: getBool(colMap, "is_auto_increment"),
						ColumnDefault:   getString(colMap, "column_default"),
						VarcharLength:   int32(getFloat64(colMap, "varchar_length")),
						// Privileged data fields
						IsPrivilegedData:      getBool(colMap, "is_privileged_data"),
						DataCategory:          getString(colMap, "data_category"),
						PrivilegedConfidence:  getFloat64(colMap, "privileged_confidence"),
						PrivilegedDescription: getString(colMap, "privileged_description"),
					}

					// Add indexes if available
					if indexesData, ok := colMap["indexes"].([]interface{}); ok {
						for _, indexData := range indexesData {
							if indexStr, ok := indexData.(string); ok {
								column.Indexes = append(column.Indexes, indexStr)
							}
						}
					}

					table.Columns = append(table.Columns, column)
				}
			}
		}

		if table.Name != "" {
			tables = append(tables, table)
		}
	}

	return tables
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
