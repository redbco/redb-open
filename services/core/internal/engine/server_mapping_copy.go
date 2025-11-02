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
	"github.com/redbco/redb-open/pkg/grpcconfig"
	"github.com/redbco/redb-open/services/core/internal/services/mapping"
	"github.com/redbco/redb-open/services/core/internal/services/workspace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// CopyMappingData handles the data copying operation for a mapping
func (s *Server) CopyMappingData(req *corev1.CopyMappingDataRequest, stream corev1.MappingService_CopyMappingDataServer) error {
	defer s.trackOperation()()

	// Generate operation ID for tracking
	operationID := fmt.Sprintf("copy_%s_%d", req.MappingName, time.Now().UnixNano())

	// Send initial response
	if err := stream.Send(&corev1.CopyMappingDataResponse{
		Status:      "started",
		Message:     fmt.Sprintf("Starting data copy operation for mapping '%s'", req.MappingName),
		OperationId: operationID,
	}); err != nil {
		return err
	}

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(stream.Context(), req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return stream.Send(&corev1.CopyMappingDataResponse{
			Status:      "error",
			Message:     fmt.Sprintf("Workspace not found: %v", err),
			OperationId: operationID,
		})
	}

	// Get mapping service
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Get the mapping
	_, err = mappingService.Get(stream.Context(), req.TenantId, workspaceID, req.MappingName)
	if err != nil {
		s.engine.IncrementErrors()
		return stream.Send(&corev1.CopyMappingDataResponse{
			Status:      "error",
			Message:     fmt.Sprintf("Mapping not found: %v", err),
			OperationId: operationID,
		})
	}

	// Get mapping rules
	mappingRules, err := mappingService.GetMappingRulesForMapping(stream.Context(), req.TenantId, workspaceID, req.MappingName)
	if err != nil {
		s.engine.IncrementErrors()
		return stream.Send(&corev1.CopyMappingDataResponse{
			Status:      "error",
			Message:     fmt.Sprintf("Failed to get mapping rules: %v", err),
			OperationId: operationID,
		})
	}

	if len(mappingRules) == 0 {
		return stream.Send(&corev1.CopyMappingDataResponse{
			Status:      "error",
			Message:     "No mapping rules found for this mapping",
			OperationId: operationID,
		})
	}

	// Set defaults
	batchSize := int32(1000)
	if req.BatchSize != nil && *req.BatchSize > 0 {
		batchSize = *req.BatchSize
	}

	parallelWorkers := int32(4)
	if req.ParallelWorkers != nil && *req.ParallelWorkers > 0 {
		parallelWorkers = *req.ParallelWorkers
	}

	dryRun := false
	if req.DryRun != nil {
		dryRun = *req.DryRun
	}

	s.engine.logger.Infof("Starting data copy for mapping '%s': batch_size=%d, parallel_workers=%d, dry_run=%t, rules=%d",
		req.MappingName, batchSize, parallelWorkers, dryRun, len(mappingRules))

	if dryRun {
		// For dry run, just validate the mapping and return success
		return stream.Send(&corev1.CopyMappingDataResponse{
			Status:        "completed",
			Message:       fmt.Sprintf("Dry run completed successfully. Found %d mapping rules ready for data copying.", len(mappingRules)),
			RowsProcessed: 0,
			TotalRows:     0,
			OperationId:   operationID,
		})
	}

	// Group mapping rules by source/target table pairs
	tablePairs := s.groupMappingRulesByTables(mappingRules)

	var totalRowsProcessed int64 = 0
	var totalRowsEstimate int64 = 0
	var allErrors []string

	// Process each table pair
	for i, tablePair := range tablePairs {
		currentTable := fmt.Sprintf("%s -> %s", tablePair.SourceTable, tablePair.TargetTable)

		// Send progress update
		if err := stream.Send(&corev1.CopyMappingDataResponse{
			Status:        "progress",
			Message:       fmt.Sprintf("Processing table pair %d/%d: %s", i+1, len(tablePairs), currentTable),
			RowsProcessed: totalRowsProcessed,
			TotalRows:     totalRowsEstimate,
			CurrentTable:  currentTable,
			OperationId:   operationID,
		}); err != nil {
			return err
		}

		// For now, simulate data copying
		// TODO: Implement actual data copying logic with anchor service
		rowsProcessed, err := s.copyTableData(stream.Context(), tablePair, batchSize)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to copy data for table pair %s: %v", currentTable, err)
			allErrors = append(allErrors, errMsg)
			s.engine.logger.Errorf("%s", errMsg)
			continue
		}

		totalRowsProcessed += rowsProcessed
		s.engine.logger.Infof("Completed copying %d rows for table pair: %s", rowsProcessed, currentTable)
	}

	// Send final completion response
	status := "completed"
	message := fmt.Sprintf("Data copy completed successfully. Processed %d rows across %d table pairs.", totalRowsProcessed, len(tablePairs))

	if len(allErrors) > 0 {
		status = "completed_with_errors"
		message = fmt.Sprintf("Data copy completed with %d errors. Processed %d rows across %d table pairs.", len(allErrors), totalRowsProcessed, len(tablePairs))
	}

	return stream.Send(&corev1.CopyMappingDataResponse{
		Status:        status,
		Message:       message,
		RowsProcessed: totalRowsProcessed,
		TotalRows:     totalRowsProcessed, // For now, set total to processed
		Errors:        allErrors,
		OperationId:   operationID,
	})
}

// GetCopyStatus returns the status of a data copy operation
func (s *Server) GetCopyStatus(ctx context.Context, req *corev1.GetCopyStatusRequest) (*corev1.GetCopyStatusResponse, error) {
	defer s.trackOperation()()

	// For now, return a simple response indicating the operation is not found
	// TODO: Implement proper operation tracking and status storage
	return &corev1.GetCopyStatusResponse{
		Status:  "not_found",
		Message: fmt.Sprintf("Operation '%s' not found or has expired", req.OperationId),
	}, nil
}

// TablePair represents a source-target table pair with associated mapping rules
type TablePair struct {
	SourceTable string
	TargetTable string
	Rules       []*mapping.Rule
}

// groupMappingRulesByTables groups mapping rules by their source/target table pairs
func (s *Server) groupMappingRulesByTables(rules []*mapping.Rule) []TablePair {
	tableMap := make(map[string]TablePair)

	for _, rule := range rules {
		// Extract identifiers from metadata
		sourceURI, ok := rule.Metadata["source_resource_uri"].(string)
		if !ok || sourceURI == "" {
			s.engine.logger.Warnf("Failed to get source_resource_uri from rule metadata")
			continue
		}

		targetURI, ok := rule.Metadata["target_resource_uri"].(string)
		if !ok || targetURI == "" {
			s.engine.logger.Warnf("Failed to get target_resource_uri from rule metadata")
			continue
		}

		// Parse source and target URIs
		sourceInfo, err := s.parseResourceIdentifier(sourceURI)
		if err != nil {
			s.engine.logger.Warnf("Failed to parse source URI '%s': %v", sourceURI, err)
			continue
		}

		targetInfo, err := s.parseResourceIdentifier(targetURI)
		if err != nil {
			s.engine.logger.Warnf("Failed to parse target URI '%s': %v", targetURI, err)
			continue
		}

		// Create table pair key
		pairKey := fmt.Sprintf("%s.%s -> %s.%s", sourceInfo.DatabaseID, sourceInfo.TableName, targetInfo.DatabaseID, targetInfo.TableName)

		// Get or create table pair
		tablePair, exists := tableMap[pairKey]
		if !exists {
			tablePair = TablePair{
				SourceTable: fmt.Sprintf("%s.%s", sourceInfo.DatabaseID, sourceInfo.TableName),
				TargetTable: fmt.Sprintf("%s.%s", targetInfo.DatabaseID, targetInfo.TableName),
				Rules:       []*mapping.Rule{},
			}
		}

		// Add rule to table pair
		tablePair.Rules = append(tablePair.Rules, rule)
		tableMap[pairKey] = tablePair
	}

	// Convert map to slice
	var tablePairs []TablePair
	for _, tablePair := range tableMap {
		tablePairs = append(tablePairs, tablePair)
	}

	return tablePairs
}

// copyTableData copies data for a table pair using the Anchor service
func (s *Server) copyTableData(ctx context.Context, tablePair TablePair, batchSize int32) (int64, error) {
	s.engine.logger.Infof("Copying data from %s to %s with %d column mappings",
		tablePair.SourceTable, tablePair.TargetTable, len(tablePair.Rules))

	// Parse source and target information
	sourceInfo, err := s.parseTableIdentifier(tablePair.SourceTable)
	if err != nil {
		return 0, fmt.Errorf("failed to parse source table: %v", err)
	}

	targetInfo, err := s.parseTableIdentifier(tablePair.TargetTable)
	if err != nil {
		return 0, fmt.Errorf("failed to parse target table: %v", err)
	}

	// Connect to Anchor service
	anchorClient, err := s.getAnchorClient()
	if err != nil {
		return 0, fmt.Errorf("failed to connect to anchor service: %v", err)
	}

	// Connect to Transformation service
	transformationClient, err := s.getTransformationClient()
	if err != nil {
		return 0, fmt.Errorf("failed to connect to transformation service: %v", err)
	}

	// Get row count for progress estimation
	countReq := &anchorv1.GetTableRowCountRequest{
		DatabaseId: sourceInfo.DatabaseID,
		TableName:  sourceInfo.TableName,
	}

	countResp, err := anchorClient.GetTableRowCount(ctx, countReq)
	if err != nil {
		s.engine.logger.Warnf("Failed to get row count for %s: %v", tablePair.SourceTable, err)
	}

	var totalRows int64
	if countResp != nil && countResp.Success {
		totalRows = countResp.RowCount
	}

	s.engine.logger.Infof("Starting data copy for %s -> %s (estimated %d rows)",
		tablePair.SourceTable, tablePair.TargetTable, totalRows)

	// Stream data from source table
	streamReq := &anchorv1.StreamTableDataRequest{
		DatabaseId: sourceInfo.DatabaseID,
		TableName:  sourceInfo.TableName,
		BatchSize:  &batchSize,
	}

	// Get specific columns from mapping rules
	sourceColumns := make([]string, len(tablePair.Rules))
	for i, rule := range tablePair.Rules {
		// Extract source URI from metadata
		sourceURI, ok := rule.Metadata["source_resource_uri"].(string)
		if !ok || sourceURI == "" {
			continue
		}

		sourceInfo, err := s.parseResourceIdentifier(sourceURI)
		if err != nil {
			continue
		}
		sourceColumns[i] = sourceInfo.ColumnName
	}
	if len(sourceColumns) > 0 {
		streamReq.Columns = sourceColumns
	}

	stream, err := anchorClient.StreamTableData(ctx, streamReq)
	if err != nil {
		return 0, fmt.Errorf("failed to start data stream: %v", err)
	}

	var totalRowsProcessed int64

	// Process each batch
	for {
		batch, err := stream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return totalRowsProcessed, fmt.Errorf("error receiving batch: %v", err)
		}

		if !batch.Success {
			return totalRowsProcessed, fmt.Errorf("batch error: %s", batch.Message)
		}

		// Apply transformations to the batch
		transformedData, err := s.applyTransformations(ctx, transformationClient, batch.Data, tablePair.Rules)
		if err != nil {
			s.engine.logger.Warnf("Failed to apply transformations to batch: %v", err)
			// Continue with original data if transformation fails
			transformedData = batch.Data
		}

		// Insert transformed data into target table
		insertReq := &anchorv1.InsertBatchDataRequest{
			DatabaseId:     targetInfo.DatabaseID,
			TableName:      targetInfo.TableName,
			Data:           transformedData,
			UseTransaction: &[]bool{true}[0], // Use transaction for batch insert
		}

		insertResp, err := anchorClient.InsertBatchData(ctx, insertReq)
		if err != nil {
			return totalRowsProcessed, fmt.Errorf("failed to insert batch: %v", err)
		}

		if !insertResp.Success {
			return totalRowsProcessed, fmt.Errorf("insert batch failed: %s", insertResp.Message)
		}

		totalRowsProcessed += insertResp.RowsAffected

		s.engine.logger.Infof("Processed batch %d: %d rows inserted (total: %d)",
			batch.BatchNumber, insertResp.RowsAffected, totalRowsProcessed)

		// Check if this was the last batch
		if batch.IsComplete {
			break
		}
	}

	s.engine.logger.Infof("Completed copying %d rows from %s to %s",
		totalRowsProcessed, tablePair.SourceTable, tablePair.TargetTable)

	return totalRowsProcessed, nil
}

// Helper method to parse table identifier (database_id.table_name)
func (s *Server) parseTableIdentifier(identifier string) (*TableIdentifierInfo, error) {
	parts := strings.Split(identifier, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid table identifier format: expected 'database_id.table_name', got '%s'", identifier)
	}

	return &TableIdentifierInfo{
		DatabaseID: parts[0],
		TableName:  parts[1],
	}, nil
}

// TableIdentifierInfo represents parsed table identifier
type TableIdentifierInfo struct {
	DatabaseID string
	TableName  string
}

// getAnchorClient creates a connection to the Anchor service
func (s *Server) getAnchorClient() (anchorv1.AnchorServiceClient, error) {
	anchorAddr := grpcconfig.GetServiceAddress(s.engine.config, "anchor")

	conn, err := grpc.Dial(anchorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to anchor service at %s: %v", anchorAddr, err)
	}

	return anchorv1.NewAnchorServiceClient(conn), nil
}

// getTransformationClient creates a connection to the Transformation service
func (s *Server) getTransformationClient() (transformationv1.TransformationServiceClient, error) {
	transformationAddr := grpcconfig.GetServiceAddress(s.engine.config, "transformation")

	conn, err := grpc.Dial(transformationAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to transformation service at %s: %v", transformationAddr, err)
	}

	return transformationv1.NewTransformationServiceClient(conn), nil
}

// applyTransformations applies transformation rules to a batch of data
func (s *Server) applyTransformations(ctx context.Context, client transformationv1.TransformationServiceClient, data []byte, rules []*mapping.Rule) ([]byte, error) {
	// Parse the JSON data (array of rows)
	var sourceRows []map[string]interface{}
	if err := json.Unmarshal(data, &sourceRows); err != nil {
		return nil, fmt.Errorf("failed to parse source data: %v", err)
	}

	// Transform each row
	targetRows := make([]map[string]interface{}, 0, len(sourceRows))
	for _, sourceRow := range sourceRows {
		targetRow := make(map[string]interface{})

		// Apply each mapping rule
		for _, rule := range rules {
			// Extract source and target column names from metadata
			sourceColumn, _ := rule.Metadata["source_column"].(string)
			targetColumn, _ := rule.Metadata["target_column"].(string)
			transformationName, _ := rule.Metadata["transformation_name"].(string)

			if sourceColumn == "" || targetColumn == "" {
				s.engine.logger.Warnf("Rule missing source or target column in metadata")
				continue
			}

			// Get the source value
			sourceValue, exists := sourceRow[sourceColumn]
			if !exists {
				s.engine.logger.Warnf("Source column '%s' not found in row data", sourceColumn)
				continue
			}

			// Apply transformation if needed
			var targetValue interface{}
			if transformationName != "" && transformationName != "direct_mapping" {
				// Call transformation service for non-direct transformations
				transformedValue, err := s.applyTransformation(ctx, client, transformationName, sourceValue)
				if err != nil {
					s.engine.logger.Warnf("Failed to apply transformation '%s' to column '%s': %v, using original value",
						transformationName, sourceColumn, err)
					targetValue = sourceValue
				} else {
					targetValue = transformedValue
				}
			} else {
				// Direct mapping - no transformation needed
				targetValue = sourceValue
			}

			// Set the target column with the (possibly transformed) value
			targetRow[targetColumn] = targetValue
		}

		targetRows = append(targetRows, targetRow)
	}

	// Convert back to JSON
	transformedData, err := json.Marshal(targetRows)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal transformed data: %v", err)
	}

	return transformedData, nil
}

// applyTransformation applies a single transformation to a value
func (s *Server) applyTransformation(ctx context.Context, client transformationv1.TransformationServiceClient, transformationName string, value interface{}) (interface{}, error) {
	// Convert value to string for transformation
	var inputStr string
	switch v := value.(type) {
	case string:
		inputStr = v
	case nil:
		return nil, nil
	default:
		// Convert other types to string
		inputStr = fmt.Sprintf("%v", v)
	}

	// Call transformation service
	transformReq := &transformationv1.TransformRequest{
		FunctionName: transformationName,
		Input:        inputStr,
	}

	transformResp, err := client.Transform(ctx, transformReq)
	if err != nil {
		return nil, fmt.Errorf("transformation service error: %v", err)
	}

	if transformResp.Status != commonv1.Status_STATUS_SUCCESS {
		return nil, fmt.Errorf("transformation failed: %s", transformResp.StatusMessage)
	}

	return transformResp.Output, nil
}
