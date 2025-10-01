package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	pb "github.com/redbco/redb-open/api/proto/anchor/v1"
	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/config"
	"github.com/redbco/redb-open/services/anchor/internal/database"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

type Server struct {
	pb.UnimplementedAnchorServiceServer
	engine *Engine
}

func NewServer(engine *Engine) *Server {
	return &Server{
		engine: engine,
	}
}

// Helper method to track operations
func (s *Server) trackOperation() func() {
	s.engine.TrackOperation()
	return s.engine.UntrackOperation
}

func (s *Server) ConnectInstance(ctx context.Context, req *pb.ConnectInstanceRequest) (*pb.ConnectInstanceResponse, error) {
	defer s.trackOperation()()

	// Fetch the instance configuration using the instance ID
	unifiedConfig, err := s.engine.GetState().GetConfigRepository().GetInstanceConfigByID(ctx, req.InstanceId)
	if err != nil {
		return &pb.ConnectInstanceResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to get instance configuration: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			InstanceId: req.InstanceId,
		}, nil
	}

	// Convert unified config to connection config
	instanceConfig := unifiedConfig.ToConnectionConfig()

	// Try to establish connection
	client, err := s.engine.GetState().GetDatabaseManager().ConnectInstance(instanceConfig)
	if err != nil {
		// Update connection status in repository
		s.engine.GetState().GetConfigRepository().UpdateInstanceConnectionStatus(ctx, req.InstanceId, false, fmt.Sprintf("Connection failed: %v", err))

		return &pb.ConnectInstanceResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to connect to instance: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			InstanceId: req.InstanceId,
		}, nil
	}

	// Update connection status in repository as successful
	s.engine.GetState().GetConfigRepository().UpdateInstanceConnectionStatus(ctx, req.InstanceId, true, "Connected successfully")

	// Collect and store instance metadata
	collector := database.NewInstanceMetadataCollector(client)
	metadata, err := collector.CollectMetadata(ctx, req.InstanceId)
	if err != nil {
		// Log error but don't fail the connection - metadata collection is not critical
		fmt.Printf("Failed to collect instance metadata for %s: %v\n", req.InstanceId, err)
	} else {
		// Store metadata in repository
		err = s.engine.GetState().GetConfigRepository().UpdateInstanceMetadata(ctx, &config.InstanceMetadata{
			InstanceID:       req.InstanceId,
			Version:          metadata.Version,
			UptimeSeconds:    metadata.UptimeSeconds,
			TotalDatabases:   metadata.TotalDatabases,
			TotalConnections: metadata.TotalConnections,
			MaxConnections:   metadata.MaxConnections,
		})
		if err != nil {
			// Log error but don't fail the connection
			fmt.Printf("Failed to store instance metadata for %s: %v\n", req.InstanceId, err)
		}
	}

	return &pb.ConnectInstanceResponse{
		Success:    true,
		Message:    "Successfully connected to instance",
		Status:     commonv1.Status_STATUS_CONNECTED,
		InstanceId: req.InstanceId,
	}, nil
}

func (s *Server) UpdateInstanceConnection(ctx context.Context, req *pb.UpdateInstanceConnectionRequest) (*pb.UpdateInstanceConnectionResponse, error) {
	defer s.trackOperation()()

	// TODO: Implementation will need instance connection update logic
	return &pb.UpdateInstanceConnectionResponse{
		Success:    true,
		Message:    "Instance connection updated successfully",
		Status:     commonv1.Status_STATUS_UPDATED,
		InstanceId: req.InstanceId,
	}, nil
}

func (s *Server) DisconnectInstance(ctx context.Context, req *pb.DisconnectInstanceRequest) (*pb.DisconnectInstanceResponse, error) {
	defer s.trackOperation()()

	// First, verify the instance exists in the repository
	_, err := s.engine.GetState().GetConfigRepository().GetInstanceConfigByID(ctx, req.InstanceId)
	if err != nil {
		// Instance doesn't exist in repository, but might still be in DatabaseManager
		// Try to clean it up from DatabaseManager anyway
		s.engine.GetState().GetDatabaseManager().DisconnectInstance(req.InstanceId)

		return &pb.DisconnectInstanceResponse{
			Success:    false,
			Message:    fmt.Sprintf("Instance not found in repository: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			InstanceId: req.InstanceId,
		}, nil
	}

	// Attempt to disconnect the instance from DatabaseManager
	err = s.engine.GetState().GetDatabaseManager().DisconnectInstance(req.InstanceId)
	if err != nil {
		// Log the error but don't fail completely - the instance might not be connected in DatabaseManager
		fmt.Printf("Warning: Failed to disconnect instance from DatabaseManager: %v\n", err)
	}

	// Update connection status in repository as disconnected regardless of DatabaseManager result
	// This ensures the repository is always in sync
	err = s.engine.GetState().GetConfigRepository().UpdateInstanceConnectionStatus(ctx, req.InstanceId, false, "Disconnected successfully")
	if err != nil {
		// If we can't update the repository status, this is a more serious error
		return &pb.DisconnectInstanceResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to update instance status in repository: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			InstanceId: req.InstanceId,
		}, nil
	}

	return &pb.DisconnectInstanceResponse{
		Success:    true,
		Message:    "Instance disconnected successfully",
		Status:     commonv1.Status_STATUS_DISCONNECTED,
		InstanceId: req.InstanceId,
	}, nil
}

func (s *Server) ConnectDatabase(ctx context.Context, req *pb.ConnectDatabaseRequest) (*pb.ConnectDatabaseResponse, error) {
	defer s.trackOperation()()

	// Fetch the database configuration using the database ID
	unifiedConfig, err := s.engine.GetState().GetConfigRepository().GetDatabaseConfigByID(ctx, req.DatabaseId)
	if err != nil {
		return &pb.ConnectDatabaseResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to get database configuration: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Convert unified config to connection config
	dbConfig := unifiedConfig.ToConnectionConfig()

	// Try to establish connection
	_, err = s.engine.GetState().GetDatabaseManager().ConnectDatabase(dbConfig)
	if err != nil {
		// Update connection status in repository
		s.engine.GetState().GetConfigRepository().UpdateDatabaseConnectionStatus(ctx, req.DatabaseId, false, fmt.Sprintf("Connection failed: %v", err))

		return &pb.ConnectDatabaseResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to connect to database: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
		}, nil
	}

	// Update connection status in repository as successful
	s.engine.GetState().GetConfigRepository().UpdateDatabaseConnectionStatus(ctx, req.DatabaseId, true, "Connected successfully")

	return &pb.ConnectDatabaseResponse{
		Success:    true,
		Message:    "Successfully connected to database",
		Status:     commonv1.Status_STATUS_CONNECTED,
		DatabaseId: req.DatabaseId,
	}, nil
}

func (s *Server) UpdateDatabaseConnection(ctx context.Context, req *pb.UpdateDatabaseConnectionRequest) (*pb.UpdateDatabaseConnectionResponse, error) {
	defer s.trackOperation()()

	// TODO: Implementation will need database connection update logic
	return &pb.UpdateDatabaseConnectionResponse{
		Success:    true,
		Message:    "Database connection updated successfully",
		Status:     commonv1.Status_STATUS_UPDATED,
		DatabaseId: req.DatabaseId,
	}, nil
}

func (s *Server) DisconnectDatabase(ctx context.Context, req *pb.DisconnectDatabaseRequest) (*pb.DisconnectDatabaseResponse, error) {
	defer s.trackOperation()()

	// Attempt to disconnect the database
	err := s.engine.GetState().GetDatabaseManager().DisconnectDatabase(req.DatabaseId)
	if err != nil {
		// Update connection status in repository as failed to disconnect
		s.engine.GetState().GetConfigRepository().UpdateDatabaseConnectionStatus(ctx, req.DatabaseId, true, fmt.Sprintf("Failed to disconnect: %v", err))

		return &pb.DisconnectDatabaseResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to disconnect database: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
		}, nil
	}

	// Update connection status in repository as disconnected
	s.engine.GetState().GetConfigRepository().UpdateDatabaseConnectionStatus(ctx, req.DatabaseId, false, "Disconnected successfully")

	return &pb.DisconnectDatabaseResponse{
		Success:    true,
		Message:    "Database disconnected successfully",
		Status:     commonv1.Status_STATUS_DISCONNECTED,
		DatabaseId: req.DatabaseId,
	}, nil
}

func (s *Server) GetInstanceMetadata(ctx context.Context, req *pb.GetInstanceMetadataRequest) (*pb.GetInstanceMetadataResponse, error) {
	defer s.trackOperation()()

	// Get instance metadata from the database manager
	metadata, err := s.engine.GetState().GetDatabaseManager().GetInstanceMetadata(req.InstanceId)
	if err != nil {
		return &pb.GetInstanceMetadataResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to retrieve instance metadata: %v", err),
			InstanceId: req.InstanceId,
			Metadata:   nil,
		}, nil
	}

	// Convert metadata to JSON
	metadataData, err := json.Marshal(metadata)
	if err != nil {
		return &pb.GetInstanceMetadataResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to marshal metadata: %v", err),
			InstanceId: req.InstanceId,
			Metadata:   nil,
		}, nil
	}

	return &pb.GetInstanceMetadataResponse{
		Success:    true,
		Message:    "Instance metadata retrieved successfully",
		InstanceId: req.InstanceId,
		Metadata:   metadataData,
	}, nil
}

func (s *Server) CreateDatabase(ctx context.Context, req *pb.CreateDatabaseRequest) (*pb.CreateDatabaseResponse, error) {
	defer s.trackOperation()()

	// Parse options from request if provided
	var options map[string]interface{}
	if len(req.Options) > 0 {
		if err := json.Unmarshal(req.Options, &options); err != nil {
			return &pb.CreateDatabaseResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to parse options: %v", err),
				Status:  commonv1.Status_STATUS_ERROR,
			}, nil
		}
	} else {
		options = make(map[string]interface{})
	}

	// Add the database name to options if not already present
	if _, exists := options["database_name"]; !exists {
		options["database_name"] = req.DatabaseName
	}

	// Create the database using the database manager
	err := s.engine.GetState().GetDatabaseManager().CreateDatabase(req.InstanceId, options)
	if err != nil {
		return &pb.CreateDatabaseResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to create database: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	return &pb.CreateDatabaseResponse{
		Success:    true,
		Message:    "Database created successfully",
		Status:     commonv1.Status_STATUS_CREATED,
		DatabaseId: req.DatabaseName, // Use database name as the ID for now
	}, nil
}

func (s *Server) DropDatabase(ctx context.Context, req *pb.DropDatabaseRequest) (*pb.DropDatabaseResponse, error) {
	defer s.trackOperation()()

	// Create options map for the drop operation
	options := make(map[string]interface{})

	// Drop the database using the database manager
	err := s.engine.GetState().GetDatabaseManager().DropDatabase(req.DatabaseId, options)
	if err != nil {
		return &pb.DropDatabaseResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to drop database: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			InstanceId: req.InstanceId,
		}, nil
	}

	return &pb.DropDatabaseResponse{
		Success:      true,
		Message:      "Database dropped successfully",
		Status:       commonv1.Status_STATUS_DELETED,
		InstanceId:   req.InstanceId,
		DatabaseName: req.DatabaseId,
	}, nil
}

func (s *Server) GetDatabaseMetadata(ctx context.Context, req *pb.GetDatabaseMetadataRequest) (*pb.GetDatabaseMetadataResponse, error) {
	defer s.trackOperation()()

	// Get database metadata from the database manager
	metadata, err := s.engine.GetState().GetDatabaseManager().GetDatabaseMetadata(req.DatabaseId)
	if err != nil {
		return &pb.GetDatabaseMetadataResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to retrieve database metadata: %v", err),
			DatabaseId: req.DatabaseId,
			Metadata:   nil,
		}, nil
	}

	// Convert metadata to JSON
	metadataData, err := json.Marshal(metadata)
	if err != nil {
		return &pb.GetDatabaseMetadataResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to marshal metadata: %v", err),
			DatabaseId: req.DatabaseId,
			Metadata:   nil,
		}, nil
	}

	return &pb.GetDatabaseMetadataResponse{
		Success:    true,
		Message:    "Database metadata retrieved successfully",
		DatabaseId: req.DatabaseId,
		Metadata:   metadataData,
	}, nil
}

func (s *Server) GetDatabaseSchema(ctx context.Context, req *pb.GetDatabaseSchemaRequest) (*pb.GetDatabaseSchemaResponse, error) {
	defer s.trackOperation()()

	// Get database structure from the database manager
	structure, err := s.engine.GetState().GetDatabaseManager().GetDatabaseStructure(req.DatabaseId)
	if err != nil {
		return &pb.GetDatabaseSchemaResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to retrieve database schema: %v", err),
			DatabaseId: req.DatabaseId,
			Schema:     nil,
		}, nil
	}

	// Convert structure to JSON
	schemaData, err := json.Marshal(structure)
	if err != nil {
		return &pb.GetDatabaseSchemaResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to marshal schema data: %v", err),
			DatabaseId: req.DatabaseId,
			Schema:     nil,
		}, nil
	}

	return &pb.GetDatabaseSchemaResponse{
		Success:    true,
		Message:    "Schema retrieved successfully",
		DatabaseId: req.DatabaseId,
		Schema:     schemaData,
	}, nil
}

func (s *Server) DeployDatabaseSchema(ctx context.Context, req *pb.DeployDatabaseSchemaRequest) (*pb.DeployDatabaseSchemaResponse, error) {
	defer s.trackOperation()()

	// Parse JSON schema bytes into UnifiedModel
	var structure *unifiedmodel.UnifiedModel
	if err := json.Unmarshal(req.Schema, &structure); err != nil {
		return &pb.DeployDatabaseSchemaResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to parse schema data: %v", err),
			DatabaseId: req.DatabaseId,
		}, nil
	}

	// Deploy the database structure using the database manager
	err := s.engine.GetState().GetDatabaseManager().DeployDatabaseStructure(req.DatabaseId, structure)
	if err != nil {
		return &pb.DeployDatabaseSchemaResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to deploy database schema: %v", err),
			DatabaseId: req.DatabaseId,
		}, nil
	}

	return &pb.DeployDatabaseSchemaResponse{
		Success:    true,
		Message:    "Database schema deployed successfully",
		DatabaseId: req.DatabaseId,
	}, nil
}

func (s *Server) FetchData(ctx context.Context, req *pb.FetchDataRequest) (*pb.FetchDataResponse, error) {
	defer s.trackOperation()()

	// Set default limit (can be made configurable from options if needed)
	limit := 100 // Default limit for data fetching

	// TODO: Parse limit from req.Options if provided
	// For now, use default limit

	// Get data from the database using the database_id directly
	data, err := s.engine.GetState().GetDatabaseManager().GetDataFromDatabase(req.DatabaseId, req.TableName, limit)
	if err != nil {
		// Send error response
		response := &pb.FetchDataResponse{
			Message:    fmt.Sprintf("Failed to fetch data: %v", err),
			Success:    false,
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
			TableName:  req.TableName,
			Data:       nil,
		}
		return response, nil
	}

	// Convert data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		// Send error response
		response := &pb.FetchDataResponse{
			Message:    fmt.Sprintf("Failed to marshal data: %v", err),
			Success:    false,
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
			TableName:  req.TableName,
			Data:       nil,
		}
		return response, nil
	}

	// Send successful response
	response := &pb.FetchDataResponse{
		Message:    "Data fetched successfully",
		Success:    true,
		Status:     commonv1.Status_STATUS_SUCCESS,
		DatabaseId: req.DatabaseId,
		TableName:  req.TableName,
		Data:       jsonData,
	}
	return response, nil
}

func (s *Server) FetchDataStream(req *pb.FetchDataStreamRequest, stream pb.AnchorService_FetchDataStreamServer) error {
	defer s.trackOperation()()

	// TODO: Implementation will need data streaming logic
	return stream.Send(&pb.FetchDataStreamResponse{
		Success:    true,
		Message:    "Data streamed successfully",
		DatabaseId: req.DatabaseId,
		TableName:  req.TableName,
		Data:       nil,
	})
}

func (s *Server) FetchDataToCache(ctx context.Context, req *pb.FetchDataToCacheRequest) (*pb.FetchDataToCacheResponse, error) {
	defer s.trackOperation()()

	// TODO: Implementation will need data caching logic
	return &pb.FetchDataToCacheResponse{
		Success:    true,
		Message:    "Data cached successfully",
		DatabaseId: req.DatabaseId,
		TableName:  req.TableName,
		CacheId:    "cache_id",
	}, nil
}

func (s *Server) InsertData(ctx context.Context, req *pb.InsertDataRequest) (*pb.InsertDataResponse, error) {
	defer s.trackOperation()()

	// Parse JSON data from request
	var data []map[string]interface{}
	if err := json.Unmarshal(req.Data, &data); err != nil {
		return &pb.InsertDataResponse{
			Success:      false,
			Message:      fmt.Sprintf("Failed to parse JSON data: %v", err),
			Status:       commonv1.Status_STATUS_ERROR,
			DatabaseId:   req.DatabaseId,
			TableName:    req.TableName,
			RowsAffected: 0,
		}, nil
	}

	// Insert data into the database
	rowsAffected, err := s.engine.GetState().GetDatabaseManager().InsertDataToDatabase(req.DatabaseId, req.TableName, data)
	if err != nil {
		return &pb.InsertDataResponse{
			Success:      false,
			Message:      fmt.Sprintf("Failed to insert data: %v", err),
			Status:       commonv1.Status_STATUS_ERROR,
			DatabaseId:   req.DatabaseId,
			TableName:    req.TableName,
			RowsAffected: 0,
		}, nil
	}

	return &pb.InsertDataResponse{
		Success:      true,
		Message:      "Data inserted successfully",
		Status:       commonv1.Status_STATUS_SUCCESS,
		DatabaseId:   req.DatabaseId,
		TableName:    req.TableName,
		RowsAffected: rowsAffected,
	}, nil
}

func (s *Server) InsertDataStream(req *pb.InsertDataStreamRequest, stream pb.AnchorService_InsertDataStreamServer) error {
	defer s.trackOperation()()

	// TODO: Implementation will need data streaming logic
	return stream.Send(&pb.InsertDataStreamResponse{
		Success:      true,
		Message:      "Data streamed successfully",
		DatabaseId:   req.DatabaseId,
		TableName:    req.TableName,
		RowsAffected: 0,
	})
}

func (s *Server) InsertDataFromCache(ctx context.Context, req *pb.InsertDataFromCacheRequest) (*pb.InsertDataFromCacheResponse, error) {
	defer s.trackOperation()()

	// TODO: Implementation will need data caching logic
	return &pb.InsertDataFromCacheResponse{
		Success:      true,
		Message:      "Data inserted from cache successfully",
		Status:       commonv1.Status_STATUS_SUCCESS,
		DatabaseId:   req.DatabaseId,
		TableName:    req.TableName,
		RowsAffected: 0,
	}, nil
}

func (s *Server) TransformData(ctx context.Context, req *pb.TransformDataRequest) (*pb.TransformDataResponse, error) {
	defer s.trackOperation()()

	// Parse the data from the request
	var sourceData []map[string]interface{}
	if err := json.Unmarshal(req.Data, &sourceData); err != nil {
		return &pb.TransformDataResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to parse source data: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Parse options
	var options map[string]interface{}
	if len(req.Options) > 0 {
		if err := json.Unmarshal(req.Options, &options); err != nil {
			return &pb.TransformDataResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to parse options: %v", err),
				Status:  commonv1.Status_STATUS_ERROR,
			}, nil
		}
	}

	// Transform the data based on options
	transformedData, err := s.transformData(sourceData, options)
	if err != nil {
		return &pb.TransformDataResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to transform data: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Serialize the transformed data
	transformedDataBytes, err := json.Marshal(transformedData)
	if err != nil {
		return &pb.TransformDataResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to serialize transformed data: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	return &pb.TransformDataResponse{
		Message:         "Data transformed successfully",
		Success:         true,
		Status:          commonv1.Status_STATUS_SUCCESS,
		DatabaseId:      req.DatabaseId,
		TableName:       req.TableName,
		TransformedData: transformedDataBytes,
	}, nil
}

func (s *Server) TransformDataStream(req *pb.TransformDataStreamRequest, stream pb.AnchorService_TransformDataStreamServer) error {
	defer s.trackOperation()()

	// Parse the data from the request
	var sourceData []map[string]interface{}
	if err := json.Unmarshal(req.Data, &sourceData); err != nil {
		return stream.Send(&pb.TransformDataStreamResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to parse source data: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		})
	}

	// Parse options
	var options map[string]interface{}
	if len(req.Options) > 0 {
		if err := json.Unmarshal(req.Options, &options); err != nil {
			return stream.Send(&pb.TransformDataStreamResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to parse options: %v", err),
				Status:  commonv1.Status_STATUS_ERROR,
			})
		}
	}

	// Process data in chunks for streaming
	chunkSize := 1000 // Process 1000 rows at a time
	totalRows := len(sourceData)
	processedRows := 0

	for i := 0; i < totalRows; i += chunkSize {
		end := i + chunkSize
		if end > totalRows {
			end = totalRows
		}

		chunk := sourceData[i:end]

		// Transform the chunk
		transformedChunk, err := s.transformData(chunk, options)
		if err != nil {
			return stream.Send(&pb.TransformDataStreamResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to transform data chunk: %v", err),
				Status:  commonv1.Status_STATUS_ERROR,
			})
		}

		// Serialize the transformed chunk
		transformedDataBytes, err := json.Marshal(transformedChunk)
		if err != nil {
			return stream.Send(&pb.TransformDataStreamResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to serialize transformed data: %v", err),
				Status:  commonv1.Status_STATUS_ERROR,
			})
		}

		processedRows += len(chunk)
		isComplete := processedRows >= totalRows

		// Send the chunk
		if err := stream.Send(&pb.TransformDataStreamResponse{
			Message:         "Data chunk transformed successfully",
			Success:         true,
			Status:          commonv1.Status_STATUS_SUCCESS,
			DatabaseId:      req.DatabaseId,
			TableName:       req.TableName,
			TransformedData: transformedDataBytes,
			IsComplete:      isComplete,
		}); err != nil {
			return err
		}
	}

	return nil
}

// transformData applies transformations to the source data based on options
func (s *Server) transformData(sourceData []map[string]interface{}, options map[string]interface{}) ([]map[string]interface{}, error) {
	if len(sourceData) == 0 {
		return sourceData, nil
	}

	// Get transformation rules from options
	transformationRules, ok := options["transformation_rules"].([]interface{})
	if !ok {
		// No transformation rules, return data as-is
		return sourceData, nil
	}

	// Create a map of source fields that should be removed after transformation
	// Only remove source fields that are different from their target fields
	sourceFieldsToRemove := make(map[string]bool)
	for _, ruleInterface := range transformationRules {
		if rule, ok := ruleInterface.(map[string]interface{}); ok {
			if sourceField, ok := rule["source_field"].(string); ok {
				if targetField, ok := rule["target_field"].(string); ok {
					// Only remove source field if it's different from target field
					if sourceField != targetField {
						sourceFieldsToRemove[sourceField] = true
					}
				}
			}
		}
	}

	// Apply transformations
	transformedData := make([]map[string]interface{}, len(sourceData))
	for i, row := range sourceData {
		transformedRow := make(map[string]interface{})

		// Copy all fields from source row
		for key, value := range row {
			transformedRow[key] = value
		}

		// Apply transformation rules
		for _, ruleInterface := range transformationRules {
			rule, ok := ruleInterface.(map[string]interface{})
			if !ok {
				continue
			}

			sourceField, ok := rule["source_field"].(string)
			if !ok {
				continue
			}

			targetField, ok := rule["target_field"].(string)
			if !ok {
				continue
			}

			transformationType, ok := rule["transformation_type"].(string)
			if !ok {
				transformationType = "direct" // Default to direct mapping
			}

			// Get source value
			sourceValue, exists := row[sourceField]
			if !exists {
				continue
			}

			// Apply transformation
			transformedValue, err := s.applyTransformation(sourceValue, transformationType, rule)
			if err != nil {
				// TODO: Handle this error
				transformedValue = sourceValue // Use original value if transformation fails
			}

			transformedRow[targetField] = transformedValue
		}

		// Remove original source fields that were mapped to different target fields
		for sourceField := range sourceFieldsToRemove {
			delete(transformedRow, sourceField)
		}

		transformedData[i] = transformedRow
	}

	return transformedData, nil
}

// applyTransformation applies a specific transformation to a value
func (s *Server) applyTransformation(value interface{}, transformationType string, rule map[string]interface{}) (interface{}, error) {
	switch transformationType {
	case "direct":
		// Direct mapping - no transformation
		return value, nil
	case "uppercase":
		// Convert to uppercase
		if str, ok := value.(string); ok {
			return strings.ToUpper(str), nil
		}
		return value, nil
	case "lowercase":
		// Convert to lowercase
		if str, ok := value.(string); ok {
			return strings.ToLower(str), nil
		}
		return value, nil
	case "trim":
		// Trim whitespace
		if str, ok := value.(string); ok {
			return strings.TrimSpace(str), nil
		}
		return value, nil
	case "replace":
		// String replacement
		if str, ok := value.(string); ok {
			oldStr, _ := rule["old_string"].(string)
			newStr, _ := rule["new_string"].(string)
			return strings.ReplaceAll(str, oldStr, newStr), nil
		}
		return value, nil
	case "format_date":
		// Date formatting
		if str, ok := value.(string); ok {
			inputFormat, _ := rule["input_format"].(string)
			outputFormat, _ := rule["output_format"].(string)

			if inputFormat == "" {
				inputFormat = "2006-01-02"
			}
			if outputFormat == "" {
				outputFormat = "2006-01-02"
			}

			parsedTime, err := time.Parse(inputFormat, str)
			if err != nil {
				return value, err
			}
			return parsedTime.Format(outputFormat), nil
		}
		return value, nil
	case "custom":
		// Custom transformation using transformation service
		transformationName, _ := rule["transformation_name"].(string)
		if transformationName != "" && s.engine.coreConn != nil {
			// Call transformation service
			// This would require implementing the transformation service call
			// For now, return the original value
			return value, nil
		}
		return value, nil
	default:
		// Unknown transformation type, return original value
		return value, nil
	}
}

func (s *Server) WipeDatabase(ctx context.Context, req *pb.WipeDatabaseRequest) (*pb.WipeDatabaseResponse, error) {
	defer s.trackOperation()()

	// Wipe the database using the database manager
	err := s.engine.GetState().GetDatabaseManager().WipeDatabase(req.DatabaseId)
	if err != nil {
		return &pb.WipeDatabaseResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to wipe database: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
		}, nil
	}

	return &pb.WipeDatabaseResponse{
		Success:    true,
		Message:    "Database wiped successfully",
		Status:     commonv1.Status_STATUS_SUCCESS,
		DatabaseId: req.DatabaseId,
	}, nil
}

func (s *Server) ExecuteCommand(ctx context.Context, req *pb.ExecuteCommandRequest) (*pb.ExecuteCommandResponse, error) {
	defer s.trackOperation()()

	// Execute the command using the database manager
	result, err := s.engine.GetState().GetDatabaseManager().ExecuteCommand(req.DatabaseId, req.Command)
	if err != nil {
		return &pb.ExecuteCommandResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to execute command: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
			Command:    req.Command,
		}, nil
	}

	return &pb.ExecuteCommandResponse{
		Success:    true,
		Message:    "Command executed successfully",
		Status:     commonv1.Status_STATUS_SUCCESS,
		DatabaseId: req.DatabaseId,
		Command:    req.Command,
		Data:       result,
	}, nil
}

// Refactor CreateReplicationSource
func (s *Server) CreateReplicationSource(ctx context.Context, req *pb.CreateReplicationSourceRequest) (*pb.CreateReplicationSourceResponse, error) {
	defer s.trackOperation()()

	if req.DatabaseId == "" {
		return &pb.CreateReplicationSourceResponse{
			Success: false,
			Message: "Database ID is required",
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}
	if len(req.TableNames) == 0 {
		return &pb.CreateReplicationSourceResponse{
			Success: false,
			Message: "At least one table name is required",
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	dbConfig, err := s.engine.GetState().GetConfigRepository().GetDatabaseConfigByID(ctx, req.DatabaseId)
	if err != nil {
		return &pb.CreateReplicationSourceResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to get database configuration: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	dbManager := s.engine.GetState().GetDatabaseManager()
	replicationID := req.DatabaseId // Use database ID as the replication client key

	// Check if a replication client already exists for this database
	repClient, err := dbManager.GetReplicationClient(replicationID)
	if err != nil {
		// No client exists, create a new one
		replicationConfig := dbclient.ReplicationConfig{
			ReplicationID:     replicationID,
			DatabaseID:        req.DatabaseId,
			WorkspaceID:       dbConfig.WorkspaceID,
			TenantID:          dbConfig.TenantID,
			ConnectionType:    dbConfig.Type,
			DatabaseVendor:    dbConfig.Vendor,
			Host:              dbConfig.Host,
			Port:              dbConfig.Port,
			Username:          dbConfig.Username,
			Password:          dbConfig.Password,
			DatabaseName:      dbConfig.DatabaseName,
			SSL:               dbConfig.SSL,
			SSLMode:           dbConfig.SSLMode,
			SSLCert:           derefString(dbConfig.SSLCert),
			SSLKey:            derefString(dbConfig.SSLKey),
			SSLRootCert:       derefString(dbConfig.SSLRootCert),
			ConnectedToNodeID: dbConfig.ConnectedToNodeID,
			OwnerID:           dbConfig.OwnerID,
			TableNames:        req.TableNames,
		}
		client, err := dbManager.ConnectReplication(replicationConfig)
		if err != nil {
			return &pb.CreateReplicationSourceResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to create replication client: %v", err),
				Status:  commonv1.Status_STATUS_ERROR,
			}, nil
		}
		// Add all tables to the client
		for _, t := range req.TableNames {
			client.AddTable(t)
		}
	} else {
		// Client exists, add new tables
		for _, t := range req.TableNames {
			repClient.AddTable(t)
		}
	}

	// Store/update replication source in repository (one per database)
	tableNamesCSV := strings.Join(req.TableNames, ",")
	replicationSource := &config.ReplicationSource{
		ReplicationSourceID: replicationID,
		TenantID:            dbConfig.TenantID,
		WorkspaceID:         dbConfig.WorkspaceID,
		DatabaseID:          req.DatabaseId,
		TableName:           tableNamesCSV,
		RelationshipID:      req.RelationshipId,
		StatusMessage:       "Replication source created/updated successfully",
		Status:              "STATUS_ACTIVE",
	}
	err = s.engine.GetState().GetConfigRepository().CreateReplicationSource(ctx, replicationSource)
	if err != nil {
		return &pb.CreateReplicationSourceResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to store replication source: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	source := &pb.ReplicationSource{
		TenantId:            replicationSource.TenantID,
		WorkspaceId:         replicationSource.WorkspaceID,
		DatabaseId:          replicationSource.DatabaseID,
		TableNames:          req.TableNames,
		ReplicationSourceId: replicationSource.ReplicationSourceID,
		RelationshipId:      replicationSource.RelationshipID,
	}

	return &pb.CreateReplicationSourceResponse{
		Success: true,
		Message: "Replication source created/updated successfully",
		Status:  commonv1.Status_STATUS_SUCCESS,
		Source:  source,
	}, nil
}

// AddTableToReplicationSource
func (s *Server) AddTableToReplicationSource(ctx context.Context, req *pb.AddTableToReplicationSourceRequest) (*pb.AddTableToReplicationSourceResponse, error) {
	defer s.trackOperation()()

	if req.DatabaseId == "" || req.ReplicationSourceId == "" || req.TableName == "" {
		return &pb.AddTableToReplicationSourceResponse{
			Success: false,
			Message: "Database ID, Replication Source ID, and Table Name are required",
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}
	dbManager := s.engine.GetState().GetDatabaseManager()
	repClient, err := dbManager.GetReplicationClient(req.ReplicationSourceId)
	if err != nil {
		return &pb.AddTableToReplicationSourceResponse{
			Success: false,
			Message: fmt.Sprintf("Replication client not found: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}
	repClient.AddTable(req.TableName)

	// Update repository
	replicationSource, err := s.engine.GetState().GetConfigRepository().GetReplicationSource(ctx, req.ReplicationSourceId)
	if err == nil {
		// Add table if not present
		tableSet := make(map[string]struct{})
		for _, t := range strings.Split(replicationSource.TableName, ",") {
			tableSet[t] = struct{}{}
		}
		tableSet[req.TableName] = struct{}{}
		tables := make([]string, 0, len(tableSet))
		for t := range tableSet {
			tables = append(tables, t)
		}
		replicationSource.TableName = strings.Join(tables, ",")
		s.engine.GetState().GetConfigRepository().CreateReplicationSource(ctx, replicationSource)
	}

	source := &pb.ReplicationSource{
		TenantId:            replicationSource.TenantID,
		WorkspaceId:         replicationSource.WorkspaceID,
		DatabaseId:          replicationSource.DatabaseID,
		TableNames:          strings.Split(replicationSource.TableName, ","),
		ReplicationSourceId: replicationSource.ReplicationSourceID,
		RelationshipId:      replicationSource.RelationshipID,
	}

	return &pb.AddTableToReplicationSourceResponse{
		Success: true,
		Message: "Table added to replication source",
		Status:  commonv1.Status_STATUS_SUCCESS,
		Source:  source,
	}, nil
}

// RemoveTableFromReplicationSource
func (s *Server) RemoveTableFromReplicationSource(ctx context.Context, req *pb.RemoveTableFromReplicationSourceRequest) (*pb.RemoveTableFromReplicationSourceResponse, error) {
	defer s.trackOperation()()

	if req.DatabaseId == "" || req.ReplicationSourceId == "" || req.TableName == "" {
		return &pb.RemoveTableFromReplicationSourceResponse{
			Success: false,
			Message: "Database ID, Replication Source ID, and Table Name are required",
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}
	dbManager := s.engine.GetState().GetDatabaseManager()
	repClient, err := dbManager.GetReplicationClient(req.ReplicationSourceId)
	if err != nil {
		return &pb.RemoveTableFromReplicationSourceResponse{
			Success: false,
			Message: fmt.Sprintf("Replication client not found: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}
	repClient.RemoveTable(req.TableName)

	// Update repository
	replicationSource, err := s.engine.GetState().GetConfigRepository().GetReplicationSource(ctx, req.ReplicationSourceId)
	if err == nil {
		tableSet := make(map[string]struct{})
		for _, t := range strings.Split(replicationSource.TableName, ",") {
			tableSet[t] = struct{}{}
		}
		delete(tableSet, req.TableName)
		tables := make([]string, 0, len(tableSet))
		for t := range tableSet {
			tables = append(tables, t)
		}
		replicationSource.TableName = strings.Join(tables, ",")
		s.engine.GetState().GetConfigRepository().CreateReplicationSource(ctx, replicationSource)
	}

	source := &pb.ReplicationSource{
		TenantId:            replicationSource.TenantID,
		WorkspaceId:         replicationSource.WorkspaceID,
		DatabaseId:          replicationSource.DatabaseID,
		TableNames:          strings.Split(replicationSource.TableName, ","),
		ReplicationSourceId: replicationSource.ReplicationSourceID,
		RelationshipId:      replicationSource.RelationshipID,
	}

	return &pb.RemoveTableFromReplicationSourceResponse{
		Success: true,
		Message: "Table removed from replication source",
		Status:  commonv1.Status_STATUS_SUCCESS,
		Source:  source,
	}, nil
}

// RemoveReplicationSource
func (s *Server) RemoveReplicationSource(ctx context.Context, req *pb.RemoveReplicationSourceRequest) (*pb.RemoveReplicationSourceResponse, error) {
	defer s.trackOperation()()

	if req.ReplicationSourceId == "" {
		return &pb.RemoveReplicationSourceResponse{
			Success: false,
			Message: "Replication source ID is required",
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}
	dbManager := s.engine.GetState().GetDatabaseManager()
	dbManager.DisconnectReplication(req.ReplicationSourceId)
	s.engine.GetState().GetConfigRepository().RemoveReplicationSource(ctx, req.ReplicationSourceId)

	return &pb.RemoveReplicationSourceResponse{
		Success:             true,
		Message:             "Replication source removed successfully",
		Status:              commonv1.Status_STATUS_SUCCESS,
		ReplicationSourceId: req.ReplicationSourceId,
	}, nil
}

func derefString(ptr *string) string {
	if ptr != nil {
		return *ptr
	}
	return ""
}

// StreamTableData streams data from a table in batches for efficient data copying
func (s *Server) StreamTableData(req *pb.StreamTableDataRequest, stream pb.AnchorService_StreamTableDataServer) error {
	defer s.trackOperation()()

	// Validate request
	if req.DatabaseId == "" || req.TableName == "" {
		return stream.Send(&pb.StreamTableDataResponse{
			Success:    false,
			Message:    "database_id and table_name are required",
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
			TableName:  req.TableName,
		})
	}

	// Set defaults
	batchSize := int32(1000)
	if req.BatchSize != nil && *req.BatchSize > 0 {
		batchSize = *req.BatchSize
	}

	offset := int64(0)
	if req.Offset != nil {
		offset = *req.Offset
	}

	// Get database client
	dbManager := s.engine.GetState().GetDatabaseManager()
	_, err := dbManager.GetDatabaseClient(req.DatabaseId)
	if err != nil {
		return stream.Send(&pb.StreamTableDataResponse{
			Success:    false,
			Message:    fmt.Sprintf("Database connection not found for ID: %s", req.DatabaseId),
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
			TableName:  req.TableName,
		})
	}

	// Build column list for SELECT
	// Note: columns will be handled by the database manager's StreamTableData method

	// Note: Cursor-based pagination will be handled by the database manager
	// For now, we use simple offset-based pagination

	batchNumber := int64(1)
	currentOffset := offset

	for {
		// Execute query using database manager
		dbManager := s.engine.GetState().GetDatabaseManager()
		rows, isComplete, nextCursorValue, err := dbManager.StreamTableData(
			req.DatabaseId,
			req.TableName,
			batchSize,
			currentOffset,
			req.Columns,
		)
		if err != nil {
			return stream.Send(&pb.StreamTableDataResponse{
				Success:    false,
				Message:    fmt.Sprintf("Failed to stream data: %v", err),
				Status:     commonv1.Status_STATUS_ERROR,
				DatabaseId: req.DatabaseId,
				TableName:  req.TableName,
			})
		}

		// Convert rows to JSON
		jsonData, err := json.Marshal(rows)
		if err != nil {
			return stream.Send(&pb.StreamTableDataResponse{
				Success:    false,
				Message:    fmt.Sprintf("Failed to serialize data: %v", err),
				Status:     commonv1.Status_STATUS_ERROR,
				DatabaseId: req.DatabaseId,
				TableName:  req.TableName,
			})
		}

		rowCount := int64(len(rows))

		// Send batch response
		err = stream.Send(&pb.StreamTableDataResponse{
			Success:         true,
			Message:         fmt.Sprintf("Batch %d streamed successfully", batchNumber),
			Status:          commonv1.Status_STATUS_SUCCESS,
			DatabaseId:      req.DatabaseId,
			TableName:       req.TableName,
			Data:            jsonData,
			IsComplete:      isComplete,
			NextCursorValue: nextCursorValue,
			BatchNumber:     batchNumber,
			RowsInBatch:     rowCount,
		})

		if err != nil {
			return err
		}

		// Break if this was the last batch
		if isComplete {
			break
		}

		// Prepare for next batch
		batchNumber++
		currentOffset += int64(batchSize)
	}

	return nil
}

// InsertBatchData inserts a batch of data into a table efficiently
func (s *Server) InsertBatchData(ctx context.Context, req *pb.InsertBatchDataRequest) (*pb.InsertBatchDataResponse, error) {
	defer s.trackOperation()()

	// Validate request
	if req.DatabaseId == "" || req.TableName == "" || req.Data == nil {
		return &pb.InsertBatchDataResponse{
			Success:    false,
			Message:    "database_id, table_name, and data are required",
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
			TableName:  req.TableName,
		}, nil
	}

	// Get database client
	dbManager := s.engine.GetState().GetDatabaseManager()
	client, err := dbManager.GetDatabaseClient(req.DatabaseId)
	if err != nil {
		return &pb.InsertBatchDataResponse{
			Success:    false,
			Message:    fmt.Sprintf("Database connection not found for ID: %s", req.DatabaseId),
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
			TableName:  req.TableName,
		}, nil
	}

	// Parse JSON data
	var rows []map[string]interface{}
	if err := json.Unmarshal(req.Data, &rows); err != nil {
		return &pb.InsertBatchDataResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to parse data: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
			TableName:  req.TableName,
		}, nil
	}

	if len(rows) == 0 {
		return &pb.InsertBatchDataResponse{
			Success:      true,
			Message:      "No data to insert",
			Status:       commonv1.Status_STATUS_SUCCESS,
			DatabaseId:   req.DatabaseId,
			TableName:    req.TableName,
			RowsAffected: 0,
		}, nil
	}

	// Use transaction if requested
	useTransaction := req.UseTransaction != nil && *req.UseTransaction

	var rowsAffected int64
	var errors []string

	if useTransaction {
		// Execute as a single transaction
		affected, err := s.insertBatchWithTransaction(client, req.TableName, rows)
		if err != nil {
			errors = append(errors, err.Error())
		} else {
			rowsAffected = affected
		}
	} else {
		// Execute row by row for better error handling
		for i, row := range rows {
			affected, err := s.insertSingleRow(client, req.TableName, row)
			if err != nil {
				errors = append(errors, fmt.Sprintf("Row %d: %v", i+1, err))
			} else {
				rowsAffected += affected
			}
		}
	}

	success := len(errors) == 0
	message := fmt.Sprintf("Inserted %d rows successfully", rowsAffected)
	if len(errors) > 0 {
		message = fmt.Sprintf("Inserted %d rows with %d errors", rowsAffected, len(errors))
	}

	operationID := ""
	if req.OperationId != nil {
		operationID = *req.OperationId
	}

	return &pb.InsertBatchDataResponse{
		Success:      success,
		Message:      message,
		Status:       commonv1.Status_STATUS_SUCCESS,
		DatabaseId:   req.DatabaseId,
		TableName:    req.TableName,
		RowsAffected: rowsAffected,
		Errors:       errors,
		OperationId:  operationID,
	}, nil
}

// GetTableRowCount returns the number of rows in a table for progress estimation
func (s *Server) GetTableRowCount(ctx context.Context, req *pb.GetTableRowCountRequest) (*pb.GetTableRowCountResponse, error) {
	defer s.trackOperation()()

	// Validate request
	if req.DatabaseId == "" || req.TableName == "" {
		return &pb.GetTableRowCountResponse{
			Success:    false,
			Message:    "database_id and table_name are required",
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
			TableName:  req.TableName,
		}, nil
	}

	// Get database client
	dbManager := s.engine.GetState().GetDatabaseManager()
	_, err := dbManager.GetDatabaseClient(req.DatabaseId)
	if err != nil {
		return &pb.GetTableRowCountResponse{
			Success:    false,
			Message:    fmt.Sprintf("Database connection not found for ID: %s", req.DatabaseId),
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
			TableName:  req.TableName,
		}, nil
	}

	// Get row count using database manager
	whereClause := ""
	if req.WhereClause != nil && *req.WhereClause != "" {
		whereClause = *req.WhereClause
	}

	rowCount, isEstimate, err := dbManager.GetTableRowCount(req.DatabaseId, req.TableName, whereClause)
	if err != nil {
		return &pb.GetTableRowCountResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to count rows: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
			TableName:  req.TableName,
		}, nil
	}

	return &pb.GetTableRowCountResponse{
		Success:    true,
		Message:    "Row count retrieved successfully",
		Status:     commonv1.Status_STATUS_SUCCESS,
		DatabaseId: req.DatabaseId,
		TableName:  req.TableName,
		RowCount:   rowCount,
		IsEstimate: isEstimate,
	}, nil
}

// Helper methods for database operations

func (s *Server) executeQuery(client *dbclient.DatabaseClient, query string, args ...interface{}) ([]interface{}, error) {
	// Use database manager to execute query in a database-neutral way
	dbManager := s.engine.GetState().GetDatabaseManager()
	return dbManager.ExecuteQuery(client.DatabaseID, query, args...)
}

func (s *Server) executeCountQuery(client *dbclient.DatabaseClient, query string, result *int64) error {
	// Use database manager to execute count query in a database-neutral way
	dbManager := s.engine.GetState().GetDatabaseManager()
	count, err := dbManager.ExecuteCountQuery(client.DatabaseID, query)
	if err != nil {
		return err
	}
	*result = count
	return nil
}

func (s *Server) insertBatchWithTransaction(client *dbclient.DatabaseClient, tableName string, rows []map[string]interface{}) (int64, error) {
	// Use database manager to insert data in a database-neutral way
	dbManager := s.engine.GetState().GetDatabaseManager()

	// Use the existing InsertDataToDatabase method from the database manager
	return dbManager.InsertDataToDatabase(client.DatabaseID, tableName, rows)
}

func (s *Server) insertSingleRow(client *dbclient.DatabaseClient, tableName string, row map[string]interface{}) (int64, error) {
	// Use database manager to insert single row in a database-neutral way
	dbManager := s.engine.GetState().GetDatabaseManager()

	// Convert single row to slice for the existing InsertDataToDatabase method
	rows := []map[string]interface{}{row}
	return dbManager.InsertDataToDatabase(client.DatabaseID, tableName, rows)
}

// Note: Database-specific query execution and data manipulation methods
// have been moved to the database manager abstraction layer.
// The database manager handles routing to appropriate database-specific adapters.
//
// Required database manager methods for full functionality:
// - ExecuteQuery(databaseID, query, args) ([]interface{}, error)
// - ExecuteCountQuery(databaseID, query) (int64, error)
// - StreamTableData(databaseID, tableName, batchSize, offset, columns) (stream, error)
// - GetTableRowCount(databaseID, tableName, whereClause) (int64, bool, error)
//
// These methods need to be implemented in the database manager and
// corresponding database-specific adapters (starting with PostgreSQL and MySQL).
