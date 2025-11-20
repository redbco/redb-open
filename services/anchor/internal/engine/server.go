package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	pb "github.com/redbco/redb-open/api/proto/anchor/v1"
	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/config"
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
	_, err = s.engine.GetState().GetConnectionRegistry().ConnectInstance(instanceConfig)
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

	// TODO: Collect and store instance metadata via adapter
	// For now, metadata collection is temporarily disabled during migration

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
		s.engine.GetState().GetConnectionRegistry().DisconnectInstance(req.InstanceId)

		return &pb.DisconnectInstanceResponse{
			Success:    false,
			Message:    fmt.Sprintf("Instance not found in repository: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			InstanceId: req.InstanceId,
		}, nil
	}

	// Attempt to disconnect the instance from DatabaseManager
	err = s.engine.GetState().GetConnectionRegistry().DisconnectInstance(req.InstanceId)
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
	_, err = s.engine.GetState().GetConnectionRegistry().ConnectDatabase(dbConfig)
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
	err := s.engine.GetState().GetConnectionRegistry().DisconnectDatabase(req.DatabaseId)
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

	// Get metadata via adapter
	registry := s.engine.GetState().GetConnectionRegistry()
	instance, err := registry.GetInstanceClient(req.InstanceId)
	if err != nil {
		return &pb.GetInstanceMetadataResponse{
			Success: false,
			Message: fmt.Sprintf("Instance not found: %v", err),
		}, nil
	}

	conn := instance.AdapterConnection.(adapter.InstanceConnection)
	metadataMap, err := conn.MetadataOperations().CollectInstanceMetadata(ctx)
	if err != nil {
		return &pb.GetInstanceMetadataResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to retrieve instance metadata: %v", err),
			InstanceId: req.InstanceId,
			Metadata:   nil,
		}, nil
	}

	// Convert metadata to JSON
	metadataData, err := json.Marshal(metadataMap)
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
	registry := s.engine.GetState().GetConnectionRegistry()
	instance, err := registry.GetInstanceClient(req.InstanceId)
	if err != nil {
		return &pb.CreateDatabaseResponse{
			Success: false,
			Message: fmt.Sprintf("Instance not found: %v", err),
		}, nil
	}
	conn := instance.AdapterConnection.(adapter.InstanceConnection)
	err = conn.CreateDatabase(ctx, req.DatabaseName, options)
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

	// Drop the database via adapter
	registry := s.engine.GetState().GetConnectionRegistry()
	client, err := registry.GetDatabaseClient(req.DatabaseId)
	if err != nil {
		return &pb.DropDatabaseResponse{
			Success: false,
			Message: fmt.Sprintf("Database not found: %v", err),
		}, nil
	}

	// For drop, we need the instance connection and database name
	registry2 := s.engine.GetState().GetConnectionRegistry()
	instance, err := registry2.GetInstanceClient(client.Config.InstanceID)
	if err != nil {
		return &pb.DropDatabaseResponse{
			Success: false,
			Message: fmt.Sprintf("Instance not found: %v", err),
		}, nil
	}

	conn := instance.AdapterConnection.(adapter.InstanceConnection)
	err = conn.DropDatabase(ctx, client.Config.DatabaseName, options)
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

	// Get metadata via adapter
	registry := s.engine.GetState().GetConnectionRegistry()
	client, err := registry.GetDatabaseClient(req.DatabaseId)
	if err != nil {
		return &pb.GetDatabaseMetadataResponse{
			Success: false,
			Message: fmt.Sprintf("Database not found: %v", err),
		}, nil
	}

	conn := client.AdapterConnection.(adapter.Connection)
	metadataMap, err := conn.MetadataOperations().CollectDatabaseMetadata(ctx)
	if err != nil {
		return &pb.GetDatabaseMetadataResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to retrieve database metadata: %v", err),
			DatabaseId: req.DatabaseId,
			Metadata:   nil,
		}, nil
	}

	// Convert metadata to JSON
	metadataData, err := json.Marshal(metadataMap)
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

	// Get database structure via adapter
	registry := s.engine.GetState().GetConnectionRegistry()
	client, err := registry.GetDatabaseClient(req.DatabaseId)
	if err != nil {
		return &pb.GetDatabaseSchemaResponse{
			Success:    false,
			Message:    fmt.Sprintf("Database not found: %v", err),
			DatabaseId: req.DatabaseId,
			Schema:     nil,
		}, nil
	}

	conn := client.AdapterConnection.(adapter.Connection)
	structure, err := conn.SchemaOperations().DiscoverSchema(ctx)
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

func (s *Server) RefreshDatabaseDiscovery(ctx context.Context, req *pb.RefreshDatabaseDiscoveryRequest) (*pb.RefreshDatabaseDiscoveryResponse, error) {
	defer s.trackOperation()()

	s.engine.logger.Infof("Refreshing database discovery for database: %s", req.DatabaseId)

	// Delegate to the schema watcher to perform the discovery refresh
	containersCreated, itemsCreated, err := s.engine.schemaWatcher.RefreshResourceRegistry(ctx, req.DatabaseId)
	if err != nil {
		return &pb.RefreshDatabaseDiscoveryResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to refresh discovery: %v", err),
			DatabaseId: req.DatabaseId,
			Status:     commonv1.Status_STATUS_ERROR,
		}, nil
	}

	s.engine.logger.Infof("Successfully refreshed discovery for database %s: %d containers, %d items created",
		req.DatabaseId, containersCreated, itemsCreated)

	return &pb.RefreshDatabaseDiscoveryResponse{
		Success:           true,
		Message:           fmt.Sprintf("Successfully refreshed discovery for database %s", req.DatabaseId),
		DatabaseId:        req.DatabaseId,
		Status:            commonv1.Status_STATUS_SUCCESS,
		ContainersCreated: int32(containersCreated),
		ItemsCreated:      int32(itemsCreated),
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

	// Deploy the database structure via adapter
	registry := s.engine.GetState().GetConnectionRegistry()
	client, err := registry.GetDatabaseClient(req.DatabaseId)
	if err != nil {
		return &pb.DeployDatabaseSchemaResponse{
			Success:    false,
			Message:    fmt.Sprintf("Database not found: %v", err),
			DatabaseId: req.DatabaseId,
		}, nil
	}

	conn := client.AdapterConnection.(adapter.Connection)
	err = conn.SchemaOperations().CreateStructure(ctx, structure)
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

	// Parse options for pagination if provided
	if len(req.Options) > 0 {
		var options map[string]interface{}
		if err := json.Unmarshal(req.Options, &options); err == nil {
			if limitVal, ok := options["limit"].(float64); ok {
				limit = int(limitVal)
			}
			// Note: offset is parsed but not currently used by adapters
			// Most adapters don't support offset-based pagination natively
		}
	}

	// Get data from the database via adapter
	registry := s.engine.GetState().GetConnectionRegistry()
	client, err := registry.GetDatabaseClient(req.DatabaseId)
	if err != nil {
		return &pb.FetchDataResponse{
			Message:    fmt.Sprintf("Database not found: %v", err),
			DatabaseId: req.DatabaseId,
			TableName:  req.TableName,
			Data:       nil,
		}, nil
	}

	conn := client.AdapterConnection.(adapter.Connection)
	
	// Note: Most adapters don't support offset directly, so we fetch with limit
	// For proper pagination support, we would need to enhance each adapter
	// For now, we just use the limit parameter
	data, err := conn.DataOperations().Fetch(ctx, req.TableName, limit)
	
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

	// Insert data into the database via adapter
	registry := s.engine.GetState().GetConnectionRegistry()
	client, err := registry.GetDatabaseClient(req.DatabaseId)
	if err != nil {
		return &pb.InsertDataResponse{
			Success:      false,
			Message:      fmt.Sprintf("Database not found: %v", err),
			Status:       commonv1.Status_STATUS_ERROR,
			DatabaseId:   req.DatabaseId,
			TableName:    req.TableName,
			RowsAffected: 0,
		}, nil
	}

	conn := client.AdapterConnection.(adapter.Connection)
	rowsAffected, err := conn.DataOperations().Insert(ctx, req.TableName, data)
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

	// Wipe the database via adapter
	registry := s.engine.GetState().GetConnectionRegistry()
	client, err := registry.GetDatabaseClient(req.DatabaseId)
	if err != nil {
		return &pb.WipeDatabaseResponse{
			Success:    false,
			Message:    fmt.Sprintf("Database not found: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
		}, nil
	}

	conn := client.AdapterConnection.(adapter.Connection)
	// Wipe all tables - get list first
	tables, err := conn.SchemaOperations().ListTables(ctx)
	if err != nil {
		return &pb.WipeDatabaseResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to list tables: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
		}, nil
	}

	// Delete from each table
	for _, table := range tables {
		_, err = conn.DataOperations().Delete(ctx, table, make(map[string]interface{}))
		if err != nil {
			return &pb.WipeDatabaseResponse{
				Success:    false,
				Message:    fmt.Sprintf("Failed to wipe table %s: %v", table, err),
				Status:     commonv1.Status_STATUS_ERROR,
				DatabaseId: req.DatabaseId,
			}, nil
		}
	}

	return &pb.WipeDatabaseResponse{
		Success:    true,
		Message:    "Database wiped successfully",
		Status:     commonv1.Status_STATUS_SUCCESS,
		DatabaseId: req.DatabaseId,
	}, nil
}

func (s *Server) WipeTable(ctx context.Context, req *pb.WipeTableRequest) (*pb.WipeTableResponse, error) {
	defer s.trackOperation()()

	// Get database connection
	registry := s.engine.GetState().GetConnectionRegistry()
	client, err := registry.GetDatabaseClient(req.DatabaseId)
	if err != nil {
		return &pb.WipeTableResponse{
			Success:    false,
			Message:    fmt.Sprintf("Database not found: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
			TableName:  req.TableName,
		}, nil
	}

	conn := client.AdapterConnection.(adapter.Connection)
	
	// Delete all data from the table
	rowsAffected, err := conn.DataOperations().Delete(ctx, req.TableName, make(map[string]interface{}))
	if err != nil {
		return &pb.WipeTableResponse{
			Success:      false,
			Message:      fmt.Sprintf("Failed to wipe table %s: %v", req.TableName, err),
			Status:       commonv1.Status_STATUS_ERROR,
			DatabaseId:   req.DatabaseId,
			TableName:    req.TableName,
			RowsAffected: 0,
		}, nil
	}

	return &pb.WipeTableResponse{
		Success:      true,
		Message:      fmt.Sprintf("Table %s wiped successfully", req.TableName),
		Status:       commonv1.Status_STATUS_SUCCESS,
		DatabaseId:   req.DatabaseId,
		TableName:    req.TableName,
		RowsAffected: rowsAffected,
	}, nil
}

func (s *Server) DropTable(ctx context.Context, req *pb.DropTableRequest) (*pb.DropTableResponse, error) {
	defer s.trackOperation()()

	// Get database connection
	registry := s.engine.GetState().GetConnectionRegistry()
	_, err := registry.GetDatabaseClient(req.DatabaseId)
	if err != nil {
		return &pb.DropTableResponse{
			Success:    false,
			Message:    fmt.Sprintf("Database not found: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
			TableName:  req.TableName,
		}, nil
	}

	// Note: DropTable is not part of the SchemaOperator interface yet
	// For now, return an error indicating this needs to be implemented
	// TODO: Add DropTable to SchemaOperator interface and implement in all adapters
	return &pb.DropTableResponse{
		Success:    false,
		Message:    "Drop table operation not yet fully implemented - needs adapter interface update",
		Status:     commonv1.Status_STATUS_ERROR,
		DatabaseId: req.DatabaseId,
		TableName:  req.TableName,
	}, nil
	
	/* Future implementation when DropTable is added to interface:
	conn := client.AdapterConnection.(adapter.Connection)
	err = conn.SchemaOperations().DropTable(ctx, req.TableName)
	if err != nil {
		return &pb.DropTableResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to drop table %s: %v", req.TableName, err),
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
			TableName:  req.TableName,
		}, nil
	}

	return &pb.DropTableResponse{
		Success:    true,
		Message:    fmt.Sprintf("Table %s dropped successfully", req.TableName),
		Status:     commonv1.Status_STATUS_SUCCESS,
		DatabaseId: req.DatabaseId,
		TableName:  req.TableName,
	}, nil
	*/
}

func (s *Server) UpdateTableData(ctx context.Context, req *pb.UpdateTableDataRequest) (*pb.UpdateTableDataResponse, error) {
	defer s.trackOperation()()

	// Get database connection
	registry := s.engine.GetState().GetConnectionRegistry()
	client, err := registry.GetDatabaseClient(req.DatabaseId)
	if err != nil {
		return &pb.UpdateTableDataResponse{
			Success:    false,
			Message:    fmt.Sprintf("Database not found: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
			TableName:  req.TableName,
		}, nil
	}

	// Parse updates (JSON array of {where: {...}, set: {...}} operations)
	var updates []map[string]interface{}
	if err := json.Unmarshal(req.Updates, &updates); err != nil {
		return &pb.UpdateTableDataResponse{
			Success:      false,
			Message:      fmt.Sprintf("Failed to parse updates: %v", err),
			Status:       commonv1.Status_STATUS_ERROR,
			DatabaseId:   req.DatabaseId,
			TableName:    req.TableName,
			RowsAffected: 0,
		}, nil
	}

	conn := client.AdapterConnection.(adapter.Connection)
	
	// Execute each update operation
	var totalRowsAffected int64
	for _, update := range updates {
		whereClause, ok := update["where"].(map[string]interface{})
		if !ok {
			return &pb.UpdateTableDataResponse{
				Success:      false,
				Message:      "Invalid update format: missing or invalid 'where' clause",
				Status:       commonv1.Status_STATUS_ERROR,
				DatabaseId:   req.DatabaseId,
				TableName:    req.TableName,
				RowsAffected: totalRowsAffected,
			}, nil
		}

		setClause, ok := update["set"].(map[string]interface{})
		if !ok {
			return &pb.UpdateTableDataResponse{
				Success:      false,
				Message:      "Invalid update format: missing or invalid 'set' clause",
				Status:       commonv1.Status_STATUS_ERROR,
				DatabaseId:   req.DatabaseId,
				TableName:    req.TableName,
				RowsAffected: totalRowsAffected,
			}, nil
		}

		// Convert to adapter format: data is an array with the set values, whereColumns are the keys from where clause
		data := []map[string]interface{}{setClause}
		whereColumns := make([]string, 0, len(whereClause))
		for col := range whereClause {
			whereColumns = append(whereColumns, col)
		}

		rowsAffected, err := conn.DataOperations().Update(ctx, req.TableName, data, whereColumns)
		if err != nil {
			return &pb.UpdateTableDataResponse{
				Success:      false,
				Message:      fmt.Sprintf("Failed to update table data: %v", err),
				Status:       commonv1.Status_STATUS_ERROR,
				DatabaseId:   req.DatabaseId,
				TableName:    req.TableName,
				RowsAffected: totalRowsAffected,
			}, nil
		}
		totalRowsAffected += rowsAffected
	}

	return &pb.UpdateTableDataResponse{
		Success:      true,
		Message:      fmt.Sprintf("Updated %d rows in table %s", totalRowsAffected, req.TableName),
		Status:       commonv1.Status_STATUS_SUCCESS,
		DatabaseId:   req.DatabaseId,
		TableName:    req.TableName,
		RowsAffected: totalRowsAffected,
	}, nil
}

func (s *Server) ExecuteCommand(ctx context.Context, req *pb.ExecuteCommandRequest) (*pb.ExecuteCommandResponse, error) {
	defer s.trackOperation()()

	// Execute the command via adapter
	registry := s.engine.GetState().GetConnectionRegistry()
	client, err := registry.GetDatabaseClient(req.DatabaseId)
	if err != nil {
		return &pb.ExecuteCommandResponse{
			Success:    false,
			Message:    fmt.Sprintf("Database not found: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
			Command:    req.Command,
		}, nil
	}

	conn := client.AdapterConnection.(adapter.Connection)
	result, err := conn.DataOperations().ExecuteQuery(ctx, req.Command)
	if err != nil {
		return &pb.ExecuteCommandResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to execute command: %v", err),
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
			Command:    req.Command,
		}, nil
	}

	// Convert result to JSON bytes
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return &pb.ExecuteCommandResponse{
			Success:    false,
			Message:    fmt.Sprintf("Failed to marshal result: %v", err),
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
		Data:       resultJSON,
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

	registry := s.engine.GetState().GetConnectionRegistry()
	replicationID := req.DatabaseId // Use database ID as the replication client key

	// Check if a replication client already exists for this database
	repClient, err := registry.GetReplicationClient(replicationID)
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
		// Create replication client via adapter
		// Note: Replication is complex and database-specific. For now, create a
		// placeholder client that tracks the configuration.
		client := &dbclient.ReplicationClient{
			ReplicationID: replicationID,
			DatabaseID:    req.DatabaseId,
			Config:        replicationConfig,
			IsConnected:   1,
		}

		// Track the client in registry
		registry.AddReplicationClient(client)

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
	registry := s.engine.GetState().GetConnectionRegistry()
	repClient, err := registry.GetReplicationClient(req.ReplicationSourceId)
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
	registry := s.engine.GetState().GetConnectionRegistry()
	repClient, err := registry.GetReplicationClient(req.ReplicationSourceId)
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
	registry := s.engine.GetState().GetConnectionRegistry()
	registry.DisconnectReplication(req.ReplicationSourceId)
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
	ctx := stream.Context()

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
	registry := s.engine.GetState().GetConnectionRegistry()
	_, err := registry.GetDatabaseClient(req.DatabaseId)
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
		// Execute query via adapter
		registry := s.engine.GetState().GetConnectionRegistry()
		client, err := registry.GetDatabaseClient(req.DatabaseId)
		if err != nil {
			return stream.Send(&pb.StreamTableDataResponse{
				Success: false,
				Message: fmt.Sprintf("Database not found: %v", err),
			})
		}

		conn := client.AdapterConnection.(adapter.Connection)
		// Simple implementation - fetch with limit
		allRows, err := conn.DataOperations().Fetch(ctx, req.TableName, int(batchSize))
		if err != nil {
			return stream.Send(&pb.StreamTableDataResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to fetch data: %v", err),
			})
		}

		// Slice for current batch
		startIdx := int(currentOffset)
		endIdx := min(startIdx+int(batchSize), len(allRows))

		rows := allRows[startIdx:endIdx]
		isComplete := endIdx >= len(allRows)
		nextCursorValue := int64(endIdx)

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
			NextCursorValue: fmt.Sprintf("%d", nextCursorValue),
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
	registry := s.engine.GetState().GetConnectionRegistry()
	client, err := registry.GetDatabaseClient(req.DatabaseId)
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
	registry := s.engine.GetState().GetConnectionRegistry()
	client, err := registry.GetDatabaseClient(req.DatabaseId)
	if err != nil {
		return &pb.GetTableRowCountResponse{
			Success:    false,
			Message:    fmt.Sprintf("Database connection not found for ID: %s", req.DatabaseId),
			Status:     commonv1.Status_STATUS_ERROR,
			DatabaseId: req.DatabaseId,
			TableName:  req.TableName,
		}, nil
	}

	// Get row count via adapter - fetch all and count (simple implementation)
	conn := client.AdapterConnection.(adapter.Connection)
	rows, err := conn.DataOperations().Fetch(ctx, req.TableName, 1000000) // Large limit
	rowCount := int64(len(rows))
	isEstimate := false
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
	// Use adapter to execute query
	conn := client.AdapterConnection.(adapter.Connection)
	ctx := context.Background()
	return conn.DataOperations().ExecuteQuery(ctx, query, args...)
}

func (s *Server) executeCountQuery(client *dbclient.DatabaseClient, query string, result *int64) error {
	// Use adapter to execute count query
	conn := client.AdapterConnection.(adapter.Connection)
	ctx := context.Background()
	results, err := conn.DataOperations().ExecuteQuery(ctx, query)
	if err != nil {
		return err
	}
	// Extract count from first result
	if len(results) > 0 {
		if countMap, ok := results[0].(map[string]interface{}); ok {
			if countVal, ok := countMap["count"]; ok {
				if count, ok := countVal.(int64); ok {
					*result = count
					return nil
				}
			}
		}
	}
	*result = 0
	return nil
}

func (s *Server) insertBatchWithTransaction(client *dbclient.DatabaseClient, tableName string, rows []map[string]interface{}) (int64, error) {
	// Use adapter to insert data
	conn := client.AdapterConnection.(adapter.Connection)
	ctx := context.Background()
	return conn.DataOperations().Insert(ctx, tableName, rows)
}

func (s *Server) insertSingleRow(client *dbclient.DatabaseClient, tableName string, row map[string]interface{}) (int64, error) {
	// Use adapter to insert single row
	conn := client.AdapterConnection.(adapter.Connection)
	ctx := context.Background()
	rows := []map[string]interface{}{row}
	return conn.DataOperations().Insert(ctx, tableName, rows)
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

// CDC Replication Management Methods

// StartCDCReplication starts CDC replication for a relationship
func (s *Server) StartCDCReplication(ctx context.Context, req *pb.StartCDCReplicationRequest) (*pb.StartCDCReplicationResponse, error) {
	defer s.trackOperation()()
	return s.engine.StartCDCReplication(ctx, req)
}

// StopCDCReplication stops CDC replication
func (s *Server) StopCDCReplication(ctx context.Context, req *pb.StopCDCReplicationRequest) (*pb.StopCDCReplicationResponse, error) {
	defer s.trackOperation()()
	return s.engine.StopCDCReplication(ctx, req)
}

// ResumeCDCReplication resumes a stopped CDC replication
func (s *Server) ResumeCDCReplication(ctx context.Context, req *pb.ResumeCDCReplicationRequest) (*pb.ResumeCDCReplicationResponse, error) {
	defer s.trackOperation()()
	return s.engine.ResumeCDCReplication(ctx, req)
}

// GetCDCReplicationStatus gets the status of a CDC replication
func (s *Server) GetCDCReplicationStatus(ctx context.Context, req *pb.GetCDCReplicationStatusRequest) (*pb.GetCDCReplicationStatusResponse, error) {
	defer s.trackOperation()()
	return s.engine.GetCDCReplicationStatus(ctx, req)
}

// StreamCDCEvents streams CDC events
func (s *Server) StreamCDCEvents(req *pb.StreamCDCEventsRequest, stream pb.AnchorService_StreamCDCEventsServer) error {
	defer s.trackOperation()()
	return s.engine.StreamCDCEvents(req, stream)
}

// extractContainerURIFromItemURI extracts the container URI from an item URI
func extractContainerURIFromItemURI(itemURI string) string {
	parts := strings.Split(itemURI, "/")

	for i := 0; i < len(parts)-2; i++ {
		segment := parts[i]
		if segment == "table" || segment == "collection" || segment == "view" ||
			segment == "materialized_view" || segment == "graph_node" ||
			segment == "graph_edge" || segment == "topic" || segment == "stream" {
			if i+1 < len(parts) {
				return strings.Join(parts[:i+2], "/")
			}
		}
	}

	return itemURI
}

