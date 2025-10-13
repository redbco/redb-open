package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/services/core/internal/services/database"
	"github.com/redbco/redb-open/services/core/internal/services/instance"
	"github.com/redbco/redb-open/services/core/internal/services/mapping"
	"github.com/redbco/redb-open/services/core/internal/services/workspace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func (s *Server) ListDatabases(ctx context.Context, req *corev1.ListDatabasesRequest) (*corev1.ListDatabasesResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get database service
	databaseService := database.NewService(s.engine.db, s.engine.logger)
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// List databases for the tenant and workspace
	databases, err := databaseService.List(ctx, req.TenantId, workspaceID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to list databases: %v", err)
	}

	// Convert to protobuf format
	protoDatabases := make([]*corev1.Database, len(databases))
	for i, db := range databases {
		protoDatabases[i] = s.databaseToProto(db)
	}

	return &corev1.ListDatabasesResponse{
		Databases: protoDatabases,
	}, nil
}

func (s *Server) ShowDatabase(ctx context.Context, req *corev1.ShowDatabaseRequest) (*corev1.ShowDatabaseResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get database service
	databaseService := database.NewService(s.engine.db, s.engine.logger)
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get the database
	db, err := databaseService.Get(ctx, req.TenantId, workspaceID, req.DatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "database not found: %v", err)
	}

	// Convert to protobuf format
	protoDatabase := s.databaseToProto(db)

	return &corev1.ShowDatabaseResponse{
		Database: protoDatabase,
	}, nil
}

func (s *Server) ConnectDatabase(ctx context.Context, req *corev1.ConnectDatabaseRequest) (*corev1.ConnectDatabaseResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get services
	databaseService := database.NewService(s.engine.db, s.engine.logger)
	instanceService := instance.NewService(s.engine.db, s.engine.logger)
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Set default values
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	ssl := false
	if req.Ssl != nil {
		ssl = *req.Ssl
	}

	sslMode := "disable"
	if req.SslMode != nil {
		sslMode = *req.SslMode
	}

	// Generate unique instance name for this database
	instanceName := fmt.Sprintf("%s_instance", req.DatabaseName)

	// Handle optional environment ID
	environmentID := ""
	if req.EnvironmentId != nil {
		environmentID = *req.EnvironmentId
	}

	// Create instance object
	instanceObj, err := instanceService.Create(
		ctx,
		req.TenantId,
		req.WorkspaceName,
		instanceName,
		fmt.Sprintf("Instance for database %s", req.DatabaseName),
		req.DatabaseType,
		req.DatabaseVendor,
		req.Host,
		req.Username,
		req.Password,
		req.NodeId,
		req.Port,
		enabled,
		ssl,
		sslMode,
		environmentID,
		req.OwnerId,
		req.SslCert,
		req.SslKey,
		req.SslRootCert,
		&req.DbName,
	)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create instance: %v", err)
	}

	// Resolve workspace ID from workspace name for database creation
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Create database object
	databaseObj, err := databaseService.Create(
		ctx,
		req.TenantId,
		workspaceID,
		req.DatabaseName,
		req.DatabaseDescription,
		req.DatabaseType,
		req.DatabaseVendor,
		req.Username,
		req.Password,
		req.DbName,
		req.NodeId,
		enabled,
		environmentID,
		instanceObj.ID,
		req.OwnerId,
	)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create database: %v", err)
	}

	// Get anchor service address using dynamic resolution
	anchorAddr := s.engine.getServiceAddress("anchor")

	// Connect to anchor service
	anchorConn, err := grpc.Dial(anchorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect to anchor service at %s: %v", anchorAddr, err)
	}
	defer anchorConn.Close()

	anchorClient := anchorv1.NewAnchorServiceClient(anchorConn)

	// Send ConnectInstance gRPC request to anchor service
	instanceReq := &anchorv1.ConnectInstanceRequest{
		TenantId:    req.TenantId,
		WorkspaceId: instanceObj.WorkspaceID,
		InstanceId:  instanceObj.ID,
	}

	instanceResp, err := anchorClient.ConnectInstance(ctx, instanceReq)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect instance via anchor service: %v", err)
	}

	if !instanceResp.Success {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "anchor service failed to connect instance: %s", instanceResp.Message)
	}

	// Send ConnectDatabase gRPC request to anchor service
	databaseReq := &anchorv1.ConnectDatabaseRequest{
		TenantId:    req.TenantId,
		WorkspaceId: databaseObj.WorkspaceID,
		DatabaseId:  databaseObj.ID,
	}

	databaseResp, err := anchorClient.ConnectDatabase(ctx, databaseReq)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect database via anchor service: %v", err)
	}

	if !databaseResp.Success {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "anchor service failed to connect database: %s", databaseResp.Message)
	}

	// Convert to protobuf format
	protoDatabase := s.databaseToProto(databaseObj)

	// Broadcast instance and database creation to other mesh nodes asynchronously
	// Note: We MUST broadcast instance BEFORE database due to foreign key constraint
	go func() {
		s.engine.logger.Debugf("Starting broadcast goroutine for instance and database")
		syncMgr := s.engine.GetSyncManager()
		if syncMgr == nil {
			s.engine.logger.Warnf("Sync manager is nil, cannot broadcast instance/database creation")
			return
		}

		broadcastCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		shouldBroadcast, err := syncMgr.ShouldBroadcastUserData(broadcastCtx)
		if err != nil {
			s.engine.logger.Errorf("Error checking if should broadcast: %v", err)
			return
		}

		s.engine.logger.Debugf("Should broadcast user data: %v", shouldBroadcast)
		if shouldBroadcast {
			// Broadcast instance creation FIRST (synchronously to ensure order)
			s.engine.logger.Infof("Broadcasting instance creation: %s", instanceObj.ID)
			instanceRecordData := s.instanceToRecordData(instanceObj)
			instancePrimaryKey := map[string]interface{}{"instance_id": instanceObj.ID}
			if err := syncMgr.BroadcastUserDataOperationSync(broadcastCtx, "instances", "INSERT", instanceRecordData, instancePrimaryKey); err != nil {
				s.engine.logger.Errorf("Failed to broadcast instance creation: %v", err)
				// Don't broadcast database if instance broadcast failed
				return
			}
			s.engine.logger.Infof("Successfully broadcasted instance %s", instanceObj.ID)

			// Now broadcast database creation (after instance is broadcasted)
			s.engine.logger.Infof("Broadcasting database creation: %s", databaseObj.ID)
			databaseRecordData := s.databaseToRecordData(databaseObj)
			databasePrimaryKey := map[string]interface{}{"database_id": databaseObj.ID}
			if err := syncMgr.BroadcastUserDataOperationSync(broadcastCtx, "databases", "INSERT", databaseRecordData, databasePrimaryKey); err != nil {
				s.engine.logger.Errorf("Failed to broadcast database creation: %v", err)
				return
			}
			s.engine.logger.Infof("Successfully broadcasted database %s", databaseObj.ID)
		} else {
			s.engine.logger.Debugf("Not broadcasting: either not in mesh or only one node")
		}
	}()

	return &corev1.ConnectDatabaseResponse{
		Message:  fmt.Sprintf("Database %s connected successfully", databaseObj.Name),
		Success:  true,
		Database: protoDatabase,
		Status:   commonv1.Status_STATUS_CONNECTED,
	}, nil
}

func (s *Server) ConnectDatabaseWithInstance(ctx context.Context, req *corev1.ConnectDatabaseWithInstanceRequest) (*corev1.ConnectDatabaseWithInstanceResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get services
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	databaseService := database.NewService(s.engine.db, s.engine.logger)
	instanceService := instance.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get existing instance details
	instanceObj, err := instanceService.Get(ctx, req.TenantId, req.WorkspaceName, req.InstanceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "instance not found: %v", err)
	}

	// Set default values
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	// Use instance credentials if not provided in request
	username := instanceObj.Username
	if req.Username != nil && *req.Username != "" {
		username = *req.Username
	}

	password := instanceObj.Password
	if req.Password != nil && *req.Password != "" {
		password = *req.Password
	}

	// Use provided environment ID or inherit from instance
	environmentID := ""
	if req.EnvironmentId != nil {
		environmentID = *req.EnvironmentId
	} else if instanceObj.EnvironmentID != nil {
		environmentID = *instanceObj.EnvironmentID
	}

	// Create database object
	databaseObj, err := databaseService.Create(
		ctx,
		req.TenantId,
		workspaceID,
		req.DatabaseName,
		req.DatabaseDescription,
		instanceObj.Type,   // Use instance type
		instanceObj.Vendor, // Use instance vendor
		username,
		password,
		req.DbName,
		req.NodeId,
		enabled,
		environmentID,
		instanceObj.ID,
		req.OwnerId,
	)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create database: %v", err)
	}

	// Get anchor service address using dynamic resolution
	anchorAddr := s.engine.getServiceAddress("anchor")

	// Connect to anchor service
	anchorConn, err := grpc.Dial(anchorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect to anchor service at %s: %v", anchorAddr, err)
	}
	defer anchorConn.Close()

	anchorClient := anchorv1.NewAnchorServiceClient(anchorConn)

	// If existing instance is disconnected or disabled, reconnect it
	if !instanceObj.Enabled || instanceObj.Status == "STATUS_DISCONNECTED" || instanceObj.Status == "STATUS_STOPPED" {
		instanceReq := &anchorv1.ConnectInstanceRequest{
			TenantId:    req.TenantId,
			WorkspaceId: instanceObj.WorkspaceID,
			InstanceId:  instanceObj.ID,
		}

		instanceResp, err := anchorClient.ConnectInstance(ctx, instanceReq)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to connect instance via anchor service: %v", err)
		}

		if !instanceResp.Success {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "anchor service failed to connect instance: %s", instanceResp.Message)
		}
	}

	// Send ConnectDatabase gRPC request to anchor service
	databaseReq := &anchorv1.ConnectDatabaseRequest{
		TenantId:    req.TenantId,
		WorkspaceId: databaseObj.WorkspaceID,
		DatabaseId:  databaseObj.ID,
	}

	databaseResp, err := anchorClient.ConnectDatabase(ctx, databaseReq)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect database via anchor service: %v", err)
	}

	if !databaseResp.Success {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "anchor service failed to connect database: %s", databaseResp.Message)
	}

	// Convert to protobuf format
	protoDatabase := s.databaseToProto(databaseObj)

	return &corev1.ConnectDatabaseWithInstanceResponse{
		Message:  fmt.Sprintf("Database %s connected with instance %s successfully", req.DatabaseName, req.InstanceName),
		Success:  true,
		Database: protoDatabase,
		Status:   commonv1.Status_STATUS_CONNECTED,
	}, nil
}

func (s *Server) ReconnectDatabase(ctx context.Context, req *corev1.ReconnectDatabaseRequest) (*corev1.ReconnectDatabaseResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get services
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	databaseService := database.NewService(s.engine.db, s.engine.logger)
	instanceService := instance.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get the database
	db, err := databaseService.Get(ctx, req.TenantId, workspaceID, req.DatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "database not found: %v", err)
	}

	// Enable the database
	err = databaseService.Enable(ctx, req.TenantId, workspaceID, req.DatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to enable database: %v", err)
	}

	// Get anchor service address using dynamic resolution
	anchorAddr := s.engine.getServiceAddress("anchor")

	// Connect to anchor service
	anchorConn, err := grpc.Dial(anchorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect to anchor service at %s: %v", anchorAddr, err)
	}
	defer anchorConn.Close()

	anchorClient := anchorv1.NewAnchorServiceClient(anchorConn)

	// If the database has an associated instance, ensure it's also connected
	if db.InstanceID != "" {
		// Get instance details
		var instanceName string
		err = s.engine.db.Pool().QueryRow(ctx, "SELECT instance_name FROM instances WHERE instance_id = $1", db.InstanceID).Scan(&instanceName)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to get instance name: %v", err)
		}

		// Get instance to check its status
		inst, err := instanceService.Get(ctx, req.TenantId, req.WorkspaceName, instanceName)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to get instance: %v", err)
		}

		// If instance is disconnected or disabled, reconnect it first
		if !inst.Enabled || inst.Status == "STATUS_DISCONNECTED" || inst.Status == "STATUS_STOPPED" {
			instanceReq := &anchorv1.ConnectInstanceRequest{
				TenantId:    req.TenantId,
				WorkspaceId: inst.WorkspaceID,
				InstanceId:  inst.ID,
			}

			instanceResp, err := anchorClient.ConnectInstance(ctx, instanceReq)
			if err != nil {
				s.engine.IncrementErrors()
				return nil, status.Errorf(codes.Internal, "failed to connect instance via anchor service: %v", err)
			}

			if !instanceResp.Success {
				s.engine.IncrementErrors()
				return nil, status.Errorf(codes.Internal, "anchor service failed to connect instance: %s", instanceResp.Message)
			}
		}
	}

	// Send ConnectDatabase gRPC request to anchor service
	databaseReq := &anchorv1.ConnectDatabaseRequest{
		TenantId:    req.TenantId,
		WorkspaceId: db.WorkspaceID,
		DatabaseId:  db.ID,
	}

	databaseResp, err := anchorClient.ConnectDatabase(ctx, databaseReq)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to reconnect database via anchor service: %v", err)
	}

	if !databaseResp.Success {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "anchor service failed to reconnect database: %s", databaseResp.Message)
	}

	// Get the updated database
	updatedDb, err := databaseService.Get(ctx, req.TenantId, workspaceID, req.DatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get updated database: %v", err)
	}

	// Convert to protobuf format
	protoDatabase := s.databaseToProto(updatedDb)

	return &corev1.ReconnectDatabaseResponse{
		Message:  fmt.Sprintf("Database %s enabled and reconnected successfully", req.DatabaseName),
		Success:  true,
		Database: protoDatabase,
		Status:   commonv1.Status_STATUS_PENDING,
	}, nil
}

func (s *Server) ModifyDatabase(ctx context.Context, req *corev1.ModifyDatabaseRequest) (*corev1.ModifyDatabaseResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get database service
	databaseService := database.NewService(s.engine.db, s.engine.logger)
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Build updates map
	updates := make(map[string]interface{})
	if req.DatabaseNameNew != nil {
		updates["database_name"] = req.DatabaseNameNew
	}
	if req.DatabaseDescription != nil {
		updates["database_description"] = *req.DatabaseDescription
	}
	if req.Username != nil {
		updates["database_username"] = *req.Username
	}
	if req.Password != nil {
		updates["database_password"] = *req.Password
	}
	if req.Enabled != nil {
		updates["database_enabled"] = *req.Enabled
	}

	// Update the database
	updatedDatabase, err := databaseService.Update(ctx, req.TenantId, workspaceID, req.DatabaseName, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update database: %v", err)
	}

	// Convert to protobuf format
	protoDatabase := s.databaseToProto(updatedDatabase)

	// Broadcast database update to other mesh nodes asynchronously
	go func() {
		if syncMgr := s.engine.GetSyncManager(); syncMgr != nil {
			broadcastCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if shouldBroadcast, _ := syncMgr.ShouldBroadcastUserData(broadcastCtx); shouldBroadcast {
				recordData := s.databaseToRecordData(updatedDatabase)
				primaryKey := map[string]interface{}{"database_id": updatedDatabase.ID}
				if err := syncMgr.BroadcastUserDataOperation(broadcastCtx, "databases", "UPDATE", recordData, primaryKey); err != nil {
					s.engine.logger.Errorf("Failed to broadcast database update: %v", err)
				}
			}
		}
	}()

	return &corev1.ModifyDatabaseResponse{
		Message:  fmt.Sprintf("Database %s updated successfully", updatedDatabase.Name),
		Success:  true,
		Database: protoDatabase,
		Status:   commonv1.Status_STATUS_UPDATED,
	}, nil
}

func (s *Server) DisconnectDatabase(ctx context.Context, req *corev1.DisconnectDatabaseRequest) (*corev1.DisconnectDatabaseResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get database service
	databaseService := database.NewService(s.engine.db, s.engine.logger)
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get the database to retrieve instance information
	db, err := databaseService.Get(ctx, req.TenantId, workspaceID, req.DatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "database not found: %v", err)
	}

	// Verify tenant access
	if db.TenantID != req.TenantId {
		return nil, status.Errorf(codes.PermissionDenied, "database not found in tenant")
	}

	// Get anchor service address using dynamic resolution
	anchorAddr := s.engine.getServiceAddress("anchor")

	anchorConn, err := grpc.Dial(anchorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect to anchor service at %s: %v", anchorAddr, err)
	}
	defer anchorConn.Close()

	anchorClient := anchorv1.NewAnchorServiceClient(anchorConn)
	anchorReq := &anchorv1.DisconnectDatabaseRequest{
		TenantId:    req.TenantId,
		WorkspaceId: db.WorkspaceID,
		DatabaseId:  db.ID,
	}

	anchorResp, err := anchorClient.DisconnectDatabase(ctx, anchorReq)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to disconnect instance via anchor service: %v", err)
	}

	if !anchorResp.Success {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "anchor service failed to disconnect instance: %s", anchorResp.Message)
	}

	// Handle database based on delete flag
	var message string
	if req.DeleteDatabaseObject != nil && *req.DeleteDatabaseObject {
		// Delete the database if requested
		err = databaseService.Delete(ctx, req.TenantId, workspaceID, req.DatabaseName)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to delete database: %v", err)
		}
		message = "Database disconnected and deleted successfully"

		// Broadcast database deletion to other mesh nodes asynchronously
		go func() {
			if syncMgr := s.engine.GetSyncManager(); syncMgr != nil {
				broadcastCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				if shouldBroadcast, _ := syncMgr.ShouldBroadcastUserData(broadcastCtx); shouldBroadcast {
					primaryKey := map[string]interface{}{"database_id": db.ID}
					if err := syncMgr.BroadcastUserDataOperation(broadcastCtx, "databases", "DELETE", nil, primaryKey); err != nil {
						s.engine.logger.Errorf("Failed to broadcast database deletion: %v", err)
					}
				}
			}
		}()
	} else {
		// Disable the database if not deleted
		err = databaseService.Disable(ctx, req.TenantId, workspaceID, req.DatabaseName)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to disable database: %v", err)
		}
		message = "Database disconnected and disabled successfully"

		// Get updated database and broadcast the status change
		updatedDb, err := databaseService.Get(ctx, req.TenantId, workspaceID, req.DatabaseName)
		if err == nil {
			// Broadcast database update to other mesh nodes asynchronously
			go func() {
				if syncMgr := s.engine.GetSyncManager(); syncMgr != nil {
					broadcastCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()

					if shouldBroadcast, _ := syncMgr.ShouldBroadcastUserData(broadcastCtx); shouldBroadcast {
						recordData := s.databaseToRecordData(updatedDb)
						primaryKey := map[string]interface{}{"database_id": updatedDb.ID}
						if err := syncMgr.BroadcastUserDataOperation(broadcastCtx, "databases", "UPDATE", recordData, primaryKey); err != nil {
							s.engine.logger.Errorf("Failed to broadcast database disable: %v", err)
						}
					}
				}
			}()
		}
	}

	return &corev1.DisconnectDatabaseResponse{
		Message: message,
		Success: true,
		Status:  commonv1.Status_STATUS_DISCONNECTED,
	}, nil
}

func (s *Server) GetLatestStoredDatabaseSchema(ctx context.Context, req *corev1.GetLatestStoredDatabaseSchemaRequest) (*corev1.GetLatestStoredDatabaseSchemaResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// TODO: Implement this

	// Get database service
	return nil, status.Errorf(codes.Unimplemented, "method GetLatestStoredDatabaseSchema not implemented")
}

func (s *Server) WipeDatabase(ctx context.Context, req *corev1.WipeDatabaseRequest) (*corev1.WipeDatabaseResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get services
	databaseService := database.NewService(s.engine.db, s.engine.logger)
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get the database to verify it exists and belongs to the tenant/workspace
	db, err := databaseService.Get(ctx, req.TenantId, workspaceID, req.DatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "database not found: %v", err)
	}

	// Verify tenant access
	if db.TenantID != req.TenantId {
		return nil, status.Errorf(codes.PermissionDenied, "database not found in tenant")
	}

	// Get anchor service address using dynamic resolution
	anchorAddr := s.engine.getServiceAddress("anchor")

	// Connect to anchor service
	anchorConn, err := grpc.Dial(anchorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect to anchor service at %s: %v", anchorAddr, err)
	}
	defer anchorConn.Close()

	anchorClient := anchorv1.NewAnchorServiceClient(anchorConn)

	// Send WipeDatabase gRPC request to anchor service
	anchorReq := &anchorv1.WipeDatabaseRequest{
		TenantId:    req.TenantId,
		WorkspaceId: db.WorkspaceID,
		DatabaseId:  db.ID,
	}

	anchorResp, err := anchorClient.WipeDatabase(ctx, anchorReq)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to wipe database via anchor service: %v", err)
	}

	if !anchorResp.Success {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "anchor service failed to wipe database: %s", anchorResp.Message)
	}

	return &corev1.WipeDatabaseResponse{
		Message: "Database wiped successfully",
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) TransformData(ctx context.Context, req *corev1.TransformDataRequest) (*corev1.TransformDataResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get services
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	databaseService := database.NewService(s.engine.db, s.engine.logger)
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get mapping rules for the mapping
	mappingRules, err := mappingService.GetMappingRulesForMapping(ctx, req.TenantId, workspaceID, req.MappingName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "mapping not found: %v", err)
	}

	if len(mappingRules) == 0 {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.FailedPrecondition, "mapping has no rules")
	}

	// Extract source and target information from the first mapping rule
	// All rules should have the same source and target databases/tables
	firstRule := mappingRules[0]
	sourceInfo, err := s.parseDatabaseIdentifier(firstRule.SourceIdentifier)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.InvalidArgument, "invalid source identifier: %v", err)
	}

	targetInfo, err := s.parseDatabaseIdentifier(firstRule.TargetIdentifier)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.InvalidArgument, "invalid target identifier: %v", err)
	}

	// Get source database by ID
	sourceDB, err := databaseService.GetByID(ctx, sourceInfo.DatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "source database not found: %v", err)
	}

	// Get target database by ID
	targetDB, err := databaseService.GetByID(ctx, targetInfo.DatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "target database not found: %v", err)
	}

	// Verify both databases are connected
	if sourceDB.Status != "STATUS_CONNECTED" {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.FailedPrecondition, "source database is not connected")
	}

	if targetDB.Status != "STATUS_CONNECTED" {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.FailedPrecondition, "target database is not connected")
	}

	// Get anchor service address using dynamic resolution
	anchorAddr := s.engine.getServiceAddress("anchor")

	// Connect to anchor service
	anchorConn, err := grpc.Dial(anchorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect to anchor service at %s: %v", anchorAddr, err)
	}
	defer anchorConn.Close()

	anchorClient := anchorv1.NewAnchorServiceClient(anchorConn)

	// Fetch data from source database
	fetchReq := &anchorv1.FetchDataRequest{
		TenantId:    req.TenantId,
		WorkspaceId: sourceDB.WorkspaceID,
		DatabaseId:  sourceDB.ID,
		TableName:   sourceInfo.TableName,
	}

	fetchResp, err := anchorClient.FetchData(ctx, fetchReq)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to fetch data from source database: %v", err)
	}

	if !fetchResp.Success {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to fetch data: %s", fetchResp.Message)
	}

	// Prepare transformation options
	transformationOptions := map[string]interface{}{
		"transformation_rules": s.convertMappingRulesToTransformationRules(mappingRules),
		"mode":                 req.Mode,
	}

	optionsBytes, err := json.Marshal(transformationOptions)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to marshal transformation options: %v", err)
	}

	// Transform data using anchor service
	transformReq := &anchorv1.TransformDataRequest{
		TenantId:    req.TenantId,
		WorkspaceId: targetDB.WorkspaceID,
		DatabaseId:  targetDB.ID,
		TableName:   targetInfo.TableName,
		Data:        fetchResp.Data,
		Options:     optionsBytes,
	}

	transformResp, err := anchorClient.TransformData(ctx, transformReq)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to transform data: %v", err)
	}

	if !transformResp.Success {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to transform data: %s", transformResp.Message)
	}

	// Insert transformed data into target database
	var insertReq *anchorv1.InsertDataRequest
	switch req.Mode {
	case "append":
		insertReq = &anchorv1.InsertDataRequest{
			TenantId:    req.TenantId,
			WorkspaceId: targetDB.WorkspaceID,
			DatabaseId:  targetDB.ID,
			TableName:   targetInfo.TableName,
			Data:        transformResp.TransformedData,
		}
	case "replace":
		// First wipe the target table
		wipeReq := &anchorv1.WipeDatabaseRequest{
			TenantId:    req.TenantId,
			WorkspaceId: targetDB.WorkspaceID,
			DatabaseId:  targetDB.ID,
		}
		wipeResp, err := anchorClient.WipeDatabase(ctx, wipeReq)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to wipe target table: %v", err)
		}
		if !wipeResp.Success {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to wipe target table: %s", wipeResp.Message)
		}

		insertReq = &anchorv1.InsertDataRequest{
			TenantId:    req.TenantId,
			WorkspaceId: targetDB.WorkspaceID,
			DatabaseId:  targetDB.ID,
			TableName:   targetInfo.TableName,
			Data:        transformResp.TransformedData,
		}
	case "update":
		// For update mode, we need to use upsert functionality
		// This would require implementing upsert in the anchor service
		// For now, we'll use regular insert
		insertReq = &anchorv1.InsertDataRequest{
			TenantId:    req.TenantId,
			WorkspaceId: targetDB.WorkspaceID,
			DatabaseId:  targetDB.ID,
			TableName:   targetInfo.TableName,
			Data:        transformResp.TransformedData,
		}
	default:
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.InvalidArgument, "invalid mode: %s", req.Mode)
	}

	insertResp, err := anchorClient.InsertData(ctx, insertReq)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to insert transformed data: %v", err)
	}

	if !insertResp.Success {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to insert transformed data: %s", insertResp.Message)
	}

	return &corev1.TransformDataResponse{
		Message:            "Data transformation completed successfully",
		Success:            true,
		Status:             commonv1.Status_STATUS_SUCCESS,
		SourceDatabaseName: sourceInfo.DatabaseName,
		SourceTableName:    sourceInfo.TableName,
		TargetDatabaseName: targetInfo.DatabaseName,
		TargetTableName:    targetInfo.TableName,
		RowsProcessed:      insertResp.RowsAffected,
		RowsTransformed:    insertResp.RowsAffected,
		RowsInserted:       insertResp.RowsAffected,
		RowsUpdated:        0, // Not implemented yet
		RowsDeleted:        0, // Not implemented yet
	}, nil
}

func (s *Server) TransformDataStream(req *corev1.TransformDataStreamRequest, stream corev1.DatabaseService_TransformDataStreamServer) error {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// This is a simplified implementation for streaming
	// In a real implementation, you would stream data in chunks
	ctx := stream.Context()

	// Get services
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	databaseService := database.NewService(s.engine.db, s.engine.logger)
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get mapping rules for the mapping
	mappingRules, err := mappingService.GetMappingRulesForMapping(ctx, req.TenantId, workspaceID, req.MappingName)
	if err != nil {
		s.engine.IncrementErrors()
		return status.Errorf(codes.NotFound, "mapping not found: %v", err)
	}

	if len(mappingRules) == 0 {
		s.engine.IncrementErrors()
		return status.Errorf(codes.FailedPrecondition, "mapping has no rules")
	}

	// Extract source and target information from the first mapping rule
	// All rules should have the same source and target databases/tables
	firstRule := mappingRules[0]
	sourceInfo, err := s.parseDatabaseIdentifier(firstRule.SourceIdentifier)
	if err != nil {
		s.engine.IncrementErrors()
		return status.Errorf(codes.InvalidArgument, "invalid source identifier: %v", err)
	}

	targetInfo, err := s.parseDatabaseIdentifier(firstRule.TargetIdentifier)
	if err != nil {
		s.engine.IncrementErrors()
		return status.Errorf(codes.InvalidArgument, "invalid target identifier: %v", err)
	}

	// Get source database by ID
	sourceDB, err := databaseService.GetByID(ctx, sourceInfo.DatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return status.Errorf(codes.NotFound, "source database not found: %v", err)
	}

	// Get target database by ID
	targetDB, err := databaseService.GetByID(ctx, targetInfo.DatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return status.Errorf(codes.NotFound, "target database not found: %v", err)
	}

	// Verify both databases are connected
	if sourceDB.Status != "STATUS_CONNECTED" {
		s.engine.IncrementErrors()
		return status.Errorf(codes.FailedPrecondition, "source database is not connected")
	}

	if targetDB.Status != "STATUS_CONNECTED" {
		s.engine.IncrementErrors()
		return status.Errorf(codes.FailedPrecondition, "target database is not connected")
	}

	// Get anchor service address using dynamic resolution
	anchorAddr := s.engine.getServiceAddress("anchor")

	// Connect to anchor service
	anchorConn, err := grpc.Dial(anchorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.engine.IncrementErrors()
		return status.Errorf(codes.Internal, "failed to connect to anchor service at %s: %v", anchorAddr, err)
	}
	defer anchorConn.Close()

	anchorClient := anchorv1.NewAnchorServiceClient(anchorConn)

	// For streaming, we'll use FetchDataStream and TransformDataStream
	// This is a simplified implementation
	fetchStreamReq := &anchorv1.FetchDataStreamRequest{
		TenantId:    req.TenantId,
		WorkspaceId: sourceDB.WorkspaceID,
		DatabaseId:  sourceDB.ID,
		TableName:   sourceInfo.TableName,
	}

	fetchStream, err := anchorClient.FetchDataStream(ctx, fetchStreamReq)
	if err != nil {
		s.engine.IncrementErrors()
		return status.Errorf(codes.Internal, "failed to start fetch data stream: %v", err)
	}

	// Prepare transformation options
	transformationOptions := map[string]interface{}{
		"transformation_rules": s.convertMappingRulesToTransformationRules(mappingRules),
		"mode":                 req.Mode,
	}

	optionsBytes, err := json.Marshal(transformationOptions)
	if err != nil {
		s.engine.IncrementErrors()
		return status.Errorf(codes.Internal, "failed to marshal transformation options: %v", err)
	}

	var totalRowsProcessed int64
	var totalRowsTransformed int64
	var totalRowsInserted int64

	// Process the stream
	for {
		fetchResp, err := fetchStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			s.engine.IncrementErrors()
			return status.Errorf(codes.Internal, "failed to receive data from stream: %v", err)
		}

		if !fetchResp.Success {
			s.engine.IncrementErrors()
			return status.Errorf(codes.Internal, "failed to fetch data: %s", fetchResp.Message)
		}

		// Transform the chunk
		transformReq := &anchorv1.TransformDataRequest{
			TenantId:    req.TenantId,
			WorkspaceId: targetDB.WorkspaceID,
			DatabaseId:  targetDB.ID,
			TableName:   targetInfo.TableName,
			Data:        fetchResp.Data,
			Options:     optionsBytes,
		}

		transformResp, err := anchorClient.TransformData(ctx, transformReq)
		if err != nil {
			s.engine.IncrementErrors()
			return status.Errorf(codes.Internal, "failed to transform data chunk: %v", err)
		}

		if !transformResp.Success {
			s.engine.IncrementErrors()
			return status.Errorf(codes.Internal, "failed to transform data: %s", transformResp.Message)
		}

		// Insert the transformed chunk
		insertReq := &anchorv1.InsertDataRequest{
			TenantId:    req.TenantId,
			WorkspaceId: targetDB.WorkspaceID,
			DatabaseId:  targetDB.ID,
			TableName:   targetInfo.TableName,
			Data:        transformResp.TransformedData,
		}

		insertResp, err := anchorClient.InsertData(ctx, insertReq)
		if err != nil {
			s.engine.IncrementErrors()
			return status.Errorf(codes.Internal, "failed to insert transformed data: %v", err)
		}

		if !insertResp.Success {
			s.engine.IncrementErrors()
			return status.Errorf(codes.Internal, "failed to insert transformed data: %s", insertResp.Message)
		}

		totalRowsProcessed += insertResp.RowsAffected
		totalRowsTransformed += insertResp.RowsAffected
		totalRowsInserted += insertResp.RowsAffected

		// Send progress update
		if err := stream.Send(&corev1.TransformDataStreamResponse{
			Message:            "Data chunk processed successfully",
			Success:            true,
			Status:             commonv1.Status_STATUS_SUCCESS,
			SourceDatabaseName: sourceInfo.DatabaseName,
			SourceTableName:    sourceInfo.TableName,
			TargetDatabaseName: targetInfo.DatabaseName,
			TargetTableName:    targetInfo.TableName,
			RowsProcessed:      totalRowsProcessed,
			RowsTransformed:    totalRowsTransformed,
			RowsInserted:       totalRowsInserted,
			RowsUpdated:        0, // Not implemented yet
			RowsDeleted:        0, // Not implemented yet
			IsComplete:         false,
		}); err != nil {
			return err
		}
	}

	// Send final completion message
	return stream.Send(&corev1.TransformDataStreamResponse{
		Message:            "Data transformation completed successfully",
		Success:            true,
		Status:             commonv1.Status_STATUS_SUCCESS,
		SourceDatabaseName: sourceInfo.DatabaseName,
		SourceTableName:    sourceInfo.TableName,
		TargetDatabaseName: targetInfo.DatabaseName,
		TargetTableName:    targetInfo.TableName,
		RowsProcessed:      totalRowsProcessed,
		RowsTransformed:    totalRowsTransformed,
		RowsInserted:       totalRowsInserted,
		RowsUpdated:        0, // Not implemented yet
		RowsDeleted:        0, // Not implemented yet
		IsComplete:         true,
	})
}

// convertMappingRulesToTransformationRules converts mapping rules to transformation rules format
// DatabaseIdentifierInfo contains parsed database identifier information
type DatabaseIdentifierInfo struct {
	DatabaseName string
	TableName    string
	ColumnName   string
}

// parseDatabaseIdentifier parses a database identifier in the format "@db://database_id.table.column"
func (s *Server) parseDatabaseIdentifier(identifier string) (*DatabaseIdentifierInfo, error) {
	// Remove the "@db://" prefix if present
	cleanIdentifier := identifier
	if strings.HasPrefix(identifier, "@db://") {
		cleanIdentifier = strings.TrimPrefix(identifier, "@db://")
	} else if strings.HasPrefix(identifier, "db://") {
		cleanIdentifier = strings.TrimPrefix(identifier, "db://")
	}

	// Split by dots to get database_id, table, and column
	parts := strings.Split(cleanIdentifier, ".")
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid identifier format: expected '@db://database_id.table.column' or 'db://database_id.table.column', got '%s'", identifier)
	}

	return &DatabaseIdentifierInfo{
		DatabaseName: parts[0], // This is actually the database_id
		TableName:    parts[1],
		ColumnName:   parts[2],
	}, nil
}

func (s *Server) convertMappingRulesToTransformationRules(mappingRules []*mapping.Rule) []map[string]interface{} {
	var transformationRules []map[string]interface{}

	for _, rule := range mappingRules {
		// Parse source and target identifiers using the proper parser
		sourceInfo, err := s.parseDatabaseIdentifier(rule.SourceIdentifier)
		if err != nil {
			// Skip this rule if parsing fails
			continue
		}

		targetInfo, err := s.parseDatabaseIdentifier(rule.TargetIdentifier)
		if err != nil {
			// Skip this rule if parsing fails
			continue
		}

		transformationRule := map[string]interface{}{
			"source_field":           sourceInfo.ColumnName,
			"target_field":           targetInfo.ColumnName,
			"transformation_type":    rule.TransformationName,
			"transformation_options": rule.TransformationOptions,
		}

		transformationRules = append(transformationRules, transformationRule)
	}

	return transformationRules
}

func (s *Server) DropDatabase(ctx context.Context, req *corev1.DropDatabaseRequest) (*corev1.DropDatabaseResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get services
	databaseService := database.NewService(s.engine.db, s.engine.logger)
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	instanceService := instance.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get the database to verify it exists and belongs to the tenant/workspace
	db, err := databaseService.Get(ctx, req.TenantId, workspaceID, req.DatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "database not found: %v", err)
	}

	// Verify tenant access
	if db.TenantID != req.TenantId {
		return nil, status.Errorf(codes.PermissionDenied, "database not found in tenant")
	}

	// Get anchor service address using dynamic resolution
	anchorAddr := s.engine.getServiceAddress("anchor")

	// Connect to anchor service
	anchorConn, err := grpc.Dial(anchorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect to anchor service at %s: %v", anchorAddr, err)
	}
	defer anchorConn.Close()

	anchorClient := anchorv1.NewAnchorServiceClient(anchorConn)

	// Step 1: Disconnect the database from anchor service
	disconnectReq := &anchorv1.DisconnectDatabaseRequest{
		TenantId:    req.TenantId,
		WorkspaceId: db.WorkspaceID,
		DatabaseId:  db.ID,
	}

	disconnectResp, err := anchorClient.DisconnectDatabase(ctx, disconnectReq)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to disconnect database via anchor service: %v", err)
	}

	if !disconnectResp.Success {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "anchor service failed to disconnect database: %s", disconnectResp.Message)
	}

	// Step 2: Find the instance associated with this database
	if db.InstanceID == "" {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "database has no associated instance")
	}

	// Get instance name from database instance_id
	var instanceName string
	err = s.engine.db.Pool().QueryRow(ctx, "SELECT instance_name FROM instances WHERE instance_id = $1", db.InstanceID).Scan(&instanceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get instance name: %v", err)
	}

	// Get instance details
	inst, err := instanceService.Get(ctx, req.TenantId, req.WorkspaceName, instanceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get instance: %v", err)
	}

	// Step 3: Call anchor service to drop the database from the instance
	dropReq := &anchorv1.DropDatabaseRequest{
		TenantId:    req.TenantId,
		WorkspaceId: inst.WorkspaceID,
		InstanceId:  inst.ID,
		DatabaseId:  db.ID,
	}

	dropResp, err := anchorClient.DropDatabase(ctx, dropReq)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to drop database via anchor service: %v", err)
	}

	if !dropResp.Success {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "anchor service failed to drop database: %s", dropResp.Message)
	}

	// Step 4: Delete the database object from internal database
	err = databaseService.Delete(ctx, req.TenantId, workspaceID, req.DatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to delete database from internal database: %v", err)
	}

	return &corev1.DropDatabaseResponse{
		Message: fmt.Sprintf("Database %s dropped successfully", req.DatabaseName),
		Success: true,
		Status:  commonv1.Status_STATUS_DELETED,
	}, nil
}

// StoreDatabaseSchema stores the database schema in the database
func (s *Server) StoreDatabaseSchema(ctx context.Context, req *corev1.StoreDatabaseSchemaRequest) (*corev1.StoreDatabaseSchemaResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get services
	databaseService := database.NewService(s.engine.db, s.engine.logger)

	// Store the database schema
	err := databaseService.StoreDatabaseSchema(ctx, req.DatabaseId, req.Schema)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to store database schema: %v", err)
	}

	return &corev1.StoreDatabaseSchemaResponse{
		Message: fmt.Sprintf("Database schema stored successfully for database with ID: %s", req.DatabaseId),
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// StoreDatabaseTables stores the database tables in the database
func (s *Server) StoreDatabaseTables(ctx context.Context, req *corev1.StoreDatabaseTablesRequest) (*corev1.StoreDatabaseTablesResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get services
	databaseService := database.NewService(s.engine.db, s.engine.logger)

	// Store the database tables
	err := databaseService.StoreDatabaseTables(ctx, req.DatabaseId, req.Tables)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to store database tables: %v", err)
	}

	return &corev1.StoreDatabaseTablesResponse{
		Message: fmt.Sprintf("Database tables stored successfully for database with ID: %s", req.DatabaseId),
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// GetDatabaseSchema retrieves the database schema from the database
func (s *Server) GetDatabaseSchema(ctx context.Context, req *corev1.GetDatabaseSchemaRequest) (*corev1.GetDatabaseSchemaResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get services
	databaseService := database.NewService(s.engine.db, s.engine.logger)

	// Get the database schema
	schema, err := databaseService.GetDatabaseSchema(ctx, req.DatabaseId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get database schema: %v", err)
	}

	return &corev1.GetDatabaseSchemaResponse{
		Schema:  schema,
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// GetDatabaseTables retrieves the database tables from the database
func (s *Server) GetDatabaseTables(ctx context.Context, req *corev1.GetDatabaseTablesRequest) (*corev1.GetDatabaseTablesResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get services
	databaseService := database.NewService(s.engine.db, s.engine.logger)

	// Get the database tables
	tables, err := databaseService.GetDatabaseTables(ctx, req.DatabaseId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get database tables: %v", err)
	}

	return &corev1.GetDatabaseTablesResponse{
		Tables:  tables,
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}
