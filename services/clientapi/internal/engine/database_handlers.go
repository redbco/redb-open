package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DatabaseHandlers contains the database endpoint handlers
type DatabaseHandlers struct {
	engine *Engine
}

// NewDatabaseHandlers creates a new instance of DatabaseHandlers
func NewDatabaseHandlers(engine *Engine) *DatabaseHandlers {
	return &DatabaseHandlers{
		engine: engine,
	}
}

// ListDatabases handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/databases
func (dh *DatabaseHandlers) ListDatabases(w http.ResponseWriter, r *http.Request) {
	dh.engine.TrackOperation()
	defer dh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if dh.engine.logger != nil {
		dh.engine.logger.Infof("List databases request for workspace: %s, tenant: %s, user: %s", workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListDatabasesRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
	}

	grpcResp, err := dh.engine.databaseClient.ListDatabases(ctx, grpcReq)
	if err != nil {
		dh.handleGRPCError(w, err, "Failed to list databases")
		return
	}

	// Convert gRPC response to REST response
	databases := make([]Database, len(grpcResp.Databases))
	for i, db := range grpcResp.Databases {
		databases[i] = Database{
			TenantID:              db.TenantId,
			WorkspaceID:           db.WorkspaceId,
			EnvironmentID:         db.EnvironmentId,
			ConnectedToNodeID:     db.ConnectedToNodeId,
			InstanceID:            db.InstanceId,
			InstanceName:          db.InstanceName,
			DatabaseID:            db.DatabaseId,
			DatabaseName:          db.DatabaseName,
			DatabaseDescription:   db.DatabaseDescription,
			DatabaseType:          db.DatabaseType,
			DatabaseVendor:        db.DatabaseVendor,
			DatabaseVersion:       db.DatabaseVersion,
			DatabaseUsername:      db.DatabaseUsername,
			DatabasePassword:      db.DatabasePassword,
			DatabaseDBName:        db.DatabaseDbName,
			DatabaseEnabled:       db.DatabaseEnabled,
			PolicyIDs:             db.PolicyIds,
			OwnerID:               db.OwnerId,
			DatabaseStatusMessage: db.DatabaseStatusMessage,
			Status:                convertStatus(db.Status),
			Created:               db.Created,
			Updated:               db.Updated,
			InstanceHost:          db.InstanceHost,
			InstancePort:          db.InstancePort,
			InstanceSSLMode:       db.InstanceSslMode,
			InstanceSSLCert:       db.InstanceSslCert,
			InstanceSSLKey:        db.InstanceSslKey,
			InstanceSSLRootCert:   db.InstanceSslRootCert,
			InstanceSSL:           db.InstanceSsl,
			InstanceStatusMessage: db.InstanceStatusMessage,
			InstanceStatus:        db.InstanceStatus,
		}
	}

	response := ListDatabasesResponse{
		Databases: databases,
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Successfully listed %d databases for workspace: %s", len(databases), workspaceName)
	}

	dh.writeJSONResponse(w, http.StatusOK, response)
}

// ShowDatabase handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/databases/{database_name}
func (dh *DatabaseHandlers) ShowDatabase(w http.ResponseWriter, r *http.Request) {
	dh.engine.TrackOperation()
	defer dh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	databaseName := vars["database_name"]

	if tenantURL == "" || workspaceName == "" || databaseName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and database_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Show database request for database: %s, workspace: %s, tenant: %s", databaseName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowDatabaseRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		DatabaseName:  databaseName,
	}

	grpcResp, err := dh.engine.databaseClient.ShowDatabase(ctx, grpcReq)
	if err != nil {
		dh.handleGRPCError(w, err, "Failed to show database")
		return
	}

	// Convert gRPC response to REST response
	database := Database{
		TenantID:              grpcResp.Database.TenantId,
		WorkspaceID:           grpcResp.Database.WorkspaceId,
		EnvironmentID:         grpcResp.Database.EnvironmentId,
		ConnectedToNodeID:     grpcResp.Database.ConnectedToNodeId,
		InstanceID:            grpcResp.Database.InstanceId,
		InstanceName:          grpcResp.Database.InstanceName,
		DatabaseID:            grpcResp.Database.DatabaseId,
		DatabaseName:          grpcResp.Database.DatabaseName,
		DatabaseDescription:   grpcResp.Database.DatabaseDescription,
		DatabaseType:          grpcResp.Database.DatabaseType,
		DatabaseVendor:        grpcResp.Database.DatabaseVendor,
		DatabaseVersion:       grpcResp.Database.DatabaseVersion,
		DatabaseUsername:      grpcResp.Database.DatabaseUsername,
		DatabasePassword:      grpcResp.Database.DatabasePassword,
		DatabaseDBName:        grpcResp.Database.DatabaseDbName,
		DatabaseEnabled:       grpcResp.Database.DatabaseEnabled,
		PolicyIDs:             grpcResp.Database.PolicyIds,
		OwnerID:               grpcResp.Database.OwnerId,
		DatabaseStatusMessage: grpcResp.Database.DatabaseStatusMessage,
		Status:                convertStatus(grpcResp.Database.Status),
		Created:               grpcResp.Database.Created,
		Updated:               grpcResp.Database.Updated,
		DatabaseSchema:        grpcResp.Database.DatabaseSchema,
		DatabaseTables:        grpcResp.Database.DatabaseTables,
		InstanceHost:          grpcResp.Database.InstanceHost,
		InstancePort:          grpcResp.Database.InstancePort,
		InstanceSSLMode:       grpcResp.Database.InstanceSslMode,
		InstanceSSLCert:       grpcResp.Database.InstanceSslCert,
		InstanceSSLKey:        grpcResp.Database.InstanceSslKey,
		InstanceSSLRootCert:   grpcResp.Database.InstanceSslRootCert,
		InstanceSSL:           grpcResp.Database.InstanceSsl,
		InstanceStatusMessage: grpcResp.Database.InstanceStatusMessage,
		InstanceStatus:        grpcResp.Database.InstanceStatus,
	}

	response := ShowDatabaseResponse{
		Database: database,
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Successfully showed database: %s for workspace: %s", databaseName, workspaceName)
	}

	dh.writeJSONResponse(w, http.StatusOK, response)
}

// ConnectDatabase handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/databases/connect
func (dh *DatabaseHandlers) ConnectDatabase(w http.ResponseWriter, r *http.Request) {
	dh.engine.TrackOperation()
	defer dh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" || workspaceName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url and workspace_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ConnectDatabaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if dh.engine.logger != nil {
			dh.engine.logger.Errorf("Failed to parse connect database request body: %v", err)
		}
		dh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.DatabaseName == "" || req.DatabaseType == "" || req.DatabaseVendor == "" || req.Host == "" || req.DBName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing", "database_name, database_type, database_vendor, host, and db_name are required")
		return
	}

	// Log request
	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Connect database request for workspace: %s, database: %s, tenant: %s", workspaceName, req.DatabaseName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ConnectDatabaseRequest{
		TenantId:            profile.TenantId,
		WorkspaceName:       workspaceName,
		OwnerId:             profile.UserId,
		DatabaseName:        req.DatabaseName,
		DatabaseDescription: req.DatabaseDescription,
		DatabaseType:        req.DatabaseType,
		DatabaseVendor:      req.DatabaseVendor,
		Host:                req.Host,
		Port:                req.Port,
		Username:            req.Username,
		Password:            req.Password,
		DbName:              req.DBName,
		NodeId:              req.NodeID,
		Enabled:             req.Enabled,
		Ssl:                 req.SSL,
	}

	if req.EnvironmentID != "" {
		grpcReq.EnvironmentId = &req.EnvironmentID
	}

	if req.SSLMode != "" {
		grpcReq.SslMode = &req.SSLMode
	}

	if req.SSLCert != "" {
		grpcReq.SslCert = &req.SSLCert
	}

	if req.SSLKey != "" {
		grpcReq.SslKey = &req.SSLKey
	}

	if req.SSLRootCert != "" {
		grpcReq.SslRootCert = &req.SSLRootCert
	}

	grpcResp, err := dh.engine.databaseClient.ConnectDatabase(ctx, grpcReq)
	if err != nil {
		dh.handleGRPCError(w, err, "Failed to connect database")
		return
	}

	// Convert gRPC response to REST response
	database := Database{
		TenantID:              grpcResp.Database.TenantId,
		WorkspaceID:           grpcResp.Database.WorkspaceId,
		EnvironmentID:         grpcResp.Database.EnvironmentId,
		ConnectedToNodeID:     grpcResp.Database.ConnectedToNodeId,
		InstanceID:            grpcResp.Database.InstanceId,
		InstanceName:          grpcResp.Database.InstanceName,
		DatabaseID:            grpcResp.Database.DatabaseId,
		DatabaseName:          grpcResp.Database.DatabaseName,
		DatabaseDescription:   grpcResp.Database.DatabaseDescription,
		DatabaseType:          grpcResp.Database.DatabaseType,
		DatabaseVendor:        grpcResp.Database.DatabaseVendor,
		DatabaseVersion:       grpcResp.Database.DatabaseVersion,
		DatabaseUsername:      grpcResp.Database.DatabaseUsername,
		DatabasePassword:      grpcResp.Database.DatabasePassword,
		DatabaseDBName:        grpcResp.Database.DatabaseDbName,
		DatabaseEnabled:       grpcResp.Database.DatabaseEnabled,
		PolicyIDs:             grpcResp.Database.PolicyIds,
		OwnerID:               grpcResp.Database.OwnerId,
		DatabaseStatusMessage: grpcResp.Database.DatabaseStatusMessage,
		Status:                convertStatus(grpcResp.Database.Status),
		Created:               grpcResp.Database.Created,
		Updated:               grpcResp.Database.Updated,
		InstanceHost:          grpcResp.Database.InstanceHost,
		InstancePort:          grpcResp.Database.InstancePort,
		InstanceSSLMode:       grpcResp.Database.InstanceSslMode,
		InstanceSSLCert:       grpcResp.Database.InstanceSslCert,
		InstanceSSLKey:        grpcResp.Database.InstanceSslKey,
		InstanceSSLRootCert:   grpcResp.Database.InstanceSslRootCert,
		InstanceSSL:           grpcResp.Database.InstanceSsl,
		InstanceStatusMessage: grpcResp.Database.InstanceStatusMessage,
		InstanceStatus:        grpcResp.Database.InstanceStatus,
	}

	response := ConnectDatabaseResponse{
		Message:  grpcResp.Message,
		Success:  grpcResp.Success,
		Database: database,
		Status:   convertStatus(grpcResp.Status),
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Successfully connected database: %s for workspace: %s", req.DatabaseName, workspaceName)
	}

	dh.writeJSONResponse(w, http.StatusCreated, response)
}

// ConnectDatabaseWithInstance handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/databases/connect-with-instance
func (dh *DatabaseHandlers) ConnectDatabaseWithInstance(w http.ResponseWriter, r *http.Request) {
	dh.engine.TrackOperation()
	defer dh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" || workspaceName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url and workspace_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ConnectDatabaseWithInstanceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if dh.engine.logger != nil {
			dh.engine.logger.Errorf("Failed to parse connect database with instance request body: %v", err)
		}
		dh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.InstanceName == "" || req.DatabaseName == "" || req.DBName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing", "instance_name, database_name, and db_name are required")
		return
	}

	// Log request
	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Connect database with instance request for workspace: %s, database: %s, instance: %s, tenant: %s", workspaceName, req.DatabaseName, req.InstanceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ConnectDatabaseWithInstanceRequest{
		TenantId:            profile.TenantId,
		WorkspaceName:       workspaceName,
		InstanceName:        req.InstanceName,
		DatabaseName:        req.DatabaseName,
		DatabaseDescription: req.DatabaseDescription,
		DbName:              req.DBName,
		Username:            req.Username,
		Password:            req.Password,
		NodeId:              req.NodeID,
		Enabled:             req.Enabled,
		OwnerId:             profile.UserId,
	}

	if req.EnvironmentID != "" {
		grpcReq.EnvironmentId = &req.EnvironmentID
	}

	grpcResp, err := dh.engine.databaseClient.ConnectDatabaseWithInstance(ctx, grpcReq)
	if err != nil {
		dh.handleGRPCError(w, err, "Failed to connect database with instance")
		return
	}

	// Convert gRPC response to REST response
	database := Database{
		TenantID:              grpcResp.Database.TenantId,
		WorkspaceID:           grpcResp.Database.WorkspaceId,
		EnvironmentID:         grpcResp.Database.EnvironmentId,
		ConnectedToNodeID:     grpcResp.Database.ConnectedToNodeId,
		InstanceID:            grpcResp.Database.InstanceId,
		InstanceName:          grpcResp.Database.InstanceName,
		DatabaseID:            grpcResp.Database.DatabaseId,
		DatabaseName:          grpcResp.Database.DatabaseName,
		DatabaseDescription:   grpcResp.Database.DatabaseDescription,
		DatabaseType:          grpcResp.Database.DatabaseType,
		DatabaseVendor:        grpcResp.Database.DatabaseVendor,
		DatabaseVersion:       grpcResp.Database.DatabaseVersion,
		DatabaseUsername:      grpcResp.Database.DatabaseUsername,
		DatabasePassword:      grpcResp.Database.DatabasePassword,
		DatabaseDBName:        grpcResp.Database.DatabaseDbName,
		DatabaseEnabled:       grpcResp.Database.DatabaseEnabled,
		PolicyIDs:             grpcResp.Database.PolicyIds,
		OwnerID:               grpcResp.Database.OwnerId,
		DatabaseStatusMessage: grpcResp.Database.DatabaseStatusMessage,
		Status:                convertStatus(grpcResp.Database.Status),
		Created:               grpcResp.Database.Created,
		Updated:               grpcResp.Database.Updated,
		InstanceHost:          grpcResp.Database.InstanceHost,
		InstancePort:          grpcResp.Database.InstancePort,
		InstanceSSLMode:       grpcResp.Database.InstanceSslMode,
		InstanceSSLCert:       grpcResp.Database.InstanceSslCert,
		InstanceSSLKey:        grpcResp.Database.InstanceSslKey,
		InstanceSSLRootCert:   grpcResp.Database.InstanceSslRootCert,
		InstanceSSL:           grpcResp.Database.InstanceSsl,
		InstanceStatusMessage: grpcResp.Database.InstanceStatusMessage,
		InstanceStatus:        grpcResp.Database.InstanceStatus,
	}

	response := ConnectDatabaseWithInstanceResponse{
		Message:  grpcResp.Message,
		Success:  grpcResp.Success,
		Database: database,
		Status:   convertStatus(grpcResp.Status),
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Successfully connected database: %s with instance: %s for workspace: %s", req.DatabaseName, req.InstanceName, workspaceName)
	}

	dh.writeJSONResponse(w, http.StatusCreated, response)
}

// ReconnectDatabase handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/databases/{database_name}/reconnect
func (dh *DatabaseHandlers) ReconnectDatabase(w http.ResponseWriter, r *http.Request) {
	dh.engine.TrackOperation()
	defer dh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	databaseName := vars["database_name"]

	if tenantURL == "" || workspaceName == "" || databaseName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and database_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Reconnect database request for database: %s, workspace: %s, tenant: %s", databaseName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ReconnectDatabaseRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		DatabaseName:  databaseName,
	}

	grpcResp, err := dh.engine.databaseClient.ReconnectDatabase(ctx, grpcReq)
	if err != nil {
		dh.handleGRPCError(w, err, "Failed to reconnect database")
		return
	}

	// Convert gRPC response to REST response
	database := Database{
		TenantID:              grpcResp.Database.TenantId,
		WorkspaceID:           grpcResp.Database.WorkspaceId,
		EnvironmentID:         grpcResp.Database.EnvironmentId,
		ConnectedToNodeID:     grpcResp.Database.ConnectedToNodeId,
		InstanceID:            grpcResp.Database.InstanceId,
		InstanceName:          grpcResp.Database.InstanceName,
		DatabaseID:            grpcResp.Database.DatabaseId,
		DatabaseName:          grpcResp.Database.DatabaseName,
		DatabaseDescription:   grpcResp.Database.DatabaseDescription,
		DatabaseType:          grpcResp.Database.DatabaseType,
		DatabaseVendor:        grpcResp.Database.DatabaseVendor,
		DatabaseVersion:       grpcResp.Database.DatabaseVersion,
		DatabaseUsername:      grpcResp.Database.DatabaseUsername,
		DatabasePassword:      grpcResp.Database.DatabasePassword,
		DatabaseDBName:        grpcResp.Database.DatabaseDbName,
		DatabaseEnabled:       grpcResp.Database.DatabaseEnabled,
		PolicyIDs:             grpcResp.Database.PolicyIds,
		OwnerID:               grpcResp.Database.OwnerId,
		DatabaseStatusMessage: grpcResp.Database.DatabaseStatusMessage,
		Status:                convertStatus(grpcResp.Database.Status),
		Created:               grpcResp.Database.Created,
		Updated:               grpcResp.Database.Updated,
		InstanceHost:          grpcResp.Database.InstanceHost,
		InstancePort:          grpcResp.Database.InstancePort,
		InstanceSSLMode:       grpcResp.Database.InstanceSslMode,
		InstanceSSLCert:       grpcResp.Database.InstanceSslCert,
		InstanceSSLKey:        grpcResp.Database.InstanceSslKey,
		InstanceSSLRootCert:   grpcResp.Database.InstanceSslRootCert,
		InstanceSSL:           grpcResp.Database.InstanceSsl,
		InstanceStatusMessage: grpcResp.Database.InstanceStatusMessage,
		InstanceStatus:        grpcResp.Database.InstanceStatus,
	}

	response := ReconnectDatabaseResponse{
		Message:  grpcResp.Message,
		Success:  grpcResp.Success,
		Database: database,
		Status:   convertStatus(grpcResp.Status),
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Successfully reconnected database: %s for workspace: %s", databaseName, workspaceName)
	}

	dh.writeJSONResponse(w, http.StatusOK, response)
}

// ModifyDatabase handles PUT /{tenant_url}/api/v1/workspaces/{workspace_name}/databases/{database_name}
func (dh *DatabaseHandlers) ModifyDatabase(w http.ResponseWriter, r *http.Request) {
	dh.engine.TrackOperation()
	defer dh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	databaseName := vars["database_name"]

	if tenantURL == "" || workspaceName == "" || databaseName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and database_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ModifyDatabaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if dh.engine.logger != nil {
			dh.engine.logger.Errorf("Failed to parse modify database request body: %v", err)
		}
		dh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Modify database request for database: %s, workspace: %s, tenant: %s", databaseName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ModifyDatabaseRequest{
		TenantId:            profile.TenantId,
		WorkspaceName:       workspaceName,
		DatabaseName:        databaseName,
		DatabaseNameNew:     &req.DatabaseNameNew,
		DatabaseDescription: &req.DatabaseDescription,
		DatabaseType:        &req.DatabaseType,
		DatabaseVendor:      &req.DatabaseVendor,
		Host:                &req.Host,
		Username:            &req.Username,
		Password:            &req.Password,
		DbName:              &req.DBName,
		SslMode:             &req.SSLMode,
		SslCert:             &req.SSLCert,
		SslKey:              &req.SSLKey,
		SslRootCert:         &req.SSLRootCert,
		NodeId:              &req.NodeID,
	}

	grpcReq.Port = req.Port
	grpcReq.Enabled = req.Enabled

	grpcReq.Ssl = req.SSL

	grpcResp, err := dh.engine.databaseClient.ModifyDatabase(ctx, grpcReq)
	if err != nil {
		dh.handleGRPCError(w, err, "Failed to modify database")
		return
	}

	// Convert gRPC response to REST response
	database := Database{
		TenantID:              grpcResp.Database.TenantId,
		WorkspaceID:           grpcResp.Database.WorkspaceId,
		EnvironmentID:         grpcResp.Database.EnvironmentId,
		ConnectedToNodeID:     grpcResp.Database.ConnectedToNodeId,
		InstanceID:            grpcResp.Database.InstanceId,
		InstanceName:          grpcResp.Database.InstanceName,
		DatabaseID:            grpcResp.Database.DatabaseId,
		DatabaseName:          grpcResp.Database.DatabaseName,
		DatabaseDescription:   grpcResp.Database.DatabaseDescription,
		DatabaseType:          grpcResp.Database.DatabaseType,
		DatabaseVendor:        grpcResp.Database.DatabaseVendor,
		DatabaseVersion:       grpcResp.Database.DatabaseVersion,
		DatabaseUsername:      grpcResp.Database.DatabaseUsername,
		DatabasePassword:      grpcResp.Database.DatabasePassword,
		DatabaseDBName:        grpcResp.Database.DatabaseDbName,
		DatabaseEnabled:       grpcResp.Database.DatabaseEnabled,
		PolicyIDs:             grpcResp.Database.PolicyIds,
		OwnerID:               grpcResp.Database.OwnerId,
		DatabaseStatusMessage: grpcResp.Database.DatabaseStatusMessage,
		Status:                convertStatus(grpcResp.Database.Status),
		Created:               grpcResp.Database.Created,
		Updated:               grpcResp.Database.Updated,
		InstanceHost:          grpcResp.Database.InstanceHost,
		InstancePort:          grpcResp.Database.InstancePort,
		InstanceSSLMode:       grpcResp.Database.InstanceSslMode,
		InstanceSSLCert:       grpcResp.Database.InstanceSslCert,
		InstanceSSLKey:        grpcResp.Database.InstanceSslKey,
		InstanceSSLRootCert:   grpcResp.Database.InstanceSslRootCert,
		InstanceSSL:           grpcResp.Database.InstanceSsl,
		InstanceStatusMessage: grpcResp.Database.InstanceStatusMessage,
		InstanceStatus:        grpcResp.Database.InstanceStatus,
	}

	response := ModifyDatabaseResponse{
		Message:  grpcResp.Message,
		Success:  grpcResp.Success,
		Database: database,
		Status:   convertStatus(grpcResp.Status),
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Successfully modified database: %s for workspace: %s", databaseName, workspaceName)
	}

	dh.writeJSONResponse(w, http.StatusOK, response)
}

// DisconnectDatabase handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/databases/{database_name}/disconnect
func (dh *DatabaseHandlers) DisconnectDatabase(w http.ResponseWriter, r *http.Request) {
	dh.engine.TrackOperation()
	defer dh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	databaseName := vars["database_name"]

	if tenantURL == "" || workspaceName == "" || databaseName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and database_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body (optional)
	var req DisconnectDatabaseRequest
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&req)
	}

	// Log request
	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Disconnect database request for database: %s, workspace: %s, tenant: %s", databaseName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DisconnectDatabaseRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		DatabaseName:  databaseName,
	}

	grpcReq.DeleteDatabaseObject = req.DeleteDatabaseObject
	grpcReq.DeleteRepo = req.DeleteRepo

	grpcResp, err := dh.engine.databaseClient.DisconnectDatabase(ctx, grpcReq)
	if err != nil {
		dh.handleGRPCError(w, err, "Failed to disconnect database")
		return
	}

	response := DisconnectDatabaseResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Successfully disconnected database: %s for workspace: %s", databaseName, workspaceName)
	}

	dh.writeJSONResponse(w, http.StatusOK, response)
}

// GetLatestStoredDatabaseSchema handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/databases/{database_name}/schema
func (dh *DatabaseHandlers) GetLatestStoredDatabaseSchema(w http.ResponseWriter, r *http.Request) {
	dh.engine.TrackOperation()
	defer dh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	databaseName := vars["database_name"]

	if tenantURL == "" || workspaceName == "" || databaseName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and database_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Get database schema request for database: %s, workspace: %s, tenant: %s", databaseName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.GetLatestStoredDatabaseSchemaRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		DatabaseName:  databaseName,
	}

	grpcResp, err := dh.engine.databaseClient.GetLatestStoredDatabaseSchema(ctx, grpcReq)
	if err != nil {
		dh.handleGRPCError(w, err, "Failed to get database schema")
		return
	}

	response := GetLatestStoredDatabaseSchemaResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
		Schema:  grpcResp.Schema,
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Successfully retrieved database schema for database: %s", databaseName)
	}

	dh.writeJSONResponse(w, http.StatusOK, response)
}

// WipeDatabase handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/databases/{database_name}/wipe
func (dh *DatabaseHandlers) WipeDatabase(w http.ResponseWriter, r *http.Request) {
	dh.engine.TrackOperation()
	defer dh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	databaseName := vars["database_name"]

	if tenantURL == "" || workspaceName == "" || databaseName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and database_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Wipe database request for database: %s, workspace: %s, tenant: %s", databaseName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.WipeDatabaseRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		DatabaseName:  databaseName,
	}

	grpcResp, err := dh.engine.databaseClient.WipeDatabase(ctx, grpcReq)
	if err != nil {
		dh.handleGRPCError(w, err, "Failed to wipe database")
		return
	}

	response := WipeDatabaseResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Successfully wiped database: %s", databaseName)
	}

	dh.writeJSONResponse(w, http.StatusOK, response)
}

// DropDatabase handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/databases/{database_name}/drop
func (dh *DatabaseHandlers) DropDatabase(w http.ResponseWriter, r *http.Request) {
	dh.engine.TrackOperation()
	defer dh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	databaseName := vars["database_name"]

	if tenantURL == "" || workspaceName == "" || databaseName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and database_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Drop database request for database: %s, workspace: %s, tenant: %s", databaseName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DropDatabaseRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		DatabaseName:  databaseName,
	}

	grpcResp, err := dh.engine.databaseClient.DropDatabase(ctx, grpcReq)
	if err != nil {
		dh.handleGRPCError(w, err, "Failed to drop database")
		return
	}

	response := DropDatabaseResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Successfully dropped database: %s", databaseName)
	}

	dh.writeJSONResponse(w, http.StatusOK, response)
}

// TransformData handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/databases/transform
func (dh *DatabaseHandlers) TransformData(w http.ResponseWriter, r *http.Request) {
	dh.engine.TrackOperation()
	defer dh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" || workspaceName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url and workspace_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req TransformDataRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if dh.engine.logger != nil {
			dh.engine.logger.Errorf("Failed to parse transform data request body: %v", err)
		}
		dh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.MappingName == "" || req.Mode == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing", "mapping_name and mode are required")
		return
	}

	// Validate mode
	if req.Mode != "append" && req.Mode != "replace" && req.Mode != "update" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "Invalid mode", "mode must be one of: append, replace, update")
		return
	}

	// Log request
	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Transform data request for workspace: %s, mapping: %s, mode: %s, tenant: %s",
			workspaceName, req.MappingName, req.Mode, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 300*time.Second) // Longer timeout for data transformation
	defer cancel()

	// Convert options to bytes
	var optionsBytes []byte
	if req.Options != nil {
		var err error
		optionsBytes, err = json.Marshal(req.Options)
		if err != nil {
			dh.writeErrorResponse(w, http.StatusBadRequest, "Invalid options format", "options must be valid JSON")
			return
		}
	}

	// Call core service gRPC
	grpcReq := &corev1.TransformDataRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		MappingName:   req.MappingName,
		Mode:          req.Mode,
		Options:       optionsBytes,
	}

	grpcResp, err := dh.engine.databaseClient.TransformData(ctx, grpcReq)
	if err != nil {
		dh.handleGRPCError(w, err, "Failed to transform data")
		return
	}

	response := TransformDataResponse{
		Message:            grpcResp.Message,
		Success:            grpcResp.Success,
		Status:             convertStatus(grpcResp.Status),
		SourceDatabaseName: grpcResp.SourceDatabaseName,
		SourceTableName:    grpcResp.SourceTableName,
		TargetDatabaseName: grpcResp.TargetDatabaseName,
		TargetTableName:    grpcResp.TargetTableName,
		RowsTransformed:    grpcResp.RowsTransformed,
		RowsAffected:       grpcResp.RowsInserted + grpcResp.RowsUpdated + grpcResp.RowsDeleted,
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Successfully transformed data from %s.%s to %s.%s, %d rows transformed, %d rows affected",
			response.SourceDatabaseName, response.SourceTableName, response.TargetDatabaseName, response.TargetTableName, response.RowsTransformed, response.RowsAffected)
	}

	dh.writeJSONResponse(w, http.StatusOK, response)
}

// Helper methods

func (dh *DatabaseHandlers) handleGRPCError(w http.ResponseWriter, err error, defaultMessage string) {
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound:
			dh.writeErrorResponse(w, http.StatusNotFound, st.Message(), defaultMessage)
		case codes.AlreadyExists:
			dh.writeErrorResponse(w, http.StatusConflict, st.Message(), defaultMessage)
		case codes.InvalidArgument:
			dh.writeErrorResponse(w, http.StatusBadRequest, st.Message(), defaultMessage)
		case codes.PermissionDenied:
			dh.writeErrorResponse(w, http.StatusForbidden, st.Message(), defaultMessage)
		case codes.Unauthenticated:
			dh.writeErrorResponse(w, http.StatusUnauthorized, st.Message(), defaultMessage)
		default:
			dh.writeErrorResponse(w, http.StatusInternalServerError, st.Message(), defaultMessage)
		}
	} else {
		dh.writeErrorResponse(w, http.StatusInternalServerError, err.Error(), defaultMessage)
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Errorf("Database handler gRPC error: %v", err)
	}
}

func (dh *DatabaseHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		if dh.engine.logger != nil {
			dh.engine.logger.Errorf("Failed to encode JSON response: %v", err)
		}
	}
}

func (dh *DatabaseHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, details string) {
	response := ErrorResponse{
		Error:   message,
		Message: details,
		Status:  StatusError,
	}
	dh.writeJSONResponse(w, statusCode, response)
}

// ConnectDatabaseString handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/databases/connect-string
func (dh *DatabaseHandlers) ConnectDatabaseString(w http.ResponseWriter, r *http.Request) {
	dh.engine.TrackOperation()
	defer dh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" || workspaceName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url and workspace_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ConnectDatabaseStringRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		dh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Validate request
	if req.ConnectionString == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "connection_string is required", "")
		return
	}
	if req.DatabaseName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "database_name is required", "")
		return
	}

	// Parse connection string
	connectionDetails, err := dh.parseConnectionString(req.ConnectionString)
	if err != nil {
		dh.writeErrorResponse(w, http.StatusBadRequest, "Invalid connection string", err.Error())
		return
	}

	// For databases, we should NOT use system database - use the specified database
	if connectionDetails.IsSystemDB {
		dh.writeErrorResponse(w, http.StatusBadRequest, "Cannot create database connection to system database", "Use instances connect for system database connections")
		return
	}

	// Log request
	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Connect database string request: %s, workspace: %s, tenant: %s, type: %s",
			req.DatabaseName, workspaceName, profile.TenantId, connectionDetails.DatabaseType)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Set default enabled if not provided
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	// Prepare gRPC request for ConnectDatabase (creates both instance and database)
	grpcReq := &corev1.ConnectDatabaseRequest{
		TenantId:            profile.TenantId,
		WorkspaceName:       workspaceName,
		DatabaseName:        req.DatabaseName,
		DatabaseDescription: req.DatabaseDescription,
		DatabaseType:        connectionDetails.DatabaseType,
		DatabaseVendor:      connectionDetails.DatabaseVendor,
		Host:                connectionDetails.Host,
		Port:                connectionDetails.Port,
		Username:            connectionDetails.Username,
		Password:            connectionDetails.Password,
		DbName:              connectionDetails.DatabaseName,
		NodeId:              &req.NodeID,
		Enabled:             &enabled,
		Ssl:                 &connectionDetails.SSL,
		SslMode:             &connectionDetails.SSLMode,
		OwnerId:             profile.UserId,
	}

	// Set optional fields from connection details
	if req.EnvironmentID != "" {
		grpcReq.EnvironmentId = &req.EnvironmentID
	}
	if sslCert, ok := connectionDetails.Parameters["ssl_cert"]; ok {
		grpcReq.SslCert = &sslCert
	}
	if sslKey, ok := connectionDetails.Parameters["ssl_key"]; ok {
		grpcReq.SslKey = &sslKey
	}
	if sslRootCert, ok := connectionDetails.Parameters["ssl_root_cert"]; ok {
		grpcReq.SslRootCert = &sslRootCert
	}

	grpcResp, err := dh.engine.databaseClient.ConnectDatabase(ctx, grpcReq)
	if err != nil {
		dh.handleGRPCError(w, err, "Failed to connect database")
		return
	}

	// Convert gRPC response to REST response
	database := Database{
		TenantID:              grpcResp.Database.TenantId,
		WorkspaceID:           grpcResp.Database.WorkspaceId,
		EnvironmentID:         grpcResp.Database.EnvironmentId,
		ConnectedToNodeID:     grpcResp.Database.ConnectedToNodeId,
		InstanceID:            grpcResp.Database.InstanceId,
		InstanceName:          grpcResp.Database.InstanceName,
		DatabaseID:            grpcResp.Database.DatabaseId,
		DatabaseName:          grpcResp.Database.DatabaseName,
		DatabaseDescription:   grpcResp.Database.DatabaseDescription,
		DatabaseType:          grpcResp.Database.DatabaseType,
		DatabaseVendor:        grpcResp.Database.DatabaseVendor,
		DatabaseVersion:       grpcResp.Database.DatabaseVersion,
		DatabaseUsername:      grpcResp.Database.DatabaseUsername,
		DatabasePassword:      grpcResp.Database.DatabasePassword,
		DatabaseDBName:        grpcResp.Database.DatabaseDbName,
		DatabaseEnabled:       grpcResp.Database.DatabaseEnabled,
		PolicyIDs:             grpcResp.Database.PolicyIds,
		OwnerID:               grpcResp.Database.OwnerId,
		DatabaseStatusMessage: grpcResp.Database.DatabaseStatusMessage,
		Status:                convertStatus(grpcResp.Status),
		Created:               grpcResp.Database.Created,
		Updated:               grpcResp.Database.Updated,
		DatabaseSchema:        grpcResp.Database.DatabaseSchema,
		DatabaseTables:        grpcResp.Database.DatabaseTables,
		InstanceHost:          grpcResp.Database.InstanceHost,
		InstancePort:          grpcResp.Database.InstancePort,
		InstanceSSLMode:       grpcResp.Database.InstanceSslMode,
		InstanceSSLCert:       grpcResp.Database.InstanceSslCert,
		InstanceSSLKey:        grpcResp.Database.InstanceSslKey,
		InstanceSSLRootCert:   grpcResp.Database.InstanceSslRootCert,
		InstanceSSL:           grpcResp.Database.InstanceSsl,
		InstanceStatusMessage: grpcResp.Database.InstanceStatusMessage,
		InstanceStatus:        grpcResp.Database.InstanceStatus,
	}

	response := ConnectDatabaseStringResponse{
		Message:  grpcResp.Message,
		Success:  grpcResp.Success,
		Database: database,
		Status:   convertStatus(grpcResp.Status),
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Successfully connected database via connection string: %s", req.DatabaseName)
	}

	dh.writeJSONResponse(w, http.StatusOK, response)
}

// parseConnectionString parses a connection string and returns connection details
func (dh *DatabaseHandlers) parseConnectionString(connectionString string) (*dbcapabilities.ConnectionDetails, error) {
	return dbcapabilities.ParseConnectionString(connectionString)
}

// CloneDatabaseRequest represents the request payload for cloning a database
type CloneDatabaseRequest struct {
	SourceDatabaseName string               `json:"source_database_name"`
	Target             CloneDatabaseTarget  `json:"target"`
	Options            CloneDatabaseOptions `json:"options"`
	SourceNodeID       *uint64              `json:"source_node_id,omitempty"`
	TargetNodeID       *uint64              `json:"target_node_id,omitempty"`
}

type CloneDatabaseTarget struct {
	NewDatabase      *NewDatabaseTarget      `json:"new_database,omitempty"`
	ExistingDatabase *ExistingDatabaseTarget `json:"existing_database,omitempty"`
}

type NewDatabaseTarget struct {
	InstanceName string `json:"instance_name"`
	DatabaseName string `json:"database_name"`
}

type ExistingDatabaseTarget struct {
	DatabaseName string `json:"database_name"`
	Wipe         bool   `json:"wipe"`
	Merge        bool   `json:"merge"`
}

type CloneDatabaseOptions struct {
	WithData              bool              `json:"with_data"`
	Wipe                  bool              `json:"wipe"`
	Merge                 bool              `json:"merge"`
	TransformationOptions map[string]string `json:"transformation_options,omitempty"`
}

// CloneDatabaseResponse represents the response from cloning a database
type CloneDatabaseResponse struct {
	Message          string   `json:"message"`
	Success          bool     `json:"success"`
	Status           string   `json:"status"`
	TargetDatabaseId string   `json:"target_database_id"`
	TargetRepoId     string   `json:"target_repo_id"`
	TargetBranchId   string   `json:"target_branch_id"`
	TargetCommitId   string   `json:"target_commit_id"`
	Warnings         []string `json:"warnings"`
	RowsCopied       int64    `json:"rows_copied"`
}

// CloneDatabase handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/databases/clone-database
func (dh *DatabaseHandlers) CloneDatabase(w http.ResponseWriter, r *http.Request) {
	dh.engine.TrackOperation()
	defer dh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req CloneDatabaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		dh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Validate request
	if req.SourceDatabaseName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "source_database_name is required", "")
		return
	}

	// Validate target (must have exactly one)
	if req.Target.NewDatabase == nil && req.Target.ExistingDatabase == nil {
		dh.writeErrorResponse(w, http.StatusBadRequest, "target must be specified (new_database or existing_database)", "")
		return
	}

	if req.Target.NewDatabase != nil && req.Target.ExistingDatabase != nil {
		dh.writeErrorResponse(w, http.StatusBadRequest, "only one target type can be specified", "")
		return
	}

	// Log request
	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Clone database request: source=%s, workspace=%s, tenant=%s, user=%s",
			req.SourceDatabaseName, workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 300*time.Second) // 5 minutes for potentially long operation
	defer cancel()

	// Build gRPC request
	grpcReq := &corev1.CloneDatabaseRequest{
		TenantId:           profile.TenantId,
		WorkspaceName:      workspaceName,
		SourceDatabaseName: req.SourceDatabaseName,
		Options: &corev1.CloneOptions{
			WithData:              req.Options.WithData,
			Wipe:                  req.Options.Wipe,
			Merge:                 req.Options.Merge,
			TransformationOptions: req.Options.TransformationOptions,
		},
	}

	// Set target
	if req.Target.NewDatabase != nil {
		grpcReq.Target = &corev1.CloneDatabaseRequest_NewDatabase{
			NewDatabase: &corev1.NewDatabaseTarget{
				InstanceName: req.Target.NewDatabase.InstanceName,
				DatabaseName: req.Target.NewDatabase.DatabaseName,
			},
		}
	} else if req.Target.ExistingDatabase != nil {
		grpcReq.Target = &corev1.CloneDatabaseRequest_ExistingDatabase{
			ExistingDatabase: &corev1.ExistingDatabaseTarget{
				DatabaseName: req.Target.ExistingDatabase.DatabaseName,
				Wipe:         req.Target.ExistingDatabase.Wipe,
				Merge:        req.Target.ExistingDatabase.Merge,
			},
		}
	}

	// Call appropriate gRPC method based on cross-node requirements
	var grpcResp *corev1.CloneDatabaseResponse
	var err error

	if req.SourceNodeID != nil && req.TargetNodeID != nil {
		// Cross-node operation
		remoteReq := &corev1.CloneDatabaseRemoteRequest{
			Request:      grpcReq,
			SourceNodeId: *req.SourceNodeID,
			TargetNodeId: *req.TargetNodeID,
		}
		remoteResp, err := dh.engine.databaseClient.CloneDatabaseRemote(ctx, remoteReq)
		if err != nil {
			dh.handleGRPCError(w, err, "Failed to clone database across nodes")
			return
		}
		// Convert remote response to regular response
		grpcResp = &corev1.CloneDatabaseResponse{
			Message:          remoteResp.Message,
			Success:          remoteResp.Success,
			Status:           remoteResp.Status,
			TargetDatabaseId: remoteResp.TargetDatabaseId,
			TargetRepoId:     remoteResp.TargetRepoId,
			TargetBranchId:   remoteResp.TargetBranchId,
			TargetCommitId:   remoteResp.TargetCommitId,
			Warnings:         remoteResp.Warnings,
			RowsCopied:       remoteResp.RowsCopied,
		}
	} else {
		// Same-node operation
		grpcResp, err = dh.engine.databaseClient.CloneDatabase(ctx, grpcReq)
		if err != nil {
			dh.handleGRPCError(w, err, "Failed to clone database")
			return
		}
	}

	// Build response
	response := CloneDatabaseResponse{
		Message:          grpcResp.Message,
		Success:          grpcResp.Success,
		Status:           string(convertStatus(grpcResp.Status)),
		TargetDatabaseId: grpcResp.TargetDatabaseId,
		TargetRepoId:     grpcResp.TargetRepoId,
		TargetBranchId:   grpcResp.TargetBranchId,
		TargetCommitId:   grpcResp.TargetCommitId,
		Warnings:         grpcResp.Warnings,
		RowsCopied:       grpcResp.RowsCopied,
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Successfully cloned database: source=%s, target=%s",
			req.SourceDatabaseName, grpcResp.TargetDatabaseId)
	}

	dh.writeJSONResponse(w, http.StatusOK, response)
}
