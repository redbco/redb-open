package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
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

	// Convert resource containers
	resourceContainers := make([]DatabaseResourceContainer, len(grpcResp.Database.ResourceContainers))
	for i, protoContainer := range grpcResp.Database.ResourceContainers {
		resourceContainers[i] = convertProtoContainer(protoContainer)
	}
	database.ResourceContainers = resourceContainers

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
	grpcReq.DeleteBranch = req.DeleteBranch
	grpcReq.DisconnectInstance = req.DisconnectInstance

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

// GetDatabaseDisconnectMetadata handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/databases/{database_name}/disconnect-metadata
func (dh *DatabaseHandlers) GetDatabaseDisconnectMetadata(w http.ResponseWriter, r *http.Request) {
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
		dh.engine.logger.Infof("Get database disconnect metadata request for database: %s, workspace: %s, tenant: %s", databaseName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.GetDatabaseDisconnectMetadataRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		DatabaseName:  databaseName,
	}

	grpcResp, err := dh.engine.databaseClient.GetDatabaseDisconnectMetadata(ctx, grpcReq)
	if err != nil {
		dh.handleGRPCError(w, err, "Failed to get database disconnect metadata")
		return
	}

	metadata := DatabaseDisconnectMetadata{
		DatabaseName:              grpcResp.DatabaseName,
		InstanceName:              grpcResp.InstanceName,
		IsLastDatabaseInInstance:  grpcResp.IsLastDatabaseInInstance,
		TotalDatabasesInInstance:  grpcResp.TotalDatabasesInInstance,
		HasAttachedBranch:         grpcResp.HasAttachedBranch,
		AttachedRepoName:          grpcResp.AttachedRepoName,
		AttachedBranchName:        grpcResp.AttachedBranchName,
		IsOnlyBranchInRepo:        grpcResp.IsOnlyBranchInRepo,
		TotalBranchesInRepo:       grpcResp.TotalBranchesInRepo,
		HasOtherDatabasesOnBranch: grpcResp.HasOtherDatabasesOnBranch,
		CanDeleteBranchOnly:       grpcResp.CanDeleteBranchOnly,
		CanDeleteEntireRepo:       grpcResp.CanDeleteEntireRepo,
		ShouldDeleteRepo:          grpcResp.ShouldDeleteRepo,
		ShouldDeleteBranch:        grpcResp.ShouldDeleteBranch,
	}

	response := GetDatabaseDisconnectMetadataResponse{
		Message:  grpcResp.Message,
		Success:  grpcResp.Success,
		Status:   convertStatus(grpcResp.Status),
		Metadata: metadata,
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Successfully retrieved disconnect metadata for database: %s for workspace: %s", databaseName, workspaceName)
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

	// Unwrap the Any type to get the actual schema data
	var schemaData interface{}
	if grpcResp.Schema != nil {
		var bytesVal wrapperspb.BytesValue
		if err := grpcResp.Schema.UnmarshalTo(&bytesVal); err != nil {
			dh.writeErrorResponse(w, http.StatusInternalServerError, "Failed to unmarshal schema data", "")
			return
		}
		// Parse the JSON bytes into an interface{}
		if err := json.Unmarshal(bytesVal.Value, &schemaData); err != nil {
			dh.writeErrorResponse(w, http.StatusInternalServerError, "Failed to parse schema JSON", "")
			return
		}
	}

	response := GetLatestStoredDatabaseSchemaResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
		Schema:  schemaData,
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
	// Log error responses for monitoring and debugging
	if dh.engine.logger != nil {
		if statusCode >= 500 {
			// Log 5xx errors as errors
			dh.engine.logger.Errorf("HTTP %d - %s: %s", statusCode, message, details)
		} else if statusCode >= 400 {
			// Log 4xx errors as warnings
			dh.engine.logger.Warnf("HTTP %d - %s: %s", statusCode, message, details)
		}
	}

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

type TableColumnSchema struct {
	Name                     string   `json:"name"`
	ItemDisplayName          string   `json:"item_display_name,omitempty"`
	DataType                 string   `json:"data_type"`
	UnifiedDataType          string   `json:"unified_data_type,omitempty"`
	IsNullable               bool     `json:"is_nullable"`
	IsPrimaryKey             bool     `json:"is_primary_key"`
	IsUnique                 bool     `json:"is_unique"`
	IsIndexed                bool     `json:"is_indexed"`
	IsRequired               bool     `json:"is_required"`
	IsArray                  bool     `json:"is_array"`
	DefaultValue             string   `json:"default_value,omitempty"`
	Constraints              []string `json:"constraints"`
	IsPrivileged             bool     `json:"is_privileged"`
	PrivilegedClassification string   `json:"privileged_classification,omitempty"`
	PrivilegedConfidence     float32  `json:"privileged_confidence,omitempty"`
	DetectionMethod          string   `json:"detection_method,omitempty"`
	DataCategory             string   `json:"data_category,omitempty"`
	ClassificationConfidence float32  `json:"classification_confidence,omitempty"`
	OrdinalPosition          int32    `json:"ordinal_position"`
	MaxLength                *int32   `json:"max_length,omitempty"`
	Precision                *int32   `json:"precision,omitempty"`
	Scale                    *int32   `json:"scale,omitempty"`
	ItemComment              string   `json:"item_comment,omitempty"`
	ResourceURI              string   `json:"resource_uri,omitempty"`
	ContainerURI             string   `json:"container_uri,omitempty"`
}

// FetchTableDataResponse represents the response from fetching table data
type FetchTableDataResponse struct {
	Message       string                   `json:"message"`
	Success       bool                     `json:"success"`
	Status        string                   `json:"status"`
	Data          []map[string]interface{} `json:"data"`
	TotalRows     int64                    `json:"total_rows"`
	Page          int32                    `json:"page"`
	PageSize      int32                    `json:"page_size"`
	TotalPages    int32                    `json:"total_pages"`
	ColumnSchemas []TableColumnSchema      `json:"column_schemas"`
}

// WipeTableResponse represents the response from wiping a table
type WipeTableResponse struct {
	Message      string `json:"message"`
	Success      bool   `json:"success"`
	Status       string `json:"status"`
	RowsAffected int64  `json:"rows_affected"`
}

// DropTableResponse represents the response from dropping a table
type DropTableResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  string `json:"status"`
}

// UpdateTableDataRequest represents a request to update table data
type UpdateTableDataRequest struct {
	Updates []map[string]interface{} `json:"updates"` // Array of {where: {...}, set: {...}}
}

// UpdateTableDataResponse represents the response from updating table data
type UpdateTableDataResponse struct {
	Message      string `json:"message"`
	Success      bool   `json:"success"`
	Status       string `json:"status"`
	RowsAffected int64  `json:"rows_affected"`
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

// FetchTableData handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/databases/{database_name}/tables/{table_name}/data
func (dh *DatabaseHandlers) FetchTableData(w http.ResponseWriter, r *http.Request) {
	dh.engine.TrackOperation()
	defer dh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	databaseName := vars["database_name"]
	tableName := vars["table_name"]

	if tenantURL == "" || workspaceName == "" || databaseName == "" || tableName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, database_name, and table_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse query parameters for pagination
	page := int32(1)
	pageSize := int32(25) // Default page size

	if pageStr := r.URL.Query().Get("page"); pageStr != "" {
		if p, err := strconv.ParseInt(pageStr, 10, 32); err == nil && p > 0 {
			page = int32(p)
		}
	}

	if pageSizeStr := r.URL.Query().Get("page_size"); pageSizeStr != "" {
		if ps, err := strconv.ParseInt(pageSizeStr, 10, 32); err == nil && ps > 0 && ps <= 100 {
			pageSize = int32(ps)
		}
	}

	// Log request
	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Fetch table data request: database=%s, table=%s, page=%d, page_size=%d, workspace=%s",
			databaseName, tableName, page, pageSize, workspaceName)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.FetchTableDataRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		DatabaseName:  databaseName,
		TableName:     tableName,
		Page:          page,
		PageSize:      pageSize,
	}

	grpcResp, err := dh.engine.databaseClient.FetchTableData(ctx, grpcReq)
	if err != nil {
		dh.handleGRPCError(w, err, "Failed to fetch table data")
		return
	}

	// Convert column schemas with all new fields
	columnSchemas := make([]TableColumnSchema, len(grpcResp.ColumnSchemas))
	for i, col := range grpcResp.ColumnSchemas {
		schema := TableColumnSchema{
			Name:                     col.Name,
			ItemDisplayName:          col.ItemDisplayName,
			DataType:                 col.DataType,
			UnifiedDataType:          col.UnifiedDataType,
			IsNullable:               col.IsNullable,
			IsPrimaryKey:             col.IsPrimaryKey,
			IsUnique:                 col.IsUnique,
			IsIndexed:                col.IsIndexed,
			IsRequired:               col.IsRequired,
			IsArray:                  col.IsArray,
			DefaultValue:             col.DefaultValue,
			Constraints:              col.Constraints,
			IsPrivileged:             col.IsPrivileged,
			PrivilegedClassification: col.PrivilegedClassification,
			PrivilegedConfidence:     col.PrivilegedConfidence,
			DetectionMethod:          col.DetectionMethod,
			DataCategory:             col.DataCategory,
			ClassificationConfidence: col.ClassificationConfidence,
			OrdinalPosition:          col.OrdinalPosition,
			ItemComment:              col.ItemComment,
			ResourceURI:              col.ResourceUri,
			ContainerURI:             col.ContainerUri,
		}

		// Set optional numeric fields
		if col.MaxLength > 0 {
			maxLen := col.MaxLength
			schema.MaxLength = &maxLen
		}
		if col.Precision > 0 {
			prec := col.Precision
			schema.Precision = &prec
		}
		if col.Scale > 0 {
			scale := col.Scale
			schema.Scale = &scale
		}

		columnSchemas[i] = schema
	}

	// Parse data bytes to JSON
	var dataRows []map[string]interface{}
	if err := json.Unmarshal(grpcResp.Data, &dataRows); err != nil {
		dh.writeErrorResponse(w, http.StatusInternalServerError, "Failed to parse table data", "")
		return
	}

	response := FetchTableDataResponse{
		Message:       grpcResp.Message,
		Success:       grpcResp.Success,
		Status:        string(convertStatus(grpcResp.Status)),
		Data:          dataRows,
		TotalRows:     grpcResp.TotalRows,
		Page:          grpcResp.Page,
		PageSize:      grpcResp.PageSize,
		TotalPages:    grpcResp.TotalPages,
		ColumnSchemas: columnSchemas,
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Successfully fetched %d rows from table %s (page %d/%d)",
			len(dataRows), tableName, page, grpcResp.TotalPages)
	}

	dh.writeJSONResponse(w, http.StatusOK, response)
}

// WipeTable handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/databases/{database_name}/tables/{table_name}/wipe
func (dh *DatabaseHandlers) WipeTable(w http.ResponseWriter, r *http.Request) {
	dh.engine.TrackOperation()
	defer dh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	databaseName := vars["database_name"]
	tableName := vars["table_name"]

	if tenantURL == "" || workspaceName == "" || databaseName == "" || tableName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, database_name, and table_name are required", "")
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
		dh.engine.logger.Warnf("Wipe table request: database=%s, table=%s, workspace=%s, user=%s",
			databaseName, tableName, workspaceName, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.WipeTableRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		DatabaseName:  databaseName,
		TableName:     tableName,
	}

	grpcResp, err := dh.engine.databaseClient.WipeTable(ctx, grpcReq)
	if err != nil {
		dh.handleGRPCError(w, err, "Failed to wipe table")
		return
	}

	response := WipeTableResponse{
		Message:      grpcResp.Message,
		Success:      grpcResp.Success,
		Status:       string(convertStatus(grpcResp.Status)),
		RowsAffected: grpcResp.RowsAffected,
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Successfully wiped table %s (%d rows affected)", tableName, grpcResp.RowsAffected)
	}

	dh.writeJSONResponse(w, http.StatusOK, response)
}

// DropTable handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/databases/{database_name}/tables/{table_name}/drop
func (dh *DatabaseHandlers) DropTable(w http.ResponseWriter, r *http.Request) {
	dh.engine.TrackOperation()
	defer dh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	databaseName := vars["database_name"]
	tableName := vars["table_name"]

	if tenantURL == "" || workspaceName == "" || databaseName == "" || tableName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, database_name, and table_name are required", "")
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
		dh.engine.logger.Warnf("Drop table request: database=%s, table=%s, workspace=%s, user=%s",
			databaseName, tableName, workspaceName, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DropTableRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		DatabaseName:  databaseName,
		TableName:     tableName,
	}

	grpcResp, err := dh.engine.databaseClient.DropTable(ctx, grpcReq)
	if err != nil {
		dh.handleGRPCError(w, err, "Failed to drop table")
		return
	}

	response := DropTableResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  string(convertStatus(grpcResp.Status)),
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Successfully dropped table %s", tableName)
	}

	dh.writeJSONResponse(w, http.StatusOK, response)
}

// UpdateTableData handles PUT /{tenant_url}/api/v1/workspaces/{workspace_name}/databases/{database_name}/tables/{table_name}/data
func (dh *DatabaseHandlers) UpdateTableData(w http.ResponseWriter, r *http.Request) {
	dh.engine.TrackOperation()
	defer dh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	databaseName := vars["database_name"]
	tableName := vars["table_name"]

	if tenantURL == "" || workspaceName == "" || databaseName == "" || tableName == "" {
		dh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, database_name, and table_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req UpdateTableDataRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if dh.engine.logger != nil {
			dh.engine.logger.Errorf("Failed to parse update table data request body: %v", err)
		}
		dh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Update table data request: database=%s, table=%s, updates=%d",
			databaseName, tableName, len(req.Updates))
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	// Convert updates to JSON bytes
	updatesJSON, err := json.Marshal(req.Updates)
	if err != nil {
		dh.writeErrorResponse(w, http.StatusBadRequest, "Invalid updates format", "")
		return
	}

	// Call core service gRPC
	grpcReq := &corev1.UpdateTableDataRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		DatabaseName:  databaseName,
		TableName:     tableName,
		Updates:       updatesJSON,
	}

	grpcResp, err := dh.engine.databaseClient.UpdateTableData(ctx, grpcReq)
	if err != nil {
		dh.handleGRPCError(w, err, "Failed to update table data")
		return
	}

	response := UpdateTableDataResponse{
		Message:      grpcResp.Message,
		Success:      grpcResp.Success,
		Status:       string(convertStatus(grpcResp.Status)),
		RowsAffected: grpcResp.RowsAffected,
	}

	if dh.engine.logger != nil {
		dh.engine.logger.Infof("Successfully updated table %s (%d rows affected)", tableName, grpcResp.RowsAffected)
	}

	dh.writeJSONResponse(w, http.StatusOK, response)
}

// convertProtoContainer converts a protobuf DatabaseResourceContainer to REST model
func convertProtoContainer(proto *corev1.DatabaseResourceContainer) DatabaseResourceContainer {
	container := DatabaseResourceContainer{
		ObjectType:                    proto.ObjectType,
		ObjectName:                    proto.ObjectName,
		ContainerClassificationSource: proto.ContainerClassificationSource,
		ItemCount:                     proto.ItemCount,
		Status:                        proto.Status,
		Items:                         make([]DatabaseResourceItem, len(proto.Items)),
	}

	if proto.ContainerClassification != nil {
		container.ContainerClassification = *proto.ContainerClassification
	}

	if proto.ContainerClassificationConfidence != nil {
		container.ContainerClassificationConfidence = *proto.ContainerClassificationConfidence
	}

	if proto.DatabaseType != nil {
		container.DatabaseType = *proto.DatabaseType
	}

	if proto.Vendor != nil {
		container.Vendor = *proto.Vendor
	}

	// Parse container metadata JSON
	if proto.ContainerMetadataJson != "" && proto.ContainerMetadataJson != "{}" {
		var metadata map[string]interface{}
		if err := json.Unmarshal([]byte(proto.ContainerMetadataJson), &metadata); err == nil {
			container.ContainerMetadata = metadata
		}
	}

	// Parse enriched metadata JSON
	if proto.EnrichedMetadataJson != "" && proto.EnrichedMetadataJson != "{}" {
		var enriched map[string]interface{}
		if err := json.Unmarshal([]byte(proto.EnrichedMetadataJson), &enriched); err == nil {
			container.EnrichedMetadata = enriched
		}
	}

	// Convert items
	for i, protoItem := range proto.Items {
		container.Items[i] = convertProtoItem(protoItem)
	}

	return container
}

// convertProtoItem converts a protobuf DatabaseResourceItem to REST model
func convertProtoItem(proto *corev1.DatabaseResourceItem) DatabaseResourceItem {
	item := DatabaseResourceItem{
		ItemName:        proto.ItemName,
		ItemDisplayName: proto.ItemDisplayName,
		DataType:        proto.DataType,
		IsNullable:      proto.IsNullable,
		IsPrimaryKey:    proto.IsPrimaryKey,
		IsUnique:        proto.IsUnique,
		IsIndexed:       proto.IsIndexed,
		IsRequired:      proto.IsRequired,
		IsArray:         proto.IsArray,
		IsPrivileged:    proto.IsPrivileged,
		OrdinalPosition: proto.OrdinalPosition,
	}

	if proto.UnifiedDataType != nil {
		item.UnifiedDataType = *proto.UnifiedDataType
	}

	if proto.DefaultValue != nil {
		item.DefaultValue = *proto.DefaultValue
	}

	if proto.PrivilegedClassification != nil {
		item.PrivilegedClassification = *proto.PrivilegedClassification
	}

	if proto.DetectionConfidence != nil {
		item.DetectionConfidence = *proto.DetectionConfidence
	}

	if proto.DetectionMethod != nil {
		item.DetectionMethod = *proto.DetectionMethod
	}

	if proto.MaxLength != nil {
		item.MaxLength = *proto.MaxLength
	}

	if proto.Precision != nil {
		item.Precision = *proto.Precision
	}

	if proto.Scale != nil {
		item.Scale = *proto.Scale
	}

	if proto.ItemComment != nil {
		item.ItemComment = *proto.ItemComment
	}

	// Parse constraints JSON
	if proto.ConstraintsJson != "" && proto.ConstraintsJson != "[]" {
		var constraints []map[string]interface{}
		if err := json.Unmarshal([]byte(proto.ConstraintsJson), &constraints); err == nil {
			item.Constraints = constraints
		}
	}

	return item
}
