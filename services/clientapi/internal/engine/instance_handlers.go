package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// InstanceHandlers contains the instance endpoint handlers
type InstanceHandlers struct {
	engine *Engine
}

// NewInstanceHandlers creates a new instance of InstanceHandlers
func NewInstanceHandlers(engine *Engine) *InstanceHandlers {
	return &InstanceHandlers{
		engine: engine,
	}
}

// ListInstances handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/instances
func (ih *InstanceHandlers) ListInstances(w http.ResponseWriter, r *http.Request) {
	ih.engine.TrackOperation()
	defer ih.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" {
		ih.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		ih.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ih.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if ih.engine.logger != nil {
		ih.engine.logger.Infof("List instances request for workspace: %s, tenant: %s, user: %s", workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListInstancesRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
	}

	grpcResp, err := ih.engine.instanceClient.ListInstances(ctx, grpcReq)
	if err != nil {
		ih.handleGRPCError(w, err, "Failed to list instances")
		return
	}

	// Convert gRPC response to REST response
	instances := make([]Instance, len(grpcResp.Instances))
	for i, inst := range grpcResp.Instances {
		instances[i] = Instance{
			TenantID:                 inst.TenantId,
			WorkspaceID:              inst.WorkspaceId,
			EnvironmentID:            inst.EnvironmentId,
			InstanceID:               inst.InstanceId,
			InstanceName:             inst.InstanceName,
			InstanceDescription:      inst.InstanceDescription,
			InstanceType:             inst.InstanceType,
			InstanceVendor:           inst.InstanceVendor,
			InstanceVersion:          inst.InstanceVersion,
			InstanceUniqueIdentifier: inst.InstanceUniqueIdentifier,
			ConnectedToNodeID:        inst.ConnectedToNodeId,
			InstanceHost:             inst.InstanceHost,
			InstancePort:             inst.InstancePort,
			InstanceUsername:         inst.InstanceUsername,
			InstancePassword:         inst.InstancePassword,
			InstanceSystemDBName:     inst.InstanceSystemDbName,
			InstanceEnabled:          inst.InstanceEnabled,
			InstanceSSL:              inst.InstanceSsl,
			InstanceSSLMode:          inst.InstanceSslMode,
			InstanceSSLCert:          inst.InstanceSslCert,
			InstanceSSLKey:           inst.InstanceSslKey,
			InstanceSSLRootCert:      inst.InstanceSslRootCert,
			PolicyIDs:                inst.PolicyIds,
			OwnerID:                  inst.OwnerId,
			InstanceStatusMessage:    inst.InstanceStatusMessage,
			Status:                   convertStatus(inst.Status),
			Created:                  inst.Created,
			Updated:                  inst.Updated,
		}
	}

	response := ListInstancesResponse{
		Instances: instances,
	}

	if ih.engine.logger != nil {
		ih.engine.logger.Infof("Successfully listed %d instances for workspace: %s", len(instances), workspaceName)
	}

	ih.writeJSONResponse(w, http.StatusOK, response)
}

// ShowInstance handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/instances/{instance_name}
func (ih *InstanceHandlers) ShowInstance(w http.ResponseWriter, r *http.Request) {
	ih.engine.TrackOperation()
	defer ih.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	instanceName := vars["instance_name"]

	if tenantURL == "" {
		ih.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		ih.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	if instanceName == "" {
		ih.writeErrorResponse(w, http.StatusBadRequest, "instance_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ih.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if ih.engine.logger != nil {
		ih.engine.logger.Infof("Show instance request for instance: %s, workspace: %s, tenant: %s, user: %s", instanceName, workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowInstanceRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		InstanceName:  instanceName,
	}

	grpcResp, err := ih.engine.instanceClient.ShowInstance(ctx, grpcReq)
	if err != nil {
		ih.handleGRPCError(w, err, "Failed to show instance")
		return
	}

	// Convert gRPC response to REST response
	instance := Instance{
		TenantID:                 grpcResp.Instance.TenantId,
		WorkspaceID:              grpcResp.Instance.WorkspaceId,
		EnvironmentID:            grpcResp.Instance.EnvironmentId,
		InstanceID:               grpcResp.Instance.InstanceId,
		InstanceName:             grpcResp.Instance.InstanceName,
		InstanceDescription:      grpcResp.Instance.InstanceDescription,
		InstanceType:             grpcResp.Instance.InstanceType,
		InstanceVendor:           grpcResp.Instance.InstanceVendor,
		InstanceVersion:          grpcResp.Instance.InstanceVersion,
		InstanceUniqueIdentifier: grpcResp.Instance.InstanceUniqueIdentifier,
		ConnectedToNodeID:        grpcResp.Instance.ConnectedToNodeId,
		InstanceHost:             grpcResp.Instance.InstanceHost,
		InstancePort:             grpcResp.Instance.InstancePort,
		InstanceUsername:         grpcResp.Instance.InstanceUsername,
		InstancePassword:         grpcResp.Instance.InstancePassword,
		InstanceSystemDBName:     grpcResp.Instance.InstanceSystemDbName,
		InstanceEnabled:          grpcResp.Instance.InstanceEnabled,
		InstanceSSL:              grpcResp.Instance.InstanceSsl,
		InstanceSSLMode:          grpcResp.Instance.InstanceSslMode,
		InstanceSSLCert:          grpcResp.Instance.InstanceSslCert,
		InstanceSSLKey:           grpcResp.Instance.InstanceSslKey,
		InstanceSSLRootCert:      grpcResp.Instance.InstanceSslRootCert,
		PolicyIDs:                grpcResp.Instance.PolicyIds,
		OwnerID:                  grpcResp.Instance.OwnerId,
		InstanceStatusMessage:    grpcResp.Instance.InstanceStatusMessage,
		Status:                   convertStatus(grpcResp.Instance.Status),
		Created:                  grpcResp.Instance.Created,
		Updated:                  grpcResp.Instance.Updated,
	}

	response := ShowInstanceResponse{
		Instance: instance,
	}

	if ih.engine.logger != nil {
		ih.engine.logger.Infof("Successfully showed instance: %s for workspace: %s", instanceName, workspaceName)
	}

	ih.writeJSONResponse(w, http.StatusOK, response)
}

// ConnectInstance handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/instances
func (ih *InstanceHandlers) ConnectInstance(w http.ResponseWriter, r *http.Request) {
	ih.engine.TrackOperation()
	defer ih.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" {
		ih.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		ih.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ih.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ConnectInstanceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if ih.engine.logger != nil {
			ih.engine.logger.Errorf("Failed to parse connect instance request body: %v", err)
		}
		ih.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.InstanceName == "" {
		ih.writeErrorResponse(w, http.StatusBadRequest, "instance_name is required", "")
		return
	}
	if req.InstanceDescription == "" {
		ih.writeErrorResponse(w, http.StatusBadRequest, "instance_description is required", "")
		return
	}
	if req.InstanceType == "" {
		ih.writeErrorResponse(w, http.StatusBadRequest, "instance_type is required", "")
		return
	}
	if req.Host == "" {
		ih.writeErrorResponse(w, http.StatusBadRequest, "host is required", "")
		return
	}

	// Log request
	if ih.engine.logger != nil {
		ih.engine.logger.Infof("Connect instance request: %s for workspace: %s, tenant: %s, user: %s", req.InstanceName, workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request - owner_id is always the authenticated user
	grpcReq := &corev1.ConnectInstanceRequest{
		TenantId:            profile.TenantId,
		WorkspaceName:       workspaceName,
		InstanceName:        req.InstanceName,
		InstanceDescription: req.InstanceDescription,
		InstanceType:        req.InstanceType,
		Host:                req.Host,
		Port:                req.Port,
		Username:            req.Username,
		Password:            req.Password,
		NodeId:              req.NodeID,
		OwnerId:             profile.UserId,
	}

	// Set optional fields
	if req.Enabled != nil {
		grpcReq.Enabled = req.Enabled
	}
	if req.SSL != nil {
		grpcReq.Ssl = req.SSL
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
	if req.EnvironmentID != "" {
		grpcReq.EnvironmentId = &req.EnvironmentID
	}

	// Detect vendor from host if not provided
	vendor := req.InstanceVendor
	if vendor == "" {
		vendor = detectVendorFromHost(req.Host)
		if ih.engine.logger != nil {
			ih.engine.logger.Infof("Auto-detected vendor '%s' from host '%s'", vendor, req.Host)
		}
	}
	grpcReq.InstanceVendor = vendor

	grpcResp, err := ih.engine.instanceClient.ConnectInstance(ctx, grpcReq)
	if err != nil {
		ih.handleGRPCError(w, err, "Failed to connect instance")
		return
	}

	// Convert gRPC response to REST response
	instance := Instance{
		TenantID:                 grpcResp.Instance.TenantId,
		WorkspaceID:              grpcResp.Instance.WorkspaceId,
		EnvironmentID:            grpcResp.Instance.EnvironmentId,
		InstanceID:               grpcResp.Instance.InstanceId,
		InstanceName:             grpcResp.Instance.InstanceName,
		InstanceDescription:      grpcResp.Instance.InstanceDescription,
		InstanceType:             grpcResp.Instance.InstanceType,
		InstanceVendor:           grpcResp.Instance.InstanceVendor,
		InstanceVersion:          grpcResp.Instance.InstanceVersion,
		InstanceUniqueIdentifier: grpcResp.Instance.InstanceUniqueIdentifier,
		ConnectedToNodeID:        grpcResp.Instance.ConnectedToNodeId,
		InstanceHost:             grpcResp.Instance.InstanceHost,
		InstancePort:             grpcResp.Instance.InstancePort,
		InstanceUsername:         grpcResp.Instance.InstanceUsername,
		InstancePassword:         grpcResp.Instance.InstancePassword,
		InstanceSystemDBName:     grpcResp.Instance.InstanceSystemDbName,
		InstanceEnabled:          grpcResp.Instance.InstanceEnabled,
		InstanceSSL:              grpcResp.Instance.InstanceSsl,
		InstanceSSLMode:          grpcResp.Instance.InstanceSslMode,
		InstanceSSLCert:          grpcResp.Instance.InstanceSslCert,
		InstanceSSLKey:           grpcResp.Instance.InstanceSslKey,
		InstanceSSLRootCert:      grpcResp.Instance.InstanceSslRootCert,
		PolicyIDs:                grpcResp.Instance.PolicyIds,
		OwnerID:                  grpcResp.Instance.OwnerId,
		InstanceStatusMessage:    grpcResp.Instance.InstanceStatusMessage,
		Status:                   convertStatus(grpcResp.Instance.Status),
		Created:                  grpcResp.Instance.Created,
		Updated:                  grpcResp.Instance.Updated,
	}

	response := ConnectInstanceResponse{
		Message:  grpcResp.Message,
		Success:  grpcResp.Success,
		Instance: instance,
		Status:   convertStatus(grpcResp.Status),
	}

	if ih.engine.logger != nil {
		ih.engine.logger.Infof("Successfully connected instance: %s for workspace: %s", req.InstanceName, workspaceName)
	}

	ih.writeJSONResponse(w, http.StatusCreated, response)
}

// ReconnectInstance handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/instances/{instance_name}/reconnect
func (ih *InstanceHandlers) ReconnectInstance(w http.ResponseWriter, r *http.Request) {
	ih.engine.TrackOperation()
	defer ih.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	instanceName := vars["instance_name"]

	if tenantURL == "" || workspaceName == "" || instanceName == "" {
		ih.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and instance_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ih.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if ih.engine.logger != nil {
		ih.engine.logger.Infof("Reconnect instance request for instance: %s, workspace: %s, tenant: %s", instanceName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.ReconnectInstanceRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		InstanceName:  instanceName,
	}

	grpcResp, err := ih.engine.instanceClient.ReconnectInstance(ctx, grpcReq)
	if err != nil {
		ih.handleGRPCError(w, err, "Failed to reconnect instance")
		return
	}

	response := ReconnectInstanceResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if ih.engine.logger != nil {
		ih.engine.logger.Infof("Successfully reconnected instance: %s", instanceName)
	}

	ih.writeJSONResponse(w, http.StatusOK, response)
}

// ModifyInstance handles PUT /{tenant_url}/api/v1/workspaces/{workspace_name}/instances/{instance_name}
func (ih *InstanceHandlers) ModifyInstance(w http.ResponseWriter, r *http.Request) {
	ih.engine.TrackOperation()
	defer ih.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	instanceName := vars["instance_name"]

	if tenantURL == "" || workspaceName == "" || instanceName == "" {
		ih.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and instance_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ih.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ModifyInstanceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if ih.engine.logger != nil {
			ih.engine.logger.Errorf("Failed to parse modify instance request body: %v", err)
		}
		ih.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if ih.engine.logger != nil {
		ih.engine.logger.Infof("Modify instance request for instance: %s, workspace: %s, tenant: %s", instanceName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.ModifyInstanceRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		InstanceName:  instanceName,
	}

	// Set optional fields
	if req.InstanceNameNew != "" {
		grpcReq.InstanceNameNew = &req.InstanceNameNew
	}
	if req.InstanceDescription != "" {
		grpcReq.InstanceDescription = &req.InstanceDescription
	}
	if req.InstanceType != "" {
		grpcReq.InstanceType = &req.InstanceType
	}
	if req.Host != "" {
		grpcReq.Host = &req.Host
	}
	if req.Port != nil {
		grpcReq.Port = req.Port
	}
	if req.Username != "" {
		grpcReq.Username = &req.Username
	}
	if req.Password != "" {
		grpcReq.Password = &req.Password
	}
	if req.Enabled != nil {
		grpcReq.Enabled = req.Enabled
	}
	if req.SSL != nil {
		grpcReq.Ssl = req.SSL
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
	if req.EnvironmentID != "" {
		grpcReq.EnvironmentId = &req.EnvironmentID
	}
	if req.NodeID != "" {
		grpcReq.NodeId = &req.NodeID
	}

	grpcResp, err := ih.engine.instanceClient.ModifyInstance(ctx, grpcReq)
	if err != nil {
		ih.handleGRPCError(w, err, "Failed to modify instance")
		return
	}

	// Convert gRPC response to REST response
	instance := Instance{
		TenantID:                 grpcResp.Instance.TenantId,
		WorkspaceID:              grpcResp.Instance.WorkspaceId,
		EnvironmentID:            grpcResp.Instance.EnvironmentId,
		InstanceID:               grpcResp.Instance.InstanceId,
		InstanceName:             grpcResp.Instance.InstanceName,
		InstanceDescription:      grpcResp.Instance.InstanceDescription,
		InstanceType:             grpcResp.Instance.InstanceType,
		InstanceVendor:           grpcResp.Instance.InstanceVendor,
		InstanceVersion:          grpcResp.Instance.InstanceVersion,
		InstanceUniqueIdentifier: grpcResp.Instance.InstanceUniqueIdentifier,
		ConnectedToNodeID:        grpcResp.Instance.ConnectedToNodeId,
		InstanceHost:             grpcResp.Instance.InstanceHost,
		InstancePort:             grpcResp.Instance.InstancePort,
		InstanceUsername:         grpcResp.Instance.InstanceUsername,
		InstancePassword:         grpcResp.Instance.InstancePassword,
		InstanceSystemDBName:     grpcResp.Instance.InstanceSystemDbName,
		InstanceEnabled:          grpcResp.Instance.InstanceEnabled,
		InstanceSSL:              grpcResp.Instance.InstanceSsl,
		InstanceSSLMode:          grpcResp.Instance.InstanceSslMode,
		InstanceSSLCert:          grpcResp.Instance.InstanceSslCert,
		InstanceSSLKey:           grpcResp.Instance.InstanceSslKey,
		InstanceSSLRootCert:      grpcResp.Instance.InstanceSslRootCert,
		PolicyIDs:                grpcResp.Instance.PolicyIds,
		OwnerID:                  grpcResp.Instance.OwnerId,
		InstanceStatusMessage:    grpcResp.Instance.InstanceStatusMessage,
		Status:                   convertStatus(grpcResp.Instance.Status),
		Created:                  grpcResp.Instance.Created,
		Updated:                  grpcResp.Instance.Updated,
	}

	response := ModifyInstanceResponse{
		Message:  grpcResp.Message,
		Success:  grpcResp.Success,
		Instance: instance,
		Status:   convertStatus(grpcResp.Status),
	}

	if ih.engine.logger != nil {
		ih.engine.logger.Infof("Successfully modified instance: %s", instanceName)
	}

	ih.writeJSONResponse(w, http.StatusOK, response)
}

// DisconnectInstance handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/instances/{instance_name}/disconnect
func (ih *InstanceHandlers) DisconnectInstance(w http.ResponseWriter, r *http.Request) {
	ih.engine.TrackOperation()
	defer ih.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	instanceName := vars["instance_name"]

	if tenantURL == "" || workspaceName == "" || instanceName == "" {
		ih.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and instance_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ih.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body (optional)
	var req DisconnectInstanceRequest
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			if ih.engine.logger != nil {
				ih.engine.logger.Errorf("Failed to parse disconnect instance request body: %v", err)
			}
			ih.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
			return
		}
	}

	// Log request
	if ih.engine.logger != nil {
		ih.engine.logger.Infof("Disconnect instance request for instance: %s, workspace: %s, tenant: %s", instanceName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.DisconnectInstanceRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		InstanceName:  instanceName,
	}

	// Set optional fields
	if req.DeleteInstance != nil {
		grpcReq.DeleteInstance = req.DeleteInstance
	}

	grpcResp, err := ih.engine.instanceClient.DisconnectInstance(ctx, grpcReq)
	if err != nil {
		ih.handleGRPCError(w, err, "Failed to disconnect instance")
		return
	}

	response := DisconnectInstanceResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if ih.engine.logger != nil {
		ih.engine.logger.Infof("Successfully disconnected instance: %s", instanceName)
	}

	ih.writeJSONResponse(w, http.StatusOK, response)
}

// CreateDatabase handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/instances/{instance_name}/create
func (ih *InstanceHandlers) CreateDatabase(w http.ResponseWriter, r *http.Request) {
	ih.engine.TrackOperation()
	defer ih.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	instanceName := vars["instance_name"]

	if tenantURL == "" || workspaceName == "" || instanceName == "" {
		ih.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and instance_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ih.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if ih.engine.logger != nil {
		ih.engine.logger.Infof("Create database request for instance: %s, workspace: %s, tenant: %s", instanceName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.CreateDatabaseRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		InstanceName:  instanceName,
	}

	grpcResp, err := ih.engine.instanceClient.CreateDatabase(ctx, grpcReq)
	if err != nil {
		ih.handleGRPCError(w, err, "Failed to create database")
		return
	}

	response := CreateDatabaseResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if ih.engine.logger != nil {
		ih.engine.logger.Infof("Successfully created database: %s", instanceName)
	}

	ih.writeJSONResponse(w, http.StatusOK, response)
}

// Helper methods

// handleGRPCError handles gRPC errors and converts them to HTTP responses
func (ih *InstanceHandlers) handleGRPCError(w http.ResponseWriter, err error, defaultMessage string) {
	if ih.engine.logger != nil {
		ih.engine.logger.Errorf("gRPC error: %v", err)
	}

	st, ok := status.FromError(err)
	if !ok {
		ih.writeErrorResponse(w, http.StatusInternalServerError, defaultMessage, err.Error())
		return
	}

	switch st.Code() {
	case codes.NotFound:
		ih.writeErrorResponse(w, http.StatusNotFound, "Resource not found", st.Message())
	case codes.AlreadyExists:
		ih.writeErrorResponse(w, http.StatusConflict, "Resource already exists", st.Message())
	case codes.InvalidArgument:
		ih.writeErrorResponse(w, http.StatusBadRequest, "Invalid request", st.Message())
	case codes.PermissionDenied:
		ih.writeErrorResponse(w, http.StatusForbidden, "Permission denied", st.Message())
	case codes.Unauthenticated:
		ih.writeErrorResponse(w, http.StatusUnauthorized, "Authentication required", st.Message())
	case codes.Unavailable:
		ih.writeErrorResponse(w, http.StatusServiceUnavailable, "Service unavailable", st.Message())
	case codes.DeadlineExceeded:
		ih.writeErrorResponse(w, http.StatusRequestTimeout, "Request timeout", st.Message())
	default:
		ih.writeErrorResponse(w, http.StatusInternalServerError, defaultMessage, st.Message())
	}
}

// writeJSONResponse writes a JSON response
func (ih *InstanceHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		if ih.engine.logger != nil {
			ih.engine.logger.Errorf("Failed to encode JSON response: %v", err)
		}
	}
}

// writeErrorResponse writes an error response
func (ih *InstanceHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, details string) {
	response := ErrorResponse{
		Error:   message,
		Message: details,
		Status:  StatusError,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		if ih.engine.logger != nil {
			ih.engine.logger.Errorf("Failed to encode error response: %v", err)
		}
	}
}

// detectVendorFromHost detects the vendor based on the host URL.
// It checks for well-known cloud providers, DBaaS providers, and database-specific patterns.
// If no pattern matches, it returns "custom".
func detectVendorFromHost(host string) string {
	// Convert to lowercase for case-insensitive matching
	hostLower := strings.ToLower(host)

	// Remove protocol if present
	if strings.HasPrefix(hostLower, "http://") {
		hostLower = strings.TrimPrefix(hostLower, "http://")
	} else if strings.HasPrefix(hostLower, "https://") {
		hostLower = strings.TrimPrefix(hostLower, "https://")
	}

	// Remove port if present
	if colonIndex := strings.Index(hostLower, ":"); colonIndex != -1 {
		hostLower = hostLower[:colonIndex]
	}

	// Cloud providers and DBaaS vendors
	vendorPatterns := map[string][]string{
		"aws": {
			".amazonaws.com",
			".rds.amazonaws.com",
			".redshift.amazonaws.com",
			".aurora.amazonaws.com",
			".docdb.amazonaws.com",
			".neptune.amazonaws.com",
			".timestream.amazonaws.com",
			".keyspaces.amazonaws.com",
			".dynamodb.amazonaws.com",
			".elasticache.amazonaws.com",
		},
		"azure": {
			".database.windows.net",
			".azure.com",
			".cloudapp.azure.com",
			".azurewebsites.net",
			".azure-sql.com",
			".azure-mysql.com",
			".azure-postgresql.com",
			".azure-cosmos.com",
		},
		"gcp": {
			".googleapis.com",
			".cloudsql.google.com",
			".run.app",
			".appspot.com",
			".compute.google.com",
			".bigquery.googleapis.com",
			".firestore.googleapis.com",
			".datastore.googleapis.com",
		},
		"digitalocean": {
			".digitaloceanspaces.com",
			".ondigitalocean.app",
			".db.ondigitalocean.com",
		},
		"heroku": {
			".herokuapp.com",
			".herokudb.com",
			".herokupostgres.com",
			".heroku-redis.com",
		},
		"mongodb": {
			".mongodb.net",
			".mongodb.com",
			".m0.mongodb.net",
			".m2.mongodb.net",
			".m4.mongodb.net",
			".m5.mongodb.net",
			".m6.mongodb.net",
			".m10.mongodb.net",
		},
		"planetscale": {
			".psdb.cloud",
			".planetscale.com",
		},
		"supabase": {
			".supabase.co",
			".supabase.com",
		},
		"vercel": {
			".vercel.app",
			".vercel.com",
		},
		"netlify": {
			".netlify.app",
			".netlify.com",
		},
		"railway": {
			".railway.app",
		},
		"render": {
			".render.com",
			".onrender.com",
		},
		"fly": {
			".fly.dev",
			".fly.io",
		},
		"cloudflare": {
			".pages.dev",
			".workers.dev",
			".cloudflare.com",
		},
		"oracle": {
			".oraclecloud.com",
			".oracle.com",
		},
		"ibm": {
			".ibm.com",
			".bluemix.net",
			".cloud.ibm.com",
		},
		"alibaba": {
			".aliyuncs.com",
			".alibaba.com",
		},
		"tencent": {
			".tencentcloud.com",
			".qcloud.com",
		},
		"baidu": {
			".baidubce.com",
			".baidu.com",
		},
		"huawei": {
			".huaweicloud.com",
			".huawei.com",
		},
		"linode": {
			".linode.com",
			".linodeapp.com",
		},
		"vultr": {
			".vultr.com",
		},
		"scaleway": {
			".scaleway.com",
			".scw.cloud",
		},
		"ovh": {
			".ovh.com",
			".ovhcloud.com",
		},
		"rackspace": {
			".rackspace.com",
			".rackspacecloud.com",
		},
		"joyent": {
			".joyent.com",
		},
		"exoscale": {
			".exoscale.com",
		},
		"upcloud": {
			".upcloud.com",
		},
		"hetzner": {
			".hetzner.com",
			".hetzner.cloud",
		},
		"ionos": {
			".ionos.com",
			".1and1.com",
		},
		"godaddy": {
			".godaddy.com",
		},
		"namecheap": {
			".namecheap.com",
		},
		"hostgator": {
			".hostgator.com",
		},
		"bluehost": {
			".bluehost.com",
		},
		"hostinger": {
			".hostinger.com",
		},
		"a2hosting": {
			".a2hosting.com",
		},
		"inmotion": {
			".inmotionhosting.com",
		},
		"dreamhost": {
			".dreamhost.com",
		},
		"siteground": {
			".siteground.com",
		},
		"wpengine": {
			".wpengine.com",
		},
		"kinsta": {
			".kinsta.com",
		},
		"pantheon": {
			".pantheon.io",
		},
		"acquia": {
			".acquia.com",
		},
		"platform": {
			".platform.sh",
		},
		"drupal": {
			".drupal.com",
		},
		"wordpress": {
			".wordpress.com",
		},
		"shopify": {
			".shopify.com",
			".myshopify.com",
		},
		"magento": {
			".magento.com",
		},
		"woocommerce": {
			".woocommerce.com",
		},
		"bigcommerce": {
			".bigcommerce.com",
		},
		"squarespace": {
			".squarespace.com",
		},
		"wix": {
			".wix.com",
			".wixsite.com",
		},
		"weebly": {
			".weebly.com",
		},
		"webflow": {
			".webflow.com",
		},
		"bubble": {
			".bubble.io",
		},
		"airtable": {
			".airtable.com",
		},
		"notion": {
			".notion.so",
		},
		"clickup": {
			".clickup.com",
		},
		"asana": {
			".asana.com",
		},
		"trello": {
			".trello.com",
		},
		"monday": {
			".monday.com",
		},
		"slack": {
			".slack.com",
		},
		"discord": {
			".discord.com",
		},
		"teams": {
			".teams.microsoft.com",
		},
		"zoom": {
			".zoom.us",
		},
		"google": {
			".google.com",
			".gmail.com",
			".googleusercontent.com",
		},
		"microsoft": {
			".microsoft.com",
			".office.com",
			".outlook.com",
			".live.com",
		},
		"apple": {
			".apple.com",
			".icloud.com",
		},
		"facebook": {
			".facebook.com",
			".fb.com",
		},
		"twitter": {
			".twitter.com",
			".x.com",
		},
		"linkedin": {
			".linkedin.com",
		},
		"github": {
			".github.com",
			".githubusercontent.com",
		},
		"gitlab": {
			".gitlab.com",
		},
		"bitbucket": {
			".bitbucket.org",
		},
		"atlassian": {
			".atlassian.com",
			".jira.com",
			".confluence.com",
		},
		"salesforce": {
			".salesforce.com",
			".force.com",
		},
		"hubspot": {
			".hubspot.com",
		},
		"mailchimp": {
			".mailchimp.com",
		},
		"sendgrid": {
			".sendgrid.com",
		},
		"twilio": {
			".twilio.com",
		},
		"stripe": {
			".stripe.com",
		},
		"paypal": {
			".paypal.com",
		},
		"square": {
			".square.com",
		},
	}

	// Check for vendor patterns
	for vendor, patterns := range vendorPatterns {
		for _, pattern := range patterns {
			if strings.Contains(hostLower, pattern) {
				return vendor
			}
		}
	}

	// Check for common database-specific patterns
	if strings.Contains(hostLower, "postgres") || strings.Contains(hostLower, "postgresql") {
		return "postgresql"
	}
	if strings.Contains(hostLower, "mysql") {
		return "mysql"
	}
	if strings.Contains(hostLower, "mariadb") {
		return "mariadb"
	}
	if strings.Contains(hostLower, "sqlserver") || strings.Contains(hostLower, "mssql") {
		return "sqlserver"
	}
	if strings.Contains(hostLower, "oracle") {
		return "oracle"
	}
	if strings.Contains(hostLower, "db2") {
		return "db2"
	}
	if strings.Contains(hostLower, "mongodb") {
		return "mongodb"
	}
	if strings.Contains(hostLower, "redis") {
		return "redis"
	}
	if strings.Contains(hostLower, "cassandra") {
		return "cassandra"
	}
	if strings.Contains(hostLower, "elasticsearch") {
		return "elasticsearch"
	}
	if strings.Contains(hostLower, "neo4j") {
		return "neo4j"
	}
	if strings.Contains(hostLower, "cockroach") {
		return "cockroachdb"
	}
	if strings.Contains(hostLower, "clickhouse") {
		return "clickhouse"
	}
	if strings.Contains(hostLower, "snowflake") {
		return "snowflake"
	}
	if strings.Contains(hostLower, "bigquery") {
		return "bigquery"
	}
	if strings.Contains(hostLower, "dynamodb") {
		return "dynamodb"
	}
	if strings.Contains(hostLower, "cosmos") {
		return "cosmosdb"
	}
	if strings.Contains(hostLower, "firestore") {
		return "firestore"
	}
	if strings.Contains(hostLower, "datastore") {
		return "datastore"
	}
	if strings.Contains(hostLower, "timestream") {
		return "timestream"
	}
	if strings.Contains(hostLower, "keyspaces") {
		return "keyspaces"
	}
	if strings.Contains(hostLower, "neptune") {
		return "neptune"
	}
	if strings.Contains(hostLower, "docdb") {
		return "docdb"
	}
	if strings.Contains(hostLower, "aurora") {
		return "aurora"
	}
	if strings.Contains(hostLower, "redshift") {
		return "redshift"
	}
	if strings.Contains(hostLower, "elasticache") {
		return "elasticache"
	}

	// If no pattern matches, return "custom"
	return "custom"
}
