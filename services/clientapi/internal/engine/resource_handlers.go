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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ResourceHandlers contains the resource endpoint handlers
type ResourceHandlers struct {
	engine *Engine
}

// NewResourceHandlers creates a new instance of ResourceHandlers
func NewResourceHandlers(engine *Engine) *ResourceHandlers {
	return &ResourceHandlers{
		engine: engine,
	}
}

// ResourceContainer represents a resource container for REST responses
type ResourceContainer struct {
	ContainerID       string                 `json:"container_id"`
	TenantID          string                 `json:"tenant_id"`
	WorkspaceID       string                 `json:"workspace_id"`
	ResourceURI       string                 `json:"resource_uri"`
	Protocol          string                 `json:"protocol"`
	Scope             string                 `json:"scope"`
	ObjectType        string                 `json:"object_type"`
	ObjectName        string                 `json:"object_name"`
	DatabaseID        string                 `json:"database_id,omitempty"`
	InstanceID        string                 `json:"instance_id,omitempty"`
	IntegrationID     string                 `json:"integration_id,omitempty"`
	McpServerID       string                 `json:"mcpserver_id,omitempty"`
	ConnectedToNodeID string                 `json:"connected_to_node_id,omitempty"`
	OwnerID           string                 `json:"owner_id"`
	Status            string                 `json:"status"`
	StatusMessage     string                 `json:"status_message,omitempty"`
	LastSeen          string                 `json:"last_seen,omitempty"`
	Online            bool                   `json:"online"`
	ContainerMetadata map[string]interface{} `json:"container_metadata,omitempty"`
	EnrichedMetadata  map[string]interface{} `json:"enriched_metadata,omitempty"`
	DatabaseType      string                 `json:"database_type,omitempty"`
	Vendor            string                 `json:"vendor,omitempty"`
	ItemCount         int32                  `json:"item_count,omitempty"`
	SizeBytes         int64                  `json:"size_bytes,omitempty"`
	Created           string                 `json:"created,omitempty"`
	Updated           string                 `json:"updated,omitempty"`
}

// ListResourceContainersResponse represents the response for listing resource containers
type ListResourceContainersResponse struct {
	Containers []ResourceContainer `json:"containers"`
	TotalCount int32               `json:"total_count,omitempty"`
}

// ShowResourceContainerResponse represents the response for showing a resource container
type ShowResourceContainerResponse struct {
	Container ResourceContainer `json:"container"`
}

// ListResourceItemsResponse represents the response for listing resource items
type ListResourceItemsResponse struct {
	Items      []ResourceItem `json:"items"`
	TotalCount int32          `json:"total_count,omitempty"`
}

// ListResourceContainers handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/resources/containers
func (rh *ResourceHandlers) ListResourceContainers(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		rh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse query parameters for filtering
	queryParams := r.URL.Query()
	filter := &corev1.ResourceContainerFilter{
		WorkspaceId:  workspaceName,
		Protocol:     queryParams.Get("protocol"),
		Scope:        queryParams.Get("scope"),
		ObjectType:   queryParams.Get("object_type"),
		DatabaseType: queryParams.Get("database_type"),
		Status:       queryParams.Get("status"),
	}

	// Parse online filter
	if onlineStr := queryParams.Get("online"); onlineStr != "" {
		if online, err := strconv.ParseBool(onlineStr); err == nil {
			filter.Online = online
		}
	}

	// Parse limit and offset
	if limitStr := queryParams.Get("limit"); limitStr != "" {
		if limit, err := strconv.ParseInt(limitStr, 10, 32); err == nil {
			filter.Limit = int32(limit)
		}
	}
	if offsetStr := queryParams.Get("offset"); offsetStr != "" {
		if offset, err := strconv.ParseInt(offsetStr, 10, 32); err == nil {
			filter.Offset = int32(offset)
		}
	}

	// Log request
	if rh.engine.logger != nil {
		rh.engine.logger.Infof("List resource containers request for workspace: %s, tenant: %s, user: %s", workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListResourceContainersRequest{
		Filter: filter,
	}

	grpcResp, err := rh.engine.resourceClient.ListResourceContainers(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to list resource containers")
		return
	}

	// Convert gRPC response to REST response
	containers := make([]ResourceContainer, len(grpcResp.Containers))
	for i, container := range grpcResp.Containers {
		containers[i] = rh.protoToResourceContainer(container)
	}

	response := ListResourceContainersResponse{
		Containers: containers,
		TotalCount: grpcResp.TotalCount,
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully listed %d resource containers for workspace: %s", len(containers), workspaceName)
	}

	rh.writeJSONResponse(w, http.StatusOK, response)
}

// ShowResourceContainer handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/resources/containers/{container_id}
func (rh *ResourceHandlers) ShowResourceContainer(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	containerID := vars["container_id"]

	if tenantURL == "" || workspaceName == "" || containerID == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and container_id are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		rh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Show resource container request: %s, workspace: %s, tenant: %s, user: %s", containerID, workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.GetResourceContainerRequest{
		ContainerId: containerID,
	}

	grpcResp, err := rh.engine.resourceClient.GetResourceContainer(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to get resource container")
		return
	}

	container := rh.protoToResourceContainer(grpcResp.Container)

	response := ShowResourceContainerResponse{
		Container: container,
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully showed resource container: %s for workspace: %s", containerID, workspaceName)
	}

	rh.writeJSONResponse(w, http.StatusOK, response)
}

// ListResourceItems handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/resources/items
func (rh *ResourceHandlers) ListResourceItems(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		rh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse query parameters for filtering
	queryParams := r.URL.Query()
	filter := &corev1.ResourceItemFilter{
		WorkspaceId:     workspaceName,
		ContainerId:     queryParams.Get("container_id"),
		ItemType:        queryParams.Get("item_type"),
		DataType:        queryParams.Get("data_type"),
		UnifiedDataType: queryParams.Get("unified_data_type"),
	}

	// Parse boolean filters
	if isPrimaryKeyStr := queryParams.Get("is_primary_key"); isPrimaryKeyStr != "" {
		if isPrimaryKey, err := strconv.ParseBool(isPrimaryKeyStr); err == nil {
			filter.IsPrimaryKey = isPrimaryKey
		}
	}
	if isUniqueStr := queryParams.Get("is_unique"); isUniqueStr != "" {
		if isUnique, err := strconv.ParseBool(isUniqueStr); err == nil {
			filter.IsUnique = isUnique
		}
	}
	if isIndexedStr := queryParams.Get("is_indexed"); isIndexedStr != "" {
		if isIndexed, err := strconv.ParseBool(isIndexedStr); err == nil {
			filter.IsIndexed = isIndexed
		}
	}
	if isPrivilegedStr := queryParams.Get("is_privileged"); isPrivilegedStr != "" {
		if isPrivileged, err := strconv.ParseBool(isPrivilegedStr); err == nil {
			filter.IsPrivileged = isPrivileged
		}
	}
	if onlineStr := queryParams.Get("online"); onlineStr != "" {
		if online, err := strconv.ParseBool(onlineStr); err == nil {
			filter.Online = online
		}
	}

	// Parse limit and offset
	if limitStr := queryParams.Get("limit"); limitStr != "" {
		if limit, err := strconv.ParseInt(limitStr, 10, 32); err == nil {
			filter.Limit = int32(limit)
		}
	}
	if offsetStr := queryParams.Get("offset"); offsetStr != "" {
		if offset, err := strconv.ParseInt(offsetStr, 10, 32); err == nil {
			filter.Offset = int32(offset)
		}
	}

	// Log request
	if rh.engine.logger != nil {
		rh.engine.logger.Infof("List resource items request for workspace: %s, tenant: %s, user: %s", workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListResourceItemsRequest{
		Filter: filter,
	}

	grpcResp, err := rh.engine.resourceClient.ListResourceItems(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to list resource items")
		return
	}

	// Convert gRPC response to REST response
	items := make([]ResourceItem, len(grpcResp.Items))
	for i, item := range grpcResp.Items {
		items[i] = rh.protoToResourceItem(item)
	}

	response := ListResourceItemsResponse{
		Items:      items,
		TotalCount: grpcResp.TotalCount,
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully listed %d resource items for workspace: %s", len(items), workspaceName)
	}

	rh.writeJSONResponse(w, http.StatusOK, response)
}

// ListResourceItemsForContainer handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/resources/containers/{container_id}/items
func (rh *ResourceHandlers) ListResourceItemsForContainer(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	containerID := vars["container_id"]

	if tenantURL == "" || workspaceName == "" || containerID == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and container_id are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		rh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse query parameters for filtering
	queryParams := r.URL.Query()
	filter := &corev1.ResourceItemFilter{
		WorkspaceId: workspaceName,
		ContainerId: containerID,
	}

	// Parse limit and offset
	if limitStr := queryParams.Get("limit"); limitStr != "" {
		if limit, err := strconv.ParseInt(limitStr, 10, 32); err == nil {
			filter.Limit = int32(limit)
		}
	}
	if offsetStr := queryParams.Get("offset"); offsetStr != "" {
		if offset, err := strconv.ParseInt(offsetStr, 10, 32); err == nil {
			filter.Offset = int32(offset)
		}
	}

	// Log request
	if rh.engine.logger != nil {
		rh.engine.logger.Infof("List resource items for container: %s, workspace: %s, tenant: %s, user: %s", containerID, workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListResourceItemsRequest{
		Filter: filter,
	}

	grpcResp, err := rh.engine.resourceClient.ListResourceItems(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to list resource items for container")
		return
	}

	// Convert gRPC response to REST response
	items := make([]ResourceItem, len(grpcResp.Items))
	for i, item := range grpcResp.Items {
		items[i] = rh.protoToResourceItem(item)
	}

	response := ListResourceItemsResponse{
		Items:      items,
		TotalCount: grpcResp.TotalCount,
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully listed %d resource items for container: %s", len(items), containerID)
	}

	rh.writeJSONResponse(w, http.StatusOK, response)
}

// Helper functions

func (rh *ResourceHandlers) protoToResourceContainer(proto *corev1.ResourceContainer) ResourceContainer {
	container := ResourceContainer{
		ContainerID:       proto.ContainerId,
		TenantID:          proto.TenantId,
		WorkspaceID:       proto.WorkspaceId,
		ResourceURI:       proto.ResourceUri,
		Protocol:          proto.Protocol,
		Scope:             proto.Scope,
		ObjectType:        proto.ObjectType,
		ObjectName:        proto.ObjectName,
		DatabaseID:        proto.DatabaseId,
		InstanceID:        proto.InstanceId,
		IntegrationID:     proto.IntegrationId,
		McpServerID:       proto.McpserverId,
		ConnectedToNodeID: proto.ConnectedToNodeId,
		OwnerID:           proto.OwnerId,
		Status:            proto.Status,
		StatusMessage:     proto.StatusMessage,
		LastSeen:          proto.LastSeen,
		Online:            proto.Online,
		DatabaseType:      proto.DatabaseType,
		Vendor:            proto.Vendor,
		ItemCount:         proto.ItemCount,
		SizeBytes:         proto.SizeBytes,
		Created:           proto.Created,
		Updated:           proto.Updated,
	}

	// Convert metadata if present
	if proto.ContainerMetadata != nil {
		container.ContainerMetadata = proto.ContainerMetadata.AsMap()
	}
	if proto.EnrichedMetadata != nil {
		container.EnrichedMetadata = proto.EnrichedMetadata.AsMap()
	}

	return container
}

func (rh *ResourceHandlers) protoToResourceItem(proto *corev1.ResourceItem) ResourceItem {
	item := ResourceItem{
		ItemID:          proto.ItemId,
		ContainerID:     proto.ContainerId,
		TenantID:        proto.TenantId,
		WorkspaceID:     proto.WorkspaceId,
		ResourceURI:     proto.ResourceUri,
		Protocol:        proto.Protocol,
		Scope:           proto.Scope,
		ItemType:        proto.ItemType,
		ItemName:        proto.ItemName,
		ItemDisplayName: proto.ItemDisplayName,
		ItemPath:        proto.ItemPath,
		DataType:        proto.DataType,
		IsNullable:      proto.IsNullable,
		IsPrimaryKey:    proto.IsPrimaryKey,
		IsUnique:        proto.IsUnique,
		IsIndexed:       proto.IsIndexed,
		IsRequired:      proto.IsRequired,
		IsArray:         proto.IsArray,
		ArrayDimensions: int(proto.ArrayDimensions),
		IsPrivileged:    proto.IsPrivileged,
		Created:         proto.Created,
		Updated:         proto.Updated,
	}

	// Handle optional fields
	if proto.UnifiedDataType != "" {
		item.UnifiedDataType = &proto.UnifiedDataType
	}
	if proto.DefaultValue != "" {
		item.DefaultValue = &proto.DefaultValue
	}
	if proto.MaxLength > 0 {
		maxLen := int(proto.MaxLength)
		item.MaxLength = &maxLen
	}
	if proto.Precision > 0 {
		precision := int(proto.Precision)
		item.Precision = &precision
	}
	if proto.Scale > 0 {
		scale := int(proto.Scale)
		item.Scale = &scale
	}
	if proto.PrivilegedClassification != "" {
		item.PrivilegedClassification = &proto.PrivilegedClassification
	}
	if proto.DetectionConfidence > 0 {
		item.DetectionConfidence = &proto.DetectionConfidence
	}
	if proto.DetectionMethod != "" {
		item.DetectionMethod = &proto.DetectionMethod
	}

	return item
}

// ModifyResourceItem handles PATCH /{tenant_url}/api/v1/workspaces/{workspace_name}/resources/items/{item_id}
func (rh *ResourceHandlers) ModifyResourceItem(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	itemID := vars["item_id"]

	if tenantURL == "" || workspaceName == "" || itemID == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and item_id are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		rh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var reqBody struct {
		ItemDisplayName *string `json:"item_display_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		rh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Log request
	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Modify resource item request: %s, workspace: %s, tenant: %s, user: %s", itemID, workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ModifyResourceItemRequest{
		ItemId: itemID,
	}

	if reqBody.ItemDisplayName != nil {
		grpcReq.ItemDisplayName = reqBody.ItemDisplayName
	}

	grpcResp, err := rh.engine.resourceClient.ModifyResourceItem(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to modify resource item")
		return
	}

	item := rh.protoToResourceItem(grpcResp.Item)

	response := map[string]interface{}{
		"success": true,
		"message": grpcResp.Message,
		"item":    item,
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully modified resource item: %s for workspace: %s", itemID, workspaceName)
	}

	rh.writeJSONResponse(w, http.StatusOK, response)
}

// writeErrorResponse writes an error response
func (rh *ResourceHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message string, detail string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	response := map[string]interface{}{
		"error":   message,
		"message": detail,
		"success": false,
	}
	json.NewEncoder(w).Encode(response)
}

// writeJSONResponse writes a JSON response
func (rh *ResourceHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

// handleGRPCError handles gRPC errors and writes appropriate HTTP responses
func (rh *ResourceHandlers) handleGRPCError(w http.ResponseWriter, err error, message string) {
	if rh.engine.logger != nil {
		rh.engine.logger.Errorf("%s: %v", message, err)
	}

	st, ok := status.FromError(err)
	if !ok {
		rh.writeErrorResponse(w, http.StatusInternalServerError, message, err.Error())
		return
	}

	switch st.Code() {
	case codes.NotFound:
		rh.writeErrorResponse(w, http.StatusNotFound, message, st.Message())
	case codes.InvalidArgument:
		rh.writeErrorResponse(w, http.StatusBadRequest, message, st.Message())
	case codes.AlreadyExists:
		rh.writeErrorResponse(w, http.StatusConflict, message, st.Message())
	case codes.PermissionDenied:
		rh.writeErrorResponse(w, http.StatusForbidden, message, st.Message())
	case codes.Unauthenticated:
		rh.writeErrorResponse(w, http.StatusUnauthorized, message, st.Message())
	default:
		rh.writeErrorResponse(w, http.StatusInternalServerError, message, st.Message())
	}
}
