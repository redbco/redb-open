package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// DataProductHandlers contains the data product endpoint handlers
type DataProductHandlers struct {
	engine *Engine
}

// NewDataProductHandlers creates a new instance of DataProductHandlers
func NewDataProductHandlers(engine *Engine) *DataProductHandlers {
	return &DataProductHandlers{
		engine: engine,
	}
}

// DataProduct represents a virtual data product (collection of resource items)
type DataProduct struct {
	ProductID          string                 `json:"product_id"`
	TenantID           string                 `json:"tenant_id"`
	WorkspaceID        string                 `json:"workspace_id"`
	ProductName        string                 `json:"product_name"`
	ProductDescription string                 `json:"product_description"`
	ResourceItems      []ResourceItem         `json:"resource_items"`
	Metadata           map[string]interface{} `json:"metadata,omitempty"`
	OwnerID            string                 `json:"owner_id"`
	Status             string                 `json:"status"`
	Created            string                 `json:"created,omitempty"`
	Updated            string                 `json:"updated,omitempty"`
}

// ListDataProductsResponse represents the response for listing data products
type ListDataProductsResponse struct {
	DataProducts []DataProduct `json:"dataproducts"`
}

// ShowDataProductResponse represents the response for showing a data product
type ShowDataProductResponse struct {
	DataProduct DataProduct `json:"dataproduct"`
}

// CreateDataProductRequest represents the request to create a data product
type CreateDataProductRequest struct {
	ProductName        string                 `json:"product_name"`
	ProductDescription string                 `json:"product_description"`
	ResourceItemIDs    []string               `json:"resource_item_ids"`
	Metadata           map[string]interface{} `json:"metadata,omitempty"`
}

// CreateDataProductResponse represents the response for creating a data product
type CreateDataProductResponse struct {
	Success     bool        `json:"success"`
	Message     string      `json:"message"`
	DataProduct DataProduct `json:"dataproduct"`
}

// ModifyDataProductRequest represents the request to modify a data product
type ModifyDataProductRequest struct {
	ProductDescription *string                `json:"product_description,omitempty"`
	ResourceItemIDs    []string               `json:"resource_item_ids,omitempty"`
	Metadata           map[string]interface{} `json:"metadata,omitempty"`
	Status             *string                `json:"status,omitempty"`
}

// ListDataProducts handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/dataproducts
func (dph *DataProductHandlers) ListDataProducts(w http.ResponseWriter, r *http.Request) {
	dph.engine.TrackOperation()
	defer dph.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" {
		dph.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		dph.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dph.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if dph.engine.logger != nil {
		dph.engine.logger.Infof("List data products request for workspace: %s, tenant: %s, user: %s", workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListDataProductsRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
	}

	grpcResp, err := dph.engine.dataProductClient.ListDataProducts(ctx, grpcReq)
	if err != nil {
		dph.handleGRPCError(w, err, "Failed to list data products")
		return
	}

	// Convert gRPC response to REST response
	dataProducts := make([]DataProduct, len(grpcResp.DataProducts))
	for i, product := range grpcResp.DataProducts {
		dataProducts[i] = dph.protoToDataProduct(product)
	}

	response := ListDataProductsResponse{
		DataProducts: dataProducts,
	}

	if dph.engine.logger != nil {
		dph.engine.logger.Infof("Successfully listed %d data products for workspace: %s", len(dataProducts), workspaceName)
	}

	dph.writeJSONResponse(w, http.StatusOK, response)
}

// ShowDataProduct handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/dataproducts/{product_name}
func (dph *DataProductHandlers) ShowDataProduct(w http.ResponseWriter, r *http.Request) {
	dph.engine.TrackOperation()
	defer dph.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	productName := vars["product_name"]

	if tenantURL == "" || workspaceName == "" || productName == "" {
		dph.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and product_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dph.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if dph.engine.logger != nil {
		dph.engine.logger.Infof("Show data product request: %s, workspace: %s, tenant: %s, user: %s", productName, workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.GetDataProductRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		ProductName:   productName,
	}

	grpcResp, err := dph.engine.dataProductClient.GetDataProduct(ctx, grpcReq)
	if err != nil {
		dph.handleGRPCError(w, err, "Failed to get data product")
		return
	}

	dataProduct := dph.protoToDataProduct(grpcResp.DataProduct)

	response := ShowDataProductResponse{
		DataProduct: dataProduct,
	}

	if dph.engine.logger != nil {
		dph.engine.logger.Infof("Successfully showed data product: %s for workspace: %s", productName, workspaceName)
	}

	dph.writeJSONResponse(w, http.StatusOK, response)
}

// CreateDataProduct handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/dataproducts
func (dph *DataProductHandlers) CreateDataProduct(w http.ResponseWriter, r *http.Request) {
	dph.engine.TrackOperation()
	defer dph.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" || workspaceName == "" {
		dph.writeErrorResponse(w, http.StatusBadRequest, "tenant_url and workspace_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dph.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var reqBody CreateDataProductRequest
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		dph.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Validate required fields
	if reqBody.ProductName == "" {
		dph.writeErrorResponse(w, http.StatusBadRequest, "product_name is required", "")
		return
	}

	if len(reqBody.ResourceItemIDs) == 0 {
		dph.writeErrorResponse(w, http.StatusBadRequest, "resource_item_ids is required and must not be empty", "")
		return
	}

	// Log request
	if dph.engine.logger != nil {
		dph.engine.logger.Infof("Create data product request: %s, workspace: %s, tenant: %s, user: %s", reqBody.ProductName, workspaceName, profile.TenantId, profile.UserId)
	}

	// Convert metadata to structpb
	var metadataStruct *structpb.Struct
	if reqBody.Metadata != nil {
		var err error
		metadataStruct, err = structpb.NewStruct(reqBody.Metadata)
		if err != nil {
			dph.writeErrorResponse(w, http.StatusBadRequest, "Invalid metadata", err.Error())
			return
		}
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.CreateDataProductRequest{
		TenantId:           profile.TenantId,
		WorkspaceName:      workspaceName,
		ProductName:        reqBody.ProductName,
		ProductDescription: reqBody.ProductDescription,
		ResourceItemIds:    reqBody.ResourceItemIDs,
		Metadata:           metadataStruct,
		OwnerId:            profile.UserId,
	}

	grpcResp, err := dph.engine.dataProductClient.CreateDataProduct(ctx, grpcReq)
	if err != nil {
		dph.handleGRPCError(w, err, "Failed to create data product")
		return
	}

	dataProduct := dph.protoToDataProduct(grpcResp.DataProduct)

	response := CreateDataProductResponse{
		Success:     true,
		Message:     grpcResp.Message,
		DataProduct: dataProduct,
	}

	if dph.engine.logger != nil {
		dph.engine.logger.Infof("Successfully created data product: %s for workspace: %s", reqBody.ProductName, workspaceName)
	}

	dph.writeJSONResponse(w, http.StatusCreated, response)
}

// ModifyDataProduct handles PUT /{tenant_url}/api/v1/workspaces/{workspace_name}/dataproducts/{product_name}
func (dph *DataProductHandlers) ModifyDataProduct(w http.ResponseWriter, r *http.Request) {
	dph.engine.TrackOperation()
	defer dph.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	productName := vars["product_name"]

	if tenantURL == "" || workspaceName == "" || productName == "" {
		dph.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and product_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dph.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var reqBody ModifyDataProductRequest
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		dph.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Log request
	if dph.engine.logger != nil {
		dph.engine.logger.Infof("Modify data product request: %s, workspace: %s, tenant: %s, user: %s", productName, workspaceName, profile.TenantId, profile.UserId)
	}

	// Convert metadata to structpb
	var metadataStruct *structpb.Struct
	if reqBody.Metadata != nil {
		var err error
		metadataStruct, err = structpb.NewStruct(reqBody.Metadata)
		if err != nil {
			dph.writeErrorResponse(w, http.StatusBadRequest, "Invalid metadata", err.Error())
			return
		}
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ModifyDataProductRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		ProductName:   productName,
		Metadata:      metadataStruct,
	}

	if reqBody.ProductDescription != nil {
		grpcReq.ProductDescription = *reqBody.ProductDescription
	}
	if reqBody.ResourceItemIDs != nil {
		grpcReq.ResourceItemIds = reqBody.ResourceItemIDs
	}
	if reqBody.Status != nil {
		grpcReq.Status = *reqBody.Status
	}

	grpcResp, err := dph.engine.dataProductClient.ModifyDataProduct(ctx, grpcReq)
	if err != nil {
		dph.handleGRPCError(w, err, "Failed to modify data product")
		return
	}

	dataProduct := dph.protoToDataProduct(grpcResp.DataProduct)

	response := map[string]interface{}{
		"success":     true,
		"message":     grpcResp.Message,
		"dataproduct": dataProduct,
	}

	if dph.engine.logger != nil {
		dph.engine.logger.Infof("Successfully modified data product: %s for workspace: %s", productName, workspaceName)
	}

	dph.writeJSONResponse(w, http.StatusOK, response)
}

// DeleteDataProduct handles DELETE /{tenant_url}/api/v1/workspaces/{workspace_name}/dataproducts/{product_name}
func (dph *DataProductHandlers) DeleteDataProduct(w http.ResponseWriter, r *http.Request) {
	dph.engine.TrackOperation()
	defer dph.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	productName := vars["product_name"]

	if tenantURL == "" || workspaceName == "" || productName == "" {
		dph.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and product_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		dph.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if dph.engine.logger != nil {
		dph.engine.logger.Infof("Delete data product request: %s, workspace: %s, tenant: %s, user: %s", productName, workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DeleteDataProductRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		ProductName:   productName,
	}

	grpcResp, err := dph.engine.dataProductClient.DeleteDataProduct(ctx, grpcReq)
	if err != nil {
		dph.handleGRPCError(w, err, "Failed to delete data product")
		return
	}

	response := map[string]interface{}{
		"success": grpcResp.Success,
		"message": grpcResp.Message,
	}

	if dph.engine.logger != nil {
		dph.engine.logger.Infof("Successfully deleted data product: %s for workspace: %s", productName, workspaceName)
	}

	dph.writeJSONResponse(w, http.StatusOK, response)
}

// Helper functions

func (dph *DataProductHandlers) protoToDataProduct(proto *corev1.DataProduct) DataProduct {
	product := DataProduct{
		ProductID:          proto.ProductId,
		TenantID:           proto.TenantId,
		WorkspaceID:        proto.WorkspaceId,
		ProductName:        proto.ProductName,
		ProductDescription: proto.ProductDescription,
		OwnerID:            proto.OwnerId,
		Status:             proto.Status,
		Created:            proto.Created,
		Updated:            proto.Updated,
	}

	// Convert resource items
	if len(proto.ResourceItems) > 0 {
		product.ResourceItems = make([]ResourceItem, len(proto.ResourceItems))
		rh := &ResourceHandlers{engine: dph.engine}
		for i, item := range proto.ResourceItems {
			product.ResourceItems[i] = rh.protoToResourceItem(item)
		}
	} else {
		product.ResourceItems = []ResourceItem{}
	}

	// Convert metadata
	if proto.Metadata != nil {
		product.Metadata = proto.Metadata.AsMap()
	}

	return product
}

// writeErrorResponse writes an error response
func (dph *DataProductHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message string, detail string) {
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
func (dph *DataProductHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

// handleGRPCError handles gRPC errors and writes appropriate HTTP responses
func (dph *DataProductHandlers) handleGRPCError(w http.ResponseWriter, err error, message string) {
	if dph.engine.logger != nil {
		dph.engine.logger.Errorf("%s: %v", message, err)
	}

	st, ok := status.FromError(err)
	if !ok {
		dph.writeErrorResponse(w, http.StatusInternalServerError, message, err.Error())
		return
	}

	switch st.Code() {
	case codes.NotFound:
		dph.writeErrorResponse(w, http.StatusNotFound, message, st.Message())
	case codes.InvalidArgument:
		dph.writeErrorResponse(w, http.StatusBadRequest, message, st.Message())
	case codes.AlreadyExists:
		dph.writeErrorResponse(w, http.StatusConflict, message, st.Message())
	case codes.PermissionDenied:
		dph.writeErrorResponse(w, http.StatusForbidden, message, st.Message())
	case codes.Unauthenticated:
		dph.writeErrorResponse(w, http.StatusUnauthorized, message, st.Message())
	default:
		dph.writeErrorResponse(w, http.StatusInternalServerError, message, st.Message())
	}
}
