package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TenantHandlers contains the tenant endpoint handlers
type TenantHandlers struct {
	engine *Engine
}

// NewTenantHandlers creates a new instance of TenantHandlers
func NewTenantHandlers(engine *Engine) *TenantHandlers {
	return &TenantHandlers{
		engine: engine,
	}
}

// ListTenants handles GET /api/v1/tenants
func (th *TenantHandlers) ListTenants(w http.ResponseWriter, r *http.Request) {
	th.engine.TrackOperation()
	defer th.engine.UntrackOperation()

	// Log incoming request details
	if th.engine.logger != nil {
		th.engine.logger.Infof("List tenants request received")
		th.engine.logger.Debugf("Request from: %s", r.RemoteAddr)
		th.engine.logger.Debugf("User-Agent: %s", r.Header.Get("User-Agent"))
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Check if tenant client is available
	if th.engine.tenantClient == nil {
		if th.engine.logger != nil {
			th.engine.logger.Errorf("Tenant client is nil - gRPC connection may have failed during startup")
		}
		th.writeErrorResponse(w, http.StatusInternalServerError, "Tenant service unavailable", "")
		return
	}

	// Call core service gRPC
	grpcReq := &corev1.ListTenantsRequest{}

	// Log gRPC call attempt
	if th.engine.logger != nil {
		th.engine.logger.Debugf("Making gRPC ListTenants call to core service")
	}

	grpcResp, err := th.engine.tenantClient.ListTenants(ctx, grpcReq)
	if err != nil {
		th.handleGRPCError(w, err, "List tenants failed")
		return
	}

	// Log successful gRPC response
	if th.engine.logger != nil {
		th.engine.logger.Infof("gRPC ListTenants call successful")
		th.engine.logger.Debugf("gRPC response: %d tenants found", len(grpcResp.Tenants))
	}

	// Convert gRPC response to REST response
	tenants := make([]Tenant, len(grpcResp.Tenants))
	for i, tenant := range grpcResp.Tenants {
		tenants[i] = Tenant{
			TenantID:          tenant.TenantId,
			TenantName:        tenant.TenantName,
			TenantDescription: tenant.TenantDescription,
			TenantURL:         tenant.TenantUrl,
		}
	}

	response := ListTenantsResponse{
		Tenants: tenants,
	}

	if th.engine.logger != nil {
		th.engine.logger.Infof("List tenants successful: %d tenants returned", len(tenants))
	}

	th.writeJSONResponse(w, http.StatusOK, response)
}

// ShowTenant handles GET /api/v1/tenants/{tenant_id}
func (th *TenantHandlers) ShowTenant(w http.ResponseWriter, r *http.Request) {
	th.engine.TrackOperation()
	defer th.engine.UntrackOperation()

	// Extract tenant_id from path
	vars := mux.Vars(r)
	tenantID := vars["tenant_id"]
	if tenantID == "" {
		th.writeErrorResponse(w, http.StatusBadRequest, "tenant_id is required", "")
		return
	}

	// Log incoming request details
	if th.engine.logger != nil {
		th.engine.logger.Infof("Show tenant request received for tenant: %s", tenantID)
		th.engine.logger.Debugf("Request from: %s", r.RemoteAddr)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Check if tenant client is available
	if th.engine.tenantClient == nil {
		if th.engine.logger != nil {
			th.engine.logger.Errorf("Tenant client is nil - gRPC connection may have failed during startup")
		}
		th.writeErrorResponse(w, http.StatusInternalServerError, "Tenant service unavailable", "")
		return
	}

	// Call core service gRPC
	grpcReq := &corev1.ShowTenantRequest{
		TenantId: tenantID,
	}

	// Log gRPC call attempt
	if th.engine.logger != nil {
		th.engine.logger.Debugf("Making gRPC ShowTenant call to core service")
		th.engine.logger.Debugf("gRPC request: tenant_id=%s", grpcReq.TenantId)
	}

	grpcResp, err := th.engine.tenantClient.ShowTenant(ctx, grpcReq)
	if err != nil {
		th.handleGRPCError(w, err, "Show tenant failed")
		return
	}

	// Log successful gRPC response
	if th.engine.logger != nil {
		th.engine.logger.Infof("gRPC ShowTenant call successful for tenant: %s", tenantID)
	}

	// Convert gRPC response to REST response
	response := ShowTenantResponse{
		Tenant: Tenant{
			TenantID:          grpcResp.Tenant.TenantId,
			TenantName:        grpcResp.Tenant.TenantName,
			TenantDescription: grpcResp.Tenant.TenantDescription,
			TenantURL:         grpcResp.Tenant.TenantUrl,
		},
	}

	if th.engine.logger != nil {
		th.engine.logger.Infof("Show tenant successful for tenant: %s", tenantID)
	}

	th.writeJSONResponse(w, http.StatusOK, response)
}

// AddTenant handles POST /api/v1/tenants
func (th *TenantHandlers) AddTenant(w http.ResponseWriter, r *http.Request) {
	th.engine.TrackOperation()
	defer th.engine.UntrackOperation()

	// Log incoming request details
	if th.engine.logger != nil {
		th.engine.logger.Infof("Add tenant request received")
		th.engine.logger.Debugf("Request from: %s", r.RemoteAddr)
		th.engine.logger.Debugf("User-Agent: %s", r.Header.Get("User-Agent"))
	}

	// Parse request body
	var req AddTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if th.engine.logger != nil {
			th.engine.logger.Errorf("Failed to parse add tenant request body: %v", err)
		}
		th.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Validate required fields
	if req.TenantName == "" {
		if th.engine.logger != nil {
			th.engine.logger.Warnf("Add tenant request missing required field: tenant_name")
		}
		th.writeErrorResponse(w, http.StatusBadRequest, "Tenant name is required", "")
		return
	}

	// Log add tenant attempt
	if th.engine.logger != nil {
		th.engine.logger.Infof("Add tenant attempt for tenant: %s", req.TenantName)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Check if tenant client is available
	if th.engine.tenantClient == nil {
		if th.engine.logger != nil {
			th.engine.logger.Errorf("Tenant client is nil - gRPC connection may have failed during startup")
		}
		th.writeErrorResponse(w, http.StatusInternalServerError, "Tenant service unavailable", "")
		return
	}

	// Call core service gRPC
	grpcReq := &corev1.AddTenantRequest{
		TenantName:        req.TenantName,
		TenantUrl:         req.TenantURL,
		TenantDescription: req.TenantDescription,
		UserEmail:         req.UserEmail,
		UserPassword:      req.UserPassword,
	}

	// Log gRPC call attempt
	if th.engine.logger != nil {
		th.engine.logger.Debugf("Making gRPC AddTenant call to core service")
		th.engine.logger.Debugf("gRPC request: tenant_name=%s", grpcReq.TenantName)
	}

	grpcResp, err := th.engine.tenantClient.AddTenant(ctx, grpcReq)
	if err != nil {
		th.handleGRPCError(w, err, "Add tenant failed")
		return
	}

	// Log successful gRPC response
	if th.engine.logger != nil {
		th.engine.logger.Infof("gRPC AddTenant call successful for tenant: %s", req.TenantName)
		th.engine.logger.Debugf("gRPC response success: %t", grpcResp.Success)
	}

	// Convert gRPC response to REST response
	response := AddTenantResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Tenant: Tenant{
			TenantID:          grpcResp.Tenant.TenantId,
			TenantName:        grpcResp.Tenant.TenantName,
			TenantDescription: grpcResp.Tenant.TenantDescription,
			TenantURL:         grpcResp.Tenant.TenantUrl,
		},
	}

	if th.engine.logger != nil {
		th.engine.logger.Infof("Add tenant successful for tenant: %s", req.TenantName)
	}

	th.writeJSONResponse(w, http.StatusOK, response)
}

// ModifyTenant handles PUT /api/v1/tenants/{tenant_id}
func (th *TenantHandlers) ModifyTenant(w http.ResponseWriter, r *http.Request) {
	th.engine.TrackOperation()
	defer th.engine.UntrackOperation()

	// Extract tenant_id from path
	vars := mux.Vars(r)
	tenantID := vars["tenant_id"]
	if tenantID == "" {
		th.writeErrorResponse(w, http.StatusBadRequest, "tenant_id is required", "")
		return
	}

	// Log incoming request details
	if th.engine.logger != nil {
		th.engine.logger.Infof("Modify tenant request received for tenant: %s", tenantID)
		th.engine.logger.Debugf("Request from: %s", r.RemoteAddr)
	}

	// Parse request body
	var req ModifyTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if th.engine.logger != nil {
			th.engine.logger.Errorf("Failed to parse modify tenant request body: %v", err)
		}
		th.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Check if tenant client is available
	if th.engine.tenantClient == nil {
		if th.engine.logger != nil {
			th.engine.logger.Errorf("Tenant client is nil - gRPC connection may have failed during startup")
		}
		th.writeErrorResponse(w, http.StatusInternalServerError, "Tenant service unavailable", "")
		return
	}

	// Call core service gRPC
	grpcReq := &corev1.ModifyTenantRequest{
		TenantId: tenantID,
	}

	// Only set optional fields if they are provided (not empty)
	if req.TenantName != "" {
		grpcReq.TenantName = &req.TenantName
	}
	if req.TenantDescription != "" {
		grpcReq.TenantDescription = &req.TenantDescription
	}

	// Log gRPC call attempt
	if th.engine.logger != nil {
		th.engine.logger.Debugf("Making gRPC ModifyTenant call to core service")
		th.engine.logger.Debugf("gRPC request: tenant_id=%s", grpcReq.TenantId)
	}

	grpcResp, err := th.engine.tenantClient.ModifyTenant(ctx, grpcReq)
	if err != nil {
		th.handleGRPCError(w, err, "Modify tenant failed")
		return
	}

	// Log successful gRPC response
	if th.engine.logger != nil {
		th.engine.logger.Infof("gRPC ModifyTenant call successful for tenant: %s", tenantID)
		th.engine.logger.Debugf("gRPC response success: %t", grpcResp.Success)
	}

	// Convert gRPC response to REST response
	response := ModifyTenantResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Tenant: Tenant{
			TenantID:          grpcResp.Tenant.TenantId,
			TenantName:        grpcResp.Tenant.TenantName,
			TenantDescription: grpcResp.Tenant.TenantDescription,
			TenantURL:         grpcResp.Tenant.TenantUrl,
		},
	}

	if th.engine.logger != nil {
		th.engine.logger.Infof("Modify tenant successful for tenant: %s", tenantID)
	}

	th.writeJSONResponse(w, http.StatusOK, response)
}

// DeleteTenant handles DELETE /api/v1/tenants/{tenant_id}
func (th *TenantHandlers) DeleteTenant(w http.ResponseWriter, r *http.Request) {
	th.engine.TrackOperation()
	defer th.engine.UntrackOperation()

	// Extract tenant_id from path
	vars := mux.Vars(r)
	tenantID := vars["tenant_id"]
	if tenantID == "" {
		th.writeErrorResponse(w, http.StatusBadRequest, "tenant_id is required", "")
		return
	}

	// Log incoming request details
	if th.engine.logger != nil {
		th.engine.logger.Infof("Delete tenant request received for tenant: %s", tenantID)
		th.engine.logger.Debugf("Request from: %s", r.RemoteAddr)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Check if tenant client is available
	if th.engine.tenantClient == nil {
		if th.engine.logger != nil {
			th.engine.logger.Errorf("Tenant client is nil - gRPC connection may have failed during startup")
		}
		th.writeErrorResponse(w, http.StatusInternalServerError, "Tenant service unavailable", "")
		return
	}

	// Call core service gRPC
	grpcReq := &corev1.DeleteTenantRequest{
		TenantId: tenantID,
	}

	// Log gRPC call attempt
	if th.engine.logger != nil {
		th.engine.logger.Debugf("Making gRPC DeleteTenant call to core service")
		th.engine.logger.Debugf("gRPC request: tenant_id=%s", grpcReq.TenantId)
	}

	grpcResp, err := th.engine.tenantClient.DeleteTenant(ctx, grpcReq)
	if err != nil {
		th.handleGRPCError(w, err, "Delete tenant failed")
		return
	}

	// Log successful gRPC response
	if th.engine.logger != nil {
		th.engine.logger.Infof("gRPC DeleteTenant call successful for tenant: %s", tenantID)
		th.engine.logger.Debugf("gRPC response success: %t", grpcResp.Success)
	}

	// Convert gRPC response to REST response
	response := DeleteTenantResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
	}

	if th.engine.logger != nil {
		th.engine.logger.Infof("Delete tenant successful for tenant: %s", tenantID)
	}

	th.writeJSONResponse(w, http.StatusOK, response)
}

// handleGRPCError maps gRPC errors to appropriate HTTP responses without exposing internal details
func (th *TenantHandlers) handleGRPCError(w http.ResponseWriter, err error, defaultMessage string) {
	// Extract gRPC status from error
	grpcStatus, ok := status.FromError(err)
	if !ok {
		// Not a gRPC error, treat as internal error
		if th.engine.logger != nil {
			th.engine.logger.Errorf("Error is not a gRPC status error - this indicates a connection or protocol issue")
			th.engine.logger.Errorf("gRPC request failed: %s", defaultMessage)
			th.engine.logger.Errorf("Original error: %v", err)
			th.engine.logger.Errorf("Error type: %T", err)
		}
		th.writeErrorResponse(w, http.StatusInternalServerError, defaultMessage, "")
		return
	}

	// Map gRPC codes to HTTP status codes and user-friendly messages
	switch grpcStatus.Code() {
	case codes.NotFound:
		// Expected user behavior - log as info
		if th.engine.logger != nil {
			th.engine.logger.Infof("Tenant not found")
			th.engine.logger.Debugf("gRPC Status: %s - %s", grpcStatus.Code().String(), grpcStatus.Message())
		}
		th.writeErrorResponse(w, http.StatusNotFound, "Tenant not found", "")
	case codes.PermissionDenied:
		// Expected user behavior - log as info
		if th.engine.logger != nil {
			th.engine.logger.Infof("Permission denied for tenant operation")
			th.engine.logger.Debugf("gRPC Status: %s - %s", grpcStatus.Code().String(), grpcStatus.Message())
		}
		th.writeErrorResponse(w, http.StatusForbidden, "Access denied", "")
	case codes.InvalidArgument:
		// User input issue - log as info
		if th.engine.logger != nil {
			th.engine.logger.Infof("Invalid argument provided: %s", grpcStatus.Message())
			th.engine.logger.Debugf("gRPC Status: %s", grpcStatus.Code().String())
		}
		th.writeErrorResponse(w, http.StatusBadRequest, grpcStatus.Message(), "")
	case codes.Unavailable:
		// System error - log as error
		if th.engine.logger != nil {
			th.engine.logger.Errorf("Core service is unavailable - check if service is running on correct port")
			th.engine.logger.Errorf("gRPC request failed: %s", defaultMessage)
			th.engine.logger.Errorf("gRPC Status: %s - %s", grpcStatus.Code().String(), grpcStatus.Message())
		}
		th.writeErrorResponse(w, http.StatusServiceUnavailable, "Service temporarily unavailable", "")
	case codes.Unimplemented:
		// System error - log as error
		if th.engine.logger != nil {
			th.engine.logger.Errorf("Core service method not implemented - this may indicate wrong service on the target port")
			th.engine.logger.Errorf("gRPC request failed: %s", defaultMessage)
			th.engine.logger.Errorf("gRPC Status: %s - %s", grpcStatus.Code().String(), grpcStatus.Message())
		}
		th.writeErrorResponse(w, http.StatusServiceUnavailable, "Service temporarily unavailable", "")
	case codes.DeadlineExceeded:
		// System error - log as error
		if th.engine.logger != nil {
			th.engine.logger.Errorf("gRPC request timeout - core service took too long to respond")
			th.engine.logger.Errorf("gRPC request failed: %s", defaultMessage)
			th.engine.logger.Errorf("gRPC Status: %s - %s", grpcStatus.Code().String(), grpcStatus.Message())
		}
		th.writeErrorResponse(w, http.StatusRequestTimeout, "Request timeout", "")
	case codes.AlreadyExists:
		// User behavior - log as info
		if th.engine.logger != nil {
			th.engine.logger.Infof("Tenant already exists")
			th.engine.logger.Debugf("gRPC Status: %s - %s", grpcStatus.Code().String(), grpcStatus.Message())
		}
		th.writeErrorResponse(w, http.StatusConflict, "Tenant already exists", "")
	case codes.ResourceExhausted:
		// Rate limiting - log as warning
		if th.engine.logger != nil {
			th.engine.logger.Warnf("Rate limit exceeded")
			th.engine.logger.Debugf("gRPC Status: %s - %s", grpcStatus.Code().String(), grpcStatus.Message())
		}
		th.writeErrorResponse(w, http.StatusTooManyRequests, "Too many requests", "")
	case codes.Internal:
		// System error - log as error
		if th.engine.logger != nil {
			th.engine.logger.Errorf("Internal server error in core service")
			th.engine.logger.Errorf("gRPC request failed: %s", defaultMessage)
			th.engine.logger.Errorf("gRPC Status: %s - %s", grpcStatus.Code().String(), grpcStatus.Message())
		}
		th.writeErrorResponse(w, http.StatusInternalServerError, "Internal server error", "")
	default:
		// Unknown error - log as error
		if th.engine.logger != nil {
			th.engine.logger.Errorf("Unmapped gRPC error code: %s", grpcStatus.Code().String())
			th.engine.logger.Errorf("gRPC request failed: %s", defaultMessage)
			th.engine.logger.Errorf("Original gRPC error: %v", err)
			th.engine.logger.Errorf("gRPC Status Message: %s", grpcStatus.Message())
		}
		th.writeErrorResponse(w, http.StatusInternalServerError, defaultMessage, "")
	}
}

func (th *TenantHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

func (th *TenantHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, error string) {
	// Log error responses for monitoring and debugging
	if th.engine.logger != nil {
		if statusCode >= 500 {
			// Log 5xx errors as errors
			th.engine.logger.Errorf("HTTP %d - %s: %s", statusCode, message, error)
		} else if statusCode >= 400 {
			// Log 4xx errors as warnings
			th.engine.logger.Warnf("HTTP %d - %s: %s", statusCode, message, error)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	response := ErrorResponse{
		Error:   error,
		Message: message,
		Status:  StatusFailure,
	}

	json.NewEncoder(w).Encode(response)
}
