package engine

// Tenant represents a tenant in the system
type Tenant struct {
	TenantID          string `json:"tenant_id"`
	TenantName        string `json:"tenant_name"`
	TenantDescription string `json:"tenant_description"`
	TenantURL         string `json:"tenant_url"`
}

// ListTenantsResponse represents the response for listing tenants
type ListTenantsResponse struct {
	Tenants []Tenant `json:"tenants"`
}

// ShowTenantResponse represents the response for showing a tenant
type ShowTenantResponse struct {
	Tenant Tenant `json:"tenant"`
}

// AddTenantRequest represents the request for adding a tenant
type AddTenantRequest struct {
	TenantName        string `json:"tenant_name"`
	TenantURL         string `json:"tenant_url"`
	TenantDescription string `json:"tenant_description"`
	UserEmail         string `json:"user_email"`
	UserPassword      string `json:"user_password"`
}

// AddTenantResponse represents the response for adding a tenant
type AddTenantResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Tenant  Tenant `json:"tenant"`
}

// ModifyTenantRequest represents the request for modifying a tenant
type ModifyTenantRequest struct {
	TenantName        string `json:"tenant_name,omitempty"`
	TenantDescription string `json:"tenant_description,omitempty"`
}

// ModifyTenantResponse represents the response for modifying a tenant
type ModifyTenantResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Tenant  Tenant `json:"tenant"`
}

// DeleteTenantResponse represents the response for deleting a tenant
type DeleteTenantResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Status  string `json:"status"`
}

// Status constants
const (
	StatusSuccess = "success"
	StatusFailure = "error"
)
