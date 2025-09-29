package engine

// Profile represents user profile information
type Profile struct {
	TenantID string `json:"tenant_id"`
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	Email    string `json:"email"`
	Name     string `json:"name"`
}

// LoginRequest represents the login request payload
type LoginRequest struct {
	Username        string `json:"username" validate:"required"`
	Password        string `json:"password" validate:"required"`
	TenantURL       string `json:"tenant_url" validate:"required"`
	ExpiryTimeHours string `json:"expiry_time_hours,omitempty"`
	SessionName     string `json:"session_name,omitempty"`
	UserAgent       string `json:"user_agent,omitempty"`
	IPAddress       string `json:"ip_address,omitempty"`
	Platform        string `json:"platform,omitempty"`
	Browser         string `json:"browser,omitempty"`
	OperatingSystem string `json:"operating_system,omitempty"`
	DeviceType      string `json:"device_type,omitempty"`
	Location        string `json:"location,omitempty"`
}

// LoginResponse represents the login response payload
type LoginResponse struct {
	Profile      Profile `json:"profile"`
	AccessToken  string  `json:"access_token"`
	RefreshToken string  `json:"refresh_token"`
	SessionID    string  `json:"session_id"`
	Status       Status  `json:"status"`
}

// LogoutRequest represents the logout request payload
type LogoutRequest struct {
	RefreshToken string `json:"refresh_token" validate:"required"`
}

// LogoutResponse represents the logout response payload
type LogoutResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

// GetProfileRequest represents the get profile request payload
type GetProfileRequest struct {
	TenantID string `json:"tenant_id" validate:"required"`
	UserID   string `json:"user_id" validate:"required"`
}

// GetProfileResponse represents the get profile response payload
type GetProfileResponse struct {
	Profile Profile `json:"profile"`
}

// RefreshTokenRequest represents the refresh token request payload
type RefreshTokenRequest struct {
	RefreshToken string `json:"refresh_token" validate:"required"`
}

// RefreshTokenResponse represents the refresh token response payload
type RefreshTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	Status       Status `json:"status"`
}

// ToggleRootRequest represents the toggle root request payload
type ToggleRootRequest struct {
	TenantID string `json:"tenant_id" validate:"required"`
	Root     bool   `json:"root"`
}

// ToggleRootResponse represents the toggle root response payload
type ToggleRootResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

// ChangePasswordRequest represents the change password request payload
type ChangePasswordRequest struct {
	TenantID    string `json:"tenant_id" validate:"required"`
	UserID      string `json:"user_id" validate:"required"`
	OldPassword string `json:"old_password" validate:"required"`
	NewPassword string `json:"new_password" validate:"required"`
}

// ChangePasswordResponse represents the change password response payload
type ChangePasswordResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

// SessionInfo represents session information
type SessionInfo struct {
	SessionID       string `json:"session_id"`
	SessionName     string `json:"session_name"`
	UserAgent       string `json:"user_agent"`
	IPAddress       string `json:"ip_address"`
	Platform        string `json:"platform"`
	Browser         string `json:"browser"`
	OperatingSystem string `json:"operating_system"`
	DeviceType      string `json:"device_type"`
	Location        string `json:"location"`
	LastActivity    string `json:"last_activity"`
	Created         string `json:"created"`
	Expires         string `json:"expires"`
	IsCurrent       bool   `json:"is_current"`
}

// ListSessionsRequest represents the list sessions request payload
type ListSessionsRequest struct {
	TenantID string `json:"tenant_id" validate:"required"`
	UserID   string `json:"user_id" validate:"required"`
}

// ListSessionsResponse represents the list sessions response payload
type ListSessionsResponse struct {
	Sessions []SessionInfo `json:"sessions"`
	Status   Status        `json:"status"`
}

// LogoutSessionRequest represents the logout session request payload
type LogoutSessionRequest struct {
	TenantID  string `json:"tenant_id" validate:"required"`
	UserID    string `json:"user_id" validate:"required"`
	SessionID string `json:"session_id" validate:"required"`
}

// LogoutSessionResponse represents the logout session response payload
type LogoutSessionResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

// LogoutAllSessionsRequest represents the logout all sessions request payload
type LogoutAllSessionsRequest struct {
	TenantID       string `json:"tenant_id" validate:"required"`
	UserID         string `json:"user_id" validate:"required"`
	ExcludeCurrent bool   `json:"exclude_current,omitempty"`
}

// LogoutAllSessionsResponse represents the logout all sessions response payload
type LogoutAllSessionsResponse struct {
	SessionsLoggedOut int32  `json:"sessions_logged_out"`
	Message           string `json:"message"`
	Success           bool   `json:"success"`
	Status            Status `json:"status"`
}

// UpdateSessionNameRequest represents the update session name request payload
type UpdateSessionNameRequest struct {
	TenantID    string `json:"tenant_id" validate:"required"`
	UserID      string `json:"user_id" validate:"required"`
	SessionID   string `json:"session_id" validate:"required"`
	SessionName string `json:"session_name" validate:"required"`
}

// UpdateSessionNameResponse represents the update session name response payload
type UpdateSessionNameResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}
