package engine

// User represents a user
type User struct {
	TenantID     string `json:"tenant_id"`
	UserID       string `json:"user_id"`
	UserName     string `json:"user_name"`
	UserEmail    string `json:"user_email"`
	UserPassword string `json:"user_password,omitempty"`
	UserEnabled  bool   `json:"user_enabled"`
}

// ListUsersRequest represents the list users request
type ListUsersRequest struct {
	TenantID string `json:"tenant_id"`
}

// ListUsersResponse represents the list users response
type ListUsersResponse struct {
	Users []User `json:"users"`
}

// ShowUserRequest represents the show user request
type ShowUserRequest struct {
	TenantID string `json:"tenant_id"`
	UserID   string `json:"user_id"`
}

// ShowUserResponse represents the show user response
type ShowUserResponse struct {
	User User `json:"user"`
}

// AddUserRequest represents the add user request
type AddUserRequest struct {
	UserName     string `json:"user_name" validate:"required"`
	UserEmail    string `json:"user_email" validate:"required"`
	UserPassword string `json:"user_password" validate:"required"`
	UserEnabled  *bool  `json:"user_enabled,omitempty"`
}

// AddUserResponse represents the add user response
type AddUserResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	User    User   `json:"user"`
	Status  Status `json:"status"`
}

// ModifyUserRequest represents the modify user request
type ModifyUserRequest struct {
	UserName     string `json:"user_name,omitempty"`
	UserEmail    string `json:"user_email,omitempty"`
	UserPassword string `json:"user_password,omitempty"`
	UserEnabled  *bool  `json:"user_enabled,omitempty"`
}

// ModifyUserResponse represents the modify user response
type ModifyUserResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	User    User   `json:"user"`
	Status  Status `json:"status"`
}

// DeleteUserRequest represents the delete user request
type DeleteUserRequest struct {
	TenantID string `json:"tenant_id"`
	UserID   string `json:"user_id"`
}

// DeleteUserResponse represents the delete user response
type DeleteUserResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}
