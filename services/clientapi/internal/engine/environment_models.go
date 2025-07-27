package engine

// Environment represents an environment
type Environment struct {
	EnvironmentID           string `json:"environment_id"`
	EnvironmentName         string `json:"environment_name"`
	EnvironmentDescription  string `json:"environment_description,omitempty"`
	EnvironmentIsProduction bool   `json:"environment_is_production"`
	EnvironmentCriticality  int32  `json:"environment_criticality"`
	EnvironmentPriority     int32  `json:"environment_priority"`
	InstanceCount           int32  `json:"instance_count"`
	DatabaseCount           int32  `json:"database_count"`
	Status                  Status `json:"status"`
	OwnerID                 string `json:"owner_id"`
}

// ListEnvironmentsResponse represents the list environments response
type ListEnvironmentsResponse struct {
	Environments []Environment `json:"environments"`
}

// ShowEnvironmentResponse represents the show environment response
type ShowEnvironmentResponse struct {
	Environment Environment `json:"environment"`
}

// AddEnvironmentRequest represents the add environment request
// Note: owner_id is automatically set from the authenticated user's profile
type AddEnvironmentRequest struct {
	EnvironmentName         string `json:"environment_name" validate:"required"`
	EnvironmentDescription  string `json:"environment_description,omitempty"`
	EnvironmentIsProduction *bool  `json:"environment_is_production,omitempty"`
	EnvironmentCriticality  *int32 `json:"environment_criticality,omitempty"`
	EnvironmentPriority     *int32 `json:"environment_priority,omitempty"`
}

// AddEnvironmentResponse represents the add environment response
type AddEnvironmentResponse struct {
	Message     string      `json:"message"`
	Success     bool        `json:"success"`
	Environment Environment `json:"environment"`
	Status      Status      `json:"status"`
}

// ModifyEnvironmentRequest represents the modify environment request
type ModifyEnvironmentRequest struct {
	EnvironmentNameNew      string `json:"environment_name_new,omitempty"`
	EnvironmentDescription  string `json:"environment_description,omitempty"`
	EnvironmentIsProduction *bool  `json:"environment_is_production,omitempty"`
	EnvironmentCriticality  *int32 `json:"environment_criticality,omitempty"`
	EnvironmentPriority     *int32 `json:"environment_priority,omitempty"`
}

// ModifyEnvironmentResponse represents the modify environment response
type ModifyEnvironmentResponse struct {
	Message     string      `json:"message"`
	Success     bool        `json:"success"`
	Environment Environment `json:"environment"`
	Status      Status      `json:"status"`
}

// DeleteEnvironmentResponse represents the delete environment response
type DeleteEnvironmentResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}
