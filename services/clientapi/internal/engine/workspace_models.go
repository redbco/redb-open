package engine

// Workspace represents a workspace
type Workspace struct {
	WorkspaceID          string `json:"workspace_id"`
	WorkspaceName        string `json:"workspace_name"`
	WorkspaceDescription string `json:"workspace_description,omitempty"`
	InstanceCount        int32  `json:"instance_count"`
	DatabaseCount        int32  `json:"database_count"`
	RepoCount            int32  `json:"repo_count"`
	MappingCount         int32  `json:"mapping_count"`
	RelationshipCount    int32  `json:"relationship_count"`
	OwnerID              string `json:"owner_id"`
}

// ListWorkspacesRequest represents the list workspaces request
type ListWorkspacesRequest struct {
	TenantID string `json:"tenant_id"`
}

// ListWorkspacesResponse represents the list workspaces response
type ListWorkspacesResponse struct {
	Workspaces []Workspace `json:"workspaces"`
}

// ShowWorkspaceRequest represents the show workspace request
type ShowWorkspaceRequest struct {
	TenantID    string `json:"tenant_id"`
	WorkspaceID string `json:"workspace_id"`
}

// ShowWorkspaceResponse represents the show workspace response
type ShowWorkspaceResponse struct {
	Workspace Workspace `json:"workspace"`
}

// AddWorkspaceRequest represents the add workspace request
// Note: owner_id is automatically set from the authenticated user's profile
type AddWorkspaceRequest struct {
	WorkspaceName        string `json:"workspace_name" validate:"required"`
	WorkspaceDescription string `json:"workspace_description,omitempty"`
}

// AddWorkspaceResponse represents the add workspace response
type AddWorkspaceResponse struct {
	Message   string    `json:"message"`
	Success   bool      `json:"success"`
	Workspace Workspace `json:"workspace"`
	Status    Status    `json:"status"`
}

// ModifyWorkspaceRequest represents the modify workspace request
type ModifyWorkspaceRequest struct {
	WorkspaceNameNew     string `json:"workspace_name_new,omitempty"`
	WorkspaceDescription string `json:"workspace_description,omitempty"`
}

// ModifyWorkspaceResponse represents the modify workspace response
type ModifyWorkspaceResponse struct {
	Message   string    `json:"message"`
	Success   bool      `json:"success"`
	Workspace Workspace `json:"workspace"`
	Status    Status    `json:"status"`
}

// DeleteWorkspaceRequest represents the delete workspace request
type DeleteWorkspaceRequest struct {
	TenantID    string `json:"tenant_id"`
	WorkspaceID string `json:"workspace_id"`
}

// DeleteWorkspaceResponse represents the delete workspace response
type DeleteWorkspaceResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}
