# Repo API Endpoints

This document describes the REST API endpoints for repository management in the Client API service.

## Base URL

All repo endpoints are nested under workspaces and follow this pattern:
```
/{tenant_url}/api/v1/workspaces/{workspace_id}/repos
```

## Authentication

All endpoints require authentication via Bearer token in the Authorization header:
```
Authorization: Bearer <token>
```

## Endpoints

### 1. List Repos

**GET** `/{tenant_url}/api/v1/workspaces/{workspace_id}/repos`

Lists all repositories within a specific workspace.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID

#### Response
```json
{
  "repos": [
    {
      "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
      "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
      "repo_id": "repo_01HGQK8F3VWXYZ123456789ABC",
      "repo_name": "application-repo",
      "repo_description": "Main application repository",
      "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
    }
  ]
}
```

### 2. Show Repo

**GET** `/{tenant_url}/api/v1/workspaces/{workspace_id}/repos/{repo_id}`

Shows details of a specific repository.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `repo_id` (string, required): The repository ID

#### Response
```json
{
  "repo": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "repo_id": "repo_01HGQK8F3VWXYZ123456789ABC",
    "repo_name": "application-repo",
    "repo_description": "Main application repository",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  }
}
```

### 3. Add Repo

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_id}/repos`

Creates a new repository within the workspace.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID

#### Request Body
```json
{
  "repo_name": "new-repo",
  "repo_description": "New repository for development"
}
```

#### Fields
- `repo_name` (string, required): Name of the repository
- `repo_description` (string, required): Description of the repository

**Note**: The `owner_id` is automatically set from the authenticated user's profile.

#### Response
```json
{
  "message": "Repository created successfully",
  "success": true,
  "repo": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "repo_id": "repo_01HGQK8F3VWXYZ123456789ABC",
    "repo_name": "new-repo",
    "repo_description": "New repository for development",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  },
  "status": "success"
}
```

### 4. Modify Repo

**PUT** `/{tenant_url}/api/v1/workspaces/{workspace_id}/repos/{repo_id}`

Updates an existing repository.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `repo_id` (string, required): The repository ID

#### Request Body
```json
{
  "repo_name": "updated-repo",
  "repo_description": "Updated repository description"
}
```

#### Fields
All fields are optional:
- `repo_name` (string): New name for the repository
- `repo_description` (string): New description

#### Response
```json
{
  "message": "Repository updated successfully",
  "success": true,
  "repo": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "repo_id": "repo_01HGQK8F3VWXYZ123456789ABC",
    "repo_name": "updated-repo",
    "repo_description": "Updated repository description",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  },
  "status": "success"
}
```

### 5. Clone Repo

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_id}/repos/{repo_id}/clone`

Creates a clone of an existing repository.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `repo_id` (string, required): The source repository ID

#### Request Body
```json
{
  "repo_name": "cloned-repo",
  "repo_description": "Cloned from application-repo"
}
```

#### Fields
- `repo_name` (string, required): Name for the cloned repository
- `repo_description` (string, required): Description for the cloned repository

**Note**: The `owner_id` is automatically set from the authenticated user's profile.

#### Response
```json
{
  "message": "Repository cloned successfully",
  "success": true,
  "repo": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "repo_id": "repo_01HGQK8F3VWXYZ123456789DEF",
    "repo_name": "cloned-repo",
    "repo_description": "Cloned from application-repo",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  },
  "status": "success"
}
```

### 6. Delete Repo

**DELETE** `/{tenant_url}/api/v1/workspaces/{workspace_id}/repos/{repo_id}`

Deletes a repository.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `repo_id` (string, required): The repository ID

#### Query Parameters
- `force` (boolean, optional): Force deletion even if repo has dependencies

#### Response
```json
{
  "message": "Repository deleted successfully",
  "success": true,
  "status": "success"
}
```

## Nested Resources

Repositories contain branches and commits. See the following documentation:
- [Branch Endpoints](branch_endpoints.md) - Manage branches within repos
- [Commit Endpoints](commit_endpoints.md) - Manage commits within branches

## Error Handling

All endpoints return appropriate HTTP status codes:

- `200 OK`: Successful GET/PUT operations
- `201 Created`: Successful POST operations
- `400 Bad Request`: Invalid request parameters or body
- `401 Unauthorized`: Authentication required
- `403 Forbidden`: Insufficient permissions
- `404 Not Found`: Resource not found
- `409 Conflict`: Resource already exists or has dependencies
- `500 Internal Server Error`: Server error

Error responses have the following format:
```json
{
  "error": "Error message",
  "message": "Detailed error description",
  "status": "error"
}
``` 