# Branch API Endpoints

This document describes the REST API endpoints for branch management in the Client API service.

## Base URL

All branch endpoints are nested under repositories and follow this pattern:
```
/{tenant_url}/api/v1/workspaces/{workspace_id}/repos/{repo_id}/branches
```

## Authentication

All endpoints require authentication via Bearer token in the Authorization header:
```
Authorization: Bearer <token>
```

## Endpoints

### 1. Show Branch

**GET** `/{tenant_url}/api/v1/workspaces/{workspace_id}/repos/{repo_id}/branches/{branch_id}`

Shows details of a specific branch including its child branches and commits.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `repo_id` (string, required): The repository ID
- `branch_id` (string, required): The branch ID

#### Response
```json
{
  "branch": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "repo_id": "repo_01HGQK8F3VWXYZ123456789ABC",
    "branch_id": "branch_01HGQK8F3VWXYZ123456789ABC",
    "branch_name": "main",
    "parent_branch_id": null,
    "parent_branch_name": null,
    "connected_to_database": true,
    "database_id": "db_01HGQK8F3VWXYZ123456789ABC",
    "child_branches": [
      {
        "branch_id": "branch_01HGQK8F3VWXYZ123456789DEF",
        "branch_name": "feature-branch",
        "status": "active"
      }
    ],
    "commits": [
      {
        "commit_id": "commit_01HGQK8F3VWXYZ123456789ABC",
        "commit_code": "abc123",
        "is_head": true,
        "commit_message": "Initial commit",
        "commit_date": "2023-12-01T10:00:00Z"
      }
    ],
    "status": "active"
  }
}
```

### 2. Attach Branch to Database

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_id}/repos/{repo_id}/branches/{branch_id}/attach`

Attaches a branch to a database for schema tracking.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `repo_id` (string, required): The repository ID
- `branch_id` (string, required): The branch ID

#### Request Body
```json
{
  "database_id": "db_01HGQK8F3VWXYZ123456789ABC"
}
```

#### Fields
- `database_id` (string, required): ID of the database to attach to

#### Response
```json
{
  "message": "Branch attached to database successfully",
  "success": true,
  "branch": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "repo_id": "repo_01HGQK8F3VWXYZ123456789ABC",
    "branch_id": "branch_01HGQK8F3VWXYZ123456789ABC",
    "branch_name": "main",
    "connected_to_database": true,
    "database_id": "db_01HGQK8F3VWXYZ123456789ABC",
    "status": "active"
  },
  "status": "success"
}
```

### 3. Detach Branch from Database

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_id}/repos/{repo_id}/branches/{branch_id}/detach`

Detaches a branch from its connected database.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `repo_id` (string, required): The repository ID
- `branch_id` (string, required): The branch ID

#### Response
```json
{
  "message": "Branch detached from database successfully",
  "success": true,
  "branch": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "repo_id": "repo_01HGQK8F3VWXYZ123456789ABC",
    "branch_id": "branch_01HGQK8F3VWXYZ123456789ABC",
    "branch_name": "main",
    "connected_to_database": false,
    "database_id": null,
    "status": "active"
  },
  "status": "success"
}
```

### 4. Modify Branch

**PUT** `/{tenant_url}/api/v1/workspaces/{workspace_id}/repos/{repo_id}/branches/{branch_id}`

Updates an existing branch.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `repo_id` (string, required): The repository ID
- `branch_id` (string, required): The branch ID

#### Request Body
```json
{
  "branch_name": "updated-branch-name"
}
```

#### Fields
- `branch_name` (string, optional): New name for the branch

#### Response
```json
{
  "message": "Branch updated successfully",
  "success": true,
  "branch": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "repo_id": "repo_01HGQK8F3VWXYZ123456789ABC",
    "branch_id": "branch_01HGQK8F3VWXYZ123456789ABC",
    "branch_name": "updated-branch-name",
    "status": "active"
  },
  "status": "success"
}
```

### 5. Delete Branch

**DELETE** `/{tenant_url}/api/v1/workspaces/{workspace_id}/repos/{repo_id}/branches/{branch_id}`

Deletes a branch.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `repo_id` (string, required): The repository ID
- `branch_id` (string, required): The branch ID

#### Query Parameters
- `force` (boolean, optional): Force deletion even if branch has child branches

#### Response
```json
{
  "message": "Branch deleted successfully",
  "success": true,
  "status": "success"
}
```

## Nested Resources

Branches contain commits. See the following documentation:
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