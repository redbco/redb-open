# Workspace API Endpoints

This document describes the workspace management endpoints available in the Client API service.

## Base URL

All endpoints are prefixed with: `/{tenant_url}/api/v1/workspaces`

## Authentication

All workspace endpoints require authentication via Bearer token in the Authorization header:

```
Authorization: Bearer <access_token>
```

## Authorization

Users must have appropriate permissions for workspace operations:
- `list_workspaces` - List all workspaces in a tenant
- `read_workspace` - View a specific workspace 
- `create_workspace` - Create new workspaces
- `update_workspace` - Modify existing workspaces
- `delete_workspace` - Delete workspaces

## Endpoints

### List Workspaces

**GET** `/{tenant_url}/api/v1/workspaces`

Lists all workspaces in the authenticated user's tenant.

**Response:**
```json
{
  "workspaces": [
    {
      "workspace_id": "ws-12345",
      "workspace_name": "Production Environment",
      "workspace_description": "Main production workspace",
      "instance_count": 5,
      "database_count": 3,
      "repo_count": 2,
      "mapping_count": 10,
      "relationship_count": 7,
      "owner_id": "user-67890"
    }
  ]
}
```

### Show Workspace

**GET** `/{tenant_url}/api/v1/workspaces/{workspace_name}`

Retrieves details for a specific workspace.

**Parameters:**
- `workspace_name` (path) - The workspace name

**Response:**
```json
{
  "workspace": {
    "workspace_id": "ws-12345",
    "workspace_name": "Production Environment", 
    "workspace_description": "Main production workspace",
    "instance_count": 5,
    "database_count": 3,
    "repo_count": 2,
    "mapping_count": 10,
    "relationship_count": 7,
    "owner_id": "user-67890"
  }
}
```

### Create Workspace

**POST** `/{tenant_url}/api/v1/workspaces`

Creates a new workspace.

**Request Body:**
```json
{
  "workspace_name": "Development Environment",
  "workspace_description": "Development and testing workspace",
}
```

**Required Fields:**
- `workspace_name` - Name of the workspace
- `owner_id` - User ID of the workspace owner

**Optional Fields:**
- `workspace_description` - Description of the workspace

**Response:**
```json
{
  "message": "Workspace created successfully",
  "success": true,
  "workspace": {
    "workspace_id": "ws-54321",
    "workspace_name": "Development Environment",
    "workspace_description": "Development and testing workspace", 
    "instance_count": 0,
    "database_count": 0,
    "repo_count": 0,
    "mapping_count": 0,
    "relationship_count": 0,
    "owner_id": "user-67890"
  },
  "status": "created"
}
```

### Update Workspace

**PUT** `/{tenant_url}/api/v1/workspaces/{workspace_name}`

Updates an existing workspace.

**Parameters:**
- `workspace_name` (path) - The workspace name

**Request Body:**
```json
{
  "workspace_name": "Updated Development Environment",
  "workspace_description": "Updated description for dev workspace"
}
```

**Optional Fields:**
- `workspace_name` - New name for the workspace
- `workspace_description` - New description for the workspace

**Response:**
```json
{
  "message": "Workspace updated successfully",
  "success": true,
  "workspace": {
    "workspace_id": "ws-54321",
    "workspace_name": "Updated Development Environment",
    "workspace_description": "Updated description for dev workspace",
    "instance_count": 2,
    "database_count": 1,
    "repo_count": 1,
    "mapping_count": 3,
    "relationship_count": 2,
    "owner_id": "user-67890"
  },
  "status": "updated"
}
```

### Delete Workspace

**DELETE** `/{tenant_url}/api/v1/workspaces/{workspace_name}`

Deletes a workspace.

**Parameters:**
- `workspace_name` (path) - The workspace name

**Response:**
```json
{
  "message": "Workspace deleted successfully",
  "success": true,
  "status": "deleted"
}
```

## Error Responses

All endpoints may return error responses in the following format:

```json
{
  "error": "Detailed error message",
  "message": "User-friendly error message",
  "status": "error"
}
```

### Common HTTP Status Codes

- `200 OK` - Request successful
- `201 Created` - Resource created successfully
- `400 Bad Request` - Invalid request data
- `401 Unauthorized` - Authentication required or invalid
- `403 Forbidden` - Insufficient permissions
- `404 Not Found` - Resource not found
- `409 Conflict` - Resource already exists
- `500 Internal Server Error` - Server error
- `503 Service Unavailable` - Core service unavailable

### Example Error Response

```json
{
  "error": "workspace not found", 
  "message": "The specified workspace does not exist",
  "status": "error"
}
```

## Rate Limiting

All endpoints are subject to rate limiting. If rate limits are exceeded, a `429 Too Many Requests` response will be returned.

## CORS

The API supports Cross-Origin Resource Sharing (CORS) with the following headers:
- `Access-Control-Allow-Origin: *`
- `Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS`
- `Access-Control-Allow-Headers: Content-Type, Authorization` 