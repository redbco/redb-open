# Environment API Endpoints

This document describes the REST API endpoints for environment management in the Client API service.

## Base URL

All environment endpoints are nested under workspaces and follow this pattern:
```
/{tenant_url}/api/v1/workspaces/{workspace_id}/environments
```

## Authentication

All endpoints require authentication via Bearer token in the Authorization header:
```
Authorization: Bearer <token>
```

## Endpoints

### 1. List Environments

**GET** `/{tenant_url}/api/v1/workspaces/{workspace_id}/environments`

Lists all environments within a specific workspace.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID

#### Response
```json
{
  "environments": [
    {
      "environment_id": "env_01HGQK8F3VWXYZ123456789ABC",
      "environment_name": "production",
      "environment_description": "Production environment",
      "production": true,
      "criticality": 5,
      "priority": 1,
      "instance_count": 3,
      "database_count": 5,
      "status": "healthy",
      "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
    }
  ]
}
```

### 2. Show Environment

**GET** `/{tenant_url}/api/v1/workspaces/{workspace_id}/environments/{environment_id}`

Shows details of a specific environment.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `environment_id` (string, required): The environment ID

#### Response
```json
{
  "environment": {
    "environment_id": "env_01HGQK8F3VWXYZ123456789ABC",
    "environment_name": "production",
    "environment_description": "Production environment",
    "production": true,
    "criticality": 5,
    "priority": 1,
    "instance_count": 3,
    "database_count": 5,
    "status": "healthy",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  }
}
```

### 3. Add Environment

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_id}/environments`

Creates a new environment within a workspace.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID

#### Request Body
```json
{
  "environment_name": "staging",
  "environment_description": "Staging environment for testing",
  "production": false,
  "criticality": 3,
  "priority": 2
}
```

#### Fields
- `environment_name` (string, required): Name of the environment
- `environment_description` (string, optional): Description of the environment
- `production` (boolean, optional): Whether this is a production environment
- `criticality` (integer, optional): Criticality level (1-5)
- `priority` (integer, optional): Priority level

**Note**: The `owner_id` is automatically set from the authenticated user's profile and cannot be specified in the request body.

#### Response
```json
{
  "message": "Environment created successfully",
  "success": true,
  "environment": {
    "environment_id": "env_01HGQK8F3VWXYZ123456789ABC",
    "environment_name": "staging",
    "environment_description": "Staging environment for testing",
    "production": false,
    "criticality": 3,
    "priority": 2,
    "instance_count": 0,
    "database_count": 0,
    "status": "pending",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  },
  "status": "success"
}
```

### 4. Modify Environment

**PUT** `/{tenant_url}/api/v1/workspaces/{workspace_id}/environments/{environment_id}`

Updates an existing environment.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `environment_id` (string, required): The environment ID

#### Request Body
```json
{
  "environment_name": "staging-updated",
  "environment_description": "Updated staging environment",
  "production": false,
  "criticality": 4,
  "priority": 1
}
```

#### Fields
All fields are optional:
- `environment_name` (string): New name for the environment
- `environment_description` (string): New description
- `production` (boolean): Update production flag
- `criticality` (integer): Update criticality level
- `priority` (integer): Update priority level

#### Response
```json
{
  "message": "Environment updated successfully",
  "success": true,
  "environment": {
    "environment_id": "env_01HGQK8F3VWXYZ123456789ABC",
    "environment_name": "staging-updated",
    "environment_description": "Updated staging environment",
    "production": false,
    "criticality": 4,
    "priority": 1,
    "instance_count": 0,
    "database_count": 0,
    "status": "healthy",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  },
  "status": "success"
}
```

### 5. Delete Environment

**DELETE** `/{tenant_url}/api/v1/workspaces/{workspace_id}/environments/{environment_id}`

Deletes an environment.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `environment_id` (string, required): The environment ID

#### Response
```json
{
  "message": "Environment deleted successfully",
  "success": true,
  "status": "success"
}
```

## Error Responses

All endpoints may return error responses in the following format:

```json
{
  "error": "detailed error message",
  "message": "user-friendly error message",
  "status": "error"
}
```

### Common Error Codes
- **400 Bad Request**: Invalid request parameters or body
- **401 Unauthorized**: Missing or invalid authentication token
- **403 Forbidden**: Insufficient permissions
- **404 Not Found**: Environment, workspace, or tenant not found
- **409 Conflict**: Environment name already exists
- **500 Internal Server Error**: Server-side error

## Examples

### Creating a production environment
```bash
curl -X POST "https://api.example.com/mycompany/api/v1/workspaces/ws_123/environments" \
  -H "Authorization: Bearer your-token-here" \
  -H "Content-Type: application/json" \
  -d '{
    "environment_name": "production",
    "environment_description": "Production environment for live traffic",
    "production": true,
    "criticality": 5,
    "priority": 1
  }'
```

### Listing environments
```bash
curl -X GET "https://api.example.com/mycompany/api/v1/workspaces/ws_123/environments" \
  -H "Authorization: Bearer your-token-here"
``` 