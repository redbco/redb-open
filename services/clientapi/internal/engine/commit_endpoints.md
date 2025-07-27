# Commit API Endpoints

This document describes the REST API endpoints for commit management in the Client API service.

## Base URL

All commit endpoints are nested under branches and follow this pattern:
```
/{tenant_url}/api/v1/workspaces/{workspace_id}/repos/{repo_id}/branches/{branch_id}/commits
```

## Authentication

All endpoints require authentication via Bearer token in the Authorization header:
```
Authorization: Bearer <token>
```

## Endpoints

### 1. Show Commit

**GET** `/{tenant_url}/api/v1/workspaces/{workspace_id}/repos/{repo_id}/branches/{branch_id}/commits/{commit_id}`

Shows details of a specific commit.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `repo_id` (string, required): The repository ID
- `branch_id` (string, required): The branch ID
- `commit_id` (string, required): The commit ID

#### Response
```json
{
  "commit": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "repo_id": "repo_01HGQK8F3VWXYZ123456789ABC",
    "branch_id": "branch_01HGQK8F3VWXYZ123456789ABC",
    "commit_id": "commit_01HGQK8F3VWXYZ123456789ABC",
    "commit_code": "abc123def456",
    "is_head": true,
    "commit_message": "Add new feature implementation",
    "schema_type": "postgresql",
    "schema_structure": "CREATE TABLE users...",
    "commit_date": "2023-12-01T10:00:00Z"
  }
}
```

### 2. Branch Commit

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_id}/repos/{repo_id}/branches/{branch_id}/commits/{commit_id}/branch`

Creates a new branch from a specific commit.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `repo_id` (string, required): The repository ID
- `branch_id` (string, required): The source branch ID
- `commit_id` (string, required): The commit ID to branch from

#### Request Body
```json
{
  "new_branch_name": "feature-new-functionality"
}
```

#### Fields
- `new_branch_name` (string, required): Name for the new branch

#### Response
```json
{
  "message": "Branch created from commit successfully",
  "success": true,
  "commit": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "repo_id": "repo_01HGQK8F3VWXYZ123456789ABC",
    "branch_id": "branch_01HGQK8F3VWXYZ123456789DEF",
    "commit_id": "commit_01HGQK8F3VWXYZ123456789ABC",
    "commit_code": "abc123def456",
    "is_head": true,
    "commit_message": "Add new feature implementation",
    "commit_date": "2023-12-01T10:00:00Z"
  },
  "status": "success"
}
```

### 3. Merge Commit

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_id}/repos/{repo_id}/branches/{branch_id}/commits/{commit_id}/merge`

Merges a commit to the parent branch.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `repo_id` (string, required): The repository ID
- `branch_id` (string, required): The branch ID
- `commit_id` (string, required): The commit ID to merge

#### Response
```json
{
  "message": "Commit merged to parent branch successfully",
  "success": true,
  "commit": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "repo_id": "repo_01HGQK8F3VWXYZ123456789ABC",
    "branch_id": "branch_01HGQK8F3VWXYZ123456789ABC",
    "commit_id": "commit_01HGQK8F3VWXYZ123456789ABC",
    "commit_code": "abc123def456",
    "is_head": true,
    "commit_message": "Merged feature implementation",
    "commit_date": "2023-12-01T10:00:00Z"
  },
  "status": "success"
}
```

### 4. Deploy Commit

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_id}/repos/{repo_id}/branches/{branch_id}/commits/{commit_id}/deploy`

Deploys a commit to the connected database.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `repo_id` (string, required): The repository ID
- `branch_id` (string, required): The branch ID
- `commit_id` (string, required): The commit ID to deploy

#### Response
```json
{
  "message": "Commit deployed successfully",
  "success": true,
  "commit": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "repo_id": "repo_01HGQK8F3VWXYZ123456789ABC",
    "branch_id": "branch_01HGQK8F3VWXYZ123456789ABC",
    "commit_id": "commit_01HGQK8F3VWXYZ123456789ABC",
    "commit_code": "abc123def456",
    "is_head": true,
    "commit_message": "Add new feature implementation",
    "commit_date": "2023-12-01T10:00:00Z"
  },
  "status": "success"
}
```

## Schema Management

Commits in this system represent database schema changes. Each commit contains:
- **Schema Type**: The type of database (postgresql, mysql, etc.)
- **Schema Structure**: The actual DDL statements for the schema
- **Commit Code**: A unique identifier for the schema version

## Error Handling

All endpoints return appropriate HTTP status codes:

- `200 OK`: Successful GET operations
- `201 Created`: Successful POST operations
- `400 Bad Request`: Invalid request parameters or body
- `401 Unauthorized`: Authentication required
- `403 Forbidden`: Insufficient permissions
- `404 Not Found`: Resource not found
- `409 Conflict`: Merge conflicts or deployment issues
- `500 Internal Server Error`: Server error

Error responses have the following format:
```json
{
  "error": "Error message",
  "message": "Detailed error description",
  "status": "error"
}
``` 