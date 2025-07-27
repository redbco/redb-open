# Instance API Endpoints

This document describes the REST API endpoints for instance management in the Client API service.

## Base URL

All instance endpoints are nested under workspaces and follow this pattern:
```
/{tenant_url}/api/v1/workspaces/{workspace_name}/instances
```

## Authentication

All endpoints require authentication via Bearer token in the Authorization header:
```
Authorization: Bearer <token>
```

## Endpoints

### 1. List Instances

**GET** `/{tenant_url}/api/v1/workspaces/{workspace_name}/instances`

Lists all instances within a specific workspace.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_name` (string, required): The workspace ID

#### Response
```json
{
  "instances": [
    {
      "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
      "workspace_name": "ws_01HGQK8F3VWXYZ123456789ABC",
      "environment_id": "env_01HGQK8F3VWXYZ123456789ABC",
      "instance_id": "inst_01HGQK8F3VWXYZ123456789ABC",
      "instance_name": "production-db",
      "instance_description": "Production database instance",
      "instance_type": "postgresql",
      "instance_vendor": "postgresql",
      "instance_version": "14.9",
      "instance_unique_identifier": "uuid_01HGQK8F3VWXYZ123456789ABC",
      "connected_to_node_id": "node_01HGQK8F3VWXYZ123456789ABC",
      "instance_host": "db.example.com",
      "instance_port": 5432,
      "instance_username": "app_user",
      "instance_password": "encrypted_password",
      "instance_system_db_name": "postgres",
      "instance_enabled": true,
      "instance_ssl": true,
      "instance_ssl_mode": "require",
      "policy_ids": ["policy_01HGQK8F3VWXYZ123456789ABC"],
      "owner_id": "user_01HGQK8F3VWXYZ123456789ABC",
      "instance_status_message": "Connected",
      "status": "healthy",
      "created": "2023-12-01T10:00:00Z",
      "updated": "2023-12-01T10:00:00Z"
    }
  ]
}
```

### 2. Show Instance

**GET** `/{tenant_url}/api/v1/workspaces/{workspace_name}/instances/{instance_name}`

Shows details of a specific instance.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_name` (string, required): The workspace ID
- `instance_name` (string, required): The instance ID

#### Response
```json
{
  "instance": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_name": "ws_01HGQK8F3VWXYZ123456789ABC",
    "environment_id": "env_01HGQK8F3VWXYZ123456789ABC",
    "instance_id": "inst_01HGQK8F3VWXYZ123456789ABC",
    "instance_name": "production-db",
    "instance_description": "Production database instance",
    "instance_type": "postgresql",
    "instance_vendor": "postgresql",
    "instance_version": "14.9",
    "instance_unique_identifier": "uuid_01HGQK8F3VWXYZ123456789ABC",
    "connected_to_node_id": "node_01HGQK8F3VWXYZ123456789ABC",
    "instance_host": "db.example.com",
    "instance_port": 5432,
    "instance_username": "app_user",
    "instance_password": "encrypted_password",
    "instance_system_db_name": "postgres",
    "instance_enabled": true,
    "instance_ssl": true,
    "instance_ssl_mode": "require",
    "policy_ids": ["policy_01HGQK8F3VWXYZ123456789ABC"],
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC",
    "instance_status_message": "Connected",
    "status": "healthy",
    "created": "2023-12-01T10:00:00Z",
    "updated": "2023-12-01T10:00:00Z"
  }
}
```

### 3. Connect Instance

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_name}/instances/connect`

Connects a new database instance to the workspace.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_name` (string, required): The workspace ID

#### Request Body
```json
{
  "instance_name": "staging-db",
  "instance_description": "Staging database instance",
  "instance_type": "postgresql",
  "instance_vendor": "postgresql",
  "host": "staging-db.example.com",
  "port": 5432,
  "username": "staging_user",
  "password": "staging_password",
  "node_id": "node_01HGQK8F3VWXYZ123456789ABC",
  "enabled": true,
  "ssl": true,
  "ssl_mode": "require",
  "environment_id": "env_01HGQK8F3VWXYZ123456789ABC"
}
```

#### Fields
- `instance_name` (string, required): Name of the instance
- `instance_description` (string, required): Description of the instance
- `instance_type` (string, required): Type of database (e.g., postgresql, mysql)
- `instance_vendor` (string, required): Database vendor
- `host` (string, required): Database host address
- `port` (integer, required): Database port
- `username` (string, required): Database username
- `password` (string, required): Database password
- `node_id` (string, optional): ID of the node to connect to
- `enabled` (boolean, optional): Whether the instance is enabled
- `ssl` (boolean, optional): Whether SSL is enabled
- `ssl_mode` (string, optional): SSL mode (require, prefer, disable)
- `ssl_cert` (string, optional): SSL certificate
- `ssl_key` (string, optional): SSL private key
- `ssl_root_cert` (string, optional): SSL root certificate
- `environment_id` (string, optional): Environment ID for the instance

**Note**: The `owner_id` is automatically set from the authenticated user's profile.

#### Response
```json
{
  "message": "Instance connected successfully",
  "success": true,
  "instance": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_name": "ws_01HGQK8F3VWXYZ123456789ABC",
    "environment_id": "env_01HGQK8F3VWXYZ123456789ABC",
    "instance_id": "inst_01HGQK8F3VWXYZ123456789ABC",
    "instance_name": "staging-db",
    "instance_description": "Staging database instance",
    "instance_type": "postgresql",
    "instance_vendor": "postgresql",
    "instance_version": "14.9",
    "connected_to_node_id": "node_01HGQK8F3VWXYZ123456789ABC",
    "instance_host": "staging-db.example.com",
    "instance_port": 5432,
    "instance_username": "staging_user",
    "instance_enabled": true,
    "instance_ssl": true,
    "instance_ssl_mode": "require",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC",
    "instance_status_message": "Connected",
    "status": "healthy",
    "created": "2023-12-01T10:00:00Z",
    "updated": "2023-12-01T10:00:00Z"
  },
  "status": "success"
}
```

### 4. Modify Instance

**PUT** `/{tenant_url}/api/v1/workspaces/{workspace_name}/instances/{instance_name}`

Updates an existing instance.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_name` (string, required): The workspace ID
- `instance_name` (string, required): The instance ID

#### Request Body
```json
{
  "instance_name": "updated-staging-db",
  "instance_description": "Updated staging database instance",
  "host": "new-staging-db.example.com",
  "port": 5433,
  "username": "new_staging_user",
  "password": "new_staging_password",
  "enabled": true,
  "ssl": true,
  "ssl_mode": "verify-full"
}
```

#### Fields
All fields are optional:
- `instance_name` (string): New name for the instance
- `instance_description` (string): New description
- `instance_type` (string): Update instance type
- `instance_vendor` (string): Update vendor
- `host` (string): Update host address
- `port` (integer): Update port
- `username` (string): Update username
- `password` (string): Update password
- `enabled` (boolean): Update enabled status
- `ssl` (boolean): Update SSL setting
- `ssl_mode` (string): Update SSL mode
- `ssl_cert` (string): Update SSL certificate
- `ssl_key` (string): Update SSL key
- `ssl_root_cert` (string): Update SSL root certificate
- `environment_id` (string): Update environment
- `node_id` (string): Update connected node

#### Response
```json
{
  "message": "Instance updated successfully",
  "success": true,
  "instance": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_name": "ws_01HGQK8F3VWXYZ123456789ABC",
    "instance_id": "inst_01HGQK8F3VWXYZ123456789ABC",
    "instance_name": "updated-staging-db",
    "instance_description": "Updated staging database instance",
    "instance_host": "new-staging-db.example.com",
    "instance_port": 5433,
    "instance_username": "new_staging_user",
    "instance_enabled": true,
    "instance_ssl": true,
    "instance_ssl_mode": "verify-full",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC",
    "status": "healthy",
    "updated": "2023-12-01T11:00:00Z"
  },
  "status": "success"
}
```

### 5. Disconnect Instance

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_name}/instances/{instance_name}/disconnect`

Disconnects an instance from the workspace.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_name` (string, required): The workspace ID
- `instance_name` (string, required): The instance ID

#### Request Body (Optional)
```json
{
  "delete_instance": false
}
```

#### Fields
- `delete_instance` (boolean, optional): Whether to delete the instance object

#### Response
```json
{
  "message": "Instance disconnected successfully",
  "success": true,
  "status": "success"
}
```

## Error Handling

All endpoints return appropriate HTTP status codes:

- `200 OK`: Successful GET/PUT operations
- `201 Created`: Successful POST operations
- `400 Bad Request`: Invalid request parameters or body
- `401 Unauthorized`: Authentication required
- `403 Forbidden`: Insufficient permissions
- `404 Not Found`: Resource not found
- `409 Conflict`: Resource already exists
- `500 Internal Server Error`: Server error

Error responses have the following format:
```json
{
  "error": "Error message",
  "message": "Detailed error description",
  "status": "error"
}
``` 