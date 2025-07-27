# Database API Endpoints

This document describes the REST API endpoints for database management in the Client API service.

## Base URL

All database endpoints are nested under workspaces and follow this pattern:
```
/{tenant_url}/api/v1/workspaces/{workspace_id}/databases
```

## Authentication

All endpoints require authentication via Bearer token in the Authorization header:
```
Authorization: Bearer <token>
```

## Endpoints

### 1. List Databases

**GET** `/{tenant_url}/api/v1/workspaces/{workspace_id}/databases`

Lists all databases within a specific workspace.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID

#### Response
```json
{
  "databases": [
    {
      "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
      "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
      "environment_id": "env_01HGQK8F3VWXYZ123456789ABC",
      "connected_to_node_id": "node_01HGQK8F3VWXYZ123456789ABC",
      "instance_id": "inst_01HGQK8F3VWXYZ123456789ABC",
      "instance_name": "production-instance",
      "database_id": "db_01HGQK8F3VWXYZ123456789ABC",
      "database_name": "app-database",
      "database_description": "Application database",
      "database_type": "postgresql",
      "database_vendor": "postgresql",
      "database_version": "14.9",
      "database_username": "app_user",
      "database_db_name": "app_db",
      "database_enabled": true,
      "policy_ids": ["policy_01HGQK8F3VWXYZ123456789ABC"],
      "owner_id": "user_01HGQK8F3VWXYZ123456789ABC",
      "database_status_message": "Connected",
      "status": "healthy",
      "created": "2023-12-01T10:00:00Z",
      "updated": "2023-12-01T10:00:00Z"
    }
  ]
}
```

### 2. Show Database

**GET** `/{tenant_url}/api/v1/workspaces/{workspace_id}/databases/{database_id}`

Shows details of a specific database.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `database_id` (string, required): The database ID

#### Response
```json
{
  "database": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "environment_id": "env_01HGQK8F3VWXYZ123456789ABC",
    "connected_to_node_id": "node_01HGQK8F3VWXYZ123456789ABC",
    "instance_id": "inst_01HGQK8F3VWXYZ123456789ABC",
    "instance_name": "production-instance",
    "database_id": "db_01HGQK8F3VWXYZ123456789ABC",
    "database_name": "app-database",
    "database_description": "Application database",
    "database_type": "postgresql",
    "database_vendor": "postgresql",
    "database_version": "14.9",
    "database_username": "app_user",
    "database_db_name": "app_db",
    "database_enabled": true,
    "policy_ids": ["policy_01HGQK8F3VWXYZ123456789ABC"],
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC",
    "database_status_message": "Connected",
    "status": "healthy",
    "created": "2023-12-01T10:00:00Z",
    "updated": "2023-12-01T10:00:00Z"
  }
}
```

### 3. Connect Database

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_id}/databases/connect`

Connects a new database to the workspace.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID

#### Request Body
```json
{
  "database_name": "staging-database",
  "database_description": "Staging database",
  "database_type": "postgresql",
  "database_vendor": "postgresql",
  "host": "staging-db.example.com",
  "port": 5432,
  "username": "staging_user",
  "password": "staging_password",
  "db_name": "staging_db",
  "node_id": "node_01HGQK8F3VWXYZ123456789ABC",
  "enabled": true,
  "ssl": true,
  "ssl_mode": "require",
  "environment_id": "env_01HGQK8F3VWXYZ123456789ABC",
  "instance_id": "inst_01HGQK8F3VWXYZ123456789ABC",
  "instance_name": "staging-instance",
  "instance_description": "Staging database instance"
}
```

#### Fields
- `database_name` (string, required): Name of the database
- `database_description` (string, required): Description of the database
- `database_type` (string, required): Type of database
- `database_vendor` (string, required): Database vendor
- `host` (string, required): Database host address
- `port` (integer, required): Database port
- `username` (string, required): Database username
- `password` (string, required): Database password
- `db_name` (string, required): Database name
- `node_id` (string, required): ID of the node to connect to
- `enabled` (boolean, optional): Whether the database is enabled
- `ssl` (boolean, optional): Whether SSL is enabled
- `ssl_mode` (string, optional): SSL mode
- `ssl_cert` (string, optional): SSL certificate
- `ssl_key` (string, optional): SSL private key
- `ssl_root_cert` (string, optional): SSL root certificate
- `environment_id` (string, optional): Environment ID
- `instance_id` (string, optional): Existing instance ID
- `instance_name` (string, optional): Instance name (if creating new)
- `instance_description` (string, optional): Instance description

### 4. Modify Database

**PUT** `/{tenant_url}/api/v1/workspaces/{workspace_id}/databases/{database_id}`

Updates an existing database.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `database_id` (string, required): The database ID

#### Request Body
```json
{
  "database_name": "updated-database",
  "database_description": "Updated database description",
  "host": "new-db.example.com",
  "port": 5433,
  "username": "new_user",
  "password": "new_password",
  "enabled": true
}
```

### 5. Disconnect Database

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_id}/databases/{database_id}/disconnect`

Disconnects a database from the workspace.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `database_id` (string, required): The database ID

#### Request Body (Optional)
```json
{
  "delete_database_object": false,
  "delete_repo": false
}
```

#### Fields
- `delete_database_object` (boolean, optional): Whether to delete the database object
- `delete_repo` (boolean, optional): Whether to delete associated repository

### 6. Get Latest Stored Database Schema

**GET** `/{tenant_url}/api/v1/workspaces/{workspace_id}/databases/{database_id}/schema`

Retrieves the latest stored schema for a database.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `database_id` (string, required): The database ID

#### Response
```json
{
  "message": "Schema retrieved successfully",
  "success": true,
  "status": "success",
  "schema": {
    // Schema object content varies by database type
    "tables": [...],
    "views": [...],
    "procedures": [...]
  }
}
```

### 7. Wipe Database

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_id}/databases/{database_id}/wipe`

Wipes all data from a database.

⚠️ **WARNING**: This operation is destructive and cannot be undone.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `database_id` (string, required): The database ID

#### Response
```json
{
  "message": "Database wiped successfully",
  "success": true,
  "status": "success"
}
```

### 8. Add Database to Instance

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_id}/databases/instances/{instance_id}/add`

Adds a new database to an existing instance.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `instance_id` (string, required): The instance ID

#### Request Body
```json
{
  "database_name": "new-database",
  "database_description": "New database on existing instance",
  "db_name": "new_db",
  "node_id": "node_01HGQK8F3VWXYZ123456789ABC",
  "enabled": true
}
```

### 9. Drop Database

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_id}/databases/{database_id}/drop`

Drops a database from the system.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `database_id` (string, required): The database ID

#### Response
```json
{
  "message": "Database dropped successfully",
  "success": true,
  "status": "success"
}
```

### 10. Transform Data

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_id}/databases/transform`

Transforms data from a source database table to a target database table using a mapping configuration.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID

#### Request Body
```json
{
  "source_database_name": "source_db",
  "source_table_name": "users",
  "target_database_name": "target_db",
  "target_table_name": "customers",
  "mapping_id": "mapping_01HGQK8F3VWXYZ123456789ABC",
  "mode": "append",
  "options": {
    "batch_size": 1000,
    "timeout": 300
  }
}
```

#### Request Fields
- `source_database_name` (string, required): Name of the source database
- `source_table_name` (string, required): Name of the source table
- `target_database_name` (string, required): Name of the target database
- `target_table_name` (string, required): Name of the target table
- `mapping_id` (string, required): ID of the mapping configuration to use
- `mode` (string, required): Transformation mode - one of "append", "replace", "update"
- `options` (object, optional): Additional options for the transformation

#### Mode Options
- `append`: Only insert new rows that don't exist in the target table
- `replace`: Delete all existing data in the target table and insert new data
- `update`: Update existing rows and insert new ones based on unique constraints

#### Response
```json
{
  "message": "Data transformation completed successfully",
  "success": true,
  "status": "success",
  "source_database_name": "source_db",
  "source_table_name": "users",
  "target_database_name": "target_db",
  "target_table_name": "customers",
  "rows_transformed": 1500,
  "rows_affected": 1500
}
```

#### Response Fields
- `message` (string): Success or error message
- `success` (boolean): Whether the operation was successful
- `status` (string): Status of the operation
- `source_database_name` (string): Name of the source database
- `source_table_name` (string): Name of the source table
- `target_database_name` (string): Name of the target database
- `target_table_name` (string): Name of the target table
- `rows_transformed` (integer): Number of rows processed during transformation
- `rows_affected` (integer): Number of rows actually inserted/updated/deleted

#### Error Responses

**400 Bad Request** - Invalid request parameters
```json
{
  "error": "Required fields missing",
  "message": "source_database_name, source_table_name, target_database_name, target_table_name, mapping_id, and mode are required",
  "status": "error"
}
```

**400 Bad Request** - Invalid mode
```json
{
  "error": "Invalid mode",
  "message": "mode must be one of: append, replace, update",
  "status": "error"
}
```

**404 Not Found** - Database or mapping not found
```json
{
  "error": "Database not found",
  "message": "Failed to transform data",
  "status": "error"
}
```

**500 Internal Server Error** - Transformation failed
```json
{
  "error": "Transformation failed",
  "message": "Failed to transform data",
  "status": "error"
}
```

## Notes

- The data transformation endpoint supports cross-database transformations
- The mapping configuration must be created before using this endpoint
- Large datasets are processed in batches for optimal performance
- The operation is transactional and will rollback on failure
- Timeout is set to 5 minutes for large transformations

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