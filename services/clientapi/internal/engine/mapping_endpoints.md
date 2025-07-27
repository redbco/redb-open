# Mapping API Endpoints

This document describes the REST API endpoints for mapping management in the Client API service.

## Base URL

All mapping endpoints are nested under workspaces and follow this pattern:
```
/{tenant_url}/api/v1/workspaces/{workspace_id}/mappings
```

## Authentication

All endpoints require authentication via Bearer token in the Authorization header:
```
Authorization: Bearer <token>
```

## Endpoints

### 1. List Mappings

**GET** `/{tenant_url}/api/v1/workspaces/{workspace_id}/mappings`

Lists all mappings within a specific workspace.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID

#### Response
```json
{
  "mappings": [
    {
      "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
      "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
      "mapping_id": "mapping_01HGQK8F3VWXYZ123456789ABC",
      "mapping_name": "user-profile-mapping",
      "mapping_description": "Maps user data to profile structure",
      "mapping_source_type": "postgresql",
      "mapping_target_type": "json",
      "mapping_source": "users_table",
      "mapping_target": "user_profile_api",
      "policy_id": "policy_01HGQK8F3VWXYZ123456789ABC",
      "map_object": {
        "fields": {
          "user_id": "id",
          "username": "name",
          "email_address": "email"
        }
      },
      "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
    }
  ]
}
```

### 2. Show Mapping

**GET** `/{tenant_url}/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}`

Shows details of a specific mapping.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `mapping_id` (string, required): The mapping ID

#### Response
```json
{
  "mapping": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "mapping_id": "mapping_01HGQK8F3VWXYZ123456789ABC",
    "mapping_name": "user-profile-mapping",
    "mapping_description": "Maps user data to profile structure",
    "mapping_source_type": "postgresql",
    "mapping_target_type": "json",
    "mapping_source": "users_table",
    "mapping_target": "user_profile_api",
    "policy_id": "policy_01HGQK8F3VWXYZ123456789ABC",
    "map_object": {
      "fields": {
        "user_id": "id",
        "username": "name",
        "email_address": "email"
      },
      "transformations": [
        {
          "field": "created_at",
          "type": "date_format",
          "format": "ISO8601"
        }
      ]
    },
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  }
}
```

### 3. Add Mapping

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_id}/mappings`

Creates a new mapping within the workspace.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID

#### Request Body
```json
{
  "mapping_name": "order-summary-mapping",
  "mapping_description": "Maps order data to summary format",
  "mapping_source_type": "postgresql",
  "mapping_target_type": "json",
  "mapping_source": "orders_table",
  "mapping_target": "order_summary_api",
  "policy_id": "policy_01HGQK8F3VWXYZ123456789ABC",
  "map_object": {
    "fields": {
      "order_id": "id",
      "customer_name": "customer",
      "total_amount": "total"
    }
  }
}
```

#### Fields
- `mapping_name` (string, required): Name of the mapping
- `mapping_description` (string, required): Description of the mapping
- `mapping_source_type` (string, required): Type of the source system
- `mapping_target_type` (string, required): Type of the target system
- `mapping_source` (string, required): Source identifier
- `mapping_target` (string, required): Target identifier
- `policy_id` (string, optional): Associated policy ID
- `map_object` (object, optional): Mapping configuration object

#### Response
```json
{
  "message": "Mapping created successfully",
  "success": true,
  "mapping": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "mapping_id": "mapping_01HGQK8F3VWXYZ123456789ABC",
    "mapping_name": "order-summary-mapping",
    "mapping_description": "Maps order data to summary format",
    "policy_id": "policy_01HGQK8F3VWXYZ123456789ABC",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC",
    "mapping_rule_count": 0
  },
  "status": "success"
}
```

### 4. Add Database Mapping

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_name}/mappings/database`

Creates a new mapping between two databases with automatically generated mapping rules based on schema analysis and column matching.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_name` (string, required): The workspace name

#### Request Body
```json
{
  "mapping_name": "user-data-migration",
  "mapping_description": "Maps user data from legacy system to new system",
  "mapping_source_database_name": "legacy_users_db",
  "mapping_target_database_name": "new_users_db",
  "policy_id": "policy_01HGQK8F3VWXYZ123456789ABC"
}
```

#### Fields
- `mapping_name` (string, required): Name of the mapping
- `mapping_description` (string, required): Description of the mapping
- `mapping_source_database_name` (string, required): Name of the source database (must exist in the workspace)
- `mapping_target_database_name` (string, required): Name of the target database (must exist in the workspace)
- `policy_id` (string, optional): Associated policy ID

#### Response
```json
{
  "message": "Database mapping created successfully",
  "success": true,
  "mapping": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "mapping_id": "mapping_01HGQK8F3VWXYZ123456789ABC",
    "mapping_name": "user-data-migration",
    "mapping_description": "Maps user data from legacy system to new system",
    "policy_id": "policy_01HGQK8F3VWXYZ123456789ABC",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC",
    "mapping_rule_count": 15
  },
  "status": "success"
}
```

#### Features
- **Automatic Schema Analysis**: Analyzes both source and target database schemas
- **Intelligent Column Matching**: Uses the unified model service to match columns based on:
  - Name similarity
  - Data type compatibility
  - Column properties (nullable, primary key, etc.)
  - Classification and privileged data detection
- **Automatic Rule Generation**: Creates mapping rules for matched columns with:
  - Appropriate transformation names
  - Match confidence scores
  - Type compatibility information
- **Strongest Match Selection**: When multiple matches exist for a column, selects the strongest match based on confidence scores

#### Error Responses
- `400 Bad Request`: Missing required fields or invalid request body
- `404 Not Found`: Source or target database not found in the workspace
- `500 Internal Server Error`: Unified model service unavailable or other server errors

### 4. Modify Mapping

**PUT** `/{tenant_url}/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}`

Updates an existing mapping.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `mapping_id` (string, required): The mapping ID

#### Request Body
```json
{
  "mapping_name": "updated-mapping",
  "mapping_description": "Updated mapping description",
  "mapping_source_type": "mysql",
  "mapping_target_type": "xml",
  "map_object": {
    "fields": {
      "user_id": "identifier",
      "username": "display_name",
      "email_address": "contact_email"
    }
  }
}
```

#### Fields
All fields are optional:
- `mapping_name` (string): New name for the mapping
- `mapping_description` (string): New description
- `mapping_source_type` (string): Update source type
- `mapping_target_type` (string): Update target type
- `mapping_source` (string): Update source identifier
- `mapping_target` (string): Update target identifier
- `policy_id` (string): Update associated policy
- `map_object` (object): Update mapping configuration

#### Response
```json
{
  "message": "Mapping updated successfully",
  "success": true,
  "mapping": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "mapping_id": "mapping_01HGQK8F3VWXYZ123456789ABC",
    "mapping_name": "updated-mapping",
    "mapping_description": "Updated mapping description",
    "mapping_source_type": "mysql",
    "mapping_target_type": "xml",
    "map_object": {
      "fields": {
        "user_id": "identifier",
        "username": "display_name",
        "email_address": "contact_email"
      }
    },
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  },
  "status": "success"
}
```

### 5. Delete Mapping

**DELETE** `/{tenant_url}/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}`

Deletes a mapping.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `mapping_id` (string, required): The mapping ID

#### Response
```json
{
  "message": "Mapping deleted successfully",
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