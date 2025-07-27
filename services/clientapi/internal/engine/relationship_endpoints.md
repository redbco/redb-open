# Relationship API Endpoints

This document describes the REST API endpoints for relationship management in the Client API service.

## Base URL

All relationship endpoints are nested under workspaces and follow this pattern:
```
/{tenant_url}/api/v1/workspaces/{workspace_id}/relationships
```

## Authentication

All endpoints require authentication via Bearer token in the Authorization header:
```
Authorization: Bearer <token>
```

## Endpoints

### 1. List Relationships

**GET** `/{tenant_url}/api/v1/workspaces/{workspace_id}/relationships`

Lists all relationships within a specific workspace.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID

#### Response
```json
{
  "relationships": [
    {
      "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
      "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
      "relationship_id": "rel_01HGQK8F3VWXYZ123456789ABC",
      "relationship_name": "user-orders-relationship",
      "relationship_description": "Links users to their orders",
      "relationship_type": "one-to-many",
      "relationship_source": "users_table",
      "relationship_target": "orders_table",
      "mapping_id": "mapping_01HGQK8F3VWXYZ123456789ABC",
      "policy_id": "policy_01HGQK8F3VWXYZ123456789ABC",
      "status_message": "Active relationship",
      "status": "active",
      "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
    }
  ]
}
```

### 2. Show Relationship

**GET** `/{tenant_url}/api/v1/workspaces/{workspace_id}/relationships/{relationship_id}`

Shows details of a specific relationship.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `relationship_id` (string, required): The relationship ID

#### Response
```json
{
  "relationship": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "relationship_id": "rel_01HGQK8F3VWXYZ123456789ABC",
    "relationship_name": "user-orders-relationship",
    "relationship_description": "Links users to their orders",
    "relationship_type": "one-to-many",
    "relationship_source": "users_table",
    "relationship_target": "orders_table",
    "mapping_id": "mapping_01HGQK8F3VWXYZ123456789ABC",
    "policy_id": "policy_01HGQK8F3VWXYZ123456789ABC",
    "status_message": "Active relationship with foreign key constraint",
    "status": "active",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  }
}
```

### 3. Add Relationship

**POST** `/{tenant_url}/api/v1/workspaces/{workspace_id}/relationships`

Creates a new relationship within the workspace.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID

#### Request Body
```json
{
  "relationship_name": "product-categories-relationship",
  "relationship_description": "Links products to their categories",
  "relationship_type": "many-to-one",
  "relationship_source": "products_table",
  "relationship_target": "categories_table",
  "mapping_id": "mapping_01HGQK8F3VWXYZ123456789DEF",
  "policy_id": "policy_01HGQK8F3VWXYZ123456789ABC"
}
```

#### Fields
- `relationship_name` (string, required): Name of the relationship
- `relationship_description` (string, required): Description of the relationship
- `relationship_type` (string, required): Type of relationship (one-to-one, one-to-many, many-to-one, many-to-many)
- `relationship_source` (string, required): Source entity/table
- `relationship_target` (string, required): Target entity/table
- `mapping_id` (string, required): Associated mapping ID
- `policy_id` (string, required): Associated policy ID

**Note**: The `owner_id` is automatically set from the authenticated user's profile.

#### Response
```json
{
  "message": "Relationship created successfully",
  "success": true,
  "relationship": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "relationship_id": "rel_01HGQK8F3VWXYZ123456789DEF",
    "relationship_name": "product-categories-relationship",
    "relationship_description": "Links products to their categories",
    "relationship_type": "many-to-one",
    "relationship_source": "products_table",
    "relationship_target": "categories_table",
    "mapping_id": "mapping_01HGQK8F3VWXYZ123456789DEF",
    "policy_id": "policy_01HGQK8F3VWXYZ123456789ABC",
    "status_message": "Relationship created",
    "status": "pending",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  },
  "status": "success"
}
```

### 4. Modify Relationship

**PUT** `/{tenant_url}/api/v1/workspaces/{workspace_id}/relationships/{relationship_id}`

Updates an existing relationship.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `relationship_id` (string, required): The relationship ID

#### Request Body
```json
{
  "relationship_name": "updated-relationship",
  "relationship_description": "Updated relationship description",
  "relationship_type": "one-to-one",
  "relationship_source": "updated_source_table",
  "relationship_target": "updated_target_table",
  "mapping_id": "mapping_01HGQK8F3VWXYZ123456789XYZ",
  "policy_id": "policy_01HGQK8F3VWXYZ123456789XYZ"
}
```

#### Fields
All fields are optional:
- `relationship_name` (string): New name for the relationship
- `relationship_description` (string): New description
- `relationship_type` (string): Update relationship type
- `relationship_source` (string): Update source entity
- `relationship_target` (string): Update target entity
- `mapping_id` (string): Update associated mapping
- `policy_id` (string): Update associated policy

#### Response
```json
{
  "message": "Relationship updated successfully",
  "success": true,
  "relationship": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "workspace_id": "ws_01HGQK8F3VWXYZ123456789ABC",
    "relationship_id": "rel_01HGQK8F3VWXYZ123456789ABC",
    "relationship_name": "updated-relationship",
    "relationship_description": "Updated relationship description",
    "relationship_type": "one-to-one",
    "relationship_source": "updated_source_table",
    "relationship_target": "updated_target_table",
    "mapping_id": "mapping_01HGQK8F3VWXYZ123456789XYZ",
    "policy_id": "policy_01HGQK8F3VWXYZ123456789XYZ",
    "status_message": "Relationship updated",
    "status": "active",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  },
  "status": "success"
}
```

### 5. Delete Relationship

**DELETE** `/{tenant_url}/api/v1/workspaces/{workspace_id}/relationships/{relationship_id}`

Deletes a relationship.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `workspace_id` (string, required): The workspace ID
- `relationship_id` (string, required): The relationship ID

#### Response
```json
{
  "message": "Relationship deleted successfully",
  "success": true,
  "status": "success"
}
```

## Relationship Types

The following relationship types are supported:

- **one-to-one**: Each record in the source relates to exactly one record in the target
- **one-to-many**: Each record in the source relates to multiple records in the target
- **many-to-one**: Multiple records in the source relate to one record in the target
- **many-to-many**: Records in both source and target can have multiple relationships

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