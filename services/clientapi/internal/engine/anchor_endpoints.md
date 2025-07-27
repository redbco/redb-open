# Anchor API Endpoints

This document describes the REST API endpoints for anchor management in the Client API service.

## Base URL

All anchor endpoints are tenant-level and follow this pattern:
```
/{tenant_url}/api/v1/anchors
```

## Authentication

All endpoints require authentication via Bearer token in the Authorization header:
```
Authorization: Bearer <token>
```

## Endpoints

### 1. List Anchors

**GET** `/{tenant_url}/api/v1/anchors`

Lists all anchors within a tenant.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL

#### Response
```json
{
  "anchors": [
    {
      "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
      "anchor_id": "anchor_01HGQK8F3VWXYZ123456789ABC",
      "anchor_name": "production-anchor",
      "anchor_description": "Production anchor node",
      "anchor_platform": "linux",
      "anchor_version": "1.0.0",
      "ip_address": "192.168.1.200",
      "node_id": "node_01HGQK8F3VWXYZ123456789ABC",
      "status": "healthy",
      "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
    }
  ]
}
```

### 2. Show Anchor

**GET** `/{tenant_url}/api/v1/anchors/{anchor_id}`

Shows details of a specific anchor.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `anchor_id` (string, required): The anchor ID

#### Response
```json
{
  "anchor": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "anchor_id": "anchor_01HGQK8F3VWXYZ123456789ABC",
    "anchor_name": "production-anchor",
    "anchor_description": "Production anchor node",
    "anchor_platform": "linux",
    "anchor_version": "1.0.0",
    "ip_address": "192.168.1.200",
    "node_id": "node_01HGQK8F3VWXYZ123456789ABC",
    "status": "healthy",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  }
}
```

### 3. Add Anchor

**POST** `/{tenant_url}/api/v1/anchors`

Creates a new anchor.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL

#### Request Body
```json
{
  "anchor_name": "staging-anchor",
  "anchor_description": "Staging anchor node",
  "anchor_platform": "linux",
  "anchor_version": "1.0.0",
  "ip_address": "192.168.1.201",
  "node_id": "node_01HGQK8F3VWXYZ123456789ABC",
  "public_key": "ssh-rsa AAAAB3NzaC1yc2E...",
  "private_key": "-----BEGIN PRIVATE KEY-----..."
}
```

#### Fields
- `anchor_name` (string, required): Name of the anchor
- `anchor_description` (string, optional): Description of the anchor
- `anchor_platform` (string, required): Platform (e.g., linux, windows)
- `anchor_version` (string, required): Version of the anchor software
- `ip_address` (string, required): IP address of the anchor
- `node_id` (string, required): ID of the node this anchor connects to
- `public_key` (string, required): SSH public key for authentication
- `private_key` (string, required): SSH private key for authentication

**Note**: The `owner_id` is automatically set from the authenticated user's profile.

#### Response
```json
{
  "message": "Anchor created successfully",
  "success": true,
  "anchor": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "anchor_id": "anchor_01HGQK8F3VWXYZ123456789ABC",
    "anchor_name": "staging-anchor",
    "anchor_description": "Staging anchor node",
    "anchor_platform": "linux",
    "anchor_version": "1.0.0",
    "ip_address": "192.168.1.201",
    "node_id": "node_01HGQK8F3VWXYZ123456789ABC",
    "status": "pending",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  },
  "status": "success"
}
```

### 4. Modify Anchor

**PUT** `/{tenant_url}/api/v1/anchors/{anchor_id}`

Updates an existing anchor.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `anchor_id` (string, required): The anchor ID

#### Request Body
```json
{
  "anchor_name": "updated-anchor",
  "anchor_description": "Updated anchor description",
  "anchor_platform": "linux",
  "anchor_version": "1.1.0",
  "ip_address": "192.168.1.202",
  "node_id": "node_01HGQK8F3VWXYZ123456789ABC"
}
```

#### Fields
All fields are optional:
- `anchor_name` (string): New name for the anchor
- `anchor_description` (string): New description
- `anchor_platform` (string): Update platform
- `anchor_version` (string): Update version
- `ip_address` (string): Update IP address
- `node_id` (string): Update connected node ID
- `public_key` (string): Update public key
- `private_key` (string): Update private key

#### Response
```json
{
  "message": "Anchor updated successfully",
  "success": true,
  "anchor": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "anchor_id": "anchor_01HGQK8F3VWXYZ123456789ABC",
    "anchor_name": "updated-anchor",
    "anchor_description": "Updated anchor description",
    "anchor_platform": "linux",
    "anchor_version": "1.1.0",
    "ip_address": "192.168.1.202",
    "node_id": "node_01HGQK8F3VWXYZ123456789ABC",
    "status": "healthy",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  },
  "status": "success"
}
```

### 5. Delete Anchor

**DELETE** `/{tenant_url}/api/v1/anchors/{anchor_id}`

Deletes an anchor.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `anchor_id` (string, required): The anchor ID

#### Response
```json
{
  "message": "Anchor deleted successfully",
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