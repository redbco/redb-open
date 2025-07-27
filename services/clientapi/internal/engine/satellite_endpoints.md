# Satellite API Endpoints

This document describes the REST API endpoints for satellite management in the Client API service.

## Base URL

All satellite endpoints are tenant-level and follow this pattern:
```
/{tenant_url}/api/v1/satellites
```

## Authentication

All endpoints require authentication via Bearer token in the Authorization header:
```
Authorization: Bearer <token>
```

## Endpoints

### 1. List Satellites

**GET** `/{tenant_url}/api/v1/satellites`

Lists all satellites within a tenant.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL

#### Response
```json
{
  "satellites": [
    {
      "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
      "satellite_id": "sat_01HGQK8F3VWXYZ123456789ABC",
      "satellite_name": "production-satellite",
      "satellite_description": "Production satellite node",
      "satellite_platform": "linux",
      "satellite_version": "1.0.0",
      "ip_address": "192.168.1.100",
      "node_id": "node_01HGQK8F3VWXYZ123456789ABC",
      "status": "healthy",
      "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
    }
  ]
}
```

### 2. Show Satellite

**GET** `/{tenant_url}/api/v1/satellites/{satellite_id}`

Shows details of a specific satellite.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `satellite_id` (string, required): The satellite ID

#### Response
```json
{
  "satellite": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "satellite_id": "sat_01HGQK8F3VWXYZ123456789ABC",
    "satellite_name": "production-satellite",
    "satellite_description": "Production satellite node",
    "satellite_platform": "linux",
    "satellite_version": "1.0.0",
    "ip_address": "192.168.1.100",
    "node_id": "node_01HGQK8F3VWXYZ123456789ABC",
    "status": "healthy",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  }
}
```

### 3. Add Satellite

**POST** `/{tenant_url}/api/v1/satellites`

Creates a new satellite.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL

#### Request Body
```json
{
  "satellite_name": "new-satellite",
  "satellite_description": "New satellite node",
  "satellite_platform": "linux",
  "satellite_version": "1.0.0",
  "ip_address": "192.168.1.101",
  "node_id": "node_01HGQK8F3VWXYZ123456789ABC",
  "public_key": "ssh-rsa AAAAB3NzaC1yc2E...",
  "private_key": "-----BEGIN PRIVATE KEY-----..."
}
```

#### Fields
- `satellite_name` (string, required): Name of the satellite
- `satellite_description` (string, optional): Description of the satellite
- `satellite_platform` (string, required): Platform (e.g., linux, windows)
- `satellite_version` (string, required): Version of the satellite software
- `ip_address` (string, required): IP address of the satellite
- `node_id` (string, required): ID of the node this satellite connects to
- `public_key` (string, required): SSH public key for authentication
- `private_key` (string, required): SSH private key for authentication

**Note**: The `owner_id` is automatically set from the authenticated user's profile and cannot be specified in the request body.

#### Response
```json
{
  "message": "Satellite created successfully",
  "success": true,
  "satellite": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "satellite_id": "sat_01HGQK8F3VWXYZ123456789ABC",
    "satellite_name": "new-satellite",
    "satellite_description": "New satellite node",
    "satellite_platform": "linux",
    "satellite_version": "1.0.0",
    "ip_address": "192.168.1.101",
    "node_id": "node_01HGQK8F3VWXYZ123456789ABC",
    "status": "pending",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  },
  "status": "success"
}
```

### 4. Modify Satellite

**PUT** `/{tenant_url}/api/v1/satellites/{satellite_id}`

Updates an existing satellite.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `satellite_id` (string, required): The satellite ID

#### Request Body
```json
{
  "satellite_name": "updated-satellite",
  "satellite_description": "Updated satellite description",
  "satellite_platform": "linux",
  "satellite_version": "1.1.0",
  "ip_address": "192.168.1.102",
  "node_id": "node_01HGQK8F3VWXYZ123456789ABC",
  "public_key": "ssh-rsa AAAAB3NzaC1yc2E...",
  "private_key": "-----BEGIN PRIVATE KEY-----..."
}
```

#### Fields
All fields are optional:
- `satellite_name` (string): New name for the satellite
- `satellite_description` (string): New description
- `satellite_platform` (string): Update platform
- `satellite_version` (string): Update version
- `ip_address` (string): Update IP address
- `node_id` (string): Update connected node ID
- `public_key` (string): Update public key
- `private_key` (string): Update private key

#### Response
```json
{
  "message": "Satellite updated successfully",
  "success": true,
  "satellite": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "satellite_id": "sat_01HGQK8F3VWXYZ123456789ABC",
    "satellite_name": "updated-satellite",
    "satellite_description": "Updated satellite description",
    "satellite_platform": "linux",
    "satellite_version": "1.1.0",
    "ip_address": "192.168.1.102",
    "node_id": "node_01HGQK8F3VWXYZ123456789ABC",
    "status": "healthy",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  },
  "status": "success"
}
```

### 5. Delete Satellite

**DELETE** `/{tenant_url}/api/v1/satellites/{satellite_id}`

Deletes a satellite.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `satellite_id` (string, required): The satellite ID

#### Response
```json
{
  "message": "Satellite deleted successfully",
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