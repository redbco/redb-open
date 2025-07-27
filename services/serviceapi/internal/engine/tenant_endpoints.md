# Service API Tenant Endpoints

This document describes the REST API tenant endpoints provided by the Service API service.

## Base URL
All endpoints are available under the `/api/v1/tenants` prefix.

## Endpoints

### 1. List Tenants
Retrieves a list of all tenants in the system.

**Endpoint:** `GET /api/v1/tenants`

**Response:**
```json
{
  "tenants": [
    {
      "tenant_id": "string",
      "tenant_name": "string",
      "tenant_description": "string",
      "tenant_url": "string"
    }
  ]
}
```

### 2. Show Tenant
Retrieves detailed information about a specific tenant.

**Endpoint:** `GET /api/v1/tenants/{tenant_id}`

**Path Parameters:**
- `tenant_id` (string, required): The unique identifier of the tenant

**Response:**
```json
{
  "tenant": {
    "tenant_id": "string",
    "tenant_name": "string",
    "tenant_description": "string",
    "tenant_url": "string"
  }
}
```

### 3. Add Tenant
Creates a new tenant in the system.

**Endpoint:** `POST /api/v1/tenants`

**Request Body:**
```json
{
  "tenant_name": "string",
  "tenant_url": "string",
  "tenant_description": "string",
  "user_email": "string",
  "user_password": "string"
}
```

**Response:**
```json
{
  "message": "string",
  "success": true,
  "tenant": {
    "tenant_id": "string",
    "tenant_name": "string",
    "tenant_description": "string",
    "tenant_url": "string"
  }
}
```

### 4. Modify Tenant
Updates an existing tenant's information.

**Endpoint:** `PUT /api/v1/tenants/{tenant_id}`

**Path Parameters:**
- `tenant_id` (string, required): The unique identifier of the tenant

**Request Body:**
```json
{
  "tenant_name": "string (optional)",
  "tenant_description": "string (optional)"
}
```

**Response:**
```json
{
  "message": "string",
  "success": true,
  "tenant": {
    "tenant_id": "string",
    "tenant_name": "string",
    "tenant_description": "string",
    "tenant_url": "string"
  }
}
```

### 5. Delete Tenant
Removes a tenant from the system.

**Endpoint:** `DELETE /api/v1/tenants/{tenant_id}`

**Path Parameters:**
- `tenant_id` (string, required): The unique identifier of the tenant

**Response:**
```json
{
  "message": "string",
  "success": true
}
```

## Error Responses
All endpoints return error responses in the following format:

```json
{
  "error": "string",
  "message": "string",
  "status": "error"
}
```

## Status Codes
- `200 OK`: Successful operation
- `400 Bad Request`: Invalid request data
- `404 Not Found`: Tenant not found
- `409 Conflict`: Tenant already exists
- `500 Internal Server Error`: Server error
- `503 Service Unavailable`: Service temporarily unavailable
- `408 Request Timeout`: Request timeout

## Headers
- `Content-Type: application/json` (required for all requests)

## Tenant Object
The tenant object contains the following fields:

- `tenant_id`: Unique identifier for the tenant
- `tenant_name`: Human-readable name for the tenant
- `tenant_description`: Optional description of the tenant
- `tenant_url`: URL identifier for the tenant (used in API paths)

## CORS
The API supports CORS with the following headers:
- `Access-Control-Allow-Origin: *`
- `Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS`
- `Access-Control-Allow-Headers: Content-Type, Authorization`

## Health Check
The service provides a health check endpoint:

**Endpoint:** `GET /health`

**Response:**
```json
{
  "status": "healthy",
  "service": "serviceapi",
  "timestamp": "2024-01-01T00:00:00Z"
}
```

## Examples

### List all tenants
```bash
curl -X GET http://localhost:8081/api/v1/tenants
```

### Get a specific tenant
```bash
curl -X GET http://localhost:8081/api/v1/tenants/tenant-123
```

### Create a new tenant
```bash
curl -X POST http://localhost:8081/api/v1/tenants \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_name": "My Company",
    "tenant_description": "A sample tenant for demonstration"
  }'
```

### Update a tenant
```bash
curl -X PUT http://localhost:8081/api/v1/tenants/tenant-123 \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_name": "Updated Company Name",
    "tenant_description": "Updated description"
  }'
```

### Delete a tenant
```bash
curl -X DELETE http://localhost:8081/api/v1/tenants/tenant-123
```

## Notes

1. **Tenant Isolation**: The Service API operates at the system level and manages tenants globally. Unlike the Client API which operates within a specific tenant context, the Service API can manage all tenants.

2. **No Authentication Required**: The Service API endpoints do not require authentication as they are designed for system administration and management.

3. **Tenant URL**: The `tenant_url` field is automatically generated and is used as the URL path component for tenant-specific operations in the Client API.

4. **Backwards Compatibility**: The legacy `/query` endpoint is maintained for backwards compatibility but is not part of the standard tenant management API. 