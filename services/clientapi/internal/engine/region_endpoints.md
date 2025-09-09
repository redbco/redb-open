# Region API Endpoints

This document describes the REST API endpoints for region management in the Client API service.

## Base URL

All region endpoints are at the tenant level and follow this pattern:
```
/{tenant_url}/api/v1/regions
```

## Authentication

All endpoints require authentication via Bearer token in the Authorization header:
```
Authorization: Bearer <token>
```

## Endpoints

### 1. List Regions

**GET** `/{tenant_url}/api/v1/regions`

Lists all regions for a tenant.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL

#### Response
```json
{
  "regions": [
    {
      "region_id": "region_01HGQK8F3VWXYZ123456789ABC",
      "region_name": "us-east-1",
      "region_description": "US East (N. Virginia)",
      "region_location": "Virginia, USA",
      "region_latitude": 38.1316,
      "region_longitude": -78.2173,
      "region_type": "aws",
      "node_count": 3,
      "instance_count": 10,
      "database_count": 15,
      "status": "healthy",
      "global_region": false,
      "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
    }
  ]
}
```

### 2. Show Region

**GET** `/{tenant_url}/api/v1/regions/{region_name}`

Shows details of a specific region.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `region_name` (string, required): The region name

#### Response
```json
{
  "region": {
    "region_id": "region_01HGQK8F3VWXYZ123456789ABC",
    "region_name": "us-east-1",
    "region_description": "US East (N. Virginia)",
    "region_location": "Virginia, USA",
    "region_latitude": 38.1316,
    "region_longitude": -78.2173,
    "region_type": "aws",
    "node_count": 3,
    "instance_count": 10,
    "database_count": 15,
    "status": "healthy",
    "global_region": false,
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  }
}
```

### 3. Add Region

**POST** `/{tenant_url}/api/v1/regions`

Creates a new region.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL

#### Request Body
```json
{
  "region_name": "us-west-2",
  "region_type": "aws",
  "region_description": "US West (Oregon)",
  "region_location": "Oregon, USA",
  "region_latitude": 45.5152,
  "region_longitude": -122.6784
}
```

#### Fields
- `region_name` (string, required): Name of the region
- `region_type` (string, required): Type of region (e.g., "aws", "azure", "gcp", "on-premise")
- `region_description` (string, optional): Description of the region
- `region_location` (string, optional): Human-readable location
- `region_latitude` (number, optional): Latitude coordinates
- `region_longitude` (number, optional): Longitude coordinates

**Note**: The `owner_id` is automatically set from the authenticated user's profile and cannot be specified in the request body.

#### Response
```json
{
  "message": "Region created successfully",
  "success": true,
  "region": {
    "region_id": "region_01HGQK8F3VWXYZ123456789ABC",
    "region_name": "us-west-2",
    "region_description": "US West (Oregon)",
    "region_location": "Oregon, USA",
    "region_latitude": 45.5152,
    "region_longitude": -122.6784,
    "region_type": "aws",
    "node_count": 0,
    "instance_count": 0,
    "database_count": 0,
    "status": "pending",
    "global_region": false,
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  },
  "status": "success"
}
```

### 4. Modify Region

**PUT** `/{tenant_url}/api/v1/regions/{region_name}`

Updates an existing region.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `region_name` (string, required): The region name

#### Request Body
```json
{
  "region_name_new": "us-west-2-updated",
  "region_description": "US West (Oregon) - Updated",
  "region_location": "Portland, Oregon, USA",
  "region_latitude": 45.5152,
  "region_longitude": -122.6784
}
```

#### Fields
All fields are optional:
- `region_name` (string): New name for the region
- `region_description` (string): New description
- `region_location` (string): New location
- `region_latitude` (number): New latitude coordinates
- `region_longitude` (number): New longitude coordinates

#### Response
```json
{
  "message": "Region updated successfully",
  "success": true,
  "region": {
    "region_id": "region_01HGQK8F3VWXYZ123456789ABC",
    "region_name": "us-west-2-updated",
    "region_description": "US West (Oregon) - Updated",
    "region_location": "Portland, Oregon, USA",
    "region_latitude": 45.5152,
    "region_longitude": -122.6784,
    "region_type": "aws",
    "node_count": 0,
    "instance_count": 0,
    "database_count": 0,
    "status": "healthy",
    "global_region": false,
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  },
  "status": "success"
}
```

### 5. Delete Region

**DELETE** `/{tenant_url}/api/v1/regions/{region_name}`

Deletes a region.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `region_name` (string, required): The region name

#### Response
```json
{
  "message": "Region deleted successfully",
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
- **404 Not Found**: Region or tenant not found
- **409 Conflict**: Region name already exists
- **500 Internal Server Error**: Server-side error

## Region Types

The following region types are supported:
- `aws` - Amazon Web Services
- `azure` - Microsoft Azure
- `gcp` - Google Cloud Platform
- `on-premise` - On-premise infrastructure
- `hybrid` - Hybrid cloud setup
- `edge` - Edge computing location

## Examples

### Creating an AWS region
```bash
curl -X POST "https://api.example.com/mycompany/api/v1/regions" \
  -H "Authorization: Bearer your-token-here" \
  -H "Content-Type: application/json" \
  -d '{
    "region_name": "eu-west-1",
    "region_type": "aws",
    "region_description": "Europe (Ireland)",
    "region_location": "Dublin, Ireland",
    "region_latitude": 53.3498,
    "region_longitude": -6.2603
  }'
```

### Listing all regions
```bash
curl -X GET "https://api.example.com/mycompany/api/v1/regions" \
  -H "Authorization: Bearer your-token-here"
```

### Updating region coordinates
```bash
curl -X PUT "https://api.example.com/mycompany/api/v1/regions/region_123" \
  -H "Authorization: Bearer your-token-here" \
  -H "Content-Type: application/json" \
  -d '{
    "region_latitude": 53.3498,
    "region_longitude": -6.2603
  }'
```

## Notes

- Global regions (managed at the system level) are not accessible through this client API
- Only tenant-specific regions can be managed through these endpoints
- Global region management will be available through the API
- Deleting a region with active nodes, instances, or databases may be prevented by the system 