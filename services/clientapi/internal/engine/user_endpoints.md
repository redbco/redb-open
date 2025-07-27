# User API Endpoints

This document describes the user management endpoints available in the Client API service.

## Base URL

All endpoints are prefixed with: `/{tenant_url}/api/v1/users`

## Authentication

All user endpoints require authentication via Bearer token in the Authorization header:

```
Authorization: Bearer <access_token>
```

## Authorization

Users must have appropriate permissions for user operations:
- `list_users` - List all users in a tenant
- `read_user` - View a specific user 
- `create_user` - Create new users
- `update_user` - Modify existing users
- `delete_user` - Delete users

## Endpoints

### List Users

**GET** `/{tenant_url}/api/v1/users`

Lists all users in the authenticated user's tenant.

**Response:**
```json
{
  "users": [
    {
      "tenant_id": "tenant-12345",
      "user_id": "user-67890",
      "user_name": "John Doe",
      "user_email": "john.doe@example.com",
      "user_enabled": true
    }
  ]
}
```

### Show User

**GET** `/{tenant_url}/api/v1/users/{user_id}`

Retrieves details for a specific user.

**Parameters:**
- `user_id` (path) - The user ID

**Response:**
```json
{
  "user": {
    "tenant_id": "tenant-12345",
    "user_id": "user-67890",
    "user_name": "John Doe",
    "user_email": "john.doe@example.com",
    "user_enabled": true
  }
}
```

### Create User

**POST** `/{tenant_url}/api/v1/users`

Creates a new user.

**Request Body:**
```json
{
  "user_name": "Jane Smith",
  "user_email": "jane.smith@example.com",
  "user_password": "securepassword123",
  "user_enabled": true
}
```

**Required Fields:**
- `user_name` - Full name of the user
- `user_email` - Email address (must be globally unique)
- `user_password` - Initial password for the user

**Optional Fields:**
- `user_enabled` - Whether the user account is enabled (default: true)

**Response:**
```json
{
  "message": "User created successfully",
  "success": true,
  "user": {
    "tenant_id": "tenant-12345",
    "user_id": "user-54321",
    "user_name": "Jane Smith",
    "user_email": "jane.smith@example.com",
    "user_enabled": true
  },
  "status": "created"
}
```

### Update User

**PUT** `/{tenant_url}/api/v1/users/{user_id}`

Updates an existing user.

**Parameters:**
- `user_id` (path) - The user ID

**Request Body:**
```json
{
  "user_name": "Jane Smith Updated",
  "user_email": "jane.updated@example.com",
  "user_password": "newsecurepassword123",
  "user_enabled": false
}
```

**Optional Fields:**
- `user_name` - New full name for the user
- `user_email` - New email address (must be globally unique)
- `user_password` - New password for the user
- `user_enabled` - Whether the user account is enabled

**Response:**
```json
{
  "message": "User updated successfully",
  "success": true,
  "user": {
    "tenant_id": "tenant-12345",
    "user_id": "user-54321",
    "user_name": "Jane Smith Updated",
    "user_email": "jane.updated@example.com",
    "user_enabled": false
  },
  "status": "updated"
}
```

### Delete User

**DELETE** `/{tenant_url}/api/v1/users/{user_id}`

Deletes a user.

**Parameters:**
- `user_id` (path) - The user ID

**Response:**
```json
{
  "message": "User deleted successfully",
  "success": true,
  "status": "deleted"
}
```

## Error Responses

All endpoints may return error responses in the following format:

```json
{
  "error": "Detailed error message",
  "message": "User-friendly error message",
  "status": "error"
}
```

### Common HTTP Status Codes

- `200 OK` - Request successful
- `201 Created` - Resource created successfully
- `400 Bad Request` - Invalid request data
- `401 Unauthorized` - Authentication required or invalid
- `403 Forbidden` - Insufficient permissions
- `404 Not Found` - Resource not found
- `409 Conflict` - Resource already exists (e.g., email already in use)
- `500 Internal Server Error` - Server error
- `503 Service Unavailable` - Core service unavailable

### Example Error Response

```json
{
  "error": "user not found", 
  "message": "The specified user does not exist",
  "status": "error"
}
```

## Important Notes

### Email Uniqueness
- User email addresses must be globally unique across all tenants
- Attempting to create a user with an email that already exists will result in a 409 Conflict error

### Password Security
- Passwords are hashed before storage
- Password fields are never returned in API responses for security reasons
- When updating a user, only provide the password field if you want to change it

### Tenant Isolation
- Users are tenant-specific and can only be managed within their assigned tenant
- Users cannot be moved between tenants through these endpoints

### User Status
- Disabled users cannot authenticate or access the system
- Users can be enabled/disabled without deleting their account

## Rate Limiting

All endpoints are subject to rate limiting. If rate limits are exceeded, a `429 Too Many Requests` response will be returned.

## CORS

The API supports Cross-Origin Resource Sharing (CORS) with the following headers:
- `Access-Control-Allow-Origin: *`
- `Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS`
- `Access-Control-Allow-Headers: Content-Type, Authorization` 