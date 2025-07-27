# Client API Authentication Endpoints

This document describes the REST API authentication endpoints provided by the Client API service.

## Base URL
All endpoints are available under the `/api/v1/auth` prefix.

## Endpoints

### 1. Login
Authenticates a user and returns access and refresh tokens along with session information.

**Endpoint:** `POST /{tenant_url}/api/v1/auth/login`

**Request Body:**
```json
{
  "username": "string",
  "password": "string",
  "expiry_time_hours": "string (optional)",
  "session_name": "string (optional)",
  "user_agent": "string (optional)",
  "ip_address": "string (optional)",
  "platform": "string (optional)",
  "browser": "string (optional)",
  "operating_system": "string (optional)",
  "device_type": "string (optional)",
  "location": "string (optional)"
}
```

**Response:**
```json
{
  "profile": {
    "tenant_id": "string",
    "user_id": "string",
    "username": "string",
    "email": "string",
    "name": "string"
  },
  "access_token": "string",
  "refresh_token": "string",
  "session_id": "string",
  "status": "success"
}
```

### 2. Logout
Logs out a user by invalidating their refresh token.

**Endpoint:** `POST /{tenant_url}/api/v1/auth/logout`

**Request Body:**
```json
{
  "refresh_token": "string"
}
```

**Response:**
```json
{
  "message": "string",
  "success": true,
  "status": "success"
}
```

### 3. Get Profile
Retrieves the profile information for the authenticated user.

**Endpoint:** `GET /{tenant_url}/api/v1/auth/profile`

**Headers:**
- `Authorization: Bearer <access_token>` (required)

**Response:**
```json
{
  "profile": {
    "tenant_id": "string",
    "user_id": "string",
    "username": "string",
    "email": "string",
    "name": "string"
  }
}
```

### 4. Change Password
Changes a user's password. Requires authentication.

**Endpoint:** `POST /{tenant_url}/api/v1/auth/change-password`

**Headers:**
- `Authorization: Bearer <access_token>` (required)

**Request Body:**
```json
{
  "old_password": "string",
  "new_password": "string"
}
```

**Response:**
```json
{
  "message": "string",
  "success": true,
  "status": "success"
}
```

### 5. List Sessions
Lists all active sessions for the authenticated user.

**Endpoint:** `GET /{tenant_url}/api/v1/auth/sessions`

**Headers:**
- `Authorization: Bearer <access_token>` (required)

**Response:**
```json
{
  "sessions": [
    {
      "session_id": "string",
      "session_name": "string",
      "user_agent": "string",
      "ip_address": "string",
      "platform": "string",
      "browser": "string",
      "operating_system": "string",
      "device_type": "string",
      "location": "string",
      "last_activity": "string",
      "created": "string",
      "expires": "string",
      "is_current": true
    }
  ],
  "status": "success"
}
```

### 6. Logout Session
Logs out a specific session by session ID.

**Endpoint:** `POST /{tenant_url}/api/v1/auth/sessions/{session_id}/logout`

**Headers:**
- `Authorization: Bearer <access_token>` (required)

**Response:**
```json
{
  "message": "string",
  "success": true,
  "status": "success"
}
```

### 7. Logout All Sessions
Logs out all sessions for the authenticated user.

**Endpoint:** `POST /{tenant_url}/api/v1/auth/sessions/logout-all`

**Headers:**
- `Authorization: Bearer <access_token>` (required)

**Request Body (optional):**
```json
{
  "exclude_current": true
}
```

**Response:**
```json
{
  "sessions_logged_out": 3,
  "message": "string",
  "success": true,
  "status": "success"
}
```

### 8. Update Session Name
Updates the name of a specific session.

**Endpoint:** `PUT /{tenant_url}/api/v1/auth/sessions/{session_id}/name`

**Headers:**
- `Authorization: Bearer <access_token>` (required)

**Request Body:**
```json
{
  "session_name": "string"
}
```

**Response:**
```json
{
  "message": "string",
  "success": true,
  "status": "success"
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
- `401 Unauthorized`: Authentication failed
- `403 Forbidden`: Access denied
- `500 Internal Server Error`: Server error

## Headers
- `Content-Type: application/json` (required for all requests)
- `Authorization: Bearer <token>` (required for authenticated endpoints)

## Session Information
When logging in, you can provide additional session information to help identify and manage your sessions:

- `session_name`: A user-friendly name for the session (e.g., "My Desktop", "Mobile App")
- `user_agent`: The user agent string from the client (automatically extracted from headers if not provided)
- `ip_address`: The IP address of the client (automatically extracted if not provided)
- `platform`: The platform/OS (e.g., "Windows", "macOS", "iOS")
- `browser`: The browser name (e.g., "Chrome", "Safari", "Firefox")
- `operating_system`: The operating system (e.g., "Windows 11", "macOS 14.0")
- `device_type`: The type of device (e.g., "Desktop", "Mobile", "Tablet")
- `location`: Geographic location (e.g., "New York, NY")

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
  "service": "clientapi",
  "timestamp": "2024-01-01T00:00:00Z"
}
``` 