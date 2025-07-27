# Transformation API Endpoints

This document describes the REST API endpoints for transformation management in the Client API service.

## Base URL

All transformation endpoints are tenant-level and follow this pattern:
```
/{tenant_url}/api/v1/transformations
```

## Authentication

All endpoints require authentication via Bearer token in the Authorization header:
```
Authorization: Bearer <token>
```

## Endpoints

### 1. List Transformations

**GET** `/{tenant_url}/api/v1/transformations`

Lists all transformations within a tenant.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL

#### Response
```json
{
  "transformations": [
    {
      "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
      "transformation_id": "trans_01HGQK8F3VWXYZ123456789ABC",
      "transformation_name": "data-sanitization",
      "transformation_description": "Sanitizes sensitive data fields",
      "transformation_type": "data_mask",
      "transformation_version": "1.0.0",
      "transformation_function": "function sanitize(data) { return data.replace(/\\d{4}-\\d{4}-\\d{4}-\\d{4}/, 'XXXX-XXXX-XXXX-XXXX'); }",
      "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
    }
  ]
}
```

### 2. Show Transformation

**GET** `/{tenant_url}/api/v1/transformations/{transformation_id}`

Shows details of a specific transformation.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `transformation_id` (string, required): The transformation ID

#### Response
```json
{
  "transformation": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "transformation_id": "trans_01HGQK8F3VWXYZ123456789ABC",
    "transformation_name": "data-sanitization",
    "transformation_description": "Sanitizes sensitive data fields including credit cards and SSNs",
    "transformation_type": "data_mask",
    "transformation_version": "1.0.0",
    "transformation_function": "function sanitize(data) {\n  data = data.replace(/\\d{4}-\\d{4}-\\d{4}-\\d{4}/, 'XXXX-XXXX-XXXX-XXXX');\n  data = data.replace(/\\d{3}-\\d{2}-\\d{4}/, 'XXX-XX-XXXX');\n  return data;\n}",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  }
}
```

### 3. Add Transformation

**POST** `/{tenant_url}/api/v1/transformations`

Creates a new transformation function.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL

#### Request Body
```json
{
  "transformation_name": "email-obfuscation",
  "transformation_description": "Obfuscates email addresses for privacy",
  "transformation_type": "data_obfuscate",
  "transformation_version": "1.0.0",
  "transformation_function": "function obfuscateEmail(email) {\n  const [user, domain] = email.split('@');\n  const obscuredUser = user.charAt(0) + '*'.repeat(user.length - 2) + user.charAt(user.length - 1);\n  return obscuredUser + '@' + domain;\n}"
}
```

#### Fields
- `transformation_name` (string, required): Name of the transformation
- `transformation_description` (string, required): Description of the transformation
- `transformation_type` (string, required): Type of transformation (data_mask, data_obfuscate, format_convert, validate, etc.)
- `transformation_version` (string, required): Version of the transformation
- `transformation_function` (string, required): JavaScript function code for the transformation

**Note**: The `owner_id` is automatically set from the authenticated user's profile.

#### Response
```json
{
  "message": "Transformation created successfully",
  "success": true,
  "transformation": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "transformation_id": "trans_01HGQK8F3VWXYZ123456789DEF",
    "transformation_name": "email-obfuscation",
    "transformation_description": "Obfuscates email addresses for privacy",
    "transformation_type": "data_obfuscate",
    "transformation_version": "1.0.0",
    "transformation_function": "function obfuscateEmail(email) {\n  const [user, domain] = email.split('@');\n  const obscuredUser = user.charAt(0) + '*'.repeat(user.length - 2) + user.charAt(user.length - 1);\n  return obscuredUser + '@' + domain;\n}",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  },
  "status": "success"
}
```

### 4. Modify Transformation

**PUT** `/{tenant_url}/api/v1/transformations/{transformation_id}`

Updates an existing transformation.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `transformation_id` (string, required): The transformation ID

#### Request Body
```json
{
  "transformation_name": "enhanced-email-obfuscation",
  "transformation_description": "Enhanced email obfuscation with domain masking",
  "transformation_type": "data_obfuscate",
  "transformation_version": "1.1.0",
  "transformation_function": "function obfuscateEmail(email) {\n  const [user, domain] = email.split('@');\n  const obscuredUser = user.charAt(0) + '*'.repeat(user.length - 2) + user.charAt(user.length - 1);\n  const [domainName, tld] = domain.split('.');\n  const obscuredDomain = domainName.charAt(0) + '*'.repeat(domainName.length - 2) + domainName.charAt(domainName.length - 1);\n  return obscuredUser + '@' + obscuredDomain + '.' + tld;\n}"
}
```

#### Fields
All fields are optional:
- `transformation_name` (string): New name for the transformation
- `transformation_description` (string): New description
- `transformation_type` (string): Update transformation type
- `transformation_version` (string): Update version
- `transformation_function` (string): Update function code

#### Response
```json
{
  "message": "Transformation updated successfully",
  "success": true,
  "transformation": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "transformation_id": "trans_01HGQK8F3VWXYZ123456789ABC",
    "transformation_name": "enhanced-email-obfuscation",
    "transformation_description": "Enhanced email obfuscation with domain masking",
    "transformation_type": "data_obfuscate",
    "transformation_version": "1.1.0",
    "transformation_function": "function obfuscateEmail(email) {\n  const [user, domain] = email.split('@');\n  const obscuredUser = user.charAt(0) + '*'.repeat(user.length - 2) + user.charAt(user.length - 1);\n  const [domainName, tld] = domain.split('.');\n  const obscuredDomain = domainName.charAt(0) + '*'.repeat(domainName.length - 2) + domainName.charAt(domainName.length - 1);\n  return obscuredUser + '@' + obscuredDomain + '.' + tld;\n}",
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  },
  "status": "success"
}
```

### 5. Delete Transformation

**DELETE** `/{tenant_url}/api/v1/transformations/{transformation_id}`

Deletes a transformation.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `transformation_id` (string, required): The transformation ID

#### Response
```json
{
  "message": "Transformation deleted successfully",
  "success": true,
  "status": "success"
}
```

## Transformation Types

The following transformation types are supported:

- **data_mask**: Masks sensitive data (credit cards, SSNs, etc.)
- **data_obfuscate**: Obfuscates data while maintaining format
- **format_convert**: Converts data between formats (JSON to XML, etc.)
- **validate**: Validates data against rules or schemas
- **normalize**: Normalizes data to standard formats
- **aggregate**: Aggregates multiple data points
- **filter**: Filters data based on criteria
- **enrich**: Enriches data with additional information

## Function Requirements

Transformation functions must:
- Be valid JavaScript functions
- Accept input data as the first parameter
- Return transformed data
- Handle errors gracefully
- Be deterministic (same input produces same output)

## Error Handling

All endpoints return appropriate HTTP status codes:

- `200 OK`: Successful GET/PUT operations
- `201 Created`: Successful POST operations
- `400 Bad Request`: Invalid request parameters or body, invalid function syntax
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