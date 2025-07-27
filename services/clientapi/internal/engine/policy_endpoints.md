# Policy API Endpoints

This document describes the REST API endpoints for policy management in the Client API service.

## Base URL

All policy endpoints are tenant-level and follow this pattern:
```
/{tenant_url}/api/v1/policies
```

## Authentication

All endpoints require authentication via Bearer token in the Authorization header:
```
Authorization: Bearer <token>
```

## Endpoints

### 1. List Policies

**GET** `/{tenant_url}/api/v1/policies`

Lists all policies within a tenant.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL

#### Response
```json
{
  "policies": [
    {
      "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
      "policy_id": "policy_01HGQK8F3VWXYZ123456789ABC",
      "policy_name": "data-privacy-policy",
      "policy_description": "Ensures compliance with data privacy regulations",
      "policy_object": {
        "rules": [
          {
            "type": "data_retention",
            "duration": "365d",
            "applies_to": ["user_data", "transaction_logs"]
          },
          {
            "type": "access_control",
            "minimum_role": "data_analyst",
            "applies_to": ["sensitive_fields"]
          }
        ],
        "enforcement": "strict"
      },
      "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
    }
  ]
}
```

### 2. Show Policy

**GET** `/{tenant_url}/api/v1/policies/{policy_id}`

Shows details of a specific policy.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `policy_id` (string, required): The policy ID

#### Response
```json
{
  "policy": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "policy_id": "policy_01HGQK8F3VWXYZ123456789ABC",
    "policy_name": "data-privacy-policy",
    "policy_description": "Comprehensive data privacy policy ensuring GDPR and CCPA compliance",
    "policy_object": {
      "version": "1.0",
      "effective_date": "2023-01-01",
      "rules": [
        {
          "id": "retention_rule_001",
          "type": "data_retention",
          "duration": "365d",
          "applies_to": ["user_data", "transaction_logs"],
          "auto_delete": true
        },
        {
          "id": "access_rule_001",
          "type": "access_control",
          "minimum_role": "data_analyst",
          "applies_to": ["sensitive_fields"],
          "conditions": ["authenticated", "authorized"]
        },
        {
          "id": "encryption_rule_001",
          "type": "encryption",
          "algorithm": "AES-256",
          "applies_to": ["pii_fields"],
          "key_rotation": "90d"
        }
      ],
      "enforcement": "strict",
      "notifications": {
        "violations": true,
        "email": "compliance@company.com"
      }
    },
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  }
}
```

### 3. Add Policy

**POST** `/{tenant_url}/api/v1/policies`

Creates a new policy within the tenant.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL

#### Request Body
```json
{
  "policy_name": "backup-retention-policy",
  "policy_description": "Defines backup retention schedules and procedures",
  "policy_object": {
    "version": "1.0",
    "effective_date": "2023-12-01",
    "rules": [
      {
        "id": "daily_backup_rule",
        "type": "backup_schedule",
        "frequency": "daily",
        "retention": "30d",
        "applies_to": ["production_databases"]
      },
      {
        "id": "weekly_backup_rule",
        "type": "backup_schedule",
        "frequency": "weekly",
        "retention": "12w",
        "applies_to": ["production_databases"]
      }
    ],
    "enforcement": "automated"
  }
}
```

#### Fields
- `policy_name` (string, required): Name of the policy
- `policy_description` (string, required): Description of the policy
- `policy_object` (object, required): Policy configuration object containing rules and settings

**Note**: The `owner_id` is automatically set from the authenticated user's profile.

#### Response
```json
{
  "message": "Policy created successfully",
  "success": true,
  "policy": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "policy_id": "policy_01HGQK8F3VWXYZ123456789DEF",
    "policy_name": "backup-retention-policy",
    "policy_description": "Defines backup retention schedules and procedures",
    "policy_object": {
      "version": "1.0",
      "effective_date": "2023-12-01",
      "rules": [
        {
          "id": "daily_backup_rule",
          "type": "backup_schedule",
          "frequency": "daily",
          "retention": "30d",
          "applies_to": ["production_databases"]
        },
        {
          "id": "weekly_backup_rule",
          "type": "backup_schedule",
          "frequency": "weekly",
          "retention": "12w",
          "applies_to": ["production_databases"]
        }
      ],
      "enforcement": "automated"
    },
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  },
  "status": "success"
}
```

### 4. Modify Policy

**PUT** `/{tenant_url}/api/v1/policies/{policy_id}`

Updates an existing policy.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `policy_id` (string, required): The policy ID

#### Request Body
```json
{
  "policy_name": "enhanced-data-privacy-policy",
  "policy_description": "Enhanced data privacy policy with additional HIPAA compliance",
  "policy_object": {
    "version": "1.1",
    "effective_date": "2023-12-01",
    "rules": [
      {
        "id": "retention_rule_001",
        "type": "data_retention",
        "duration": "2555d",
        "applies_to": ["user_data", "transaction_logs", "medical_records"],
        "auto_delete": true
      },
      {
        "id": "access_rule_001",
        "type": "access_control",
        "minimum_role": "senior_data_analyst",
        "applies_to": ["sensitive_fields", "medical_data"],
        "conditions": ["authenticated", "authorized", "mfa_verified"]
      }
    ],
    "enforcement": "strict",
    "compliance_frameworks": ["GDPR", "CCPA", "HIPAA"]
  }
}
```

#### Fields
All fields are optional:
- `policy_name` (string): New name for the policy
- `policy_description` (string): New description
- `policy_object` (object): Updated policy configuration

#### Response
```json
{
  "message": "Policy updated successfully",
  "success": true,
  "policy": {
    "tenant_id": "tenant_01HGQK8F3VWXYZ123456789ABC",
    "policy_id": "policy_01HGQK8F3VWXYZ123456789ABC",
    "policy_name": "enhanced-data-privacy-policy",
    "policy_description": "Enhanced data privacy policy with additional HIPAA compliance",
    "policy_object": {
      "version": "1.1",
      "effective_date": "2023-12-01",
      "rules": [
        {
          "id": "retention_rule_001",
          "type": "data_retention",
          "duration": "2555d",
          "applies_to": ["user_data", "transaction_logs", "medical_records"],
          "auto_delete": true
        },
        {
          "id": "access_rule_001",
          "type": "access_control",
          "minimum_role": "senior_data_analyst",
          "applies_to": ["sensitive_fields", "medical_data"],
          "conditions": ["authenticated", "authorized", "mfa_verified"]
        }
      ],
      "enforcement": "strict",
      "compliance_frameworks": ["GDPR", "CCPA", "HIPAA"]
    },
    "owner_id": "user_01HGQK8F3VWXYZ123456789ABC"
  },
  "status": "success"
}
```

### 5. Delete Policy

**DELETE** `/{tenant_url}/api/v1/policies/{policy_id}`

Deletes a policy.

#### Path Parameters
- `tenant_url` (string, required): The tenant URL
- `policy_id` (string, required): The policy ID

#### Response
```json
{
  "message": "Policy deleted successfully",
  "success": true,
  "status": "success"
}
```

## Policy Object Structure

The `policy_object` field contains the policy configuration and supports various rule types:

### Common Rule Types

- **data_retention**: Defines how long data should be retained
- **access_control**: Controls who can access specific data
- **encryption**: Specifies encryption requirements
- **backup_schedule**: Defines backup schedules and retention
- **audit_logging**: Configures audit logging requirements
- **data_classification**: Classifies data sensitivity levels

### Enforcement Levels

- **strict**: All violations block operations
- **warning**: Violations generate warnings but allow operations
- **automated**: System automatically enforces rules
- **manual**: Requires manual review and approval

## Error Handling

All endpoints return appropriate HTTP status codes:

- `200 OK`: Successful GET/PUT operations
- `201 Created`: Successful POST operations
- `400 Bad Request`: Invalid request parameters or body, invalid policy syntax
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