# Multi-Instance Configuration Guide

This document describes the new configuration options that allow running multiple instances of reDB on a single machine and the single-tenant mode for the open-source version.

## Overview

The reDB open-source version now supports:

1. **Single-tenant mode**: Simplified deployment with a single default tenant
2. **Multi-instance support**: Run multiple reDB instances on the same machine without conflicts
3. **Flexible keyring configuration**: Choose between system keyring or file-based storage
4. **Port isolation**: Automatic port offset management for multi-instance deployments

## Configuration Sections

### Global Configuration

```yaml
global:
  multi_tenancy:
    # Mode: "single-tenant" for open-source version, "multi-tenant" for enterprise
    mode: "single-tenant"
    # Default tenant settings for single-tenant mode
    default_tenant_id: "default-tenant"
    default_tenant_name: "Default Tenant"
    default_tenant_url: "default"
```

**Options:**
- `mode`: Set to `"single-tenant"` for the open-source version
- `default_tenant_id`: The tenant ID used throughout the system in single-tenant mode
- `default_tenant_name`: Human-readable name for the default tenant
- `default_tenant_url`: URL identifier for the default tenant

### Keyring Configuration

```yaml
keyring:
  # Backend: "auto" (try system first, fallback to file), "system", or "file"
  backend: "auto"
  # Path for file-based keyring (optional, uses default if not specified)
  path: "/custom/path/to/keyring.json"
  # Master key for file-based keyring encryption (use REDB_KEYRING_PASSWORD env var in production)
  master_key: "your-secure-master-key"
  # Service name prefix for system keyring entries
  service_name: "redb"
```

**Options:**
- `backend`: Choose keyring backend
  - `"auto"`: Try system keyring first, fallback to file-based
  - `"system"`: Use system keyring only (macOS Keychain, Windows Credential Manager, Linux Secret Service)
  - `"file"`: Use encrypted file-based keyring only
- `path`: Custom path for file-based keyring (optional)
- `master_key`: Master key for encryption (use environment variable `REDB_KEYRING_PASSWORD` in production)
- `service_name`: Prefix for keyring entries

### Instance Group Configuration

```yaml
instance_group:
  # Unique identifier for this instance group (change for each instance)
  group_id: "default"
  # Port offset to avoid conflicts (0 = no offset, 1000 = add 1000 to all ports)
  port_offset: 0
```

**Options:**
- `group_id`: Unique identifier for this instance group
  - Used to isolate keyring entries between instances
  - Change this for each instance running on the same machine
- `port_offset`: Port offset added to all configured ports
  - Set to `0` for the first instance
  - Set to `1000`, `2000`, etc. for additional instances

## Multi-Instance Setup

To run multiple reDB instances on the same machine:

### Instance 1 (Default)
```yaml
instance_group:
  group_id: "default"
  port_offset: 0

keyring:
  backend: "auto"
  service_name: "redb"
```

### Instance 2
```yaml
instance_group:
  group_id: "instance2"
  port_offset: 1000

keyring:
  backend: "file"
  service_name: "redb"
```

### Instance 3
```yaml
instance_group:
  group_id: "instance3"
  port_offset: 2000

keyring:
  backend: "file"
  service_name: "redb"
```

## Port Mapping

With port offsets, the following ports are automatically adjusted:

| Service | Base Port | Instance 1 (offset 0) | Instance 2 (offset 1000) | Instance 3 (offset 2000) |
|---------|-----------|------------------------|---------------------------|---------------------------|
| Supervisor | 50000 | 50000 | 51000 | 52000 |
| Security | 50051 | 50051 | 51051 | 52051 |
| UnifiedModel | 50052 | 50052 | 51052 | 52052 |
| Webhook | 50053 | 50053 | 51053 | 52053 |
| Transformation | 50054 | 50054 | 51054 | 52054 |
| Core | 50055 | 50055 | 51055 | 52055 |
| Mesh (gRPC) | 50056 | 50056 | 51056 | 52056 |
| Mesh (External) | 10001 | 10001 | 11001 | 12001 |
| Anchor | 50057 | 50057 | 51057 | 52057 |
| Integration | 50058 | 50058 | 51058 | 52058 |
| ClientAPI | 50059 | 50059 | 51059 | 52059 |
| MCPServer | 50060 | 50060 | 51060 | 52060 |

## Keyring Isolation

Each instance group has isolated keyring storage:

### System Keyring
- Service names are prefixed with group ID: `redb-instance2-database`, `redb-instance2-security`, etc.

### File-based Keyring
- File paths include group ID: `~/.local/share/redb/keyring-instance2.json`

## Single-Tenant Mode

In single-tenant mode:

1. **Automatic tenant creation**: The default tenant is created automatically during initialization
2. **Simplified API usage**: No need to specify tenant information in most API calls
3. **Consistent tenant ID**: All operations use the configured `default_tenant_id`

### Initialization

```bash
# Initialize with single-tenant mode
./redb-supervisor --config config.yaml --initialize

# Auto-initialize (headless mode)
./redb-supervisor --config config.yaml --autoinitialize
```

## Environment Variables

### Keyring Configuration
- `REDB_KEYRING_PASSWORD`: Master password for file-based keyring encryption
- `REDB_KEYRING_PATH`: Override default keyring file path

### Database Configuration
- `REDB_DATABASE_NAME`: Override database name
- `REDB_POSTGRES_USER`: PostgreSQL username
- `REDB_POSTGRES_PASSWORD`: PostgreSQL password
- `REDB_POSTGRES_HOST`: PostgreSQL host
- `REDB_POSTGRES_PORT`: PostgreSQL port
- `REDB_POSTGRES_DATABASE`: PostgreSQL database name

## Best Practices

### Production Deployment
1. Use system keyring when available for better security
2. Set `REDB_KEYRING_PASSWORD` environment variable instead of `master_key` in config
3. Use unique `group_id` values for each instance
4. Plan port offsets to avoid conflicts with other services

### Development
1. Use file-based keyring for easier debugging
2. Use port offsets in multiples of 1000 for clarity
3. Keep configuration files organized with descriptive names

### Security
1. Never commit keyring master keys to version control
2. Use strong, unique passwords for keyring encryption
3. Restrict file permissions on keyring files (600)
4. Regularly rotate keyring master passwords

## Service Dependencies

The correct service startup order is:

1. **Foundation Services** (no dependencies):
   - `security`: Authentication and authorization
   - `unifiedmodel`: Data model management
   - `webhook`: Event handling
   - `transformation`: Data transformation

2. **Mesh Service** (depends on foundation services):
   - `mesh`: Network communication layer

3. **Core Service** (depends on mesh):
   - `core`: Main business logic

4. **API Services** (depend on core):
   - `anchor`: Anchor management
   - `integration`: External integrations
   - `clientapi`: Client API
   - `mcpserver`: MCP server

## Troubleshooting

### Service Startup Issues
- Check service dependencies in configuration
- Verify all required services are enabled
- Look for circular dependencies in logs
- Ensure correct startup order: foundation → mesh → core → APIs

### Port Conflicts
- Check if ports are already in use: `netstat -an | grep LISTEN`
- Increase port offset values to avoid conflicts
- Ensure firewall rules allow the new port ranges

### Keyring Issues
- Verify keyring backend availability
- Check file permissions for file-based keyring
- Ensure environment variables are set correctly
- Test keyring access with simple operations

### Multi-Instance Communication
- Each instance operates independently
- Database isolation is handled at the application level
- Mesh networking allows instances to discover each other
- Use different mesh node IDs for each instance
