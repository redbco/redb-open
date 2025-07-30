# reDB Node Docker Deployment

This document describes how to deploy reDB Node Open Source using Docker.

## Overview

The Docker setup includes:
- **Single Container Architecture**: All services run in one container
- **Built-in Databases**: PostgreSQL 17 and Redis included
- **Auto-Initialization**: Headless setup with `--autoinitialize` flag
- **CLI Access**: CLI is available within the container for external access

## Quick Start

### Using Docker Compose (Recommended)

1. **Clone the repository**:
   ```bash
   git clone https://github.com/redbco/redb-open
   cd redb-open
   ```

2. **Build and start the services**:
   ```bash
   docker compose up -d
   ```

3. **Check the logs**:
   ```bash
   docker compose logs -f redb-node
   ```

4. **Access the CLI**:
   ```bash
   docker compose exec redb-node redb-cli --help
   ```

### Using Docker Directly

1. **Build the image**:
   ```bash
   docker build -t redb-node .
   ```

2. **Run the container**:
   ```bash
   docker run -d \
     --name redb-node \
     -p 8080:8080 \
     -p 8081:8081 \
     -p 8082:8082 \
     -v redb_data:/opt/redb/data \
     -e REDB_KEYRING_PATH=/opt/redb/data/keyring.json \
     redb-node
   ```

## Container Architecture

### Main Container (`redb-node`)

**Services Included**:
- PostgreSQL 17 (database)
- Redis (caching)
- Supervisor (orchestrator)
- All microservices (security, core, mesh, etc.)

**Ports Exposed**:

**HTTP API Ports (External Access)**:
- `8080`: Client API (HTTP) - Main API for CLI and web clients
- `8081`: Service API (HTTP) - Administrative API
- `8082`: Query API (HTTP) - Database query execution

**Internal gRPC Ports (Service Communication)**:
- `50000`: Supervisor (internal)
- `50051`: Security service (internal)
- `50053`: Unified Model service (internal)
- `50054`: Transformation service (internal)
- `50055`: Anchor service (internal)
- `50056`: Mesh service (internal)
- `50057`: Service API gRPC (internal)
- `50058`: Query API gRPC (internal)
- `50059`: Client API gRPC (internal)
- `50060`: Webhook service (internal)
- `50061`: MCP Server (internal)
- `50062`: Core service (internal)

## Environment Variables

### Database Configuration

**Note**: In the Docker container, PostgreSQL is managed internally by the container. The database credentials are set up automatically during container initialization. No external database configuration is needed.

For external database connections (if needed), you can use these environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `REDB_POSTGRES_USER` | `postgres` | PostgreSQL username (for external DB) |
| `REDB_POSTGRES_PASSWORD` | `postgres` | PostgreSQL password (for external DB) |
| `REDB_POSTGRES_HOST` | `localhost` | PostgreSQL host (for external DB) |
| `REDB_POSTGRES_PORT` | `5432` | PostgreSQL port (for external DB) |
| `REDB_POSTGRES_DATABASE` | `postgres` | PostgreSQL database (for external DB) |

### Keyring Configuration

The Docker container uses a file-based keyring for secure storage of credentials and encryption keys. The keyring is stored in a persistent volume to survive container restarts.

| Variable | Default | Description |
|----------|---------|-------------|
| `REDB_KEYRING_PATH` | `/opt/redb/data/keyring.json` | Path to the keyring file in the container |
| `REDB_KEYRING_PASSWORD` | `default-master-password-change-me` | Master password for encrypting the keyring (change in production!) |

### Initial Setup

**Note**: The initial tenant, user, and workspace are now created via the Service API endpoint `/api/v1/setup`. No environment variables are needed for this process.

For external database connections (if needed), you can use these environment variables:

## Initialization Process

### Auto-Initialization

The container automatically runs initialization on first startup:

1. **PostgreSQL Setup**:
   - Initialize data directory
   - Configure for container environment
   - Set up internal database with default credentials (postgres/postgres)
   - **No external database configuration needed**

2. **Redis Setup**:
   - Configure Redis for container
   - Start Redis service

3. **reDB Initialization**:
   - Run `--autoinitialize` flag (fully idempotent)
   - Connect to internal PostgreSQL using default credentials
   - Create production database (`redb`)
   - Generate and store node keys (preserves existing keys)
   - Create database schema (skips if already exists)
   - Create local node (skips if already exists)
   - **No default tenant/user creation** - This is now done via API
   - **Supervisor starts automatically** after initialization

**Idempotent Features**:
- ✅ Safe to run multiple times
- ✅ Preserves existing node keys and passwords
- ✅ Skips schema creation if already exists
- ✅ Reuses existing local node if present
- ✅ Graceful handling of "already exists" scenarios

### Manual Tenant/User Setup

After auto-initialization completes, you need to create the initial tenant, user, and workspace:

1. **Check System Status**:
   ```bash
   curl http://localhost:8081/health
   ```

2. **Create Initial Setup** (using CLI - recommended):
   ```bash
   docker compose exec redb-node redb-cli setup
   ```
   
   The CLI will prompt you for:
   - Tenant Name (e.g., "My Company")
   - Tenant URL (e.g., "mycompany")
   - Tenant Description (optional)
   - Admin User Email (e.g., "admin@mycompany.com")
   - Admin User Password
   - Workspace Name (defaults to "default")

3. **Or Create Initial Setup** (using API directly):
   ```bash
   curl -X POST http://localhost:8081/api/v1/setup \
     -H "Content-Type: application/json" \
     -d '{
       "tenant_name": "my-company",
       "tenant_url": "mycompany",
       "tenant_description": "My Company Tenant",
       "user_email": "admin@mycompany.com",
       "user_password": "your-secure-password",
       "workspace_name": "default"
     }'
   ```

4. **Verify Setup**:
   ```bash
   curl http://localhost:8081/api/v1/tenants
   ```

### Manual Initialization

If you need to re-initialize:

```bash
# Stop the container
docker compose down

# Remove volumes to start fresh
docker volume rm redb-open_postgres_data redb-open_redb_data

# Start again
docker compose up -d
```

## Data Persistence

### Volumes

The following data is persisted:

- **`postgres_data`**: PostgreSQL database files
- **`redis_data`**: Redis database files
- **`redb_data`**: Application data
- **`redb_logs`**: Application logs

### Backup

To backup your data:

```bash
# Backup PostgreSQL
docker exec redb-node pg_dump -U postgres redb > backup.sql

# Backup Redis
docker exec redb-node redis-cli BGSAVE
docker cp redb-node:/var/lib/redis/dump.rdb ./redis-backup.rdb
```

## Monitoring and Health Checks

### Health Check

The container includes a health check that monitors the supervisor service:

```bash
# Check container health
docker ps

# View health check logs
docker inspect redb-node | grep Health -A 10
```

### Logs

```bash
# View all logs
docker compose logs -f

# View specific service logs
docker compose logs -f redb-node

# View logs in real-time
docker exec redb-node tail -f /opt/redb/logs/redb-node-event.log
```

## Password Management

### Initial Setup Password

When creating the initial tenant and user via the API, you specify the password directly:

1. **API Setup** (Recommended):
   ```bash
   curl -X POST http://localhost:8081/api/v1/setup \
     -H "Content-Type: application/json" \
     -d '{
       "tenant_name": "my-company",
       "tenant_url": "mycompany",
       "user_email": "admin@mycompany.com",
       "user_password": "your-secure-password",
       "workspace_name": "default"
     }'
   ```

2. **Security**: The password is set during the initial setup and stored securely in the database

### Using the Password

Once you've created the initial setup, you can use the credentials with the CLI:
```bash
# Login with the CLI
docker compose exec redb-node redb-cli auth login

# Enter the email: (the email you set during setup)
# Enter the password: (the password you set during setup)
```

## CLI Usage

### Using Docker Compose

```bash
# Show CLI help
docker compose exec redb-node redb-cli --help

# Initial setup (create first tenant, user, and workspace)
docker compose exec redb-node redb-cli setup

# List tenants
docker compose exec redb-node redb-cli tenants list

# Authenticate
docker compose exec redb-node redb-cli auth login

# Create a database
docker compose exec redb-node redb-cli databases create
```

### Using Docker Directly

```bash
# Run CLI commands directly in the container
docker exec redb-node redb-cli --help
docker exec redb-node redb-cli tenants list
```

## Troubleshooting

### Common Issues

1. **PostgreSQL Connection Failed**:
   ```bash
   # Check if PostgreSQL is running
   docker exec redb-node pg_isready -U postgres
   
   # Check PostgreSQL logs
   docker exec redb-node tail -f /var/lib/postgresql/data/log/*
   
   # Note: In Docker, PostgreSQL is managed internally
   # No external database configuration is needed
   ```

2. **Initialization Failed**:
   ```bash
   # Check initialization logs
   docker compose logs redb-node | grep -i "auto-initialization"
   
   # Re-run initialization (safe to run multiple times)
   docker exec redb-node /opt/redb/bin/redb-node --autoinitialize
   ```

3. **Service Not Starting**:
   ```bash
   # Check service status
   docker exec redb-node ps aux
   
   # Check supervisor logs
   docker exec redb-node tail -f /opt/redb/logs/redb-node-event.log
   ```

4. **Keyring Issues**:
   ```bash
   # Check if keyring file exists and has proper permissions
   docker exec redb-node ls -la /opt/redb/data/keyring.json
   
   # Check keyring file contents (if it exists)
   docker exec redb-node cat /opt/redb/data/keyring.json
   
   # Check keyring environment variables
   docker exec redb-node env | grep REDB_KEYRING
   
   # If keyring is corrupted, you can remove it and re-initialize
   docker exec redb-node rm -f /opt/redb/data/keyring.json
   docker compose restart redb-node
   ```

### Debug Mode

To run in debug mode with more verbose logging:

```bash
# Set debug logging
export REDB_LOG_LEVEL=debug

# Start with debug
docker compose up
```

## Production Deployment

### Security Considerations

1. **Use External Databases** (Optional):
   ```bash
   # Point to external PostgreSQL instead of container's internal DB
   export REDB_POSTGRES_HOST=your-postgres-host
   export REDB_POSTGRES_PASSWORD=your-password
   docker compose up -d
   ```
   
   **Note**: By default, the container uses its internal PostgreSQL. Only set these variables if you want to use an external database.

2. **Secure Keyring Configuration**:
   ```bash
   # Set a strong master password for the keyring
   export REDB_KEYRING_PASSWORD="your-strong-master-password"
   
   # Optionally use a custom keyring path
   export REDB_KEYRING_PATH="/secure/path/to/keyring.json"
   
   docker compose up -d
   ```
   
   **Important**: Change the default keyring master password in production environments.

3. **Network Security**:
   ```bash
   # Use custom network
   docker network create redb-network
   docker compose --network redb-network up -d
   ```

### Resource Limits

```yaml
# In docker-compose.yml
services:
  redb-node:
    deploy:
      resources:
        limits:
          memory: 4G
          cpus: '2.0'
        reservations:
          memory: 2G
          cpus: '1.0'
```

### High Availability

For production deployments, consider:
- Using external PostgreSQL cluster
- Using external Redis cluster
- Running multiple instances behind a load balancer
- Implementing proper backup strategies

## Development

### Building for Development

```bash
# Build with development flags
docker build --build-arg GOOS=linux --build-arg GOARCH=amd64 -t redb-node:dev .

# Run with development config
docker compose -f docker-compose.dev.yml up -d
```

### Debugging

```bash
# Attach to running container
docker exec -it redb-node bash

# View real-time logs
docker exec redb-node tail -f /opt/redb/logs/*.log

# Check service processes
docker exec redb-node ps aux
```

## Support

For issues and questions:
- Check the logs: `docker compose logs -f`
- Review this documentation
- Check the main project README
- Open an issue on GitHub 