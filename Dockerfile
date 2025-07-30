# Multi-stage Dockerfile for reDB Node Open Source
# This Dockerfile creates a single container with all services and databases

# =============================================================================
# BUILD STAGE
# =============================================================================
FROM golang:1.23.5-alpine AS builder

# Install build dependencies
RUN apk add --no-cache \
    git \
    make \
    protobuf \
    protobuf-dev \
    gcc \
    musl-dev

# Install protobuf Go tools
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Set working directory
WORKDIR /app

# Copy go mod files and Makefile
COPY go.mod go.sum go.work go.work.sum Makefile ./
COPY cmd/ ./cmd/
COPY pkg/ ./pkg/
COPY services/ ./services/
COPY api/ ./api/

# Download dependencies
RUN go mod download

# Generate Protocol Buffer code
RUN make proto

# Build all services for Linux
RUN make build-all

# =============================================================================
# RUNTIME STAGE
# =============================================================================
FROM alpine:3.22

# Install runtime dependencies
RUN apk add --no-cache \
    postgresql17 \
    postgresql17-client \
    postgresql17-contrib \
    redis \
    bash \
    curl \
    ca-certificates \
    tzdata \
    gosu

# Create application user and postgres user
RUN addgroup -g 1000 redb && \
    adduser -D -s /bin/bash -u 1000 -G redb redb && \
    (addgroup -g 999 postgres 2>/dev/null || true) && \
    (adduser -D -s /bin/bash -u 999 -G postgres postgres 2>/dev/null || true)

# Create necessary directories and set permissions
RUN mkdir -p /opt/redb/bin && \
    mkdir -p /opt/redb/config && \
    mkdir -p /opt/redb/logs && \
    mkdir -p /opt/redb/data && \
    mkdir -p /var/lib/postgresql/data && \
    mkdir -p /var/lib/redis && \
    mkdir -p /run/postgresql && \
    chown -R postgres:postgres /var/lib/postgresql && \
    chown -R postgres:postgres /run/postgresql && \
    chown -R redb:redb /var/lib/redis && \
    chown -R postgres:postgres /opt/redb/config && \
    chown -R postgres:postgres /opt/redb/bin && \
    chown -R postgres:postgres /opt/redb/logs && \
    chown -R postgres:postgres /opt/redb/data

# Copy binaries from builder
COPY --from=builder /app/build/linux-amd64/* /opt/redb/bin/
RUN chmod +x /opt/redb/bin/*

# Copy configuration
COPY sample_config/config_docker.yaml /opt/redb/config/config.yaml

# Create entry point script
COPY <<EOF /opt/redb/entrypoint.sh
#!/bin/bash
set -e

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Function to wait for PostgreSQL to be ready
wait_for_postgres() {
    log "Waiting for PostgreSQL to be ready..."
    until pg_isready -h localhost -p 5432 -U postgres; do
        sleep 1
    done
    log "PostgreSQL is ready"
}

# Function to initialize PostgreSQL
init_postgres() {
    log "Initializing PostgreSQL..."
    
    # Initialize PostgreSQL data directory
    if [ ! -f /var/lib/postgresql/data/postgresql.conf ]; then
        log "Initializing PostgreSQL data directory..."
        initdb -D /var/lib/postgresql/data
        
        # Configure PostgreSQL for container environment
        echo "listen_addresses = '*'" >> /var/lib/postgresql/data/postgresql.conf
        echo "port = 5432" >> /var/lib/postgresql/data/postgresql.conf
        echo "max_connections = 100" >> /var/lib/postgresql/data/postgresql.conf
        echo "shared_buffers = 128MB" >> /var/lib/postgresql/data/postgresql.conf
        echo "effective_cache_size = 256MB" >> /var/lib/postgresql/data/postgresql.conf
        echo "log_statement = 'all'" >> /var/lib/postgresql/data/postgresql.conf
        echo "log_destination = 'stderr'" >> /var/lib/postgresql/data/postgresql.conf
        echo "logging_collector = on" >> /var/lib/postgresql/data/postgresql.conf
        echo "log_directory = 'log'" >> /var/lib/postgresql/data/postgresql.conf
        echo "log_filename = 'postgresql-%Y-%m-%d_%H%M%S.log'" >> /var/lib/postgresql/data/postgresql.conf
        echo "log_rotation_age = 1d" >> /var/lib/postgresql/data/postgresql.conf
        echo "log_rotation_size = 10MB" >> /var/lib/postgresql/data/postgresql.conf
        
        # Configure access
        echo "local all postgres trust" > /var/lib/postgresql/data/pg_hba.conf
        echo "host all postgres 127.0.0.1/32 trust" >> /var/lib/postgresql/data/pg_hba.conf
        echo "host all postgres ::1/128 trust" >> /var/lib/postgresql/data/pg_hba.conf
        echo "local all all trust" >> /var/lib/postgresql/data/pg_hba.conf
        echo "host all all 127.0.0.1/32 trust" >> /var/lib/postgresql/data/pg_hba.conf
        echo "host all all ::1/128 trust" >> /var/lib/postgresql/data/pg_hba.conf
    fi
    
    # Start PostgreSQL
    log "Starting PostgreSQL..."
    pg_ctl -D /var/lib/postgresql/data -l /var/lib/postgresql/data/logfile start
    
    # Wait for PostgreSQL to be ready
    wait_for_postgres
    
    # Set default password for postgres user
    psql -c "ALTER USER postgres PASSWORD 'postgres';"
    
    log "PostgreSQL initialized successfully"
}

# Function to initialize Redis
init_redis() {
    log "Initializing Redis..."
    
    # Create Redis configuration
    cat > /opt/redb/config/redis.conf << 'REDISCONF'
bind 127.0.0.1
port 6379
timeout 0
tcp-keepalive 300
daemonize no
supervised no
pidfile /var/run/redis_6379.pid
loglevel notice
logfile ""
databases 16
save 900 1
save 300 10
save 60 10000
stop-writes-on-bgsave-error yes
rdbcompression yes
rdbchecksum yes
dbfilename dump.rdb
dir /var/lib/redis
maxmemory 256mb
maxmemory-policy allkeys-lru
REDISCONF
    
    # Start Redis
    log "Starting Redis..."
    redis-server /opt/redb/config/redis.conf &
    REDIS_PID=$!
    
    # Wait for Redis to be ready
    until redis-cli ping > /dev/null 2>&1; do
        sleep 1
    done
    log "Redis is ready"
}

# Function to check if initialization is needed
check_initialization() {
    log "Checking if initialization is needed..."
    
    # Check if redb database exists
    if psql -lqt | cut -d'|' -f1 | grep -qw redb; then
        log "Database 'redb' exists, checking if schema is initialized..."
        
        # Check if the schema is fully initialized by checking for multiple key tables
        if psql -d redb -c "\\dt" | grep -q localidentity && \
           psql -d redb -c "\\dt" | grep -q tenants && \
           psql -d redb -c "\\dt" | grep -q users && \
           psql -d redb -c "\\dt" | grep -q nodes; then
            log "Schema already initialized, checking for local node..."
            
            # Check if local node exists
            if psql -d redb -c "SELECT COUNT(*) FROM localidentity li JOIN nodes n ON n.node_id = li.identity_id;" | grep -q "1"; then
                log "System is fully initialized, skipping initialization"
                return 1
            else
                log "Schema exists but local node missing, running initialization"
                return 0
            fi
        else
            log "Database exists but schema not initialized, running initialization"
            return 0
        fi
    else
        log "Database 'redb' does not exist, initialization needed"
        return 0
    fi
}

# Function to run auto-initialization
run_auto_initialize() {
    log "Running reDB auto-initialization..."
    
    # In Docker, we use the container's internal PostgreSQL setup
    # The container manages its own database with default credentials (postgres/postgres)
    # No environment variables needed - the auto-initialization will use the default credentials
    cd /opt/redb
    ./bin/redb-node --autoinitialize --config=/opt/redb/config/config.yaml
    
    if [ $? -eq 0 ]; then
        log "Auto-initialization completed successfully"
    else
        log "Auto-initialization failed"
        exit 1
    fi
}

# Function to start supervisor
start_supervisor() {
    log "Starting reDB supervisor..."
    
    cd /opt/redb
    exec ./bin/redb-node --config=/opt/redb/config/config.yaml
}

# Main execution
main() {
    log "Starting reDB Node container..."
    
    # Initialize PostgreSQL
    init_postgres
    
    # Initialize Redis
    init_redis
    
    # Check if initialization is needed
    if check_initialization; then
        run_auto_initialize
    fi
    
    # Start supervisor
    start_supervisor
}

# Run main function
main "$@"
EOF

# Make entrypoint executable
RUN chmod +x /opt/redb/entrypoint.sh

# Set working directory
WORKDIR /opt/redb

# Switch to postgres user for database operations
USER postgres

# Expose ports
# HTTP API ports (for external access)
EXPOSE 8080 8081 8082
# Internal gRPC ports (for service communication)
EXPOSE 50000 50051 50053 50054 50055 50056 50057 50058 50059 50060 50061 50062

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:50000/health || exit 1

# Set entrypoint
ENTRYPOINT ["/opt/redb/entrypoint.sh"] 