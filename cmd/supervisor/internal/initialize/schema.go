package initialize

// DatabaseSchema contains the complete reDB database schema
// This is embedded directly in the code to avoid security risks of external SQL files
const DatabaseSchema = `
-- =============================================================================
-- EXTENSIONS AND FUNCTIONS
-- =============================================================================

-- Add pgcrypto extension
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Create function generate_ulid
CREATE OR REPLACE FUNCTION generate_ulid(prefix TEXT)
RETURNS ulid AS $$
DECLARE
    -- Crockford base32 alphabet (excludes I, L, O, U to avoid confusion)
    alphabet TEXT := '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
    timestamp_ms BIGINT;
    random_bytes BYTEA;
    result TEXT := '';
    i INTEGER;
    val BIGINT;
BEGIN
    -- Validate prefix
    IF prefix !~ '^[a-z]{2,10}$' THEN
        RAISE EXCEPTION 'Invalid prefix: must be 2-10 lowercase letters';
    END IF;
    
    -- Get timestamp in milliseconds
    timestamp_ms := EXTRACT(EPOCH FROM clock_timestamp()) * 1000;
    
    -- Generate 10 bytes (80 bits) of randomness
    random_bytes := gen_random_bytes(10);
    
    -- For simplicity, using a hex-based approach that's ULID-compatible
    result := prefix || '_' || 
              lpad(upper(encode(int8send(timestamp_ms), 'hex')), 12, '0') ||
              upper(encode(random_bytes, 'hex'));
    
    -- Truncate to proper ULID length (26 chars after prefix)
    result := prefix || '_' || substring(
        lpad(upper(encode(int8send(timestamp_ms), 'hex')), 12, '0') ||
        upper(encode(random_bytes, 'hex')), 1, 26
    );
    
    RETURN result::ulid;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- CUSTOM ENUMS
-- =============================================================================

-- Status enum
CREATE TYPE status_enum AS ENUM ('STATUS_HEALTHY', 'STATUS_DEGRADED', 'STATUS_UNHEALTHY', 'STATUS_PENDING', 'STATUS_UNKNOWN', 'STATUS_SUCCESS', 'STATUS_FAILURE', 'STATUS_STARTING', 'STATUS_STOPPING', 'STATUS_STOPPED', 'STATUS_STARTED', 'STATUS_CREATED', 'STATUS_DELETED', 'STATUS_UPDATED', 'STATUS_CONNECTED', 'STATUS_DISCONNECTED', 'STATUS_CONNECTING', 'STATUS_DISCONNECTING', 'STATUS_RECONNECTING', 'STATUS_ERROR', 'STATUS_WARNING', 'STATUS_INFO', 'STATUS_DEBUG', 'STATUS_TRACE', 'STATUS_EMPTY', 'STATUS_JOINING', 'STATUS_LEAVING', 'STATUS_SEEDING', 'STATUS_ORPHANED', 'STATUS_SENT', 'STATUS_CANCELLED', 'STATUS_PROCESSING', 'STATUS_DONE', 'STATUS_RECEIVED', 'STATUS_ACTIVE', 'STATUS_CLEAN', 'STATUS_INCONSISTENT');

-- Join key hash enum
CREATE TYPE join_key_enum AS ENUM ('OPEN', 'KEY_REQUIRED', 'CLOSED');

-- =============================================================================
-- CORE SYSTEM TABLES
-- =============================================================================

-- Local Identity for system identification
CREATE TABLE localidentity (
    identity_id BIGINT PRIMARY KEY
);

-- Mesh management for distributed system coordination
CREATE TABLE mesh (
    mesh_id BIGSERIAL PRIMARY KEY,
    mesh_name VARCHAR(255) UNIQUE NOT NULL,
    mesh_description TEXT DEFAULT '',
    allow_join join_key_enum DEFAULT 'KEY_REQUIRED',
    join_key_hash TEXT,
    status status_enum DEFAULT 'STATUS_CREATED',
    split_strategy VARCHAR(50) DEFAULT 'SEED_NODE_PREVAILS_IN_EVEN_SPLIT',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Multi-tenant organization structure
CREATE TABLE tenants (
    tenant_id ulid PRIMARY KEY DEFAULT generate_ulid('tenant'),
    tenant_name VARCHAR(255) UNIQUE NOT NULL,
    tenant_description TEXT DEFAULT '',
    tenant_url VARCHAR(255) UNIQUE NOT NULL,
    status status_enum DEFAULT 'STATUS_HEALTHY',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Geographic regions
CREATE TABLE regions (
    region_id ulid PRIMARY KEY DEFAULT generate_ulid('region'),
    tenant_id ulid REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    region_name VARCHAR(255) UNIQUE NOT NULL,
    region_description TEXT DEFAULT '',
    region_type VARCHAR(255) DEFAULT 'AWS',
    region_location VARCHAR(255) DEFAULT '',
    region_latitude DOUBLE PRECISION,
    region_longitude DOUBLE PRECISION,
    global_region BOOLEAN DEFAULT false,
    status status_enum DEFAULT 'STATUS_EMPTY',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Core mesh topology (replaces old mesh_* tables)
CREATE TABLE nodes (
    node_id BIGSERIAL PRIMARY KEY,
    node_name VARCHAR(255) NOT NULL,
    node_description TEXT DEFAULT '',
    node_public_key BYTEA NOT NULL,
    node_last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    node_incarnation BIGINT DEFAULT 1,
    node_meta JSONB DEFAULT '{}',
    node_platform VARCHAR(100) DEFAULT '',
    node_version VARCHAR(100) DEFAULT '',
    region_id ulid REFERENCES regions(region_id) ON DELETE SET NULL,
    ip_address INET,
    port INTEGER,
    status status_enum DEFAULT 'STATUS_CREATED',
    seed_node BOOLEAN DEFAULT FALSE,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE routes (
    route_id ulid PRIMARY KEY DEFAULT generate_ulid('route'),
    a_node BIGINT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    b_node BIGINT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    latency_ms INTEGER DEFAULT 0,
    bandwidth_mbps INTEGER DEFAULT 0,
    loss DECIMAL(5,4) DEFAULT 0.0,
    utilization DECIMAL(5,4) DEFAULT 0.0,
    status status_enum DEFAULT 'STATUS_CREATED',
    meta JSONB DEFAULT '{}',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(a_node, b_node)
);

-- =============================================================================
-- TENANT AND USER MANAGEMENT
-- =============================================================================

-- Users within tenants
CREATE TABLE users (
    user_id ulid PRIMARY KEY DEFAULT generate_ulid('user'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    user_email VARCHAR(255) UNIQUE NOT NULL,
    user_name VARCHAR(255) DEFAULT '',
    user_password_hash VARCHAR(255) NOT NULL,
    user_enabled BOOLEAN DEFAULT true,
    password_changed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User JWT tokens
CREATE TABLE user_jwt_tokens (
    user_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    session_id TEXT NOT NULL DEFAULT substring(md5(random()::text || clock_timestamp()::text), 1, 16),
    refresh_token TEXT UNIQUE NOT NULL,
    access_token TEXT UNIQUE NOT NULL,
    session_name TEXT DEFAULT '',
    session_agent TEXT DEFAULT '',
    session_ip_address TEXT DEFAULT '',
    session_platform TEXT DEFAULT '',
    session_browser TEXT DEFAULT '',
    session_os TEXT DEFAULT '',
    session_device_type TEXT DEFAULT 'unknown',
    session_location TEXT DEFAULT '',
    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires TIMESTAMP DEFAULT CURRENT_TIMESTAMP + INTERVAL '24 hours',
    PRIMARY KEY (user_id, session_id),
    UNIQUE(refresh_token),
    UNIQUE(access_token)
);

-- API tokens for programmatic access
CREATE TABLE apitokens (
    apitoken_id ulid PRIMARY KEY DEFAULT generate_ulid('apitoken'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    user_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    apitoken_name VARCHAR(255) NOT NULL,
    apitoken_description TEXT DEFAULT '',
    apitoken_key TEXT UNIQUE NOT NULL,
    apitoken_enabled BOOLEAN DEFAULT true,
    apitoken_auto_expires BOOLEAN DEFAULT true,
    apitoken_expiry_time_days INTEGER DEFAULT 90,
    apitoken_key_cycled TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, apitoken_name)
);

-- =============================================================================
-- AUTHORIZATION SYSTEM
-- =============================================================================

-- User groups for organizing users
CREATE TABLE groups (
    group_id ulid PRIMARY KEY DEFAULT generate_ulid('group'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    group_name VARCHAR(255) NOT NULL,
    group_description TEXT DEFAULT '',
    parent_group_id ulid REFERENCES groups(group_id),
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, group_name)
);

-- Roles for grouping permissions
CREATE TABLE roles (
    role_id ulid PRIMARY KEY DEFAULT generate_ulid('role'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    role_name VARCHAR(255) NOT NULL,
    role_description TEXT DEFAULT '',
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, role_name)
);

-- Granular permissions
CREATE TABLE permissions (
    permission_id ulid PRIMARY KEY DEFAULT generate_ulid('perm'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    action VARCHAR(255) NOT NULL,
    resource VARCHAR(255) NOT NULL,
    scope VARCHAR(255) DEFAULT '*',
    constraints JSONB DEFAULT '{}',
    conditions JSONB DEFAULT '{}',
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User to group assignments
CREATE TABLE user_groups (
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    user_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    group_id ulid NOT NULL REFERENCES groups(group_id) ON DELETE CASCADE ON UPDATE CASCADE,
    granted_by ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    owner_id ulid NOT NULL REFERENCES users(user_id),
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, user_id, group_id)
);

-- User to role assignments
CREATE TABLE user_roles (
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    user_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    role_id ulid NOT NULL REFERENCES roles(role_id) ON DELETE CASCADE ON UPDATE CASCADE,
    granted_by ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,
    owner_id ulid NOT NULL REFERENCES users(user_id),
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, user_id, role_id)
);

-- Group to role assignments
CREATE TABLE group_roles (
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    group_id ulid NOT NULL REFERENCES groups(group_id) ON DELETE CASCADE ON UPDATE CASCADE,
    role_id ulid NOT NULL REFERENCES roles(role_id) ON DELETE CASCADE ON UPDATE CASCADE,
    granted_by ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,
    owner_id ulid NOT NULL REFERENCES users(user_id),
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, group_id, role_id)
);

-- Role to permission assignments
CREATE TABLE role_permissions (
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    role_id ulid NOT NULL REFERENCES roles(role_id) ON DELETE CASCADE ON UPDATE CASCADE,
    permission_id ulid NOT NULL REFERENCES permissions(permission_id) ON DELETE CASCADE ON UPDATE CASCADE,
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, role_id, permission_id)
);

-- Role templates for standardized role creation
CREATE TABLE role_templates (
    template_id ulid PRIMARY KEY DEFAULT generate_ulid('template'),
    template_name VARCHAR(255) UNIQUE NOT NULL,
    template_description TEXT DEFAULT '',
    template_category VARCHAR(255) DEFAULT 'general',
    template_metadata JSONB DEFAULT '{}',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Template to permission mappings
CREATE TABLE template_permissions (
    template_id ulid NOT NULL REFERENCES role_templates(template_id) ON DELETE CASCADE ON UPDATE CASCADE,
    permission_id ulid NOT NULL REFERENCES permissions(permission_id) ON DELETE CASCADE ON UPDATE CASCADE,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (template_id, permission_id)
);

-- =============================================================================
-- INFRASTRUCTURE COMPONENTS
-- =============================================================================

-- Satellite components
CREATE TABLE satellites (
    satellite_id ulid PRIMARY KEY DEFAULT generate_ulid('satellite'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    satellite_name VARCHAR(255) UNIQUE NOT NULL,
    satellite_description TEXT DEFAULT '',
    satellite_platform VARCHAR(255) DEFAULT 'linux-amd64',
    satellite_version VARCHAR(255) DEFAULT '1.0.0',
    satellite_region_id ulid REFERENCES regions(region_id),
    satellite_ip_address VARCHAR(255) NOT NULL,
    connected_to_node_id BIGINT NOT NULL REFERENCES nodes(node_id),
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    status status_enum DEFAULT 'STATUS_PENDING',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Anchor components
CREATE TABLE anchors (
    anchor_id ulid PRIMARY KEY DEFAULT generate_ulid('anchor'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    anchor_name VARCHAR(255) UNIQUE NOT NULL,
    anchor_description TEXT DEFAULT '',
    anchor_platform VARCHAR(255) DEFAULT 'linux-amd64',
    anchor_version VARCHAR(255) DEFAULT '1.0.0',
    anchor_region_id ulid REFERENCES regions(region_id),
    anchor_ip_address VARCHAR(255) NOT NULL,
    connected_to_node_id BIGINT REFERENCES nodes(node_id),
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    status status_enum DEFAULT 'STATUS_PENDING',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Stream components for data streaming platform connections
CREATE TABLE streams (
    stream_id ulid PRIMARY KEY DEFAULT generate_ulid('stream'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    stream_name VARCHAR(255) UNIQUE NOT NULL,
    stream_description TEXT DEFAULT '',
    stream_platform VARCHAR(255) DEFAULT 'kafka',
    stream_version VARCHAR(255) DEFAULT '1.0.0',
    stream_region_id ulid REFERENCES regions(region_id),
    connection_config JSONB NOT NULL DEFAULT '{}',
    credential_key TEXT DEFAULT '',
    stream_metadata JSONB NOT NULL DEFAULT '{}',
    monitored_topics JSONB DEFAULT '[]',
    connected_to_node_id BIGINT NOT NULL REFERENCES nodes(node_id),
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    status status_enum DEFAULT 'STATUS_PENDING',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- WORKSPACE AND DEVELOPMENT STRUCTURE
-- =============================================================================

-- Policies for access control and configuration
CREATE TABLE policies (
    policy_id ulid PRIMARY KEY DEFAULT generate_ulid('policy'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    policy_name VARCHAR(255) NOT NULL,
    policy_description TEXT DEFAULT '',
    policy_object JSONB DEFAULT '{}',
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, policy_name)
);

-- Workspaces for project organization
CREATE TABLE workspaces (
    workspace_id ulid PRIMARY KEY DEFAULT generate_ulid('ws'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    workspace_name VARCHAR(255) NOT NULL,
    workspace_description TEXT DEFAULT '',
    policy_ids ulid[] DEFAULT '{}',
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    status status_enum DEFAULT 'STATUS_CREATED',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, workspace_name)
);

-- Environments within workspaces
CREATE TABLE environments (
    environment_id ulid PRIMARY KEY DEFAULT generate_ulid('env'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    workspace_id ulid NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE ON UPDATE CASCADE,
    environment_name VARCHAR(255) NOT NULL,
    environment_description TEXT DEFAULT '',
    environment_is_production BOOLEAN DEFAULT false,
    environment_criticality INTEGER DEFAULT 0,
    environment_priority INTEGER DEFAULT 0,
    policy_ids ulid[] DEFAULT '{}',
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    status status_enum DEFAULT 'STATUS_EMPTY',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(workspace_id, environment_name)
);

-- =============================================================================
-- DATABASE INFRASTRUCTURE
-- =============================================================================

-- Database instances
CREATE TABLE instances (
    instance_id ulid PRIMARY KEY DEFAULT generate_ulid('instance'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    workspace_id ulid NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE ON UPDATE CASCADE,
    environment_id ulid REFERENCES environments(environment_id) ON DELETE SET NULL ON UPDATE CASCADE,
    connected_to_node_id BIGINT REFERENCES nodes(node_id) ON DELETE SET NULL ON UPDATE CASCADE,
    instance_name VARCHAR(255) UNIQUE NOT NULL,
    instance_description TEXT DEFAULT '',
    instance_type VARCHAR(255) NOT NULL,
    instance_vendor VARCHAR(255) DEFAULT 'generic',
    instance_version VARCHAR(255) DEFAULT '',
    instance_unique_identifier VARCHAR(255) UNIQUE NOT NULL,
    instance_host VARCHAR(255) NOT NULL,
    instance_port INTEGER NOT NULL,
    instance_username VARCHAR(255) NOT NULL,
    instance_password TEXT NOT NULL,
    instance_system_db_name VARCHAR(255) NOT NULL,
    instance_enabled BOOLEAN DEFAULT true,
    instance_ssl BOOLEAN DEFAULT false,
    instance_ssl_mode VARCHAR(255) DEFAULT 'disable',
    instance_ssl_cert VARCHAR(255),
    instance_ssl_key VARCHAR(255),
    instance_ssl_root_cert VARCHAR(255),
    policy_ids ulid[] NOT NULL DEFAULT '{}',
    instance_metadata JSONB NOT NULL DEFAULT '{}',
    instance_databases JSONB NOT NULL DEFAULT '{}',
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    instance_status_message VARCHAR(255) DEFAULT '',
    status status_enum DEFAULT 'STATUS_PENDING',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Databases within instances
CREATE TABLE databases (
    database_id ulid PRIMARY KEY DEFAULT generate_ulid('db'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    workspace_id ulid NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE ON UPDATE CASCADE,
    environment_id ulid REFERENCES environments(environment_id) ON DELETE SET NULL ON UPDATE CASCADE,
    connected_to_node_id BIGINT REFERENCES nodes(node_id) ON DELETE SET NULL ON UPDATE CASCADE,
    instance_id ulid NOT NULL REFERENCES instances(instance_id) ON DELETE CASCADE ON UPDATE CASCADE,
    database_name VARCHAR(255) NOT NULL,
    database_description TEXT DEFAULT '',
    database_type VARCHAR(255) NOT NULL,
    database_vendor VARCHAR(255) DEFAULT 'generic',
    database_version VARCHAR(255) DEFAULT '',
    database_username VARCHAR(255) NOT NULL,
    database_password TEXT NOT NULL,
    database_db_name VARCHAR(255) NOT NULL,
    database_enabled BOOLEAN DEFAULT true,
    policy_ids ulid[] NOT NULL DEFAULT '{}',
    database_metadata JSONB NOT NULL DEFAULT '{}',
    database_schema JSONB NOT NULL DEFAULT '{}',
    database_tables JSONB NOT NULL DEFAULT '{}',
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    database_status_message VARCHAR(255) DEFAULT '',
    status status_enum DEFAULT 'STATUS_PENDING',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(workspace_id, database_name)
);

-- =============================================================================
-- VERSION CONTROL SYSTEM
-- =============================================================================

-- Repositories for schema versioning
CREATE TABLE repos (
    repo_id ulid PRIMARY KEY DEFAULT generate_ulid('repo'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    workspace_id ulid NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE ON UPDATE CASCADE,
    repo_name VARCHAR(255) NOT NULL,
    repo_description TEXT NOT NULL DEFAULT '',
    policy_ids ulid[] NOT NULL DEFAULT '{}',
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    status status_enum NOT NULL DEFAULT 'STATUS_EMPTY',
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(workspace_id, repo_name)
);

-- Branches within repositories
CREATE TABLE branches (
    branch_id ulid PRIMARY KEY DEFAULT generate_ulid('branch'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    workspace_id ulid NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE ON UPDATE CASCADE,
    repo_id ulid NOT NULL REFERENCES repos(repo_id) ON DELETE CASCADE ON UPDATE CASCADE,
    branch_name VARCHAR(255) NOT NULL DEFAULT 'main',
    parent_branch_id ulid REFERENCES branches(branch_id) ON DELETE SET NULL ON UPDATE CASCADE,
    connected_to_database BOOLEAN NOT NULL DEFAULT false,
    connected_database_id ulid REFERENCES databases(database_id) ON DELETE SET NULL ON UPDATE CASCADE,
    policy_ids ulid[] NOT NULL DEFAULT '{}',
    status status_enum NOT NULL DEFAULT 'STATUS_EMPTY',
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Commits within branches
CREATE TABLE commits (
    commit_id SERIAL PRIMARY KEY,
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    workspace_id ulid NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE ON UPDATE CASCADE,
    repo_id ulid NOT NULL REFERENCES repos(repo_id) ON DELETE CASCADE ON UPDATE CASCADE,
    branch_id ulid NOT NULL REFERENCES branches(branch_id) ON DELETE CASCADE ON UPDATE CASCADE,
    commit_code VARCHAR(8) NOT NULL UNIQUE DEFAULT "substring"(md5(random()::text || clock_timestamp()::text), 1, 8),
    commit_is_head BOOLEAN NOT NULL DEFAULT true,
    commit_message TEXT NOT NULL DEFAULT '',
    schema_type VARCHAR(255) NOT NULL DEFAULT 'unified',
    schema_structure JSONB NOT NULL DEFAULT '{}',
    policy_ids ulid[] NOT NULL DEFAULT '{}',
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- DATA MAPPING AND RELATIONSHIPS
-- =============================================================================

CREATE TABLE mappings (
    mapping_id ulid PRIMARY KEY DEFAULT generate_ulid('map'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    workspace_id ulid NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE ON UPDATE CASCADE,
    mapping_name VARCHAR(255) NOT NULL,
    mapping_description TEXT DEFAULT '',
    mapping_type VARCHAR(255) NOT NULL DEFAULT 'table',
    mapping_source_type VARCHAR(255),
    mapping_target_type VARCHAR(255),
    mapping_source_identifier VARCHAR(255),
    mapping_target_identifier VARCHAR(255),
    mapping_source_container_id ulid,
    mapping_target_container_id ulid,
    mapping_object JSONB DEFAULT '{}',
    policy_ids ulid[] DEFAULT '{}',
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    validated BOOLEAN DEFAULT false,
    validated_at TIMESTAMP DEFAULT NULL,
    validation_errors JSONB DEFAULT '[]',
    validation_warnings JSONB DEFAULT '[]',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(workspace_id, mapping_name)
);

-- Mapping rules
CREATE TABLE mapping_rules (
    mapping_rule_id ulid PRIMARY KEY DEFAULT generate_ulid('maprule'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    workspace_id ulid NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE ON UPDATE CASCADE,
    mapping_rule_name VARCHAR(255) NOT NULL,
    mapping_rule_description TEXT NOT NULL DEFAULT '',
    mapping_rule_metadata JSONB NOT NULL DEFAULT '{}',
    mapping_rule_workflow_type VARCHAR(50) DEFAULT 'simple',
    mapping_rule_cardinality VARCHAR(50) DEFAULT 'one-to-one' CHECK (mapping_rule_cardinality IN ('one-to-one', 'one-to-many', 'many-to-one', 'many-to-many', 'generator', 'sink')),
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Mapping rules to mappings
CREATE TABLE mapping_rule_mappings (
    mapping_rule_id ulid NOT NULL REFERENCES mapping_rules(mapping_rule_id) ON DELETE CASCADE ON UPDATE CASCADE,
    mapping_id ulid NOT NULL REFERENCES mappings(mapping_id) ON DELETE CASCADE ON UPDATE CASCADE,
    mapping_rule_order INTEGER NOT NULL DEFAULT 0,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (mapping_rule_id, mapping_id)
);

-- Mapping rule source items (many-to-many for flexible cardinality)
CREATE TABLE mapping_rule_source_items (
    mapping_rule_id ulid NOT NULL REFERENCES mapping_rules(mapping_rule_id) ON DELETE CASCADE ON UPDATE CASCADE,
    resource_item_id ulid NOT NULL,
    item_order INTEGER NOT NULL DEFAULT 0,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (mapping_rule_id, resource_item_id)
);

-- Mapping rule target items (many-to-many for flexible cardinality)
CREATE TABLE mapping_rule_target_items (
    mapping_rule_id ulid NOT NULL REFERENCES mapping_rules(mapping_rule_id) ON DELETE CASCADE ON UPDATE CASCADE,
    resource_item_id ulid NOT NULL,
    item_order INTEGER NOT NULL DEFAULT 0,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (mapping_rule_id, resource_item_id)
);

-- Mapping filters for data filtering (one mapping -> many filters)
CREATE TABLE mapping_filters (
    filter_id ulid PRIMARY KEY DEFAULT generate_ulid('filter'),
    mapping_id ulid NOT NULL REFERENCES mappings(mapping_id) ON DELETE CASCADE ON UPDATE CASCADE,
    filter_type VARCHAR(50) NOT NULL CHECK (filter_type IN ('where_clause', 'column_condition', 'json_path', 'null_check', 'range')),
    filter_expression JSONB NOT NULL,
    filter_order INTEGER NOT NULL DEFAULT 0,
    filter_operator VARCHAR(10) NOT NULL DEFAULT 'AND' CHECK (filter_operator IN ('AND', 'OR')),
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Relationships between data sources
CREATE TABLE relationships (
    relationship_id ulid PRIMARY KEY DEFAULT generate_ulid('rel'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    workspace_id ulid NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE ON UPDATE CASCADE,
    relationship_name VARCHAR(255) NOT NULL,
    relationship_description TEXT DEFAULT '',
    relationship_type VARCHAR(255) DEFAULT 'replication',
    relationship_source_type VARCHAR(255) DEFAULT 'table',
    relationship_target_type VARCHAR(255) DEFAULT 'table',
    relationship_source_database_id ulid NOT NULL REFERENCES databases(database_id) ON DELETE CASCADE ON UPDATE CASCADE,
    relationship_source_table_name VARCHAR(255) NOT NULL,
    relationship_target_database_id ulid NOT NULL REFERENCES databases(database_id) ON DELETE CASCADE ON UPDATE CASCADE,
    relationship_target_table_name VARCHAR(255) NOT NULL,
    mapping_id ulid NOT NULL REFERENCES mappings(mapping_id),
    policy_ids ulid[] DEFAULT '{}',
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    status_message VARCHAR(255) DEFAULT '',
    status status_enum DEFAULT 'STATUS_PENDING',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(workspace_id, relationship_name)
);

-- Replication sources (internal for mapping relationships to CDC)
CREATE TABLE replication_sources (
    replication_source_id ulid PRIMARY KEY DEFAULT generate_ulid('cdcs'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    workspace_id ulid NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE ON UPDATE CASCADE,
    database_id ulid NOT NULL REFERENCES databases(database_id) ON DELETE CASCADE ON UPDATE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    relationship_id ulid NOT NULL REFERENCES relationships(relationship_id) ON DELETE CASCADE ON UPDATE CASCADE,
    publication_name VARCHAR(255) NOT NULL,
    slot_name VARCHAR(255) NOT NULL,
    cdc_connection_id VARCHAR(255) DEFAULT '',
    cdc_position TEXT DEFAULT '',
    cdc_state JSONB DEFAULT '{}',
    events_processed BIGINT DEFAULT 0,
    events_pending BIGINT DEFAULT 0,
    last_event_timestamp TIMESTAMP,
    last_sync_timestamp TIMESTAMP,
    target_database_id ulid REFERENCES databases(database_id) ON DELETE CASCADE ON UPDATE CASCADE,
    target_table_name VARCHAR(255) DEFAULT '',
    mapping_rules JSONB DEFAULT '{}',
    status_message VARCHAR(255) DEFAULT '',
    status status_enum DEFAULT 'STATUS_PENDING',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(workspace_id, database_id, table_name)
);

-- Data transformations
CREATE TABLE transformations (
    transformation_id ulid PRIMARY KEY DEFAULT generate_ulid('transform'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    transformation_name VARCHAR(255) NOT NULL,
    transformation_description TEXT DEFAULT '',
    transformation_type VARCHAR(255) DEFAULT 'mutate',
    transformation_version VARCHAR(255) DEFAULT '1.0.0',
    transformation_function TEXT DEFAULT '',
    transformation_cardinality VARCHAR(50) DEFAULT 'one-to-one',
    requires_input BOOLEAN DEFAULT true,
    produces_output BOOLEAN DEFAULT true,
    transformation_implementation TEXT DEFAULT '',
    transformation_metadata JSONB DEFAULT '{}',
    transformation_enabled BOOLEAN DEFAULT false,
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, transformation_name)
);

-- Transformation I/O definitions
CREATE TABLE transformation_io_definitions (
    io_id ulid PRIMARY KEY DEFAULT generate_ulid('tfio'),
    transformation_id ulid NOT NULL REFERENCES transformations(transformation_id) ON DELETE CASCADE ON UPDATE CASCADE,
    io_type VARCHAR(10) NOT NULL CHECK (io_type IN ('input', 'output')),
    io_name VARCHAR(255) NOT NULL,
    io_data_type VARCHAR(100) NOT NULL,
    is_mandatory BOOLEAN DEFAULT true,
    is_array BOOLEAN DEFAULT false,
    default_value JSONB DEFAULT NULL,
    description TEXT DEFAULT '',
    validation_rules JSONB DEFAULT '{}',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(transformation_id, io_type, io_name)
);

-- Transformation workflow nodes
CREATE TABLE transformation_workflow_nodes (
    node_id ulid PRIMARY KEY DEFAULT generate_ulid('tfnode'),
    mapping_rule_id ulid NOT NULL REFERENCES mapping_rules(mapping_rule_id) ON DELETE CASCADE ON UPDATE CASCADE,
    node_type VARCHAR(20) NOT NULL CHECK (node_type IN ('source', 'transformation', 'target')),
    transformation_id ulid REFERENCES transformations(transformation_id) ON DELETE CASCADE ON UPDATE CASCADE,
    node_config JSONB DEFAULT '{}',
    node_order INTEGER DEFAULT 0,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Transformation workflow edges
CREATE TABLE transformation_workflow_edges (
    edge_id ulid PRIMARY KEY DEFAULT generate_ulid('tfedge'),
    mapping_rule_id ulid NOT NULL REFERENCES mapping_rules(mapping_rule_id) ON DELETE CASCADE ON UPDATE CASCADE,
    source_node_id ulid NOT NULL REFERENCES transformation_workflow_nodes(node_id) ON DELETE CASCADE ON UPDATE CASCADE,
    source_output_name VARCHAR(255) NOT NULL,
    target_node_id ulid NOT NULL REFERENCES transformation_workflow_nodes(node_id) ON DELETE CASCADE ON UPDATE CASCADE,
    target_input_name VARCHAR(255) NOT NULL,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(mapping_rule_id, source_node_id, source_output_name, target_node_id, target_input_name)
);

-- =============================================================================
-- INTEGRATIONS
-- =============================================================================

-- Integrations registry
CREATE TABLE integrations (
    integration_id ulid PRIMARY KEY DEFAULT generate_ulid('integration'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    integration_name VARCHAR(255) NOT NULL,
    integration_description TEXT DEFAULT '',
    integration_type VARCHAR(64) NOT NULL,
    integration_config JSONB NOT NULL DEFAULT '{}',
    credential_key TEXT DEFAULT '',
    integration_metadata JSONB NOT NULL DEFAULT '{}',
    supported_operations TEXT[] NOT NULL DEFAULT '{}',
    health JSONB NOT NULL DEFAULT '{}',
    owner_id ulid REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    status VARCHAR(255) DEFAULT 'STATUS_CREATED',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, integration_name)
);

-- Integration execution jobs (for async/stream tracking)
CREATE TABLE integration_jobs (
    job_id ulid PRIMARY KEY DEFAULT generate_ulid('intjob'),
    integration_id ulid NOT NULL REFERENCES integrations(integration_id) ON DELETE CASCADE ON UPDATE CASCADE,
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    operation VARCHAR(255) NOT NULL,
    mode VARCHAR(32) NOT NULL DEFAULT 'EXECUTION_MODE_SYNC',
    status VARCHAR(64) NOT NULL DEFAULT 'JOB_STATUS_PENDING',
    request_payload JSONB NOT NULL DEFAULT '{}',
    request_parameters JSONB NOT NULL DEFAULT '{}',
    progress JSONB NOT NULL DEFAULT '{}',
    result JSONB NOT NULL DEFAULT '{}',
    error_message TEXT DEFAULT '',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed TIMESTAMP
);

-- =============================================================================
-- MCP (MODEL CONTEXT PROTOCOL) SYSTEM
-- =============================================================================

-- MCP servers
CREATE TABLE mcpservers (
    mcpserver_id ulid PRIMARY KEY DEFAULT generate_ulid('mcpserver'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    workspace_id ulid NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE ON UPDATE CASCADE,
    mcpserver_name VARCHAR(255) NOT NULL,
    mcpserver_description TEXT DEFAULT '',
    mcpserver_host_ids BIGINT[] DEFAULT '{}',
    mcpserver_port INTEGER DEFAULT 9000,
    mcpserver_enabled BOOLEAN DEFAULT false,
    policy_ids ulid[] DEFAULT '{}',
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    status_message VARCHAR(255) DEFAULT '',
    status status_enum DEFAULT 'STATUS_CREATED',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(workspace_id, mcpserver_name)
);

-- MCP resources
CREATE TABLE mcpresources (
    mcpresource_id ulid PRIMARY KEY DEFAULT generate_ulid('mcpres'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    workspace_id ulid NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE ON UPDATE CASCADE,
    mcpresource_name VARCHAR(255) NOT NULL,
    mcpresource_description TEXT DEFAULT '',
    mcpresource_config JSONB DEFAULT '{}',
    mapping_id ulid NOT NULL REFERENCES mappings(mapping_id) ON DELETE CASCADE ON UPDATE CASCADE,
    policy_ids ulid[] DEFAULT '{}',
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(workspace_id, mcpresource_name)
);

-- MCP tools
CREATE TABLE mcptools (
    mcptool_id ulid PRIMARY KEY DEFAULT generate_ulid('mcptool'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    workspace_id ulid NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE ON UPDATE CASCADE,
    mcptool_name VARCHAR(255) NOT NULL,
    mcptool_description TEXT DEFAULT '',
    mcptool_config JSONB DEFAULT '{}',
    mapping_id ulid NOT NULL REFERENCES mappings(mapping_id) ON DELETE CASCADE ON UPDATE CASCADE,
    policy_ids ulid[] DEFAULT '{}',
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(workspace_id, mcptool_name)
);

-- MCP prompts
CREATE TABLE mcpprompts (
    mcpprompt_id ulid PRIMARY KEY DEFAULT generate_ulid('mcpprompt'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    workspace_id ulid NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE ON UPDATE CASCADE,
    mcpprompt_name VARCHAR(255) NOT NULL,
    mcpprompt_description TEXT DEFAULT '',
    mcpprompt_config JSONB DEFAULT '{}',
    mapping_id ulid NOT NULL REFERENCES mappings(mapping_id) ON DELETE CASCADE ON UPDATE CASCADE,
    policy_ids ulid[] DEFAULT '{}',
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(workspace_id, mcpprompt_name)
);

-- =============================================================================
-- MCP ASSOCIATION TABLES
-- =============================================================================

-- MCP server to resource associations
CREATE TABLE mcp_server_resources (
    mcpserver_id ulid NOT NULL REFERENCES mcpservers(mcpserver_id) ON DELETE CASCADE ON UPDATE CASCADE,
    mcpresource_id ulid NOT NULL REFERENCES mcpresources(mcpresource_id) ON DELETE CASCADE ON UPDATE CASCADE,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (mcpserver_id, mcpresource_id)
);

-- MCP server to tool associations
CREATE TABLE mcp_server_tools (
    mcpserver_id ulid NOT NULL REFERENCES mcpservers(mcpserver_id) ON DELETE CASCADE ON UPDATE CASCADE,
    mcptool_id ulid NOT NULL REFERENCES mcptools(mcptool_id) ON DELETE CASCADE ON UPDATE CASCADE,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (mcpserver_id, mcptool_id)
);

-- MCP server to prompt associations
CREATE TABLE mcp_server_prompts (
    mcpserver_id ulid NOT NULL REFERENCES mcpservers(mcpserver_id) ON DELETE CASCADE ON UPDATE CASCADE,
    mcpprompt_id ulid NOT NULL REFERENCES mcpprompts(mcpprompt_id) ON DELETE CASCADE ON UPDATE CASCADE,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (mcpserver_id, mcpprompt_id)
);

-- =============================================================================
-- AUDIT AND COMPLIANCE
-- =============================================================================

-- Comprehensive audit log (partitioned by date for performance)
CREATE TABLE audit_log (
    audit_id ulid DEFAULT generate_ulid('audit'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE ON UPDATE CASCADE,
    user_id ulid NOT NULL,
    action VARCHAR(255) NOT NULL,
    resource_type VARCHAR(255) NOT NULL,
    resource_id VARCHAR(255),
    resource_name VARCHAR(255),
    target_user_id ulid,
    change_details JSONB DEFAULT '{}',
    ip_address VARCHAR(255),
    user_agent VARCHAR(255),
    status status_enum DEFAULT 'STATUS_SUCCESS',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (audit_id, created)
) PARTITION BY RANGE (created);

-- Create initial partitions for current and next year
CREATE TABLE audit_log_2024 PARTITION OF audit_log
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE audit_log_2025 PARTITION OF audit_log
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

-- Create default partition for any dates outside defined ranges
CREATE TABLE audit_log_default PARTITION OF audit_log DEFAULT;

-- Function to automatically create monthly partitions
CREATE OR REPLACE FUNCTION create_audit_log_partition(start_date DATE)
RETURNS VOID AS $$
DECLARE
    partition_name TEXT;
    end_date DATE;
BEGIN
    -- Calculate partition name and end date
    partition_name := 'audit_log_' || to_char(start_date, 'YYYY_MM');
    end_date := start_date + INTERVAL '1 month';
    
    -- Create the partition
    EXECUTE format('CREATE TABLE %I PARTITION OF audit_log FOR VALUES FROM (%L) TO (%L)',
                   partition_name, start_date, end_date);
                   
    -- Create indexes on the new partition
    EXECUTE format('CREATE INDEX %I ON %I (tenant_id)', 
                   'idx_' || partition_name || '_tenant_id', partition_name);
    EXECUTE format('CREATE INDEX %I ON %I (user_id) WHERE user_id IS NOT NULL', 
                   'idx_' || partition_name || '_user_id', partition_name);
    EXECUTE format('CREATE INDEX %I ON %I (action)', 
                   'idx_' || partition_name || '_action', partition_name);
    EXECUTE format('CREATE INDEX %I ON %I (resource_type)', 
                   'idx_' || partition_name || '_resource_type', partition_name);
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- MESH SERVICE SCHEMA
-- =============================================================================

CREATE TABLE mesh_lsa_versions (
    node_id BIGINT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    hash VARCHAR(64) NOT NULL,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (node_id, version)
);

-- Raft consensus system (unified for MCG and DSG)
CREATE TABLE mesh_raft_groups (
    id ulid PRIMARY KEY DEFAULT generate_ulid('raft'),
    type VARCHAR(10) NOT NULL CHECK (type IN ('MCG', 'DSG')),
    members JSONB NOT NULL DEFAULT '[]',
    term BIGINT DEFAULT 0,
    leader_id BIGINT REFERENCES nodes(node_id) ON DELETE SET NULL,
    meta JSONB DEFAULT '{}',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE mesh_raft_log (
    group_id ulid NOT NULL REFERENCES mesh_raft_groups(id) ON DELETE CASCADE,
    log_index BIGINT NOT NULL,
    term BIGINT NOT NULL,
    payload BYTEA NOT NULL,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_id, log_index)
);

-- Stream management
CREATE TABLE mesh_streams (
    id ulid PRIMARY KEY DEFAULT generate_ulid('strm'),
    tenant_id ulid REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    src_node BIGINT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    dst_nodes JSONB NOT NULL DEFAULT '[]',
    qos VARCHAR(20) DEFAULT 'QOS_NORMAL' CHECK (qos IN ('QOS_CRITICAL', 'QOS_HIGH', 'QOS_NORMAL', 'QOS_LOW')),
    priority INTEGER DEFAULT 0,
    meta JSONB DEFAULT '{}',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE mesh_stream_offsets (
    stream_id ulid NOT NULL REFERENCES mesh_streams(id) ON DELETE CASCADE,
    node_id BIGINT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    committed_seq BIGINT DEFAULT 0,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (stream_id, node_id)
);

CREATE TABLE mesh_delivery_log (
    stream_id ulid NOT NULL REFERENCES mesh_streams(id) ON DELETE CASCADE,
    message_id ulid NOT NULL,
    src_node BIGINT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    dst_node BIGINT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    state status_enum DEFAULT 'STATUS_RECEIVED',
    err TEXT DEFAULT '',
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (stream_id, message_id, dst_node)
);

-- Message queuing (outbox pattern)
CREATE TABLE mesh_outbox (
    stream_id ulid NOT NULL REFERENCES mesh_streams(id) ON DELETE CASCADE,
    message_id ulid NOT NULL,
    payload BYTEA NOT NULL,
    headers JSONB DEFAULT '{}',
    next_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    attempts INTEGER DEFAULT 0,
    status status_enum DEFAULT 'STATUS_PENDING',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (stream_id, message_id)
);

CREATE TABLE mesh_inbox (
    stream_id ulid NOT NULL REFERENCES mesh_streams(id) ON DELETE CASCADE,
    message_id ulid NOT NULL,
    payload BYTEA NOT NULL,
    headers JSONB DEFAULT '{}',
    received TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed TIMESTAMP,
    PRIMARY KEY (stream_id, message_id)
);

-- Topology and routing
CREATE TABLE mesh_topology_snapshots (
    version BIGINT PRIMARY KEY,
    graph JSONB NOT NULL,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE mesh_config_kv (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- MESH STATE SYNCHRONIZATION AND CONSENSUS TABLES
-- =============================================================================

-- Event log for mesh state synchronization
CREATE TABLE mesh_event_log (
    event_id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    originator_node BIGINT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    affected_node BIGINT REFERENCES nodes(node_id) ON DELETE CASCADE,
    sequence_number BIGINT NOT NULL,
    event_data JSONB DEFAULT '{}',
    processed BOOLEAN DEFAULT FALSE,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(originator_node, sequence_number)
);

-- Table versioning for database synchronization
CREATE TABLE mesh_table_versions (
    table_name VARCHAR(100) PRIMARY KEY,
    version BIGINT NOT NULL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Node membership tracking in meshes
CREATE TABLE mesh_node_membership (
    mesh_id BIGINT NOT NULL REFERENCES mesh(mesh_id) ON DELETE CASCADE,
    node_id BIGINT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'ACTIVE',
    PRIMARY KEY (mesh_id, node_id)
);

-- Consensus state tracking for split-brain detection
CREATE TABLE mesh_consensus_state (
    mesh_id BIGINT PRIMARY KEY REFERENCES mesh(mesh_id) ON DELETE CASCADE,
    total_nodes INTEGER NOT NULL DEFAULT 0,
    online_nodes INTEGER NOT NULL DEFAULT 0,
    split_detected BOOLEAN DEFAULT FALSE,
    majority_side BOOLEAN DEFAULT TRUE,
    last_consensus_check TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- LICENSE MANAGEMENT TABLES
-- =============================================================================

-- License keys storage
CREATE TABLE license_keys (
    key_id VARCHAR(255) PRIMARY KEY,
    local_identity BIGINT NOT NULL,
    mesh_id BIGINT REFERENCES mesh(mesh_id),
    key_hash VARCHAR(64) UNIQUE NOT NULL,  -- SHA-256 hash of the JWT key
    features TEXT[] NOT NULL DEFAULT '{}',
    issued_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Feature usage tracking
CREATE TABLE license_feature_usage (
    feature VARCHAR(255) NOT NULL,
    local_identity BIGINT NOT NULL,
    mesh_id BIGINT REFERENCES mesh(mesh_id),
    current_usage INTEGER DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (feature, local_identity, mesh_id)
);

-- =============================================================================
-- RESOURCE REGISTRY SYSTEM
-- =============================================================================

-- Resource containers for high-level addressable resources
CREATE TABLE resource_containers (
    container_id ulid PRIMARY KEY DEFAULT generate_ulid('container'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    workspace_id ulid NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    resource_uri TEXT UNIQUE NOT NULL,
    protocol VARCHAR(50) NOT NULL,
    scope VARCHAR(50) NOT NULL,
    object_type VARCHAR(100) NOT NULL,
    object_name VARCHAR(255) NOT NULL,
    database_id ulid REFERENCES databases(database_id) ON DELETE CASCADE,
    instance_id ulid REFERENCES instances(instance_id) ON DELETE SET NULL,
    integration_id ulid REFERENCES integrations(integration_id) ON DELETE CASCADE,
    mcpserver_id ulid REFERENCES mcpservers(mcpserver_id) ON DELETE CASCADE,
    connected_to_node_id BIGINT REFERENCES nodes(node_id) ON DELETE CASCADE,
    
    -- Virtual resource tracking
    is_virtual BOOLEAN DEFAULT FALSE,
    virtual_source VARCHAR(50) DEFAULT 'user',
    virtual_namespace VARCHAR(255) DEFAULT 'default',
    binding_mode VARCHAR(20) DEFAULT 'unbound',
    bound_database_id ulid REFERENCES databases(database_id) ON DELETE SET NULL,
    reconciliation_status VARCHAR(50) DEFAULT 'pending',
    reconciled_container_id ulid REFERENCES resource_containers(container_id) ON DELETE SET NULL,
    reconciliation_details JSONB DEFAULT '{}',
    reconciled_at TIMESTAMP,
    
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    status status_enum DEFAULT 'STATUS_CREATED',
    status_message VARCHAR(255) DEFAULT '',
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    online BOOLEAN DEFAULT true,
    container_metadata JSONB DEFAULT '{}',
	enriched_metadata JSONB DEFAULT '{}',
	database_type VARCHAR(100),
	vendor VARCHAR(100),
	item_count INTEGER DEFAULT 0,
	size_bytes BIGINT DEFAULT 0,
	container_classification VARCHAR(255),
	container_classification_confidence NUMERIC(3,2),
	container_classification_source VARCHAR(20) DEFAULT 'auto' CHECK (container_classification_source IN ('auto', 'manual')),
	created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	UNIQUE(workspace_id, object_type, object_name, database_id),
	CHECK (protocol IN ('redb', 'stream', 'webhook', 'mcp', 'template')),
	CHECK (scope IN ('data', 'metadata', 'schema')),
	CHECK (container_classification_confidence IS NULL OR (container_classification_confidence >= 0.00 AND container_classification_confidence <= 1.00)),
	CHECK (binding_mode IN ('template', 'unbound', 'bound', 'auto_bind')),
	CHECK (reconciliation_status IN ('pending', 'matched', 'conflict', 'orphaned', 'resolved')),
	CHECK (virtual_source IN ('user', 'inferred', 'template', 'mcp'))
);

-- Resource items for low-level addressable resources
CREATE TABLE resource_items (
    item_id ulid PRIMARY KEY DEFAULT generate_ulid('item'),
    container_id ulid NOT NULL REFERENCES resource_containers(container_id) ON DELETE CASCADE,
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    workspace_id ulid NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    resource_uri TEXT UNIQUE NOT NULL,
    protocol VARCHAR(50) NOT NULL,
    scope VARCHAR(50) NOT NULL,
	item_type VARCHAR(100) NOT NULL,
	item_name VARCHAR(255) NOT NULL,
	item_display_name VARCHAR(255) DEFAULT '',
	item_path TEXT[],
    data_type VARCHAR(255) NOT NULL,
    unified_data_type VARCHAR(100),
    is_nullable BOOLEAN DEFAULT true,
    is_primary_key BOOLEAN DEFAULT false,
    is_unique BOOLEAN DEFAULT false,
    is_indexed BOOLEAN DEFAULT false,
    is_required BOOLEAN DEFAULT false,
    is_array BOOLEAN DEFAULT false,
    array_dimensions INTEGER DEFAULT 1,
    default_value TEXT,
    constraints JSONB DEFAULT '[]',
    is_custom_type BOOLEAN DEFAULT false,
    custom_type_name VARCHAR(255),
    custom_type_definition JSONB,
    has_schema BOOLEAN DEFAULT false,
    schema_format VARCHAR(50),
    schema_definition JSONB,
    schema_version VARCHAR(50),
    schema_evolution_version INTEGER DEFAULT 1,
    schema_validation_mode VARCHAR(50) DEFAULT 'strict',
    schema_mismatch_action VARCHAR(50) DEFAULT 'reject',
    allow_new_fields BOOLEAN DEFAULT false,
    allow_field_type_widening BOOLEAN DEFAULT false,
    allow_field_removal BOOLEAN DEFAULT false,
    schema_evolution_log JSONB DEFAULT '[]',
    nested_items JSONB DEFAULT '[]',
    max_length INTEGER,
    precision INTEGER,
    scale INTEGER,
    connected_to_node_id BIGINT REFERENCES nodes(node_id) ON DELETE CASCADE,
    
    -- Virtual resource tracking
    is_virtual BOOLEAN DEFAULT FALSE,
    virtual_source VARCHAR(50) DEFAULT 'user',
    binding_mode VARCHAR(20),
    reconciliation_status VARCHAR(50) DEFAULT 'pending',
    reconciled_item_id ulid REFERENCES resource_items(item_id) ON DELETE SET NULL,
    reconciliation_details JSONB DEFAULT '{}',
    reconciled_at TIMESTAMP,
    
    status status_enum DEFAULT 'STATUS_CREATED',
    online BOOLEAN DEFAULT true,
    item_metadata JSONB DEFAULT '{}',
    enriched_metadata JSONB DEFAULT '{}',
    item_comment TEXT,
    is_privileged BOOLEAN DEFAULT false,
    privileged_classification VARCHAR(100),
    detection_confidence DECIMAL(3,2),
    detection_method VARCHAR(100),
    ordinal_position INTEGER,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(container_id, item_name, item_path),
    CHECK (protocol IN ('redb', 'stream', 'webhook', 'mcp', 'template')),
    CHECK (scope IN ('data', 'metadata', 'schema')),
    CHECK (detection_confidence IS NULL OR (detection_confidence >= 0.00 AND detection_confidence <= 1.00)),
    CHECK (schema_format IN ('json_schema', 'avro', 'protobuf', 'xml_schema', 'cbor_schema', 'thrift', NULL)),
    CHECK (array_dimensions > 0),
    CHECK (schema_validation_mode IN ('strict', 'permissive', 'evolving', 'disabled')),
    CHECK (schema_mismatch_action IN ('reject', 'accept', 'accept_and_log', 'coerce', 'evolve_schema')),
    CHECK (binding_mode IN ('template', 'unbound', 'bound', 'auto_bind', NULL)),
    CHECK (reconciliation_status IN ('pending', 'matched', 'conflict', 'orphaned', 'resolved')),
    CHECK (virtual_source IN ('user', 'inferred', 'from_mapping', 'mcp'))
);

-- Add the foreign key constraint to the the mappings table
ALTER TABLE mappings ADD CONSTRAINT fk_mapping_source_container_id FOREIGN KEY (mapping_source_container_id) REFERENCES resource_containers(container_id) ON DELETE CASCADE;
ALTER TABLE mappings ADD CONSTRAINT fk_mapping_target_container_id FOREIGN KEY (mapping_target_container_id) REFERENCES resource_containers(container_id) ON DELETE CASCADE;

-- Add the foreign key constraint to the the mapping_rule_source_items table
ALTER TABLE mapping_rule_source_items ADD CONSTRAINT fk_mapping_rule_source_items_resource_item_id FOREIGN KEY (resource_item_id) REFERENCES resource_items(item_id) ON DELETE CASCADE ON UPDATE CASCADE;

-- Add the foreign key constraint to the the mapping_rule_target_items table
ALTER TABLE mapping_rule_target_items ADD CONSTRAINT fk_mapping_rule_target_items_resource_item_id FOREIGN KEY (resource_item_id) REFERENCES resource_items(item_id) ON DELETE CASCADE ON UPDATE CASCADE;

-- =============================================================================
-- DATA PRODUCTS (DATA AS A PRODUCT)
-- =============================================================================

-- Data products for organizing resource items into reusable data assets
CREATE TABLE data_products (
    product_id ulid PRIMARY KEY DEFAULT generate_ulid('product'),
    tenant_id ulid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    workspace_id ulid NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    product_name VARCHAR(255) NOT NULL,
    product_description TEXT DEFAULT '',
    metadata JSONB DEFAULT '{}',
    owner_id ulid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    status status_enum DEFAULT 'STATUS_CREATED',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(workspace_id, product_name)
);

-- Data product to resource item associations (many-to-many)
CREATE TABLE data_product_items (
    product_id ulid NOT NULL REFERENCES data_products(product_id) ON DELETE CASCADE,
    resource_item_id ulid NOT NULL REFERENCES resource_items(item_id) ON DELETE CASCADE,
    item_order INTEGER NOT NULL DEFAULT 0,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id, resource_item_id)
);

`

// DatabaseIndexes contains the performance indexes for the database
const DatabaseIndexes = `
-- =============================================================================
-- PERFORMANCE INDEXES
-- =============================================================================

-- Authentication and user lookups
CREATE INDEX idx_users_tenant_id ON users(tenant_id);
CREATE INDEX idx_users_email_login ON users(user_email) WHERE user_enabled = true;
CREATE INDEX idx_users_name_search ON users(user_name) WHERE user_name != '';

-- API token lookups
CREATE INDEX idx_apitokens_user_id ON apitokens(user_id);
CREATE INDEX idx_apitokens_key_lookup ON apitokens(apitoken_key) WHERE apitoken_enabled = true;
CREATE INDEX idx_apitokens_tenant_list ON apitokens(tenant_id);

-- JWT tokens and session management
CREATE INDEX idx_user_jwt_tokens_user_id ON user_jwt_tokens(user_id);
CREATE INDEX idx_user_jwt_tokens_refresh_token ON user_jwt_tokens(refresh_token);
CREATE INDEX idx_user_jwt_tokens_access_token ON user_jwt_tokens(access_token);
CREATE INDEX idx_user_jwt_tokens_expires ON user_jwt_tokens(expires);
CREATE INDEX idx_user_jwt_tokens_last_activity ON user_jwt_tokens(last_activity);
CREATE INDEX idx_user_jwt_tokens_session_name ON user_jwt_tokens(session_name) WHERE session_name != '';
CREATE INDEX idx_user_jwt_tokens_device_type ON user_jwt_tokens(session_device_type);

-- Workspace hierarchy queries
CREATE INDEX idx_workspaces_tenant_id ON workspaces(tenant_id);
CREATE INDEX idx_workspaces_owner_id ON workspaces(owner_id);
CREATE INDEX idx_environments_workspace_id ON environments(workspace_id);
CREATE INDEX idx_environments_tenant_workspace ON environments(tenant_id, workspace_id);

-- Database infrastructure queries
CREATE INDEX idx_instances_tenant_workspace ON instances(tenant_id, workspace_id);
CREATE INDEX idx_instances_node_id ON instances(connected_to_node_id);
CREATE INDEX idx_instances_environment_id ON instances(environment_id);
CREATE INDEX idx_databases_tenant_workspace ON databases(tenant_id, workspace_id);
CREATE INDEX idx_databases_instance_id ON databases(instance_id);

-- Repository and version control queries
CREATE INDEX idx_repos_tenant_workspace ON repos(tenant_id, workspace_id);
CREATE INDEX idx_branches_repo_id ON branches(repo_id);
CREATE INDEX idx_branches_parent_id ON branches(parent_branch_id) WHERE parent_branch_id IS NOT NULL;
CREATE INDEX idx_branches_database_attached ON branches(connected_database_id) WHERE connected_to_database = true;
CREATE INDEX idx_commits_branch_id ON commits(branch_id);
CREATE INDEX idx_commits_head_lookup ON commits(branch_id, commit_is_head) WHERE commit_is_head = true;
CREATE INDEX idx_commits_tenant_workspace_repo ON commits(tenant_id, workspace_id, repo_id);

-- Data mapping and relationship queries
CREATE INDEX idx_mappings_tenant_workspace ON mappings(tenant_id, workspace_id);
CREATE INDEX idx_mappings_source_container ON mappings(mapping_source_container_id);
CREATE INDEX idx_mappings_target_container ON mappings(mapping_target_container_id);
CREATE INDEX idx_mapping_rule_source_items_rule_id ON mapping_rule_source_items(mapping_rule_id);
CREATE INDEX idx_mapping_rule_source_items_item_id ON mapping_rule_source_items(resource_item_id);
CREATE INDEX idx_mapping_rule_target_items_rule_id ON mapping_rule_target_items(mapping_rule_id);
CREATE INDEX idx_mapping_rule_target_items_item_id ON mapping_rule_target_items(resource_item_id);
CREATE INDEX idx_mapping_filters_mapping_id ON mapping_filters(mapping_id, filter_order);
CREATE INDEX idx_relationships_tenant_workspace ON relationships(tenant_id, workspace_id);
CREATE INDEX idx_relationships_mapping_id ON relationships(mapping_id);

-- Transformation queries
CREATE INDEX idx_transformations_tenant_id ON transformations(tenant_id);
CREATE INDEX idx_transformations_enabled ON transformations(transformation_enabled) WHERE transformation_enabled = true;
CREATE INDEX idx_transformations_cardinality ON transformations(transformation_cardinality);
CREATE INDEX idx_transformation_io_definitions_transformation_id ON transformation_io_definitions(transformation_id);
CREATE INDEX idx_transformation_io_definitions_io_type ON transformation_io_definitions(io_type);
CREATE INDEX idx_transformation_workflow_nodes_mapping_rule_id ON transformation_workflow_nodes(mapping_rule_id);
CREATE INDEX idx_transformation_workflow_nodes_transformation_id ON transformation_workflow_nodes(transformation_id) WHERE transformation_id IS NOT NULL;
CREATE INDEX idx_transformation_workflow_nodes_node_type ON transformation_workflow_nodes(node_type);
CREATE INDEX idx_transformation_workflow_edges_mapping_rule_id ON transformation_workflow_edges(mapping_rule_id);
CREATE INDEX idx_transformation_workflow_edges_source_node ON transformation_workflow_edges(source_node_id);
CREATE INDEX idx_transformation_workflow_edges_target_node ON transformation_workflow_edges(target_node_id);

-- MCP system queries
CREATE INDEX idx_mcpservers_tenant_workspace ON mcpservers(tenant_id, workspace_id);
CREATE INDEX idx_mcpresources_tenant_id ON mcpresources(tenant_id);
CREATE INDEX idx_mcptools_tenant_id ON mcptools(tenant_id);
CREATE INDEX idx_mcpprompts_tenant_id ON mcpprompts(tenant_id);
CREATE INDEX idx_mcpresources_mapping_id ON mcpresources(mapping_id);
CREATE INDEX idx_mcptools_mapping_id ON mcptools(mapping_id);
CREATE INDEX idx_mcpprompts_mapping_id ON mcpprompts(mapping_id);

-- MCP association queries
CREATE INDEX idx_mcp_server_resources_server ON mcp_server_resources(mcpserver_id);
CREATE INDEX idx_mcp_server_resources_resource ON mcp_server_resources(mcpresource_id);
CREATE INDEX idx_mcp_server_tools_server ON mcp_server_tools(mcpserver_id);
CREATE INDEX idx_mcp_server_tools_tool ON mcp_server_tools(mcptool_id);
CREATE INDEX idx_mcp_server_prompts_server ON mcp_server_prompts(mcpserver_id);
CREATE INDEX idx_mcp_server_prompts_prompt ON mcp_server_prompts(mcpprompt_id);

-- Integration queries
CREATE INDEX idx_integrations_tenant ON integrations(tenant_id);
CREATE INDEX idx_integrations_type ON integrations(integration_type);
CREATE INDEX idx_integrations_name ON integrations(integration_name);
CREATE INDEX idx_integration_jobs_integration ON integration_jobs(integration_id);
CREATE INDEX idx_integration_jobs_status ON integration_jobs(status);

-- Authorization system queries
CREATE INDEX idx_groups_tenant_id ON groups(tenant_id);
CREATE INDEX idx_groups_parent_id ON groups(parent_group_id) WHERE parent_group_id IS NOT NULL;
CREATE INDEX idx_roles_tenant_id ON roles(tenant_id);
CREATE INDEX idx_permissions_tenant_id ON permissions(tenant_id);
CREATE INDEX idx_permissions_action_resource ON permissions(action, resource);

-- Authorization assignment queries
CREATE INDEX idx_user_groups_user_id ON user_groups(user_id);
CREATE INDEX idx_user_groups_group_id ON user_groups(group_id);
CREATE INDEX idx_user_groups_tenant_user ON user_groups(tenant_id, user_id);
CREATE INDEX idx_user_roles_user_id ON user_roles(user_id);
CREATE INDEX idx_user_roles_role_id ON user_roles(role_id);
CREATE INDEX idx_user_roles_tenant_user ON user_roles(tenant_id, user_id);
CREATE INDEX idx_user_roles_expires ON user_roles(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX idx_group_roles_group_id ON group_roles(group_id);
CREATE INDEX idx_group_roles_role_id ON group_roles(role_id);
CREATE INDEX idx_role_permissions_role_id ON role_permissions(role_id);
CREATE INDEX idx_role_permissions_permission_id ON role_permissions(permission_id);

-- Policy and ownership queries
CREATE INDEX idx_policies_tenant_id ON policies(tenant_id);
CREATE INDEX idx_satellites_tenant_id ON satellites(tenant_id);
CREATE INDEX idx_satellites_node_id ON satellites(connected_to_node_id);
CREATE INDEX idx_anchors_tenant_id ON anchors(tenant_id);
CREATE INDEX idx_anchors_node_id ON anchors(connected_to_node_id) WHERE connected_to_node_id IS NOT NULL;

-- Regional and mesh network queries
CREATE INDEX idx_regions_global ON regions(global_region) WHERE global_region = true;
CREATE INDEX idx_nodes_region_id ON nodes(region_id) WHERE region_id IS NOT NULL;

-- Audit and compliance queries
CREATE INDEX idx_audit_log_tenant_id ON audit_log(tenant_id);
CREATE INDEX idx_audit_log_user_id ON audit_log(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX idx_audit_log_action ON audit_log(action);
CREATE INDEX idx_audit_log_resource_type ON audit_log(resource_type);
CREATE INDEX idx_audit_log_created ON audit_log(created);
CREATE INDEX idx_audit_log_tenant_user_action ON audit_log(tenant_id, user_id, action);
CREATE INDEX idx_audit_log_date_range ON audit_log(created, tenant_id);

-- Status and monitoring queries
CREATE INDEX idx_workspaces_status ON workspaces(status);
CREATE INDEX idx_instances_status ON instances(status);
CREATE INDEX idx_databases_status ON databases(status);
CREATE INDEX idx_mcpservers_enabled ON mcpservers(mcpserver_enabled) WHERE mcpserver_enabled = true;

-- Full-text search indexes for descriptions and names
CREATE INDEX idx_workspaces_name_search ON workspaces USING gin(to_tsvector('english', workspace_name || ' ' || workspace_description));
CREATE INDEX idx_databases_name_search ON databases USING gin(to_tsvector('english', database_name || ' ' || database_description));
CREATE INDEX idx_repos_name_search ON repos USING gin(to_tsvector('english', repo_name || ' ' || repo_description));

-- Composite indexes for complex queries from proto services
CREATE INDEX idx_environments_production_priority ON environments(environment_is_production, environment_priority) WHERE environment_is_production = true;
CREATE INDEX idx_instances_tenant_workspace_env ON instances(tenant_id, workspace_id, environment_id);
CREATE INDEX idx_databases_tenant_workspace_instance ON databases(tenant_id, workspace_id, instance_id);

-- Timestamp-based queries for monitoring and cleanup
CREATE INDEX idx_apitokens_expiry_cleanup ON apitokens(apitoken_key_cycled, apitoken_auto_expires) WHERE apitoken_auto_expires = true;
CREATE INDEX idx_user_roles_active ON user_roles(user_id, expires_at) WHERE expires_at IS NULL;
CREATE INDEX idx_user_roles_user_expires ON user_roles(user_id, expires_at) WHERE expires_at IS NOT NULL;

-- JSONB indexes for policy and configuration queries
CREATE INDEX idx_policies_object_gin ON policies USING gin(policy_object);
CREATE INDEX idx_commits_schema_gin ON commits USING gin(schema_structure);
CREATE INDEX idx_database_schema_gin ON databases USING gin(database_schema);
CREATE INDEX idx_database_tables_gin ON databases USING gin(database_tables);
CREATE INDEX idx_mcpresources_config_gin ON mcpresources USING gin(mcpresource_config);
CREATE INDEX idx_mcptools_config_gin ON mcptools USING gin(mcptool_config);
CREATE INDEX idx_mcpprompts_config_gin ON mcpprompts USING gin(mcpprompt_config);

-- Resource name indexes
CREATE INDEX idx_workspaces_name ON workspaces(workspace_name);
CREATE INDEX idx_regions_name ON regions(region_name);
CREATE INDEX idx_databases_name ON databases(database_name);
CREATE INDEX idx_repos_name ON repos(repo_name);
CREATE INDEX idx_environments_name ON environments(environment_name);
CREATE INDEX idx_instances_name ON instances(instance_name);
CREATE INDEX idx_mcpservers_name ON mcpservers(mcpserver_name);
CREATE INDEX idx_tenants_name ON tenants(tenant_name);
CREATE INDEX idx_tenants_url ON tenants(tenant_url);

-- License indexes
CREATE INDEX idx_license_keys_local_identity ON license_keys(local_identity);
CREATE INDEX idx_license_keys_mesh_id ON license_keys(mesh_id);
CREATE INDEX idx_license_keys_active ON license_keys(active);
CREATE INDEX idx_license_keys_expires_at ON license_keys(expires_at);
CREATE INDEX idx_license_keys_key_hash ON license_keys(key_hash);

CREATE INDEX idx_license_feature_usage_feature ON license_feature_usage(feature);
CREATE INDEX idx_license_feature_usage_local_identity ON license_feature_usage(local_identity);
CREATE INDEX idx_license_feature_usage_mesh_id ON license_feature_usage(mesh_id);

-- Mesh network indexes for performance
CREATE INDEX idx_mesh_status ON mesh(status);
CREATE INDEX idx_nodes_status ON nodes(status);
CREATE INDEX idx_nodes_last_seen ON nodes(node_last_seen);
CREATE INDEX idx_routes_nodes ON routes(a_node, b_node);
CREATE INDEX idx_routes_status ON routes(status);
CREATE INDEX idx_stream_offsets_updated ON mesh_stream_offsets(updated);
CREATE INDEX idx_outbox_next_attempt ON mesh_outbox(next_attempt) WHERE status = 'STATUS_PENDING';
CREATE INDEX idx_outbox_stream_status ON mesh_outbox(stream_id, status);
CREATE INDEX idx_inbox_processed ON mesh_inbox(processed) WHERE processed IS NULL;

-- Resource registry queries
CREATE INDEX idx_resource_containers_tenant_id ON resource_containers(tenant_id);
CREATE INDEX idx_resource_containers_workspace_id ON resource_containers(workspace_id);
CREATE INDEX idx_resource_containers_database_id ON resource_containers(database_id) WHERE database_id IS NOT NULL;
CREATE INDEX idx_resource_containers_instance_id ON resource_containers(instance_id) WHERE instance_id IS NOT NULL;
CREATE INDEX idx_resource_containers_integration_id ON resource_containers(integration_id) WHERE integration_id IS NOT NULL;
CREATE INDEX idx_resource_containers_mcpserver_id ON resource_containers(mcpserver_id) WHERE mcpserver_id IS NOT NULL;
CREATE INDEX idx_resource_containers_node_id ON resource_containers(connected_to_node_id);
CREATE INDEX idx_resource_containers_resource_uri ON resource_containers(resource_uri);
CREATE INDEX idx_resource_containers_protocol ON resource_containers(protocol);
CREATE INDEX idx_resource_containers_scope ON resource_containers(scope);
CREATE INDEX idx_resource_containers_object_type ON resource_containers(object_type);
CREATE INDEX idx_resource_containers_status ON resource_containers(status);
CREATE INDEX idx_resource_containers_online ON resource_containers(online);
CREATE INDEX idx_resource_containers_last_seen ON resource_containers(last_seen);
CREATE INDEX idx_resource_containers_metadata_gin ON resource_containers USING gin(container_metadata);
CREATE INDEX idx_resource_containers_enriched_gin ON resource_containers USING gin(enriched_metadata);
CREATE INDEX idx_resource_containers_classification ON resource_containers(container_classification) WHERE container_classification IS NOT NULL;
CREATE INDEX idx_resource_containers_classification_confidence ON resource_containers(container_classification_confidence) WHERE container_classification_confidence IS NOT NULL;
CREATE INDEX idx_resource_containers_classification_source ON resource_containers(container_classification_source);

CREATE INDEX idx_resource_items_tenant_id ON resource_items(tenant_id);
CREATE INDEX idx_resource_items_workspace_id ON resource_items(workspace_id);
CREATE INDEX idx_resource_items_container_id ON resource_items(container_id);
CREATE INDEX idx_resource_items_node_id ON resource_items(connected_to_node_id);
CREATE INDEX idx_resource_items_resource_uri ON resource_items(resource_uri);
CREATE INDEX idx_resource_items_protocol ON resource_items(protocol);
CREATE INDEX idx_resource_items_scope ON resource_items(scope);
CREATE INDEX idx_resource_items_item_type ON resource_items(item_type);
CREATE INDEX idx_resource_items_data_type ON resource_items(data_type);
CREATE INDEX idx_resource_items_unified_data_type ON resource_items(unified_data_type) WHERE unified_data_type IS NOT NULL;
CREATE INDEX idx_resource_items_is_primary_key ON resource_items(is_primary_key) WHERE is_primary_key = true;
CREATE INDEX idx_resource_items_is_unique ON resource_items(is_unique) WHERE is_unique = true;
CREATE INDEX idx_resource_items_is_indexed ON resource_items(is_indexed) WHERE is_indexed = true;
CREATE INDEX idx_resource_items_is_privileged ON resource_items(is_privileged) WHERE is_privileged = true;
CREATE INDEX idx_resource_items_privileged_class ON resource_items(privileged_classification) WHERE privileged_classification IS NOT NULL;
CREATE INDEX idx_resource_items_has_schema ON resource_items(has_schema) WHERE has_schema = true;
CREATE INDEX idx_resource_items_schema_format ON resource_items(schema_format) WHERE schema_format IS NOT NULL;
CREATE INDEX idx_resource_items_status ON resource_items(status);
CREATE INDEX idx_resource_items_online ON resource_items(online);
CREATE INDEX idx_resource_items_ordinal ON resource_items(ordinal_position) WHERE ordinal_position IS NOT NULL;
CREATE INDEX idx_resource_items_display_name ON resource_items(item_display_name) WHERE item_display_name != '';
CREATE INDEX idx_resource_items_schema_def_gin ON resource_items USING gin(schema_definition) WHERE schema_definition IS NOT NULL;
CREATE INDEX idx_resource_items_metadata_gin ON resource_items USING gin(item_metadata);
CREATE INDEX idx_resource_items_enriched_gin ON resource_items USING gin(enriched_metadata);

-- Data product queries
CREATE INDEX idx_data_products_tenant_id ON data_products(tenant_id);
CREATE INDEX idx_data_products_workspace_id ON data_products(workspace_id);
CREATE INDEX idx_data_products_owner_id ON data_products(owner_id);
CREATE INDEX idx_data_products_status ON data_products(status);
CREATE INDEX idx_data_products_name ON data_products(product_name);
CREATE INDEX idx_data_products_metadata_gin ON data_products USING gin(metadata);
CREATE INDEX idx_data_product_items_product_id ON data_product_items(product_id);
CREATE INDEX idx_data_product_items_resource_item_id ON data_product_items(resource_item_id);
CREATE INDEX idx_data_product_items_order ON data_product_items(product_id, item_order);

-- Stream queries
CREATE INDEX idx_streams_tenant_id ON streams(tenant_id);
CREATE INDEX idx_streams_node_id ON streams(connected_to_node_id);
CREATE INDEX idx_streams_platform ON streams(stream_platform);
CREATE INDEX idx_streams_status ON streams(status);
CREATE INDEX idx_streams_config_gin ON streams USING gin(connection_config);
CREATE INDEX idx_streams_metadata_gin ON streams USING gin(stream_metadata);

`
