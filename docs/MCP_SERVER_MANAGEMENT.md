# MCP Server Management Guide

This guide covers the complete CLI interface for managing MCP (Model Context Protocol) servers, resources, and tools in REDB.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [MCP Mappings](#mcp-mappings)
- [MCP Server Management](#mcp-server-management)
- [MCP Resource Management](#mcp-resource-management)
- [MCP Tool Management](#mcp-tool-management)
- [Complete Workflow Example](#complete-workflow-example)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Overview

REDB supports running multiple virtual MCP servers across the mesh network. Each MCP server:

- Runs on a specific port across designated mesh nodes
- Serves MCP resources (database tables) to AI assistants
- Provides MCP tools for database operations
- Enforces policy-based access control
- Automatically synchronizes with the mesh

The MCP management system consists of four main components:

1. **Mappings**: Define how database/table entities map to MCP resources
2. **MCP Servers**: Virtual MCP server instances
3. **MCP Resources**: Database tables exposed as MCP resources
4. **MCP Tools**: Database operations exposed as callable tools

## Prerequisites

Before managing MCP servers, ensure:

1. You have an active REDB workspace selected
2. Your mesh network is running
3. You have connected at least one database
4. You understand the database/table structure you want to expose

## MCP Mappings

### Create Mapping to MCP Resource

Before creating MCP resources, you must first create a mapping from your database/table to an MCP resource identifier.

**Syntax:**

```bash
redb mappings add --scope [database|table] --source [database[.table]] --target mcp://[resource_name]
```

**Examples:**

```bash
# Map a table to MCP resource
redb mappings add --scope table --source mydb.users --target mcp://users_resource \
  --name "Users Mapping" \
  --description "Maps users table to MCP resource"

# Map an entire database to MCP resource
redb mappings add --scope database --source mydb --target mcp://mydb_resource \
  --name "Database Mapping" \
  --description "Maps entire database to MCP"

# With policy enforcement
redb mappings add --scope table --source mydb.orders --target mcp://orders_resource \
  --name "Orders Mapping" \
  --description "Maps orders table with policy" \
  --policy-id policy_001
```

**Important Notes:**

- The `mcp://` prefix is required to distinguish MCP mappings from database-to-database mappings
- The resource name after `mcp://` will be used when creating MCP resources
- MCP mappings do not auto-generate mapping rules (unlike database-to-database mappings)

### List and Verify Mappings

```bash
# List all mappings
redb mappings list

# Show specific mapping
redb mappings show "Users Mapping"
```

## MCP Server Management

### List MCP Servers

```bash
# List all MCP servers in the active workspace
redb mcpservers list
```

**Output:**

```
NAME              PORT   ENABLED  NODES             DESCRIPTION
my-mcp-server     8080   true     node1,node2       Production MCP server
dev-mcp-server    8081   false    node1             Development MCP server
```

### Show MCP Server Details

```bash
# Show detailed information about a specific MCP server
redb mcpservers show my-mcp-server
```

**Output:**

```
MCP Server Details:
================================================================================
ID:                  01HYFZK8G3Y7PQWX9K2VNST0M4
Name:                my-mcp-server
Description:         Production MCP server
Port:                8080
Enabled:             true
Host IDs:            node1, node2
Policy IDs:          policy_001
Owner ID:            user_123
Status:              STATUS_HEALTHY
Status Message:      Server running
```

### Create MCP Server

```bash
# Create a new MCP server
redb mcpservers add \
  --name my-mcp-server \
  --description "Production MCP server" \
  --port 8080 \
  --nodes node1,node2 \
  --enabled

# Create with policies
redb mcpservers add \
  --name secure-mcp-server \
  --description "Secure MCP server with policies" \
  --port 8082 \
  --nodes node1 \
  --enabled \
  --policy-ids policy_001,policy_002
```

**Port Selection:**

- Default MCP server port: 8080
- Each server needs a unique port
- Ports must be available on all specified nodes
- Recommended range: 8080-8099 for MCP servers

**Node Selection:**

- Specify one or more mesh node IDs
- Server will run on all specified nodes
- High availability: Use multiple nodes
- Development: Single node is sufficient

### Modify MCP Server

```bash
# Change server port
redb mcpservers modify my-mcp-server --port 8081

# Enable/disable server
redb mcpservers modify my-mcp-server --enabled true
redb mcpservers modify my-mcp-server --enabled false

# Update description
redb mcpservers modify my-mcp-server --description "Updated description"

# Change nodes
redb mcpservers modify my-mcp-server --nodes node1,node2,node3

# Update policies
redb mcpservers modify my-mcp-server --policy-ids policy_003
```

### Delete MCP Server

```bash
# Delete a server (this also detaches all resources and tools)
redb mcpservers delete my-mcp-server
```

**Warning:** Deleting a server removes all associations with resources and tools.

## MCP Resource Management

### List MCP Resources

```bash
# List all MCP resources in the active workspace
redb mcpresources list
```

**Output:**

```
NAME              DESCRIPTION                    MAPPING_ID
users_resource    Users table resource           01HYFZK8G3Y7PQWX9K2VNST0M4
orders_resource   Orders table resource          01HYFZK8G3Y7PQWX9K2VNST0M5
products_resource Products table resource        01HYFZK8G3Y7PQWX9K2VNST0M6
```

### Show MCP Resource Details

```bash
# Show detailed information about a specific resource
redb mcpresources show users_resource
```

**Output:**

```
MCP Resource Details:
================================================================================
ID:                  01HYFZK8G3Y7PQWX9K2VNST0M4
Name:                users_resource
Description:         Users table resource
Mapping ID:          01HYFZK8G3Y7PQWX9K2VNST0M3
Policy IDs:          policy_001
Owner ID:            user_123
Configuration:
{
  "type": "direct_table",
  "database_id": "mydb",
  "table_name": "users"
}
```

### Create MCP Resource

MCP resources define which database tables are exposed through the MCP server.

**Syntax:**

```bash
redb mcpresources add \
  --name [resource_name] \
  --description "[description]" \
  --mapping [mapping_name] \
  --config '[json_config]'
```

**Configuration Types:**

1. **Direct Table:** Expose a table directly
2. **Mapped Table:** Expose a table through a mapping (with transformations)

**Examples:**

```bash
# Direct table resource
redb mcpresources add \
  --name users_resource \
  --description "Users table resource" \
  --mapping "Users Mapping" \
  --config '{"type":"direct_table","database_id":"mydb","table_name":"users"}'

# Mapped table resource (with transformation)
redb mcpresources add \
  --name customers_resource \
  --description "Customers view through mapping" \
  --mapping "Customers Mapping" \
  --config '{"type":"mapped_table","database_id":"sourcedb","table_name":"customers","target_database_id":"targetdb"}'

# Resource with policies
redb mcpresources add \
  --name sensitive_resource \
  --description "Sensitive data with policy" \
  --mapping "Sensitive Mapping" \
  --config '{"type":"direct_table","database_id":"mydb","table_name":"sensitive"}' \
  --policy-ids policy_001,policy_002
```

### Attach Resource to Server

After creating a resource, attach it to one or more MCP servers:

```bash
# Attach resource to server
redb mcpresources attach --resource users_resource --server my-mcp-server

# Attach to multiple servers
redb mcpresources attach --resource users_resource --server dev-mcp-server
redb mcpresources attach --resource users_resource --server prod-mcp-server
```

### Detach Resource from Server

```bash
# Detach resource from server
redb mcpresources detach --resource users_resource --server my-mcp-server
```

### Delete MCP Resource

```bash
# Delete a resource (automatically detached from all servers)
redb mcpresources delete users_resource
```

## MCP Tool Management

### List MCP Tools

```bash
# List all MCP tools in the active workspace
redb mcptools list
```

**Output:**

```
NAME              DESCRIPTION                    MAPPING_ID
query_tool        Query database tool            01HYFZK8G3Y7PQWX9K2VNST0M4
insert_tool       Insert data tool               01HYFZK8G3Y7PQWX9K2VNST0M5
schema_tool       Get schema tool                01HYFZK8G3Y7PQWX9K2VNST0M6
```

### Show MCP Tool Details

```bash
# Show detailed information about a specific tool
redb mcptools show query_tool
```

**Output:**

```
MCP Tool Details:
================================================================================
ID:                  01HYFZK8G3Y7PQWX9K2VNST0M4
Name:                query_tool
Description:         Query database tool
Mapping ID:          01HYFZK8G3Y7PQWX9K2VNST0M3
Policy IDs:          policy_001
Owner ID:            user_123
Configuration:
{
  "operation": "query_database",
  "input_schema": {
    "type": "object",
    "properties": {
      "database_id": {"type": "string"},
      "table_name": {"type": "string"},
      "query": {"type": "string"}
    },
    "required": ["database_id", "table_name", "query"]
  }
}
```

### Create MCP Tool

MCP tools expose database operations as callable functions for AI assistants.

**Tool Types:**

1. `query_database`: Execute SELECT queries
2. `insert_data`: Insert records into tables
3. `update_data`: Update existing records
4. `delete_data`: Delete records
5. `get_schema`: Retrieve table schema
6. `list_databases`: List available databases
7. `list_tables`: List tables in a database

**Examples:**

```bash
# Query tool
redb mcptools add \
  --name query_tool \
  --description "Query database tables" \
  --mapping "Database Mapping" \
  --config '{"operation":"query_database","input_schema":{"type":"object","properties":{"database_id":{"type":"string"},"table_name":{"type":"string"},"query":{"type":"string"}},"required":["database_id","table_name","query"]}}'

# Insert tool
redb mcptools add \
  --name insert_tool \
  --description "Insert data into tables" \
  --mapping "Database Mapping" \
  --config '{"operation":"insert_data","input_schema":{"type":"object","properties":{"database_id":{"type":"string"},"table_name":{"type":"string"},"data":{"type":"object"}},"required":["database_id","table_name","data"]}}'

# Schema tool
redb mcptools add \
  --name schema_tool \
  --description "Get table schema" \
  --mapping "Database Mapping" \
  --config '{"operation":"get_schema","input_schema":{"type":"object","properties":{"database_id":{"type":"string"},"table_name":{"type":"string"}},"required":["database_id","table_name"]}}'

# Tool with policies
redb mcptools add \
  --name secure_query_tool \
  --description "Secure query tool" \
  --mapping "Secure Mapping" \
  --config '{"operation":"query_database","input_schema":{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}}' \
  --policy-ids policy_001
```

### Attach Tool to Server

```bash
# Attach tool to server
redb mcptools attach --tool query_tool --server my-mcp-server

# Attach multiple tools to a server
redb mcptools attach --tool query_tool --server my-mcp-server
redb mcptools attach --tool insert_tool --server my-mcp-server
redb mcptools attach --tool schema_tool --server my-mcp-server
```

### Detach Tool from Server

```bash
# Detach tool from server
redb mcptools detach --tool query_tool --server my-mcp-server
```

### Delete MCP Tool

```bash
# Delete a tool (automatically detached from all servers)
redb mcptools delete query_tool
```

## Complete Workflow Example

Here's a complete example of setting up an MCP server for a production application:

### Step 1: Connect Database

```bash
# Connect your database
redb databases connect --name mydb --host localhost --port 5432 --user postgres
```

### Step 2: Create Mappings

```bash
# Create mappings for tables
redb mappings add --scope table --source mydb.users --target mcp://users_resource \
  --name "Users Mapping" --description "Maps users table"

redb mappings add --scope table --source mydb.orders --target mcp://orders_resource \
  --name "Orders Mapping" --description "Maps orders table"

redb mappings add --scope table --source mydb.products --target mcp://products_resource \
  --name "Products Mapping" --description "Maps products table"

# Create database-level mapping for tools
redb mappings add --scope database --source mydb --target mcp://db_access \
  --name "Database Mapping" --description "Database access for tools"
```

### Step 3: Create MCP Server

```bash
# Create MCP server
redb mcpservers add \
  --name production-mcp \
  --description "Production MCP server" \
  --port 8080 \
  --nodes node1,node2 \
  --enabled
```

### Step 4: Create MCP Resources

```bash
# Create resources for each table
redb mcpresources add \
  --name users_resource \
  --description "Users table" \
  --mapping "Users Mapping" \
  --config '{"type":"direct_table","database_id":"mydb","table_name":"users"}'

redb mcpresources add \
  --name orders_resource \
  --description "Orders table" \
  --mapping "Orders Mapping" \
  --config '{"type":"direct_table","database_id":"mydb","table_name":"orders"}'

redb mcpresources add \
  --name products_resource \
  --description "Products table" \
  --mapping "Products Mapping" \
  --config '{"type":"direct_table","database_id":"mydb","table_name":"products"}'
```

### Step 5: Attach Resources to Server

```bash
# Attach all resources to the server
redb mcpresources attach --resource users_resource --server production-mcp
redb mcpresources attach --resource orders_resource --server production-mcp
redb mcpresources attach --resource products_resource --server production-mcp
```

### Step 6: Create MCP Tools

```bash
# Create query tool
redb mcptools add \
  --name query_database \
  --description "Query any table" \
  --mapping "Database Mapping" \
  --config '{"operation":"query_database","input_schema":{"type":"object","properties":{"database_id":{"type":"string"},"table_name":{"type":"string"},"query":{"type":"string"}},"required":["database_id","table_name","query"]}}'

# Create schema tool
redb mcptools add \
  --name get_schema \
  --description "Get table schema" \
  --mapping "Database Mapping" \
  --config '{"operation":"get_schema","input_schema":{"type":"object","properties":{"database_id":{"type":"string"},"table_name":{"type":"string"}},"required":["database_id","table_name"]}}'

# Create insert tool
redb mcptools add \
  --name insert_data \
  --description "Insert data" \
  --mapping "Database Mapping" \
  --config '{"operation":"insert_data","input_schema":{"type":"object","properties":{"database_id":{"type":"string"},"table_name":{"type":"string"},"data":{"type":"object"}},"required":["database_id","table_name","data"]}}'
```

### Step 7: Attach Tools to Server

```bash
# Attach all tools to the server
redb mcptools attach --tool query_database --server production-mcp
redb mcptools attach --tool get_schema --server production-mcp
redb mcptools attach --tool insert_data --server production-mcp
```

### Step 8: Verify Setup

```bash
# Verify server is running
redb mcpservers show production-mcp

# List attached resources
redb mcpresources list

# List attached tools
redb mcptools list
```

The MCP server is now running on port 8080 across nodes `node1` and `node2`, serving three resources and three tools to AI assistants!

## Best Practices

### Security

1. **Always use policies**: Attach policy IDs to sensitive resources and tools
2. **Least privilege**: Only expose necessary tables and operations
3. **Node placement**: Place servers on secure, trusted nodes
4. **Audit logging**: Monitor MCP server activity through mesh logs

### Performance

1. **Server distribution**: Distribute servers across multiple nodes for high availability
2. **Port allocation**: Use dedicated port ranges for MCP servers
3. **Resource limits**: Monitor resource usage and adjust server placement
4. **Mapping efficiency**: Use appropriate mapping scopes (table vs database)

### Organization

1. **Naming conventions**: Use consistent naming for servers, resources, and tools
   - Servers: `environment-purpose-mcp` (e.g., `prod-api-mcp`)
   - Resources: `table_resource` (e.g., `users_resource`)
   - Tools: `operation_tool` (e.g., `query_tool`)

2. **Documentation**: Document the purpose and configuration of each component
3. **Workspace organization**: Group related resources and tools by workspace
4. **Version control**: Track configuration changes through git

### Maintenance

1. **Regular updates**: Review and update policies periodically
2. **Server health**: Monitor server status and performance
3. **Cleanup**: Delete unused resources, tools, and servers
4. **Testing**: Test MCP servers in development before production deployment

## Troubleshooting

### Server Not Starting

**Problem:** MCP server shows `STATUS_ERROR`

**Solutions:**
1. Check if port is available: `redb mcpservers show [server-name]`
2. Verify nodes are online: `redb nodes list`
3. Check mesh connectivity: `redb mesh status`
4. Review logs: Check `redb-node-event.log` for errors

### Resource Not Accessible

**Problem:** AI assistant cannot access MCP resource

**Solutions:**
1. Verify resource is attached: `redb mcpresources show [resource-name]`
2. Check mapping exists: `redb mappings list`
3. Verify database connection: `redb databases show [database-name]`
4. Check policies: Ensure policy allows access

### Tool Execution Fails

**Problem:** MCP tool returns errors when executed

**Solutions:**
1. Verify tool configuration: `redb mcptools show [tool-name]`
2. Check mapping: Ensure mapping exists and is valid
3. Test database connectivity: Try direct database query
4. Review input schema: Ensure tool input matches schema

### Port Already in Use

**Problem:** Cannot create server due to port conflict

**Solutions:**
1. List existing servers: `redb mcpservers list`
2. Choose different port: Use `--port` flag with unused port
3. Stop conflicting server: `redb mcpservers modify [server] --enabled false`

### Performance Issues

**Problem:** MCP server responses are slow

**Solutions:**
1. Add more nodes: `redb mcpservers modify [server] --nodes node1,node2,node3`
2. Optimize queries: Review and optimize database queries
3. Check network latency: Ensure mesh network is healthy
4. Monitor resources: Check CPU/memory usage on nodes

## API Reference

For developers integrating with the MCP management API, refer to:

- REST API: `/api/v1/workspaces/{workspace_name}/mcpservers/*`
- REST API: `/api/v1/workspaces/{workspace_name}/mcpresources/*`
- REST API: `/api/v1/workspaces/{workspace_name}/mcptools/*`
- gRPC Service: `MCPService` in `api/proto/core/v1/core.proto`

## See Also

- [MCP Server Internal Documentation](../services/mcpserver/README.md)
- [Database Adapter Implementation](DATABASE_ADAPTER_IMPLEMENTATION_GUIDE.md)
- [Mappings Documentation](ARCHITECTURE.md#mappings)
- [Policy Management](CLI_REFERENCE.md#policies)

## Support

For issues or questions:

1. Check the troubleshooting section above
2. Review logs in `bin/logs/redb-node-event.log`
3. Consult the MCP server documentation
4. Open an issue on GitHub

---