# MCP Server Service

The MCP Server service implements the [Model Context Protocol](https://modelcontextprotocol.io/) specification, providing AI assistants with structured access to database resources, tools, and prompts through a secure, policy-based interface.

## Features

- **Official MCP Protocol**: Full implementation of MCP specification (JSON-RPC 2.0 over HTTP)
- **Multi-Tenant Architecture**: Ready for multi-tenant deployments with tenant/workspace isolation
- **Policy-Based Access Control**: Integration with Security service for authorization
- **Resource Serving**: Exposes database tables (direct and mapped) as MCP resources
- **Tool Execution**: Database operations (query, insert, schema management) as callable tools
- **Prompt Templates**: Templated prompts with argument substitution
- **Virtual Servers**: Run multiple MCP servers on different ports across mesh nodes
- **Mesh Integration**: Distributed deployment across mesh network

## Architecture

### Components

1. **Protocol Handler** (`internal/protocol/`)
   - JSON-RPC 2.0 message handling
   - MCP capability negotiation
   - Standard MCP method routing

2. **Authentication Middleware** (`internal/auth/`)
   - JWT and API token validation
   - Security service integration
   - Session context management

3. **Resource Handlers** (`internal/resources/`)
   - Direct database table resources
   - Mapped/virtual table resources with transformations
   - Resource discovery and metadata

4. **Tool Handlers** (`internal/tools/`)
   - Database query tools
   - Data modification tools (insert, update, delete)
   - Schema management tools
   - Execution via Anchor service gRPC

5. **Prompt Handlers** (`internal/prompts/`)
   - Template loading and rendering
   - Argument substitution
   - Context-aware prompt generation

6. **Engine** (`internal/engine/`)
   - Virtual server management
   - Server reconciliation loop
   - Per-server HTTP routing
   - Metrics tracking

## Configuration

### Internal Database Schema

The service uses the following tables from the internal PostgreSQL database:

- `mcpservers` - Virtual MCP server definitions
- `mcpresources` - Resource definitions with mappings
- `mcptools` - Tool definitions with configurations
- `mcpprompts` - Prompt templates
- `mcp_server_resources` - Server-resource associations
- `mcp_server_tools` - Server-tool associations
- `mcp_server_prompts` - Server-prompt associations

### Virtual Server Configuration

Each virtual MCP server is defined in the `mcpservers` table:

```sql
INSERT INTO mcpservers (
    tenant_id,
    workspace_id,
    mcpserver_name,
    mcpserver_description,
    mcpserver_host_ids,  -- Array of node IDs where this server should run
    mcpserver_port,       -- Port number for this virtual server
    mcpserver_enabled,
    owner_id
) VALUES (
    'tenant_...',
    'ws_...',
    'my-mcp-server',
    'MCP server for database access',
    ARRAY['node_...'],   -- Node IDs
    9000,
    true,
    'user_...'
);
```

### Resource Configuration

Resources can be:
1. **Direct Tables**: Direct access to database tables
2. **Mapped Tables**: Virtual tables with transformations

Example resource configuration in `mcpresources`:

```json
{
  "type": "direct_table",
  "database_id": "my_database",
  "table_name": "users"
}
```

Note: The `database_id` field can contain either a database name or a database ID. Database names are automatically resolved to IDs at runtime.

### Tool Configuration

Tools define operations that can be executed. Example in `mcptools`:

```json
{
  "operation": "query_database",
  "input_schema": {
    "type": "object",
    "properties": {
      "database_id": {"type": "string"},
      "table_name": {"type": "string"},
      "options": {"type": "object"}
    },
    "required": ["database_id", "table_name"]
  }
}
```

### Prompt Configuration

Prompts are templates with arguments. Example in `mcpprompts`:

```json
{
  "arguments": [
    {
      "name": "table_name",
      "description": "Name of the table to query",
      "required": true
    }
  ],
  "messages": [
    {
      "role": "user",
      "text": "Query the {table_name} table for all records"
    }
  ]
}
```

## Authentication

The MCP server requires authentication for all requests:

### JWT Token

```http
POST http://localhost:9000/
Authorization: Bearer <jwt_token>
Content-Type: application/json

{
  "jsonrpc": "2.0",
  "method": "initialize",
  "params": {
    "protocolVersion": "2024-11-05",
    "capabilities": {},
    "clientInfo": {
      "name": "my-client",
      "version": "1.0.0"
    }
  },
  "id": 1
}
```

### API Token

```http
POST http://localhost:9000/
Authorization: APIToken <api_token>
```

## MCP Protocol Methods

### Initialization

```json
{
  "jsonrpc": "2.0",
  "method": "initialize",
  "params": {
    "protocolVersion": "2024-11-05",
    "capabilities": {},
    "clientInfo": {
      "name": "client-name",
      "version": "1.0.0"
    }
  },
  "id": 1
}
```

### List Resources

```json
{
  "jsonrpc": "2.0",
  "method": "resources/list",
  "params": {},
  "id": 2
}
```

### Read Resource

```json
{
  "jsonrpc": "2.0",
  "method": "resources/read",
  "params": {
    "uri": "redb://database/my_database/table/users"
  },
  "id": 3
}
```

Note: URIs support both database names and IDs:
- `redb://database/my_database/table/users` (using database name)
- `redb://database/db_.../table/users` (using database ID)

### List Tools

```json
{
  "jsonrpc": "2.0",
  "method": "tools/list",
  "params": {},
  "id": 4
}
```

### Call Tool

```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "params": {
    "name": "query_database",
    "arguments": {
      "database_id": "my_database",
      "table_name": "users",
      "options": {"limit": 10}
    }
  },
  "id": 5
}
```

Note: Tool arguments support both database names and IDs:
- Use `"database_id": "my_database"` (database name)
- Use `"database_id": "db_..."` (database ID)
- Or `"database_name": "my_database"` (explicit field)

### List Prompts

```json
{
  "jsonrpc": "2.0",
  "method": "prompts/list",
  "params": {},
  "id": 6
}
```

### Get Prompt

```json
{
  "jsonrpc": "2.0",
  "method": "prompts/get",
  "params": {
    "name": "query-table",
    "arguments": {
      "table_name": "users"
    }
  },
  "id": 7
}
```

## Security Integration

### Authorization Flow

1. Client sends request with JWT or API token
2. Authentication middleware validates token via Security service
3. Session context is created with tenant_id, workspace_id, user_id
4. For each operation, authorization check is performed:
   - `resource_list` - Can list resources
   - `resource_read` - Can read specific resource
   - `tool_list` - Can list tools
   - `tool_call` - Can execute specific tool
   - `prompt_list` - Can list prompts
   - `prompt_get` - Can get specific prompt

### Policy Enforcement

Policies are attached to:
- MCP servers (`mcpservers.policy_ids`)
- Resources (`mcpresources.policy_ids`)
- Tools (`mcptools.policy_ids`)
- Prompts (`mcpprompts.policy_ids`)

The Security service evaluates these policies to determine authorization.

## Deployment

### Single Node

The service automatically starts virtual MCP servers configured for the current node:

```bash
./redb-node
```

### Multi-Node Mesh

Each node runs its own copy of the MCP server service. Virtual servers are distributed based on `mcpserver_host_ids`:

```sql
-- Server on node1
mcpserver_host_ids = ARRAY['node_001']

-- Server on both node1 and node2
mcpserver_host_ids = ARRAY['node_001', 'node_002']
```

## Monitoring

### Metrics

The engine tracks:
- `request_count` - Total requests processed
- `active_sessions` - Current active sessions
- `error_count` - Total errors encountered

Access metrics via the supervisor health endpoint.

### Logging

All MCP operations are logged with:
- Client information
- Operation type
- Authorization results
- Execution results

### Audit Trail

All MCP operations are audited in the `audit_log` table with:
- User ID
- Action performed
- Resource accessed
- Timestamp
- Success/failure status

## Development

### Adding New Tool Types

1. Define tool configuration in `mcptools` table
2. Add operation handler in `internal/tools/handler.go`
3. Implement execution logic via Anchor gRPC calls

### Adding New Resource Types

1. Define resource configuration in `mcpresources` table
2. Add resource type handler in `internal/resources/handler.go`
3. Implement data fetching logic

### Testing

```bash
# Build
make build-mcpserver

# Run tests (to be implemented)
go test ./services/mcpserver/...
```

## Future Enhancements

- Resource subscriptions via CDC
- Streaming responses for large datasets
- Custom tool plugins
- Prompt chaining
- Rate limiting per virtual server
- Advanced caching strategies
- WebSocket support for real-time updates

