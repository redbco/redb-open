## CLI Reference

Commands are grouped by functional areas. Use `redb-cli help` or `redb-cli <group> --help` for details.

### Profile Management
reDB CLI uses profiles to manage connections to multiple reDB instances:
- `profiles create <name>` - Create a new connection profile
- `profiles list` - List all available profiles
- `profiles show [name]` - Show profile details (current if no name specified)
- `profiles activate <name>` - Switch to a different profile
- `profiles delete <name>` - Delete a profile
- `clean` - Remove all profiles and logout from all sessions

### Authentication & Session Management
- `auth login --profile <name>` - Login using a specific profile
- `auth logout` - Logout from current profile
- `auth status` - Show authentication status and profile information
- `auth password` - Change password for current user
- `auth sessions list` - List active sessions
- `auth sessions logout <session-id>` - Logout specific session
- `auth sessions logout-all` - Logout all sessions
- `select workspace <name>` - Select active workspace

### Core Resource Management
- Tenants & Users: `tenants list|show|add|modify|delete`
- Workspaces: `workspaces list|show|add|modify|delete`
- Regions: `regions list|show|add|modify|delete`

### Database Operations
- Instances: `instances connect|list|show|modify`
- Databases: `databases connect|list|wipe`, `databases clone table-data`
- Schema Management: inspect and modify database schemas

### Version Control & Schema
- Repos: `repos list|show|add|modify`
- Branches: `branches show|attach|detach`
- Commits: `commits show`

### Data Integration
- Mappings: `mappings list`, `mappings add table-mapping`
- Relationships: define replication/migration relationships
- Transformations: schema-aware transforms and obfuscation

### Mesh & Network
- Mesh: `mesh seed|join|show topology`
- Satellites & Anchors: specialized node management
- Routes: network topology and routing

### AI Integration (Model Context Protocol)
- MCP Servers: `mcpservers list|show|add|modify|delete` - Manage virtual MCP server instances
- MCP Resources: `mcpresources list|show|add|attach|detach|delete` - Expose database tables to AI assistants
- MCP Tools: `mcptools list|show|add|attach|detach|delete` - Define AI-callable database operations
- Mappings: `mappings add --target mcp://resource_name` - Create mappings to MCP resources

See [MCP Server Management Guide](MCP_SERVER_MANAGEMENT.md) for complete documentation.

## Getting Started Examples

### Initial Setup
```bash
# 1. Create your first profile
./bin/redb-cli profiles create default

# 2. Login to the profile
./bin/redb-cli auth login --profile default

# 3. Check your authentication status
./bin/redb-cli auth status
```

### Working with Multiple Environments
```bash
# Create profiles for different environments
./bin/redb-cli profiles create dev
./bin/redb-cli profiles create production

# Switch between environments
./bin/redb-cli profiles activate dev
./bin/redb-cli auth login --profile dev

./bin/redb-cli profiles activate production
./bin/redb-cli auth login --profile production

# List all your profiles
./bin/redb-cli profiles list
```

### Database Operations
```bash
# Connect databases using connection strings
./bin/redb-cli databases connect --string "postgresql://user:password@localhost:5432/testdb1" --name "pg"
./bin/redb-cli databases connect --string "mysql://user:password@localhost:3307/testdb2" --name "my"
./bin/redb-cli databases connect --string "mongodb://redb:Test123@localhost:27017/redb_test" --name "mg"

# Show connected instances and databases
./bin/redb-cli instances list
./bin/redb-cli databases list

# Show the discovered tables and their metadata
./bin/redb-cli databases show pg --tables

# Show collections in a MongoDB database
./bin/redb-cli databases show mg --collections
```

### Version Control & Schema Deployment
```bash
# Show the repositories and commits
./bin/redb-cli repos list
./bin/redb-cli branches show pg/main

# Deploy the PostgreSQL database schema to a new database in MySQL
./bin/redb-cli commits deploy-schema pg/main/12345abc --instance my_instance --db-name deployed1

# Show the deployed database tables
./bin/redb-cli databases show deployed1 --tables
```

### Data Mapping & Replication
```bash
# Create a database-to-database mapping
./bin/redb-cli mappings add --scope database --source pg --target deployed1
./bin/redb-cli mappings show pg_to_deployed1

# Create a table-to-table mapping
./bin/redb-cli mappings add --scope table --source pg.test --target deployed1.test
./bin/redb-cli mappings show pg_test_to_deployed1_test

# Clone the data from the PostgreSQL database table to the deployed MySQL database table
./bin/redb-cli mappings copy-data pg_test_to_deployed1_test
```

### AI Integration with MCP Servers
```bash
# Step 1: Create mappings to MCP resources
./bin/redb-cli mappings add --scope table --source mydb.users --target mcp://users_resource \
  --name "Users MCP Mapping" --description "Map users table to MCP"

./bin/redb-cli mappings add --scope database --source mydb --target mcp://db_access \
  --name "Database MCP Mapping" --description "Database access for MCP tools"

# Step 2: Create MCP server
./bin/redb-cli mcpservers add \
  --name production-mcp \
  --description "Production MCP server for AI assistants" \
  --port 8080 \
  --nodes node1,node2 \
  --enabled

# Step 3: Create MCP resources
./bin/redb-cli mcpresources add \
  --name users_resource \
  --description "Users table resource" \
  --mapping "Users MCP Mapping" \
  --config '{"type":"direct_table","database_id":"mydb","table_name":"users"}'

# Step 4: Attach resource to server
./bin/redb-cli mcpresources attach --resource users_resource --server production-mcp

# Step 5: Create MCP tools
./bin/redb-cli mcptools add \
  --name query_database \
  --description "Query database tables" \
  --mapping "Database MCP Mapping" \
  --config '{"operation":"query_database","input_schema":{"type":"object","properties":{"database_id":{"type":"string"},"table_name":{"type":"string"},"query":{"type":"string"}},"required":["database_id","table_name","query"]}}'

# Step 6: Attach tool to server
./bin/redb-cli mcptools attach --tool query_database --server production-mcp

# View the complete setup
./bin/redb-cli mcpservers show production-mcp
./bin/redb-cli mcpresources list
./bin/redb-cli mcptools list
```

For complete MCP server management documentation, see [MCP Server Management Guide](MCP_SERVER_MANAGEMENT.md).


