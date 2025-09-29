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

### AI Integration
- MCP Servers: manage MCP servers
- MCP Resources: configure AI-accessible resources
- MCP Tools: define AI tools and functions

## Getting Started Examples

### Initial Setup
```bash
# 1. Create your first profile
./bin/redb-cli profiles create production --hostname api.redb.example.com --tenant-url https://api.redb.example.com/my-org

# 2. Login to the profile
./bin/redb-cli auth login --profile production

# 3. Select your workspace
./bin/redb-cli select workspace main

# 4. Check your authentication status
./bin/redb-cli auth status
```

### Working with Multiple Environments
```bash
# Create profiles for different environments
./bin/redb-cli profiles create dev --hostname localhost:8080 --tenant-url http://localhost:8080/dev-tenant
./bin/redb-cli profiles create staging --hostname staging.redb.example.com --tenant-url https://staging.redb.example.com/my-org

# Switch between environments
./bin/redb-cli profiles activate dev
./bin/redb-cli auth login --profile dev

./bin/redb-cli profiles activate staging
./bin/redb-cli auth login --profile staging

# List all your profiles
./bin/redb-cli profiles list
```

### Database Operations
```bash
# Connect a database (requires active profile and workspace)
./bin/redb-cli databases connect --name mydb --type postgres --host db.example.com --port 5432

# List databases in current workspace
./bin/redb-cli databases list

# Show database details
./bin/redb-cli databases show mydb
```


