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


