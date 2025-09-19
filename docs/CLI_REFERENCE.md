## CLI Reference

Commands are grouped by functional areas. Use `redb-cli help` or `redb-cli <group> --help` for details.

### Core Resource Management
- Authentication: `auth login`, `auth logout`, `auth profile`, `auth status`, `auth password`
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


