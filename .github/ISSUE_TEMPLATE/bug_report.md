---
name: Bug Report
about: Create a report to help us improve reDB Node
title: '[BUG] '
labels: ['bug', 'needs-triage']
assignees: ''
---

## Bug Description
<!-- A clear and concise description of what the bug is -->

## Expected Behavior
<!-- A clear and concise description of what you expected to happen -->

## Actual Behavior
<!-- A clear and concise description of what actually happened -->

## Steps to Reproduce
<!-- Detailed steps to reproduce the bug -->

1. **Environment Setup:**
   ```bash
   # List the commands you used to set up the environment
   ```

2. **Configuration:**
   ```bash
   # Show relevant configuration commands
   ```

3. **Reproduction Steps:**
   ```bash
   # List the exact commands that caused the bug
   ```

4. **Error Output:**
   ```bash
   # Paste the complete error output here
   ```

## Environment Information

### System Information
- **OS:** [e.g., Ubuntu 22.04, macOS 14.0, Windows 11]
- **Architecture:** [e.g., x86_64, arm64]
- **Go Version:** [e.g., go version go1.21.0 linux/amd64]
- **PostgreSQL Version:** [e.g., PostgreSQL 17.0]
- **Redis Version:** [e.g., Redis 7.0]

### reDB Node Information
- **Version:** [e.g., v1.0.0, commit hash, or "latest from main"]
- **Installation Method:** [e.g., from source, binary download, Docker]
- **Build Command:** [e.g., `make local`, `make build`]

### Database Information
<!-- If the bug is related to a specific database -->

- **Database Type:** [e.g., PostgreSQL, MySQL, MongoDB]
- **Database Version:** [e.g., PostgreSQL 17.0, MySQL 8.0]
- **Connection Details:** [e.g., localhost:5432, remote server]

## Affected Service/Component
<!-- Mark the service(s) or component(s) that are affected -->

- [ ] **Supervisor Service** (`cmd/supervisor/`)
- [ ] **Security Service** (`services/security/`)
- [ ] **Core Service** (`services/core/`)
- [ ] **Unified Model Service** (`services/unifiedmodel/`)
- [ ] **Anchor Service** (`services/anchor/`)
- [ ] **Transformation Service** (`services/transformation/`)
- [ ] **Mesh Service** (`services/mesh/`)
- [ ] **Client API** (`services/clientapi/`)
- [ ] **Service API** (`services/serviceapi/`)
- [ ] **Query API** (`services/queryapi/`)
- [ ] **Webhook Service** (`services/webhook/`)
- [ ] **MCP Server Service** (`services/mcpserver/`)
- [ ] **CLI** (`cmd/cli/`)
- [ ] **Shared Libraries** (`pkg/`)
- [ ] **Protocol Buffers** (`api/proto/`)
- [ ] **Build System** (Makefile, scripts, etc.)

## Database Adapter (if applicable)
<!-- If the bug is related to a specific database adapter -->

- [ ] **PostgreSQL**
- [ ] **MySQL**
- [ ] **MariaDB**
- [ ] **SQL Server**
- [ ] **Oracle**
- [ ] **IBM Db2**
- [ ] **MongoDB**
- [ ] **CosmosDB**
- [ ] **Redis**
- [ ] **DynamoDB**
- [ ] **Neo4j**
- [ ] **Pinecone**
- [ ] **ClickHouse**
- [ ] **Snowflake**
- [ ] **Elasticsearch**
- [ ] **Cassandra**
- [ ] **EdgeDB**
- [ ] **CockroachDB**
- [ ] **Chroma**
- [ ] **Milvus**
- [ ] **Weaviate**

## Logs and Debug Information

### Application Logs
<!-- Paste relevant log output here -->

```bash
# Application startup logs
```

```bash
# Error logs when the bug occurs
```

### System Logs
<!-- If applicable, include system logs -->

```bash
# PostgreSQL logs (if relevant)
```

```bash
# Redis logs (if relevant)
```

### Debug Information
<!-- Run these commands and paste the output -->

```bash
# reDB Node version and build info
./bin/redb-node --version
```

```bash
# reDB Node version and build info
./bin/redb-cli --version
```

```bash
# Service status
./bin/redb-cli auth status
```

```bash
# Database connectivity test
./bin/redb-cli instances list
```

## Additional Context
<!-- Add any other context about the problem here -->

### Workarounds
<!-- If you found a workaround, describe it here -->

### Related Issues
<!-- Link to any related issues or discussions -->

### Screenshots
<!-- If applicable, add screenshots to help explain the problem -->

## Impact Assessment
<!-- Help us understand the severity of this bug -->

- [ ] **Critical** - System completely unusable
- [ ] **High** - Major functionality broken
- [ ] **Medium** - Some functionality affected
- [ ] **Low** - Minor issue, workaround available
- [ ] **Cosmetic** - Visual or documentation issue

## Reproduction Rate
<!-- How often does this bug occur? -->

- [ ] **Always** - 100% reproducible
- [ ] **Often** - >50% of the time
- [ ] **Sometimes** - 10-50% of the time
- [ ] **Rarely** - <10% of the time
- [ ] **Unknown** - Not sure

## Checklist
<!-- Before submitting, please ensure you've completed these steps -->

- [ ] I have searched existing issues to avoid duplicates
- [ ] I have provided all required information
- [ ] I have included relevant logs and debug information
- [ ] I have tested with the latest version from main branch
- [ ] I have tried to reproduce the issue in a clean environment 