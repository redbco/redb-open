## Description
<!-- Provide a clear and concise description of the changes made. Include the motivation for the change and how it addresses the issue. -->

## Type of Change
<!-- Mark the appropriate option(s) with an 'x' -->

- [ ] **Bug fix** (non-breaking change which fixes an issue)
- [ ] **New feature** (non-breaking change which adds functionality)
- [ ] **Breaking change** (fix or feature that would cause existing functionality to not work as expected)
- [ ] **Documentation update** (updates to README, API docs, etc.)
- [ ] **Performance improvement** (code change that improves performance)
- [ ] **Refactoring** (code change that neither fixes a bug nor adds a feature)
- [ ] **Test addition** (adding missing tests or correcting existing tests)
- [ ] **Build/CI change** (changes to build system, CI configuration, etc.)

## Service/Component Affected
<!-- Mark the service(s) or component(s) that are affected by this change -->

- [ ] **Supervisor Service** (`cmd/supervisor/`)
- [ ] **Security Service** (`services/security/`)
- [ ] **License Service** (`services/license/`)
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
- [ ] **Documentation** (README, CONTRIBUTING, etc.)
- [ ] **Build System** (Makefile, scripts, etc.)

## Database Adapter Changes
<!-- If this PR affects database adapters, specify which ones -->

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

## Testing
<!-- Describe the tests you ran and their results -->

### Test Coverage
- [ ] **Unit tests** added/updated and passing
- [ ] **Integration tests** added/updated and passing
- [ ] **Manual testing** completed
- [ ] **Performance testing** completed (if applicable)
- [ ] **Security testing** completed (if applicable)

### Test Commands Executed
```bash
# List the test commands you ran
make test
go test -cover ./...
# Add any other relevant test commands
```

## Checklist
<!-- Mark items with an 'x' to indicate completion -->

### Code Quality
- [ ] **Code follows** the project's style guidelines
- [ ] **Self-review** of code completed
- [ ] **Code is commented**, particularly in hard-to-understand areas
- [ ] **No new warnings** generated
- [ ] **Linting passes** (`make lint`)
- [ ] **Static analysis passes** (`go vet ./...`)

### Documentation
- [ ] **Code documentation** updated (comments, godoc)
- [ ] **API documentation** updated (if applicable)
- [ ] **README files** updated (if applicable)
- [ ] **Protocol Buffer** definitions documented (if applicable)
- [ ] **CLI help** updated (if applicable)

### Security & Compatibility
- [ ] **Security implications** considered and addressed
- [ ] **Backward compatibility** maintained (if applicable)
- [ ] **Database schema changes** are backward compatible
- [ ] **API changes** are documented and versioned appropriately

### Performance & Reliability
- [ ] **Performance impact** considered and tested
- [ ] **Error handling** is comprehensive
- [ ] **Logging** is appropriate and structured
- [ ] **Resource usage** is optimized

## Breaking Changes
<!-- If this PR includes breaking changes, describe them here -->

**Breaking Changes:**
- [ ] This PR includes breaking changes
- [ ] Breaking changes are documented in the description above
- [ ] Migration guide is provided (if applicable)

## Screenshots/Examples
<!-- If applicable, add screenshots or examples to help explain the changes -->

## Related Issues
<!-- Link related issues using keywords like "Fixes", "Closes", "Resolves" -->

- **Fixes:** #(issue number)
- **Closes:** #(issue number)
- **Related to:** #(issue number)

## Additional Notes
<!-- Add any other context about the pull request here -->

## Commit Message
<!-- Provide the commit message that will be used when this PR is merged -->

```
<type>(<scope>): <description>

<optional body>

<optional footer>
```

Example:
```
feat(anchor): add MySQL database adapter support

- Implements MySQL connection adapter
- Adds MySQL-specific type conversions
- Includes comprehensive test coverage
- Updates documentation with MySQL examples

Closes #123
``` 