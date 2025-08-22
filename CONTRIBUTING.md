# Contributing to reDB Node

Thank you for your interest in contributing to reDB Node! This document provides guidelines and information for contributors to help make the contribution process smooth and effective.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Environment](#development-environment)
- [Project Structure](#project-structure)
- [Development Workflow](#development-workflow)
- [Branch Protection and Governance](#branch-protection-and-governance)
- [Code Standards](#code-standards)
- [Testing](#testing)
- [Documentation](#documentation)
- [Submitting Changes](#submitting-changes)
- [Review Process](#review-process)
- [Release Process](#release-process)
- [Community Guidelines](#community-guidelines)

## Code of Conduct

This project is committed to providing a welcoming and inclusive environment for all contributors. By participating in this project, you agree to abide by our Code of Conduct. Please report unacceptable behavior to the project maintainers.

### Our Standards

- Be respectful and inclusive of all contributors
- Use welcoming and inclusive language
- Be collaborative and open to constructive feedback
- Focus on what is best for the community
- Show empathy towards other community members

## Getting Started

### Prerequisites

Before you begin contributing, ensure you have the following installed:

- **Go 1.23+** - [Download](https://golang.org/dl/)
- **PostgreSQL 17+** - [Download](https://www.postgresql.org/download/)
- **Redis Server** - [Download](https://redis.io/download)
- **Protocol Buffers Compiler** - [Installation Guide](https://grpc.io/docs/protoc-installation/)
- **Git** - [Download](https://git-scm.com/downloads)
- **Make** - Usually pre-installed on Unix-like systems

### Fork and Clone

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/redb-open.git
   cd redb-open
   ```
3. **Add the upstream remote**:
   ```bash
   git remote add upstream https://github.com/redbco/redb-open.git
   ```

## Development Environment

### Initial Setup

1. **Install development tools**:
   ```bash
   make dev-tools
   ```

2. **Generate Protocol Buffer code**:
   ```bash
   make proto
   ```

3. **Build the project**:
   ```bash
   make local
   ```

4. **Run tests** to ensure everything is working:
   ```bash
   make test
   ```

### Database Setup and Initialization

1. **Start PostgreSQL**:
   ```bash
   # Ubuntu/Debian
   sudo systemctl start postgresql
   
   # macOS (with Homebrew)
   brew services start postgresql
   ```

2. **Start Redis**:
   ```bash
   # Ubuntu/Debian
   sudo systemctl start redis-server
   
   # macOS (with Homebrew)
   brew services start redis
   ```

3. **Create a database user**:
   ```bash
   sudo -u postgres psql
   CREATE USER your_admin_user WITH ENCRYPTED PASSWORD 'your_admin_password' CREATEDB CREATEROLE LOGIN;
   \q
   ```

4. **Initialize the application**:
    ```bash
    ./redb-node --initialize
    ```

## Project Structure

Understanding the project structure will help you navigate and contribute effectively:

```
redb-open/
├── cmd/                  # Command-line applications
│   ├── cli/              # CLI client
│   └── supervisor/       # Service orchestrator
├── services/             # Core microservices
│   ├── anchor/           # Database connectivity (16+ adapters)
│   ├── clientapi/        # Primary REST API (50+ endpoints)
│   ├── core/             # Central business logic hub
│   ├── mcpserver/        # AI/LLM integration (MCP protocol)
│   ├── mesh/             # Distributed coordination and consensus
│   ├── security/         # Authentication and authorization
│   ├── transformation/   # Data processing and obfuscation
│   ├── unifiedmodel/     # Database abstraction and schema translation
│   └── webhook/          # External system integration
├── pkg/                  # Shared libraries and utilities
└── api/proto/            # Protocol Buffer definitions
```

### Key Directories for Contributors

- **`services/`** - Main microservices (most contributions will be here)
- **`pkg/`** - Shared libraries (common utilities and frameworks)
- **`api/proto/`** - Protocol Buffer definitions (for API changes)
- **`cmd/`** - Command-line applications

## Development Workflow

### Branch Strategy

We use a feature branch workflow:

1. **Main branch** (`main`) - Always contains production-ready code
2. **Feature branches** - Created from `main` for new features/fixes
3. **Release branches** - Created from `main` for release preparation

### Creating a Feature Branch

```bash
# Ensure you're on main and up to date
git checkout main
git pull upstream main

# Create and switch to a new feature branch
git checkout -b feature/your-feature-name

# Or for bug fixes
git checkout -b fix/your-bug-description
```

### Branch Naming Convention

- `feature/` - New features
- `fix/` - Bug fixes
- `docs/` - Documentation updates
- `refactor/` - Code refactoring
- `test/` - Adding or updating tests
- `chore/` - Maintenance tasks

Examples:
- `feature/add-mysql-adapter`
- `fix/security-jwt-validation`
- `docs/update-api-documentation`

## Branch Protection and Governance

### Current Protection Status

This project implements branch protection to ensure code quality and security. The current configuration is designed to support both single maintainer and community-driven development.

#### Current Settings (Single Maintainer Phase)
- **Required approvals**: 1 (will increase to 2 as community grows)
- **Status checks**: Build, test, and lint must pass
- **Emergency bypass**: Available for maintainers
- **Signed commits**: Not required (will be enabled later)
- **Linear history**: Not required (will be enabled later)

#### Protected Branches
- **`main`** - Production-ready code (strictest protection)
- **`develop`** - Integration branch (moderate protection)
- **`release/*`** - Release preparation branches
- **`hotfix/*`** - Critical bug fixes

### Governance Evolution Plan

As the community grows, we will progressively enhance our governance structure:

#### Phase 1: Single Maintainer (Current)
- ✅ 1 approval required for pull requests
- ✅ Basic CI/CD checks (build, test, lint)
- ✅ Maintainer bypass for emergencies
- ✅ Simple CODEOWNERS (maintainer only)

#### Phase 2: First Contributors (2-5 people)
- ✅ Keep 1 approval requirement
- ✅ Enable CODEOWNERS reviews
- ✅ Add signed commits requirement
- ✅ Add security scanning
- ✅ Add dependency scanning

#### Phase 3: Established Community (5+ people)
- ✅ Increase to 2 required approvals
- ✅ Add community maintainers
- ✅ Enable advanced protection features
- ✅ Add performance monitoring
- ✅ Establish governance committee

#### Phase 4: Enterprise Ready (20+ people)
- ✅ Multiple maintainer teams
- ✅ Advanced security features
- ✅ Comprehensive testing requirements
- ✅ Formal governance structure

### Governance Milestones

- **5 contributors**: Enable CODEOWNERS reviews
- **10 contributors**: Increase to 2 required approvals
- **50 contributors**: Add community maintainers
- **100 contributors**: Establish governance committee

### Emergency Procedures

For critical fixes and emergencies:

#### Option 1: Maintainer Bypass
Maintainers can bypass protection rules for urgent fixes:
```bash
# Maintainers can push directly to protected branches
git push origin main
```

#### Option 2: Emergency Branch Pattern
```bash
# Create emergency fix branch
git checkout -b hotfix/critical-security-fix

# Make the fix
git commit -m "fix(security): critical vulnerability patch"

# Merge directly
git checkout main
git merge hotfix/critical-security-fix --no-ff

# Clean up
git branch -d hotfix/critical-security-fix
```

#### Option 3: Emergency PR with Bypass
For critical issues, create PRs with emergency prefix:
```bash
# Create emergency PR with [EMERGENCY] prefix
git checkout -b hotfix/urgent-fix
# Make changes
git commit -m "fix: urgent issue resolution"
git push origin hotfix/urgent-fix
# Create PR with title: "[EMERGENCY] Urgent fix: description"
```

**For detailed emergency procedures, see [EMERGENCY_PROCEDURES.md](.github/EMERGENCY_PROCEDURES.md)**

### Code Ownership

The project uses CODEOWNERS to ensure appropriate review of changes:

#### Current Ownership Structure
- **Global owner**: Project maintainer
- **Service-specific owners**: Maintainer with fallback
- **Database adapters**: Maintainer with future expert assignment
- **Documentation**: Maintainer with future docs team
- **Infrastructure**: Maintainer with future DevOps team

#### Future Ownership Structure
As the community grows, ownership will be distributed to specialized teams:
- **Core services**: Core team
- **Security**: Security team
- **Database adapters**: Database team with database-specific experts
- **API**: API team
- **Documentation**: Documentation team
- **Infrastructure**: DevOps team

### Review Process Expectations

#### Current Review Timeline
As a single maintainer, I aim to review pull requests within:
- **Critical issues**: 24 hours
- **Regular features**: 3-5 days
- **Documentation**: 1 week

#### Review Guidelines
- **Be patient**: Single maintainer means limited bandwidth
- **Self-review**: Use the PR template checklist before submitting
- **Automated checks**: Ensure all CI/CD checks pass
- **Clear descriptions**: Provide detailed explanations of changes
- **Follow templates**: Complete all required sections

#### Getting Help
If your PR has been waiting too long:
1. **Ping the maintainer** with a friendly reminder
2. **Check if all checks are passing**
3. **Ensure the PR template is complete**
4. **Consider if the change is truly urgent**

### Security Considerations

#### Current Security Measures
- **Branch protection**: Prevents direct pushes to main
- **Required reviews**: Ensures code review before merging
- **Status checks**: Validates code quality and tests
- **Emergency procedures**: Allows urgent security fixes

#### Planned Security Enhancements
- **Signed commits**: Will be required for all commits
- **CodeQL analysis**: Automated security scanning
- **Dependency scanning**: Vulnerability detection
- **Security-focused reviews**: Specialized security review process

### Performance and Quality

#### Current Quality Measures
- **Automated testing**: Unit and integration tests
- **Code linting**: Style and quality checks
- **Build verification**: Ensures code compiles
- **Template completion**: Ensures complete information

#### Planned Quality Enhancements
- **Performance testing**: Automated performance benchmarks
- **Coverage requirements**: Minimum test coverage thresholds
- **Documentation checks**: Automated documentation validation
- **API compatibility**: Automated API compatibility testing

## Code Standards

### Go Code Style

We follow the [Effective Go](https://golang.org/doc/effective_go.html) guidelines and use `gofmt` for formatting.

1. **Run the linter** before committing:
   ```bash
   make lint
   ```

2. **Format your code**:
   ```bash
   go fmt ./...
   ```

3. **Run static analysis**:
   ```bash
   go vet ./...
   ```

### Code Organization

1. **Package structure** - Follow Go conventions for package organization
2. **Error handling** - Always check and handle errors appropriately
3. **Logging** - Use structured logging with appropriate log levels
4. **Comments** - Document exported functions and complex logic
5. **Naming** - Use clear, descriptive names for variables, functions, and packages

### Example Code Style

```go
// Good: Clear function name with documentation
// CreateUser creates a new user in the specified tenant
func (s *UserService) CreateUser(ctx context.Context, tenantID string, user *User) (*User, error) {
    if user == nil {
        return nil, errors.New("user cannot be nil")
    }
    
    // Validate user data
    if err := user.Validate(); err != nil {
        return nil, fmt.Errorf("invalid user data: %w", err)
    }
    
    // Implementation...
    return user, nil
}
```

### Protocol Buffer Standards

When modifying API definitions:

1. **Backward compatibility** - Maintain backward compatibility when possible
2. **Field numbering** - Never reuse field numbers
3. **Documentation** - Add clear comments for all fields and messages
4. **Validation** - Include validation rules where appropriate

## Testing

### Running Tests

```bash
# Run all tests
make test

# Run tests for a specific package
go test ./services/core/...

# Run tests with coverage
go test -cover ./...

# Run tests with verbose output
go test -v ./...
```

### Writing Tests

1. **Test coverage** - Aim for at least 80% test coverage
2. **Test naming** - Use descriptive test names that explain the scenario
3. **Test organization** - Group related tests using subtests
4. **Mocking** - Use interfaces for testability

### Example Test

```go
func TestUserService_CreateUser(t *testing.T) {
    tests := []struct {
        name      string
        user      *User
        wantErr   bool
        errString string
    }{
        {
            name:      "valid user",
            user:      &User{Email: "test@example.com", Name: "Test User"},
            wantErr:   false,
        },
        {
            name:      "nil user",
            user:      nil,
            wantErr:   true,
            errString: "user cannot be nil",
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            service := NewUserService()
            _, err := service.CreateUser(context.Background(), "tenant1", tt.user)
            
            if tt.wantErr {
                assert.Error(t, err)
                if tt.errString != "" {
                    assert.Contains(t, err.Error(), tt.errString)
                }
            } else {
                assert.NoError(t, err)
            }
        })
    }
}
```

## Documentation

### Code Documentation

1. **Package comments** - Document the purpose of each package
2. **Function comments** - Document exported functions
3. **Type comments** - Document exported types and structs
4. **Example usage** - Provide examples for complex APIs

### API Documentation

When adding new API endpoints:

1. **Update Protocol Buffer definitions** with clear comments
2. **Add API documentation** in the service's README
3. **Include examples** of request/response formats
4. **Document error codes** and their meanings

### README Updates

When adding new features:

1. **Update service README** with new functionality
2. **Add usage examples** for new commands or APIs
3. **Update architecture diagrams** if needed
4. **Document configuration changes**

## Submitting Changes

### Commit Guidelines

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

Types:
- `feat` - New feature
- `fix` - Bug fix
- `docs` - Documentation changes
- `style` - Code style changes (formatting, etc.)
- `refactor` - Code refactoring
- `test` - Adding or updating tests
- `chore` - Maintenance tasks

Examples:
```
feat(anchor): add MySQL database adapter

fix(security): validate JWT token expiration

docs(api): update authentication endpoint documentation

test(core): add unit tests for user service
```

### Pull Request Process

1. **Create a pull request** from your feature branch to `main`
2. **Use a descriptive title** that summarizes the change
3. **Fill out the PR template** completely
4. **Link related issues** using keywords (e.g., "Fixes #123")
5. **Request reviews** from maintainers

### Pull Request Template

```markdown
## Description
Brief description of the changes made.

## Type of Change
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update

## Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Manual testing completed

## Checklist
- [ ] Code follows the style guidelines
- [ ] Self-review of code completed
- [ ] Code is commented, particularly in hard-to-understand areas
- [ ] Corresponding changes to documentation made
- [ ] No new warnings generated
- [ ] Tests added that prove fix is effective or feature works

## Related Issues
Closes #(issue number)
```

## Review Process

### Review Guidelines

1. **Be respectful** and constructive in feedback
2. **Focus on the code** and its impact
3. **Ask questions** when something is unclear
4. **Suggest improvements** when appropriate
5. **Approve only when satisfied** with the changes

### Review Checklist

- [ ] Code follows project standards
- [ ] Tests are included and pass
- [ ] Documentation is updated
- [ ] No security issues introduced
- [ ] Performance impact considered
- [ ] Backward compatibility maintained (if applicable)

### Addressing Review Comments

1. **Respond to all comments** - acknowledge feedback
2. **Make requested changes** - update code as needed
3. **Push updates** - commit and push changes to your branch
4. **Request re-review** - when ready for another review

## Release Process

### Versioning

We follow [Semantic Versioning](https://semver.org/) (SemVer):

- **MAJOR** - Breaking changes
- **MINOR** - New features (backward compatible)
- **PATCH** - Bug fixes (backward compatible)

### Release Steps

1. **Create release branch** from `main`
2. **Update version** in relevant files
3. **Update changelog** with all changes
4. **Run full test suite** and integration tests
5. **Create release tag** and push to GitHub
6. **Update documentation** for the new release

## Community Guidelines

### Communication

- **Be respectful** and inclusive in all communications
- **Use clear language** and avoid jargon when possible
- **Ask questions** if something is unclear
- **Share knowledge** and help other contributors

### Getting Help

- **Check existing issues** before creating new ones
- **Search documentation** for answers
- **Ask in discussions** for general questions
- **Create detailed issues** for bugs or feature requests

### Recognition

Contributors will be recognized in:
- **Contributors list** on GitHub
- **Release notes** for significant contributions
- **Project documentation** for major features

## Questions or Need Help?

If you have questions or need help with contributing:

- **GitHub Issues** - For bugs and feature requests
- **GitHub Discussions** - For questions and general discussion
- **Documentation** - Check the project wiki and README
- **Community Chat** - Join our Discord/Slack for real-time help

Thank you for contributing to reDB Node! Your contributions help make this project better for everyone. 