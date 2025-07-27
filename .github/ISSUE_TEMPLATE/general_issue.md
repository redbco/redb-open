---
name: General Issue
about: Report an issue that doesn't fit other templates
title: '[ISSUE] '
labels: ['needs-triage']
assignees: ''
---

## Issue Description
<!-- A clear and concise description of the issue -->

## Issue Type
<!-- What type of issue is this? -->

- [ ] **Question** - General question about reDB Node
- [ ] **Discussion** - Topic for community discussion
- [ ] **Support Request** - Need help with configuration or usage
- [ ] **Performance Issue** - Performance problems or concerns
- [ ] **Security Concern** - Security-related issue or question
- [ ] **Compatibility Issue** - Compatibility with other systems
- [ ] **Build Issue** - Problems with building or compiling
- [ ] **Deployment Issue** - Problems with deployment or installation
- [ ] **Configuration Issue** - Problems with configuration
- [ ] **Integration Issue** - Problems with third-party integrations
- [ ] **Other** - Specify type

## Affected Service/Component
<!-- Mark the service(s) or component(s) that are affected -->

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
- [ ] **Multiple Services** - Affects multiple components
- [ ] **System-wide** - Affects the entire system

## Environment Information
<!-- Provide relevant environment information -->

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

## Detailed Description
<!-- Provide a detailed description of the issue -->

### What Happened
<!-- Describe what happened -->

### What You Expected
<!-- Describe what you expected to happen -->

### Steps to Reproduce
<!-- If applicable, provide steps to reproduce the issue -->

```bash
# Step 1: Command or action
# Step 2: Command or action
# Step 3: Command or action
```

### Error Messages
<!-- If applicable, paste any error messages -->

```bash
# Error output here
```

## Impact Assessment
<!-- Help us understand the impact of this issue -->

- [ ] **Critical** - System completely unusable
- [ ] **High** - Major functionality affected
- [ ] **Medium** - Some functionality affected
- [ ] **Low** - Minor issue, workaround available
- [ ] **Cosmetic** - Visual or documentation issue
- [ ] **Information Only** - Just a question or discussion

## Current Workaround
<!-- If you found a workaround, describe it here -->

## Additional Context
<!-- Add any other context about the issue here -->

### Related Issues
<!-- Link to any related issues or discussions -->

### External References
<!-- Links to relevant documentation, standards, or similar issues -->

### Screenshots
<!-- If applicable, add screenshots to help explain the issue -->

## Questions for Maintainers
<!-- If this is a question, list specific questions you have -->

1. **Question 1:** [Your question]
2. **Question 2:** [Your question]
3. **Question 3:** [Your question]

## Community Discussion
<!-- If this is a discussion topic, provide context for the discussion -->

### Topic
<!-- What should the community discuss? -->

### Background
<!-- Provide background information for the discussion -->

### Questions for Discussion
<!-- What specific questions should be discussed? -->

## Support Request
<!-- If this is a support request, provide details -->

### What You're Trying to Do
<!-- Describe what you're trying to accomplish -->

### What You've Tried
<!-- Describe what you've already tried -->

### Specific Questions
<!-- What specific help do you need? -->

## Checklist
<!-- Before submitting, please ensure you've completed these steps -->

- [ ] I have searched existing issues to avoid duplicates
- [ ] I have provided a clear description of the issue
- [ ] I have included relevant environment information
- [ ] I have provided steps to reproduce (if applicable)
- [ ] I have included error messages (if applicable)
- [ ] I have assessed the impact of the issue
- [ ] I have provided additional context
- [ ] I have checked the documentation for answers 