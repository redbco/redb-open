# reDB Client Dashboard

Multi-tenant operational dashboard for reDB platform providing comprehensive database management, workspace operations, and mesh infrastructure monitoring.

## Overview

The Client Dashboard is a Next.js application that provides a unified interface for managing reDB infrastructure across multiple organizational levels: tenant-wide operations, workspace-specific management, and mesh network monitoring.

## Architecture

The dashboard follows a three-tier architectural pattern:

- **Tenant Level**: Organization-wide management (users, integrations, mesh overview)
- **Workspace Level**: Environment-specific operations (databases, jobs, repositories)  
- **Mesh Level**: Network infrastructure management (satellites, anchors, regions)

```
web/dashboard/
├── src/
│   ├── app/                           # Next.js 15 App Router
│   │   ├── [tenant]/                  # Tenant-scoped routes
│   │   │   ├── (tenant)/              # Tenant-level pages
│   │   │   │   ├── overview/          # Tenant operations overview
│   │   │   │   ├── workspaces/        # Workspace management
│   │   │   │   ├── mesh/              # Mesh topology overview
│   │   │   │   ├── satellites/        # Satellite node management
│   │   │   │   ├── anchors/           # Anchor node management
│   │   │   │   ├── regions/           # Regional infrastructure
│   │   │   │   ├── integrations/      # RAG, LLM, Webhook integrations
│   │   │   │   ├── access/            # User and permission management
│   │   │   │   └── profile/           # User profile and settings
│   │   │   ├── workspace/[workspaceId]/ # Workspace-scoped routes
│   │   │   │   ├── dashboard/         # Workspace operations overview
│   │   │   │   ├── instances/         # Database instances
│   │   │   │   ├── databases/         # Logical databases
│   │   │   │   ├── repositories/      # Schema version control
│   │   │   │   ├── mappings/          # Data mappings
│   │   │   │   ├── relationships/     # Data replication/migration
│   │   │   │   ├── jobs/              # Jobs and tasks
│   │   │   │   ├── environments/      # Environment classification
│   │   │   │   ├── mcp-servers/       # MCP server management
│   │   │   │   └── settings/          # Workspace settings
│   │   │   └── auth/                  # Authentication
│   │   │       └── login/             # Tenant-aware login
│   │   ├── layout.tsx                 # Root layout with theme provider
│   │   └── page.tsx                   # Landing page redirect
│   ├── components/                    # React components
│   │   ├── auth/                      # Authentication components
│   │   │   ├── LoginForm.tsx          # Login form with tenant support
│   │   │   ├── TenantBranding.tsx     # Tenant-specific branding
│   │   │   └── LoadingSpinner.tsx     # Loading states
│   │   ├── layout/                    # Layout components
│   │   │   ├── IconTenantSidebar.tsx  # Icon-only tenant navigation
│   │   │   ├── TenantDashboardLayout.tsx # Tenant-level layout
│   │   │   ├── WorkspaceDashboardLayout.tsx # Workspace-level layout
│   │   │   ├── AsideMenu.tsx          # Reusable aside component
│   │   │   └── WorkspaceSidebar.tsx   # Workspace navigation items
│   │   ├── workspace/                 # Workspace components
│   │   │   ├── WorkspaceOverview.tsx  # Workspace operational dashboard
│   │   │   ├── WorkspaceMenu.tsx      # Workspace aside navigation
│   │   │   ├── WorkspaceListMenu.tsx  # Workspace list for tenant
│   │   │   └── WorkspaceSelector.tsx  # Workspace switching
│   │   ├── mesh/                      # Mesh components
│   │   │   └── MeshMenu.tsx           # Mesh navigation and status
│   │   └── theme/                     # Theme components
│   │       └── ThemeToggle.tsx        # Dark/light mode toggle
│   ├── lib/                           # Utilities and providers
│   │   ├── auth/                      # Authentication utilities
│   │   │   ├── api.ts                 # Auth API integration
│   │   │   └── storage.ts             # Session storage management
│   │   ├── theme/                     # Theme management
│   │   │   └── theme-provider.tsx     # Theme context provider
│   │   └── workspace/                 # Workspace context
│   │       ├── index.ts               # Workspace provider exports
│   │       └── workspace-context.tsx  # Workspace context
│   └── types/                         # TypeScript type definitions
│       ├── auth.ts                    # Authentication types
│       └── index.ts                   # General types
├── public/                            # Static assets
├── package.json
├── next.config.ts
├── tsconfig.json
└── tailwind.config.ts
```

## Technology Stack

- **Next.js 15** - React framework with App Router
- **React 19** - UI framework  
- **TypeScript 5** - Type safety
- **Tailwind CSS** - Utility-first styling
- **Lucide React** - Icon library
- **next-themes** - Theme management

## Features

### Multi-Level Navigation Architecture

The dashboard uses a dual-sidebar layout with contextual aside menus:

- **Icon Tenant Sidebar** (Left): Persistent tenant-level navigation
- **Contextual Aside Menu** (Right): Dynamic content based on current context
  - Workspace menu for workspace-level pages
  - Mesh menu for mesh-level pages  
  - Workspace list for tenant-level workspace management

### Tenant Level Operations

#### 🏢 **Operations Overview** (`/[tenant]/overview`)
- Real-time monitoring dashboard for entire tenant infrastructure
- Mesh topology visualization with health indicators
- Active jobs tracking across all workspaces
- Database instances health matrix
- Data relationships monitoring
- Integration health status (RAG, LLM, Webhooks)
- Quick action center for immediate operations

#### 💼 **Workspace Management** (`/[tenant]/workspaces`)
- Comprehensive workspace administration dashboard
- Cross-workspace statistics and analytics
- Workspace creation, deletion, and configuration
- Health overview across all workspaces
- Resource utilization tracking
- Performance analytics and recent activity

#### 🕸️ **Mesh Infrastructure** (`/[tenant]/mesh`, `/satellites`, `/anchors`, `/regions`)
- Network topology management and monitoring
- Satellite node management (API/MCP nodes)
- Anchor node management (database nodes)
- Regional infrastructure distribution
- Node health monitoring and diagnostics

#### 🔌 **Integrations** (`/[tenant]/integrations`)
- RAG integrations for document embeddings
- LLM integrations for data processing
- Webhook integrations for event triggers
- Integration health monitoring and configuration

#### 👤 **User Profile** (`/[tenant]/profile`)
- Personal and work information management
- Security settings (password, 2FA, active sessions)
- Preferences (theme, notifications, language)
- Activity history and audit log

### Workspace Level Operations

#### 💼 **Workspace Operations** (`/[tenant]/workspace/[workspaceId]/dashboard`)
- Environment-specific operational overview
- Real-time database instance monitoring
- Active jobs and tasks tracking
- Data relationships health status
- Schema repository management
- Performance metrics dashboard

#### 🗄️ **Database Management** (`/[tenant]/workspace/[workspaceId]/databases`)
- Logical database management across instances
- Instance grouping and environment classification
- Repository connections at branch level
- Schema management and version control

#### 📚 **Schema Repositories** (`/[tenant]/workspace/[workspaceId]/repositories`)
- Git-like schema version control
- Branch management and merging
- Commit history and schema evolution
- Database connections at branch level

#### 🔗 **Data Relationships** (`/[tenant]/workspace/[workspaceId]/relationships`)
- Active data replication and migration
- Performance monitoring (latency, throughput)
- Data flow visualization
- Relationship health tracking

#### ⚡ **Jobs & Tasks** (`/[tenant]/workspace/[workspaceId]/jobs`)
- Data migration jobs
- RAG processing tasks
- Background operations
- Progress tracking with ETAs

#### 🔄 **Mappings** (`/[tenant]/workspace/[workspaceId]/mappings`)
- Column-to-column data mappings
- Data transformation definitions
- Usage tracking across relationships

## Component Architecture

### Layout System

The dashboard uses a flexible layout system with three main components:

```typescript
// Tenant-level layout with optional aside content
<TenantDashboardLayout tenantId={tenant} asideContent={<WorkspaceListMenu />}>
  {children}
</TenantDashboardLayout>

// Workspace-level layout with workspace menu
<WorkspaceDashboardLayout tenantId={tenant} workspaceId={workspaceId}>
  {children}
</WorkspaceDashboardLayout>

// Reusable aside menu component
<AsideMenu>
  <WorkspaceMenu tenantId={tenant} workspaceId={workspaceId} />
</AsideMenu>
```

### Navigation Components

- **IconTenantSidebar**: Icon-only persistent navigation for tenant-level items
- **WorkspaceMenu**: Comprehensive workspace navigation and overview
- **MeshMenu**: Mesh infrastructure navigation and status
- **WorkspaceSelector**: Quick workspace switching component

### Operational Components

- **WorkspaceOverview**: Environment-specific operational dashboard
- **Various page components**: Specialized dashboards for each functional area

## Development

### Getting Started

```bash
# Install dependencies
npm install

# Start development server
npm run dev
# Dashboard available at http://localhost:3000

# Build for production
npm run build

# Start production server
npm start
```

### Environment Variables

Copy the example environment file and configure for your setup:

```bash
# Copy the example file
cp env.example .env.local

# Edit the configuration
nano .env.local
```

The example file includes all available configuration options:

```bash
# API Endpoints (Required)
NEXT_PUBLIC_CLIENT_API_URL=http://localhost:8080
NEXT_PUBLIC_SERVICE_API_URL=http://localhost:8081
NEXT_PUBLIC_QUERY_API_URL=http://localhost:8082

# Authentication (Required)
NEXT_PUBLIC_SESSION_NAME=reDB Client Dashboard
NEXT_PUBLIC_USER_AGENT=redb-dashboard/1.0.0

# Optional Development Settings
NODE_ENV=development
NEXT_PUBLIC_DEBUG=true
```

See `env.example` for complete configuration options and documentation.

## Routing Structure

### Tenant-Level Routes
```
/[tenant]/overview              # Tenant operations overview
/[tenant]/workspaces           # Workspace management
/[tenant]/mesh                 # Mesh topology
/[tenant]/satellites           # Satellite management
/[tenant]/anchors              # Anchor management  
/[tenant]/regions              # Regional infrastructure
/[tenant]/integrations         # Integration management
/[tenant]/access               # User management
/[tenant]/profile              # User profile
```

### Workspace-Level Routes
```
/[tenant]/workspace/[workspaceId]/dashboard      # Workspace operations
/[tenant]/workspace/[workspaceId]/instances      # Database instances
/[tenant]/workspace/[workspaceId]/databases      # Logical databases
/[tenant]/workspace/[workspaceId]/repositories   # Schema repositories
/[tenant]/workspace/[workspaceId]/mappings       # Data mappings
/[tenant]/workspace/[workspaceId]/relationships  # Data relationships
/[tenant]/workspace/[workspaceId]/jobs           # Jobs and tasks
/[tenant]/workspace/[workspaceId]/environments   # Environment classification
/[tenant]/workspace/[workspaceId]/settings       # Workspace settings
```

### Authentication Routes
```
/[tenant]/auth/login           # Tenant-aware authentication
```

## Authentication & Session Management

The dashboard includes a complete authentication system:

- **SessionStorage**: Client-side session management
- **Auth API**: Authentication service integration
- **Tenant-aware login**: Multi-tenant authentication support
- **Session validation**: Automatic session verification
- **Logout handling**: Secure session cleanup

## Theme Support

Built-in dark/light mode support with:

- **System preference detection**
- **Manual theme switching**
- **Persistent theme storage**
- **Smooth theme transitions**

## Performance Features

- **Server Components**: Optimized rendering with Next.js App Router
- **Dynamic imports**: Code splitting for better performance
- **Responsive design**: Mobile-first approach with Tailwind CSS
- **Optimized assets**: Next.js automatic optimization

## Contributing

When adding new features:

1. **Follow the three-tier architecture** (tenant/workspace/mesh levels)
2. **Use the established layout patterns** (dual sidebar with aside menus)
3. **Maintain consistent navigation** (icon sidebar + contextual aside)
4. **Follow TypeScript patterns** for type safety
5. **Use Tailwind CSS** for consistent styling
6. **Test across different tenant/workspace contexts**

## Deployment

The dashboard is designed to be deployed as a standalone Next.js application with support for:

- **Static export** for CDN deployment
- **Server-side rendering** for dynamic content
- **Docker containerization** for scalable deployment
- **Environment-specific configuration** for different deployment targets