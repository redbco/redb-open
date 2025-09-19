## reDB Client Dashboard

Next.js dashboard for multi-tenant operations, workspace management, and mesh monitoring.

### Location

- Folder: `web/dashboard`
- Port: `http://localhost:3000` (dev)

### Requirements

- Node.js 18.18+ (or current LTS)
- npm 9+ (or pnpm/yarn)
- Running reDB services (Client API on 8080 by default)

### Environment

Copy and edit the example file:

```bash
cp web/dashboard/env.example web/dashboard/.env.local
```

Key variables:

- `NEXT_PUBLIC_CLIENT_API_URL` (required): Base URL of Client API, e.g. `http://localhost:8080`
- `NEXT_PUBLIC_SESSION_NAME` (optional): Display name for sessions
- `NEXT_PUBLIC_USER_AGENT` (optional): User agent string used in requests

See `web/dashboard/env.example` for the full list and inline docs.

### Develop

```bash
cd web/dashboard
npm install
npm run dev
# open http://localhost:3000
```

### Build & run (production)

```bash
cd web/dashboard
npm run build
npm start
```

### Authentication

The dashboard is tenant-aware. Authentication requests are sent to the Client API using the pattern:

```
{CLIENT_API_URL}/{tenantId}/api/v1/auth/*
```

Auth client source: `web/dashboard/src/lib/auth/api.ts`.

### App structure (high level)

```
web/dashboard/
├── src/app/                   # App Router (Next.js 15)
│   └── [tenant]/              # Tenant-scoped routes
│       ├── (tenant)/          # Tenant-level pages (overview, mesh, anchors, ...)
│       ├── workspace/[workspaceId]/  # Workspace-level pages (databases, jobs, ...)
│       └── auth/login/        # Tenant-aware login
├── src/components/            # Layout, menus, workspace/mesh components
├── src/lib/                   # Auth client, providers, utils
├── src/types/                 # TypeScript types
├── public/                    # Static assets
└── package.json
```

For a deeper tour, see `web/dashboard/README.md`.


