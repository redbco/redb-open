import { Users, Shield, Key, UserPlus, Plus } from 'lucide-react';

interface AccessPageProps {
  params: Promise<{
    tenant: string;
  }>;
}

export default async function AccessPage({ params }: AccessPageProps) {
  const { tenant } = await params;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Access Management</h2>
          <p className="text-muted-foreground mt-2">
            Manage users, roles, permissions, and API tokens for your tenant.
          </p>
        </div>
        <div className="flex space-x-2">
          <button className="inline-flex items-center px-4 py-2 border border-input bg-background hover:bg-accent hover:text-accent-foreground rounded-md transition-colors">
            <Key className="h-4 w-4 mr-2" />
            Create API Token
          </button>
          <button className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors">
            <UserPlus className="h-4 w-4 mr-2" />
            Invite User
          </button>
        </div>
      </div>

      {/* Access Overview */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {[
          {
            title: 'Active Users',
            value: '24',
            change: '+3 this month',
            icon: Users,
            color: 'text-blue-600 dark:text-blue-400'
          },
          {
            title: 'Roles Defined',
            value: '8',
            change: '2 custom roles',
            icon: Shield,
            color: 'text-green-600 dark:text-green-400'
          },
          {
            title: 'API Tokens',
            value: '12',
            change: '3 expiring soon',
            icon: Key,
            color: 'text-orange-600 dark:text-orange-400'
          }
        ].map((metric, index) => (
          <div key={index} className="bg-card border border-border rounded-lg p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-muted-foreground">{metric.title}</p>
                <p className="text-2xl font-bold text-foreground mt-1">{metric.value}</p>
                <p className="text-sm text-muted-foreground mt-1">{metric.change}</p>
              </div>
              <div className={`w-12 h-12 rounded-lg bg-muted/50 flex items-center justify-center ${metric.color}`}>
                <metric.icon className="h-6 w-6" />
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Users Table */}
      <div className="bg-card border border-border rounded-lg">
        <div className="px-6 py-4 border-b border-border">
          <h3 className="text-lg font-semibold text-foreground">Users</h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-muted/50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">User</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">Role</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">Workspaces</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">Last Active</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {[
                { name: 'John Doe', email: 'john.doe@company.com', role: 'Admin', workspaces: 3, lastActive: '2 hours ago', status: 'active' },
                { name: 'Jane Smith', email: 'jane.smith@company.com', role: 'Developer', workspaces: 2, lastActive: '1 day ago', status: 'active' },
                { name: 'Mike Johnson', email: 'mike.johnson@company.com', role: 'Viewer', workspaces: 1, lastActive: '3 days ago', status: 'inactive' }
              ].map((user, index) => (
                <tr key={index} className="hover:bg-muted/50">
                  <td className="px-6 py-4">
                    <div>
                      <div className="text-sm font-medium text-foreground">{user.name}</div>
                      <div className="text-sm text-muted-foreground">{user.email}</div>
                    </div>
                  </td>
                  <td className="px-6 py-4">
                    <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-primary/10 text-primary">
                      {user.role}
                    </span>
                  </td>
                  <td className="px-6 py-4 text-sm text-foreground">{user.workspaces}</td>
                  <td className="px-6 py-4 text-sm text-muted-foreground">{user.lastActive}</td>
                  <td className="px-6 py-4">
                    <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${
                      user.status === 'active' 
                        ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
                        : 'bg-gray-100 text-gray-800 dark:bg-gray-900/20 dark:text-gray-400'
                    }`}>
                      {user.status}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Roles & Permissions */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">Roles</h3>
            <button className="text-sm text-primary hover:text-primary/80 font-medium">
              Manage Roles
            </button>
          </div>
          <div className="space-y-3">
            {[
              { name: 'Admin', users: 2, permissions: 'Full access' },
              { name: 'Developer', users: 8, permissions: 'Read/Write workspaces' },
              { name: 'Analyst', users: 6, permissions: 'Read-only access' },
              { name: 'Viewer', users: 8, permissions: 'View-only access' }
            ].map((role, index) => (
              <div key={index} className="flex items-center justify-between p-3 border border-border rounded-md">
                <div>
                  <p className="font-medium text-foreground">{role.name}</p>
                  <p className="text-sm text-muted-foreground">{role.permissions}</p>
                </div>
                <span className="text-sm text-muted-foreground">{role.users} users</span>
              </div>
            ))}
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">API Tokens</h3>
            <button className="text-sm text-primary hover:text-primary/80 font-medium">
              View All
            </button>
          </div>
          <div className="space-y-3">
            {[
              { name: 'Production API', created: '2024-01-01', expires: '2024-12-31', status: 'active' },
              { name: 'Staging API', created: '2024-01-15', expires: '2024-06-15', status: 'expiring' },
              { name: 'Analytics Service', created: '2024-02-01', expires: '2025-02-01', status: 'active' }
            ].map((token, index) => (
              <div key={index} className="flex items-center justify-between p-3 border border-border rounded-md">
                <div>
                  <p className="font-medium text-foreground">{token.name}</p>
                  <p className="text-sm text-muted-foreground">Expires: {token.expires}</p>
                </div>
                <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                  token.status === 'active' 
                    ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
                    : 'bg-orange-100 text-orange-800 dark:bg-orange-900/20 dark:text-orange-400'
                }`}>
                  {token.status}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

export async function generateMetadata({ params }: AccessPageProps) {
  const { tenant } = await params;
  
  return {
    title: `Access Management | ${tenant} | reDB`,
    description: `User and access management for ${tenant}`,
  };
}
