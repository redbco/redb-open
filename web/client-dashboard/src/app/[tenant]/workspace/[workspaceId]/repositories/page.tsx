import { GitBranch, Plus, GitCommit, Database, GitMerge, Tag, Clock, User } from 'lucide-react';

interface RepositoriesPageProps {
  params: Promise<{
    tenant: string;
    workspaceId: string;
  }>;
}

export default async function RepositoriesPage({ params }: RepositoriesPageProps) {
  const { tenant, workspaceId } = await params;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Schema Repositories</h2>
          <p className="text-muted-foreground mt-2">
            Version control for database schemas with branches and commits.
          </p>
        </div>
        <button className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors">
          <Plus className="h-4 w-4 mr-2" />
          Create Repository
        </button>
      </div>

      {/* Repository List */}
      <div className="bg-card border border-border rounded-lg">
        <div className="px-6 py-4 border-b border-border">
          <h3 className="text-lg font-semibold text-foreground">Schema Repositories</h3>
        </div>
        <div className="divide-y divide-border">
          {[
            {
              id: 'repo-users',
              name: 'users-schema',
              description: 'User management and authentication schemas',
              branches: 3,
              commits: 24,
              lastCommit: '2 hours ago',
              lastCommitAuthor: 'john.doe',
              lastCommitMessage: 'Add user preferences table schema',
              connectedDatabases: ['users_prod', 'users_staging'],
              defaultBranch: 'main'
            },
            {
              id: 'repo-orders',
              name: 'orders-schema',
              description: 'E-commerce order processing schemas',
              branches: 5,
              commits: 18,
              lastCommit: '1 day ago',
              lastCommitAuthor: 'jane.smith',
              lastCommitMessage: 'Update order status enum values',
              connectedDatabases: ['orders_prod', 'orders_staging', 'orders_dev'],
              defaultBranch: 'main'
            },
            {
              id: 'repo-analytics',
              name: 'analytics-schema',
              description: 'Data warehouse and analytics schemas',
              branches: 2,
              commits: 12,
              lastCommit: '3 days ago',
              lastCommitAuthor: 'mike.johnson',
              lastCommitMessage: 'Add new fact tables for reporting',
              connectedDatabases: ['analytics_prod'],
              defaultBranch: 'main'
            },
            {
              id: 'repo-inventory',
              name: 'inventory-schema',
              description: 'Product inventory and catalog schemas',
              branches: 4,
              commits: 32,
              lastCommit: '5 hours ago',
              lastCommitAuthor: 'sarah.wilson',
              lastCommitMessage: 'Refactor product categorization schema',
              connectedDatabases: [],
              defaultBranch: 'main'
            }
          ].map((repo) => (
            <div key={repo.id} className="px-6 py-4">
              <div className="flex items-start justify-between mb-3">
                <div className="flex items-center space-x-3">
                  <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
                    <GitBranch className="h-5 w-5 text-primary" />
                  </div>
                  <div>
                    <h4 className="font-medium text-foreground">{repo.name}</h4>
                    <p className="text-sm text-muted-foreground">{repo.description}</p>
                  </div>
                </div>
                <div className="flex items-center space-x-2">
                  <span className="px-2 py-1 bg-muted text-muted-foreground text-xs rounded">
                    {repo.defaultBranch}
                  </span>
                  <button className="p-1 rounded-md hover:bg-accent hover:text-accent-foreground">
                    <GitBranch className="h-4 w-4" />
                  </button>
                </div>
              </div>

              {/* Last Commit Info */}
              <div className="flex items-center space-x-3 mb-3 p-3 bg-muted/50 rounded-lg">
                <GitCommit className="h-4 w-4 text-muted-foreground" />
                <div className="flex-1">
                  <p className="text-sm font-medium text-foreground">{repo.lastCommitMessage}</p>
                  <div className="flex items-center space-x-2 text-xs text-muted-foreground mt-1">
                    <User className="h-3 w-3" />
                    <span>{repo.lastCommitAuthor}</span>
                    <Clock className="h-3 w-3" />
                    <span>{repo.lastCommit}</span>
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm mb-3">
                <div>
                  <p className="text-muted-foreground">Branches</p>
                  <p className="font-medium text-foreground">{repo.branches}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Total Commits</p>
                  <p className="font-medium text-foreground">{repo.commits}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Connected Databases</p>
                  <p className="font-medium text-foreground">{repo.connectedDatabases.length}</p>
                </div>
              </div>

              {/* Connected Databases */}
              {repo.connectedDatabases.length > 0 && (
                <div className="mb-3">
                  <p className="text-xs text-muted-foreground mb-2">Connected Databases</p>
                  <div className="flex flex-wrap gap-1">
                    {repo.connectedDatabases.map((db) => (
                      <span key={db} className="inline-flex items-center px-2 py-1 bg-primary/10 text-primary text-xs rounded">
                        <Database className="h-3 w-3 mr-1" />
                        {db}
                      </span>
                    ))}
                  </div>
                </div>
              )}

              <div className="flex space-x-2">
                <button className="text-xs text-primary hover:text-primary/80 font-medium">
                  View Branches
                </button>
                <button className="text-xs text-muted-foreground hover:text-foreground font-medium">
                  Commit History
                </button>
                <button className="text-xs text-muted-foreground hover:text-foreground font-medium">
                  Compare Changes
                </button>
                {repo.connectedDatabases.length === 0 && (
                  <button className="text-xs text-green-600 hover:text-green-700 font-medium">
                    Connect Database
                  </button>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Repository Features */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">Branch Management</h3>
            <GitBranch className="h-5 w-5 text-muted-foreground" />
          </div>
          
          <div className="space-y-3">
            {[
              { branch: 'main', type: 'default', commits: 45, lastUpdate: '2 hours ago' },
              { branch: 'staging', type: 'environment', commits: 42, lastUpdate: '1 day ago' },
              { branch: 'feature/user-roles', type: 'feature', commits: 3, lastUpdate: '3 hours ago' },
              { branch: 'hotfix/order-validation', type: 'hotfix', commits: 2, lastUpdate: '5 hours ago' }
            ].map((branch, index) => (
              <div key={index} className="flex items-center justify-between p-3 border border-border rounded-md">
                <div className="flex items-center space-x-3">
                  <GitBranch className="h-4 w-4 text-muted-foreground" />
                  <div>
                    <p className="font-medium text-foreground text-sm">{branch.branch}</p>
                    <p className="text-xs text-muted-foreground">{branch.commits} commits • {branch.lastUpdate}</p>
                  </div>
                </div>
                <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                  branch.type === 'default' 
                    ? 'bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-400'
                    : branch.type === 'feature'
                    ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
                    : branch.type === 'hotfix'
                    ? 'bg-red-100 text-red-800 dark:bg-red-900/20 dark:text-red-400'
                    : 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400'
                }`}>
                  {branch.type}
                </span>
              </div>
            ))}
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">Recent Activity</h3>
            <GitCommit className="h-5 w-5 text-muted-foreground" />
          </div>
          
          <div className="space-y-3">
            {[
              { action: 'Commit', repo: 'users-schema', author: 'john.doe', message: 'Add user preferences table', time: '2 hours ago' },
              { action: 'Merge', repo: 'orders-schema', author: 'jane.smith', message: 'Merge feature/payment-methods', time: '6 hours ago' },
              { action: 'Branch', repo: 'inventory-schema', author: 'sarah.wilson', message: 'Create feature/product-variants', time: '1 day ago' },
              { action: 'Tag', repo: 'analytics-schema', author: 'mike.johnson', message: 'Release v2.1.0', time: '2 days ago' }
            ].map((activity, index) => (
              <div key={index} className="flex items-start space-x-3 p-3 border border-border rounded-md">
                <div className={`w-6 h-6 rounded-full flex items-center justify-center ${
                  activity.action === 'Commit' ? 'bg-green-100 dark:bg-green-900/20' :
                  activity.action === 'Merge' ? 'bg-blue-100 dark:bg-blue-900/20' :
                  activity.action === 'Branch' ? 'bg-purple-100 dark:bg-purple-900/20' :
                  'bg-orange-100 dark:bg-orange-900/20'
                }`}>
                  {activity.action === 'Commit' && <GitCommit className="h-3 w-3 text-green-600 dark:text-green-400" />}
                  {activity.action === 'Merge' && <GitMerge className="h-3 w-3 text-blue-600 dark:text-blue-400" />}
                  {activity.action === 'Branch' && <GitBranch className="h-3 w-3 text-purple-600 dark:text-purple-400" />}
                  {activity.action === 'Tag' && <Tag className="h-3 w-3 text-orange-600 dark:text-orange-400" />}
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-foreground">{activity.message}</p>
                  <div className="flex items-center space-x-2 text-xs text-muted-foreground mt-1">
                    <span>{activity.author}</span>
                    <span>•</span>
                    <span>{activity.repo}</span>
                    <span>•</span>
                    <span>{activity.time}</span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

export async function generateMetadata({ params }: RepositoriesPageProps) {
  const { tenant, workspaceId } = await params;
  
  return {
    title: `Repositories | ${workspaceId} | ${tenant} | reDB`,
    description: `Schema repositories for ${workspaceId} workspace`,
  };
}
