import { Database, Plus, Server, GitBranch, Table, Settings, Activity, Link } from 'lucide-react';

interface DatabasesPageProps {
  params: Promise<{
    tenant: string;
    workspaceId: string;
  }>;
}

export default async function DatabasesPage({ params }: DatabasesPageProps) {
  const { tenant, workspaceId } = await params;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Logical Databases</h2>
          <p className="text-muted-foreground mt-2">
            Logical databases across instances with schema management and repository connections.
          </p>
        </div>
        <div className="flex items-center space-x-2">
          <select className="px-3 py-2 border border-input bg-background rounded-md text-sm">
            <option value="all">All Databases</option>
            <option value="databases-only">Databases Only</option>
            <option value="with-instances">Group by Instance</option>
          </select>
          <button className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors">
            <Plus className="h-4 w-4 mr-2" />
            Connect Database
          </button>
        </div>
      </div>

      {/* Databases Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        {[
          {
            title: 'Total Databases',
            value: '28',
            change: 'Across 8 instances',
            icon: Database,
            color: 'text-blue-600 dark:text-blue-400'
          },
          {
            title: 'Connected Repos',
            value: '18',
            change: 'Schema versioned',
            icon: GitBranch,
            color: 'text-green-600 dark:text-green-400'
          },
          {
            title: 'Total Tables',
            value: '342',
            change: 'Across all databases',
            icon: Table,
            color: 'text-purple-600 dark:text-purple-400'
          },
          {
            title: 'Active Connections',
            value: '156',
            change: 'Current sessions',
            icon: Activity,
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

      {/* Database Management Info */}
      <div className="bg-muted/50 border border-border rounded-lg p-6">
        <h3 className="text-lg font-semibold text-foreground mb-2">About Logical Databases</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 text-sm text-muted-foreground">
          <div>
            <h4 className="font-medium text-foreground mb-2">Database Management</h4>
            <ul className="space-y-1">
              <li>• Logical databases exist within instances</li>
              <li>• Each database can connect to a repository branch</li>
              <li>• Schema changes are tracked through repositories</li>
              <li>• Environment classification for deployment stages</li>
              <li>• Connection pooling and session management</li>
            </ul>
          </div>
          <div>
            <h4 className="font-medium text-foreground mb-2">Repository Integration</h4>
            <ul className="space-y-1">
              <li>• Databases connect to repository branches</li>
              <li>• Schema versions tracked through commits</li>
              <li>• Automatic schema drift detection</li>
              <li>• Migration scripts generated from changes</li>
              <li>• Rollback capabilities through version control</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}

export async function generateMetadata({ params }: DatabasesPageProps) {
  const { tenant, workspaceId } = await params;
  
  return {
    title: `Databases | ${workspaceId} | ${tenant} | reDB`,
    description: `Database management for ${workspaceId} workspace`,
  };
}
