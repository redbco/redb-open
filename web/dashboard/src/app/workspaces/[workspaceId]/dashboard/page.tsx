import { LayoutDashboard, Database, Server, Activity } from 'lucide-react';

interface DashboardPageProps {
  params: Promise<{
    workspaceId: string;
  }>;
}

export default async function DashboardPage({ params }: DashboardPageProps) {
  const { workspaceId } = await params;

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-3xl font-bold text-foreground">Workspace Dashboard</h2>
        <p className="text-muted-foreground mt-2">
          Overview of your databases, instances, and active relationships.
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        {[
          {
            title: 'Total Databases',
            value: '0',
            change: 'No databases yet',
            icon: Database,
            color: 'text-blue-600 dark:text-blue-400'
          },
          {
            title: 'Active Instances',
            value: '0',
            change: 'No instances yet',
            icon: Server,
            color: 'text-green-600 dark:text-green-400'
          },
          {
            title: 'Relationships',
            value: '0',
            change: 'No relationships yet',
            icon: Activity,
            color: 'text-purple-600 dark:text-purple-400'
          },
          {
            title: 'Mappings',
            value: '0',
            change: 'No mappings yet',
            icon: LayoutDashboard,
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

      {/* Getting Started */}
      <div className="bg-card border border-border rounded-lg p-6">
        <h3 className="text-lg font-semibold text-foreground mb-4">Getting Started</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="p-4 border border-border rounded-lg">
            <Server className="h-8 w-8 text-primary mb-2" />
            <h4 className="font-medium text-foreground mb-1">Connect an Instance</h4>
            <p className="text-sm text-muted-foreground mb-3">
              Connect to your database servers (PostgreSQL, MySQL, MongoDB, etc.)
            </p>
            <a href={`/workspaces/${workspaceId}/instances`} className="text-sm text-primary hover:underline">
              Go to Instances →
            </a>
          </div>
          
          <div className="p-4 border border-border rounded-lg">
            <Database className="h-8 w-8 text-primary mb-2" />
            <h4 className="font-medium text-foreground mb-1">Connect a Database</h4>
            <p className="text-sm text-muted-foreground mb-3">
              Connect to specific databases within your instances
            </p>
            <a href={`/workspaces/${workspaceId}/databases`} className="text-sm text-primary hover:underline">
              Go to Databases →
            </a>
          </div>
        </div>
      </div>
    </div>
  );
}

export async function generateMetadata({ params }: DashboardPageProps) {
  const { workspaceId } = await params;
  
  return {
    title: `Dashboard | ${workspaceId} | reDB`,
    description: `Dashboard for ${workspaceId} workspace`,
  };
}

