import { Layers, Plus, Settings, Activity } from 'lucide-react';

interface EnvironmentsPageProps {
  params: Promise<{
    tenant: string;
  }>;
}

export default async function EnvironmentsPage({ params }: EnvironmentsPageProps) {
  const { tenant } = await params;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Environments</h2>
          <p className="text-muted-foreground mt-2">
            Logical groupings of resources to model application environments within this workspace.
          </p>
        </div>
        <button className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors">
          <Plus className="h-4 w-4 mr-2" />
          Create Environment
        </button>
      </div>

      {/* Environment Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {/* Mock environments for now */}
        {[
          {
            id: 'env-production',
            name: 'Production',
            description: 'Live production environment',
            status: 'active',
            instances: 3,
            databases: 12,
            lastDeployment: '2024-01-15T14:30:00Z'
          },
          {
            id: 'env-staging',
            name: 'Staging',
            description: 'Pre-production testing environment',
            status: 'active',
            instances: 2,
            databases: 8,
            lastDeployment: '2024-01-15T10:15:00Z'
          },
          {
            id: 'env-development',
            name: 'Development',
            description: 'Development and feature testing',
            status: 'active',
            instances: 1,
            databases: 5,
            lastDeployment: '2024-01-14T16:45:00Z'
          }
        ].map((environment) => (
          <div key={environment.id} className="bg-card border border-border rounded-lg p-6 hover:shadow-md transition-shadow">
            <div className="flex items-start justify-between mb-4">
              <div className="flex items-center space-x-3">
                <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
                  <Layers className="h-5 w-5 text-primary" />
                </div>
                <div>
                  <h3 className="font-semibold text-foreground">{environment.name}</h3>
                  <p className="text-sm text-muted-foreground">{environment.description}</p>
                </div>
              </div>
              <div className="px-2 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400">
                {environment.status}
              </div>
            </div>

            <div className="space-y-3">
              <div className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground">Instances</span>
                <span className="font-medium text-foreground">{environment.instances}</span>
              </div>
              <div className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground">Databases</span>
                <span className="font-medium text-foreground">{environment.databases}</span>
              </div>
              <div className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground">Last Deployment</span>
                <span className="font-medium text-foreground">
                  {new Date(environment.lastDeployment).toLocaleDateString()}
                </span>
              </div>
            </div>

            <div className="mt-4 pt-4 border-t border-border flex space-x-2">
              <button className="flex-1 text-sm text-primary hover:text-primary/80 font-medium flex items-center justify-center py-2">
                <Settings className="h-4 w-4 mr-1" />
                Configure
              </button>
              <button className="flex-1 text-sm text-muted-foreground hover:text-foreground font-medium flex items-center justify-center py-2">
                <Activity className="h-4 w-4 mr-1" />
                Activity
              </button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export async function generateMetadata({ params }: EnvironmentsPageProps) {
  const { tenant } = await params;
  
  return {
    title: `Environments | ${tenant} | reDB`,
    description: `Environment management for ${tenant} workspace`,
  };
}
