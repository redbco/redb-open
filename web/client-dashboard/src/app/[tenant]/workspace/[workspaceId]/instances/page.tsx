import { Server, Plus, Database, Activity } from 'lucide-react';
import { useWorkspace } from '@/lib/workspace';

interface InstancesPageProps {
  params: Promise<{
    tenant: string;
  }>;
}

export default async function InstancesPage({ params }: InstancesPageProps) {
  const { tenant } = await params;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Database Instances</h2>
          <p className="text-muted-foreground mt-2">
            Manage database instances and their connections within this workspace.
          </p>
        </div>
        <button className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors">
          <Plus className="h-4 w-4 mr-2" />
          Add Instance
        </button>
      </div>

      {/* Instance Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {/* Mock instances for now */}
        {[
          {
            id: 'inst-prod-001',
            name: 'Production Primary',
            type: 'PostgreSQL',
            status: 'healthy',
            databases: 8,
            connections: 45,
            region: 'us-east-1'
          },
          {
            id: 'inst-prod-002',
            name: 'Production Replica',
            type: 'PostgreSQL',
            status: 'healthy',
            databases: 8,
            connections: 23,
            region: 'us-east-1'
          },
          {
            id: 'inst-cache-001',
            name: 'Redis Cache',
            type: 'Redis',
            status: 'warning',
            databases: 1,
            connections: 156,
            region: 'us-east-1'
          }
        ].map((instance) => (
          <div key={instance.id} className="bg-card border border-border rounded-lg p-6 hover:shadow-md transition-shadow">
            <div className="flex items-start justify-between mb-4">
              <div className="flex items-center space-x-3">
                <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
                  <Server className="h-5 w-5 text-primary" />
                </div>
                <div>
                  <h3 className="font-semibold text-foreground">{instance.name}</h3>
                  <p className="text-sm text-muted-foreground">{instance.type}</p>
                </div>
              </div>
              <div className={`px-2 py-1 rounded-full text-xs font-medium ${
                instance.status === 'healthy' 
                  ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
                  : 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400'
              }`}>
                {instance.status}
              </div>
            </div>

            <div className="space-y-3">
              <div className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground flex items-center">
                  <Database className="h-4 w-4 mr-1" />
                  Databases
                </span>
                <span className="font-medium text-foreground">{instance.databases}</span>
              </div>
              <div className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground flex items-center">
                  <Activity className="h-4 w-4 mr-1" />
                  Connections
                </span>
                <span className="font-medium text-foreground">{instance.connections}</span>
              </div>
              <div className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground">Region</span>
                <span className="font-medium text-foreground">{instance.region}</span>
              </div>
            </div>

            <div className="mt-4 pt-4 border-t border-border">
              <button className="w-full text-sm text-primary hover:text-primary/80 font-medium">
                Manage Instance
              </button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export async function generateMetadata({ params }: InstancesPageProps) {
  const { tenant } = await params;
  
  return {
    title: `Database Instances | ${tenant} | reDB`,
    description: `Database instance management for ${tenant} workspace`,
  };
}
