import { Network, Server, Activity, AlertTriangle, CheckCircle, Clock } from 'lucide-react';

interface MeshPageProps {
  params: Promise<{
    tenant: string;
  }>;
}

export default async function MeshPage({ params }: MeshPageProps) {
  const { tenant } = await params;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Mesh Topology</h2>
          <p className="text-muted-foreground mt-2">
            Read-only view of the mesh network topology and node status. Mesh configuration is managed from the service dashboard.
          </p>
        </div>
        <div className="flex items-center space-x-2">
          <div className="w-2 h-2 bg-green-500 rounded-full"></div>
          <span className="text-sm text-muted-foreground">Mesh Status: Healthy</span>
        </div>
      </div>

      {/* Mesh Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        {[
          {
            title: 'Total Nodes',
            value: '12',
            change: 'Across 4 regions',
            icon: Network,
            color: 'text-blue-600 dark:text-blue-400'
          },
          {
            title: 'Healthy Nodes',
            value: '11',
            change: '1 maintenance',
            icon: CheckCircle,
            color: 'text-green-600 dark:text-green-400'
          },
          {
            title: 'Active Connections',
            value: '48',
            change: 'Inter-node links',
            icon: Activity,
            color: 'text-purple-600 dark:text-purple-400'
          },
          {
            title: 'Avg Latency',
            value: '12ms',
            change: 'Cross-region',
            icon: Clock,
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

      {/* Regional Distribution */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">Regional Distribution</h3>
            <Network className="h-5 w-5 text-muted-foreground" />
          </div>
          
          <div className="space-y-4">
            {[
              { region: 'us-east-1', nodes: 4, status: 'healthy', latency: '8ms' },
              { region: 'us-west-2', nodes: 3, status: 'healthy', latency: '12ms' },
              { region: 'eu-west-1', nodes: 3, status: 'healthy', latency: '15ms' },
              { region: 'ap-southeast-1', nodes: 2, status: 'maintenance', latency: '18ms' }
            ].map((region, index) => (
              <div key={index} className="flex items-center justify-between p-4 border border-border rounded-lg">
                <div className="flex items-center space-x-3">
                  <div className={`w-3 h-3 rounded-full ${
                    region.status === 'healthy' ? 'bg-green-500' : 'bg-yellow-500'
                  }`}></div>
                  <div>
                    <p className="font-medium text-foreground">{region.region}</p>
                    <p className="text-sm text-muted-foreground">{region.nodes} nodes</p>
                  </div>
                </div>
                <div className="text-right">
                  <p className="text-sm font-medium text-foreground">{region.latency}</p>
                  <p className="text-xs text-muted-foreground">avg latency</p>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">Node Status</h3>
            <Activity className="h-5 w-5 text-muted-foreground" />
          </div>
          
          <div className="space-y-3">
            {[
              { id: 'node-001', region: 'us-east-1', type: 'Primary', status: 'healthy', uptime: '99.9%' },
              { id: 'node-002', region: 'us-east-1', type: 'Replica', status: 'healthy', uptime: '99.8%' },
              { id: 'node-003', region: 'us-west-2', type: 'Primary', status: 'healthy', uptime: '99.9%' },
              { id: 'node-004', region: 'ap-southeast-1', type: 'Replica', status: 'maintenance', uptime: '98.5%' }
            ].map((node, index) => (
              <div key={index} className="flex items-center justify-between p-3 border border-border rounded-md">
                <div className="flex items-center space-x-3">
                  <div className={`w-2 h-2 rounded-full ${
                    node.status === 'healthy' ? 'bg-green-500' : 'bg-yellow-500'
                  }`}></div>
                  <div>
                    <p className="text-sm font-medium text-foreground">{node.id}</p>
                    <p className="text-xs text-muted-foreground">{node.region} • {node.type}</p>
                  </div>
                </div>
                <div className="text-right">
                  <p className="text-sm font-medium text-foreground">{node.uptime}</p>
                  <p className="text-xs text-muted-foreground">uptime</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Network Topology Visualization */}
      <div className="bg-card border border-border rounded-lg p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-foreground">Network Topology</h3>
          <div className="flex items-center space-x-4 text-sm text-muted-foreground">
            <div className="flex items-center space-x-2">
              <div className="w-3 h-3 bg-green-500 rounded-full"></div>
              <span>Healthy</span>
            </div>
            <div className="flex items-center space-x-2">
              <div className="w-3 h-3 bg-yellow-500 rounded-full"></div>
              <span>Maintenance</span>
            </div>
            <div className="flex items-center space-x-2">
              <div className="w-3 h-3 bg-red-500 rounded-full"></div>
              <span>Offline</span>
            </div>
          </div>
        </div>
        
        {/* Placeholder for network topology visualization */}
        <div className="h-64 bg-muted/20 rounded-lg border-2 border-dashed border-border flex items-center justify-center">
          <div className="text-center">
            <Network className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
            <p className="text-muted-foreground">Network topology visualization</p>
            <p className="text-sm text-muted-foreground mt-1">Interactive mesh diagram would be displayed here</p>
          </div>
        </div>
      </div>

      {/* Read-only Notice */}
      <div className="bg-muted/50 border border-border rounded-lg p-4">
        <div className="flex items-start space-x-3">
          <AlertTriangle className="h-5 w-5 text-orange-600 dark:text-orange-400 mt-0.5" />
          <div>
            <h4 className="font-medium text-foreground">Read-only View</h4>
            <p className="text-sm text-muted-foreground mt-1">
              This is a read-only view of the mesh topology. To configure mesh settings, add/remove nodes, 
              or modify network topology, please use the service dashboard with appropriate administrative privileges.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

export async function generateMetadata({ params }: MeshPageProps) {
  const { tenant } = await params;
  
  return {
    title: `Mesh Topology | ${tenant} | reDB`,
    description: `Mesh network topology for ${tenant}`,
  };
}
