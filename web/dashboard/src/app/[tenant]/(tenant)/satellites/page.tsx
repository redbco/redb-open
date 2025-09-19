import { Satellite, Plus, Activity, Server, Globe, Settings } from 'lucide-react';

interface SatellitesPageProps {
  params: Promise<{
    tenant: string;
  }>;
}

export default async function SatellitesPage() {

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Satellites</h2>
          <p className="text-muted-foreground mt-2">
            Manage satellite nodes that provide API access and host MCP servers. Satellites do not connect to databases directly.
          </p>
        </div>
        <button className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors">
          <Plus className="h-4 w-4 mr-2" />
          Deploy Satellite
        </button>
      </div>

      {/* Satellites Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        {[
          {
            title: 'Active Satellites',
            value: '6',
            change: '+1 this week',
            icon: Satellite,
            color: 'text-blue-600 dark:text-blue-400'
          },
          {
            title: 'API Endpoints',
            value: '18',
            change: '3 per satellite',
            icon: Server,
            color: 'text-green-600 dark:text-green-400'
          },
          {
            title: 'MCP Servers',
            value: '12',
            change: '8 active',
            icon: Activity,
            color: 'text-purple-600 dark:text-purple-400'
          },
          {
            title: 'Regions',
            value: '4',
            change: 'Global coverage',
            icon: Globe,
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

      {/* Satellite Nodes */}
      <div className="bg-card border border-border rounded-lg">
        <div className="px-6 py-4 border-b border-border">
          <h3 className="text-lg font-semibold text-foreground">Satellite Nodes</h3>
        </div>
        <div className="divide-y divide-border">
          {[
            {
              id: 'sat-api-001',
              name: 'API Gateway East',
              region: 'us-east-1',
              status: 'healthy',
              endpoints: 6,
              mcpServers: 3,
              uptime: '99.9%',
              lastSeen: '2 minutes ago'
            },
            {
              id: 'sat-api-002',
              name: 'API Gateway West',
              region: 'us-west-2',
              status: 'healthy',
              endpoints: 4,
              mcpServers: 2,
              uptime: '99.8%',
              lastSeen: '1 minute ago'
            },
            {
              id: 'sat-mcp-001',
              name: 'MCP Hub Europe',
              region: 'eu-west-1',
              status: 'healthy',
              endpoints: 2,
              mcpServers: 4,
              uptime: '99.7%',
              lastSeen: '30 seconds ago'
            },
            {
              id: 'sat-api-003',
              name: 'API Gateway Asia',
              region: 'ap-southeast-1',
              status: 'warning',
              endpoints: 3,
              mcpServers: 1,
              uptime: '98.5%',
              lastSeen: '5 minutes ago'
            }
          ].map((satellite) => (
            <div key={satellite.id} className="px-6 py-4">
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center space-x-3">
                  <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
                    <Satellite className="h-5 w-5 text-primary" />
                  </div>
                  <div>
                    <h4 className="font-medium text-foreground">{satellite.name}</h4>
                    <p className="text-sm text-muted-foreground">{satellite.id} • {satellite.region}</p>
                  </div>
                </div>
                <div className="flex items-center space-x-2">
                  <div className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${
                    satellite.status === 'healthy' 
                      ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
                      : 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400'
                  }`}>
                    {satellite.status}
                  </div>
                  <button className="p-1 rounded-md hover:bg-accent hover:text-accent-foreground">
                    <Settings className="h-4 w-4" />
                  </button>
                </div>
              </div>

              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                <div>
                  <p className="text-muted-foreground">API Endpoints</p>
                  <p className="font-medium text-foreground">{satellite.endpoints}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">MCP Servers</p>
                  <p className="font-medium text-foreground">{satellite.mcpServers}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Uptime</p>
                  <p className="font-medium text-foreground">{satellite.uptime}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Last Seen</p>
                  <p className="font-medium text-foreground">{satellite.lastSeen}</p>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Satellite Capabilities */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">API Endpoints</h3>
            <Server className="h-5 w-5 text-muted-foreground" />
          </div>
          
          <div className="space-y-3">
            {[
              { endpoint: '/api/v1/client', satellite: 'API Gateway East', requests: '1.2M/day' },
              { endpoint: '/api/v1/query', satellite: 'API Gateway West', requests: '850K/day' },
              { endpoint: '/api/v1/service', satellite: 'API Gateway East', requests: '450K/day' },
              { endpoint: '/api/v2/analytics', satellite: 'MCP Hub Europe', requests: '320K/day' }
            ].map((api, index) => (
              <div key={index} className="flex items-center justify-between p-3 border border-border rounded-md">
                <div>
                  <p className="font-medium text-foreground">{api.endpoint}</p>
                  <p className="text-sm text-muted-foreground">{api.satellite}</p>
                </div>
                <span className="text-sm text-muted-foreground">{api.requests}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">MCP Server Distribution</h3>
            <Activity className="h-5 w-5 text-muted-foreground" />
          </div>
          
          <div className="space-y-3">
            {[
              { server: 'Analytics MCP', satellite: 'MCP Hub Europe', status: 'active', version: 'v2.1.0' },
              { server: 'Reporting MCP', satellite: 'API Gateway East', status: 'active', version: 'v1.8.3' },
              { server: 'ML Pipeline MCP', satellite: 'API Gateway West', status: 'updating', version: 'v3.0.1' },
              { server: 'Data Export MCP', satellite: 'API Gateway Asia', status: 'active', version: 'v1.5.2' }
            ].map((mcp, index) => (
              <div key={index} className="flex items-center justify-between p-3 border border-border rounded-md">
                <div>
                  <p className="font-medium text-foreground">{mcp.server}</p>
                  <p className="text-sm text-muted-foreground">{mcp.satellite} • {mcp.version}</p>
                </div>
                <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                  mcp.status === 'active' 
                    ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
                    : 'bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-400'
                }`}>
                  {mcp.status}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Satellite Info */}
      <div className="bg-muted/50 border border-border rounded-lg p-6">
        <h3 className="text-lg font-semibold text-foreground mb-2">About Satellites</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 text-sm text-muted-foreground">
          <div>
            <h4 className="font-medium text-foreground mb-2">Satellite Capabilities</h4>
            <ul className="space-y-1">
              <li>• Host API endpoints for external access</li>
              <li>• Run Model Context Protocol (MCP) servers</li>
              <li>• Provide computational resources for processing</li>
              <li>• Handle authentication and authorization</li>
              <li>• Route requests to appropriate mesh nodes</li>
            </ul>
          </div>
          <div>
            <h4 className="font-medium text-foreground mb-2">Key Characteristics</h4>
            <ul className="space-y-1">
              <li>• No direct database connections</li>
              <li>• Stateless and horizontally scalable</li>
              <li>• Regional deployment for low latency</li>
              <li>• Automatic failover and load balancing</li>
              <li>• Isolated from data storage layer</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}

export async function generateMetadata({ params }: SatellitesPageProps) {
  const { tenant } = await params;
  
  return {
    title: `Satellites | ${tenant} | reDB`,
    description: `Satellite node management for ${tenant}`,
  };
}
