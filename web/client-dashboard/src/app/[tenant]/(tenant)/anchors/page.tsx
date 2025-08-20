import { Anchor, Plus, Database, Activity, Globe, Settings } from 'lucide-react';

interface AnchorsPageProps {
  params: Promise<{
    tenant: string;
  }>;
}

export default async function AnchorsPage({ params }: AnchorsPageProps) {
  const { tenant } = await params;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Anchors</h2>
          <p className="text-muted-foreground mt-2">
            Manage anchor nodes that provide database-only access. Anchors do not host API endpoints or MCP servers.
          </p>
        </div>
        <button className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors">
          <Plus className="h-4 w-4 mr-2" />
          Deploy Anchor
        </button>
      </div>

      {/* Anchors Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        {[
          {
            title: 'Active Anchors',
            value: '8',
            change: '+2 this month',
            icon: Anchor,
            color: 'text-blue-600 dark:text-blue-400'
          },
          {
            title: 'DB Connections',
            value: '24',
            change: '3 per anchor',
            icon: Database,
            color: 'text-green-600 dark:text-green-400'
          },
          {
            title: 'Active Queries',
            value: '156',
            change: 'Per minute',
            icon: Activity,
            color: 'text-purple-600 dark:text-purple-400'
          },
          {
            title: 'Regions',
            value: '4',
            change: 'Data locality',
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

      {/* Anchor Nodes */}
      <div className="bg-card border border-border rounded-lg">
        <div className="px-6 py-4 border-b border-border">
          <h3 className="text-lg font-semibold text-foreground">Anchor Nodes</h3>
        </div>
        <div className="divide-y divide-border">
          {[
            {
              id: 'anc-db-001',
              name: 'Database Anchor East',
              region: 'us-east-1',
              status: 'healthy',
              connections: 8,
              databases: 12,
              uptime: '99.9%',
              lastSeen: '1 minute ago'
            },
            {
              id: 'anc-db-002',
              name: 'Database Anchor West',
              region: 'us-west-2',
              status: 'healthy',
              connections: 6,
              databases: 9,
              uptime: '99.8%',
              lastSeen: '2 minutes ago'
            },
            {
              id: 'anc-db-003',
              name: 'Database Anchor Europe',
              region: 'eu-west-1',
              status: 'healthy',
              connections: 4,
              databases: 6,
              uptime: '99.7%',
              lastSeen: '30 seconds ago'
            },
            {
              id: 'anc-db-004',
              name: 'Database Anchor Asia',
              region: 'ap-southeast-1',
              status: 'maintenance',
              connections: 2,
              databases: 3,
              uptime: '98.2%',
              lastSeen: '10 minutes ago'
            }
          ].map((anchor) => (
            <div key={anchor.id} className="px-6 py-4">
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center space-x-3">
                  <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
                    <Anchor className="h-5 w-5 text-primary" />
                  </div>
                  <div>
                    <h4 className="font-medium text-foreground">{anchor.name}</h4>
                    <p className="text-sm text-muted-foreground">{anchor.id} • {anchor.region}</p>
                  </div>
                </div>
                <div className="flex items-center space-x-2">
                  <div className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${
                    anchor.status === 'healthy' 
                      ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
                      : 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400'
                  }`}>
                    {anchor.status}
                  </div>
                  <button className="p-1 rounded-md hover:bg-accent hover:text-accent-foreground">
                    <Settings className="h-4 w-4" />
                  </button>
                </div>
              </div>

              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                <div>
                  <p className="text-muted-foreground">DB Connections</p>
                  <p className="font-medium text-foreground">{anchor.connections}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Databases</p>
                  <p className="font-medium text-foreground">{anchor.databases}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Uptime</p>
                  <p className="font-medium text-foreground">{anchor.uptime}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Last Seen</p>
                  <p className="font-medium text-foreground">{anchor.lastSeen}</p>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Database Connections */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">Database Connections</h3>
            <Database className="h-5 w-5 text-muted-foreground" />
          </div>
          
          <div className="space-y-3">
            {[
              { database: 'users_prod', anchor: 'Database Anchor East', connections: 12, status: 'active' },
              { database: 'orders_prod', anchor: 'Database Anchor East', connections: 8, status: 'active' },
              { database: 'analytics_prod', anchor: 'Database Anchor West', connections: 6, status: 'active' },
              { database: 'logs_prod', anchor: 'Database Anchor Europe', connections: 4, status: 'readonly' }
            ].map((db, index) => (
              <div key={index} className="flex items-center justify-between p-3 border border-border rounded-md">
                <div>
                  <p className="font-medium text-foreground">{db.database}</p>
                  <p className="text-sm text-muted-foreground">{db.anchor}</p>
                </div>
                <div className="text-right">
                  <p className="text-sm font-medium text-foreground">{db.connections} conn</p>
                  <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${
                    db.status === 'active' 
                      ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
                      : 'bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-400'
                  }`}>
                    {db.status}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">Query Performance</h3>
            <Activity className="h-5 w-5 text-muted-foreground" />
          </div>
          
          <div className="space-y-3">
            {[
              { anchor: 'Database Anchor East', queries: '45/min', avgLatency: '12ms', p99: '45ms' },
              { anchor: 'Database Anchor West', queries: '32/min', avgLatency: '18ms', p99: '52ms' },
              { anchor: 'Database Anchor Europe', queries: '28/min', avgLatency: '15ms', p99: '48ms' },
              { anchor: 'Database Anchor Asia', queries: '18/min', avgLatency: '22ms', p99: '68ms' }
            ].map((perf, index) => (
              <div key={index} className="p-3 border border-border rounded-md">
                <div className="flex items-center justify-between mb-2">
                  <p className="font-medium text-foreground">{perf.anchor}</p>
                  <span className="text-sm text-muted-foreground">{perf.queries}</span>
                </div>
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div>
                    <p className="text-muted-foreground">Avg Latency</p>
                    <p className="font-medium text-foreground">{perf.avgLatency}</p>
                  </div>
                  <div>
                    <p className="text-muted-foreground">P99 Latency</p>
                    <p className="font-medium text-foreground">{perf.p99}</p>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Anchor Info */}
      <div className="bg-muted/50 border border-border rounded-lg p-6">
        <h3 className="text-lg font-semibold text-foreground mb-2">About Anchors</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 text-sm text-muted-foreground">
          <div>
            <h4 className="font-medium text-foreground mb-2">Anchor Capabilities</h4>
            <ul className="space-y-1">
              <li>• Direct database connections and access</li>
              <li>• Query execution and result processing</li>
              <li>• Database connection pooling and management</li>
              <li>• Data caching and query optimization</li>
              <li>• Regional data access for compliance</li>
            </ul>
          </div>
          <div>
            <h4 className="font-medium text-foreground mb-2">Key Characteristics</h4>
            <ul className="space-y-1">
              <li>• No API endpoints or external access</li>
              <li>• No MCP server hosting capabilities</li>
              <li>• Optimized for database performance</li>
              <li>• Regional deployment for data locality</li>
              <li>• Secure database-only access layer</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}

export async function generateMetadata({ params }: AnchorsPageProps) {
  const { tenant } = await params;
  
  return {
    title: `Anchors | ${tenant} | reDB`,
    description: `Anchor node management for ${tenant}`,
  };
}
