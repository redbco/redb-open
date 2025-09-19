import { Globe, Plus, MapPin, Server, Database, Activity } from 'lucide-react';

interface RegionsPageProps {
  params: Promise<{
    tenant: string;
  }>;
}

export default async function RegionsPage() {

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Regions</h2>
          <p className="text-muted-foreground mt-2">
            Manage physical locations and data centers where mesh nodes, satellites, anchors, and instances are deployed.
          </p>
        </div>
        <button className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors">
          <Plus className="h-4 w-4 mr-2" />
          Add Region
        </button>
      </div>

      {/* Regions Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        {[
          {
            title: 'Active Regions',
            value: '4',
            change: 'Global coverage',
            icon: Globe,
            color: 'text-blue-600 dark:text-blue-400'
          },
          {
            title: 'Total Nodes',
            value: '20',
            change: 'Across all regions',
            icon: Server,
            color: 'text-green-600 dark:text-green-400'
          },
          {
            title: 'DB Instances',
            value: '32',
            change: 'Regional distribution',
            icon: Database,
            color: 'text-purple-600 dark:text-purple-400'
          },
          {
            title: 'Avg Latency',
            value: '15ms',
            change: 'Inter-region',
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

      {/* Regional Distribution */}
      <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-2 gap-6">
        {[
          {
            id: 'us-east-1',
            name: 'US East (N. Virginia)',
            location: 'Virginia, United States',
            status: 'healthy',
            meshNodes: 4,
            satellites: 2,
            anchors: 3,
            instances: 12,
            latency: '8ms',
            compliance: ['SOC2', 'HIPAA']
          },
          {
            id: 'us-west-2',
            name: 'US West (Oregon)',
            location: 'Oregon, United States',
            status: 'healthy',
            meshNodes: 3,
            satellites: 1,
            anchors: 2,
            instances: 8,
            latency: '12ms',
            compliance: ['SOC2']
          },
          {
            id: 'eu-west-1',
            name: 'Europe (Ireland)',
            location: 'Dublin, Ireland',
            status: 'healthy',
            meshNodes: 3,
            satellites: 1,
            anchors: 2,
            instances: 6,
            latency: '15ms',
            compliance: ['GDPR', 'SOC2']
          },
          {
            id: 'ap-southeast-1',
            name: 'Asia Pacific (Singapore)',
            location: 'Singapore',
            status: 'maintenance',
            meshNodes: 2,
            satellites: 1,
            anchors: 1,
            instances: 4,
            latency: '18ms',
            compliance: ['SOC2']
          }
        ].map((region) => (
          <div key={region.id} className="bg-card border border-border rounded-lg p-6">
            <div className="flex items-start justify-between mb-4">
              <div className="flex items-center space-x-3">
                <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
                  <MapPin className="h-5 w-5 text-primary" />
                </div>
                <div>
                  <h3 className="font-semibold text-foreground">{region.name}</h3>
                  <p className="text-sm text-muted-foreground">{region.location}</p>
                </div>
              </div>
              <div className={`px-2 py-1 rounded-full text-xs font-medium ${
                region.status === 'healthy' 
                  ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
                  : 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400'
              }`}>
                {region.status}
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4 mb-4">
              <div className="text-center p-3 bg-muted/50 rounded-lg">
                <p className="text-lg font-bold text-foreground">{region.meshNodes}</p>
                <p className="text-xs text-muted-foreground">Mesh Nodes</p>
              </div>
              <div className="text-center p-3 bg-muted/50 rounded-lg">
                <p className="text-lg font-bold text-foreground">{region.satellites}</p>
                <p className="text-xs text-muted-foreground">Satellites</p>
              </div>
              <div className="text-center p-3 bg-muted/50 rounded-lg">
                <p className="text-lg font-bold text-foreground">{region.anchors}</p>
                <p className="text-xs text-muted-foreground">Anchors</p>
              </div>
              <div className="text-center p-3 bg-muted/50 rounded-lg">
                <p className="text-lg font-bold text-foreground">{region.instances}</p>
                <p className="text-xs text-muted-foreground">DB Instances</p>
              </div>
            </div>

            <div className="flex items-center justify-between text-sm mb-3">
              <span className="text-muted-foreground">Average Latency</span>
              <span className="font-medium text-foreground">{region.latency}</span>
            </div>

            <div className="mb-4">
              <p className="text-xs text-muted-foreground mb-2">Compliance</p>
              <div className="flex flex-wrap gap-1">
                {region.compliance.map((cert) => (
                  <span key={cert} className="px-2 py-1 bg-muted text-muted-foreground text-xs rounded">
                    {cert}
                  </span>
                ))}
              </div>
            </div>

            <button className="w-full text-sm text-primary hover:text-primary/80 font-medium py-2">
              Manage Region
            </button>
          </div>
        ))}
      </div>

      {/* Regional Performance */}
      <div className="bg-card border border-border rounded-lg">
        <div className="px-6 py-4 border-b border-border">
          <h3 className="text-lg font-semibold text-foreground">Inter-Region Connectivity</h3>
        </div>
        <div className="p-6">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left py-2 text-sm font-medium text-muted-foreground">From / To</th>
                  <th className="text-center py-2 text-sm font-medium text-muted-foreground">US East</th>
                  <th className="text-center py-2 text-sm font-medium text-muted-foreground">US West</th>
                  <th className="text-center py-2 text-sm font-medium text-muted-foreground">EU West</th>
                  <th className="text-center py-2 text-sm font-medium text-muted-foreground">AP Southeast</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {[
                  { region: 'US East', latencies: ['-', '65ms', '89ms', '187ms'] },
                  { region: 'US West', latencies: ['65ms', '-', '142ms', '154ms'] },
                  { region: 'EU West', latencies: ['89ms', '142ms', '-', '178ms'] },
                  { region: 'AP Southeast', latencies: ['187ms', '154ms', '178ms', '-'] }
                ].map((row, index) => (
                  <tr key={index}>
                    <td className="py-2 text-sm font-medium text-foreground">{row.region}</td>
                    {row.latencies.map((latency, i) => (
                      <td key={i} className="text-center py-2 text-sm text-muted-foreground">
                        {latency}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* Region Management Info */}
      <div className="bg-muted/50 border border-border rounded-lg p-6">
        <h3 className="text-lg font-semibold text-foreground mb-2">Regional Deployment Strategy</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 text-sm text-muted-foreground">
          <div>
            <h4 className="font-medium text-foreground mb-2">Data Locality Benefits</h4>
            <ul className="space-y-1">
              <li>• Reduced latency for regional users</li>
              <li>• Compliance with data residency requirements</li>
              <li>• Improved disaster recovery capabilities</li>
              <li>• Better performance for regional workloads</li>
              <li>• Reduced cross-region data transfer costs</li>
            </ul>
          </div>
          <div>
            <h4 className="font-medium text-foreground mb-2">Regional Components</h4>
            <ul className="space-y-1">
              <li>• Mesh nodes for network coordination</li>
              <li>• Satellites for API and MCP hosting</li>
              <li>• Anchors for database access</li>
              <li>• Database instances for data storage</li>
              <li>• Compliance and security controls</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}

export async function generateMetadata({ params }: RegionsPageProps) {
  const { tenant } = await params;
  
  return {
    title: `Regions | ${tenant} | reDB`,
    description: `Regional deployment management for ${tenant}`,
  };
}
