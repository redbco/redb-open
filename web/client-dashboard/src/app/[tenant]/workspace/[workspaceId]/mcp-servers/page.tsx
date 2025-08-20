import { Server, Plus, Activity, Code, Globe, Settings, Play } from 'lucide-react';

interface McpServersPageProps {
  params: Promise<{
    tenant: string;
  }>;
}

export default async function McpServersPage() {
  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">MCP Servers</h2>
          <p className="text-muted-foreground mt-2">
            Manage Model Context Protocol servers that provide access to resources, tools, and prompts.
          </p>
        </div>
        <button className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors">
          <Plus className="h-4 w-4 mr-2" />
          Deploy MCP Server
        </button>
      </div>

      {/* MCP Servers Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        {[
          {
            title: 'Active Servers',
            value: '8',
            change: '+2 this week',
            icon: Server,
            color: 'text-blue-600 dark:text-blue-400'
          },
          {
            title: 'Total Resources',
            value: '24',
            change: 'Exposed resources',
            icon: Code,
            color: 'text-green-600 dark:text-green-400'
          },
          {
            title: 'API Calls',
            value: '45K',
            change: 'This month',
            icon: Activity,
            color: 'text-purple-600 dark:text-purple-400'
          },
          {
            title: 'Regions',
            value: '4',
            change: 'Global deployment',
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

      {/* MCP Server Instances */}
      <div className="bg-card border border-border rounded-lg">
        <div className="px-6 py-4 border-b border-border">
          <h3 className="text-lg font-semibold text-foreground">MCP Server Instances</h3>
        </div>
        <div className="divide-y divide-border">
          {[
            {
              id: 'mcp-analytics-001',
              name: 'Analytics MCP Server',
              version: 'v2.1.0',
              status: 'running',
              satellite: 'MCP Hub Europe',
              region: 'eu-west-1',
              resources: 8,
              tools: 4,
              prompts: 2,
              uptime: '99.9%',
              requests: '12K/day'
            },
            {
              id: 'mcp-reporting-001',
              name: 'Reporting MCP Server',
              version: 'v1.8.3',
              status: 'running',
              satellite: 'API Gateway East',
              region: 'us-east-1',
              resources: 6,
              tools: 3,
              prompts: 4,
              uptime: '99.8%',
              requests: '8.5K/day'
            },
            {
              id: 'mcp-ml-pipeline-001',
              name: 'ML Pipeline MCP Server',
              version: 'v3.0.1',
              status: 'updating',
              satellite: 'API Gateway West',
              region: 'us-west-2',
              resources: 4,
              tools: 6,
              prompts: 1,
              uptime: '98.5%',
              requests: '15K/day'
            },
            {
              id: 'mcp-data-export-001',
              name: 'Data Export MCP Server',
              version: 'v1.5.2',
              status: 'running',
              satellite: 'API Gateway Asia',
              region: 'ap-southeast-1',
              resources: 3,
              tools: 2,
              prompts: 3,
              uptime: '99.2%',
              requests: '5.2K/day'
            }
          ].map((server) => (
            <div key={server.id} className="px-6 py-4">
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center space-x-3">
                  <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
                    <Server className="h-5 w-5 text-primary" />
                  </div>
                  <div>
                    <h4 className="font-medium text-foreground">{server.name}</h4>
                    <p className="text-sm text-muted-foreground">{server.id} • {server.version}</p>
                  </div>
                </div>
                <div className="flex items-center space-x-2">
                  <div className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${
                    server.status === 'running' 
                      ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
                      : 'bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-400'
                  }`}>
                    {server.status === 'running' ? (
                      <>
                        <Play className="h-3 w-3 mr-1" />
                        Running
                      </>
                    ) : (
                      <>
                        <Activity className="h-3 w-3 mr-1" />
                        Updating
                      </>
                    )}
                  </div>
                  <button className="p-1 rounded-md hover:bg-accent hover:text-accent-foreground">
                    <Settings className="h-4 w-4" />
                  </button>
                </div>
              </div>

              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm mb-3">
                <div>
                  <p className="text-muted-foreground">Satellite</p>
                  <p className="font-medium text-foreground">{server.satellite}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Region</p>
                  <p className="font-medium text-foreground">{server.region}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Uptime</p>
                  <p className="font-medium text-foreground">{server.uptime}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Requests</p>
                  <p className="font-medium text-foreground">{server.requests}</p>
                </div>
              </div>

              <div className="grid grid-cols-3 gap-4 text-sm">
                <div className="text-center p-2 bg-muted/50 rounded">
                  <p className="font-bold text-foreground">{server.resources}</p>
                  <p className="text-xs text-muted-foreground">Resources</p>
                </div>
                <div className="text-center p-2 bg-muted/50 rounded">
                  <p className="font-bold text-foreground">{server.tools}</p>
                  <p className="text-xs text-muted-foreground">Tools</p>
                </div>
                <div className="text-center p-2 bg-muted/50 rounded">
                  <p className="font-bold text-foreground">{server.prompts}</p>
                  <p className="text-xs text-muted-foreground">Prompts</p>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* MCP Resources & Tools */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">MCP Resources</h3>
            <Code className="h-5 w-5 text-muted-foreground" />
          </div>
          
          <div className="space-y-3">
            {[
              { name: 'database://users', server: 'Analytics MCP', type: 'Database', access: 'read-only' },
              { name: 'file://reports/*', server: 'Reporting MCP', type: 'File System', access: 'read-write' },
              { name: 'api://analytics/metrics', server: 'Analytics MCP', type: 'API Endpoint', access: 'read-only' },
              { name: 'queue://ml-jobs', server: 'ML Pipeline MCP', type: 'Message Queue', access: 'read-write' }
            ].map((resource, index) => (
              <div key={index} className="flex items-center justify-between p-3 border border-border rounded-md">
                <div>
                  <p className="font-medium text-foreground text-sm">{resource.name}</p>
                  <p className="text-xs text-muted-foreground">{resource.server} • {resource.type}</p>
                </div>
                <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                  resource.access === 'read-only' 
                    ? 'bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-400'
                    : 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
                }`}>
                  {resource.access}
                </span>
              </div>
            ))}
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">MCP Tools</h3>
            <Activity className="h-5 w-5 text-muted-foreground" />
          </div>
          
          <div className="space-y-3">
            {[
              { name: 'generate_report', server: 'Reporting MCP', description: 'Generate custom reports', usage: '2.1K calls' },
              { name: 'query_analytics', server: 'Analytics MCP', description: 'Query analytics data', usage: '5.8K calls' },
              { name: 'train_model', server: 'ML Pipeline MCP', description: 'Train ML models', usage: '340 calls' },
              { name: 'export_data', server: 'Data Export MCP', description: 'Export data in various formats', usage: '1.2K calls' }
            ].map((tool, index) => (
              <div key={index} className="p-3 border border-border rounded-md">
                <div className="flex items-center justify-between mb-1">
                  <p className="font-medium text-foreground text-sm">{tool.name}</p>
                  <span className="text-xs text-muted-foreground">{tool.usage}</span>
                </div>
                <p className="text-xs text-muted-foreground mb-1">{tool.description}</p>
                <p className="text-xs text-muted-foreground">{tool.server}</p>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* MCP Server Templates */}
      <div className="bg-card border border-border rounded-lg p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-foreground">Available MCP Server Templates</h3>
          <button className="text-sm text-primary hover:text-primary/80 font-medium">
            Browse Templates
          </button>
        </div>
        
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {[
            {
              name: 'Database Connector',
              description: 'Connect to databases and expose tables as resources',
              resources: 'Tables, Views, Procedures',
              tools: 'Query, Insert, Update',
              category: 'Data Access'
            },
            {
              name: 'File System',
              description: 'Access file systems and cloud storage',
              resources: 'Files, Directories',
              tools: 'Read, Write, List',
              category: 'Storage'
            },
            {
              name: 'API Gateway',
              description: 'Expose REST APIs as MCP resources',
              resources: 'Endpoints, Schemas',
              tools: 'GET, POST, PUT, DELETE',
              category: 'Integration'
            }
          ].map((template, index) => (
            <div key={index} className="p-4 border border-border rounded-lg hover:bg-accent/50 transition-colors cursor-pointer">
              <div className="flex items-center justify-between mb-2">
                <h4 className="font-medium text-foreground">{template.name}</h4>
                <span className="px-2 py-1 bg-muted text-muted-foreground text-xs rounded">
                  {template.category}
                </span>
              </div>
              <p className="text-sm text-muted-foreground mb-3">{template.description}</p>
              <div className="space-y-1 text-xs">
                <p><span className="text-muted-foreground">Resources:</span> {template.resources}</p>
                <p><span className="text-muted-foreground">Tools:</span> {template.tools}</p>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* MCP Info */}
      <div className="bg-muted/50 border border-border rounded-lg p-6">
        <h3 className="text-lg font-semibold text-foreground mb-2">Model Context Protocol (MCP)</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 text-sm text-muted-foreground">
          <div>
            <h4 className="font-medium text-foreground mb-2">MCP Server Capabilities</h4>
            <ul className="space-y-1">
              <li>• Expose resources (databases, files, APIs)</li>
              <li>• Provide tools for data manipulation</li>
              <li>• Offer prompt templates for AI models</li>
              <li>• Handle authentication and authorization</li>
              <li>• Support real-time data access</li>
            </ul>
          </div>
          <div>
            <h4 className="font-medium text-foreground mb-2">Deployment & Management</h4>
            <ul className="space-y-1">
              <li>• Deployed on satellite nodes</li>
              <li>• Horizontally scalable instances</li>
              <li>• Version control and rollback support</li>
              <li>• Monitoring and logging integration</li>
              <li>• Regional deployment for low latency</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}

export async function generateMetadata({ params }: McpServersPageProps) {
  const { tenant } = await params;
  
  return {
    title: `MCP Servers | ${tenant} | reDB`,
    description: `MCP server management for ${tenant}`,
  };
}
