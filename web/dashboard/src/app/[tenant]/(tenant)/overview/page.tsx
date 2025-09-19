import { 
  Network, 
  Database, 
  AlertTriangle, 
  Activity, 
  Zap, 
  Brain, 
  FileText, 
  Webhook,
  AlertCircle,
  Globe,
  Satellite,
  Anchor
} from 'lucide-react';

export default async function TenantOverviewPage() {
  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-3xl font-bold text-foreground">Operations Overview</h2>
        <p className="text-muted-foreground mt-2">
          Real-time monitoring dashboard - what needs attention across your infrastructure.
        </p>
      </div>

      {/* Critical Alerts Bar */}
      <div className="bg-gradient-to-r from-red-50 to-orange-50 dark:from-red-950/20 dark:to-orange-950/20 border-l-4 border-red-500 p-4 rounded-lg">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <AlertTriangle className="h-5 w-5 text-red-600 dark:text-red-400" />
            <div>
              <h3 className="font-medium text-red-800 dark:text-red-200">2 Issues Require Attention</h3>
              <p className="text-sm text-red-700 dark:text-red-300">1 mesh node disconnected, 1 integration failing</p>
            </div>
          </div>
          <button className="text-sm bg-red-100 dark:bg-red-900/30 text-red-800 dark:text-red-200 px-3 py-1 rounded-md hover:bg-red-200 dark:hover:bg-red-900/50">
            View Details
          </button>
        </div>
      </div>

      {/* System Status Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        {[
          {
            title: 'Mesh Health',
            value: '98.5%',
            status: 'healthy',
            detail: '41/42 nodes online',
            icon: Network,
            color: 'green'
          },
          {
            title: 'Database Health',
            value: '96.7%',
            status: 'warning',
            detail: '58/60 databases healthy',
            icon: Database,
            color: 'yellow'
          },
          {
            title: 'Active Jobs',
            value: '12',
            status: 'healthy',
            detail: '11 running, 1 queued',
            icon: Zap,
            color: 'green'
          },
          {
            title: 'Integrations',
            value: '92%',
            status: 'warning',
            detail: '23/25 integrations healthy',
            icon: Activity,
            color: 'yellow'
          }
        ].map((metric, index) => (
          <div key={index} className="bg-card border border-border rounded-lg p-4">
            <div className="flex items-center justify-between mb-2">
              <metric.icon className={`h-5 w-5 ${
                metric.color === 'green' ? 'text-green-600 dark:text-green-400' :
                metric.color === 'yellow' ? 'text-yellow-600 dark:text-yellow-400' :
                'text-red-600 dark:text-red-400'
              }`} />
              <div className={`w-3 h-3 rounded-full ${
                metric.status === 'healthy' ? 'bg-green-500' :
                metric.status === 'warning' ? 'bg-yellow-500' :
                'bg-red-500'
              }`}></div>
            </div>
            <h3 className="font-medium text-foreground text-sm">{metric.title}</h3>
            <p className="text-2xl font-bold text-foreground mt-1">{metric.value}</p>
            <p className="text-xs text-muted-foreground mt-1">{metric.detail}</p>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        
        {/* Mesh Topology & Health */}
        <div className="bg-card border border-border rounded-lg">
          <div className="px-6 py-4 border-b border-border">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold text-foreground">Mesh Topology</h3>
              <span className="text-sm bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400 px-2 py-1 rounded-full">
                98.5% Healthy
              </span>
            </div>
          </div>
          <div className="p-6">
            {/* Simple topology visualization */}
            <div className="relative">
              <div className="flex justify-center mb-6">
                <div className="text-center">
                  <div className="w-16 h-16 bg-blue-100 dark:bg-blue-900/20 rounded-full flex items-center justify-center mx-auto mb-2">
                    <Globe className="h-8 w-8 text-blue-600 dark:text-blue-400" />
                  </div>
                  <p className="text-sm font-medium text-foreground">Global Mesh</p>
                  <p className="text-xs text-muted-foreground">5 regions</p>
                </div>
              </div>
              
              <div className="grid grid-cols-2 gap-4">
                {/* Satellites */}
                <div className="text-center">
                  <div className="w-12 h-12 bg-green-100 dark:bg-green-900/20 rounded-lg flex items-center justify-center mx-auto mb-2">
                    <Satellite className="h-6 w-6 text-green-600 dark:text-green-400" />
                  </div>
                  <p className="text-sm font-medium text-foreground">Satellites</p>
                  <p className="text-xs text-green-600 dark:text-green-400">18/18 online</p>
                </div>
                
                {/* Anchors */}
                <div className="text-center">
                  <div className="w-12 h-12 bg-yellow-100 dark:bg-yellow-900/20 rounded-lg flex items-center justify-center mx-auto mb-2">
                    <Anchor className="h-6 w-6 text-yellow-600 dark:text-yellow-400" />
                  </div>
                  <p className="text-sm font-medium text-foreground">Anchors</p>
                  <p className="text-xs text-yellow-600 dark:text-yellow-400">23/24 online</p>
                </div>
              </div>
              
              {/* Connection lines */}
              <div className="absolute top-20 left-1/2 transform -translate-x-1/2 w-px h-8 bg-border"></div>
              <div className="absolute top-28 left-1/4 right-1/4 h-px bg-border"></div>
            </div>

            {/* Region status */}
            <div className="mt-6 space-y-2">
              <h4 className="font-medium text-foreground text-sm">Regional Status</h4>
              <div className="space-y-1">
                {[
                  { region: 'us-east-1', status: 'healthy', nodes: '8/8' },
                  { region: 'us-west-2', status: 'healthy', nodes: '10/10' },
                  { region: 'eu-west-1', status: 'healthy', nodes: '12/12' },
                  { region: 'ap-south-1', status: 'warning', nodes: '7/8' },
                  { region: 'ap-east-1', status: 'healthy', nodes: '6/6' }
                ].map((region, index) => (
                  <div key={index} className="flex items-center justify-between text-sm">
                    <span className="text-muted-foreground">{region.region}</span>
                    <div className="flex items-center space-x-2">
                      <span className="text-foreground">{region.nodes}</span>
                      <div className={`w-2 h-2 rounded-full ${
                        region.status === 'healthy' ? 'bg-green-500' : 'bg-yellow-500'
                      }`}></div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>

        {/* Active Jobs Status */}
        <div className="bg-card border border-border rounded-lg">
          <div className="px-6 py-4 border-b border-border">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold text-foreground">Active Jobs</h3>
              <span className="text-sm text-muted-foreground">12 total</span>
            </div>
          </div>
          <div className="p-6">
            <div className="space-y-3">
              {[
                {
                  name: 'Customer DB Migration',
                  type: 'migration',
                  workspace: 'Production',
                  status: 'running',
                  progress: 78,
                  eta: '2h 15m'
                },
                {
                  name: 'Document Embeddings',
                  type: 'rag',
                  workspace: 'Analytics',
                  status: 'running',
                  progress: 34,
                  eta: '45m'
                },
                {
                  name: 'Schema Sync - Staging',
                  type: 'replication',
                  workspace: 'Staging',
                  status: 'running',
                  progress: 92,
                  eta: '8m'
                },
                {
                  name: 'Data Quality Check',
                  type: 'analysis',
                  workspace: 'Development',
                  status: 'queued',
                  progress: 0,
                  eta: 'Pending'
                },
                {
                  name: 'Backup Validation',
                  type: 'backup',
                  workspace: 'Production',
                  status: 'running',
                  progress: 12,
                  eta: '3h 20m'
                }
              ].map((job, index) => (
                <div key={index} className="border border-border rounded-lg p-3">
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center space-x-2">
                      <div className={`w-2 h-2 rounded-full ${
                        job.status === 'running' ? 'bg-green-500 animate-pulse' :
                        job.status === 'queued' ? 'bg-yellow-500' :
                        'bg-gray-500'
                      }`}></div>
                      <span className="font-medium text-foreground text-sm">{job.name}</span>
                    </div>
                    <span className={`text-xs px-2 py-1 rounded-full ${
                      job.status === 'running' ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400' :
                      'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400'
                    }`}>
                      {job.status}
                    </span>
                  </div>
                  
                  <div className="flex items-center justify-between text-xs text-muted-foreground mb-2">
                    <span>{job.workspace} • {job.type}</span>
                    <span>ETA: {job.eta}</span>
                  </div>
                  
                  {job.status === 'running' && (
                    <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-1.5">
                      <div 
                        className="bg-green-500 h-1.5 rounded-full transition-all duration-300"
                        style={{ width: `${job.progress}%` }}
                      ></div>
                    </div>
                  )}
                </div>
              ))}
            </div>
            
            <div className="mt-4 pt-3 border-t border-border">
              <div className="flex justify-between text-sm">
                <span className="text-muted-foreground">Job Distribution</span>
                <div className="flex space-x-4">
                  <span className="text-green-600 dark:text-green-400">11 Running</span>
                  <span className="text-yellow-600 dark:text-yellow-400">1 Queued</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Database Instances & Health */}
      <div className="bg-card border border-border rounded-lg">
        <div className="px-6 py-4 border-b border-border">
          <div className="flex items-center justify-between">
            <h3 className="text-lg font-semibold text-foreground">Database Instances & Health</h3>
            <span className="text-sm bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400 px-2 py-1 rounded-full">
              2 Issues
            </span>
          </div>
        </div>
        <div className="p-6">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {[
              {
                workspace: 'Production',
                instances: 8,
                databases: 24,
                health: 'healthy',
                issues: 0,
                color: 'green'
              },
              {
                workspace: 'Staging',
                instances: 4,
                databases: 18,
                health: 'warning',
                issues: 1,
                color: 'yellow'
              },
              {
                workspace: 'Development',
                instances: 3,
                databases: 12,
                health: 'healthy',
                issues: 0,
                color: 'green'
              },
              {
                workspace: 'Analytics',
                instances: 2,
                databases: 6,
                health: 'warning',
                issues: 1,
                color: 'yellow'
              }
            ].map((workspace, index) => (
              <div key={index} className={`border-2 rounded-lg p-4 ${
                workspace.color === 'green' ? 'border-green-200 dark:border-green-800' :
                'border-yellow-200 dark:border-yellow-800'
              }`}>
                <div className="flex items-center justify-between mb-3">
                  <h4 className="font-medium text-foreground">{workspace.workspace}</h4>
                  <div className={`w-3 h-3 rounded-full ${
                    workspace.color === 'green' ? 'bg-green-500' : 'bg-yellow-500'
                  }`}></div>
                </div>
                
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Instances</span>
                    <span className="font-medium text-foreground">{workspace.instances}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Databases</span>
                    <span className="font-medium text-foreground">{workspace.databases}</span>
                  </div>
                  {workspace.issues > 0 && (
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Issues</span>
                      <span className="font-medium text-yellow-600 dark:text-yellow-400">{workspace.issues}</span>
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>

          {/* Critical Issues */}
          <div className="mt-6">
            <h4 className="font-medium text-foreground mb-3">Critical Issues</h4>
            <div className="space-y-2">
              {[
                {
                  instance: 'staging-db-2',
                  issue: 'High memory usage (89%)',
                  workspace: 'Staging',
                  severity: 'warning'
                },
                {
                  instance: 'analytics-db-1',
                  issue: 'Connection timeout spike',
                  workspace: 'Analytics',
                  severity: 'warning'
                }
              ].map((issue, index) => (
                <div key={index} className="flex items-center justify-between p-3 border border-yellow-200 dark:border-yellow-800 rounded-md bg-yellow-50 dark:bg-yellow-950/20">
                  <div className="flex items-center space-x-3">
                    <AlertCircle className="h-4 w-4 text-yellow-600 dark:text-yellow-400" />
                    <div>
                      <p className="font-medium text-foreground text-sm">{issue.instance}</p>
                      <p className="text-xs text-muted-foreground">{issue.workspace} • {issue.issue}</p>
                    </div>
                  </div>
                  <button className="text-xs bg-yellow-100 dark:bg-yellow-900/30 text-yellow-800 dark:text-yellow-200 px-2 py-1 rounded">
                    Investigate
                  </button>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* Data Relationships Health */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        
        <div className="bg-card border border-border rounded-lg">
          <div className="px-6 py-4 border-b border-border">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold text-foreground">Data Relationships</h3>
              <span className="text-sm text-muted-foreground">15 active</span>
            </div>
          </div>
          <div className="p-6">
            <div className="space-y-3">
              {[
                {
                  name: 'Prod → Staging Sync',
                  type: 'replication',
                  status: 'healthy',
                  latency: '2.3s',
                  throughput: '1.2k rows/min'
                },
                {
                  name: 'Customer Data Migration',
                  type: 'migration',
                  status: 'running',
                  latency: '5.1s',
                  throughput: '850 rows/min'
                },
                {
                  name: 'Analytics ETL Pipeline',
                  type: 'etl',
                  status: 'healthy',
                  latency: '1.8s',
                  throughput: '2.1k rows/min'
                },
                {
                  name: 'Backup Replication',
                  type: 'backup',
                  status: 'warning',
                  latency: '12.4s',
                  throughput: '340 rows/min'
                }
              ].map((relationship, index) => (
                <div key={index} className="flex items-center justify-between p-3 border border-border rounded-md">
                  <div className="flex items-center space-x-3">
                    <div className={`w-3 h-3 rounded-full ${
                      relationship.status === 'healthy' ? 'bg-green-500' :
                      relationship.status === 'running' ? 'bg-blue-500 animate-pulse' :
                      'bg-yellow-500'
                    }`}></div>
                    <div>
                      <p className="font-medium text-foreground text-sm">{relationship.name}</p>
                      <p className="text-xs text-muted-foreground">{relationship.type} • {relationship.latency} avg</p>
                    </div>
                  </div>
                  <div className="text-right">
                    <p className="text-sm font-medium text-foreground">{relationship.throughput}</p>
                    <p className="text-xs text-muted-foreground">throughput</p>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Integration Health */}
        <div className="bg-card border border-border rounded-lg">
          <div className="px-6 py-4 border-b border-border">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold text-foreground">Integration Health</h3>
              <span className="text-sm bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400 px-2 py-1 rounded-full">
                1 Failing
              </span>
            </div>
          </div>
          <div className="p-6">
            <div className="space-y-3">
              
              {/* RAG Integrations */}
              <div>
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center space-x-2">
                    <FileText className="h-4 w-4 text-blue-600 dark:text-blue-400" />
                    <span className="font-medium text-foreground text-sm">RAG Integrations</span>
                  </div>
                  <span className="text-xs text-green-600 dark:text-green-400">5/5 healthy</span>
                </div>
                <div className="ml-6 text-xs text-muted-foreground">
                  OpenAI, Pinecone, Weaviate, ChromaDB, Elasticsearch
                </div>
              </div>

              {/* LLM Integrations */}
              <div>
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center space-x-2">
                    <Brain className="h-4 w-4 text-green-600 dark:text-green-400" />
                    <span className="font-medium text-foreground text-sm">LLM Integrations</span>
                  </div>
                  <span className="text-xs text-yellow-600 dark:text-yellow-400">7/8 healthy</span>
                </div>
                <div className="ml-6 text-xs text-muted-foreground">
                  GPT-4, Claude, Llama-2, Gemini • <span className="text-yellow-600 dark:text-yellow-400">Gemini timeout</span>
                </div>
              </div>

              {/* Webhooks */}
              <div>
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center space-x-2">
                    <Webhook className="h-4 w-4 text-purple-600 dark:text-purple-400" />
                    <span className="font-medium text-foreground text-sm">Webhooks</span>
                  </div>
                  <span className="text-xs text-green-600 dark:text-green-400">12/12 healthy</span>
                </div>
                <div className="ml-6 text-xs text-muted-foreground">
                  Slack, Discord, Datadog, PagerDuty, Teams
                </div>
              </div>

              {/* Recent Issues */}
              <div className="mt-4 pt-3 border-t border-border">
                <h4 className="font-medium text-foreground text-sm mb-2">Recent Issues</h4>
                <div className="p-2 border border-yellow-200 dark:border-yellow-800 rounded-md bg-yellow-50 dark:bg-yellow-950/20">
                  <div className="flex items-center space-x-2">
                    <AlertCircle className="h-3 w-3 text-yellow-600 dark:text-yellow-400" />
                    <span className="text-xs font-medium text-foreground">Gemini API timeout</span>
                  </div>
                  <p className="text-xs text-muted-foreground mt-1 ml-5">
                    Rate limit exceeded • Started 23m ago
                  </p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Quick Actions */}
      <div className="bg-card border border-border rounded-lg">
        <div className="px-6 py-4 border-b border-border">
          <h3 className="text-lg font-semibold text-foreground">Quick Actions</h3>
        </div>
        <div className="p-6">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <button className="flex items-center justify-center p-4 border border-border rounded-lg hover:bg-muted transition-colors">
              <div className="text-center">
                <AlertTriangle className="h-6 w-6 text-yellow-600 dark:text-yellow-400 mx-auto mb-2" />
                <p className="font-medium text-foreground text-sm">Investigate Issues</p>
                <p className="text-xs text-muted-foreground">Review all alerts</p>
              </div>
            </button>
            
            <button className="flex items-center justify-center p-4 border border-border rounded-lg hover:bg-muted transition-colors">
              <div className="text-center">
                <Network className="h-6 w-6 text-blue-600 dark:text-blue-400 mx-auto mb-2" />
                <p className="font-medium text-foreground text-sm">Mesh Diagnostics</p>
                <p className="text-xs text-muted-foreground">Run network tests</p>
              </div>
            </button>
            
            <button className="flex items-center justify-center p-4 border border-border rounded-lg hover:bg-muted transition-colors">
              <div className="text-center">
                <Activity className="h-6 w-6 text-green-600 dark:text-green-400 mx-auto mb-2" />
                <p className="font-medium text-foreground text-sm">System Health Check</p>
                <p className="text-xs text-muted-foreground">Full system scan</p>
              </div>
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

export async function generateMetadata() {
  return {
    title: 'Operations Overview | reDB',
    description: 'Real-time monitoring and operations dashboard',
  };
}