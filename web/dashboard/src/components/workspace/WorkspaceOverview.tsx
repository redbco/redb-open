'use client';

import { 
  Database, 
  AlertTriangle, 
  Zap, 
  ArrowRightLeft, 
  GitBranch,
  AlertCircle,
  CheckCircle,
  Settings,
  TrendingUp,
  TrendingDown
} from 'lucide-react';

interface WorkspaceOverviewProps {
  tenantId: string;
  workspaceId: string;
}

export function WorkspaceOverview({ workspaceId }: WorkspaceOverviewProps) {
  // Determine workspace type for different mock data
  const isProduction = workspaceId.toLowerCase().includes('prod');
  const isStaging = workspaceId.toLowerCase().includes('staging');
  const isDevelopment = workspaceId.toLowerCase().includes('dev');

  return (
    <div className="space-y-6">
      {/* Workspace Health Alert */}
      {(isStaging || isDevelopment) && (
        <div className={`${
          isStaging ? 'bg-gradient-to-r from-yellow-50 to-orange-50 dark:from-yellow-950/20 dark:to-orange-950/20 border-l-4 border-yellow-500' :
          'bg-gradient-to-r from-blue-50 to-green-50 dark:from-blue-950/20 dark:to-green-950/20 border-l-4 border-blue-500'
        } p-4 rounded-lg`}>
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              {isStaging ? (
                <AlertTriangle className="h-5 w-5 text-yellow-600 dark:text-yellow-400" />
              ) : (
                <CheckCircle className="h-5 w-5 text-blue-600 dark:text-blue-400" />
              )}
              <div>
                <h3 className={`font-medium ${
                  isStaging ? 'text-yellow-800 dark:text-yellow-200' : 'text-blue-800 dark:text-blue-200'
                }`}>
                  {isStaging ? '1 Issue in Staging Environment' : 'Development Environment Active'}
                </h3>
                <p className={`text-sm ${
                  isStaging ? 'text-yellow-700 dark:text-yellow-300' : 'text-blue-700 dark:text-blue-300'
                }`}>
                  {isStaging ? 'Database memory usage spike detected' : 'All systems operational, 2 active development tasks'}
                </p>
              </div>
            </div>
            <button className={`text-sm px-3 py-1 rounded-md hover:opacity-80 ${
              isStaging ? 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-800 dark:text-yellow-200' :
              'bg-blue-100 dark:bg-blue-900/30 text-blue-800 dark:text-blue-200'
            }`}>
              {isStaging ? 'Investigate' : 'View Tasks'}
            </button>
          </div>
        </div>
      )}

      {/* Workspace Status Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        {[
          {
            title: 'Database Instances',
            value: isProduction ? '8' : isStaging ? '4' : '3',
            status: isStaging ? 'warning' : 'healthy',
            detail: isProduction ? '8 instances, all healthy' : isStaging ? '4 instances, 1 warning' : '3 instances, all healthy',
            icon: Database,
            color: isStaging ? 'yellow' : 'green'
          },
          {
            title: 'Active Jobs',
            value: isProduction ? '5' : isStaging ? '3' : '2',
            status: 'healthy',
            detail: isProduction ? '3 migrations, 2 backups' : isStaging ? '2 sync jobs, 1 test' : '1 migration, 1 analysis',
            icon: Zap,
            color: 'green'
          },
          {
            title: 'Schema Repositories',
            value: isProduction ? '6' : isStaging ? '4' : '3',
            status: 'healthy',
            detail: isProduction ? '6 repos, 12 branches' : isStaging ? '4 repos, 8 branches' : '3 repos, 6 branches',
            icon: GitBranch,
            color: 'green'
          },
          {
            title: 'Data Relationships',
            value: isProduction ? '12' : isStaging ? '6' : '4',
            status: isProduction ? 'healthy' : 'warning',
            detail: isProduction ? '12 active replications' : isStaging ? '6 active, 1 slow' : '4 active relationships',
            icon: ArrowRightLeft,
            color: isProduction ? 'green' : 'yellow'
          }
        ].map((metric, index) => (
          <div key={index} className="bg-card border border-border rounded-lg p-4">
            <div className="flex items-center justify-between mb-2">
              <metric.icon className={`h-5 w-5 ${
                metric.color === 'green' ? 'text-green-600 dark:text-green-400' :
                'text-yellow-600 dark:text-yellow-400'
              }`} />
              <div className={`w-3 h-3 rounded-full ${
                metric.status === 'healthy' ? 'bg-green-500' : 'bg-yellow-500'
              }`}></div>
            </div>
            <h3 className="font-medium text-foreground text-sm">{metric.title}</h3>
            <p className="text-2xl font-bold text-foreground mt-1">{metric.value}</p>
            <p className="text-xs text-muted-foreground mt-1">{metric.detail}</p>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        
        {/* Database Instances Health */}
        <div className="bg-card border border-border rounded-lg">
          <div className="px-6 py-4 border-b border-border">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold text-foreground">Database Instances</h3>
              <span className={`text-sm px-2 py-1 rounded-full ${
                isStaging ? 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400' :
                'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
              }`}>
                {isStaging ? '1 Warning' : 'All Healthy'}
              </span>
            </div>
          </div>
          <div className="p-6">
            <div className="space-y-3">
              {(isProduction ? [
                { name: 'prod-primary-db', type: 'PostgreSQL', status: 'healthy', cpu: '45%', memory: '62%', connections: '234/500' },
                { name: 'prod-replica-db', type: 'PostgreSQL', status: 'healthy', cpu: '38%', memory: '58%', connections: '156/500' },
                { name: 'prod-analytics-db', type: 'ClickHouse', status: 'healthy', cpu: '52%', memory: '71%', connections: '89/200' },
                { name: 'prod-cache-redis', type: 'Redis', status: 'healthy', cpu: '23%', memory: '34%', connections: '445/1000' }
              ] : isStaging ? [
                { name: 'staging-primary-db', type: 'PostgreSQL', status: 'warning', cpu: '72%', memory: '89%', connections: '89/200' },
                { name: 'staging-replica-db', type: 'PostgreSQL', status: 'healthy', cpu: '34%', memory: '45%', connections: '45/200' },
                { name: 'staging-test-db', type: 'PostgreSQL', status: 'healthy', cpu: '28%', memory: '38%', connections: '23/200' },
                { name: 'staging-cache-redis', type: 'Redis', status: 'healthy', cpu: '15%', memory: '22%', connections: '67/500' }
              ] : [
                { name: 'dev-main-db', type: 'PostgreSQL', status: 'healthy', cpu: '25%', memory: '35%', connections: '12/100' },
                { name: 'dev-test-db', type: 'PostgreSQL', status: 'healthy', cpu: '18%', memory: '28%', connections: '8/100' },
                { name: 'dev-cache-redis', type: 'Redis', status: 'healthy', cpu: '8%', memory: '15%', connections: '23/200' }
              ]).map((instance, index) => (
                <div key={index} className={`p-3 border rounded-lg ${
                  instance.status === 'warning' ? 'border-yellow-200 dark:border-yellow-800 bg-yellow-50 dark:bg-yellow-950/20' :
                  'border-border'
                }`}>
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center space-x-2">
                      <div className={`w-2 h-2 rounded-full ${
                        instance.status === 'healthy' ? 'bg-green-500' : 'bg-yellow-500'
                      }`}></div>
                      <span className="font-medium text-foreground text-sm">{instance.name}</span>
                      <span className="text-xs text-muted-foreground">{instance.type}</span>
                    </div>
                    {instance.status === 'warning' && (
                      <AlertCircle className="h-4 w-4 text-yellow-600 dark:text-yellow-400" />
                    )}
                  </div>
                  <div className="grid grid-cols-3 gap-2 text-xs">
                    <div>
                      <span className="text-muted-foreground">CPU: </span>
                      <span className={`font-medium ${
                        parseInt(instance.cpu) > 70 ? 'text-yellow-600 dark:text-yellow-400' : 'text-foreground'
                      }`}>{instance.cpu}</span>
                    </div>
                    <div>
                      <span className="text-muted-foreground">Memory: </span>
                      <span className={`font-medium ${
                        parseInt(instance.memory) > 80 ? 'text-yellow-600 dark:text-yellow-400' : 'text-foreground'
                      }`}>{instance.memory}</span>
                    </div>
                    <div>
                      <span className="text-muted-foreground">Conn: </span>
                      <span className="font-medium text-foreground">{instance.connections}</span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Active Jobs & Tasks */}
        <div className="bg-card border border-border rounded-lg">
          <div className="px-6 py-4 border-b border-border">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold text-foreground">Active Jobs & Tasks</h3>
              <span className="text-sm text-muted-foreground">
                {isProduction ? '5 running' : isStaging ? '3 running' : '2 running'}
              </span>
            </div>
          </div>
          <div className="p-6">
            <div className="space-y-3">
              {(isProduction ? [
                { name: 'Daily Customer Backup', type: 'backup', status: 'running', progress: 67, eta: '15m' },
                { name: 'Order Data Migration', type: 'migration', status: 'running', progress: 23, eta: '2h 45m' },
                { name: 'Analytics Sync', type: 'replication', status: 'running', progress: 89, eta: '5m' },
                { name: 'Security Audit Scan', type: 'analysis', status: 'queued', progress: 0, eta: 'Pending' },
                { name: 'Schema Validation', type: 'validation', status: 'running', progress: 45, eta: '32m' }
              ] : isStaging ? [
                { name: 'Staging Data Refresh', type: 'refresh', status: 'running', progress: 78, eta: '8m' },
                { name: 'Feature Test Suite', type: 'testing', status: 'running', progress: 34, eta: '25m' },
                { name: 'Schema Sync Check', type: 'validation', status: 'running', progress: 92, eta: '3m' }
              ] : [
                { name: 'Dev Schema Migration', type: 'migration', status: 'running', progress: 56, eta: '12m' },
                { name: 'Code Analysis', type: 'analysis', status: 'running', progress: 12, eta: '45m' }
              ]).map((job, index) => (
                <div key={index} className="border border-border rounded-lg p-3">
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center space-x-2">
                      <div className={`w-2 h-2 rounded-full ${
                        job.status === 'running' ? 'bg-green-500 animate-pulse' :
                        'bg-yellow-500'
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
                    <span>{job.type}</span>
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
          </div>
        </div>
      </div>

      {/* Data Relationships & Schema Status */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        
        {/* Data Relationships */}
        <div className="bg-card border border-border rounded-lg">
          <div className="px-6 py-4 border-b border-border">
            <h3 className="text-lg font-semibold text-foreground">Data Relationships</h3>
          </div>
          <div className="p-6">
            <div className="space-y-3">
              {(isProduction ? [
                { name: 'Prod → Analytics', type: 'replication', status: 'healthy', latency: '1.2s', throughput: '2.1k/min' },
                { name: 'Orders → Warehouse', type: 'migration', status: 'healthy', latency: '2.8s', throughput: '1.5k/min' },
                { name: 'Customer → Backup', type: 'backup', status: 'healthy', latency: '0.9s', throughput: '3.2k/min' }
              ] : isStaging ? [
                { name: 'Staging → Test', type: 'replication', status: 'warning', latency: '8.3s', throughput: '450/min' },
                { name: 'Feature → Validation', type: 'testing', status: 'healthy', latency: '2.1s', throughput: '890/min' }
              ] : [
                { name: 'Dev → Local Test', type: 'testing', status: 'healthy', latency: '1.5s', throughput: '234/min' },
                { name: 'Feature → Integration', type: 'migration', status: 'healthy', latency: '3.2s', throughput: '156/min' }
              ]).map((relationship, index) => (
                <div key={index} className="flex items-center justify-between p-3 border border-border rounded-md">
                  <div className="flex items-center space-x-3">
                    <div className={`w-3 h-3 rounded-full ${
                      relationship.status === 'healthy' ? 'bg-green-500' : 'bg-yellow-500'
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

        {/* Schema Repositories */}
        <div className="bg-card border border-border rounded-lg">
          <div className="px-6 py-4 border-b border-border">
            <h3 className="text-lg font-semibold text-foreground">Schema Repositories</h3>
          </div>
          <div className="p-6">
            <div className="space-y-3">
              {(isProduction ? [
                { name: 'customer-schema', branch: 'main', commits: 156, lastUpdate: '2h ago', status: 'healthy' },
                { name: 'orders-schema', branch: 'main', commits: 234, lastUpdate: '4h ago', status: 'healthy' },
                { name: 'analytics-schema', branch: 'main', commits: 89, lastUpdate: '1d ago', status: 'healthy' }
              ] : isStaging ? [
                { name: 'customer-schema', branch: 'staging', commits: 145, lastUpdate: '30m ago', status: 'healthy' },
                { name: 'orders-schema', branch: 'staging', commits: 223, lastUpdate: '1h ago', status: 'healthy' },
                { name: 'feature-schema', branch: 'feature/v2', commits: 12, lastUpdate: '15m ago', status: 'active' }
              ] : [
                { name: 'customer-schema', branch: 'develop', commits: 134, lastUpdate: '45m ago', status: 'active' },
                { name: 'orders-schema', branch: 'feature/refactor', commits: 45, lastUpdate: '2h ago', status: 'active' }
              ]).map((repo, index) => (
                <div key={index} className="flex items-center justify-between p-3 border border-border rounded-md">
                  <div className="flex items-center space-x-3">
                    <GitBranch className="h-4 w-4 text-blue-600 dark:text-blue-400" />
                    <div>
                      <p className="font-medium text-foreground text-sm">{repo.name}</p>
                      <p className="text-xs text-muted-foreground">{repo.branch} • {repo.commits} commits</p>
                    </div>
                  </div>
                  <div className="text-right">
                    <p className="text-sm font-medium text-foreground">{repo.lastUpdate}</p>
                    <span className={`text-xs px-2 py-0.5 rounded-full ${
                      repo.status === 'healthy' ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400' :
                      'bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-400'
                    }`}>
                      {repo.status}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* Performance Metrics */}
      <div className="bg-card border border-border rounded-lg">
        <div className="px-6 py-4 border-b border-border">
          <h3 className="text-lg font-semibold text-foreground">Performance Metrics</h3>
        </div>
        <div className="p-6">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
            {[
              {
                metric: 'Query Response Time',
                value: isProduction ? '245ms' : isStaging ? '340ms' : '180ms',
                change: isProduction ? '-15ms' : isStaging ? '+45ms' : '-12ms',
                trend: isProduction ? 'up' : isStaging ? 'down' : 'up',
                target: '< 300ms'
              },
              {
                metric: 'Database Throughput',
                value: isProduction ? '2.1k qps' : isStaging ? '450 qps' : '120 qps',
                change: isProduction ? '+200 qps' : isStaging ? '-50 qps' : '+15 qps',
                trend: isProduction ? 'up' : isStaging ? 'down' : 'up',
                target: isProduction ? '> 2k qps' : isStaging ? '> 400 qps' : '> 100 qps'
              },
              {
                metric: 'Success Rate',
                value: isProduction ? '99.7%' : isStaging ? '98.2%' : '99.9%',
                change: isProduction ? '+0.1%' : isStaging ? '-0.3%' : '+0.2%',
                trend: isProduction ? 'up' : isStaging ? 'down' : 'up',
                target: '> 99%'
              },
              {
                metric: 'Data Freshness',
                value: isProduction ? '< 1min' : isStaging ? '< 5min' : '< 30s',
                change: isProduction ? '-10s' : isStaging ? '+30s' : '-5s',
                trend: isProduction ? 'up' : isStaging ? 'down' : 'up',
                target: isProduction ? '< 2min' : isStaging ? '< 10min' : '< 1min'
              }
            ].map((metric, index) => (
              <div key={index} className="text-center">
                <div className="flex items-center justify-center mb-2">
                  {metric.trend === 'up' ? (
                    <TrendingUp className="h-4 w-4 text-green-600 dark:text-green-400" />
                  ) : (
                    <TrendingDown className="h-4 w-4 text-red-600 dark:text-red-400" />
                  )}
                </div>
                <p className="text-sm text-muted-foreground">{metric.metric}</p>
                <p className="text-xl font-bold text-foreground mt-1">{metric.value}</p>
                <p className={`text-xs mt-1 ${
                  metric.trend === 'up' ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'
                }`}>
                  {metric.change}
                </p>
                <p className="text-xs text-muted-foreground">Target: {metric.target}</p>
              </div>
            ))}
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
                <Database className="h-6 w-6 text-blue-600 dark:text-blue-400 mx-auto mb-2" />
                <p className="font-medium text-foreground text-sm">Connect Database</p>
                <p className="text-xs text-muted-foreground">Add new instance</p>
              </div>
            </button>
            
            <button className="flex items-center justify-center p-4 border border-border rounded-lg hover:bg-muted transition-colors">
              <div className="text-center">
                <Zap className="h-6 w-6 text-green-600 dark:text-green-400 mx-auto mb-2" />
                <p className="font-medium text-foreground text-sm">Run Migration</p>
                <p className="text-xs text-muted-foreground">Start data migration</p>
              </div>
            </button>
            
            <button className="flex items-center justify-center p-4 border border-border rounded-lg hover:bg-muted transition-colors">
              <div className="text-center">
                <Settings className="h-6 w-6 text-purple-600 dark:text-purple-400 mx-auto mb-2" />
                <p className="font-medium text-foreground text-sm">Workspace Settings</p>
                <p className="text-xs text-muted-foreground">Configure workspace</p>
              </div>
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
