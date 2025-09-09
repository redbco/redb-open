import { 
  Briefcase, 
  Database, 
  Server, 
  Activity, 
  Plus, 
  Settings, 
  Copy, 
  AlertTriangle, 
  CheckCircle, 
  Clock,
  MoreHorizontal,
  Edit,
  Eye,
  BarChart3,
  Zap,
  ArrowRightLeft,
  GitBranch,
  Users,
  Calendar,
  TrendingUp,
  AlertCircle
} from 'lucide-react';

export default async function WorkspacesPage() {
  // Mock workspace data
  const workspaces = [
    {
      id: 'production',
      name: 'Production',
      environment: 'production',
      status: 'healthy',
      instances: 8,
      databases: 24,
      activeJobs: 5,
      relationships: 12,
      repositories: 6,
      lastActivity: '2 minutes ago',
      created: '2023-01-15',
      owner: 'DevOps Team',
      description: 'Production environment for customer-facing applications',
      health: {
        uptime: '99.9%',
        performance: 'excellent',
        issues: 0
      }
    },
    {
      id: 'staging',
      name: 'Staging',
      environment: 'staging',
      status: 'warning',
      instances: 4,
      databases: 18,
      activeJobs: 3,
      relationships: 6,
      repositories: 4,
      lastActivity: '15 minutes ago',
      created: '2023-01-20',
      owner: 'QA Team',
      description: 'Pre-production testing and validation environment',
      health: {
        uptime: '98.2%',
        performance: 'good',
        issues: 1
      }
    },
    {
      id: 'development',
      name: 'Development',
      environment: 'development',
      status: 'active',
      instances: 3,
      databases: 12,
      activeJobs: 2,
      relationships: 4,
      repositories: 3,
      lastActivity: '5 minutes ago',
      created: '2023-02-01',
      owner: 'Engineering Team',
      description: 'Development and feature testing environment',
      health: {
        uptime: '97.8%',
        performance: 'good',
        issues: 0
      }
    },
    {
      id: 'analytics',
      name: 'Analytics',
      environment: 'analytics',
      status: 'maintenance',
      instances: 2,
      databases: 6,
      activeJobs: 1,
      relationships: 3,
      repositories: 2,
      lastActivity: '1 hour ago',
      created: '2023-03-10',
      owner: 'Data Team',
      description: 'Data analytics and reporting environment',
      health: {
        uptime: '95.5%',
        performance: 'fair',
        issues: 0
      }
    }
  ];

  const totalStats = {
    workspaces: workspaces.length,
    instances: workspaces.reduce((sum, w) => sum + w.instances, 0),
    databases: workspaces.reduce((sum, w) => sum + w.databases, 0),
    activeJobs: workspaces.reduce((sum, w) => sum + w.activeJobs, 0),
    relationships: workspaces.reduce((sum, w) => sum + w.relationships, 0),
    repositories: workspaces.reduce((sum, w) => sum + w.repositories, 0),
    healthyWorkspaces: workspaces.filter(w => w.status === 'healthy').length,
    issuesCount: workspaces.reduce((sum, w) => sum + w.health.issues, 0)
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Workspace Management</h2>
          <p className="text-muted-foreground mt-2">
            Manage, monitor, and configure all workspaces across your tenant infrastructure.
          </p>
        </div>
        <div className="flex space-x-3">
          <button className="flex items-center space-x-2 bg-primary text-primary-foreground px-4 py-2 rounded-lg hover:bg-primary/90 transition-colors">
            <Plus className="h-4 w-4" />
            <span>Create Workspace</span>
          </button>
          <button className="flex items-center space-x-2 border border-border px-4 py-2 rounded-lg hover:bg-muted transition-colors">
            <Settings className="h-4 w-4" />
            <span>Bulk Actions</span>
          </button>
        </div>
      </div>

      {/* Global Statistics */}
      <div className="grid grid-cols-1 md:grid-cols-6 gap-4">
        <div className="bg-card border border-border rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Total Workspaces</p>
              <p className="text-2xl font-bold text-foreground">{totalStats.workspaces}</p>
              <p className="text-sm text-green-600 dark:text-green-400">+1 this month</p>
            </div>
            <Briefcase className="h-8 w-8 text-blue-600 dark:text-blue-400" />
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Database Instances</p>
              <p className="text-2xl font-bold text-foreground">{totalStats.instances}</p>
              <p className="text-sm text-green-600 dark:text-green-400">+2 this week</p>
            </div>
            <Server className="h-8 w-8 text-green-600 dark:text-green-400" />
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Total Databases</p>
              <p className="text-2xl font-bold text-foreground">{totalStats.databases}</p>
              <p className="text-sm text-blue-600 dark:text-blue-400">Across all workspaces</p>
            </div>
            <Database className="h-8 w-8 text-purple-600 dark:text-purple-400" />
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Active Jobs</p>
              <p className="text-2xl font-bold text-foreground">{totalStats.activeJobs}</p>
              <p className="text-sm text-orange-600 dark:text-orange-400">Currently running</p>
            </div>
            <Zap className="h-8 w-8 text-orange-600 dark:text-orange-400" />
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Data Relationships</p>
              <p className="text-2xl font-bold text-foreground">{totalStats.relationships}</p>
              <p className="text-sm text-teal-600 dark:text-teal-400">Active replications</p>
            </div>
            <ArrowRightLeft className="h-8 w-8 text-teal-600 dark:text-teal-400" />
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Schema Repos</p>
              <p className="text-2xl font-bold text-foreground">{totalStats.repositories}</p>
              <p className="text-sm text-indigo-600 dark:text-indigo-400">Version controlled</p>
            </div>
            <GitBranch className="h-8 w-8 text-indigo-600 dark:text-indigo-400" />
          </div>
        </div>
      </div>

      {/* Health Overview */}
      <div className="bg-card border border-border rounded-lg">
        <div className="px-6 py-4 border-b border-border">
          <h3 className="text-lg font-semibold text-foreground">Workspace Health Overview</h3>
        </div>
        <div className="p-6">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div className="text-center">
              <div className="w-16 h-16 bg-green-100 dark:bg-green-900/20 rounded-full flex items-center justify-center mx-auto mb-2">
                <CheckCircle className="h-8 w-8 text-green-600 dark:text-green-400" />
              </div>
              <p className="text-2xl font-bold text-foreground">{totalStats.healthyWorkspaces}</p>
              <p className="text-sm text-muted-foreground">Healthy Workspaces</p>
            </div>
            
            <div className="text-center">
              <div className="w-16 h-16 bg-yellow-100 dark:bg-yellow-900/20 rounded-full flex items-center justify-center mx-auto mb-2">
                <AlertTriangle className="h-8 w-8 text-yellow-600 dark:text-yellow-400" />
              </div>
              <p className="text-2xl font-bold text-foreground">1</p>
              <p className="text-sm text-muted-foreground">Warnings</p>
            </div>
            
            <div className="text-center">
              <div className="w-16 h-16 bg-blue-100 dark:bg-blue-900/20 rounded-full flex items-center justify-center mx-auto mb-2">
                <Clock className="h-8 w-8 text-blue-600 dark:text-blue-400" />
              </div>
              <p className="text-2xl font-bold text-foreground">1</p>
              <p className="text-sm text-muted-foreground">In Maintenance</p>
            </div>
            
            <div className="text-center">
              <div className="w-16 h-16 bg-gray-100 dark:bg-gray-900/20 rounded-full flex items-center justify-center mx-auto mb-2">
                <BarChart3 className="h-8 w-8 text-gray-600 dark:text-gray-400" />
              </div>
              <p className="text-2xl font-bold text-foreground">98.1%</p>
              <p className="text-sm text-muted-foreground">Avg Uptime</p>
            </div>
          </div>
        </div>
      </div>

      {/* Workspace Management Table */}
      <div className="bg-card border border-border rounded-lg">
        <div className="px-6 py-4 border-b border-border">
          <div className="flex items-center justify-between">
            <h3 className="text-lg font-semibold text-foreground">Workspace Management</h3>
            <div className="flex items-center space-x-2">
              <select className="text-sm border border-border rounded-md px-3 py-1 bg-background">
                <option>All Environments</option>
                <option>Production</option>
                <option>Staging</option>
                <option>Development</option>
                <option>Analytics</option>
              </select>
              <select className="text-sm border border-border rounded-md px-3 py-1 bg-background">
                <option>All Status</option>
                <option>Healthy</option>
                <option>Warning</option>
                <option>Maintenance</option>
              </select>
            </div>
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-muted/50">
              <tr>
                <th className="text-left p-4 font-medium text-muted-foreground">Workspace</th>
                <th className="text-left p-4 font-medium text-muted-foreground">Status</th>
                <th className="text-left p-4 font-medium text-muted-foreground">Resources</th>
                <th className="text-left p-4 font-medium text-muted-foreground">Health</th>
                <th className="text-left p-4 font-medium text-muted-foreground">Owner</th>
                <th className="text-left p-4 font-medium text-muted-foreground">Last Activity</th>
                <th className="text-left p-4 font-medium text-muted-foreground">Actions</th>
              </tr>
            </thead>
            <tbody>
              {workspaces.map((workspace) => (
                <tr key={workspace.id} className="border-t border-border hover:bg-muted/30">
                  <td className="p-4">
                    <div className="flex items-center space-x-3">
                      <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${
                        workspace.environment === 'production' ? 'bg-red-100 dark:bg-red-900/20' :
                        workspace.environment === 'staging' ? 'bg-yellow-100 dark:bg-yellow-900/20' :
                        workspace.environment === 'development' ? 'bg-green-100 dark:bg-green-900/20' :
                        'bg-purple-100 dark:bg-purple-900/20'
                      }`}>
                        <Briefcase className={`h-5 w-5 ${
                          workspace.environment === 'production' ? 'text-red-600 dark:text-red-400' :
                          workspace.environment === 'staging' ? 'text-yellow-600 dark:text-yellow-400' :
                          workspace.environment === 'development' ? 'text-green-600 dark:text-green-400' :
                          'text-purple-600 dark:text-purple-400'
                        }`} />
                      </div>
                      <div>
                        <p className="font-medium text-foreground">{workspace.name}</p>
                        <p className="text-sm text-muted-foreground">{workspace.description}</p>
                      </div>
                    </div>
                  </td>
                  <td className="p-4">
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                      workspace.status === 'healthy' ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400' :
                      workspace.status === 'warning' ? 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400' :
                      workspace.status === 'active' ? 'bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-400' :
                      'bg-gray-100 text-gray-800 dark:bg-gray-900/20 dark:text-gray-400'
                    }`}>
                      {workspace.status}
                    </span>
                  </td>
                  <td className="p-4">
                    <div className="space-y-1 text-sm">
                      <div className="flex items-center space-x-4">
                        <span className="text-muted-foreground">Instances:</span>
                        <span className="font-medium text-foreground">{workspace.instances}</span>
                      </div>
                      <div className="flex items-center space-x-4">
                        <span className="text-muted-foreground">Databases:</span>
                        <span className="font-medium text-foreground">{workspace.databases}</span>
                      </div>
                      <div className="flex items-center space-x-4">
                        <span className="text-muted-foreground">Jobs:</span>
                        <span className="font-medium text-foreground">{workspace.activeJobs}</span>
                      </div>
                    </div>
                  </td>
                  <td className="p-4">
                    <div className="space-y-1 text-sm">
                      <div className="flex items-center space-x-2">
                        <span className="text-muted-foreground">Uptime:</span>
                        <span className={`font-medium ${
                          parseFloat(workspace.health.uptime) > 99 ? 'text-green-600 dark:text-green-400' :
                          parseFloat(workspace.health.uptime) > 97 ? 'text-yellow-600 dark:text-yellow-400' :
                          'text-red-600 dark:text-red-400'
                        }`}>
                          {workspace.health.uptime}
                        </span>
                      </div>
                      <div className="flex items-center space-x-2">
                        <span className="text-muted-foreground">Performance:</span>
                        <span className="font-medium text-foreground capitalize">{workspace.health.performance}</span>
                      </div>
                      {workspace.health.issues > 0 && (
                        <div className="flex items-center space-x-2">
                          <AlertCircle className="h-3 w-3 text-yellow-600 dark:text-yellow-400" />
                          <span className="text-yellow-600 dark:text-yellow-400 text-xs">{workspace.health.issues} issue(s)</span>
                        </div>
                      )}
                    </div>
                  </td>
                  <td className="p-4">
                    <div className="flex items-center space-x-2">
                      <Users className="h-4 w-4 text-muted-foreground" />
                      <span className="text-sm text-foreground">{workspace.owner}</span>
                    </div>
                    <div className="flex items-center space-x-2 mt-1">
                      <Calendar className="h-3 w-3 text-muted-foreground" />
                      <span className="text-xs text-muted-foreground">Created {workspace.created}</span>
                    </div>
                  </td>
                  <td className="p-4">
                    <span className="text-sm text-foreground">{workspace.lastActivity}</span>
                  </td>
                  <td className="p-4">
                    <div className="flex items-center space-x-2">
                      <button className="p-1 hover:bg-muted rounded" title="View Workspace">
                        <Eye className="h-4 w-4 text-muted-foreground hover:text-foreground" />
                      </button>
                      <button className="p-1 hover:bg-muted rounded" title="Edit Workspace">
                        <Edit className="h-4 w-4 text-muted-foreground hover:text-foreground" />
                      </button>
                      <button className="p-1 hover:bg-muted rounded" title="Clone Workspace">
                        <Copy className="h-4 w-4 text-muted-foreground hover:text-foreground" />
                      </button>
                      <button className="p-1 hover:bg-muted rounded" title="Workspace Settings">
                        <Settings className="h-4 w-4 text-muted-foreground hover:text-foreground" />
                      </button>
                      <div className="relative">
                        <button className="p-1 hover:bg-muted rounded" title="More Actions">
                          <MoreHorizontal className="h-4 w-4 text-muted-foreground hover:text-foreground" />
                        </button>
                      </div>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Performance Analytics */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        
        {/* Resource Utilization */}
        <div className="bg-card border border-border rounded-lg">
          <div className="px-6 py-4 border-b border-border">
            <h3 className="text-lg font-semibold text-foreground">Resource Utilization</h3>
          </div>
          <div className="p-6">
            <div className="space-y-4">
              {workspaces.map((workspace) => (
                <div key={workspace.id} className="flex items-center justify-between">
                  <div className="flex items-center space-x-3">
                    <div className={`w-3 h-3 rounded-full ${
                      workspace.status === 'healthy' ? 'bg-green-500' :
                      workspace.status === 'warning' ? 'bg-yellow-500' :
                      workspace.status === 'active' ? 'bg-blue-500' :
                      'bg-gray-500'
                    }`}></div>
                    <span className="font-medium text-foreground">{workspace.name}</span>
                  </div>
                  <div className="flex items-center space-x-4 text-sm">
                    <div className="text-center">
                      <p className="font-medium text-foreground">{workspace.instances}</p>
                      <p className="text-xs text-muted-foreground">Instances</p>
                    </div>
                    <div className="text-center">
                      <p className="font-medium text-foreground">{workspace.databases}</p>
                      <p className="text-xs text-muted-foreground">Databases</p>
                    </div>
                    <div className="text-center">
                      <p className="font-medium text-foreground">{workspace.activeJobs}</p>
                      <p className="text-xs text-muted-foreground">Jobs</p>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Recent Activity */}
        <div className="bg-card border border-border rounded-lg">
          <div className="px-6 py-4 border-b border-border">
            <h3 className="text-lg font-semibold text-foreground">Recent Workspace Activity</h3>
          </div>
          <div className="p-6">
            <div className="space-y-4">
              {[
                {
                  workspace: 'Production',
                  action: 'Database backup completed',
                  time: '2 minutes ago',
                  type: 'success',
                  icon: CheckCircle
                },
                {
                  workspace: 'Staging',
                  action: 'Memory usage spike detected',
                  time: '15 minutes ago',
                  type: 'warning',
                  icon: AlertTriangle
                },
                {
                  workspace: 'Development',
                  action: 'New schema migration started',
                  time: '1 hour ago',
                  type: 'info',
                  icon: Activity
                },
                {
                  workspace: 'Analytics',
                  action: 'Maintenance window started',
                  time: '2 hours ago',
                  type: 'maintenance',
                  icon: Clock
                },
                {
                  workspace: 'Production',
                  action: 'Performance optimization applied',
                  time: '4 hours ago',
                  type: 'success',
                  icon: TrendingUp
                }
              ].map((activity, index) => (
                <div key={index} className="flex items-start space-x-3">
                  <div className={`w-8 h-8 rounded-full flex items-center justify-center ${
                    activity.type === 'success' ? 'bg-green-100 dark:bg-green-900/20' :
                    activity.type === 'warning' ? 'bg-yellow-100 dark:bg-yellow-900/20' :
                    activity.type === 'info' ? 'bg-blue-100 dark:bg-blue-900/20' :
                    'bg-gray-100 dark:bg-gray-900/20'
                  }`}>
                    <activity.icon className={`h-4 w-4 ${
                      activity.type === 'success' ? 'text-green-600 dark:text-green-400' :
                      activity.type === 'warning' ? 'text-yellow-600 dark:text-yellow-400' :
                      activity.type === 'info' ? 'text-blue-600 dark:text-blue-400' :
                      'text-gray-600 dark:text-gray-400'
                    }`} />
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium text-foreground">{activity.action}</p>
                    <p className="text-xs text-muted-foreground">
                      {activity.workspace} • {activity.time}
                    </p>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export async function generateMetadata() {
  return {
    title: 'Workspace Management | reDB',
    description: 'Manage and monitor all workspaces',
  };
}