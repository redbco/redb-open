import { Zap, Plus, Play, Pause, Clock, CheckCircle, AlertCircle, Database, ArrowRightLeft, Brain, FileText } from 'lucide-react';

interface JobsPageProps {
  params: Promise<{
    tenant: string;
    workspaceId: string;
  }>;
}

export default async function JobsPage({ params }: JobsPageProps) {
  const { tenant, workspaceId } = await params;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Jobs & Tasks</h2>
          <p className="text-muted-foreground mt-2">
            Data migrations using mappings, RAG processes, and temporary background tasks.
          </p>
        </div>
        <button className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors">
          <Plus className="h-4 w-4 mr-2" />
          Create Job
        </button>
      </div>

      {/* Job Status Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        {[
          { status: 'Migrations', count: 8, color: 'text-blue-600 dark:text-blue-400', bgColor: 'bg-blue-100 dark:bg-blue-900/20' },
          { status: 'RAG Processes', count: 5, color: 'text-green-600 dark:text-green-400', bgColor: 'bg-green-100 dark:bg-green-900/20' },
          { status: 'Background Tasks', count: 12, color: 'text-purple-600 dark:text-purple-400', bgColor: 'bg-purple-100 dark:bg-purple-900/20' },
          { status: 'Scheduled', count: 6, color: 'text-orange-600 dark:text-orange-400', bgColor: 'bg-orange-100 dark:bg-orange-900/20' }
        ].map((stat) => (
          <div key={stat.status} className="bg-card border border-border rounded-lg p-4">
            <div className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${stat.bgColor} ${stat.color} mb-2`}>
              {stat.status}
            </div>
            <p className="text-2xl font-bold text-foreground">{stat.count}</p>
          </div>
        ))}
      </div>

      {/* Active Jobs */}
      <div className="bg-card border border-border rounded-lg">
        <div className="px-6 py-4 border-b border-border">
          <h3 className="text-lg font-semibold text-foreground">Active Jobs</h3>
        </div>
        <div className="divide-y divide-border">
          {[
            {
              id: 'job-migration-001',
              name: 'Users Legacy to V2 Migration',
              type: 'Migration',
              status: 'running',
              progress: 65,
              mapping: 'map-users-001',
              startTime: '2024-01-15T02:00:00Z',
              estimatedCompletion: '2024-01-15T02:45:00Z',
              recordsProcessed: '1.2M / 1.8M'
            },
            {
              id: 'job-rag-002',
              name: 'Product Documentation RAG Update',
              type: 'RAG Process',
              status: 'running',
              progress: 23,
              mapping: null,
              startTime: '2024-01-15T01:30:00Z',
              estimatedCompletion: '2024-01-15T03:15:00Z',
              recordsProcessed: '450 / 2000 documents'
            },
            {
              id: 'job-migration-003',
              name: 'Orders Analytics Migration',
              type: 'Migration',
              status: 'scheduled',
              progress: 0,
              mapping: 'map-orders-001',
              startTime: '2024-01-15T03:00:00Z',
              estimatedCompletion: '2024-01-15T03:30:00Z',
              recordsProcessed: '0 / 850K'
            },
            {
              id: 'job-task-004',
              name: 'Data Quality Validation',
              type: 'Background Task',
              status: 'running',
              progress: 89,
              mapping: null,
              startTime: '2024-01-15T01:00:00Z',
              estimatedCompletion: '2024-01-15T02:30:00Z',
              recordsProcessed: '2.1M / 2.4M'
            }
          ].map((job) => (
            <div key={job.id} className="px-6 py-4">
              <div className="flex items-center justify-between mb-2">
                <div className="flex items-center space-x-3">
                  <div className="w-8 h-8 bg-primary/10 rounded-lg flex items-center justify-center">
                    {job.type === 'Migration' && <ArrowRightLeft className="h-4 w-4 text-primary" />}
                    {job.type === 'RAG Process' && <Brain className="h-4 w-4 text-primary" />}
                    {job.type === 'Background Task' && <Zap className="h-4 w-4 text-primary" />}
                  </div>
                  <div>
                    <h4 className="font-medium text-foreground">{job.name}</h4>
                    <div className="flex items-center space-x-2 text-sm text-muted-foreground">
                      <span>{job.type}</span>
                      <span>•</span>
                      <span>{job.id}</span>
                      {job.mapping && (
                        <>
                          <span>•</span>
                          <span>Using {job.mapping}</span>
                        </>
                      )}
                    </div>
                  </div>
                </div>
                <div className="flex items-center space-x-2">
                  <div className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${
                    job.status === 'running' 
                      ? 'bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-400'
                      : 'bg-orange-100 text-orange-800 dark:bg-orange-900/20 dark:text-orange-400'
                  }`}>
                    {job.status === 'running' ? (
                      <>
                        <Play className="h-3 w-3 mr-1" />
                        Running
                      </>
                    ) : (
                      <>
                        <Clock className="h-3 w-3 mr-1" />
                        Scheduled
                      </>
                    )}
                  </div>
                  <button className="p-1 rounded-md hover:bg-accent hover:text-accent-foreground">
                    <Pause className="h-4 w-4" />
                  </button>
                </div>
              </div>
              
              {job.status === 'running' && (
                <div className="mb-2">
                  <div className="flex items-center justify-between text-sm text-muted-foreground mb-1">
                    <span>Progress</span>
                    <span>{job.progress}%</span>
                  </div>
                  <div className="w-full bg-muted rounded-full h-2">
                    <div 
                      className="bg-primary h-2 rounded-full transition-all duration-300" 
                      style={{ width: `${job.progress}%` }}
                    ></div>
                  </div>
                </div>
              )}
              
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm text-muted-foreground">
                <div>
                  <span>Started: {new Date(job.startTime).toLocaleTimeString()}</span>
                </div>
                <div>
                  <span>ETA: {new Date(job.estimatedCompletion).toLocaleTimeString()}</span>
                </div>
                <div>
                  <span>Processed: {job.recordsProcessed}</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Recent Job History */}
      <div className="bg-card border border-border rounded-lg">
        <div className="px-6 py-4 border-b border-border">
          <h3 className="text-lg font-semibold text-foreground">Recent History</h3>
        </div>
        <div className="divide-y divide-border">
          {[
            {
              id: 'job-backup-yesterday',
              name: 'Daily Database Backup',
              status: 'completed',
              duration: '42 minutes',
              completedAt: '2024-01-14T02:42:00Z'
            },
            {
              id: 'job-migration-failed',
              name: 'Schema Migration v2.0.9',
              status: 'failed',
              duration: '5 minutes',
              completedAt: '2024-01-14T01:15:00Z',
              error: 'Foreign key constraint violation'
            },
            {
              id: 'job-sync-completed',
              name: 'Staging Data Refresh',
              status: 'completed',
              duration: '1 hour 23 minutes',
              completedAt: '2024-01-13T23:45:00Z'
            }
          ].map((job) => (
            <div key={job.id} className="px-6 py-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-3">
                  <div className={`w-8 h-8 rounded-lg flex items-center justify-center ${
                    job.status === 'completed' 
                      ? 'bg-green-100 dark:bg-green-900/20'
                      : 'bg-red-100 dark:bg-red-900/20'
                  }`}>
                    {job.status === 'completed' ? (
                      <CheckCircle className="h-4 w-4 text-green-600 dark:text-green-400" />
                    ) : (
                      <AlertCircle className="h-4 w-4 text-red-600 dark:text-red-400" />
                    )}
                  </div>
                  <div>
                    <h4 className="font-medium text-foreground">{job.name}</h4>
                    <div className="flex items-center space-x-2 text-sm text-muted-foreground">
                      <span>Duration: {job.duration}</span>
                      <span>•</span>
                      <span>Completed: {new Date(job.completedAt).toLocaleString()}</span>
                    </div>
                    {job.error && (
                      <p className="text-sm text-red-600 dark:text-red-400 mt-1">{job.error}</p>
                    )}
                  </div>
                </div>
                <button className="text-sm text-primary hover:text-primary/80 font-medium">
                  View Details
                </button>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export async function generateMetadata({ params }: JobsPageProps) {
  const { tenant, workspaceId } = await params;
  
  return {
    title: `Jobs & Tasks | ${workspaceId} | ${tenant} | reDB`,
    description: `Job management for ${workspaceId} workspace`,
  };
}
