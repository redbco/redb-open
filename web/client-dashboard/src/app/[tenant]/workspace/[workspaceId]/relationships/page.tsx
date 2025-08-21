import { Link, Plus, Play, Pause, Activity, Database, ArrowRightLeft, Clock, Settings } from 'lucide-react';

interface RelationshipsPageProps {
  params: Promise<{
    tenant: string;
    workspaceId: string;
  }>;
}

export default async function RelationshipsPage({ params }: RelationshipsPageProps) {
  const { tenant, workspaceId } = await params;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Data Relationships</h2>
          <p className="text-muted-foreground mt-2">
            Active data replication and migration relationships using schema mappings for continuous data movement.
          </p>
        </div>
        <button className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors">
          <Plus className="h-4 w-4 mr-2" />
          Create Relationship
        </button>
      </div>

      {/* Relationships Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        {[
          {
            title: 'Active Relationships',
            value: '12',
            change: '8 replication, 4 migration',
            icon: Link,
            color: 'text-blue-600 dark:text-blue-400'
          },
          {
            title: 'Data Transferred',
            value: '2.4TB',
            change: 'This month',
            icon: Activity,
            color: 'text-green-600 dark:text-green-400'
          },
          {
            title: 'Avg Latency',
            value: '45ms',
            change: 'Replication lag',
            icon: Clock,
            color: 'text-purple-600 dark:text-purple-400'
          },
          {
            title: 'Success Rate',
            value: '99.8%',
            change: 'Last 30 days',
            icon: Database,
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

      {/* Active Relationships */}
      <div className="bg-card border border-border rounded-lg">
        <div className="px-6 py-4 border-b border-border">
          <h3 className="text-lg font-semibold text-foreground">Active Relationships</h3>
        </div>
        <div className="divide-y divide-border">
          {[
            {
              id: 'rel-replication-001',
              name: 'Users Production → Analytics',
              type: 'replication',
              status: 'running',
              mapping: 'map-users-001',
              sourceDb: 'users_prod',
              targetDb: 'analytics_warehouse',
              frequency: 'Real-time',
              lastSync: '2 minutes ago',
              recordsTransferred: '1.2M',
              errorRate: '0.01%'
            },
            {
              id: 'rel-replication-002',
              name: 'Orders → Data Lake',
              type: 'replication',
              status: 'running',
              mapping: 'map-orders-001',
              sourceDb: 'orders_prod',
              targetDb: 'data_lake',
              frequency: 'Every 5 minutes',
              lastSync: '3 minutes ago',
              recordsTransferred: '850K',
              errorRate: '0.02%'
            },
            {
              id: 'rel-migration-001',
              name: 'Legacy System Migration',
              type: 'migration',
              status: 'paused',
              mapping: 'map-legacy-001',
              sourceDb: 'legacy_crm',
              targetDb: 'crm_v2',
              frequency: 'One-time',
              lastSync: '2 hours ago',
              recordsTransferred: '2.4M',
              errorRate: '0.05%'
            },
            {
              id: 'rel-replication-003',
              name: 'Analytics Events Stream',
              type: 'replication',
              status: 'running',
              mapping: 'map-analytics-001',
              sourceDb: 'raw_events',
              targetDb: 'processed_events',
              frequency: 'Real-time',
              lastSync: '30 seconds ago',
              recordsTransferred: '5.8M',
              errorRate: '0.001%'
            }
          ].map((relationship) => (
            <div key={relationship.id} className="px-6 py-4">
              <div className="flex items-start justify-between mb-3">
                <div className="flex items-center space-x-3">
                  <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
                    <Link className="h-5 w-5 text-primary" />
                  </div>
                  <div>
                    <h4 className="font-medium text-foreground">{relationship.name}</h4>
                    <p className="text-sm text-muted-foreground">{relationship.id} • Using {relationship.mapping}</p>
                  </div>
                </div>
                <div className="flex items-center space-x-2">
                  <div className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${
                    relationship.status === 'running' 
                      ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
                      : 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400'
                  }`}>
                    {relationship.status === 'running' ? (
                      <>
                        <Play className="h-3 w-3 mr-1" />
                        Running
                      </>
                    ) : (
                      <>
                        <Pause className="h-3 w-3 mr-1" />
                        Paused
                      </>
                    )}
                  </div>
                  <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                    relationship.type === 'replication' 
                      ? 'bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-400'
                      : 'bg-purple-100 text-purple-800 dark:bg-purple-900/20 dark:text-purple-400'
                  }`}>
                    {relationship.type}
                  </span>
                  <button className="p-1 rounded-md hover:bg-accent hover:text-accent-foreground">
                    <Settings className="h-4 w-4" />
                  </button>
                </div>
              </div>

              {/* Data Flow Visualization */}
              <div className="flex items-center space-x-4 mb-3 p-3 bg-muted/50 rounded-lg">
                <div className="flex-1">
                  <div className="flex items-center space-x-2 mb-1">
                    <Database className="h-4 w-4 text-muted-foreground" />
                    <span className="text-sm font-medium text-foreground">{relationship.sourceDb}</span>
                  </div>
                  <p className="text-xs text-muted-foreground">Source Database</p>
                </div>
                
                <div className="flex flex-col items-center space-y-1">
                  <ArrowRightLeft className="h-5 w-5 text-primary" />
                  <span className="text-xs text-muted-foreground bg-background px-2 py-1 rounded">
                    {relationship.frequency}
                  </span>
                </div>
                
                <div className="flex-1">
                  <div className="flex items-center space-x-2 mb-1">
                    <Database className="h-4 w-4 text-muted-foreground" />
                    <span className="text-sm font-medium text-foreground">{relationship.targetDb}</span>
                  </div>
                  <p className="text-xs text-muted-foreground">Target Database</p>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-4 gap-4 text-sm">
                <div>
                  <p className="text-muted-foreground">Last Sync</p>
                  <p className="font-medium text-foreground">{relationship.lastSync}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Records</p>
                  <p className="font-medium text-foreground">{relationship.recordsTransferred}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Error Rate</p>
                  <p className={`font-medium ${
                    parseFloat(relationship.errorRate) > 0.1 ? 'text-red-600 dark:text-red-400' : 'text-foreground'
                  }`}>
                    {relationship.errorRate}
                  </p>
                </div>
                <div>
                  <p className="text-muted-foreground">Actions</p>
                  <div className="flex space-x-2">
                    {relationship.status === 'running' ? (
                      <button className="text-xs text-orange-600 hover:text-orange-700 font-medium">
                        Pause
                      </button>
                    ) : (
                      <button className="text-xs text-green-600 hover:text-green-700 font-medium">
                        Resume
                      </button>
                    )}
                    <button className="text-xs text-primary hover:text-primary/80 font-medium">
                      Details
                    </button>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Relationship Types */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">Replication Relationships</h3>
            <Activity className="h-5 w-5 text-muted-foreground" />
          </div>
          
          <div className="space-y-3">
            {[
              { name: 'Real-time Replication', count: 5, description: 'Continuous data sync' },
              { name: 'Scheduled Replication', count: 3, description: 'Periodic data sync' },
              { name: 'Event-driven Replication', count: 2, description: 'Trigger-based sync' }
            ].map((type, index) => (
              <div key={index} className="flex items-center justify-between p-3 border border-border rounded-md">
                <div>
                  <p className="font-medium text-foreground">{type.name}</p>
                  <p className="text-sm text-muted-foreground">{type.description}</p>
                </div>
                <span className="text-sm font-medium text-foreground">{type.count}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">Migration Relationships</h3>
            <Database className="h-5 w-5 text-muted-foreground" />
          </div>
          
          <div className="space-y-3">
            {[
              { name: 'One-time Migration', count: 2, description: 'Complete data transfer' },
              { name: 'Incremental Migration', count: 1, description: 'Gradual data transfer' },
              { name: 'Validation Migration', count: 1, description: 'Data verification transfer' }
            ].map((type, index) => (
              <div key={index} className="flex items-center justify-between p-3 border border-border rounded-md">
                <div>
                  <p className="font-medium text-foreground">{type.name}</p>
                  <p className="text-sm text-muted-foreground">{type.description}</p>
                </div>
                <span className="text-sm font-medium text-foreground">{type.count}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Relationship Info */}
      <div className="bg-muted/50 border border-border rounded-lg p-6">
        <h3 className="text-lg font-semibold text-foreground mb-2">About Data Relationships</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 text-sm text-muted-foreground">
          <div>
            <h4 className="font-medium text-foreground mb-2">Relationship Types</h4>
            <ul className="space-y-1">
              <li>• <strong>Replication:</strong> Continuous data synchronization</li>
              <li>• <strong>Migration:</strong> One-time or incremental data transfer</li>
              <li>• <strong>Backup:</strong> Scheduled data backup relationships</li>
              <li>• <strong>Archival:</strong> Long-term data storage relationships</li>
            </ul>
          </div>
          <div>
            <h4 className="font-medium text-foreground mb-2">Key Features</h4>
            <ul className="space-y-1">
              <li>• Uses schema mappings for data transformation</li>
              <li>• Supports real-time and scheduled operations</li>
              <li>• Monitors data quality and error rates</li>
              <li>• Provides rollback and recovery options</li>
              <li>• Tracks performance and throughput metrics</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}

export async function generateMetadata({ params }: RelationshipsPageProps) {
  const { tenant, workspaceId } = await params;
  
  return {
    title: `Relationships | ${workspaceId} | ${tenant} | reDB`,
    description: `Data relationships for ${workspaceId} workspace`,
  };
}
