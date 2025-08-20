import { ArrowRightLeft, Plus, Database, Table, ArrowRight, Settings, Play, Pause } from 'lucide-react';

interface MappingsPageProps {
  params: Promise<{
    tenant: string;
    workspaceId: string;
  }>;
}

export default async function MappingsPage({ params }: MappingsPageProps) {
  const { tenant, workspaceId } = await params;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Schema Mappings</h2>
          <p className="text-muted-foreground mt-2">
            Define column-to-column mappings between database schemas and tables for migration and replication.
          </p>
        </div>
        <button className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors">
          <Plus className="h-4 w-4 mr-2" />
          Create Mapping
        </button>
      </div>

      {/* Mappings Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        {[
          {
            title: 'Total Mappings',
            value: '24',
            change: '+3 this week',
            icon: ArrowRightLeft,
            color: 'text-blue-600 dark:text-blue-400'
          },
          {
            title: 'Active Mappings',
            value: '18',
            change: 'In use by relationships',
            icon: Play,
            color: 'text-green-600 dark:text-green-400'
          },
          {
            title: 'Column Mappings',
            value: '156',
            change: 'Total column pairs',
            icon: Table,
            color: 'text-purple-600 dark:text-purple-400'
          },
          {
            title: 'Transformations',
            value: '42',
            change: 'Data transformations',
            icon: Settings,
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

      {/* Mapping List */}
      <div className="bg-card border border-border rounded-lg">
        <div className="px-6 py-4 border-b border-border">
          <h3 className="text-lg font-semibold text-foreground">Schema Mappings</h3>
        </div>
        <div className="divide-y divide-border">
          {[
            {
              id: 'map-users-001',
              name: 'Users Migration Mapping',
              sourceSchema: 'legacy_users',
              sourceTable: 'user_accounts',
              targetSchema: 'users_v2',
              targetTable: 'users',
              columns: 12,
              transformations: 3,
              status: 'active',
              lastUsed: '2 hours ago',
              usedBy: ['replication-001', 'migration-005']
            },
            {
              id: 'map-orders-001',
              name: 'Orders Replication Mapping',
              sourceSchema: 'orders_prod',
              sourceTable: 'orders',
              targetSchema: 'analytics',
              targetTable: 'order_facts',
              columns: 18,
              transformations: 8,
              status: 'active',
              lastUsed: '15 minutes ago',
              usedBy: ['replication-002']
            },
            {
              id: 'map-products-001',
              name: 'Product Catalog Sync',
              sourceSchema: 'inventory',
              sourceTable: 'products',
              targetSchema: 'catalog_v3',
              targetTable: 'product_catalog',
              columns: 24,
              transformations: 5,
              status: 'draft',
              lastUsed: 'Never',
              usedBy: []
            },
            {
              id: 'map-analytics-001',
              name: 'Analytics Data Mapping',
              sourceSchema: 'raw_events',
              sourceTable: 'user_events',
              targetSchema: 'analytics',
              targetTable: 'processed_events',
              columns: 15,
              transformations: 12,
              status: 'active',
              lastUsed: '5 minutes ago',
              usedBy: ['replication-003', 'migration-008']
            }
          ].map((mapping) => (
            <div key={mapping.id} className="px-6 py-4">
              <div className="flex items-start justify-between mb-3">
                <div className="flex items-center space-x-3">
                  <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
                    <ArrowRightLeft className="h-5 w-5 text-primary" />
                  </div>
                  <div>
                    <h4 className="font-medium text-foreground">{mapping.name}</h4>
                    <p className="text-sm text-muted-foreground">{mapping.id}</p>
                  </div>
                </div>
                <div className="flex items-center space-x-2">
                  <div className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${
                    mapping.status === 'active' 
                      ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
                      : 'bg-gray-100 text-gray-800 dark:bg-gray-900/20 dark:text-gray-400'
                  }`}>
                    {mapping.status}
                  </div>
                  <button className="p-1 rounded-md hover:bg-accent hover:text-accent-foreground">
                    <Settings className="h-4 w-4" />
                  </button>
                </div>
              </div>

              {/* Source to Target Flow */}
              <div className="flex items-center space-x-4 mb-3 p-3 bg-muted/50 rounded-lg">
                <div className="flex-1">
                  <div className="flex items-center space-x-2 mb-1">
                    <Database className="h-4 w-4 text-muted-foreground" />
                    <span className="text-sm font-medium text-foreground">{mapping.sourceSchema}</span>
                  </div>
                  <div className="flex items-center space-x-2">
                    <Table className="h-4 w-4 text-muted-foreground" />
                    <span className="text-sm text-muted-foreground">{mapping.sourceTable}</span>
                  </div>
                </div>
                
                <div className="flex items-center space-x-2">
                  <ArrowRight className="h-5 w-5 text-primary" />
                  <span className="text-xs text-muted-foreground bg-background px-2 py-1 rounded">
                    {mapping.columns} cols
                  </span>
                </div>
                
                <div className="flex-1">
                  <div className="flex items-center space-x-2 mb-1">
                    <Database className="h-4 w-4 text-muted-foreground" />
                    <span className="text-sm font-medium text-foreground">{mapping.targetSchema}</span>
                  </div>
                  <div className="flex items-center space-x-2">
                    <Table className="h-4 w-4 text-muted-foreground" />
                    <span className="text-sm text-muted-foreground">{mapping.targetTable}</span>
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                <div>
                  <p className="text-muted-foreground">Transformations</p>
                  <p className="font-medium text-foreground">{mapping.transformations}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Last Used</p>
                  <p className="font-medium text-foreground">{mapping.lastUsed}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Used By</p>
                  <div className="flex flex-wrap gap-1 mt-1">
                    {mapping.usedBy.length > 0 ? (
                      mapping.usedBy.map((relationship) => (
                        <span key={relationship} className="px-2 py-1 bg-primary/10 text-primary text-xs rounded">
                          {relationship}
                        </span>
                      ))
                    ) : (
                      <span className="text-muted-foreground text-xs">Not in use</span>
                    )}
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Mapping Types */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">Mapping Types</h3>
            <ArrowRightLeft className="h-5 w-5 text-muted-foreground" />
          </div>
          
          <div className="space-y-3">
            {[
              { type: 'Schema to Schema', count: 8, description: 'Full schema mappings' },
              { type: 'Table to Table', count: 12, description: 'Individual table mappings' },
              { type: 'Column Subset', count: 4, description: 'Partial column mappings' }
            ].map((type, index) => (
              <div key={index} className="flex items-center justify-between p-3 border border-border rounded-md">
                <div>
                  <p className="font-medium text-foreground">{type.type}</p>
                  <p className="text-sm text-muted-foreground">{type.description}</p>
                </div>
                <span className="text-sm font-medium text-foreground">{type.count}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">Common Transformations</h3>
            <Settings className="h-5 w-5 text-muted-foreground" />
          </div>
          
          <div className="space-y-3">
            {[
              { transformation: 'Data Type Conversion', usage: '18 mappings', example: 'VARCHAR → TEXT' },
              { transformation: 'Date Format Change', usage: '12 mappings', example: 'MM/DD/YYYY → YYYY-MM-DD' },
              { transformation: 'String Concatenation', usage: '8 mappings', example: 'first_name + last_name' },
              { transformation: 'Value Lookup', usage: '4 mappings', example: 'status_id → status_name' }
            ].map((transform, index) => (
              <div key={index} className="p-3 border border-border rounded-md">
                <div className="flex items-center justify-between mb-1">
                  <p className="font-medium text-foreground text-sm">{transform.transformation}</p>
                  <span className="text-xs text-muted-foreground">{transform.usage}</span>
                </div>
                <p className="text-xs text-muted-foreground">{transform.example}</p>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Mapping Info */}
      <div className="bg-muted/50 border border-border rounded-lg p-6">
        <h3 className="text-lg font-semibold text-foreground mb-2">About Schema Mappings</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 text-sm text-muted-foreground">
          <div>
            <h4 className="font-medium text-foreground mb-2">Mapping Capabilities</h4>
            <ul className="space-y-1">
              <li>• Column-to-column field mapping</li>
              <li>• Data type transformations</li>
              <li>• Value lookups and conversions</li>
              <li>• String manipulation functions</li>
              <li>• Conditional mapping rules</li>
            </ul>
          </div>
          <div>
            <h4 className="font-medium text-foreground mb-2">Usage in Relationships</h4>
            <ul className="space-y-1">
              <li>• Used by replication relationships</li>
              <li>• Applied in migration jobs</li>
              <li>• Reusable across multiple relationships</li>
              <li>• Version controlled with repositories</li>
              <li>• Testable with sample data</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}

export async function generateMetadata({ params }: MappingsPageProps) {
  const { tenant, workspaceId } = await params;
  
  return {
    title: `Mappings | ${workspaceId} | ${tenant} | reDB`,
    description: `Schema mappings for ${workspaceId} workspace`,
  };
}
