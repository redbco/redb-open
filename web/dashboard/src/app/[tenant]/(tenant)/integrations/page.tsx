import { Plus, Brain, Webhook, Settings, FileText } from 'lucide-react';

interface IntegrationsPageProps {
  params: Promise<{
    tenant: string;
  }>;
}

export default async function IntegrationsPage() {
  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Integrations</h2>
          <p className="text-muted-foreground mt-2">
            Manage RAG embeddings, LLM processing, and webhook integrations for data transformation and external connectivity.
          </p>
        </div>
        <button className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors">
          <Plus className="h-4 w-4 mr-2" />
          Add Integration
        </button>
      </div>

      {/* Integration Categories Overview */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {[
          {
            title: 'RAG Integrations',
            value: '5',
            change: 'Document → Vector',
            icon: FileText,
            color: 'text-blue-600 dark:text-blue-400',
            bgColor: 'bg-blue-100 dark:bg-blue-900/20'
          },
          {
            title: 'LLM Integrations',
            value: '8',
            change: 'Data Processing',
            icon: Brain,
            color: 'text-green-600 dark:text-green-400',
            bgColor: 'bg-green-100 dark:bg-green-900/20'
          },
          {
            title: 'Webhooks',
            value: '12',
            change: 'Event Triggers',
            icon: Webhook,
            color: 'text-purple-600 dark:text-purple-400',
            bgColor: 'bg-purple-100 dark:bg-purple-900/20'
          }
        ].map((category, index) => (
          <div key={index} className="bg-card border border-border rounded-lg p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-muted-foreground">{category.title}</p>
                <p className="text-2xl font-bold text-foreground mt-1">{category.value}</p>
                <p className="text-sm text-muted-foreground mt-1">{category.change}</p>
              </div>
              <div className={`w-12 h-12 rounded-lg ${category.bgColor} flex items-center justify-center`}>
                <category.icon className={`h-6 w-6 ${category.color}`} />
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* RAG Integrations */}
      <div className="bg-card border border-border rounded-lg">
        <div className="px-6 py-4 border-b border-border">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              <FileText className="h-5 w-5 text-blue-600 dark:text-blue-400" />
              <h3 className="text-lg font-semibold text-foreground">RAG Integrations</h3>
            </div>
            <button className="text-sm text-primary hover:text-primary/80">
              + Add RAG Integration
            </button>
          </div>
          <p className="text-sm text-muted-foreground mt-1">
            Generate embeddings from document databases into vector databases for retrieval-augmented generation
          </p>
        </div>
        <div className="divide-y divide-border">
          {[
            {
              name: 'Product Documentation RAG',
              source: 'docs_mongodb',
              target: 'embeddings_pinecone',
              model: 'text-embedding-ada-002',
              status: 'active',
              lastRun: '2 hours ago',
              documentsProcessed: '2,543'
            },
            {
              name: 'Customer Support Knowledge Base',
              source: 'support_articles',
              target: 'knowledge_vectors',
              model: 'text-embedding-3-small',
              status: 'active',
              lastRun: '6 hours ago',
              documentsProcessed: '1,829'
            },
            {
              name: 'Legal Documents RAG',
              source: 'legal_docs_postgres',
              target: 'legal_vectors',
              model: 'text-embedding-3-large',
              status: 'pending',
              lastRun: 'Never',
              documentsProcessed: '0'
            }
          ].map((integration, index) => (
            <div key={index} className="px-6 py-4">
              <div className="flex items-start justify-between mb-3">
                <div>
                  <h4 className="font-medium text-foreground">{integration.name}</h4>
                  <p className="text-sm text-muted-foreground">
                    {integration.source} → {integration.target}
                  </p>
                </div>
                <div className="flex items-center space-x-2">
                  <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                    integration.status === 'active' 
                      ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
                      : 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400'
                  }`}>
                    {integration.status}
                  </span>
                  <button className="p-1 rounded-md hover:bg-accent hover:text-accent-foreground">
                    <Settings className="h-4 w-4" />
                  </button>
                </div>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                <div>
                  <p className="text-muted-foreground">Model</p>
                  <p className="font-medium text-foreground">{integration.model}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Last Run</p>
                  <p className="font-medium text-foreground">{integration.lastRun}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Documents Processed</p>
                  <p className="font-medium text-foreground">{integration.documentsProcessed}</p>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* LLM Integrations */}
      <div className="bg-card border border-border rounded-lg">
        <div className="px-6 py-4 border-b border-border">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              <Brain className="h-5 w-5 text-green-600 dark:text-green-400" />
              <h3 className="text-lg font-semibold text-foreground">LLM Integrations</h3>
            </div>
            <button className="text-sm text-primary hover:text-primary/80">
              + Add LLM Integration
            </button>
          </div>
          <p className="text-sm text-muted-foreground mt-1">
            Process and transform data through large language models for analysis, enrichment, and generation
          </p>
        </div>
        <div className="divide-y divide-border">
          {[
            {
              name: 'Data Quality Analysis',
              provider: 'OpenAI GPT-4',
              model: 'gpt-4-turbo',
              purpose: 'Analyze data quality and suggest improvements',
              status: 'active',
              requestsToday: '1,247',
              avgLatency: '2.3s'
            },
            {
              name: 'Schema Documentation Generator',
              provider: 'Anthropic Claude',
              model: 'claude-3-sonnet',
              purpose: 'Generate documentation from database schemas',
              status: 'active',
              requestsToday: '89',
              avgLatency: '1.8s'
            },
            {
              name: 'SQL Query Optimization',
              provider: 'OpenAI GPT-4',
              model: 'gpt-4',
              purpose: 'Optimize SQL queries for performance',
              status: 'active',
              requestsToday: '324',
              avgLatency: '3.1s'
            },
            {
              name: 'Data Anomaly Detection',
              provider: 'Google PaLM',
              model: 'text-bison-001',
              purpose: 'Detect anomalies in data patterns',
              status: 'maintenance',
              requestsToday: '0',
              avgLatency: 'N/A'
            }
          ].map((integration, index) => (
            <div key={index} className="px-6 py-4">
              <div className="flex items-start justify-between mb-3">
                <div>
                  <h4 className="font-medium text-foreground">{integration.name}</h4>
                  <p className="text-sm text-muted-foreground">{integration.purpose}</p>
                </div>
                <div className="flex items-center space-x-2">
                  <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                    integration.status === 'active' 
                      ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
                      : 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400'
                  }`}>
                    {integration.status}
                  </span>
                  <button className="p-1 rounded-md hover:bg-accent hover:text-accent-foreground">
                    <Settings className="h-4 w-4" />
                  </button>
                </div>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                <div>
                  <p className="text-muted-foreground">Provider & Model</p>
                  <p className="font-medium text-foreground">{integration.provider}</p>
                  <p className="text-xs text-muted-foreground">{integration.model}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Requests Today</p>
                  <p className="font-medium text-foreground">{integration.requestsToday}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Avg Latency</p>
                  <p className="font-medium text-foreground">{integration.avgLatency}</p>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Webhooks */}
      <div className="bg-card border border-border rounded-lg">
        <div className="px-6 py-4 border-b border-border">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              <Webhook className="h-5 w-5 text-purple-600 dark:text-purple-400" />
              <h3 className="text-lg font-semibold text-foreground">Webhooks</h3>
            </div>
            <button className="text-sm text-primary hover:text-primary/80">
              + Add Webhook
            </button>
          </div>
          <p className="text-sm text-muted-foreground mt-1">
            Trigger external events based on internal database and workspace activities
          </p>
        </div>
        <div className="divide-y divide-border">
          {[
            {
              name: 'Schema Change Notifications',
              url: 'https://api.slack.com/hooks/...',
              trigger: 'schema_updated',
              status: 'active',
              lastTriggered: '3 minutes ago',
              successRate: '99.2%'
            },
            {
              name: 'Data Migration Alerts',
              url: 'https://hooks.zapier.com/...',
              trigger: 'migration_completed',
              status: 'active',
              lastTriggered: '2 hours ago',
              successRate: '100%'
            },
            {
              name: 'Instance Health Monitoring',
              url: 'https://monitoring.example.com/webhook',
              trigger: 'instance_unhealthy',
              status: 'active',
              lastTriggered: '1 day ago',
              successRate: '98.7%'
            },
            {
              name: 'User Access Audit',
              url: 'https://security.example.com/audit',
              trigger: 'user_access_granted',
              status: 'paused',
              lastTriggered: '5 days ago',
              successRate: '97.3%'
            },
            {
              name: 'Backup Completion Notice',
              url: 'https://api.teams.microsoft.com/...',
              trigger: 'backup_completed',
              status: 'active',
              lastTriggered: '6 hours ago',
              successRate: '100%'
            }
          ].map((webhook, index) => (
            <div key={index} className="px-6 py-4">
              <div className="flex items-start justify-between mb-3">
                <div>
                  <h4 className="font-medium text-foreground">{webhook.name}</h4>
                  <p className="text-sm text-muted-foreground">
                    Trigger: <span className="font-mono text-xs bg-muted px-1 py-0.5 rounded">{webhook.trigger}</span>
                  </p>
                </div>
                <div className="flex items-center space-x-2">
                  <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                    webhook.status === 'active' 
                      ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
                      : 'bg-gray-100 text-gray-800 dark:bg-gray-900/20 dark:text-gray-400'
                  }`}>
                    {webhook.status}
                  </span>
                  <button className="p-1 rounded-md hover:bg-accent hover:text-accent-foreground">
                    <Settings className="h-4 w-4" />
                  </button>
                </div>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                <div>
                  <p className="text-muted-foreground">Endpoint</p>
                  <p className="font-medium text-foreground text-xs font-mono truncate">{webhook.url}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Last Triggered</p>
                  <p className="font-medium text-foreground">{webhook.lastTriggered}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Success Rate</p>
                  <p className="font-medium text-foreground">{webhook.successRate}</p>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Integration Templates */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">RAG Templates</h3>
            <FileText className="h-5 w-5 text-muted-foreground" />
          </div>
          
          <div className="space-y-3">
            {[
              { name: 'Document Knowledge Base', description: 'PDF/Markdown → Vector DB' },
              { name: 'Code Repository RAG', description: 'Source code → Embeddings' },
              { name: 'Customer Support RAG', description: 'Support tickets → Search' }
            ].map((template, index) => (
              <div key={index} className="p-3 border border-border rounded-md hover:bg-accent cursor-pointer transition-colors">
                <p className="font-medium text-foreground text-sm">{template.name}</p>
                <p className="text-xs text-muted-foreground">{template.description}</p>
              </div>
            ))}
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">LLM Templates</h3>
            <Brain className="h-5 w-5 text-muted-foreground" />
          </div>
          
          <div className="space-y-3">
            {[
              { name: 'Data Quality Checker', description: 'Validate data integrity' },
              { name: 'Schema Documenter', description: 'Generate table docs' },
              { name: 'Query Optimizer', description: 'Improve SQL performance' }
            ].map((template, index) => (
              <div key={index} className="p-3 border border-border rounded-md hover:bg-accent cursor-pointer transition-colors">
                <p className="font-medium text-foreground text-sm">{template.name}</p>
                <p className="text-xs text-muted-foreground">{template.description}</p>
              </div>
            ))}
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-foreground">Webhook Templates</h3>
            <Webhook className="h-5 w-5 text-muted-foreground" />
          </div>
          
          <div className="space-y-3">
            {[
              { name: 'Slack Notifications', description: 'Schema changes → Slack' },
              { name: 'Email Alerts', description: 'System events → Email' },
              { name: 'Monitoring Integration', description: 'Health checks → Monitor' }
            ].map((template, index) => (
              <div key={index} className="p-3 border border-border rounded-md hover:bg-accent cursor-pointer transition-colors">
                <p className="font-medium text-foreground text-sm">{template.name}</p>
                <p className="text-xs text-muted-foreground">{template.description}</p>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Integration Info */}
      <div className="bg-muted/50 border border-border rounded-lg p-6">
        <h3 className="text-lg font-semibold text-foreground mb-2">About Integrations</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 text-sm text-muted-foreground">
          <div>
            <h4 className="font-medium text-foreground mb-2">RAG Integrations</h4>
            <ul className="space-y-1">
              <li>• Generate vector embeddings from documents</li>
              <li>• Support multiple embedding models</li>
              <li>• Connect to various vector databases</li>
              <li>• Batch processing and real-time updates</li>
            </ul>
          </div>
          <div>
            <h4 className="font-medium text-foreground mb-2">LLM Integrations</h4>
            <ul className="space-y-1">
              <li>• Process data through language models</li>
              <li>• Support OpenAI, Anthropic, Google</li>
              <li>• Custom prompts and fine-tuning</li>
              <li>• Rate limiting and cost management</li>
            </ul>
          </div>
          <div>
            <h4 className="font-medium text-foreground mb-2">Webhooks</h4>
            <ul className="space-y-1">
              <li>• React to database and system events</li>
              <li>• HTTP/HTTPS endpoint support</li>
              <li>• Retry logic and error handling</li>
              <li>• Payload customization and filtering</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}

export async function generateMetadata({ params }: IntegrationsPageProps) {
  const { tenant } = await params;
  
  return {
    title: `Integrations | ${tenant} | reDB`,
    description: `Integration management for ${tenant}`,
  };
}