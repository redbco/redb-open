'use client';

import { useState } from 'react';
import { useConnectDatabase } from '@/lib/hooks/useDatabases';
import { useToast } from '@/components/ui/Toast';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { X, Database } from 'lucide-react';
import { ApiClientError } from '@/lib/api/client';

interface ConnectDatabaseDialogProps {
  workspaceId: string;
  onClose: () => void;
  onSuccess: () => void;
}

export function ConnectDatabaseDialog({ workspaceId, onClose, onSuccess }: ConnectDatabaseDialogProps) {
  const [step, setStep] = useState(1);
  const [formData, setFormData] = useState({
    database_name: '',
    database_description: '',
    database_type: 'postgresql',
    database_vendor: 'postgresql',
    host: '',
    port: 5432,
    username: '',
    password: '',
    db_name: '',
    node_id: 'node_1',
    enabled: true,
    ssl: false,
    ssl_mode: 'disable',
    instance_name: '',
    instance_description: '',
  });

  const { connect, isLoading } = useConnectDatabase(workspaceId);
  const { showToast } = useToast();

  const handleChange = (field: string, value: any) => {
    setFormData(prev => ({ ...prev, [field]: value }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    try {
      await connect(formData);
      onSuccess();
    } catch (error) {
      if (error instanceof ApiClientError) {
        showToast({
          type: 'error',
          title: 'Connection Failed',
          message: error.apiError.message || 'Failed to connect database',
        });
      } else {
        showToast({
          type: 'error',
          title: 'Connection Failed',
          message: 'An unexpected error occurred',
        });
      }
    }
  };

  return (
    <div className="fixed inset-0 bg-background/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
      <div className="bg-card border border-border rounded-lg shadow-lg max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-border">
          <div className="flex items-center space-x-3">
            <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
              <Database className="h-5 w-5 text-primary" />
            </div>
            <div>
              <h2 className="text-xl font-semibold text-foreground">Connect Database</h2>
              <p className="text-sm text-muted-foreground">Step {step} of 2</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2 rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        <form onSubmit={handleSubmit}>
          {/* Form Content */}
          <div className="p-6 space-y-6">
            {step === 1 && (
              <>
                <div>
                  <label className="block text-sm font-medium text-foreground mb-2">
                    Database Name *
                  </label>
                  <input
                    type="text"
                    value={formData.database_name}
                    onChange={(e) => handleChange('database_name', e.target.value)}
                    className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent text-foreground"
                    placeholder="my-database"
                    required
                  />
                  <p className="text-xs text-muted-foreground mt-1">A friendly name for your database</p>
                </div>

                <div>
                  <label className="block text-sm font-medium text-foreground mb-2">
                    Description
                  </label>
                  <textarea
                    value={formData.database_description}
                    onChange={(e) => handleChange('database_description', e.target.value)}
                    className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent text-foreground"
                    placeholder="Production database for..."
                    rows={3}
                  />
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-foreground mb-2">
                      Database Type *
                    </label>
                    <select
                      value={formData.database_type}
                      onChange={(e) => {
                        handleChange('database_type', e.target.value);
                        handleChange('database_vendor', e.target.value);
                        // Set default port based on type
                        const ports: Record<string, number> = {
                          postgresql: 5432,
                          mysql: 3306,
                          mongodb: 27017,
                          redis: 6379,
                        };
                        handleChange('port', ports[e.target.value] || 5432);
                      }}
                      className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent text-foreground"
                      required
                    >
                      <option value="postgresql">PostgreSQL</option>
                      <option value="mysql">MySQL</option>
                      <option value="mongodb">MongoDB</option>
                      <option value="redis">Redis</option>
                    </select>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-foreground mb-2">
                      Instance Name
                    </label>
                    <input
                      type="text"
                      value={formData.instance_name}
                      onChange={(e) => handleChange('instance_name', e.target.value)}
                      className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent text-foreground"
                      placeholder="my-instance"
                    />
                    <p className="text-xs text-muted-foreground mt-1">Leave empty to use existing</p>
                  </div>
                </div>
              </>
            )}

            {step === 2 && (
              <>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-foreground mb-2">
                      Host *
                    </label>
                    <input
                      type="text"
                      value={formData.host}
                      onChange={(e) => handleChange('host', e.target.value)}
                      className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent text-foreground"
                      placeholder="localhost"
                      required
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-foreground mb-2">
                      Port *
                    </label>
                    <input
                      type="number"
                      value={formData.port}
                      onChange={(e) => handleChange('port', parseInt(e.target.value))}
                      className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent text-foreground"
                      required
                    />
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-foreground mb-2">
                      Username *
                    </label>
                    <input
                      type="text"
                      value={formData.username}
                      onChange={(e) => handleChange('username', e.target.value)}
                      className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent text-foreground"
                      placeholder="postgres"
                      required
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-foreground mb-2">
                      Password *
                    </label>
                    <input
                      type="password"
                      value={formData.password}
                      onChange={(e) => handleChange('password', e.target.value)}
                      className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent text-foreground"
                      placeholder="••••••••"
                      required
                    />
                  </div>
                </div>

                <div>
                  <label className="block text-sm font-medium text-foreground mb-2">
                    Database Name *
                  </label>
                  <input
                    type="text"
                    value={formData.db_name}
                    onChange={(e) => handleChange('db_name', e.target.value)}
                    className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent text-foreground"
                    placeholder="myapp_production"
                    required
                  />
                  <p className="text-xs text-muted-foreground mt-1">The actual database name in the server</p>
                </div>

                <div className="flex items-center space-x-4">
                  <label className="flex items-center space-x-2 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={formData.ssl}
                      onChange={(e) => {
                        handleChange('ssl', e.target.checked);
                        if (e.target.checked) {
                          handleChange('ssl_mode', 'require');
                        } else {
                          handleChange('ssl_mode', 'disable');
                        }
                      }}
                      className="rounded border-input"
                    />
                    <span className="text-sm text-foreground">Enable SSL</span>
                  </label>

                  {formData.ssl && (
                    <select
                      value={formData.ssl_mode}
                      onChange={(e) => handleChange('ssl_mode', e.target.value)}
                      className="px-3 py-1 border border-input bg-background rounded-md text-sm"
                    >
                      <option value="disable">Disable</option>
                      <option value="require">Require</option>
                      <option value="verify-ca">Verify CA</option>
                      <option value="verify-full">Verify Full</option>
                    </select>
                  )}
                </div>

                <div className="flex items-center space-x-2">
                  <input
                    type="checkbox"
                    checked={formData.enabled}
                    onChange={(e) => handleChange('enabled', e.target.checked)}
                    className="rounded border-input"
                  />
                  <label className="text-sm text-foreground cursor-pointer">
                    Enable database after connection
                  </label>
                </div>
              </>
            )}
          </div>

          {/* Footer */}
          <div className="flex items-center justify-between p-6 border-t border-border">
            {step === 1 ? (
              <>
                <button
                  type="button"
                  onClick={onClose}
                  className="px-4 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
                >
                  Cancel
                </button>
                <button
                  type="button"
                  onClick={() => setStep(2)}
                  className="px-6 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
                  disabled={!formData.database_name}
                >
                  Next
                </button>
              </>
            ) : (
              <>
                <button
                  type="button"
                  onClick={() => setStep(1)}
                  className="px-4 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
                  disabled={isLoading}
                >
                  Back
                </button>
                <button
                  type="submit"
                  className="px-6 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors flex items-center disabled:opacity-50 disabled:cursor-not-allowed"
                  disabled={isLoading}
                >
                  {isLoading ? (
                    <>
                      <LoadingSpinner size="sm" className="mr-2" />
                      Connecting...
                    </>
                  ) : (
                    'Connect Database'
                  )}
                </button>
              </>
            )}
          </div>
        </form>
      </div>
    </div>
  );
}

