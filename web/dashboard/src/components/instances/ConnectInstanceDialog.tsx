'use client';

import { useState } from 'react';
import { X, AlertCircle, Info } from 'lucide-react';
import { api } from '@/lib/api/endpoints';
import { ConnectInstanceRequest } from '@/lib/api/types';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';

interface ConnectInstanceDialogProps {
  workspaceId: string;
  onClose: () => void;
  onSuccess: () => void;
}

export function ConnectInstanceDialog({ workspaceId, onClose, onSuccess }: ConnectInstanceDialogProps) {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [useConnectionString, setUseConnectionString] = useState(false);
  
  // Form state
  const [formData, setFormData] = useState<ConnectInstanceRequest>({
    instance_name: '',
    instance_description: '',
    instance_type: 'postgresql',
    instance_vendor: '',
    host: '',
    port: 5432,
    username: '',
    password: '',
    node_id: undefined,
    enabled: true,
    ssl: false,
    ssl_mode: 'disable',
    ssl_cert: '',
    ssl_key: '',
    ssl_root_cert: '',
    environment_id: '',
  });

  const [connectionString, setConnectionString] = useState('');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setIsLoading(true);

    try {
      if (useConnectionString) {
        // TODO: Implement connection string parsing and submission
        throw new Error('Connection string parsing not yet implemented');
      } else {
        await api.instances.connect(workspaceId, formData);
        onSuccess();
      }
    } catch (err) {
      console.error('Failed to connect instance:', err);
      setError(err instanceof Error ? err.message : 'Failed to connect instance');
    } finally {
      setIsLoading(false);
    }
  };

  const handleChange = (field: keyof ConnectInstanceRequest, value: any) => {
    setFormData(prev => ({
      ...prev,
      [field]: value,
    }));
  };

  // Update default port based on instance type
  const handleTypeChange = (type: string) => {
    const defaultPorts: Record<string, number> = {
      postgresql: 5432,
      postgres: 5432,
      mysql: 3306,
      mariadb: 3306,
      mongodb: 27017,
      redis: 6379,
      mssql: 1433,
      sqlserver: 1433,
      oracle: 1521,
    };

    handleChange('instance_type', type);
    handleChange('instance_vendor', type);
    if (defaultPorts[type.toLowerCase()]) {
      handleChange('port', defaultPorts[type.toLowerCase()]);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-background/80 backdrop-blur-sm">
      <div className="bg-card border border-border rounded-lg shadow-lg max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-border">
          <h2 className="text-2xl font-bold text-foreground">Connect Instance</h2>
          <button
            onClick={onClose}
            className="text-muted-foreground hover:text-foreground transition-colors"
            disabled={isLoading}
          >
            <X className="h-6 w-6" />
          </button>
        </div>

        {/* Mode Toggle */}
        <div className="p-6 border-b border-border">
          <div className="flex items-center space-x-4">
            <button
              type="button"
              onClick={() => setUseConnectionString(false)}
              className={`px-4 py-2 rounded-md transition-colors ${
                !useConnectionString
                  ? 'bg-primary text-primary-foreground'
                  : 'bg-muted text-muted-foreground hover:bg-muted/80'
              }`}
            >
              Manual Configuration
            </button>
            <button
              type="button"
              onClick={() => setUseConnectionString(true)}
              className={`px-4 py-2 rounded-md transition-colors ${
                useConnectionString
                  ? 'bg-primary text-primary-foreground'
                  : 'bg-muted text-muted-foreground hover:bg-muted/80'
              }`}
            >
              Connection String
            </button>
          </div>
        </div>

        <form onSubmit={handleSubmit}>
          <div className="p-6 space-y-6">
            {error && (
              <div className="bg-destructive/10 border border-destructive/20 rounded-lg p-4 flex items-start">
                <AlertCircle className="h-5 w-5 text-destructive mt-0.5 mr-3 flex-shrink-0" />
                <div>
                  <h4 className="text-sm font-medium text-destructive">Error</h4>
                  <p className="text-sm text-destructive/80 mt-1">{error}</p>
                </div>
              </div>
            )}

            {useConnectionString ? (
              <>
                {/* Connection String Mode */}
                <div>
                  <label className="block text-sm font-medium text-foreground mb-2">
                    Connection String
                  </label>
                  <input
                    type="text"
                    value={connectionString}
                    onChange={(e) => setConnectionString(e.target.value)}
                    className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
                    placeholder="postgresql://user:password@host:port/database"
                    required
                  />
                  <p className="text-sm text-muted-foreground mt-2">
                    Examples: postgresql://user:pass@localhost:5432/db, mysql://user:pass@host:3306/db
                  </p>
                </div>

                <div>
                  <label className="block text-sm font-medium text-foreground mb-2">
                    Instance Name <span className="text-destructive">*</span>
                  </label>
                  <input
                    type="text"
                    value={formData.instance_name}
                    onChange={(e) => handleChange('instance_name', e.target.value)}
                    className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
                    placeholder="my-postgres-instance"
                    required
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-foreground mb-2">
                    Description
                  </label>
                  <input
                    type="text"
                    value={formData.instance_description}
                    onChange={(e) => handleChange('instance_description', e.target.value)}
                    className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
                    placeholder="Production database instance"
                  />
                </div>
              </>
            ) : (
              <>
                {/* Manual Configuration Mode */}
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-foreground mb-2">
                      Instance Name <span className="text-destructive">*</span>
                    </label>
                    <input
                      type="text"
                      value={formData.instance_name}
                      onChange={(e) => handleChange('instance_name', e.target.value)}
                      className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
                      placeholder="my-postgres-instance"
                      required
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-foreground mb-2">
                      Database Type <span className="text-destructive">*</span>
                    </label>
                    <select
                      value={formData.instance_type}
                      onChange={(e) => handleTypeChange(e.target.value)}
                      className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
                      required
                    >
                      <option value="postgresql">PostgreSQL</option>
                      <option value="mysql">MySQL</option>
                      <option value="mariadb">MariaDB</option>
                      <option value="mongodb">MongoDB</option>
                      <option value="redis">Redis</option>
                      <option value="mssql">SQL Server</option>
                      <option value="oracle">Oracle</option>
                    </select>
                  </div>
                </div>

                <div>
                  <label className="block text-sm font-medium text-foreground mb-2">
                    Description
                  </label>
                  <input
                    type="text"
                    value={formData.instance_description}
                    onChange={(e) => handleChange('instance_description', e.target.value)}
                    className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
                    placeholder="Production database instance"
                  />
                </div>

                <div className="grid grid-cols-3 gap-4">
                  <div className="col-span-2">
                    <label className="block text-sm font-medium text-foreground mb-2">
                      Host <span className="text-destructive">*</span>
                    </label>
                    <input
                      type="text"
                      value={formData.host}
                      onChange={(e) => handleChange('host', e.target.value)}
                      className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
                      placeholder="localhost"
                      required
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-foreground mb-2">
                      Port <span className="text-destructive">*</span>
                    </label>
                    <input
                      type="number"
                      value={formData.port}
                      onChange={(e) => handleChange('port', parseInt(e.target.value))}
                      className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
                      required
                    />
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-foreground mb-2">
                      Username
                    </label>
                    <input
                      type="text"
                      value={formData.username}
                      onChange={(e) => handleChange('username', e.target.value)}
                      className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
                      placeholder="admin"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-foreground mb-2">
                      Password
                    </label>
                    <input
                      type="password"
                      value={formData.password}
                      onChange={(e) => handleChange('password', e.target.value)}
                      className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
                      placeholder="••••••••"
                    />
                  </div>
                </div>

                {/* SSL Configuration */}
                <div className="border border-border rounded-lg p-4 space-y-4">
                  <div className="flex items-center justify-between">
                    <div>
                      <h3 className="text-sm font-medium text-foreground">SSL Configuration</h3>
                      <p className="text-xs text-muted-foreground mt-1">
                        Enable SSL/TLS for secure connections
                      </p>
                    </div>
                    <label className="relative inline-flex items-center cursor-pointer">
                      <input
                        type="checkbox"
                        checked={formData.ssl || false}
                        onChange={(e) => handleChange('ssl', e.target.checked)}
                        className="sr-only peer"
                      />
                      <div className="w-11 h-6 bg-muted peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-primary/20 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary"></div>
                    </label>
                  </div>

                  {formData.ssl && (
                    <div>
                      <label className="block text-sm font-medium text-foreground mb-2">
                        SSL Mode
                      </label>
                      <select
                        value={formData.ssl_mode}
                        onChange={(e) => handleChange('ssl_mode', e.target.value)}
                        className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
                      >
                        <option value="disable">Disable</option>
                        <option value="require">Require</option>
                        <option value="verify-ca">Verify CA</option>
                        <option value="verify-full">Verify Full</option>
                      </select>
                    </div>
                  )}
                </div>

                {/* Advanced Options */}
                <div className="flex items-center space-x-4">
                  <label className="relative inline-flex items-center cursor-pointer">
                    <input
                      type="checkbox"
                      checked={formData.enabled || false}
                      onChange={(e) => handleChange('enabled', e.target.checked)}
                      className="sr-only peer"
                    />
                    <div className="w-11 h-6 bg-muted peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-primary/20 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary"></div>
                    <span className="ml-3 text-sm font-medium text-foreground">Enable instance after connection</span>
                  </label>
                </div>
              </>
            )}
          </div>

          {/* Footer */}
          <div className="flex items-center justify-end space-x-3 p-6 border-t border-border bg-muted/50">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors text-foreground"
              disabled={isLoading}
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors inline-flex items-center"
              disabled={isLoading}
            >
              {isLoading ? (
                <>
                  <LoadingSpinner size="sm" className="mr-2" />
                  Connecting...
                </>
              ) : (
                'Connect Instance'
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

