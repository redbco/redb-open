'use client';

import { useState } from 'react';
import { useConnectDatabase } from '@/lib/hooks/useDatabases';
import { useToast } from '@/components/ui/Toast';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { X, Database } from 'lucide-react';
import { ApiClientError } from '@/lib/api/client';
import type { Instance, LogicalDatabase } from '@/lib/api/types';

interface ConnectLogicalDatabaseDialogProps {
  workspaceId: string;
  instance: Instance;
  logicalDatabase: LogicalDatabase;
  onClose: () => void;
  onSuccess: () => void;
}

export function ConnectLogicalDatabaseDialog({
  workspaceId,
  instance,
  logicalDatabase,
  onClose,
  onSuccess,
}: ConnectLogicalDatabaseDialogProps) {
  const [useExistingCredentials, setUseExistingCredentials] = useState(true);
  const [formData, setFormData] = useState({
    database_name: logicalDatabase.name,
    database_description: `Database ${logicalDatabase.name} from ${instance.instance_name}`,
    database_type: instance.instance_type,
    database_vendor: instance.instance_vendor,
    host: instance.instance_host,
    port: instance.instance_port,
    username: instance.instance_username || '',
    password: '',
    db_name: logicalDatabase.name,
    node_id: '',
    enabled: true,
    ssl: instance.instance_ssl || false,
    ssl_mode: instance.instance_ssl_mode || 'disable',
    instance_name: instance.instance_name,
    instance_description: instance.instance_description || '',
  });

  const { connect, isLoading } = useConnectDatabase(workspaceId);
  const { showToast } = useToast();

  const handleChange = (field: string, value: any) => {
    setFormData(prev => ({ ...prev, [field]: value }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    // If using existing credentials, we don't need to pass the password
    const requestData = useExistingCredentials
      ? {
          ...formData,
          // Use existing instance credentials by not passing password or using instance details
        }
      : formData;

    try {
      await connect(requestData);
      showToast({
        type: 'success',
        title: 'Database Connected',
        message: `Successfully connected to ${logicalDatabase.name}`,
      });
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
              <h2 className="text-xl font-semibold text-foreground">Connect to Logical Database</h2>
              <p className="text-sm text-muted-foreground">
                Connect to {logicalDatabase.name} from {instance.instance_name}
              </p>
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
          <div className="p-6 space-y-6">
            {/* Database Connection Name */}
            <div>
              <label className="block text-sm font-medium text-foreground mb-2">
                Connection Name *
              </label>
              <input
                type="text"
                value={formData.database_name}
                onChange={(e) => handleChange('database_name', e.target.value)}
                className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent text-foreground"
                placeholder="my-database"
                required
              />
              <p className="text-xs text-muted-foreground mt-1">
                A friendly name for this database connection
              </p>
            </div>

            {/* Description */}
            <div>
              <label className="block text-sm font-medium text-foreground mb-2">
                Description
              </label>
              <textarea
                value={formData.database_description}
                onChange={(e) => handleChange('database_description', e.target.value)}
                className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent text-foreground"
                placeholder="Optional description..."
                rows={3}
              />
            </div>

            {/* Credentials Mode Selection */}
            <div className="border border-border rounded-lg p-4 space-y-4">
              <div className="flex items-center space-x-2">
                <input
                  type="radio"
                  id="use-existing"
                  checked={useExistingCredentials}
                  onChange={() => setUseExistingCredentials(true)}
                  className="rounded-full border-input"
                />
                <label htmlFor="use-existing" className="text-sm text-foreground cursor-pointer">
                  Use existing instance credentials
                </label>
              </div>

              <div className="flex items-center space-x-2">
                <input
                  type="radio"
                  id="use-custom"
                  checked={!useExistingCredentials}
                  onChange={() => setUseExistingCredentials(false)}
                  className="rounded-full border-input"
                />
                <label htmlFor="use-custom" className="text-sm text-foreground cursor-pointer">
                  Use different credentials
                </label>
              </div>

              {/* Custom Credentials Fields */}
              {!useExistingCredentials && (
                <div className="mt-4 space-y-4 pl-6 border-l-2 border-border">
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
                        required={!useExistingCredentials}
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
                        required={!useExistingCredentials}
                      />
                    </div>
                  </div>
                </div>
              )}
            </div>

            {/* Database Details (Read-only) */}
            <div className="bg-muted/30 border border-border rounded-lg p-4 space-y-2">
              <p className="text-sm font-medium text-foreground mb-2">Connection Details</p>
              <div className="grid grid-cols-2 gap-2 text-xs">
                <div>
                  <span className="text-muted-foreground">Instance:</span>
                  <span className="ml-2 text-foreground font-mono">{instance.instance_name}</span>
                </div>
                <div>
                  <span className="text-muted-foreground">Host:</span>
                  <span className="ml-2 text-foreground font-mono">{instance.instance_host}:{instance.instance_port}</span>
                </div>
                <div>
                  <span className="text-muted-foreground">Logical DB:</span>
                  <span className="ml-2 text-foreground font-mono">{logicalDatabase.name}</span>
                </div>
                <div>
                  <span className="text-muted-foreground">Size:</span>
                  <span className="ml-2 text-foreground font-mono">{(logicalDatabase.size_bytes / 1024 / 1024).toFixed(2)} MB</span>
                </div>
                <div>
                  <span className="text-muted-foreground">SSL:</span>
                  <span className="ml-2 text-foreground font-mono">{instance.instance_ssl ? 'Enabled' : 'Disabled'}</span>
                </div>
                <div>
                  <span className="text-muted-foreground">Encoding:</span>
                  <span className="ml-2 text-foreground font-mono">{logicalDatabase.encoding}</span>
                </div>
              </div>
            </div>

            {/* Enable checkbox */}
            <div className="flex items-center space-x-2">
              <input
                id="enable-db"
                type="checkbox"
                checked={formData.enabled}
                onChange={(e) => handleChange('enabled', e.target.checked)}
                className="rounded border-input"
              />
              <label htmlFor="enable-db" className="text-sm text-foreground cursor-pointer">
                Enable database after connection
              </label>
            </div>
          </div>

          {/* Footer */}
          <div className="flex items-center justify-between p-6 border-t border-border">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
              disabled={isLoading}
            >
              Cancel
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
          </div>
        </form>
      </div>
    </div>
  );
}

