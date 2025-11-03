'use client';

import { useState } from 'react';
import { useConnectDatabase } from '@/lib/hooks/useDatabases';
import { useToast } from '@/components/ui/Toast';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { X, Database, ChevronDown, ChevronRight, AlertCircle } from 'lucide-react';
import { ApiClientError } from '@/lib/api/client';
import { validateConnectionString } from '@/lib/utils/databaseCapabilities';

interface ConnectDatabaseDialogProps {
  workspaceId: string;
  onClose: () => void;
  onSuccess: () => void;
}

type ConnectionMode = 'string' | 'manual';

export function ConnectDatabaseDialog({ workspaceId, onClose, onSuccess }: ConnectDatabaseDialogProps) {
  const [mode, setMode] = useState<ConnectionMode>('string');
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [step, setStep] = useState(1);
  const [connectionStringError, setConnectionStringError] = useState<string>('');
  
  // Connection string form data
  const [stringFormData, setStringFormData] = useState({
    connection_string: '',
    database_name: '',
    database_description: '',
    node_id: '',
    enabled: true,
  });

  // Manual connection form data
  const [manualFormData, setManualFormData] = useState({
    database_name: '',
    database_description: '',
    database_type: 'postgresql',
    database_vendor: 'postgresql',
    host: '',
    port: 5432,
    username: '',
    password: '',
    db_name: '',
    node_id: '',
    enabled: true,
    ssl: false,
    ssl_mode: 'disable',
    instance_name: '',
    instance_description: '',
  });

  const { connect, connectString, isLoading } = useConnectDatabase(workspaceId);
  const { showToast } = useToast();

  const handleStringChange = (field: string, value: any) => {
    setStringFormData(prev => ({ ...prev, [field]: value }));
    
    // Validate connection string in real-time
    if (field === 'connection_string') {
      const connectionString = value as string;
      if (connectionString.trim()) {
        const validation = validateConnectionString(connectionString);
        if (!validation.isValid) {
          setConnectionStringError(validation.error || 'Invalid connection string');
        } else {
          setConnectionStringError('');
        }
      } else {
        setConnectionStringError('');
      }
    }
  };

  const handleManualChange = (field: string, value: any) => {
    setManualFormData(prev => ({ ...prev, [field]: value }));
  };

  const handleStringSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    // Final validation before submission
    const validation = validateConnectionString(stringFormData.connection_string);
    if (!validation.isValid) {
      setConnectionStringError(validation.error || 'Invalid connection string');
      showToast({
        type: 'error',
        title: 'Invalid Connection String',
        message: validation.error || 'Please check your connection string format',
      });
      return;
    }

    try {
      await connectString(stringFormData);
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

  const handleManualSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    try {
      await connect(manualFormData);
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
              <p className="text-sm text-muted-foreground">
                {mode === 'manual' ? `Step ${step} of 2` : 'Quick connect with connection string'}
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

        {/* Connection String Form */}
        {mode === 'string' && (
          <form onSubmit={handleStringSubmit}>
            <div className="p-6 space-y-6">
              <div>
                <label className="block text-sm font-medium text-foreground mb-2">
                  Database Name *
                </label>
                <input
                  type="text"
                  value={stringFormData.database_name}
                  onChange={(e) => handleStringChange('database_name', e.target.value)}
                  className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent text-foreground"
                  placeholder="my-database"
                  required
                />
                <p className="text-xs text-muted-foreground mt-1">A friendly name for your database</p>
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-2">
                  Connection String *
                </label>
                <div className="relative">
                  <input
                    type="text"
                    value={stringFormData.connection_string}
                    onChange={(e) => handleStringChange('connection_string', e.target.value)}
                    className={`w-full px-4 py-2 border rounded-md focus:outline-none focus:ring-2 focus:border-transparent text-foreground font-mono text-sm ${
                      connectionStringError
                        ? 'border-red-500 focus:ring-red-500 bg-red-50 dark:bg-red-950/10'
                        : 'border-input bg-background focus:ring-primary'
                    }`}
                    placeholder="postgresql://user:password@host:port/database"
                    required
                  />
                  {connectionStringError && (
                    <div className="absolute right-3 top-1/2 -translate-y-1/2">
                      <AlertCircle className="h-5 w-5 text-red-500" />
                    </div>
                  )}
                </div>
                {connectionStringError ? (
                  <div className="mt-2 flex items-start gap-2 text-sm text-red-600 dark:text-red-400">
                    <AlertCircle className="h-4 w-4 mt-0.5 flex-shrink-0" />
                    <span>{connectionStringError}</span>
                  </div>
                ) : (
                  <p className="text-xs text-muted-foreground mt-1">
                    Example: postgresql://user:password@host:port/database
                  </p>
                )}
              </div>

              {/* Advanced Options */}
              <div className="border-t border-border pt-4">
                <button
                  type="button"
                  onClick={() => setShowAdvanced(!showAdvanced)}
                  className="flex items-center gap-2 text-sm font-medium text-foreground hover:text-primary transition-colors"
                >
                  {showAdvanced ? (
                    <ChevronDown className="h-4 w-4" />
                  ) : (
                    <ChevronRight className="h-4 w-4" />
                  )}
                  Advanced Options
                </button>

                {showAdvanced && (
                  <div className="mt-4 space-y-4 pl-6">
                    <div>
                      <label className="block text-sm font-medium text-foreground mb-2">
                        Description
                      </label>
                      <textarea
                        value={stringFormData.database_description}
                        onChange={(e) => handleStringChange('database_description', e.target.value)}
                        className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent text-foreground"
                        placeholder="Optional description..."
                        rows={3}
                      />
                    </div>

                    <div className="flex items-center space-x-2">
                      <input
                        id="enable-db"
                        type="checkbox"
                        checked={stringFormData.enabled}
                        onChange={(e) => handleStringChange('enabled', e.target.checked)}
                        className="rounded border-input"
                      />
                      <label htmlFor="enable-db" className="text-sm text-foreground cursor-pointer">
                        Enable database after connection
                      </label>
                    </div>

                    <div className="pt-2 border-t border-border">
                      <button
                        type="button"
                        onClick={() => {
                          setMode('manual');
                          setStep(1);
                        }}
                        className="text-sm text-primary hover:text-primary/80 underline"
                      >
                        Switch to manual configuration
                      </button>
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* Footer */}
            <div className="flex items-center justify-between p-6 border-t border-border">
              <button
                type="button"
                onClick={onClose}
                className="px-4 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
              >
                Cancel
              </button>
              <button
                type="submit"
                className="px-6 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors flex items-center disabled:opacity-50 disabled:cursor-not-allowed"
                disabled={isLoading || !!connectionStringError}
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
        )}

        {/* Manual Configuration Form */}
        {mode === 'manual' && (
          <form onSubmit={handleManualSubmit}>
            <div className="p-6 space-y-6">
              {/* Back to connection string link */}
              <div className="pb-4 border-b border-border">
                <button
                  type="button"
                  onClick={() => setMode('string')}
                  className="text-sm text-primary hover:text-primary/80 underline"
                >
                  ← Back to connection string
                </button>
              </div>

              {step === 1 && (
                <>
                  <div>
                    <label className="block text-sm font-medium text-foreground mb-2">
                      Database Name *
                    </label>
                    <input
                      type="text"
                      value={manualFormData.database_name}
                      onChange={(e) => handleManualChange('database_name', e.target.value)}
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
                      value={manualFormData.database_description}
                      onChange={(e) => handleManualChange('database_description', e.target.value)}
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
                        value={manualFormData.database_type}
                        onChange={(e) => {
                          handleManualChange('database_type', e.target.value);
                          handleManualChange('database_vendor', e.target.value);
                          // Set default port based on type
                          const ports: Record<string, number> = {
                            postgresql: 5432,
                            mysql: 3306,
                            mongodb: 27017,
                            redis: 6379,
                          };
                          handleManualChange('port', ports[e.target.value] || 5432);
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
                        value={manualFormData.instance_name}
                        onChange={(e) => handleManualChange('instance_name', e.target.value)}
                        className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent text-foreground"
                        placeholder="my-instance"
                      />
                      <p className="text-xs text-muted-foreground mt-1">Leave empty to auto-create</p>
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
                        value={manualFormData.host}
                        onChange={(e) => handleManualChange('host', e.target.value)}
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
                        value={manualFormData.port}
                        onChange={(e) => handleManualChange('port', parseInt(e.target.value))}
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
                        value={manualFormData.username}
                        onChange={(e) => handleManualChange('username', e.target.value)}
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
                        value={manualFormData.password}
                        onChange={(e) => handleManualChange('password', e.target.value)}
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
                      value={manualFormData.db_name}
                      onChange={(e) => handleManualChange('db_name', e.target.value)}
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
                        checked={manualFormData.ssl}
                        onChange={(e) => {
                          handleManualChange('ssl', e.target.checked);
                          if (e.target.checked) {
                            handleManualChange('ssl_mode', 'require');
                          } else {
                            handleManualChange('ssl_mode', 'disable');
                          }
                        }}
                        className="rounded border-input"
                      />
                      <span className="text-sm text-foreground">Enable SSL</span>
                    </label>

                    {manualFormData.ssl && (
                      <select
                        value={manualFormData.ssl_mode}
                        onChange={(e) => handleManualChange('ssl_mode', e.target.value)}
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
                      checked={manualFormData.enabled}
                      onChange={(e) => handleManualChange('enabled', e.target.checked)}
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
                    disabled={!manualFormData.database_name}
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
        )}
      </div>
    </div>
  );
}

