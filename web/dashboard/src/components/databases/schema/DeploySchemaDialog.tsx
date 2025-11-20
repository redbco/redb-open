'use client';

import { useState } from 'react';
import { X, GitBranch, AlertCircle } from 'lucide-react';

interface DeploySchemaDialogProps {
  databaseName: string;
  onClose: () => void;
  onDeploy: (repoName: string, branchName: string, paradigm?: string) => void;
}

export function DeploySchemaDialog({ databaseName, onClose, onDeploy }: DeploySchemaDialogProps) {
  const [repoName, setRepoName] = useState('');
  const [branchName, setBranchName] = useState('main');
  const [paradigm, setParadigm] = useState('');
  const [isDeploying, setIsDeploying] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!repoName.trim()) return;

    setIsDeploying(true);
    try {
      await onDeploy(repoName.trim(), branchName.trim(), paradigm || undefined);
      onClose();
    } catch (error) {
      console.error('Failed to deploy schema:', error);
    } finally {
      setIsDeploying(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
      <div className="bg-card border border-border rounded-lg shadow-xl max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-border">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center">
              <GitBranch className="h-5 w-5 text-primary" />
            </div>
            <div>
              <h2 className="text-xl font-semibold text-foreground">Deploy Schema to Repository</h2>
              <p className="text-sm text-muted-foreground mt-0.5">
                Deploy the schema from <strong>{databaseName}</strong> to a repository branch
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-accent rounded-md transition-colors"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} className="p-6 space-y-6">
          {/* Info Banner */}
          <div className="bg-blue-50 dark:bg-blue-900/10 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
            <div className="flex items-start gap-3">
              <AlertCircle className="h-5 w-5 text-blue-600 dark:text-blue-400 flex-shrink-0 mt-0.5" />
              <div className="flex-1">
                <p className="text-sm text-blue-900 dark:text-blue-100 font-medium">
                  Schema Deployment
                </p>
                <p className="text-sm text-blue-800 dark:text-blue-200 mt-1">
                  This will create a snapshot of the current database schema and save it to the specified repository branch.
                  If the repository doesn&apos;t exist, it will be created automatically.
                </p>
              </div>
            </div>
          </div>

          {/* Repository Name */}
          <div>
            <label htmlFor="repoName" className="block text-sm font-medium text-foreground mb-2">
              Repository Name <span className="text-red-500">*</span>
            </label>
            <input
              id="repoName"
              type="text"
              value={repoName}
              onChange={(e) => setRepoName(e.target.value)}
              placeholder="e.g., my-app-schema"
              className="w-full px-3 py-2 bg-background border border-input rounded-md text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              required
            />
            <p className="text-xs text-muted-foreground mt-1">
              Enter the name of the repository to deploy to
            </p>
          </div>

          {/* Branch Name */}
          <div>
            <label htmlFor="branchName" className="block text-sm font-medium text-foreground mb-2">
              Branch Name <span className="text-red-500">*</span>
            </label>
            <input
              id="branchName"
              type="text"
              value={branchName}
              onChange={(e) => setBranchName(e.target.value)}
              placeholder="e.g., main"
              className="w-full px-3 py-2 bg-background border border-input rounded-md text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              required
            />
            <p className="text-xs text-muted-foreground mt-1">
              The branch where the schema will be saved
            </p>
          </div>

          {/* Paradigm Conversion (Optional) */}
          <div>
            <label htmlFor="paradigm" className="block text-sm font-medium text-foreground mb-2">
              Target Paradigm (Optional)
            </label>
            <select
              id="paradigm"
              value={paradigm}
              onChange={(e) => setParadigm(e.target.value)}
              className="w-full px-3 py-2 bg-background border border-input rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
            >
              <option value="">Keep Original (No Conversion)</option>
              <option value="relational">Relational</option>
              <option value="document">Document (NoSQL)</option>
              <option value="keyvalue">Key-Value</option>
              <option value="graph">Graph</option>
              <option value="timeseries">Time Series</option>
            </select>
            <p className="text-xs text-muted-foreground mt-1">
              Optionally convert the schema to a different database paradigm
            </p>
          </div>

          {/* Action Buttons */}
          <div className="flex items-center justify-end gap-3 pt-4 border-t border-border">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
              disabled={isDeploying}
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              disabled={isDeploying || !repoName.trim()}
            >
              {isDeploying ? (
                <>
                  <div className="inline-block animate-spin rounded-full h-4 w-4 border-2 border-white border-t-transparent mr-2"></div>
                  Deploying...
                </>
              ) : (
                'Deploy Schema'
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

