'use client';

import { useState, useEffect } from 'react';
import { api } from '@/lib/api/endpoints';
import { useToast } from '@/components/ui/Toast';
import type { Database, DatabaseDisconnectMetadata } from '@/lib/api/types';
import { X, AlertTriangle, Loader2 } from 'lucide-react';

interface DisconnectDatabaseDialogProps {
  database: Database;
  workspaceName: string;
  onClose: () => void;
  onSuccess: () => void;
}

export function DisconnectDatabaseDialog({
  database,
  workspaceName,
  onClose,
  onSuccess,
}: DisconnectDatabaseDialogProps) {
  const { showToast } = useToast();
  const [isLoadingMetadata, setIsLoadingMetadata] = useState(true);
  const [isDisconnecting, setIsDisconnecting] = useState(false);
  const [metadata, setMetadata] = useState<DatabaseDisconnectMetadata | null>(null);
  const [metadataError, setMetadataError] = useState<string | null>(null);

  // Form state
  const [deleteDatabaseObject, setDeleteDatabaseObject] = useState(false);
  const [disconnectInstance, setDisconnectInstance] = useState(false);
  const [deleteBranch, setDeleteBranch] = useState(false);
  const [deleteRepo, setDeleteRepo] = useState(false);

  // Fetch metadata on mount
  useEffect(() => {
    const fetchMetadata = async () => {
      try {
        setIsLoadingMetadata(true);
        const response = await api.databases.getDisconnectMetadata(workspaceName, database.database_name);
        if (response.success) {
          setMetadata(response.metadata);
          setMetadataError(null);
        } else {
          setMetadataError(response.message || 'Failed to fetch disconnect metadata');
        }
      } catch (error: any) {
        setMetadataError(error.message || 'Failed to fetch disconnect metadata');
      } finally {
        setIsLoadingMetadata(false);
      }
    };

    fetchMetadata();
  }, [workspaceName, database.database_name]);

  const handleDisconnect = async () => {
    try {
      setIsDisconnecting(true);
      
      const request = {
        delete_database_object: deleteDatabaseObject,
        disconnect_instance: disconnectInstance,
        delete_branch: deleteBranch,
        delete_repo: deleteRepo,
      };

      const response = await api.databases.disconnect(workspaceName, database.database_name, request);
      
      if (response.success) {
        showToast({
          type: 'success',
          title: 'Database disconnected successfully',
          message: response.message,
        });
        onSuccess();
        onClose();
      } else {
        showToast({
          type: 'error',
          title: 'Failed to disconnect database',
          message: response.message || 'An error occurred',
        });
      }
    } catch (error: any) {
      showToast({
        type: 'error',
        title: 'Failed to disconnect database',
        message: error.message || 'An unexpected error occurred',
      });
    } finally {
      setIsDisconnecting(false);
    }
  };

  // Compute option availability
  const canDeleteBranch = metadata?.can_delete_branch_only && !metadata?.should_delete_repo;
  const canDeleteRepo = metadata?.can_delete_entire_repo && !metadata?.should_delete_branch;
  const canDisconnectInstance = metadata?.is_last_database_in_instance;

  // Ensure mutual exclusivity between delete_branch and delete_repo
  useEffect(() => {
    if (deleteBranch && deleteRepo) {
      // If both are checked, uncheck the one that wasn't just clicked
      // Since we can't track which was clicked, prefer branch deletion
      setDeleteRepo(false);
    }
  }, [deleteBranch, deleteRepo]);

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
      <div className="bg-card border border-border rounded-lg shadow-xl max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-start justify-between p-6 border-b border-border">
          <div>
            <h2 className="text-2xl font-bold text-foreground flex items-center gap-2">
              <AlertTriangle className="h-6 w-6 text-yellow-500" />
              Disconnect Database
            </h2>
            <p className="text-sm text-muted-foreground mt-1">
              Disconnect "{database.database_name}" from the system
            </p>
          </div>
          <button
            onClick={onClose}
            className="text-muted-foreground hover:text-foreground transition-colors"
            disabled={isDisconnecting}
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Content */}
        <div className="p-6 space-y-6">
          {/* Loading State */}
          {isLoadingMetadata && (
            <div className="flex items-center justify-center py-8">
              <Loader2 className="h-8 w-8 animate-spin text-primary" />
              <span className="ml-3 text-muted-foreground">Loading disconnect options...</span>
            </div>
          )}

          {/* Error State */}
          {metadataError && (
            <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-4">
              <p className="text-red-500 text-sm">{metadataError}</p>
            </div>
          )}

          {/* Metadata Loaded */}
          {!isLoadingMetadata && !metadataError && metadata && (
            <>
              {/* Warning Message */}
              <div className="bg-yellow-500/10 border border-yellow-500/20 rounded-lg p-4">
                <p className="text-yellow-600 dark:text-yellow-500 text-sm font-medium">
                  ⚠️ Warning: This action will disconnect the database from the system.
                </p>
                <p className="text-yellow-600/80 dark:text-yellow-500/80 text-sm mt-1">
                  The connection will be closed, but the actual database on the server will remain intact.
                </p>
              </div>

              {/* Database Information */}
              <div className="bg-muted/50 rounded-lg p-4 space-y-2">
                <div className="flex justify-between text-sm">
                  <span className="text-muted-foreground">Database:</span>
                  <span className="font-medium text-foreground">{metadata.database_name}</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-muted-foreground">Instance:</span>
                  <span className="font-medium text-foreground">{metadata.instance_name}</span>
                </div>
                {metadata.has_attached_branch && (
                  <>
                    <div className="flex justify-between text-sm">
                      <span className="text-muted-foreground">Repository:</span>
                      <span className="font-medium text-foreground">{metadata.attached_repo_name}</span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-muted-foreground">Branch:</span>
                      <span className="font-medium text-foreground">{metadata.attached_branch_name}</span>
                    </div>
                  </>
                )}
              </div>

              {/* Disconnect Options */}
              <div className="space-y-4">
                <h3 className="font-semibold text-foreground">Disconnect Options</h3>

                {/* Delete Database Object */}
                <label className="flex items-start space-x-3 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={deleteDatabaseObject}
                    onChange={(e) => setDeleteDatabaseObject(e.target.checked)}
                    className="mt-1 h-4 w-4 rounded border-gray-300 text-primary focus:ring-primary"
                    disabled={isDisconnecting}
                  />
                  <div className="flex-1">
                    <div className="font-medium text-foreground">Delete database object</div>
                    <div className="text-sm text-muted-foreground">
                      Remove the database record from the system completely
                    </div>
                  </div>
                </label>

                {/* Disconnect Instance */}
                <label className={`flex items-start space-x-3 ${canDisconnectInstance ? 'cursor-pointer' : 'cursor-not-allowed opacity-50'}`}>
                  <input
                    type="checkbox"
                    checked={disconnectInstance}
                    onChange={(e) => setDisconnectInstance(e.target.checked)}
                    className="mt-1 h-4 w-4 rounded border-gray-300 text-primary focus:ring-primary"
                    disabled={isDisconnecting || !canDisconnectInstance}
                  />
                  <div className="flex-1">
                    <div className="font-medium text-foreground">
                      Also disconnect instance
                      {!canDisconnectInstance && (
                        <span className="ml-2 text-xs text-muted-foreground">(Not available)</span>
                      )}
                    </div>
                    <div className="text-sm text-muted-foreground">
                      {canDisconnectInstance
                        ? `This is the last database on instance "${metadata.instance_name}". Disconnecting it will also disconnect the instance.`
                        : `Instance "${metadata.instance_name}" has ${metadata.total_databases_in_instance} database(s). It cannot be disconnected while other databases are connected.`}
                    </div>
                  </div>
                </label>

                {/* Repository/Branch Options */}
                {metadata.has_attached_branch && (
                  <>
                    {/* Delete Branch */}
                    <label className={`flex items-start space-x-3 ${canDeleteBranch ? 'cursor-pointer' : 'cursor-not-allowed opacity-50'}`}>
                      <input
                        type="checkbox"
                        checked={deleteBranch}
                        onChange={(e) => setDeleteBranch(e.target.checked)}
                        className="mt-1 h-4 w-4 rounded border-gray-300 text-primary focus:ring-primary"
                        disabled={isDisconnecting || !canDeleteBranch}
                      />
                      <div className="flex-1">
                        <div className="font-medium text-foreground">
                          Delete branch "{metadata.attached_branch_name}"
                          {!canDeleteBranch && (
                            <span className="ml-2 text-xs text-muted-foreground">(Not available)</span>
                          )}
                        </div>
                        <div className="text-sm text-muted-foreground">
                          {canDeleteBranch
                            ? `Delete only this branch from repository "${metadata.attached_repo_name}"`
                            : metadata.should_delete_repo
                            ? `This is the only branch in the repository. You must delete the entire repository instead.`
                            : `Branch deletion not available for this configuration`}
                        </div>
                      </div>
                    </label>

                    {/* Delete Repository */}
                    <label className={`flex items-start space-x-3 ${canDeleteRepo ? 'cursor-pointer' : 'cursor-not-allowed opacity-50'}`}>
                      <input
                        type="checkbox"
                        checked={deleteRepo}
                        onChange={(e) => setDeleteRepo(e.target.checked)}
                        className="mt-1 h-4 w-4 rounded border-gray-300 text-primary focus:ring-primary"
                        disabled={isDisconnecting || !canDeleteRepo}
                      />
                      <div className="flex-1">
                        <div className="font-medium text-foreground">
                          Delete repository "{metadata.attached_repo_name}"
                          {!canDeleteRepo && (
                            <span className="ml-2 text-xs text-muted-foreground">(Not available)</span>
                          )}
                        </div>
                        <div className="text-sm text-muted-foreground">
                          {canDeleteRepo
                            ? metadata.is_only_branch_in_repo
                              ? `This is the only branch in the repository. Deleting the entire repository.`
                              : `Delete the entire repository including all ${metadata.total_branches_in_repo} branches`
                            : metadata.should_delete_branch
                            ? `Repository has other branches. You can only delete this specific branch.`
                            : `Repository deletion not available for this configuration`}
                        </div>
                      </div>
                    </label>
                  </>
                )}

                {!metadata.has_attached_branch && (
                  <div className="text-sm text-muted-foreground italic p-4 bg-muted/30 rounded-lg">
                    This database is not attached to any repository or branch.
                  </div>
                )}
              </div>
            </>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 p-6 border-t border-border">
          <button
            onClick={onClose}
            disabled={isDisconnecting}
            className="px-4 py-2 text-sm font-medium text-foreground bg-muted hover:bg-muted/80 rounded-md transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Cancel
          </button>
          <button
            onClick={handleDisconnect}
            disabled={isDisconnecting || isLoadingMetadata || !!metadataError}
            className="px-4 py-2 text-sm font-medium text-white bg-red-600 hover:bg-red-700 rounded-md transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
          >
            {isDisconnecting && <Loader2 className="h-4 w-4 animate-spin" />}
            Disconnect Database
          </button>
        </div>
      </div>
    </div>
  );
}

