'use client';

import { useState } from 'react';
import { useDisconnectStream } from '@/lib/hooks/useStreams';
import { useToast } from '@/components/ui/Toast';
import { X, AlertTriangle } from 'lucide-react';

interface DisconnectStreamDialogProps {
  workspaceName: string;
  streamName: string;
  onClose: () => void;
  onSuccess?: () => void;
}

export function DisconnectStreamDialog({ 
  workspaceName, 
  streamName, 
  onClose, 
  onSuccess 
}: DisconnectStreamDialogProps) {
  const { disconnect, isLoading } = useDisconnectStream(workspaceName, streamName);
  const { showToast } = useToast();
  const [deleteStream, setDeleteStream] = useState(false);

  const handleDisconnect = async () => {
    try {
      await disconnect({ delete_stream: deleteStream });

      showToast({
        type: 'success',
        title: 'Stream Disconnected',
        message: `Successfully disconnected ${streamName}`,
      });

      if (onSuccess) {
        onSuccess();
      }
      onClose();
    } catch (error) {
      showToast({
        type: 'error',
        title: 'Disconnection Failed',
        message: error instanceof Error ? error.message : 'Failed to disconnect stream',
      });
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="bg-card border border-border rounded-lg shadow-lg w-full max-w-md">
        <div className="px-6 py-4 border-b border-border flex items-center justify-between">
          <h2 className="text-xl font-semibold text-foreground">Disconnect Stream</h2>
          <button
            onClick={onClose}
            className="text-muted-foreground hover:text-foreground transition-colors"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="p-6 space-y-4">
          <div className="flex items-start space-x-3 p-4 bg-destructive/10 border border-destructive/20 rounded-lg">
            <AlertTriangle className="h-5 w-5 text-destructive flex-shrink-0 mt-0.5" />
            <div className="flex-1">
              <p className="text-sm font-medium text-foreground">
                Are you sure you want to disconnect this stream?
              </p>
              <p className="text-sm text-muted-foreground mt-1">
                This will stop all message consumption and production for <strong>{streamName}</strong>.
              </p>
            </div>
          </div>

          <div className="flex items-start space-x-2">
            <input
              type="checkbox"
              id="delete-metadata"
              checked={deleteStream}
              onChange={(e) => setDeleteStream(e.target.checked)}
              className="mt-1"
            />
            <label htmlFor="delete-metadata" className="text-sm text-foreground">
              <span className="font-medium">Delete stream metadata</span>
              <span className="block text-muted-foreground mt-0.5">
                This will permanently remove the stream configuration. You will need to reconnect from scratch.
              </span>
            </label>
          </div>

          <div className="flex items-center justify-end space-x-3 pt-4">
            <button
              onClick={onClose}
              className="px-4 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
            >
              Cancel
            </button>
            <button
              onClick={handleDisconnect}
              disabled={isLoading}
              className="px-4 py-2 bg-destructive text-destructive-foreground rounded-md hover:bg-destructive/90 transition-colors disabled:opacity-50"
            >
              {isLoading ? 'Disconnecting...' : 'Disconnect'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

