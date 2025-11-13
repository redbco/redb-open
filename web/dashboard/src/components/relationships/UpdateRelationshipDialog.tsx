'use client';

import { useState, useEffect } from 'react';
import { X } from 'lucide-react';
import type { Relationship } from '@/lib/api/types';

interface UpdateRelationshipDialogProps {
  isOpen: boolean;
  onClose: () => void;
  relationship: Relationship;
  onUpdate: (description: string, batchSize: number, parallelWorkers: number) => Promise<void>;
}

export function UpdateRelationshipDialog({
  isOpen,
  onClose,
  relationship,
  onUpdate,
}: UpdateRelationshipDialogProps) {
  const [description, setDescription] = useState('');
  const [batchSize, setBatchSize] = useState(1000);
  const [parallelWorkers, setParallelWorkers] = useState(4);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (isOpen && relationship) {
      setDescription(relationship.relationship_description || '');
      // Note: batch_size and parallel_workers aren't in the Relationship type yet
      // These would come from the backend once implemented
      setBatchSize(1000);
      setParallelWorkers(4);
      setError(null);
    }
  }, [isOpen, relationship]);

  if (!isOpen) return null;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);

    // Validation
    if (batchSize < 1 || batchSize > 10000) {
      setError('Batch size must be between 1 and 10,000');
      return;
    }

    if (parallelWorkers < 1 || parallelWorkers > 32) {
      setError('Parallel workers must be between 1 and 32');
      return;
    }

    setIsSubmitting(true);

    try {
      await onUpdate(description, batchSize, parallelWorkers);
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to update relationship');
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 overflow-y-auto">
      {/* Backdrop */}
      <div 
        className="fixed inset-0 bg-black/50 transition-opacity"
        onClick={onClose}
      />

      {/* Dialog */}
      <div className="flex min-h-full items-center justify-center p-4">
        <div className="relative bg-card border border-border rounded-lg shadow-lg w-full max-w-md">
          {/* Header */}
          <div className="flex items-center justify-between p-6 border-b border-border">
            <h3 className="text-lg font-semibold text-foreground">
              Update Relationship
            </h3>
            <button
              onClick={onClose}
              className="text-muted-foreground hover:text-foreground transition-colors"
              disabled={isSubmitting}
            >
              <X className="h-5 w-5" />
            </button>
          </div>

          {/* Form */}
          <form onSubmit={handleSubmit}>
            <div className="p-6 space-y-4">
              {/* Relationship Name (read-only) */}
              <div>
                <label className="block text-sm font-medium text-foreground mb-2">
                  Relationship Name
                </label>
                <input
                  type="text"
                  value={relationship.relationship_name}
                  disabled
                  className="w-full px-3 py-2 bg-muted text-muted-foreground border border-input rounded-md"
                />
              </div>

              {/* Description */}
              <div>
                <label htmlFor="description" className="block text-sm font-medium text-foreground mb-2">
                  Description
                </label>
                <textarea
                  id="description"
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                  rows={3}
                  className="w-full px-3 py-2 bg-background text-foreground border border-input rounded-md focus:outline-none focus:ring-2 focus:ring-ring"
                  placeholder="Optional description for this relationship"
                />
              </div>

              {/* Batch Size */}
              <div>
                <label htmlFor="batchSize" className="block text-sm font-medium text-foreground mb-2">
                  Batch Size
                </label>
                <input
                  id="batchSize"
                  type="number"
                  value={batchSize}
                  onChange={(e) => setBatchSize(Number(e.target.value))}
                  min={1}
                  max={10000}
                  className="w-full px-3 py-2 bg-background text-foreground border border-input rounded-md focus:outline-none focus:ring-2 focus:ring-ring"
                  disabled={isSubmitting}
                />
                <p className="mt-1 text-xs text-muted-foreground">
                  Number of records to process per batch (1-10,000)
                </p>
              </div>

              {/* Parallel Workers */}
              <div>
                <label htmlFor="parallelWorkers" className="block text-sm font-medium text-foreground mb-2">
                  Parallel Workers
                </label>
                <input
                  id="parallelWorkers"
                  type="number"
                  value={parallelWorkers}
                  onChange={(e) => setParallelWorkers(Number(e.target.value))}
                  min={1}
                  max={32}
                  className="w-full px-3 py-2 bg-background text-foreground border border-input rounded-md focus:outline-none focus:ring-2 focus:ring-ring"
                  disabled={isSubmitting}
                />
                <p className="mt-1 text-xs text-muted-foreground">
                  Number of parallel worker threads (1-32)
                </p>
              </div>

              {/* Error Message */}
              {error && (
                <div className="p-3 bg-destructive/10 border border-destructive/20 rounded-md">
                  <p className="text-sm text-destructive">{error}</p>
                </div>
              )}
            </div>

            {/* Footer */}
            <div className="flex items-center justify-end gap-3 p-6 border-t border-border">
              <button
                type="button"
                onClick={onClose}
                className="px-4 py-2 text-sm font-medium text-foreground border border-input rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
                disabled={isSubmitting}
              >
                Cancel
              </button>
              <button
                type="submit"
                className="px-4 py-2 text-sm font-medium bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                disabled={isSubmitting}
              >
                {isSubmitting ? 'Updating...' : 'Update Relationship'}
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}

