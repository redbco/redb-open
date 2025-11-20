'use client';

import { useState } from 'react';
import { X } from 'lucide-react';
import { api } from '@/lib/api/endpoints';
import { useToast } from '@/components/ui/Toast';

interface AddRepositoryDialogProps {
  workspaceId: string;
  onClose: () => void;
  onSuccess: () => void;
}

export function AddRepositoryDialog({ workspaceId, onClose, onSuccess }: AddRepositoryDialogProps) {
  const { showToast } = useToast();
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [formData, setFormData] = useState({
    repo_name: '',
    repo_description: '',
    repo_type: 'git',
  });

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!formData.repo_name.trim()) {
      showToast({ type: 'error', title: 'Validation Error', message: 'Repository name is required' });
      return;
    }

    setIsSubmitting(true);
    try {
      await api.repositories.add(workspaceId, {
        repo_name: formData.repo_name.trim(),
        repo_description: formData.repo_description.trim() || undefined,
        repo_type: formData.repo_type,
      });

      showToast({ type: 'success', title: 'Success', message: 'Repository created successfully' });
      onSuccess();
      onClose();
    } catch (error) {
      console.error('Error creating repository:', error);
      showToast({ type: 'error', title: 'Error', message: 'Failed to create repository' });
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-background/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
      <div className="bg-card border border-border rounded-lg shadow-lg max-w-md w-full p-6">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-xl font-semibold text-foreground">Add Repository</h2>
          <button
            onClick={onClose}
            className="text-muted-foreground hover:text-foreground"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Repository Name <span className="text-destructive">*</span>
            </label>
            <input
              type="text"
              value={formData.repo_name}
              onChange={(e) => setFormData({ ...formData, repo_name: e.target.value })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              placeholder="my-repository"
              required
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Description
            </label>
            <textarea
              value={formData.repo_description}
              onChange={(e) => setFormData({ ...formData, repo_description: e.target.value })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              placeholder="Repository description"
              rows={3}
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Type
            </label>
            <select
              value={formData.repo_type}
              onChange={(e) => setFormData({ ...formData, repo_type: e.target.value })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
            >
              <option value="git">Git</option>
              <option value="schema">Schema</option>
            </select>
          </div>

          {/* Actions */}
          <div className="flex justify-end space-x-3 pt-4">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-sm font-medium text-foreground bg-background border border-border rounded-md hover:bg-accent"
              disabled={isSubmitting}
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-4 py-2 text-sm font-medium text-primary-foreground bg-primary rounded-md hover:bg-primary/90 disabled:opacity-50"
              disabled={isSubmitting}
            >
              {isSubmitting ? 'Creating...' : 'Create Repository'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

