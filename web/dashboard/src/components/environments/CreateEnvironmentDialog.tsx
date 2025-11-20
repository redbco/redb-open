'use client';

import { useState } from 'react';
import { X } from 'lucide-react';
import { api } from '@/lib/api/endpoints';
import { useToast } from '@/components/ui/Toast';

interface CreateEnvironmentDialogProps {
  workspaceId: string;
  onClose: () => void;
  onSuccess: () => void;
}

export function CreateEnvironmentDialog({ workspaceId, onClose, onSuccess }: CreateEnvironmentDialogProps) {
  const { showToast } = useToast();
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [formData, setFormData] = useState({
    environment_name: '',
    environment_description: '',
    environment_production: false,
    environment_criticality: 5,
    environment_priority: 5,
  });

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!formData.environment_name.trim()) {
      showToast({ type: 'error', title: 'Validation Error', message: 'Environment name is required' });
      return;
    }

    setIsSubmitting(true);
    try {
      await api.environments.create(workspaceId, {
        environment_name: formData.environment_name.trim(),
        environment_description: formData.environment_description.trim() || undefined,
        environment_production: formData.environment_production,
        environment_criticality: formData.environment_criticality,
        environment_priority: formData.environment_priority,
      });

      showToast({ type: 'success', title: 'Success', message: 'Environment created successfully' });
      onSuccess();
      onClose();
    } catch (error) {
      console.error('Error creating environment:', error);
      showToast({ type: 'error', title: 'Error', message: 'Failed to create environment' });
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-background/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
      <div className="bg-card border border-border rounded-lg shadow-lg max-w-md w-full p-6">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-xl font-semibold text-foreground">Create Environment</h2>
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
              Environment Name <span className="text-destructive">*</span>
            </label>
            <input
              type="text"
              value={formData.environment_name}
              onChange={(e) => setFormData({ ...formData, environment_name: e.target.value })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              placeholder="production"
              required
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Description
            </label>
            <textarea
              value={formData.environment_description}
              onChange={(e) => setFormData({ ...formData, environment_description: e.target.value })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              placeholder="Production environment"
              rows={3}
            />
          </div>

          <div className="flex items-center space-x-2">
            <input
              type="checkbox"
              id="production"
              checked={formData.environment_production}
              onChange={(e) => setFormData({ ...formData, environment_production: e.target.checked })}
              className="w-4 h-4 text-primary bg-background border-border rounded focus:ring-2 focus:ring-primary"
            />
            <label htmlFor="production" className="text-sm font-medium text-foreground">
              Production Environment
            </label>
          </div>

          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Criticality (1-10)
            </label>
            <input
              type="number"
              min="1"
              max="10"
              value={formData.environment_criticality}
              onChange={(e) => setFormData({ ...formData, environment_criticality: parseInt(e.target.value) || 5 })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Priority (1-10)
            </label>
            <input
              type="number"
              min="1"
              max="10"
              value={formData.environment_priority}
              onChange={(e) => setFormData({ ...formData, environment_priority: parseInt(e.target.value) || 5 })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
            />
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
              {isSubmitting ? 'Creating...' : 'Create Environment'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

