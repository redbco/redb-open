'use client';

import { useState } from 'react';
import { X } from 'lucide-react';
import { api } from '@/lib/api/endpoints';
import { useToast } from '@/components/ui/Toast';
import { Transformation } from '@/lib/api/types';

interface AddMappingRuleDialogProps {
  workspaceId: string;
  mappingName: string;
  transformations: Transformation[];
  onClose: () => void;
  onSuccess: () => void;
}

export function AddMappingRuleDialog({ 
  workspaceId, 
  mappingName, 
  transformations,
  onClose, 
  onSuccess 
}: AddMappingRuleDialogProps) {
  const { showToast } = useToast();
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [formData, setFormData] = useState({
    rule_name: '',
    source: '',
    target: '',
    transformation: 'direct_mapping',
    order: '',
  });

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!formData.rule_name.trim() || !formData.source.trim() || !formData.target.trim()) {
      showToast({ type: 'error', title: 'Validation Error', message: 'Rule name, source, and target are required' });
      return;
    }

    setIsSubmitting(true);
    try {
      await api.mappingRules.add(workspaceId, mappingName, {
        rule_name: formData.rule_name.trim(),
        source: formData.source.trim(),
        target: formData.target.trim(),
        transformation: formData.transformation,
        order: formData.order ? parseInt(formData.order) : undefined,
      });

      showToast({ type: 'success', title: 'Success', message: 'Mapping rule added successfully' });
      onSuccess();
      onClose();
    } catch (error) {
      console.error('Error adding mapping rule:', error);
      showToast({ type: 'error', title: 'Error', message: 'Failed to add mapping rule' });
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-background/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
      <div className="bg-card border border-border rounded-lg shadow-lg max-w-md w-full p-6">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-xl font-semibold text-foreground">Add Mapping Rule</h2>
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
              Rule Name <span className="text-destructive">*</span>
            </label>
            <input
              type="text"
              value={formData.rule_name}
              onChange={(e) => setFormData({ ...formData, rule_name: e.target.value })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              placeholder="user_id_rule"
              required
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Source <span className="text-destructive">*</span>
            </label>
            <input
              type="text"
              value={formData.source}
              onChange={(e) => setFormData({ ...formData, source: e.target.value })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              placeholder="sourcedb.users.user_id"
              required
            />
            <p className="text-xs text-muted-foreground mt-1">Format: database.table.column</p>
          </div>

          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Target <span className="text-destructive">*</span>
            </label>
            <input
              type="text"
              value={formData.target}
              onChange={(e) => setFormData({ ...formData, target: e.target.value })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              placeholder="targetdb.profiles.profile_id"
              required
            />
            <p className="text-xs text-muted-foreground mt-1">Format: database.table.column</p>
          </div>

          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Transformation
            </label>
            <select
              value={formData.transformation}
              onChange={(e) => setFormData({ ...formData, transformation: e.target.value })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
            >
              <option value="direct_mapping">Direct Mapping</option>
              {transformations.map((t) => (
                <option key={t.transformation_id} value={t.transformation_name}>
                  {t.transformation_name}
                </option>
              ))}
            </select>
          </div>

          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Order (optional)
            </label>
            <input
              type="number"
              value={formData.order}
              onChange={(e) => setFormData({ ...formData, order: e.target.value })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              placeholder="Auto-assigned if not specified"
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
              {isSubmitting ? 'Adding...' : 'Add Rule'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

