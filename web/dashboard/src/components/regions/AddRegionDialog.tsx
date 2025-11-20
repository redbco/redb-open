'use client';

import { useState } from 'react';
import { X } from 'lucide-react';
import { api } from '@/lib/api/endpoints';
import { useToast } from '@/components/ui/Toast';

interface AddRegionDialogProps {
  onClose: () => void;
  onSuccess: () => void;
}

export function AddRegionDialog({ onClose, onSuccess }: AddRegionDialogProps) {
  const { showToast } = useToast();
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [formData, setFormData] = useState({
    region_name: '',
    region_description: '',
    region_type: 'on-premise' as 'aws' | 'azure' | 'gcp' | 'on-premise',
    region_location: '',
    region_latitude: '',
    region_longitude: '',
  });

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!formData.region_name.trim()) {
      showToast({ type: 'error', title: 'Validation Error', message: 'Region name is required' });
      return;
    }

    setIsSubmitting(true);
    try {
      await api.regions.create({
        region_name: formData.region_name.trim(),
        region_description: formData.region_description.trim() || undefined,
        region_type: formData.region_type,
        region_location: formData.region_location.trim() || undefined,
        region_latitude: formData.region_latitude ? parseFloat(formData.region_latitude) : undefined,
        region_longitude: formData.region_longitude ? parseFloat(formData.region_longitude) : undefined,
      });

      showToast({ type: 'success', title: 'Success', message: 'Region created successfully' });
      onSuccess();
      onClose();
    } catch (error) {
      console.error('Error creating region:', error);
      showToast({ type: 'error', title: 'Error', message: 'Failed to create region' });
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-background/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
      <div className="bg-card border border-border rounded-lg shadow-lg max-w-md w-full p-6">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-xl font-semibold text-foreground">Add Region</h2>
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
              Region Name <span className="text-destructive">*</span>
            </label>
            <input
              type="text"
              value={formData.region_name}
              onChange={(e) => setFormData({ ...formData, region_name: e.target.value })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              placeholder="us-east-1"
              required
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Type <span className="text-destructive">*</span>
            </label>
            <select
              value={formData.region_type}
              onChange={(e) => setFormData({ ...formData, region_type: e.target.value as any })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              required
            >
              <option value="on-premise">On-Premise</option>
              <option value="aws">AWS</option>
              <option value="azure">Azure</option>
              <option value="gcp">Google Cloud</option>
            </select>
          </div>

          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Description
            </label>
            <textarea
              value={formData.region_description}
              onChange={(e) => setFormData({ ...formData, region_description: e.target.value })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              placeholder="Region description"
              rows={3}
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Location
            </label>
            <input
              type="text"
              value={formData.region_location}
              onChange={(e) => setFormData({ ...formData, region_location: e.target.value })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              placeholder="US East (N. Virginia)"
            />
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-foreground mb-2">
                Latitude
              </label>
              <input
                type="number"
                step="any"
                value={formData.region_latitude}
                onChange={(e) => setFormData({ ...formData, region_latitude: e.target.value })}
                className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
                placeholder="37.7749"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-foreground mb-2">
                Longitude
              </label>
              <input
                type="number"
                step="any"
                value={formData.region_longitude}
                onChange={(e) => setFormData({ ...formData, region_longitude: e.target.value })}
                className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
                placeholder="-122.4194"
              />
            </div>
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
              {isSubmitting ? 'Creating...' : 'Create Region'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

