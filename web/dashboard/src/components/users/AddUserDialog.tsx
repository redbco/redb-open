'use client';

import { useState } from 'react';
import { X } from 'lucide-react';
import { api } from '@/lib/api/endpoints';
import { useToast } from '@/components/ui/Toast';

interface AddUserDialogProps {
  onClose: () => void;
  onSuccess: () => void;
}

export function AddUserDialog({ onClose, onSuccess }: AddUserDialogProps) {
  const { showToast } = useToast();
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [formData, setFormData] = useState({
    user_name: '',
    user_email: '',
    user_password: '',
    confirm_password: '',
    user_first_name: '',
    user_last_name: '',
    user_role: 'user',
    user_enabled: true,
  });

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!formData.user_name.trim() || !formData.user_email.trim() || !formData.user_password) {
      showToast({ type: 'error', title: 'Validation Error', message: 'Username, email, and password are required' });
      return;
    }

    if (formData.user_password !== formData.confirm_password) {
      showToast({ type: 'error', title: 'Validation Error', message: 'Passwords do not match' });
      return;
    }

    if (formData.user_password.length < 8) {
      showToast({ type: 'error', title: 'Validation Error', message: 'Password must be at least 8 characters' });
      return;
    }

    setIsSubmitting(true);
    try {
      await api.users.add({
        user_name: formData.user_name.trim(),
        user_email: formData.user_email.trim(),
        user_password: formData.user_password,
        user_first_name: formData.user_first_name.trim() || undefined,
        user_last_name: formData.user_last_name.trim() || undefined,
        user_role: formData.user_role,
        user_enabled: formData.user_enabled,
      });

      showToast({ type: 'success', title: 'Success', message: 'User created successfully' });
      onSuccess();
      onClose();
    } catch (error) {
      console.error('Error creating user:', error);
      showToast({ type: 'error', title: 'Error', message: 'Failed to create user' });
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-background/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
      <div className="bg-card border border-border rounded-lg shadow-lg max-w-md w-full p-6 max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-xl font-semibold text-foreground">Add User</h2>
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
              Username <span className="text-destructive">*</span>
            </label>
            <input
              type="text"
              value={formData.user_name}
              onChange={(e) => setFormData({ ...formData, user_name: e.target.value })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              placeholder="johndoe"
              required
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Email <span className="text-destructive">*</span>
            </label>
            <input
              type="email"
              value={formData.user_email}
              onChange={(e) => setFormData({ ...formData, user_email: e.target.value })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              placeholder="john@example.com"
              required
            />
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-foreground mb-2">
                First Name
              </label>
              <input
                type="text"
                value={formData.user_first_name}
                onChange={(e) => setFormData({ ...formData, user_first_name: e.target.value })}
                className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
                placeholder="John"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-foreground mb-2">
                Last Name
              </label>
              <input
                type="text"
                value={formData.user_last_name}
                onChange={(e) => setFormData({ ...formData, user_last_name: e.target.value })}
                className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
                placeholder="Doe"
              />
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Password <span className="text-destructive">*</span>
            </label>
            <input
              type="password"
              value={formData.user_password}
              onChange={(e) => setFormData({ ...formData, user_password: e.target.value })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              placeholder="••••••••"
              required
              minLength={8}
            />
            <p className="text-xs text-muted-foreground mt-1">Minimum 8 characters</p>
          </div>

          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Confirm Password <span className="text-destructive">*</span>
            </label>
            <input
              type="password"
              value={formData.confirm_password}
              onChange={(e) => setFormData({ ...formData, confirm_password: e.target.value })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              placeholder="••••••••"
              required
              minLength={8}
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Role
            </label>
            <select
              value={formData.user_role}
              onChange={(e) => setFormData({ ...formData, user_role: e.target.value })}
              className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
            >
              <option value="user">User</option>
              <option value="admin">Admin</option>
              <option value="developer">Developer</option>
            </select>
          </div>

          <div className="flex items-center space-x-2">
            <input
              type="checkbox"
              id="enabled"
              checked={formData.user_enabled}
              onChange={(e) => setFormData({ ...formData, user_enabled: e.target.checked })}
              className="w-4 h-4 text-primary bg-background border-border rounded focus:ring-2 focus:ring-primary"
            />
            <label htmlFor="enabled" className="text-sm font-medium text-foreground">
              Enable User
            </label>
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
              {isSubmitting ? 'Creating...' : 'Create User'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

