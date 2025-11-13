'use client';

import { useState } from 'react';
import { useAuth } from '@/lib/auth/auth-context';
import { useSessions } from '@/lib/hooks/useUserProfile';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { User, Mail, Shield, Calendar, Key, Monitor, LogOut, X } from 'lucide-react';
import { api } from '@/lib/api/endpoints';

export default function ProfilePage() {
  const { profile, isLoading: profileLoading } = useAuth();
  const { sessions, isLoading: sessionsLoading, refetch } = useSessions();
  const { showToast } = useToast();
  const [showPasswordDialog, setShowPasswordDialog] = useState(false);
  const [passwordForm, setPasswordForm] = useState({
    old_password: '',
    new_password: '',
    confirm_password: '',
  });
  const [isSubmitting, setIsSubmitting] = useState(false);

  const handlePasswordChange = async (e: React.FormEvent) => {
    e.preventDefault();

    if (passwordForm.new_password !== passwordForm.confirm_password) {
      showToast({
        type: 'error',
        title: 'New passwords do not match',
      });
      return;
    }

    if (passwordForm.new_password.length < 8) {
      showToast({
        type: 'error',
        title: 'Password must be at least 8 characters',
      });
      return;
    }

    setIsSubmitting(true);
    try {
      await api.users.changePassword({
        old_password: passwordForm.old_password,
        new_password: passwordForm.new_password,
      });

      showToast({
        type: 'success',
        title: 'Password changed successfully',
      });
      setShowPasswordDialog(false);
      setPasswordForm({ old_password: '', new_password: '', confirm_password: '' });
    } catch (error) {
      console.error('Error changing password:', error);
      showToast({
        type: 'error',
        title: 'Failed to change password',
      });
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleLogoutSession = async (sessionId: string) => {
    try {
      await api.users.logoutSession(sessionId);
      showToast({
        type: 'success',
        title: 'Session logged out successfully',
      });
      refetch();
    } catch (error) {
      console.error('Error logging out session:', error);
      showToast({
        type: 'error',
        title: 'Failed to logout session',
      });
    }
  };

  const handleLogoutAll = async () => {
    if (!confirm('Are you sure you want to logout all sessions? You will be logged out from all devices.')) {
      return;
    }

    try {
      await api.users.logoutAllSessions();
      showToast({
        type: 'success',
        title: 'All sessions logged out successfully',
      });
      // This will likely log the user out, so they'll be redirected to login
    } catch (error) {
      console.error('Error logging out all sessions:', error);
      showToast({
        type: 'error',
        title: 'Failed to logout all sessions',
      });
    }
  };

  if (profileLoading) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  if (!profile) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <div className="text-center">
          <User className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
          <p className="text-muted-foreground">No profile information available</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-4xl">
      {/* Header */}
      <div>
        <h2 className="text-3xl font-bold text-foreground">User Profile</h2>
        <p className="text-muted-foreground mt-2">
          Manage your account settings and active sessions
        </p>
      </div>

      {/* Profile Information */}
      <div className="bg-card border border-border rounded-lg p-6">
        <h3 className="text-xl font-semibold text-foreground mb-4">Profile Information</h3>
        <div className="space-y-4">
          <div className="flex items-center space-x-3">
            <User className="h-5 w-5 text-muted-foreground" />
            <div>
              <p className="text-sm text-muted-foreground">Username</p>
              <p className="text-foreground font-medium">{profile.username}</p>
            </div>
          </div>
          <div className="flex items-center space-x-3">
            <Mail className="h-5 w-5 text-muted-foreground" />
            <div>
              <p className="text-sm text-muted-foreground">Email</p>
              <p className="text-foreground font-medium">{profile.email}</p>
            </div>
          </div>
          {profile.role && (
            <div className="flex items-center space-x-3">
              <Shield className="h-5 w-5 text-muted-foreground" />
              <div>
                <p className="text-sm text-muted-foreground">Role</p>
                <p className="text-foreground font-medium capitalize">{profile.role}</p>
              </div>
            </div>
          )}
          {(profile.first_name || profile.last_name) && (
            <div className="flex items-center space-x-3">
              <User className="h-5 w-5 text-muted-foreground" />
              <div>
                <p className="text-sm text-muted-foreground">Full Name</p>
                <p className="text-foreground font-medium">
                  {[profile.first_name, profile.last_name].filter(Boolean).join(' ')}
                </p>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Security */}
      <div className="bg-card border border-border rounded-lg p-6">
        <h3 className="text-xl font-semibold text-foreground mb-4">Security</h3>
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              <Key className="h-5 w-5 text-muted-foreground" />
              <div>
                <p className="text-foreground font-medium">Password</p>
                <p className="text-sm text-muted-foreground">Change your password</p>
              </div>
            </div>
            <button
              onClick={() => setShowPasswordDialog(true)}
              className="px-4 py-2 text-sm font-medium text-primary-foreground bg-primary rounded-md hover:bg-primary/90"
            >
              Change Password
            </button>
          </div>
        </div>
      </div>

      {/* Active Sessions */}
      <div className="bg-card border border-border rounded-lg p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-xl font-semibold text-foreground">Active Sessions</h3>
          <button
            onClick={handleLogoutAll}
            className="px-4 py-2 text-sm font-medium text-destructive border border-destructive rounded-md hover:bg-destructive/10"
          >
            Logout All Sessions
          </button>
        </div>
        
        {sessionsLoading ? (
          <div className="space-y-3">
            {[...Array(2)].map((_, i) => (
              <div key={i} className="animate-pulse">
                <div className="h-16 bg-muted rounded"></div>
              </div>
            ))}
          </div>
        ) : sessions.length === 0 ? (
          <p className="text-muted-foreground text-center py-8">No active sessions</p>
        ) : (
          <div className="space-y-3">
            {sessions.map((session) => (
              <div
                key={session.session_id}
                className="flex items-center justify-between p-4 bg-background border border-border rounded-md"
              >
                <div className="flex items-center space-x-3">
                  <Monitor className="h-5 w-5 text-muted-foreground" />
                  <div>
                    <div className="flex items-center space-x-2">
                      <p className="text-foreground font-medium">
                        {session.session_name || 'Unnamed Session'}
                      </p>
                      {session.is_current && (
                        <span className="px-2 py-0.5 text-xs font-medium bg-primary text-primary-foreground rounded">
                          Current
                        </span>
                      )}
                    </div>
                    <p className="text-sm text-muted-foreground">
                      {session.user_agent || 'Unknown device'} • {session.ip_address || 'Unknown IP'}
                    </p>
                    <p className="text-xs text-muted-foreground mt-1">
                      Last active: {new Date(session.last_active || session.created).toLocaleString()}
                    </p>
                  </div>
                </div>
                {!session.is_current && (
                  <button
                    onClick={() => handleLogoutSession(session.session_id)}
                    className="p-2 text-destructive hover:bg-destructive/10 rounded-md"
                  >
                    <LogOut className="h-4 w-4" />
                  </button>
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Password Change Dialog */}
      {showPasswordDialog && (
        <div className="fixed inset-0 bg-background/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
          <div className="bg-card border border-border rounded-lg shadow-lg max-w-md w-full p-6">
            <div className="flex items-center justify-between mb-6">
              <h2 className="text-xl font-semibold text-foreground">Change Password</h2>
              <button
                onClick={() => setShowPasswordDialog(false)}
                className="text-muted-foreground hover:text-foreground"
              >
                <X className="h-5 w-5" />
              </button>
            </div>

            <form onSubmit={handlePasswordChange} className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-foreground mb-2">
                  Current Password
                </label>
                <input
                  type="password"
                  value={passwordForm.old_password}
                  onChange={(e) => setPasswordForm({ ...passwordForm, old_password: e.target.value })}
                  className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
                  required
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-2">
                  New Password
                </label>
                <input
                  type="password"
                  value={passwordForm.new_password}
                  onChange={(e) => setPasswordForm({ ...passwordForm, new_password: e.target.value })}
                  className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
                  required
                  minLength={8}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-2">
                  Confirm New Password
                </label>
                <input
                  type="password"
                  value={passwordForm.confirm_password}
                  onChange={(e) => setPasswordForm({ ...passwordForm, confirm_password: e.target.value })}
                  className="w-full px-3 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
                  required
                  minLength={8}
                />
              </div>

              <div className="flex justify-end space-x-3 pt-4">
                <button
                  type="button"
                  onClick={() => setShowPasswordDialog(false)}
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
                  {isSubmitting ? 'Changing...' : 'Change Password'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}

