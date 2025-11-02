'use client';

import { useState } from 'react';
import { User } from '@/lib/api/types';
import {
  User as UserIcon,
  Mail,
  Shield,
  MoreVertical,
  Settings,
  Trash2,
  Calendar,
  CheckCircle2,
  XCircle,
} from 'lucide-react';

interface UserCardProps {
  user: User;
  onUpdate: () => void;
}

export function UserCard({ user, onUpdate }: UserCardProps) {
  const [showMenu, setShowMenu] = useState(false);

  const formatDate = (dateString?: string) => {
    if (!dateString) return 'N/A';
    try {
      return new Date(dateString).toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric',
        year: 'numeric',
      });
    } catch {
      return dateString;
    }
  };

  return (
    <div className="bg-card border border-border rounded-lg p-6 hover:border-primary/50 transition-all duration-200 hover:shadow-lg">
      {/* Header */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center space-x-3">
          <div className="w-12 h-12 bg-muted rounded-full flex items-center justify-center">
            <UserIcon className="h-6 w-6 text-muted-foreground" />
          </div>
          <div>
            <h3 className="text-lg font-semibold text-foreground flex items-center">
              {user.user_name}
              {user.user_enabled ? (
                <CheckCircle2 className="ml-2 h-4 w-4 text-green-500" />
              ) : (
                <XCircle className="ml-2 h-4 w-4 text-red-500" />
              )}
            </h3>
            {(user.user_first_name || user.user_last_name) && (
              <p className="text-sm text-muted-foreground">
                {[user.user_first_name, user.user_last_name].filter(Boolean).join(' ')}
              </p>
            )}
          </div>
        </div>
        
        <div className="relative">
          <button
            onClick={() => setShowMenu(!showMenu)}
            className="p-1 rounded-md hover:bg-accent text-muted-foreground hover:text-foreground"
          >
            <MoreVertical className="h-5 w-5" />
          </button>
          
          {showMenu && (
            <>
              <div
                className="fixed inset-0 z-10"
                onClick={() => setShowMenu(false)}
              />
              <div className="absolute right-0 mt-2 w-48 bg-popover border border-border rounded-md shadow-lg z-20 py-1">
                <button
                  className="flex items-center w-full px-4 py-2 text-sm text-popover-foreground hover:bg-accent"
                  onClick={() => {
                    setShowMenu(false);
                    // TODO: Implement modify
                  }}
                >
                  <Settings className="h-4 w-4 mr-2" />
                  Modify
                </button>
                <div className="border-t border-border my-1" />
                <button
                  className="flex items-center w-full px-4 py-2 text-sm text-destructive hover:bg-accent"
                  onClick={() => {
                    setShowMenu(false);
                    // TODO: Implement delete
                  }}
                >
                  <Trash2 className="h-4 w-4 mr-2" />
                  Delete
                </button>
              </div>
            </>
          )}
        </div>
      </div>

      {/* Info */}
      <div className="space-y-2 mb-4">
        <div className="flex items-center text-sm">
          <Mail className="h-4 w-4 mr-2 text-muted-foreground" />
          <span className="text-foreground">{user.user_email}</span>
        </div>
        {user.user_role && (
          <div className="flex items-center text-sm">
            <Shield className="h-4 w-4 mr-2 text-muted-foreground" />
            <span className="text-foreground capitalize">{user.user_role}</span>
          </div>
        )}
        <div className="flex items-center text-sm">
          <Calendar className="h-4 w-4 mr-2 text-muted-foreground" />
          <span className="text-muted-foreground">
            Created {formatDate(user.created)}
          </span>
        </div>
      </div>

      {/* Status and Workspaces */}
      <div className="flex items-center justify-between pt-4 border-t border-border">
        <div className="text-sm text-muted-foreground">
          {user.workspace_ids?.length || 0} workspace{(user.workspace_ids?.length || 0) !== 1 ? 's' : ''}
        </div>
        <span
          className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
            user.user_enabled
              ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400'
              : 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400'
          }`}
        >
          {user.user_enabled ? 'Enabled' : 'Disabled'}
        </span>
      </div>
    </div>
  );
}

