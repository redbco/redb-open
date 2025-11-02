'use client';

import { useState } from 'react';
import { Environment } from '@/lib/api/types';
import {
  Layers,
  Eye,
  MoreVertical,
  Settings,
  Trash2,
  Calendar,
  AlertCircle,
  CheckCircle2,
} from 'lucide-react';

interface EnvironmentCardProps {
  environment: Environment;
  workspaceId: string;
  onUpdate: () => void;
}

export function EnvironmentCard({ environment, workspaceId, onUpdate }: EnvironmentCardProps) {
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

  const getCriticalityColor = (criticality?: number) => {
    if (!criticality) return 'bg-gray-500';
    if (criticality >= 8) return 'bg-red-500';
    if (criticality >= 5) return 'bg-yellow-500';
    return 'bg-green-500';
  };

  return (
    <div className="bg-card border border-border rounded-lg p-6 hover:border-primary/50 transition-all duration-200 hover:shadow-lg">
      {/* Header */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center space-x-3">
          <div className={`p-2 rounded-lg ${environment.environment_production ? 'bg-red-100 dark:bg-red-900/30' : 'bg-primary/10'}`}>
            <Layers className={`h-6 w-6 ${environment.environment_production ? 'text-red-600 dark:text-red-400' : 'text-primary'}`} />
          </div>
          <div>
            <h3 className="text-lg font-semibold text-foreground flex items-center">
              {environment.environment_name}
              {environment.environment_production && (
                <span className="ml-2 px-2 py-0.5 text-xs font-medium bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400 rounded">
                  PRODUCTION
                </span>
              )}
            </h3>
            {environment.environment_criticality !== undefined && (
              <div className="flex items-center mt-1">
                <span className={`w-2 h-2 rounded-full mr-2 ${getCriticalityColor(environment.environment_criticality)}`} />
                <span className="text-sm text-muted-foreground">
                  Criticality: {environment.environment_criticality}/10
                </span>
              </div>
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

      {/* Description */}
      {environment.environment_description && (
        <p className="text-sm text-muted-foreground mb-4 line-clamp-2">
          {environment.environment_description}
        </p>
      )}

      {/* Info */}
      <div className="space-y-2 mb-4">
        <div className="flex items-center text-sm">
          <Calendar className="h-4 w-4 mr-2 text-muted-foreground" />
          <span className="text-muted-foreground">
            Created {formatDate(environment.created)}
          </span>
        </div>
        {environment.environment_priority !== undefined && (
          <div className="flex items-center text-sm">
            <AlertCircle className="h-4 w-4 mr-2 text-muted-foreground" />
            <span className="text-muted-foreground">
              Priority: {environment.environment_priority}
            </span>
          </div>
        )}
      </div>

      {/* Resource Counts */}
      <div className="flex items-center justify-between pt-4 border-t border-border">
        <div className="flex items-center space-x-4 text-sm text-muted-foreground">
          <span>{environment.instance_count || 0} instances</span>
          <span>{environment.database_count || 0} databases</span>
        </div>
        <span
          className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
            environment.status === 'active'
              ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400'
              : 'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-400'
          }`}
        >
          {environment.status || 'Active'}
        </span>
      </div>
    </div>
  );
}

