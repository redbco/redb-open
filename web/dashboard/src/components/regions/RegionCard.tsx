'use client';

import { useState } from 'react';
import { Region } from '@/lib/api/types';
import {
  MapPin,
  Eye,
  MoreVertical,
  Settings,
  Trash2,
  Calendar,
  Server,
  Database,
} from 'lucide-react';

interface RegionCardProps {
  region: Region;
  onUpdate: () => void;
}

export function RegionCard({ region, onUpdate }: RegionCardProps) {
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

  const getRegionTypeColor = (type: string) => {
    switch (type.toLowerCase()) {
      case 'aws':
        return 'bg-orange-100 text-orange-800 dark:bg-orange-900/30 dark:text-orange-400';
      case 'azure':
        return 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400';
      case 'gcp':
        return 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400';
      case 'on-premise':
        return 'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-400';
      default:
        return 'bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-400';
    }
  };

  return (
    <div className="bg-card border border-border rounded-lg p-6 hover:border-primary/50 transition-all duration-200 hover:shadow-lg">
      {/* Header */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center space-x-3">
          <div className="p-2 bg-primary/10 rounded-lg">
            <MapPin className="h-6 w-6 text-primary" />
          </div>
          <div>
            <h3 className="text-lg font-semibold text-foreground">
              {region.region_name}
            </h3>
            <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${getRegionTypeColor(region.region_type)}`}>
              {region.region_type.toUpperCase()}
            </span>
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
      {region.region_description && (
        <p className="text-sm text-muted-foreground mb-4 line-clamp-2">
          {region.region_description}
        </p>
      )}

      {/* Location Info */}
      {region.region_location && (
        <div className="mb-4">
          <p className="text-sm text-foreground">
            📍 {region.region_location}
          </p>
        </div>
      )}

      {/* Info */}
      <div className="space-y-2 mb-4">
        <div className="flex items-center text-sm">
          <Calendar className="h-4 w-4 mr-2 text-muted-foreground" />
          <span className="text-muted-foreground">
            Created {formatDate(region.created)}
          </span>
        </div>
      </div>

      {/* Resource Counts */}
      <div className="flex items-center justify-between pt-4 border-t border-border">
        <div className="flex items-center space-x-4 text-sm text-muted-foreground">
          <div className="flex items-center">
            <Server className="h-4 w-4 mr-1" />
            <span>{region.node_count || 0} nodes</span>
          </div>
          <div className="flex items-center">
            <Server className="h-4 w-4 mr-1" />
            <span>{region.instance_count || 0} instances</span>
          </div>
          <div className="flex items-center">
            <Database className="h-4 w-4 mr-1" />
            <span>{region.database_count || 0} databases</span>
          </div>
        </div>
      </div>
    </div>
  );
}

