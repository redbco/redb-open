'use client';

import { useState } from 'react';
import { useRegions } from '@/lib/hooks/useRegions';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { MapPin, Plus, RefreshCw } from 'lucide-react';
import { RegionCard } from '@/components/regions/RegionCard';
import { AddRegionDialog } from '@/components/regions/AddRegionDialog';

export default function RegionsPage() {
  const [showAddDialog, setShowAddDialog] = useState(false);
  const { showToast } = useToast();
  const { regions, isLoading, error, refetch } = useRegions();

  const handleRefresh = () => {
    refetch();
    showToast({
      type: 'info',
      title: 'Refreshing regions...',
    });
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Regions</h2>
          <p className="text-muted-foreground mt-2">
            Manage geographic regions for your infrastructure
          </p>
        </div>
        <div className="flex items-center space-x-3">
          <button
            onClick={handleRefresh}
            className="inline-flex items-center px-4 py-2 bg-background border border-border text-foreground rounded-md hover:bg-accent transition-colors"
            disabled={isLoading}
          >
            <RefreshCw className={`h-4 w-4 mr-2 ${isLoading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
          <button
            onClick={() => setShowAddDialog(true)}
            className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-4 w-4 mr-2" />
            Add Region
          </button>
        </div>
      </div>

      {/* Error State */}
      {error && (
        <div className="bg-destructive/10 border border-destructive/20 rounded-lg p-4">
          <p className="text-destructive text-sm">{error.message}</p>
        </div>
      )}

      {/* Region List */}
      {isLoading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {[...Array(3)].map((_, i) => (
            <div key={i} className="bg-card border border-border rounded-lg p-6 animate-pulse">
              <div className="h-6 bg-muted rounded w-3/4 mb-4"></div>
              <div className="h-4 bg-muted rounded w-full mb-2"></div>
              <div className="h-4 bg-muted rounded w-2/3"></div>
            </div>
          ))}
        </div>
      ) : regions.length === 0 ? (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <MapPin className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
          <h3 className="text-2xl font-semibold text-foreground mb-2">No Regions</h3>
          <p className="text-muted-foreground mb-6">
            Get started by adding your first region
          </p>
          <button
            onClick={() => setShowAddDialog(true)}
            className="inline-flex items-center px-6 py-3 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-5 w-5 mr-2" />
            Add Region
          </button>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {regions.map((region) => (
            <RegionCard
              key={region.region_id}
              region={region}
              onUpdate={refetch}
            />
          ))}
        </div>
      )}

      {/* Add Region Dialog */}
      {showAddDialog && (
        <AddRegionDialog
          onClose={() => setShowAddDialog(false)}
          onSuccess={refetch}
        />
      )}
    </div>
  );
}

