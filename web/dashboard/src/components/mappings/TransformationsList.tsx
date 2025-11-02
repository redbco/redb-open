'use client';

import { Transformation } from '@/lib/api/types';
import { Sparkles, Info } from 'lucide-react';

interface TransformationsListProps {
  transformations: Transformation[];
  isLoading: boolean;
}

export function TransformationsList({ transformations, isLoading }: TransformationsListProps) {
  const getTypeColor = (type: string) => {
    switch (type.toLowerCase()) {
      case 'passthrough':
        return 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400';
      case 'generator':
        return 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400';
      case 'null_returning':
        return 'bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-400';
      default:
        return 'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-400';
    }
  };

  const getTypeLabel = (type: string) => {
    switch (type.toLowerCase()) {
      case 'passthrough':
        return 'Passthrough';
      case 'generator':
        return 'Generator';
      case 'null_returning':
        return 'Null-Returning';
      default:
        return type;
    }
  };

  if (isLoading) {
    return (
      <div className="space-y-3">
        {[...Array(5)].map((_, i) => (
          <div key={i} className="bg-card border border-border rounded-lg p-4 animate-pulse">
            <div className="h-5 bg-muted rounded w-1/4 mb-2"></div>
            <div className="h-4 bg-muted rounded w-3/4"></div>
          </div>
        ))}
      </div>
    );
  }

  if (transformations.length === 0) {
    return (
      <div className="bg-card border border-border rounded-lg p-12 text-center">
        <Sparkles className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
        <h3 className="text-xl font-semibold text-foreground mb-2">No Transformations</h3>
        <p className="text-muted-foreground">
          No transformations available
        </p>
      </div>
    );
  }

  // Group by type
  const grouped = transformations.reduce((acc, t) => {
    const type = t.transformation_type || 'other';
    if (!acc[type]) acc[type] = [];
    acc[type].push(t);
    return acc;
  }, {} as Record<string, Transformation[]>);

  return (
    <div className="space-y-6">
      <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4 flex items-start space-x-3">
        <Info className="h-5 w-5 text-blue-600 dark:text-blue-400 flex-shrink-0 mt-0.5" />
        <div>
          <p className="text-sm text-blue-900 dark:text-blue-100 font-medium">Built-in Transformations</p>
          <p className="text-sm text-blue-700 dark:text-blue-300 mt-1">
            These are system-provided transformations that can be used in mapping rules. Use them when defining how to transform data from source to target columns.
          </p>
        </div>
      </div>

      {Object.entries(grouped).map(([type, items]) => (
        <div key={type}>
          <h3 className="text-lg font-semibold text-foreground mb-3 capitalize">
            {getTypeLabel(type)} Transformations ({items.length})
          </h3>
          <div className="space-y-2">
            {items.map((transformation) => (
              <div
                key={transformation.transformation_id}
                className="bg-card border border-border rounded-lg p-4 hover:border-primary/50 transition-all"
              >
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <div className="flex items-center space-x-2 mb-2">
                      <h4 className="text-base font-semibold text-foreground font-mono">
                        {transformation.transformation_name}
                      </h4>
                      <span className={`px-2 py-0.5 rounded text-xs font-medium ${getTypeColor(transformation.transformation_type)}`}>
                        {getTypeLabel(transformation.transformation_type)}
                      </span>
                      {transformation.transformation_builtin && (
                        <span className="px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-400">
                          Built-in
                        </span>
                      )}
                    </div>
                    {transformation.transformation_description && (
                      <p className="text-sm text-muted-foreground">
                        {transformation.transformation_description}
                      </p>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

