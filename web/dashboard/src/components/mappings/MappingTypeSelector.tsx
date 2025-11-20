'use client';

import { Database, Container } from 'lucide-react';

export type MappingFlowType = 'container' | 'database';

interface MappingTypeSelectorProps {
  selected: MappingFlowType | null;
  onSelect: (type: MappingFlowType) => void;
  disabled?: boolean;
}

export function MappingTypeSelector({
  selected,
  onSelect,
  disabled = false,
}: MappingTypeSelectorProps) {
  return (
    <div className="space-y-3">
      <div>
        <h3 className="text-sm font-medium text-foreground mb-2">
          Choose Mapping Type
        </h3>
        <p className="text-xs text-muted-foreground mb-4">
          Select whether you want to map individual containers or entire databases
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Container Mapping */}
        <button
          type="button"
          onClick={() => !disabled && onSelect('container')}
          disabled={disabled}
          className={`
            relative p-5 rounded-lg border-2 transition-all text-left
            ${
              selected === 'container'
                ? 'border-primary bg-primary/5 shadow-md'
                : 'border-border bg-background hover:border-primary/50 hover:shadow-sm'
            }
            ${disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}
          `}
        >
          <div className="flex items-start space-x-3">
            <div
              className={`
                p-3 rounded-lg transition-colors flex-shrink-0
                ${
                  selected === 'container'
                    ? 'bg-primary text-primary-foreground'
                    : 'bg-muted text-muted-foreground'
                }
              `}
            >
              <Container className="h-6 w-6" />
            </div>
            <div className="flex-1 min-w-0">
              <h4
                className={`font-semibold text-base mb-1 ${
                  selected === 'container' ? 'text-primary' : 'text-foreground'
                }`}
              >
                Container Mapping
              </h4>
              <p className="text-xs text-muted-foreground leading-relaxed">
                Map individual data containers (tables, documents, collections, nodes, etc.) 
                between sources and targets with field-level transformations
              </p>
              <div className="mt-3 flex flex-wrap gap-1">
                <span className="inline-flex items-center px-2 py-0.5 rounded text-[10px] bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300">
                  Tables
                </span>
                <span className="inline-flex items-center px-2 py-0.5 rounded text-[10px] bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300">
                  Documents
                </span>
                <span className="inline-flex items-center px-2 py-0.5 rounded text-[10px] bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300">
                  Streams
                </span>
              </div>
            </div>
          </div>
          {selected === 'container' && (
            <div className="absolute top-3 right-3">
              <div className="h-3 w-3 rounded-full bg-primary"></div>
            </div>
          )}
        </button>

        {/* Database Mapping */}
        <button
          type="button"
          onClick={() => !disabled && onSelect('database')}
          disabled={disabled}
          className={`
            relative p-5 rounded-lg border-2 transition-all text-left
            ${
              selected === 'database'
                ? 'border-primary bg-primary/5 shadow-md'
                : 'border-border bg-background hover:border-primary/50 hover:shadow-sm'
            }
            ${disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}
          `}
        >
          <div className="flex items-start space-x-3">
            <div
              className={`
                p-3 rounded-lg transition-colors flex-shrink-0
                ${
                  selected === 'database'
                    ? 'bg-primary text-primary-foreground'
                    : 'bg-muted text-muted-foreground'
                }
              `}
            >
              <Database className="h-6 w-6" />
            </div>
            <div className="flex-1 min-w-0">
              <h4
                className={`font-semibold text-base mb-1 ${
                  selected === 'database' ? 'text-primary' : 'text-foreground'
                }`}
              >
                Database Mapping
              </h4>
              <p className="text-xs text-muted-foreground leading-relaxed">
                Map entire databases with bulk operations. Create multiple container 
                mappings at once with automatic schema matching
              </p>
              <div className="mt-3 flex flex-wrap gap-1">
                <span className="inline-flex items-center px-2 py-0.5 rounded text-[10px] bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-300">
                  Bulk Operations
                </span>
                <span className="inline-flex items-center px-2 py-0.5 rounded text-[10px] bg-indigo-100 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300">
                  Auto-matching
                </span>
              </div>
            </div>
          </div>
          {selected === 'database' && (
            <div className="absolute top-3 right-3">
              <div className="h-3 w-3 rounded-full bg-primary"></div>
            </div>
          )}
        </button>
      </div>
    </div>
  );
}

