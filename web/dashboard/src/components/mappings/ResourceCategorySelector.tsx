'use client';

import { Database, Activity, Bot } from 'lucide-react';

export type ResourceCategory = 'containers' | 'endpoints' | 'ai';

interface ResourceCategorySelectorProps {
  onSelect: (category: ResourceCategory) => void;
  selected: ResourceCategory | null;
  disabled?: boolean;
}

interface CategoryOption {
  id: ResourceCategory;
  icon: typeof Database;
  label: string;
  description: string;
}

const CATEGORIES: CategoryOption[] = [
  {
    id: 'containers',
    icon: Database,
    label: 'Data Containers',
    description: 'Tables, documents, graphs',
  },
  {
    id: 'endpoints',
    icon: Activity,
    label: 'Endpoints',
    description: 'Streams, webhooks',
  },
  {
    id: 'ai',
    icon: Bot,
    label: 'AI (Model Context Protocol)',
    description: 'Model Context Protocol (MCP) resources',
  },
];

export function ResourceCategorySelector({
  onSelect,
  selected,
  disabled = false,
}: ResourceCategorySelectorProps) {
  return (
    <div className="grid grid-cols-1 gap-3">
      {CATEGORIES.map((category) => {
        const Icon = category.icon;
        const isSelected = selected === category.id;

        return (
          <button
            key={category.id}
            type="button"
            onClick={() => !disabled && onSelect(category.id)}
            disabled={disabled}
            className={`
              relative p-4 rounded-lg border-2 transition-all text-left
              ${
                isSelected
                  ? 'border-primary bg-primary/5 shadow-sm'
                  : 'border-border bg-background hover:border-primary/50'
              }
              ${disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}
              disabled:cursor-not-allowed
            `}
          >
            <div className="flex items-center space-x-3">
              <div
                className={`
                  p-2 rounded-full transition-colors
                  ${isSelected ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground'}
                `}
              >
                <Icon className="h-5 w-5" />
              </div>
              <div className="flex-1">
                <div className={`font-medium text-sm ${isSelected ? 'text-primary' : 'text-foreground'}`}>
                  {category.label}
                </div>
                <div className="text-xs text-muted-foreground">
                  {category.description}
                </div>
              </div>
              {isSelected && (
                <div className="h-2 w-2 rounded-full bg-primary"></div>
              )}
            </div>
          </button>
        );
      })}
    </div>
  );
}

