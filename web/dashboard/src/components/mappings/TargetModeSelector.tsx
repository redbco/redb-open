'use client';

import { Plus, Database } from 'lucide-react';

export type TargetMode = 'new' | 'existing';

interface TargetModeSelectorProps {
  onSelect: (mode: TargetMode) => void;
  selected: TargetMode | null;
  disabled?: boolean;
  resourceTypeLabel: string; // e.g., "Container", "Stream", "Webhook"
}

export function TargetModeSelector({
  onSelect,
  selected,
  disabled = false,
  resourceTypeLabel,
}: TargetModeSelectorProps) {
  const modes = [
    {
      id: 'new' as TargetMode,
      icon: Plus,
      label: `New ${resourceTypeLabel}`,
      description: `Create a new ${resourceTypeLabel.toLowerCase()}`,
    },
    {
      id: 'existing' as TargetMode,
      icon: Database,
      label: `Existing ${resourceTypeLabel}`,
      description: `Use an existing ${resourceTypeLabel.toLowerCase()}`,
    },
  ];

  return (
    <div className="space-y-2">
      <div className="text-xs font-medium text-muted-foreground mb-2">
        Select Target Mode
      </div>
      <div className="grid grid-cols-2 gap-3">
        {modes.map((mode) => {
          const Icon = mode.icon;
          const isSelected = selected === mode.id;

          return (
            <button
              key={mode.id}
              type="button"
              onClick={() => !disabled && onSelect(mode.id)}
              disabled={disabled}
              className={`
                relative p-3 rounded-lg border-2 transition-all text-left
                ${
                  isSelected
                    ? 'border-primary bg-primary/5 shadow-sm'
                    : 'border-border bg-background hover:border-primary/50'
                }
                ${disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}
                disabled:cursor-not-allowed
              `}
            >
              <div className="flex items-center space-x-2">
                <div
                  className={`
                    p-1.5 rounded-full transition-colors
                    ${isSelected ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground'}
                  `}
                >
                  <Icon className="h-4 w-4" />
                </div>
                <div className="flex-1">
                  <div className={`font-medium text-xs ${isSelected ? 'text-primary' : 'text-foreground'}`}>
                    {mode.label}
                  </div>
                  <div className="text-[10px] text-muted-foreground leading-tight mt-0.5">
                    {mode.description}
                  </div>
                </div>
                {isSelected && (
                  <div className="h-1.5 w-1.5 rounded-full bg-primary"></div>
                )}
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}

