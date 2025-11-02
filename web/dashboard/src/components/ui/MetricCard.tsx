'use client';

import { LucideIcon } from 'lucide-react';

interface MetricCardProps {
  title: string;
  value: string | number;
  change?: string;
  icon: LucideIcon;
  color?: string;
}

export function MetricCard({ title, value, change, icon: Icon, color = 'text-blue-600 dark:text-blue-400' }: MetricCardProps) {
  return (
    <div className="bg-card border border-border rounded-lg p-6 hover:shadow-md transition-shadow">
      <div className="flex items-center justify-between">
        <div className="flex-1">
          <p className="text-sm font-medium text-muted-foreground">{title}</p>
          <p className="text-2xl font-bold text-foreground mt-1">{value}</p>
          {change && (
            <p className="text-sm text-muted-foreground mt-1">{change}</p>
          )}
        </div>
        <div className={`w-12 h-12 rounded-lg bg-muted/50 flex items-center justify-center ${color}`}>
          <Icon className="h-6 w-6" />
        </div>
      </div>
    </div>
  );
}

