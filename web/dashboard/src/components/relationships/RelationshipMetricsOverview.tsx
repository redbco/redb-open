import { Activity, TrendingUp, AlertCircle, Database } from 'lucide-react';
import type { RelationshipMetrics } from '@/lib/api/types';
import { 
  formatThroughput, 
  formatBytes, 
  getLagStatusColor, 
  getErrorRateStatusColor 
} from '@/lib/utils/dummyMetrics';

interface RelationshipMetricsOverviewProps {
  metrics: RelationshipMetrics;
}

export function RelationshipMetricsOverview({ metrics }: RelationshipMetricsOverviewProps) {
  const lagStatus = getLagStatusColor(metrics.current_replication_lag_seconds);
  const errorStatus = getErrorRateStatusColor(metrics.current_error_rate);

  const metricCards = [
    {
      title: 'Replication Lag',
      value: `${metrics.current_replication_lag_seconds.toFixed(1)}s`,
      status: lagStatus.status,
      statusColor: lagStatus.color,
      statusBgColor: lagStatus.bgColor,
      icon: Activity,
      iconColor: 'text-blue-600 dark:text-blue-400',
      description: 'Current lag time',
    },
    {
      title: 'Throughput',
      value: formatThroughput(metrics.current_throughput_records_per_second),
      status: 'Active',
      statusColor: 'text-green-600 dark:text-green-400',
      statusBgColor: 'bg-green-100 dark:bg-green-900/30',
      icon: TrendingUp,
      iconColor: 'text-green-600 dark:text-green-400',
      description: 'Records per second',
    },
    {
      title: 'Error Rate',
      value: `${metrics.current_error_rate.toFixed(2)}%`,
      status: errorStatus.status,
      statusColor: errorStatus.color,
      statusBgColor: errorStatus.bgColor,
      icon: AlertCircle,
      iconColor: 'text-orange-600 dark:text-orange-400',
      description: 'Failed operations',
    },
    {
      title: 'Data Transfer',
      value: formatBytes(metrics.current_bytes_per_second) + '/s',
      status: 'Streaming',
      statusColor: 'text-purple-600 dark:text-purple-400',
      statusBgColor: 'bg-purple-100 dark:bg-purple-900/30',
      icon: Database,
      iconColor: 'text-purple-600 dark:text-purple-400',
      description: 'Bytes per second',
    },
  ];

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
      {metricCards.map((card, index) => (
        <div key={index} className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <div className={`w-12 h-12 rounded-lg bg-muted/50 flex items-center justify-center ${card.iconColor}`}>
              <card.icon className="h-6 w-6" />
            </div>
            <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${card.statusBgColor} ${card.statusColor}`}>
              {card.status}
            </span>
          </div>
          <div>
            <p className="text-sm font-medium text-muted-foreground mb-1">{card.title}</p>
            <p className="text-3xl font-bold text-foreground mb-1">{card.value}</p>
            <p className="text-xs text-muted-foreground">{card.description}</p>
          </div>
        </div>
      ))}
    </div>
  );
}

