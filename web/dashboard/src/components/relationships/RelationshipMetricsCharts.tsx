'use client';

import { 
  LineChart, 
  Line, 
  AreaChart, 
  Area, 
  BarChart, 
  Bar, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  Legend, 
  ResponsiveContainer 
} from 'recharts';
import type { RelationshipMetrics } from '@/lib/api/types';
import { formatBytes } from '@/lib/utils/dummyMetrics';

interface RelationshipMetricsChartsProps {
  metrics: RelationshipMetrics;
}

export function RelationshipMetricsCharts({ metrics }: RelationshipMetricsChartsProps) {
  // Format time series data for charts
  const chartData = metrics.time_series.map((point) => ({
    time: new Date(point.timestamp).toLocaleTimeString('en-US', { 
      hour: 'numeric', 
      minute: '2-digit',
      hour12: true 
    }),
    lag: point.replication_lag_seconds,
    throughput: point.throughput_records_per_second,
    errorRate: point.error_rate,
    bytes: point.bytes_transferred,
  }));

  // Sample every Nth point to avoid overcrowding (show every 12th point = hourly for 24h data)
  const sampledData = chartData.filter((_, index) => index % 12 === 0);

  // Custom tooltip for better formatting
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-card border border-border rounded-lg p-3 shadow-lg">
          <p className="text-sm font-medium text-foreground mb-2">{label}</p>
          {payload.map((entry: any, index: number) => (
            <p key={index} className="text-xs text-muted-foreground">
              <span style={{ color: entry.color }}>●</span> {entry.name}: {entry.value}
            </p>
          ))}
        </div>
      );
    }
    return null;
  };

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      {/* Replication Lag Chart */}
      <div className="bg-card border border-border rounded-lg p-6">
        <h3 className="text-lg font-semibold text-foreground mb-4">Replication Lag</h3>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={sampledData}>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
            <XAxis 
              dataKey="time" 
              stroke="hsl(var(--muted-foreground))"
              style={{ fontSize: '12px' }}
            />
            <YAxis 
              stroke="hsl(var(--muted-foreground))"
              style={{ fontSize: '12px' }}
              label={{ value: 'Seconds', angle: -90, position: 'insideLeft', style: { fontSize: '12px' } }}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: '12px' }} />
            <Line 
              type="monotone" 
              dataKey="lag" 
              name="Lag (seconds)"
              stroke="hsl(220 70% 50%)" 
              strokeWidth={2}
              dot={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Throughput Chart */}
      <div className="bg-card border border-border rounded-lg p-6">
        <h3 className="text-lg font-semibold text-foreground mb-4">Throughput</h3>
        <ResponsiveContainer width="100%" height={300}>
          <AreaChart data={sampledData}>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
            <XAxis 
              dataKey="time" 
              stroke="hsl(var(--muted-foreground))"
              style={{ fontSize: '12px' }}
            />
            <YAxis 
              stroke="hsl(var(--muted-foreground))"
              style={{ fontSize: '12px' }}
              label={{ value: 'Records/sec', angle: -90, position: 'insideLeft', style: { fontSize: '12px' } }}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: '12px' }} />
            <Area 
              type="monotone" 
              dataKey="throughput" 
              name="Throughput (rec/s)"
              stroke="hsl(160 60% 45%)" 
              fill="hsl(160 60% 45% / 0.2)"
              strokeWidth={2}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Error Rate Chart */}
      <div className="bg-card border border-border rounded-lg p-6">
        <h3 className="text-lg font-semibold text-foreground mb-4">Error Rate</h3>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={sampledData}>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
            <XAxis 
              dataKey="time" 
              stroke="hsl(var(--muted-foreground))"
              style={{ fontSize: '12px' }}
            />
            <YAxis 
              stroke="hsl(var(--muted-foreground))"
              style={{ fontSize: '12px' }}
              label={{ value: 'Error %', angle: -90, position: 'insideLeft', style: { fontSize: '12px' } }}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: '12px' }} />
            <Line 
              type="monotone" 
              dataKey="errorRate" 
              name="Error Rate (%)"
              stroke="hsl(30 80% 55%)" 
              strokeWidth={2}
              dot={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Bytes Transferred Chart */}
      <div className="bg-card border border-border rounded-lg p-6">
        <h3 className="text-lg font-semibold text-foreground mb-4">Data Volume</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={sampledData}>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
            <XAxis 
              dataKey="time" 
              stroke="hsl(var(--muted-foreground))"
              style={{ fontSize: '12px' }}
            />
            <YAxis 
              stroke="hsl(var(--muted-foreground))"
              style={{ fontSize: '12px' }}
              tickFormatter={(value) => formatBytes(value)}
              label={{ value: 'Bytes', angle: -90, position: 'insideLeft', style: { fontSize: '12px' } }}
            />
            <Tooltip 
              content={<CustomTooltip />}
              formatter={(value: any) => formatBytes(Number(value))}
            />
            <Legend wrapperStyle={{ fontSize: '12px' }} />
            <Bar 
              dataKey="bytes" 
              name="Bytes Transferred"
              fill="hsl(280 65% 60%)"
            />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

