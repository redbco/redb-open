import type { RelationshipMetrics } from '@/lib/api/types';

/**
 * Generate realistic dummy CDC metrics data for a relationship
 * This function simulates 24 hours of CDC replication metrics with:
 * - 5-minute intervals (288 data points)
 * - Business hour patterns (higher throughput during work hours)
 * - Realistic variance in lag and error rates
 * - Occasional error spikes
 * 
 * @param relationshipName - The name of the relationship
 * @returns Complete metrics data with time series
 */
export function generateDummyMetrics(relationshipName: string): RelationshipMetrics {
  const now = new Date();
  const intervals = 288; // 24 hours * 12 (5-minute intervals)
  const timeSeries: RelationshipMetrics['time_series'] = [];
  
  let totalRecords = 0;
  let totalBytes = 0;
  let totalErrors = 0;
  
  // Generate time series data
  for (let i = intervals - 1; i >= 0; i--) {
    const timestamp = new Date(now.getTime() - i * 5 * 60 * 1000);
    const hour = timestamp.getHours();
    
    // Business hours pattern (8am - 6pm has higher activity)
    const isBusinessHours = hour >= 8 && hour < 18;
    const businessMultiplier = isBusinessHours ? 2.5 : 0.8;
    
    // Base throughput with variance
    const baseThroughput = 150 + Math.random() * 100;
    const throughput = baseThroughput * businessMultiplier;
    
    // Replication lag (lower during off-peak hours)
    const baseLag = isBusinessHours ? 15 + Math.random() * 30 : 5 + Math.random() * 10;
    const replicationLag = Math.max(0.5, baseLag);
    
    // Error rate (occasional spikes, mostly low)
    const hasErrorSpike = Math.random() < 0.02; // 2% chance of error spike
    const baseErrorRate = hasErrorSpike ? 3 + Math.random() * 5 : Math.random() * 0.5;
    const errorRate = Math.min(10, baseErrorRate); // Cap at 10%
    
    // Bytes transferred (proportional to throughput, avg ~2KB per record)
    const avgRecordSize = 1800 + Math.random() * 400; // 1.8KB - 2.2KB
    const bytesTransferred = throughput * avgRecordSize;
    
    // Accumulate totals
    const recordsInInterval = throughput * 300; // 5 minutes = 300 seconds
    const errorsInInterval = (recordsInInterval * errorRate) / 100;
    totalRecords += recordsInInterval;
    totalBytes += bytesTransferred * 300;
    totalErrors += errorsInInterval;
    
    timeSeries.push({
      timestamp: timestamp.toISOString(),
      replication_lag_seconds: Number(replicationLag.toFixed(2)),
      throughput_records_per_second: Number(throughput.toFixed(2)),
      error_rate: Number(errorRate.toFixed(3)),
      bytes_transferred: Math.floor(bytesTransferred),
    });
  }
  
  // Current metrics (last data point)
  const lastPoint = timeSeries[timeSeries.length - 1];
  
  // Calculate uptime (assume occasional downtime)
  const uptimePercentage = 98.5 + Math.random() * 1.4; // 98.5% - 99.9%
  
  // Average record size
  const averageRecordSize = totalBytes / totalRecords;
  
  // Last sync timestamp (most recent)
  const lastSyncTimestamp = lastPoint.timestamp;
  
  // Next sync timestamp (5 minutes from now for continuous CDC)
  const nextSyncDate = new Date(now.getTime() + 5 * 60 * 1000);
  const nextSyncTimestamp = nextSyncDate.toISOString();
  
  return {
    relationship_name: relationshipName,
    
    // Current metrics
    current_replication_lag_seconds: lastPoint.replication_lag_seconds,
    current_throughput_records_per_second: lastPoint.throughput_records_per_second,
    current_error_rate: lastPoint.error_rate,
    current_bytes_per_second: lastPoint.bytes_transferred,
    
    // Time series data
    time_series: timeSeries,
    
    // Aggregate statistics
    total_records_replicated: Math.floor(totalRecords),
    total_bytes_transferred: Math.floor(totalBytes),
    total_errors: Math.floor(totalErrors),
    uptime_percentage: Number(uptimePercentage.toFixed(2)),
    
    // Additional metrics
    average_record_size_bytes: Math.floor(averageRecordSize),
    last_sync_timestamp: lastSyncTimestamp,
    next_sync_timestamp: nextSyncTimestamp,
  };
}

/**
 * Format bytes to human-readable format
 */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`;
}

/**
 * Format number with thousand separators
 */
export function formatNumber(num: number): string {
  return new Intl.NumberFormat('en-US').format(Math.floor(num));
}

/**
 * Format throughput to human-readable format
 */
export function formatThroughput(recordsPerSecond: number): string {
  if (recordsPerSecond < 1000) {
    return `${recordsPerSecond.toFixed(1)} rec/s`;
  }
  return `${(recordsPerSecond / 1000).toFixed(2)}k rec/s`;
}

/**
 * Get status color based on replication lag
 */
export function getLagStatusColor(lagSeconds: number): {
  color: string;
  bgColor: string;
  status: string;
} {
  if (lagSeconds < 30) {
    return {
      color: 'text-green-600 dark:text-green-400',
      bgColor: 'bg-green-100 dark:bg-green-900/30',
      status: 'Excellent',
    };
  } else if (lagSeconds < 60) {
    return {
      color: 'text-yellow-600 dark:text-yellow-400',
      bgColor: 'bg-yellow-100 dark:bg-yellow-900/30',
      status: 'Good',
    };
  } else {
    return {
      color: 'text-red-600 dark:text-red-400',
      bgColor: 'bg-red-100 dark:bg-red-900/30',
      status: 'High',
    };
  }
}

/**
 * Get status color based on error rate
 */
export function getErrorRateStatusColor(errorRate: number): {
  color: string;
  bgColor: string;
  status: string;
} {
  if (errorRate < 1) {
    return {
      color: 'text-green-600 dark:text-green-400',
      bgColor: 'bg-green-100 dark:bg-green-900/30',
      status: 'Healthy',
    };
  } else if (errorRate < 3) {
    return {
      color: 'text-yellow-600 dark:text-yellow-400',
      bgColor: 'bg-yellow-100 dark:bg-yellow-900/30',
      status: 'Warning',
    };
  } else {
    return {
      color: 'text-red-600 dark:text-red-400',
      bgColor: 'bg-red-100 dark:bg-red-900/30',
      status: 'Critical',
    };
  }
}

