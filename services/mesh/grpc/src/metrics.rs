//! Message status metrics and monitoring

use crate::message_tracker::{MessageTracker, MessageTrackerStats};
use crate::proto::mesh::v1::MessageStatus;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{debug, info, warn};

/// Message status metrics collector
#[derive(Debug)]
pub struct MessageMetrics {
    /// Message tracker reference
    message_tracker: Arc<MessageTracker>,
    /// Metrics collection interval
    collection_interval: Duration,
}

impl MessageMetrics {
    /// Create a new message metrics collector
    pub fn new(message_tracker: Arc<MessageTracker>) -> Self {
        Self {
            message_tracker,
            collection_interval: Duration::from_secs(30), // Collect metrics every 30 seconds
        }
    }
    
    /// Set the metrics collection interval
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.collection_interval = interval;
        self
    }
    
    /// Start the metrics collection task
    pub fn start_collection_task(self) {
        tokio::spawn(async move {
            let mut interval_timer = interval(self.collection_interval);
            
            info!("Starting message metrics collection task (interval: {:?})", self.collection_interval);
            
            loop {
                interval_timer.tick().await;
                
                let stats = self.message_tracker.get_stats();
                self.log_metrics(&stats);
                
                // Check for potential issues and log warnings
                self.check_health(&stats);
            }
        });
    }
    
    /// Log current message status metrics
    fn log_metrics(&self, stats: &MessageTrackerStats) {
        info!(
            "Message Status Metrics - Total: {}, Undeliverable: {}, Queued: {}, Pending Node: {}, Pending Client: {}, Delivered: {}, Waiting ACK: {}, ACK Success: {}, ACK Failure: {}",
            stats.total_messages,
            stats.undeliverable,
            stats.queued,
            stats.pending_node,
            stats.pending_client,
            stats.delivered,
            stats.waiting_for_ack,
            stats.ack_success,
            stats.ack_failure
        );
        
        // Calculate percentages for better insights
        if stats.total_messages > 0 {
            let success_rate = ((stats.delivered + stats.ack_success) as f64 / stats.total_messages as f64) * 100.0;
            let failure_rate = ((stats.undeliverable + stats.ack_failure) as f64 / stats.total_messages as f64) * 100.0;
            let pending_rate = ((stats.queued + stats.pending_node + stats.pending_client + stats.waiting_for_ack) as f64 / stats.total_messages as f64) * 100.0;
            
            info!(
                "Message Success Rate: {:.1}%, Failure Rate: {:.1}%, Pending Rate: {:.1}%",
                success_rate, failure_rate, pending_rate
            );
        }
    }
    
    /// Check message status health and log warnings for potential issues
    fn check_health(&self, stats: &MessageTrackerStats) {
        // Warn if too many messages are undeliverable
        if stats.total_messages > 0 {
            let undeliverable_rate = (stats.undeliverable as f64 / stats.total_messages as f64) * 100.0;
            if undeliverable_rate > 10.0 {
                warn!(
                    "High undeliverable message rate: {:.1}% ({}/{})",
                    undeliverable_rate, stats.undeliverable, stats.total_messages
                );
            }
            
            // Warn if too many messages are pending for too long
            let pending_total = stats.queued + stats.pending_node + stats.pending_client + stats.waiting_for_ack;
            let pending_rate = (pending_total as f64 / stats.total_messages as f64) * 100.0;
            if pending_rate > 20.0 {
                warn!(
                    "High pending message rate: {:.1}% ({}/{})",
                    pending_rate, pending_total, stats.total_messages
                );
            }
            
            // Warn if acknowledgment failure rate is high
            let ack_total = stats.ack_success + stats.ack_failure;
            if ack_total > 0 {
                let ack_failure_rate = (stats.ack_failure as f64 / ack_total as f64) * 100.0;
                if ack_failure_rate > 5.0 {
                    warn!(
                        "High acknowledgment failure rate: {:.1}% ({}/{})",
                        ack_failure_rate, stats.ack_failure, ack_total
                    );
                }
            }
        }
        
        // Debug log for detailed breakdown
        debug!(
            "Detailed message status breakdown: undeliverable={}, queued={}, pending_node={}, pending_client={}, delivered={}, waiting_ack={}, ack_success={}, ack_failure={}",
            stats.undeliverable,
            stats.queued,
            stats.pending_node,
            stats.pending_client,
            stats.delivered,
            stats.waiting_for_ack,
            stats.ack_success,
            stats.ack_failure
        );
    }
}

/// Message status distribution for analysis
#[derive(Debug, Clone)]
pub struct MessageStatusDistribution {
    /// Total number of messages
    pub total: usize,
    /// Distribution by status
    pub by_status: std::collections::HashMap<MessageStatus, usize>,
    /// Success rate (delivered + ack_success) / total
    pub success_rate: f64,
    /// Failure rate (undeliverable + ack_failure) / total
    pub failure_rate: f64,
    /// Pending rate (queued + pending_*) / total
    pub pending_rate: f64,
}

impl MessageStatusDistribution {
    /// Create distribution from stats
    pub fn from_stats(stats: &MessageTrackerStats) -> Self {
        let mut by_status = std::collections::HashMap::new();
        by_status.insert(MessageStatus::Undeliverable, stats.undeliverable);
        by_status.insert(MessageStatus::Queued, stats.queued);
        by_status.insert(MessageStatus::PendingNode, stats.pending_node);
        by_status.insert(MessageStatus::PendingClient, stats.pending_client);
        by_status.insert(MessageStatus::Delivered, stats.delivered);
        by_status.insert(MessageStatus::WaitingForClientAck, stats.waiting_for_ack);
        by_status.insert(MessageStatus::AckSuccess, stats.ack_success);
        by_status.insert(MessageStatus::AckFailure, stats.ack_failure);
        
        let total = stats.total_messages;
        let success_rate = if total > 0 {
            ((stats.delivered + stats.ack_success) as f64 / total as f64) * 100.0
        } else {
            0.0
        };
        
        let failure_rate = if total > 0 {
            ((stats.undeliverable + stats.ack_failure) as f64 / total as f64) * 100.0
        } else {
            0.0
        };
        
        let pending_rate = if total > 0 {
            ((stats.queued + stats.pending_node + stats.pending_client + stats.waiting_for_ack) as f64 / total as f64) * 100.0
        } else {
            0.0
        };
        
        Self {
            total,
            by_status,
            success_rate,
            failure_rate,
            pending_rate,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_tracker::MessageTracker;
    use crate::proto::mesh::v1::MessageStatus;
    
    #[tokio::test]
    async fn test_message_metrics() {
        let tracker = Arc::new(MessageTracker::new());
        let _metrics = MessageMetrics::new(tracker.clone());
        
        // Add some test messages
        tracker.track_message(1, MessageStatus::Delivered, "Delivered successfully".to_string(), false);
        tracker.track_message(2, MessageStatus::Undeliverable, "Node unreachable".to_string(), false);
        tracker.track_message(3, MessageStatus::PendingNode, "Node offline".to_string(), false);
        
        let stats = tracker.get_stats();
        assert_eq!(stats.total_messages, 3);
        assert_eq!(stats.delivered, 1);
        assert_eq!(stats.undeliverable, 1);
        assert_eq!(stats.pending_node, 1);
        
        // Test distribution calculation
        let distribution = MessageStatusDistribution::from_stats(&stats);
        assert_eq!(distribution.total, 3);
        assert!((distribution.success_rate - 33.3).abs() < 0.1);
        assert!((distribution.failure_rate - 33.3).abs() < 0.1);
        assert!((distribution.pending_rate - 33.3).abs() < 0.1);
    }
}
