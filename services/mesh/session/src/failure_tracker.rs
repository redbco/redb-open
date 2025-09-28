//! Routing failure tracker for detecting session interruptions

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Tracks routing failures to detect session interruptions
#[derive(Debug)]
pub struct RoutingFailureTracker {
    /// Failure counts per destination node
    failures: Arc<RwLock<HashMap<u64, FailureInfo>>>,
    /// Threshold for considering a session interrupted
    failure_threshold: u32,
    /// Time window for failure counting
    failure_window: Duration,
}

#[derive(Debug, Clone)]
struct FailureInfo {
    /// Number of consecutive failures
    count: u32,
    /// Timestamp of first failure in current sequence
    first_failure: Instant,
    /// Timestamp of last failure
    last_failure: Instant,
    /// Whether we've already notified about interruption
    interruption_notified: bool,
}

impl RoutingFailureTracker {
    /// Create a new routing failure tracker
    pub fn new(failure_threshold: u32, failure_window: Duration) -> Self {
        Self {
            failures: Arc::new(RwLock::new(HashMap::new())),
            failure_threshold,
            failure_window,
        }
    }
    
    /// Record a routing failure
    pub async fn record_failure(&self, dst_node: u64) -> (u32, bool) {
        let mut failures = self.failures.write().await;
        let now = Instant::now();
        
        let failure_info = failures.entry(dst_node).or_insert(FailureInfo {
            count: 0,
            first_failure: now,
            last_failure: now,
            interruption_notified: false,
        });
        
        // Check if this failure is within the time window
        if now.duration_since(failure_info.first_failure) > self.failure_window {
            // Reset the failure count for a new window
            failure_info.count = 1;
            failure_info.first_failure = now;
            failure_info.interruption_notified = false;
        } else {
            failure_info.count += 1;
        }
        
        failure_info.last_failure = now;
        
        let should_notify = failure_info.count >= self.failure_threshold && !failure_info.interruption_notified;
        if should_notify {
            failure_info.interruption_notified = true;
            warn!("Session interruption detected for node {} after {} failures", dst_node, failure_info.count);
        }
        
        debug!("Recorded routing failure for node {} (count: {})", dst_node, failure_info.count);
        (failure_info.count, should_notify)
    }
    
    /// Record a successful routing (clears failure count)
    pub async fn record_success(&self, dst_node: u64) -> bool {
        let mut failures = self.failures.write().await;
        
        if let Some(failure_info) = failures.get(&dst_node) {
            let was_interrupted = failure_info.interruption_notified;
            if was_interrupted {
                debug!("Session recovery detected for node {} (was interrupted)", dst_node);
            }
            failures.remove(&dst_node);
            was_interrupted
        } else {
            false
        }
    }
    
    /// Get current failure count for a node
    pub async fn get_failure_count(&self, dst_node: u64) -> u32 {
        let failures = self.failures.read().await;
        failures.get(&dst_node).map(|info| info.count).unwrap_or(0)
    }
    
    /// Check if a node is considered interrupted
    pub async fn is_interrupted(&self, dst_node: u64) -> bool {
        let failures = self.failures.read().await;
        failures.get(&dst_node).map(|info| info.interruption_notified).unwrap_or(false)
    }
    
    /// Clean up old failure records
    pub async fn cleanup_old_failures(&self) {
        let mut failures = self.failures.write().await;
        let now = Instant::now();
        
        failures.retain(|_, failure_info| {
            now.duration_since(failure_info.last_failure) < self.failure_window * 2
        });
    }
}
