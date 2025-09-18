//! Message status tracking system

use crate::proto::mesh::v1::{MessageStatus, MessageStatusInfo};
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::time::{interval, Duration, Instant};
use tracing::{debug, warn};

/// Maximum time to keep completed message status records (in seconds)
const CLEANUP_RETENTION_SECONDS: u64 = 300; // 5 minutes

/// Cleanup interval (in seconds)
const CLEANUP_INTERVAL_SECONDS: u64 = 60; // 1 minute

/// Internal message status record
#[derive(Debug, Clone)]
pub struct MessageRecord {
    /// The message ID
    pub msg_id: u64,
    /// The status of the message
    pub status: MessageStatus,
    /// The status message
    pub status_message: String,
    /// The timestamp of the message
    pub timestamp: u64,
    /// Whether the message requires an acknowledgment
    pub require_ack: bool,
    /// When this record was created
    pub created_at: Instant,
}

impl MessageRecord {
    /// Create a new message record
    pub fn new(msg_id: u64, status: MessageStatus, status_message: String, require_ack: bool) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        Self {
            msg_id,
            status,
            status_message,
            timestamp,
            require_ack,
            created_at: Instant::now(),
        }
    }
    
    /// Update the status of this record
    pub fn update_status(&mut self, status: MessageStatus, status_message: String) {
        self.status = status;
        self.status_message = status_message;
        self.timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }
    
    /// Check if this record is completed and can be cleaned up
    pub fn is_completed(&self) -> bool {
        matches!(
            self.status,
            MessageStatus::Delivered
                | MessageStatus::AckSuccess
                | MessageStatus::AckFailure
                | MessageStatus::Undeliverable
        )
    }
    
    /// Check if this record is old enough to be cleaned up
    pub fn should_cleanup(&self) -> bool {
        self.is_completed() && 
        self.created_at.elapsed().as_secs() > CLEANUP_RETENTION_SECONDS
    }
    
    /// Convert to protobuf MessageStatusInfo
    pub fn to_proto(&self) -> MessageStatusInfo {
        MessageStatusInfo {
            msg_id: self.msg_id,
            status: self.status as i32,
            status_message: self.status_message.clone(),
            timestamp: self.timestamp,
            require_ack: self.require_ack,
        }
    }
}

/// Message tracker for managing message status lifecycle
#[derive(Debug)]
pub struct MessageTracker {
    /// Active message records indexed by message ID
    records: Arc<DashMap<u64, MessageRecord>>,
    /// Cleanup task handle
    cleanup_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

impl MessageTracker {
    /// Create a new message tracker
    pub fn new() -> Self {
        let records = Arc::new(DashMap::new());
        let cleanup_handle = Arc::new(RwLock::new(None));
        
        let tracker = Self {
            records,
            cleanup_handle,
        };
        
        // Start cleanup task
        tracker.start_cleanup_task();
        
        tracker
    }
    
    /// Start the cleanup task
    fn start_cleanup_task(&self) {
        let records = Arc::clone(&self.records);
        let cleanup_handle = Arc::clone(&self.cleanup_handle);
        
        let handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(CLEANUP_INTERVAL_SECONDS));
            
            loop {
                interval.tick().await;
                
                let mut cleanup_count = 0;
                let mut to_remove = Vec::new();
                
                // Collect records to remove
                for entry in records.iter() {
                    let record = entry.value();
                    if record.should_cleanup() {
                        to_remove.push(*entry.key());
                    }
                }
                
                // Remove old records
                for msg_id in to_remove {
                    if records.remove(&msg_id).is_some() {
                        cleanup_count += 1;
                    }
                }
                
                if cleanup_count > 0 {
                    debug!("Cleaned up {} completed message records", cleanup_count);
                }
            }
        });
        
        // Store the handle
        if let Ok(mut guard) = cleanup_handle.try_write() {
            *guard = Some(handle);
        };
    }
    
    /// Track a new message with initial status
    pub fn track_message(
        &self,
        msg_id: u64,
        status: MessageStatus,
        status_message: String,
        require_ack: bool,
    ) {
        let record = MessageRecord::new(msg_id, status, status_message, require_ack);
        self.records.insert(msg_id, record);
        
        debug!(
            "Tracking message {} with status {:?}",
            msg_id,
            status
        );
    }
    
    /// Update message status
    pub fn update_status(
        &self,
        msg_id: u64,
        status: MessageStatus,
        status_message: String,
    ) -> bool {
        if let Some(mut record) = self.records.get_mut(&msg_id) {
            record.update_status(status, status_message);
            debug!(
                "Updated message {} status to {:?}",
                msg_id,
                status
            );
            true
        } else {
            warn!("Attempted to update status for unknown message {}", msg_id);
            false
        }
    }
    
    /// Get message status
    pub fn get_status(&self, msg_id: u64) -> Option<MessageRecord> {
        self.records.get(&msg_id).map(|entry| entry.value().clone())
    }
    
    /// Get multiple message statuses
    pub fn get_statuses(&self, msg_ids: &[u64]) -> Vec<MessageStatusInfo> {
        msg_ids
            .iter()
            .filter_map(|&msg_id| {
                self.records
                    .get(&msg_id)
                    .map(|entry| entry.value().to_proto())
            })
            .collect()
    }
    
    /// Get all pending messages (not completed)
    pub fn get_pending_messages(&self) -> Vec<MessageStatusInfo> {
        self.records
            .iter()
            .filter_map(|entry| {
                let record = entry.value();
                if !record.is_completed() {
                    Some(record.to_proto())
                } else {
                    None
                }
            })
            .collect()
    }
    
    /// Get statistics about tracked messages
    pub fn get_stats(&self) -> MessageTrackerStats {
        let mut stats = MessageTrackerStats::default();
        
        for entry in self.records.iter() {
            let record = entry.value();
            stats.total_messages += 1;
            
            match record.status {
                MessageStatus::Undeliverable => stats.undeliverable += 1,
                MessageStatus::Queued => stats.queued += 1,
                MessageStatus::PendingNode => stats.pending_node += 1,
                MessageStatus::PendingClient => stats.pending_client += 1,
                MessageStatus::Delivered => stats.delivered += 1,
                MessageStatus::WaitingForClientAck => stats.waiting_for_ack += 1,
                MessageStatus::AckSuccess => stats.ack_success += 1,
                MessageStatus::AckFailure => stats.ack_failure += 1,
                _ => {}
            }
        }
        
        stats
    }
    
    /// Remove a message record (for manual cleanup)
    pub fn remove_message(&self, msg_id: u64) -> bool {
        self.records.remove(&msg_id).is_some()
    }
    
    /// Get the number of tracked messages
    pub fn message_count(&self) -> usize {
        self.records.len()
    }
}

impl Default for MessageTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for MessageTracker {
    fn drop(&mut self) {
        // Cancel cleanup task
        if let Ok(mut guard) = self.cleanup_handle.try_write() {
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }
    }
}

/// Statistics about message tracker
#[derive(Debug, Default, Clone)]
pub struct MessageTrackerStats {
    /// Total number of messages
    pub total_messages: usize,
    /// Number of undeliverable messages
    pub undeliverable: usize,
    /// Number of queued messages
    pub queued: usize,
    /// Number of pending node messages
    pub pending_node: usize,
    /// Number of pending client messages
    pub pending_client: usize,
    /// Number of delivered messages
    pub delivered: usize,
    /// Number of waiting for ack messages
    pub waiting_for_ack: usize,
    /// Number of ack success messages
    pub ack_success: usize,
    /// Number of ack failure messages
    pub ack_failure: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_message_tracking() {
        let tracker = MessageTracker::new();
        
        // Track a new message
        tracker.track_message(
            12345,
            MessageStatus::Queued,
            "Message queued for delivery".to_string(),
            false,
        );
        
        // Verify message is tracked
        let status = tracker.get_status(12345).unwrap();
        assert_eq!(status.msg_id, 12345);
        assert_eq!(status.status, MessageStatus::Queued);
        assert!(!status.require_ack);
        
        // Update status
        tracker.update_status(
            12345,
            MessageStatus::Delivered,
            "Message delivered successfully".to_string(),
        );
        
        // Verify status update
        let status = tracker.get_status(12345).unwrap();
        assert_eq!(status.status, MessageStatus::Delivered);
        assert_eq!(status.status_message, "Message delivered successfully");
        
        // Test batch query
        let statuses = tracker.get_statuses(&[12345, 99999]);
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].msg_id, 12345);
    }
    
    #[tokio::test]
    async fn test_message_completion() {
        let tracker = MessageTracker::new();
        
        // Track messages with different statuses
        tracker.track_message(1, MessageStatus::Queued, "Queued".to_string(), false);
        tracker.track_message(2, MessageStatus::Delivered, "Delivered".to_string(), false);
        tracker.track_message(3, MessageStatus::AckSuccess, "Ack success".to_string(), true);
        
        let pending = tracker.get_pending_messages();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].msg_id, 1);
        
        let stats = tracker.get_stats();
        assert_eq!(stats.total_messages, 3);
        assert_eq!(stats.queued, 1);
        assert_eq!(stats.delivered, 1);
        assert_eq!(stats.ack_success, 1);
    }
    
    #[test]
    fn test_message_record() {
        let mut record = MessageRecord::new(
            123,
            MessageStatus::Queued,
            "Initial status".to_string(),
            true,
        );
        
        assert_eq!(record.msg_id, 123);
        assert_eq!(record.status, MessageStatus::Queued);
        assert!(record.require_ack);
        assert!(!record.is_completed());
        
        // Update to completed status
        record.update_status(MessageStatus::Delivered, "Delivered".to_string());
        assert!(record.is_completed());
        
        // Convert to proto
        let proto = record.to_proto();
        assert_eq!(proto.msg_id, 123);
        assert_eq!(proto.status, MessageStatus::Delivered as i32);
        assert!(proto.require_ack);
    }
}
