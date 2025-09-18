//! Persistent message queue for handling delivery retries and message ownership

use crate::proto::mesh::v1::{MessageStatus, SendMode, MessageStatusInfo};
use crate::message_tracker::{MessageTracker, MessageRecord};
use mesh_session::manager::OutboundMessage;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock, Mutex};
use tokio::time::interval;
use tracing::{debug, info, warn, error};
use dashmap::DashMap;

/// Configuration for message queue behavior
#[derive(Debug, Clone)]
pub struct MessageQueueConfig {
    /// Maximum number of retry attempts for pending messages
    pub max_retry_attempts: u32,
    /// Base retry interval (exponential backoff)
    pub base_retry_interval: Duration,
    /// Maximum retry interval
    pub max_retry_interval: Duration,
    /// How often to run the retry processor
    pub retry_check_interval: Duration,
    /// Maximum time to keep completed messages for status queries
    pub completed_message_ttl: Duration,
}

impl Default for MessageQueueConfig {
    fn default() -> Self {
        Self {
            max_retry_attempts: 10,
            base_retry_interval: Duration::from_secs(1),
            max_retry_interval: Duration::from_secs(60),
            retry_check_interval: Duration::from_secs(5),
            completed_message_ttl: Duration::from_secs(300), // 5 minutes
        }
    }
}

/// Queued message with retry information
#[derive(Debug, Clone)]
pub struct QueuedMessage {
    /// The original outbound message
    pub message: OutboundMessage,
    /// Number of retry attempts made
    pub retry_count: u32,
    /// When this message was first queued
    pub queued_at: Instant,
    /// When to next attempt delivery
    pub next_retry_at: Instant,
    /// Send mode for this message
    pub send_mode: SendMode,
    /// Timeout for wait modes
    pub timeout_seconds: u32,
    /// Channel to notify about status updates (for streaming)
    pub status_tx: Option<mpsc::UnboundedSender<MessageStatusInfo>>,
}

/// Message status information for streaming (internal type)
#[derive(Debug, Clone)]
pub struct MessageStatusUpdate {
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
}

impl From<&MessageRecord> for MessageStatusInfo {
    fn from(record: &MessageRecord) -> Self {
        Self {
            msg_id: record.msg_id,
            status: record.status as i32,
            status_message: record.status_message.clone(),
            timestamp: record.timestamp,
            require_ack: record.require_ack,
        }
    }
}

/// Persistent message queue with retry logic and ownership semantics
#[derive(Debug)]
pub struct MessageQueue {
    /// Configuration
    config: MessageQueueConfig,
    /// Messages pending delivery (by message ID)
    pending_messages: Arc<DashMap<u64, QueuedMessage>>,
    /// Messages waiting for specific conditions (by condition type)
    waiting_messages: Arc<RwLock<HashMap<WaitCondition, Vec<u64>>>>,
    /// Channel to send outbound messages to SessionManager
    outbound_tx: mpsc::UnboundedSender<OutboundMessage>,
    /// Message tracker for status updates
    message_tracker: Arc<MessageTracker>,
    /// Task handle for retry processor
    retry_task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

/// Conditions that messages can wait for
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WaitCondition {
    /// Waiting for a specific node to come online
    NodeOnline(u64),
    /// Waiting for clients to subscribe on a specific node
    ClientSubscription(u64),
}

impl MessageQueue {
    /// Create a new message queue
    pub fn new(
        config: MessageQueueConfig,
        outbound_tx: mpsc::UnboundedSender<OutboundMessage>,
        message_tracker: Arc<MessageTracker>,
    ) -> Self {
        Self {
            config,
            pending_messages: Arc::new(DashMap::new()),
            waiting_messages: Arc::new(RwLock::new(HashMap::new())),
            outbound_tx,
            message_tracker,
            retry_task_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Start the retry processor task
    pub async fn start_retry_processor(&self) {
        let mut handle_guard = self.retry_task_handle.lock().await;
        if handle_guard.is_some() {
            warn!("Retry processor already running");
            return;
        }

        let pending_messages = self.pending_messages.clone();
        let _waiting_messages = self.waiting_messages.clone();
        let outbound_tx = self.outbound_tx.clone();
        let message_tracker = self.message_tracker.clone();
        let config = self.config.clone();

        let handle = tokio::spawn(async move {
            let mut retry_interval = interval(config.retry_check_interval);
            
            info!("Message queue retry processor started");
            
            loop {
                retry_interval.tick().await;
                
                let now = Instant::now();
                let mut messages_to_retry = Vec::new();
                
                // Find messages ready for retry
                for entry in pending_messages.iter() {
                    let msg_id = *entry.key();
                    let queued_msg = entry.value();
                    
                    if now >= queued_msg.next_retry_at {
                        messages_to_retry.push(msg_id);
                    }
                }
                
                // Process retry attempts
                for msg_id in messages_to_retry {
                    if let Some((_, mut queued_msg)) = pending_messages.remove(&msg_id) {
                        queued_msg.retry_count += 1;
                        
                        if queued_msg.retry_count > config.max_retry_attempts {
                            // Max retries exceeded, mark as undeliverable
                            message_tracker.update_status(
                                msg_id,
                                MessageStatus::Undeliverable,
                                format!("Max retry attempts ({}) exceeded", config.max_retry_attempts),
                            );
                            
                            // Notify streaming clients
                            if let Some(ref tx) = queued_msg.status_tx {
                                if let Some(record) = message_tracker.get_status(msg_id) {
                                    let _ = tx.send(MessageStatusInfo::from(&record));
                                }
                            }
                            
                            continue;
                        }
                        
                        // Calculate next retry time with exponential backoff
                        let retry_delay = config.base_retry_interval
                            * 2_u32.pow(queued_msg.retry_count.saturating_sub(1));
                        let retry_delay = retry_delay.min(config.max_retry_interval);
                        queued_msg.next_retry_at = now + retry_delay;
                        
                        debug!(
                            "Retrying message {} (attempt {}/{})",
                            msg_id, queued_msg.retry_count, config.max_retry_attempts
                        );
                        
                        // Update status
                        message_tracker.update_status(
                            msg_id,
                            MessageStatus::Queued,
                            format!("Retry attempt {} of {}", queued_msg.retry_count, config.max_retry_attempts),
                        );
                        
                        // Notify streaming clients
                        if let Some(ref tx) = queued_msg.status_tx {
                            if let Some(record) = message_tracker.get_status(msg_id) {
                                let _ = tx.send(MessageStatusInfo::from(&record));
                            }
                        }
                        
                        // Try to send again
                        if let Err(e) = outbound_tx.send(queued_msg.message.clone()) {
                            error!("Failed to retry message {}: {}", msg_id, e);
                            
                            message_tracker.update_status(
                                msg_id,
                                MessageStatus::Undeliverable,
                                format!("Failed to retry: {}", e),
                            );
                        } else {
                            // Re-queue for potential future retry
                            pending_messages.insert(msg_id, queued_msg);
                        }
                    }
                }
            }
        });

        *handle_guard = Some(handle);
    }

    /// Queue a message for delivery with retry logic
    pub async fn queue_message(
        &self,
        message: OutboundMessage,
        send_mode: SendMode,
        timeout_seconds: u32,
        status_tx: Option<mpsc::UnboundedSender<MessageStatusInfo>>,
    ) -> Result<(), String> {
        let msg_id = message.msg_id.ok_or("Message must have an ID")?;
        
        let queued_msg = QueuedMessage {
            message: message.clone(),
            retry_count: 0,
            queued_at: Instant::now(),
            next_retry_at: Instant::now(),
            send_mode,
            timeout_seconds,
            status_tx,
        };

        // Try initial send
        if let Err(e) = self.outbound_tx.send(message) {
            return Err(format!("Failed to send message: {}", e));
        }

        // Queue for potential retry
        self.pending_messages.insert(msg_id, queued_msg);
        
        debug!("Message {} queued with mode {:?}", msg_id, send_mode);
        Ok(())
    }

    /// Handle successful message delivery (remove from pending queue)
    pub async fn handle_message_delivered(&self, msg_id: u64) {
        if let Some((_, queued_msg)) = self.pending_messages.remove(&msg_id) {
            debug!("Message {} delivered, removed from pending queue", msg_id);
            
            // Notify streaming clients
            if let Some(ref tx) = queued_msg.status_tx {
                if let Some(record) = self.message_tracker.get_status(msg_id) {
                    let _ = tx.send(MessageStatusInfo::from(&record));
                }
            }
        }
    }

    /// Handle message acknowledgment (complete the message lifecycle)
    pub async fn handle_message_acked(&self, msg_id: u64) {
        if let Some((_, queued_msg)) = self.pending_messages.remove(&msg_id) {
            debug!("Message {} acknowledged, removed from pending queue", msg_id);
            
            // Notify streaming clients and close stream
            if let Some(ref tx) = queued_msg.status_tx {
                if let Some(record) = self.message_tracker.get_status(msg_id) {
                    let _ = tx.send(MessageStatusInfo::from(&record));
                }
                // Close the status stream by dropping the sender
            }
        }
    }

    /// Handle routing feedback to update message status
    pub async fn handle_routing_feedback(&self, msg_id: u64, status: MessageStatus, message: String) {
        // Update message tracker
        self.message_tracker.update_status(msg_id, status, message);
        
        // Notify streaming clients
        if let Some(queued_msg) = self.pending_messages.get(&msg_id) {
            if let Some(ref tx) = queued_msg.status_tx {
                if let Some(record) = self.message_tracker.get_status(msg_id) {
                    let _ = tx.send(MessageStatusInfo::from(&record));
                }
            }
        }
        
        // Handle specific status updates
        match status {
            MessageStatus::Delivered | MessageStatus::WaitingForClientAck => {
                self.handle_message_delivered(msg_id).await;
            }
            MessageStatus::AckSuccess | MessageStatus::AckFailure => {
                self.handle_message_acked(msg_id).await;
            }
            MessageStatus::PendingNode => {
                // Message is waiting for node to come online
                if let Some(queued_msg) = self.pending_messages.get(&msg_id) {
                    let dst_node = queued_msg.message.dst_node;
                    let mut waiting = self.waiting_messages.write().await;
                    waiting.entry(WaitCondition::NodeOnline(dst_node))
                        .or_insert_with(Vec::new)
                        .push(msg_id);
                }
            }
            MessageStatus::PendingClient => {
                // Message is waiting for client subscription
                if let Some(queued_msg) = self.pending_messages.get(&msg_id) {
                    let dst_node = queued_msg.message.dst_node;
                    let mut waiting = self.waiting_messages.write().await;
                    waiting.entry(WaitCondition::ClientSubscription(dst_node))
                        .or_insert_with(Vec::new)
                        .push(msg_id);
                }
            }
            _ => {}
        }
    }

    /// Notify that a node has come online
    pub async fn notify_node_online(&self, node_id: u64) {
        let mut waiting = self.waiting_messages.write().await;
        if let Some(waiting_msgs) = waiting.remove(&WaitCondition::NodeOnline(node_id)) {
            info!("Node {} came online, retrying {} pending messages", node_id, waiting_msgs.len());
            
            for msg_id in waiting_msgs {
                if let Some(mut queued_msg) = self.pending_messages.get_mut(&msg_id) {
                    // Reset retry time to trigger immediate retry
                    queued_msg.next_retry_at = Instant::now();
                }
            }
        }
    }

    /// Notify that a client has subscribed on a node
    pub async fn notify_client_subscribed(&self, node_id: u64) {
        let mut waiting = self.waiting_messages.write().await;
        if let Some(waiting_msgs) = waiting.remove(&WaitCondition::ClientSubscription(node_id)) {
            info!("Client subscribed on node {}, retrying {} pending messages", node_id, waiting_msgs.len());
            
            for msg_id in waiting_msgs {
                if let Some(mut queued_msg) = self.pending_messages.get_mut(&msg_id) {
                    // Reset retry time to trigger immediate retry
                    queued_msg.next_retry_at = Instant::now();
                }
            }
        }
    }

    /// Get statistics about the message queue
    pub fn get_stats(&self) -> MessageQueueStats {
        let pending_count = self.pending_messages.len();
        let waiting_node_count = 0;
        let waiting_client_count = 0;
        
        // Note: This is a simplified stats collection to avoid blocking
        // In a real implementation, you might want to use atomic counters
        
        MessageQueueStats {
            pending_messages: pending_count,
            waiting_for_node: waiting_node_count,
            waiting_for_client: waiting_client_count,
        }
    }
}

/// Statistics about the message queue
#[derive(Debug, Clone)]
pub struct MessageQueueStats {
    /// Number of messages pending retry
    pub pending_messages: usize,
    /// Number of messages waiting for nodes to come online
    pub waiting_for_node: usize,
    /// Number of messages waiting for client subscriptions
    pub waiting_for_client: usize,
}
