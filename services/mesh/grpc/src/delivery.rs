//! Local message delivery queue for gRPC subscribers

use crate::proto::mesh::v1::{Received, SubscribeRequest};
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, warn};

/// Subscription filter criteria
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscriptionFilter {
    /// Optional partition filter
    pub partition: Option<u32>,
    /// Optional QoS class filter
    pub qos_class: Option<u32>,
    /// Optional source node filter
    pub src_node: Option<u64>,
}

impl From<&SubscribeRequest> for SubscriptionFilter {
    fn from(req: &SubscribeRequest) -> Self {
        Self {
            partition: if req.partition == 0 { None } else { Some(req.partition) },
            qos_class: if req.qos_class == 0 { None } else { Some(req.qos_class) },
            src_node: if req.src_node == 0 { None } else { Some(req.src_node) },
        }
    }
}

impl SubscriptionFilter {
    /// Check if a received message matches this filter
    pub fn matches(&self, msg: &Received) -> bool {
        if let Some(_partition) = self.partition {
            // Note: partition info would need to be added to Received message
            // For now, we'll assume all messages match partition filters
        }
        
        if let Some(_qos_class) = self.qos_class {
            // Note: qos_class info would need to be added to Received message
            // For now, we'll assume all messages match qos_class filters
        }
        
        if let Some(src_node) = self.src_node {
            if msg.src_node != src_node {
                return false;
            }
        }
        
        true
    }
}

/// A subscriber with its filter and message queue
#[derive(Debug)]
pub struct Subscriber {
    /// Subscription filter
    pub filter: SubscriptionFilter,
    /// Message sender channel
    pub sender: mpsc::UnboundedSender<Received>,
    /// Buffer for messages when subscriber is slow
    pub buffer: Arc<RwLock<VecDeque<Received>>>,
}

/// Local delivery queue manager
#[derive(Debug)]
pub struct DeliveryQueue {
    /// Active subscribers indexed by subscription ID
    subscribers: DashMap<u64, Subscriber>,
    /// Next subscription ID
    next_sub_id: Arc<RwLock<u64>>,
    /// Broadcast channel for notifying about new messages
    message_broadcast: broadcast::Sender<Received>,
}

impl DeliveryQueue {
    /// Create a new delivery queue
    pub fn new() -> Self {
        let (message_broadcast, _) = broadcast::channel(1000);
        
        Self {
            subscribers: DashMap::new(),
            next_sub_id: Arc::new(RwLock::new(1)),
            message_broadcast,
        }
    }
    
    /// Subscribe to messages with the given filter
    pub async fn subscribe(
        &self,
        filter: SubscriptionFilter,
    ) -> (u64, mpsc::UnboundedReceiver<Received>) {
        let sub_id = {
            let mut next_id = self.next_sub_id.write().await;
            let id = *next_id;
            *next_id += 1;
            id
        };
        
        let (sender, receiver) = mpsc::unbounded_channel();
        let buffer = Arc::new(RwLock::new(VecDeque::new()));
        
        let subscriber = Subscriber {
            filter,
            sender,
            buffer,
        };
        
        self.subscribers.insert(sub_id, subscriber);
        
        debug!("New subscriber {} registered", sub_id);
        
        (sub_id, receiver)
    }
    
    /// Unsubscribe a subscriber
    pub fn unsubscribe(&self, sub_id: u64) {
        if self.subscribers.remove(&sub_id).is_some() {
            warn!("Subscriber {} unregistered", sub_id);
        } else {
            debug!("Attempted to unsubscribe non-existent subscriber {}", sub_id);
        }
    }
    
    /// Deliver a message to matching subscribers
    /// Returns the number of subscribers the message was successfully delivered to
    pub async fn deliver(&self, message: Received) -> usize {
        debug!(
            "Delivering message {} from node {} to {} subscribers",
            message.msg_id,
            message.src_node,
            self.subscribers.len()
        );
        
        // Broadcast to all subscribers for processing
        let _ = self.message_broadcast.send(message.clone());
        
        let mut delivered_count = 0;
        let mut failed_deliveries = Vec::new();
        
        for entry in self.subscribers.iter() {
            let sub_id = *entry.key();
            let subscriber = entry.value();
            
            // Check if message matches subscriber's filter
            if !subscriber.filter.matches(&message) {
                continue;
            }
            
            // Try to send message to subscriber
            match subscriber.sender.send(message.clone()) {
                Ok(()) => {
                    delivered_count += 1;
                    debug!("Message delivered to subscriber {}", sub_id);
                }
                Err(_) => {
                    // Subscriber channel is closed, mark for removal
                    failed_deliveries.push(sub_id);
                    warn!("Subscriber {} channel closed, marking for removal", sub_id);
                }
            }
        }
        
        // Remove failed subscribers
        for sub_id in failed_deliveries {
            self.subscribers.remove(&sub_id);
        }
        
        debug!(
            "Message {} delivered to {} subscribers",
            message.msg_id, delivered_count
        );
        
        delivered_count
    }
    
    /// Get the number of active subscribers
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }
    
    /// Get subscriber information for admin purposes
    pub fn get_subscriber_info(&self) -> Vec<(u64, SubscriptionFilter)> {
        self.subscribers
            .iter()
            .map(|entry| (*entry.key(), entry.value().filter.clone()))
            .collect()
    }
}

impl Default for DeliveryQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::mesh::v1::{Header, Received};

    #[tokio::test]
    async fn test_subscription_and_delivery() {
        let queue = DeliveryQueue::new();
        
        // Create a subscription
        let filter = SubscriptionFilter {
            partition: None,
            qos_class: None,
            src_node: Some(1001),
        };
        
        let (sub_id, mut receiver) = queue.subscribe(filter).await;
        
        // Create a test message
        let message = Received {
            src_node: 1001,
            dst_node: 2002,
            msg_id: 12345,
            corr_id: 67890,
            headers: vec![Header {
                key: "test".to_string(),
                value: b"value".to_vec(),
            }],
            payload: b"Hello, World!".to_vec(),
            require_ack: false,
        };
        
        // Deliver the message
        let delivered_count = queue.deliver(message.clone()).await;
        assert_eq!(delivered_count, 1);
        
        // Check that we received the message
        let received = receiver.recv().await.unwrap();
        assert_eq!(received.src_node, message.src_node);
        assert_eq!(received.msg_id, message.msg_id);
        assert_eq!(received.payload, message.payload);
        
        // Unsubscribe
        queue.unsubscribe(sub_id);
        assert_eq!(queue.subscriber_count(), 0);
    }
    
    #[tokio::test]
    async fn test_filter_matching() {
        let queue = DeliveryQueue::new();
        
        // Create a subscription with source node filter
        let filter = SubscriptionFilter {
            partition: None,
            qos_class: None,
            src_node: Some(1001),
        };
        
        let (_sub_id, mut receiver) = queue.subscribe(filter).await;
        
        // Message that should match
        let matching_message = Received {
            src_node: 1001,
            dst_node: 2002,
            msg_id: 1,
            corr_id: 1,
            headers: vec![],
            payload: b"match".to_vec(),
            require_ack: false,
        };
        
        // Message that should not match
        let non_matching_message = Received {
            src_node: 9999,
            dst_node: 2002,
            msg_id: 2,
            corr_id: 2,
            headers: vec![],
            payload: b"no match".to_vec(),
            require_ack: false,
        };
        
        // Deliver both messages
        let matching_delivered = queue.deliver(matching_message.clone()).await;
        let non_matching_delivered = queue.deliver(non_matching_message).await;
        
        assert_eq!(matching_delivered, 1);
        assert_eq!(non_matching_delivered, 0);
        
        // Should only receive the matching message
        let received = receiver.recv().await.unwrap();
        assert_eq!(received.msg_id, 1);
        assert_eq!(received.payload, b"match");
        
        // Should not receive the non-matching message
        tokio::select! {
            _ = receiver.recv() => panic!("Should not receive non-matching message"),
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)) => {}
        }
    }
}
