//! Event notification system for mesh state changes

use crate::proto::mesh::v1::{MeshStateEvent, MeshEventType};
use crate::proto::mesh::v1::mesh_data_server::MeshData;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Trait for handling mesh events (local definition to avoid circular dependency)
pub trait MeshEventHandler: Send + Sync + std::fmt::Debug {
    /// Notify that a session was added
    fn notify_session_added(&self, peer_node_id: u64, remote_addr: String);
    /// Notify that a session was removed
    fn notify_session_removed(&self, peer_node_id: u64, reason: String);
    /// Notify that a session was interrupted
    fn notify_session_interrupted(&self, peer_node_id: u64, reason: String);
    /// Notify that a session was recovered
    fn notify_session_recovered(&self, peer_node_id: u64);
    /// Notify about routing failure
    fn notify_routing_failure(&self, dst_node: u64, reason: String, consecutive_failures: u32);
}

/// Event notifier for sending mesh state events to core service
#[derive(Debug, Clone)]
pub struct MeshEventNotifier {
    /// Local node ID
    local_node_id: u64,
    /// Channel to send events for broadcasting
    event_tx: mpsc::UnboundedSender<MeshStateEvent>,
    /// Sequence number for events
    sequence_counter: Arc<std::sync::atomic::AtomicU64>,
}

impl MeshEventNotifier {
    /// Create a new event notifier
    pub fn new(local_node_id: u64, event_tx: mpsc::UnboundedSender<MeshStateEvent>) -> Self {
        Self {
            local_node_id,
            event_tx,
            sequence_counter: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }
    
    /// Get the next sequence number
    fn next_sequence(&self) -> u64 {
        self.sequence_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
    
    /// Create a base event with common fields
    fn create_base_event(&self, event_type: MeshEventType, affected_node: u64) -> MeshStateEvent {
        MeshStateEvent {
            event_type: event_type as i32,
            originator_node: self.local_node_id,
            affected_node,
            sequence_number: self.next_sequence(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            metadata: HashMap::new(),
            payload: vec![],
        }
    }
    
    /// Send an event
    fn send_event(&self, event: MeshStateEvent) {
        if let Err(e) = self.event_tx.send(event.clone()) {
            error!("Failed to send mesh event {:?}: {}", event.event_type, e);
        } else {
            debug!("Sent mesh event {:?} for node {} (seq: {})", 
                   event.event_type, event.affected_node, event.sequence_number);
        }
    }
    
    /// Notify that a session was added (connection established)
    pub fn notify_session_added(&self, peer_node_id: u64, remote_addr: String) {
        let mut event = self.create_base_event(MeshEventType::MeshEventSessionAdded, peer_node_id);
        event.metadata.insert("remote_addr".to_string(), remote_addr);
        event.metadata.insert("event_source".to_string(), "session_manager".to_string());
        
        info!("Notifying session added: node {} connected", peer_node_id);
        self.send_event(event);
    }
    
    /// Notify that a session was removed (connection lost)
    pub fn notify_session_removed(&self, peer_node_id: u64, reason: String) {
        let mut event = self.create_base_event(MeshEventType::MeshEventSessionRemoved, peer_node_id);
        event.metadata.insert("reason".to_string(), reason.clone());
        event.metadata.insert("event_source".to_string(), "session_manager".to_string());
        
        info!("Notifying session removed: node {} disconnected ({})", peer_node_id, reason);
        self.send_event(event);
    }
    
    /// Notify that a session was interrupted (temporary failure)
    pub fn notify_session_interrupted(&self, peer_node_id: u64, reason: String) {
        let mut event = self.create_base_event(MeshEventType::MeshEventSessionInterrupted, peer_node_id);
        event.metadata.insert("reason".to_string(), reason.clone());
        event.metadata.insert("event_source".to_string(), "routing_failure".to_string());
        
        warn!("Notifying session interrupted: node {} ({})", peer_node_id, reason);
        self.send_event(event);
    }
    
    /// Notify that a session was recovered (connection restored)
    pub fn notify_session_recovered(&self, peer_node_id: u64) {
        let mut event = self.create_base_event(MeshEventType::MeshEventSessionRecovered, peer_node_id);
        event.metadata.insert("event_source".to_string(), "topology_update".to_string());
        
        info!("Notifying session recovered: node {} reconnected", peer_node_id);
        self.send_event(event);
    }
    
    /// Notify that a node went offline (all sessions lost)
    pub fn notify_node_offline(&self, node_id: u64, reason: String) {
        let mut event = self.create_base_event(MeshEventType::MeshEventNodeOffline, node_id);
        event.metadata.insert("reason".to_string(), reason.clone());
        event.metadata.insert("event_source".to_string(), "topology_change".to_string());
        
        warn!("Notifying node offline: node {} ({})", node_id, reason);
        self.send_event(event);
    }
    
    /// Notify that a node came back online (topology update received)
    pub fn notify_node_recovered(&self, node_id: u64) {
        let mut event = self.create_base_event(MeshEventType::MeshEventNodeRecovered, node_id);
        event.metadata.insert("event_source".to_string(), "topology_update".to_string());
        
        info!("Notifying node recovered: node {} is back online", node_id);
        self.send_event(event);
    }
    
    /// Notify about routing failure (for session interruption detection)
    pub fn notify_routing_failure(&self, dst_node: u64, reason: String, consecutive_failures: u32) {
        // Only trigger session interruption after multiple consecutive failures
        if consecutive_failures >= 3 {
            self.notify_session_interrupted(dst_node, format!("routing_failure: {}", reason));
        } else {
            debug!("Routing failure to node {} ({}): {} consecutive failures", 
                   dst_node, reason, consecutive_failures);
        }
    }
    
    /// Notify about topology change detection
    pub fn notify_topology_change(&self, change_type: &str, affected_nodes: Vec<u64>) {
        debug!("Topology change detected: {} affecting {} nodes", change_type, affected_nodes.len());
        
        // For now, we'll use this for node offline/recovery detection
        // This could be enhanced to detect specific topology patterns
        for node_id in affected_nodes {
            match change_type {
                "node_disappeared" => {
                    self.notify_node_offline(node_id, "topology_change".to_string());
                }
                "node_appeared" => {
                    self.notify_node_recovered(node_id);
                }
                _ => {
                    debug!("Unknown topology change type: {}", change_type);
                }
            }
        }
    }
}

/// Event processing task that handles mesh events and broadcasts them
pub async fn start_event_processor(
    mut event_rx: mpsc::UnboundedReceiver<MeshStateEvent>,
    data_service: Arc<crate::data::MeshDataService>,
) {
    info!("Starting mesh event processor");
    
    while let Some(event) = event_rx.recv().await {
        debug!("Processing mesh event {:?} for node {} (seq: {})", 
               event.event_type, event.affected_node, event.sequence_number);
        
        // Broadcast the event using the MeshData service
        match data_service.broadcast_state_event(tonic::Request::new(event.clone())).await {
            Ok(_) => {
                debug!("Successfully broadcasted mesh event {:?} (seq: {})", 
                       event.event_type, event.sequence_number);
            }
            Err(e) => {
                error!("Failed to broadcast mesh event {:?}: {}", event.event_type, e);
            }
        }
    }
    
    warn!("Mesh event processor ended");
}

impl MeshEventHandler for MeshEventNotifier {
    fn notify_session_added(&self, peer_node_id: u64, remote_addr: String) {
        self.notify_session_added(peer_node_id, remote_addr);
    }
    
    fn notify_session_removed(&self, peer_node_id: u64, reason: String) {
        self.notify_session_removed(peer_node_id, reason);
    }
    
    fn notify_session_interrupted(&self, peer_node_id: u64, reason: String) {
        self.notify_session_interrupted(peer_node_id, reason);
    }
    
    fn notify_session_recovered(&self, peer_node_id: u64) {
        self.notify_session_recovered(peer_node_id);
    }
    
    fn notify_routing_failure(&self, dst_node: u64, reason: String, consecutive_failures: u32) {
        self.notify_routing_failure(dst_node, reason, consecutive_failures);
    }
}
