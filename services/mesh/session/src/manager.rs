//! Session manager for handling multiple sessions and routing
//!
//! This module provides the SessionManager that coordinates multiple sessions,
//! handles routing decisions, and manages message forwarding between sessions.

use crate::session::SessionEvent;
use crate::failure_tracker::RoutingFailureTracker;
use mesh_routing::{Router, RoutingContext, RoutingDecision, RoutingTable, DropReason};
use mesh_wire::{FrameBuilder, FrameType, TopologyUpdate};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};
use once_cell::sync::Lazy;
use anyhow;

/// Trait for handling mesh events
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

/// Global session registry for message channel management
static GLOBAL_SESSION_REGISTRY: Lazy<Arc<RwLock<HashMap<u64, mpsc::UnboundedSender<OutboundMessage>>>>> = 
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Message to be sent through the mesh
#[derive(Debug, Clone)]
pub struct OutboundMessage {
    /// Source node ID (original sender)
    pub src_node: u64,
    /// Destination node ID
    pub dst_node: u64,
    /// Message payload
    pub payload: Vec<u8>,
    /// Optional headers
    pub headers: HashMap<String, Vec<u8>>,
    /// Correlation ID for tracking
    pub corr_id: u64,
    /// Optional message ID for status tracking
    pub msg_id: Option<u64>,
    /// Whether client acknowledgment is required
    pub require_ack: bool,
}

impl OutboundMessage {
    /// Create a session termination message
    pub fn create_termination_message(local_node_id: u64, target_node_id: u64) -> Self {
        let mut headers = HashMap::new();
        headers.insert("frame_type".to_string(), b"session_terminate".to_vec());
        
        Self {
            src_node: local_node_id,
            dst_node: target_node_id,
            payload: Vec::new(),
            headers,
            corr_id: 0xFFFFFFFFFFFFFFFE, // Reserved corr_id for session termination
            msg_id: None, // Don't track termination messages
            require_ack: false, // Termination messages don't require ack
        }
    }
    
    /// Check if this is a session termination message
    pub fn is_termination_message(&self) -> bool {
        self.corr_id == 0xFFFFFFFFFFFFFFFE &&
        self.headers.get("frame_type")
            .map(|v| v == b"session_terminate")
            .unwrap_or(false)
    }
}

/// Session information for the manager
#[derive(Debug, Clone)]
pub struct SessionInfo {
    /// Remote node ID
    pub remote_node_id: u64,
    /// Remote address
    pub remote_addr: SocketAddr,
    /// Channel to send messages to this session
    pub message_tx: mpsc::UnboundedSender<OutboundMessage>,
}

/// Session manager that coordinates multiple sessions and routing
#[derive(Debug)]
pub struct SessionManager {
    /// Local node ID
    local_node_id: u64,
    /// Routing table
    routing_table: Arc<RoutingTable>,
    /// Active sessions by remote node ID
    sessions: Arc<RwLock<HashMap<u64, SessionInfo>>>,
    /// Channel for receiving outbound messages from gRPC
    outbound_rx: Option<mpsc::UnboundedReceiver<OutboundMessage>>,
    /// Channel for receiving session events
    event_rx: mpsc::Receiver<SessionEvent>,
    /// Channel for local message delivery (to gRPC)
    delivery_tx: Option<mpsc::UnboundedSender<InboundMessage>>,
    /// Channel for receiving topology updates to broadcast
    topology_update_rx: Option<mpsc::UnboundedReceiver<TopologyUpdate>>,
    /// Channel for sending received topology updates to main loop
    received_topology_tx: Option<mpsc::UnboundedSender<TopologyUpdate>>,
    /// Channel for sending routing feedback for message status tracking
    routing_feedback_tx: Option<mpsc::UnboundedSender<RoutingFeedback>>,
    /// Event handler for mesh state changes
    event_handler: Option<Arc<dyn MeshEventHandler>>,
    /// Routing failure tracker for detecting session interruptions
    failure_tracker: Arc<RoutingFailureTracker>,
}

/// Message received from the mesh
#[derive(Debug, Clone)]
pub struct InboundMessage {
    /// Source node ID
    pub src_node: u64,
    /// Destination node ID (should be local)
    pub dst_node: u64,
    /// Message payload
    pub payload: Vec<u8>,
    /// Optional headers
    pub headers: HashMap<String, Vec<u8>>,
    /// Correlation ID
    pub corr_id: u64,
    /// Message ID for status tracking (if available)
    pub msg_id: Option<u64>,
    /// Whether client acknowledgment is required
    pub require_ack: bool,
}

/// Routing feedback for message status tracking
#[derive(Debug, Clone)]
pub struct RoutingFeedback {
    /// Message ID for tracking
    pub msg_id: u64,
    /// Routing decision result
    pub decision: RoutingDecision,
    /// Additional context message
    pub message: String,
}

impl SessionManager {
    /// Create a new session manager
    pub fn new(
        local_node_id: u64,
        routing_table: Arc<RoutingTable>,
        event_rx: mpsc::Receiver<SessionEvent>,
    ) -> Self {
        Self {
            local_node_id,
            routing_table,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            outbound_rx: None,
            event_rx,
            delivery_tx: None,
            topology_update_rx: None,
            received_topology_tx: None,
            routing_feedback_tx: None,
            event_handler: None,
            failure_tracker: Arc::new(RoutingFailureTracker::new(3, Duration::from_secs(30))),
        }
    }

    /// Set the outbound message receiver (from gRPC)
    pub fn set_outbound_receiver(&mut self, rx: mpsc::UnboundedReceiver<OutboundMessage>) {
        self.outbound_rx = Some(rx);
    }

    /// Set the delivery sender (to gRPC)
    pub fn set_delivery_sender(&mut self, tx: mpsc::UnboundedSender<InboundMessage>) {
        self.delivery_tx = Some(tx);
    }

    /// Set the topology update receiver
    pub fn set_topology_update_receiver(&mut self, rx: mpsc::UnboundedReceiver<TopologyUpdate>) {
        self.topology_update_rx = Some(rx);
    }

    /// Set the received topology update sender
    pub fn set_received_topology_sender(&mut self, tx: mpsc::UnboundedSender<TopologyUpdate>) {
        self.received_topology_tx = Some(tx);
    }
    
    /// Set the routing feedback sender
    pub fn set_routing_feedback_sender(&mut self, tx: mpsc::UnboundedSender<RoutingFeedback>) {
        self.routing_feedback_tx = Some(tx);
    }
    
    /// Set the event handler for mesh state changes
    pub fn set_event_handler<T>(&mut self, handler: Arc<T>) 
    where 
        T: MeshEventHandler + 'static,
    {
        self.event_handler = Some(handler);
    }

    /// Run the session manager
    pub async fn run(mut self) -> anyhow::Result<()> {
        info!("Starting session manager for node {}", self.local_node_id);

        let mut outbound_rx = self.outbound_rx.take()
            .ok_or_else(|| anyhow::anyhow!("Outbound receiver not set"))?;

        let mut topology_update_rx = self.topology_update_rx.take();

        loop {
            tokio::select! {
                // Handle outbound messages from gRPC
                Some(message) = outbound_rx.recv() => {
                    if let Err(e) = self.handle_outbound_message(message).await {
                        error!("Failed to handle outbound message: {}", e);
                    }
                }

                // Handle session events
                Some(event) = self.event_rx.recv() => {
                    if let Err(e) = self.handle_session_event(event).await {
                        error!("Failed to handle session event: {}", e);
                    }
                }

                // Handle topology updates to broadcast
                Some(topology_update) = async {
                    match &mut topology_update_rx {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Err(e) = self.broadcast_topology_update(topology_update).await {
                        error!("Failed to broadcast topology update: {}", e);
                    }
                }

                else => {
                    info!("Session manager shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Handle an outbound message from gRPC
    async fn handle_outbound_message(&self, message: OutboundMessage) -> anyhow::Result<()> {
        debug!("Handling outbound message to node {}", message.dst_node);

        // Check if destination is local
        if message.dst_node == self.local_node_id {
            warn!("Received message for local node, delivering locally");
            self.deliver_locally(InboundMessage {
                src_node: message.src_node,  // Use original sender, not local node
                dst_node: message.dst_node,
                payload: message.payload,
                headers: message.headers,
                corr_id: message.corr_id,
                msg_id: message.msg_id, // Preserve message ID for tracking
                require_ack: message.require_ack, // Preserve acknowledgment requirement
            }).await?;
            return Ok(());
        }

        // Make routing decision
        let routing_context = RoutingContext {
            src_node: self.local_node_id,
            dst_node: message.dst_node,
            ttl: 64, // Default TTL
            corr_id: message.corr_id,
            route_class: 0, // Default route class
            partition: 0, // Default partition
            epoch: 0, // Current epoch
        };

        let decision = self.routing_table.decide(&routing_context).await;
        
        // Send routing feedback if message has an ID
        if let Some(msg_id) = message.msg_id {
            self.send_routing_feedback(msg_id, decision.clone()).await;
        }
        
        match decision {
            RoutingDecision::Forward(ecmp_decision) => {
                let next_hop = ecmp_decision.next_hop.node_id;
                debug!("Forwarding message to next hop: {}", next_hop);
                
                // Find session for next hop
                let sessions = self.sessions.read().await;
                if let Some(session_info) = sessions.get(&next_hop) {
                    // Send message to session
                    if let Err(e) = session_info.message_tx.send(message) {
                        error!("Failed to send message to session {}: {}", next_hop, e);
                        
                        // Record routing failure
                        let (failure_count, should_notify) = self.failure_tracker.record_failure(next_hop).await;
                        if should_notify {
                            if let Some(ref handler) = self.event_handler {
                                handler.notify_routing_failure(next_hop, "session_send_failed".to_string(), failure_count);
                            }
                        }
                    } else {
                        // Record successful routing
                        let was_interrupted = self.failure_tracker.record_success(next_hop).await;
                        if was_interrupted {
                            if let Some(ref handler) = self.event_handler {
                                handler.notify_session_recovered(next_hop);
                            }
                        }
                    }
                } else {
                    warn!("No session found for next hop node {}", next_hop);
                    
                    // Record routing failure for missing session
                    let (failure_count, should_notify) = self.failure_tracker.record_failure(next_hop).await;
                    if should_notify {
                        if let Some(ref handler) = self.event_handler {
                            handler.notify_routing_failure(next_hop, "no_session".to_string(), failure_count);
                        }
                    }
                }
            }
            RoutingDecision::Local => {
                // This shouldn't happen as we checked above, but handle it
                self.deliver_locally(InboundMessage {
                    src_node: message.src_node,  // Use original sender, not local node
                    dst_node: message.dst_node,
                    payload: message.payload,
                    headers: message.headers,
                    corr_id: message.corr_id,
                    msg_id: message.msg_id, // Preserve message ID for tracking
                    require_ack: message.require_ack, // Preserve acknowledgment requirement
                }).await?;
            }
            RoutingDecision::Drop(reason) => {
                warn!("Dropping message to node {}: {:?}", message.dst_node, reason);
                
                // Record routing failure for dropped messages
                if matches!(reason, DropReason::NoRoute) {
                    let (failure_count, should_notify) = self.failure_tracker.record_failure(message.dst_node).await;
                    if should_notify {
                        if let Some(ref handler) = self.event_handler {
                            handler.notify_routing_failure(message.dst_node, format!("routing_drop: {}", reason), failure_count);
                        }
                    }
                }
            }
        }

        Ok(())
    }
    
    /// Send routing feedback for message status tracking
    async fn send_routing_feedback(&self, msg_id: u64, decision: RoutingDecision) {
        if let Some(ref tx) = self.routing_feedback_tx {
            let feedback = RoutingFeedback {
                msg_id,
                decision: decision.clone(),
                message: match decision {
                    RoutingDecision::Forward(ref ecmp) => {
                        format!("Message forwarded to next hop node {}", ecmp.next_hop.node_id)
                    }
                    RoutingDecision::Local => {
                        "Message delivered locally".to_string()
                    }
                    RoutingDecision::Drop(ref reason) => {
                        format!("Message dropped: {}", reason)
                    }
                },
            };
            
            if let Err(e) = tx.send(feedback) {
                warn!("Failed to send routing feedback for message {}: {}", msg_id, e);
            }
        }
    }

    /// Handle a session event
    async fn handle_session_event(&self, event: SessionEvent) -> anyhow::Result<()> {
        match event {
            SessionEvent::Connected { peer, remote_node_id } => {
                info!("Session connected to node {} at {}", remote_node_id, peer);
                
                // Get the message channel from the global registry
                if let Some(message_tx) = get_global_session_channel(remote_node_id).await {
                    let session_info = SessionInfo {
                        remote_node_id,
                        remote_addr: peer,
                        message_tx,
                    };
                    
                    let mut sessions = self.sessions.write().await;
                    sessions.insert(remote_node_id, session_info);
                    info!("Auto-registered session for node {} at {} with existing channel", remote_node_id, peer);
                    drop(sessions); // Release lock before async call
                    
                    // Notify about session added
                    if let Some(ref handler) = self.event_handler {
                        handler.notify_session_added(remote_node_id, peer.to_string());
                    }
                    
                    info!("Topology changed: new neighbor {}", remote_node_id);
                } else {
                    warn!("No message channel found for node {} in global registry", remote_node_id);
                }
            }
            SessionEvent::Disconnected { remote_node_id } => {
                if let Some(node_id) = remote_node_id {
                    info!("Session disconnected from node {}", node_id);
                    let mut sessions = self.sessions.write().await;
                    sessions.remove(&node_id);
                    drop(sessions); // Release lock before async call
                    
                    // Notify about session removed
                    if let Some(ref handler) = self.event_handler {
                        handler.notify_session_removed(node_id, "session_disconnected".to_string());
                    }
                    
                    info!("Topology changed: removed neighbor {}", node_id);
                }
            }
            SessionEvent::Pong { remote_node_id, rtt } => {
                debug!("Received pong from node {} (RTT: {:?})", remote_node_id, rtt);
            }
            SessionEvent::MessageReceived { message } => {
                debug!("Received message from node {}", message.src_node);
                
                // Check if message is for local node
                if message.dst_node == self.local_node_id {
                    // Convert to InboundMessage with msg_id preserved if available
                    let inbound_message = InboundMessage {
                        src_node: message.src_node,
                        dst_node: message.dst_node,
                        payload: message.payload,
                        headers: message.headers,
                        corr_id: message.corr_id,
                        msg_id: message.msg_id, // Preserve message ID if available
                        require_ack: message.require_ack, // Preserve acknowledgment requirement
                    };
                    self.deliver_locally(inbound_message).await?;
                } else {
                    // Forward the message (preserve original src_node)
                    let outbound = OutboundMessage {
                        src_node: message.src_node,  // Preserve original sender
                        dst_node: message.dst_node,
                        payload: message.payload,
                        headers: message.headers,
                        corr_id: message.corr_id,
                        msg_id: message.msg_id, // Preserve message ID for forwarded messages too
                        require_ack: message.require_ack, // Preserve acknowledgment requirement
                    };
                    self.handle_outbound_message(outbound).await?;
                }
            }
        SessionEvent::TopologyUpdate { update } => {
            debug!("Received topology update from node {} (seq: {})",
                   update.originator_node, update.sequence_number);
            info!("Received topology update from node {} (seq: {}, {} neighbors)",
                  update.originator_node, update.sequence_number, update.neighbors.len());
            
            // Forward topology update to main event loop for processing
            if let Some(ref tx) = self.received_topology_tx {
                if let Err(e) = tx.send(update) {
                    warn!("Failed to forward received topology update: {}", e);
                }
            }
        }
            SessionEvent::TopologyRequest { request } => {
                debug!("Received topology request from node {} (target: {:?})", 
                       request.requesting_node, request.target_node);
                // TODO: Handle topology request
                // This will be implemented when we connect to the TopologyDatabase
            }
        }

        Ok(())
    }

    /// Deliver a message locally (to gRPC)
    async fn deliver_locally(&self, message: InboundMessage) -> anyhow::Result<()> {
        if let Some(delivery_tx) = &self.delivery_tx {
            if let Err(e) = delivery_tx.send(message) {
                error!("Failed to deliver message locally: {}", e);
            }
        } else {
            warn!("No delivery channel configured, dropping local message");
        }
        Ok(())
    }

    /// Broadcast a topology update to all connected sessions
    pub async fn broadcast_topology_update(&self, topology_update: TopologyUpdate) -> anyhow::Result<()> {
        let sessions = self.sessions.read().await;
        
        if sessions.is_empty() {
            debug!("No sessions to broadcast topology update to");
            return Ok(());
        }

        // Serialize topology update to CBOR
        let payload = match serde_cbor::to_vec(&topology_update) {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to serialize topology update: {}", e);
                return Err(e.into());
            }
        };

        info!("Broadcasting topology update (seq: {}) to {} sessions", 
              topology_update.sequence_number, sessions.len());

        // Send to all connected sessions as TopologyUpdate messages
        let mut sent_count = 0;
        for (node_id, session_info) in sessions.iter() {
            // Create a special outbound message for topology updates
            // We'll use a reserved correlation ID to indicate this is a topology update
            let outbound_msg = OutboundMessage {
                src_node: self.local_node_id,  // Topology updates originate from local node
                dst_node: *node_id,
                payload: payload.clone(),
                headers: {
                    let mut headers = HashMap::new();
                    headers.insert("frame_type".to_string(), b"topology_update".to_vec());
                    headers
                },
                corr_id: 0xFFFFFFFFFFFFFFFF, // Reserved corr_id for topology updates
                msg_id: None, // Don't track topology update messages
                require_ack: false, // Topology updates don't require ack
            };

            if let Err(e) = session_info.message_tx.send(outbound_msg) {
                warn!("Failed to send topology update to node {}: {}", node_id, e);
            } else {
                sent_count += 1;
            }
        }

        info!("Sent topology update to {} sessions", sent_count);
        Ok(())
    }


    /// Register a new session
    pub async fn register_session(
        &self,
        remote_node_id: u64,
        remote_addr: SocketAddr,
        message_tx: mpsc::UnboundedSender<OutboundMessage>,
    ) {
        let session_info = SessionInfo {
            remote_node_id,
            remote_addr,
            message_tx,
        };

        let mut sessions = self.sessions.write().await;
        sessions.insert(remote_node_id, session_info);
        info!("Registered session for node {} at {}", remote_node_id, remote_addr);
    }

    /// Get session information
    pub async fn get_sessions(&self) -> HashMap<u64, SessionInfo> {
        self.sessions.read().await.clone()
    }

    /// Get shared session registry for external session registration
    pub fn get_session_registry(&self) -> Arc<RwLock<HashMap<u64, SessionInfo>>> {
        self.sessions.clone()
    }
}

/// Register a session with the session registry
pub async fn register_session_with_registry(
    registry: &Arc<RwLock<HashMap<u64, SessionInfo>>>,
    remote_node_id: u64,
    remote_addr: SocketAddr,
    message_tx: mpsc::UnboundedSender<OutboundMessage>,
) {
    let session_info = SessionInfo {
        remote_node_id,
        remote_addr,
        message_tx,
    };

    let mut sessions = registry.write().await;
    sessions.insert(remote_node_id, session_info);
    info!("Registered session for node {} at {}", remote_node_id, remote_addr);
}

/// Unregister a session from the session registry
pub async fn unregister_session_with_registry(
    registry: &Arc<RwLock<HashMap<u64, SessionInfo>>>,
    remote_node_id: u64,
) {
    let mut sessions = registry.write().await;
    if sessions.remove(&remote_node_id).is_some() {
        info!("Unregistered session for node {}", remote_node_id);
    }
}

/// Register a session's message channel in the global registry
pub async fn register_global_session_channel(
    node_id: u64,
    message_tx: mpsc::UnboundedSender<OutboundMessage>,
) {
    let mut registry = GLOBAL_SESSION_REGISTRY.write().await;
    registry.insert(node_id, message_tx);
    info!("Registered global message channel for node {}", node_id);
}

/// Unregister a session's message channel from the global registry
pub async fn unregister_global_session_channel(node_id: u64) {
    let mut registry = GLOBAL_SESSION_REGISTRY.write().await;
    if registry.remove(&node_id).is_some() {
        info!("Unregistered global message channel for node {}", node_id);
    }
}

/// Get a message sender for a specific node from the global registry
pub async fn get_global_session_channel(node_id: u64) -> Option<mpsc::UnboundedSender<OutboundMessage>> {
    let registry = GLOBAL_SESSION_REGISTRY.read().await;
    registry.get(&node_id).cloned()
}

/// Build a frame from an outbound message (Data or TopologyUpdate)
pub fn build_data_frame(
    _local_node_id: u64,  // Not used - we use message.src_node instead
    message: &OutboundMessage,
) -> anyhow::Result<Vec<u8>> {
    use mesh_wire::FastHeader;
    use bytes::Bytes;
    
    // Check if this is a topology update based on headers
    let frame_type = if message.headers.get("frame_type")
        .map(|v| v == b"topology_update")
        .unwrap_or(false) {
        FrameType::TopologyUpdate
    } else {
        FrameType::Data
    };
    
    let msg_id = message.msg_id.unwrap_or(0); // Use actual message ID or 0 as fallback
    let mut fast_header = FastHeader::new(frame_type, message.src_node, message.dst_node, msg_id);
    fast_header.corr_id = message.corr_id;
    let mut builder = FrameBuilder::new(fast_header);
    
    // Add require_ack as metadata if true
    if message.require_ack {
        builder = builder.meta_insert_str("require_ack", "true");
    }
    
    // Add headers as metadata (except the special frame_type header)
    for (key, value) in &message.headers {
        if key != "frame_type" {
            builder = builder.meta_insert_bytes(key, value);
        }
    }
    
    // Set payload
    let payload_bytes = Bytes::from(message.payload.clone());
    builder = builder.payload(payload_bytes);
    
    // Build with max frame size (64KB)
    let frame_bytes = builder.build(65536)?;
    Ok(frame_bytes.to_vec())
}
