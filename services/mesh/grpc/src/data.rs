//! MeshData gRPC service implementation

use crate::delivery::{DeliveryQueue, SubscriptionFilter};
use crate::message_tracker::MessageTracker;
use crate::message_queue::MessageQueue;
use crate::proto::mesh::v1::{
    mesh_data_server::MeshData, Ack, MessageStatus, MessageStatusInfo, QueryMessageStatusRequest, 
    QueryMessageStatusResponse, Received, SendRequest, SendResponse, SubscribeRequest, SendMode,
    MeshStateEvent, DatabaseSyncRequest, DatabaseSyncResponse,
};
use mesh_session::manager::RoutingFeedback;
use mesh_topology::TopologyDatabase;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Result, Status};
use tracing::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json;

/// Message ID generator
#[derive(Debug)]
pub struct MessageIdGenerator {
    next_id: std::sync::atomic::AtomicU64,
}

impl MessageIdGenerator {
    /// Create a new message ID generator
    pub fn new() -> Self {
        Self {
            next_id: std::sync::atomic::AtomicU64::new(1),
        }
    }
    
    /// Generate the next message ID
    pub fn next(&self) -> u64 {
        self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

impl Default for MessageIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// MeshData service implementation
#[derive(Debug)]
pub struct MeshDataService {
    /// Local node ID
    node_id: u64,
    /// Message ID generator
    msg_id_gen: MessageIdGenerator,
    /// Local delivery queue
    delivery_queue: Arc<DeliveryQueue>,
    /// Channel to send outbound messages to the mesh
    outbound_tx: mpsc::UnboundedSender<OutboundMessage>,
    /// Acknowledged messages for app-level idempotency
    acked_messages: Arc<DashMap<(u64, u64), ()>>, // (src_node, msg_id) -> ()
    /// Message status tracker
    message_tracker: Arc<MessageTracker>,
    /// Channel for receiving routing feedback
    routing_feedback_rx: Option<mpsc::UnboundedReceiver<RoutingFeedback>>,
    /// Topology database for node existence validation
    topology_db: Option<Arc<tokio::sync::RwLock<TopologyDatabase>>>,
    /// Message queue for handling retries and delivery modes
    message_queue: Arc<MessageQueue>,
}

/// Re-export OutboundMessage from mesh-session
pub use mesh_session::OutboundMessage;

/// Delivery status message sent back to source node
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeliveryStatusMessage {
    /// Original message ID that this status refers to
    original_msg_id: u64,
    /// Delivery status
    status: i32,
    /// Status message
    status_message: String,
}

impl MeshDataService {
    /// Create a new MeshData service
    pub fn new(
        node_id: u64,
        delivery_queue: Arc<DeliveryQueue>,
        outbound_tx: mpsc::UnboundedSender<OutboundMessage>,
        message_tracker: Arc<MessageTracker>,
        message_queue: Arc<MessageQueue>,
    ) -> Self {
        Self {
            node_id,
            msg_id_gen: MessageIdGenerator::new(),
            delivery_queue,
            outbound_tx,
            acked_messages: Arc::new(DashMap::new()),
            message_tracker,
            routing_feedback_rx: None,
            topology_db: None,
            message_queue,
        }
    }
    
    /// Set the routing feedback receiver
    pub fn set_routing_feedback_receiver(&mut self, rx: mpsc::UnboundedReceiver<RoutingFeedback>) {
        self.routing_feedback_rx = Some(rx);
    }
    
    /// Set the topology database
    pub fn set_topology_db(&mut self, db: Arc<tokio::sync::RwLock<TopologyDatabase>>) {
        self.topology_db = Some(db);
    }
    
    /// Start the routing feedback processing task
    pub fn start_routing_feedback_task(&mut self) {
        if let Some(mut rx) = self.routing_feedback_rx.take() {
            let message_tracker = Arc::clone(&self.message_tracker);
            
            tokio::spawn(async move {
                info!("Starting routing feedback processing task");
                
                while let Some(feedback) = rx.recv().await {
                    Self::handle_routing_feedback(&message_tracker, feedback).await;
                }
                
                info!("Routing feedback processing task ended");
            });
        }
    }
    
    /// Handle routing feedback from SessionManager
    async fn handle_routing_feedback(message_tracker: &MessageTracker, feedback: RoutingFeedback) {
        use mesh_routing::{RoutingDecision, DropReason};
        
        let (status, message) = match feedback.decision {
            RoutingDecision::Forward(_) => {
                // Message is being forwarded - no status change needed yet
                return;
            }
            RoutingDecision::Local => {
                // Message delivered locally - this should be handled by delivery tracking
                return;
            }
            RoutingDecision::Drop(reason) => {
                match reason {
                    DropReason::NoRoute => (
                        MessageStatus::PendingNode,
                        "No route to destination node - node may be offline".to_string()
                    ),
                    DropReason::InvalidDestination => (
                        MessageStatus::Undeliverable,
                        "Invalid destination node".to_string()
                    ),
                    DropReason::TtlExpired => (
                        MessageStatus::Undeliverable,
                        "Message TTL expired during routing".to_string()
                    ),
                    DropReason::RoutingLoop => (
                        MessageStatus::Undeliverable,
                        "Routing loop detected".to_string()
                    ),
                    DropReason::AdminProhibited => (
                        MessageStatus::Undeliverable,
                        "Message delivery administratively prohibited".to_string()
                    ),
                }
            }
        };
        
        message_tracker.update_status(feedback.msg_id, status, message);
        debug!("Updated message {} status based on routing feedback", feedback.msg_id);
    }
    
    /// Check if a destination node exists in the topology
    async fn is_node_known(&self, dst_node: u64) -> bool {
        // Check local node
        if dst_node == self.node_id {
            return true;
        }
        
        // Check topology database if available
        if let Some(ref topology_db) = self.topology_db {
            let db = topology_db.read().await;
            // Check if the node exists in the topology database
            db.get_nodes().contains_key(&dst_node)
        } else {
            // Fallback: assume all non-zero nodes are potentially valid
            // This maintains backward compatibility when topology DB is not available
            dst_node != 0
        }
    }
    
    /// Handle an incoming message from the mesh network
    pub async fn handle_incoming_message(&self, message: Received) {
        info!(
            "Handling incoming message {} from node {} to node {} (require_ack: {})",
            message.msg_id, message.src_node, message.dst_node, message.require_ack
        );
        
        // Check if this message is for us
        if message.dst_node != self.node_id {
            warn!(
                "Received message {} for node {} but we are node {}",
                message.msg_id, message.dst_node, self.node_id
            );
            return;
        }
        
        // Check if this is a delivery status message
        for header in &message.headers {
            if header.key == "message_type" && header.value == b"delivery_status" {
                self.handle_delivery_status_message(&message).await;
                return;
            }
        }
        
        // Check if there are any subscribers
        let subscriber_count = self.delivery_queue.subscriber_count();
        if subscriber_count == 0 {
            // No subscribers - send status back to source node (only if we have a message ID)
            if message.msg_id != 0 {
                // Send delivery status back to source node
                self.send_delivery_status_to_source(
                    message.src_node,
                    message.msg_id,
                    MessageStatus::PendingClient,
                    "No active subscribers to deliver message to".to_string(),
                ).await;
            }
            warn!(
                "No subscribers available for message {} from node {}",
                message.msg_id, message.src_node
            );
            return;
        }
        
        // Deliver to local subscribers and get delivery count
        let delivered_count = self.delivery_queue.deliver(message.clone()).await;
        
        info!(
            "Message {} delivered to {} subscribers (msg_id: {}, require_ack: {})",
            message.corr_id, delivered_count, message.msg_id, message.require_ack
        );
        
        if delivered_count > 0 {
            // Send delivery status back to source node (only if we have a message ID)
            if message.msg_id != 0 {
                if message.require_ack {
                    // Send delivery status back to source node
                    self.send_delivery_status_to_source(
                        message.src_node,
                        message.msg_id,
                        MessageStatus::WaitingForClientAck,
                        format!("Message delivered to {} subscribers, waiting for acknowledgment", delivered_count),
                    ).await;
                } else {
                    // Send delivery status back to source node
                    self.send_delivery_status_to_source(
                        message.src_node,
                        message.msg_id,
                        MessageStatus::Delivered,
                        format!("Message delivered successfully to {} subscribers", delivered_count),
                    ).await;
                }
            }
        } else {
            // No matching subscribers - send status back to source node (only if we have a message ID)
            if message.msg_id != 0 {
                // Send delivery status back to source node
                self.send_delivery_status_to_source(
                    message.src_node,
                    message.msg_id,
                    MessageStatus::PendingClient,
                    "No subscribers matched the message filters".to_string(),
                ).await;
            }
        }
    }
    
    /// Handle a delivery status message from another node
    async fn handle_delivery_status_message(&self, message: &Received) {
        // Parse the delivery status message
        let delivery_status: DeliveryStatusMessage = match serde_json::from_slice(&message.payload) {
            Ok(status) => status,
            Err(e) => {
                warn!("Failed to parse delivery status message from node {}: {}", message.src_node, e);
                return;
            }
        };
        
        debug!(
            "Received delivery status for message {} from node {}: status={}, message={}",
            delivery_status.original_msg_id,
            message.src_node,
            delivery_status.status,
            delivery_status.status_message
        );
        
        // Convert status code back to MessageStatus
        let status = match delivery_status.status {
            x if x == MessageStatus::Delivered as i32 => MessageStatus::Delivered,
            x if x == MessageStatus::PendingClient as i32 => MessageStatus::PendingClient,
            x if x == MessageStatus::WaitingForClientAck as i32 => MessageStatus::WaitingForClientAck,
            x if x == MessageStatus::Undeliverable as i32 => MessageStatus::Undeliverable,
            _ => {
                warn!("Unknown delivery status code: {}", delivery_status.status);
                return;
            }
        };
        
        // Update message status and notify message queue
        self.message_tracker.update_status(
            delivery_status.original_msg_id,
            status,
            delivery_status.status_message.clone(),
        );
        
        // Notify message queue about the status update
        self.message_queue.handle_routing_feedback(
            delivery_status.original_msg_id,
            status,
            delivery_status.status_message,
        ).await;
        
        info!(
            "Updated message {} status to {:?} based on delivery feedback from node {}",
            delivery_status.original_msg_id,
            status,
            message.src_node
        );
    }
    
    /// Send delivery status back to the source node
    async fn send_delivery_status_to_source(
        &self,
        src_node: u64,
        msg_id: u64,
        status: MessageStatus,
        status_message: String,
    ) {
        // Don't send status back to ourselves
        if src_node == self.node_id {
            return;
        }
        
        // Create a delivery status message to send back to source
        let delivery_status = OutboundMessage {
            src_node: self.node_id,
            dst_node: src_node,
            payload: serde_json::to_vec(&DeliveryStatusMessage {
                original_msg_id: msg_id,
                status: status as i32,
                status_message,
            }).unwrap_or_default(),
            headers: {
                let mut headers = std::collections::HashMap::new();
                headers.insert("message_type".to_string(), b"delivery_status".to_vec());
                headers
            },
            corr_id: 0, // Use 0 for internal messages
            msg_id: None, // Don't track delivery status messages
            require_ack: false,
        };
        
        // Send the delivery status back to source node
        if let Err(e) = self.outbound_tx.send(delivery_status) {
            warn!("Failed to send delivery status back to source node {}: {}", src_node, e);
        } else {
            info!("Sent delivery status for message {} back to source node {}", msg_id, src_node);
        }
    }
    
    /// Wait for a message to reach one of the specified statuses
    async fn wait_for_status(
        &self,
        msg_id: u64,
        target_statuses: &[MessageStatus],
        timeout_duration: Duration,
    ) -> Result<MessageStatusInfo, String> {
        let start_time = std::time::Instant::now();
        
        loop {
            // Check current status
            if let Some(record) = self.message_tracker.get_status(msg_id) {
                if target_statuses.contains(&record.status) {
                    return Ok(MessageStatusInfo {
                        msg_id: record.msg_id,
                        status: record.status as i32,
                        status_message: record.status_message.clone(),
                        timestamp: record.timestamp,
                        require_ack: record.require_ack,
                    });
                }
            }
            
            // Check timeout
            if start_time.elapsed() >= timeout_duration {
                return Err("Timeout waiting for message status".to_string());
            }
            
            // Wait a bit before checking again
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    
    /// Get statistics about the service
    pub fn get_stats(&self) -> MeshDataStats {
        MeshDataStats {
            node_id: self.node_id,
            subscriber_count: self.delivery_queue.subscriber_count(),
            acked_message_count: self.acked_messages.len(),
        }
    }
}

/// Statistics about the MeshData service
#[derive(Debug, Clone)]
pub struct MeshDataStats {
    /// Local node ID
    pub node_id: u64,
    /// Number of active subscribers
    pub subscriber_count: usize,
    /// Number of acknowledged messages
    pub acked_message_count: usize,
}

#[tonic::async_trait]
impl MeshData for MeshDataService {
    async fn send(&self, request: Request<SendRequest>) -> Result<Response<SendResponse>> {
        let req = request.into_inner();
        
        debug!(
            "Send request: dst_node={}, mode={:?}, timeout={}s, payload_len={}",
            req.dst_node,
            req.mode(),
            req.timeout_seconds,
            req.payload.len()
        );
        
        // Validate request
        if req.dst_node == 0 {
            return Err(Status::invalid_argument("dst_node cannot be 0"));
        }
        
        if req.payload.is_empty() {
            return Err(Status::invalid_argument("payload cannot be empty"));
        }
        
        // Check if destination node is known in the topology
        if !self.is_node_known(req.dst_node).await {
            let msg_id = self.msg_id_gen.next();
            
            // Track as undeliverable
            self.message_tracker.track_message(
                msg_id,
                MessageStatus::Undeliverable,
                format!("Destination node {} is not known in the mesh topology", req.dst_node),
                req.require_ack,
            );
            
            return Ok(Response::new(SendResponse {
                msg_id,
                status: MessageStatus::Undeliverable as i32,
                status_message: format!("Destination node {} is not known in the mesh topology", req.dst_node),
                require_ack: req.require_ack,
            }));
        }
        
        // Generate message ID
        let msg_id = self.msg_id_gen.next();
        
        // Store values before moving req
        let send_mode = req.mode();
        let timeout_seconds = req.timeout_seconds;
        let require_ack = req.require_ack;
        let dst_node = req.dst_node;
        
        // Track message as queued initially
        self.message_tracker.track_message(
            msg_id,
            MessageStatus::Queued,
            "Message queued for delivery".to_string(),
            require_ack,
        );
        
        // Convert headers
        let headers: HashMap<String, Vec<u8>> = req
            .headers
            .into_iter()
            .map(|h| (h.key, h.value))
            .collect();
        
        // Create outbound message
        let outbound_msg = OutboundMessage {
            src_node: self.node_id,  // Set original sender to local node ID
            dst_node,
            payload: req.payload,
            headers,
            corr_id: req.corr_id,
            msg_id: Some(msg_id), // Include message ID for tracking
            require_ack, // Include acknowledgment requirement
        };
        
        // Handle different send modes
        match send_mode {
            SendMode::FireAndForget => {
                // Fire and forget: queue message and return immediately
                if let Err(e) = self.message_queue.queue_message(
                    outbound_msg,
                    SendMode::FireAndForget,
                    timeout_seconds,
                    None, // No status streaming for fire-and-forget
                ).await {
                    error!("Failed to queue message: {}", e);
                    
                    self.message_tracker.update_status(
                        msg_id,
                        MessageStatus::Undeliverable,
                        format!("Failed to queue message: {}", e),
                    );
                    
                    return Err(Status::internal("Failed to queue message"));
                }
                
                info!("Message {} queued for fire-and-forget delivery to node {}", msg_id, dst_node);
                
                Ok(Response::new(SendResponse {
                    msg_id,
                    status: MessageStatus::Queued as i32,
                    status_message: "Message queued for delivery".to_string(),
                    require_ack,
                }))
            }
            
            SendMode::WaitForDelivery => {
                // Wait for delivery: queue message and wait for delivery confirmation
                if let Err(e) = self.message_queue.queue_message(
                    outbound_msg,
                    SendMode::WaitForDelivery,
                    timeout_seconds,
                    None, // No status streaming for wait mode
                ).await {
                    error!("Failed to queue message: {}", e);
                    
                    self.message_tracker.update_status(
                        msg_id,
                        MessageStatus::Undeliverable,
                        format!("Failed to queue message: {}", e),
                    );
                    
                    return Err(Status::internal("Failed to queue message"));
                }
                
                // Wait for delivery with timeout
                let timeout_duration = if timeout_seconds > 0 {
                    Duration::from_secs(timeout_seconds as u64)
                } else {
                    Duration::from_secs(300) // Default 5 minutes
                };
                
                match self.wait_for_status(msg_id, &[MessageStatus::Delivered, MessageStatus::PendingClient, MessageStatus::Undeliverable], timeout_duration).await {
                    Ok(status_info) => {
                        info!("Message {} delivered with status: {:?}", msg_id, status_info.status);
                        
                        Ok(Response::new(SendResponse {
                            msg_id,
                            status: status_info.status,
                            status_message: status_info.status_message,
                            require_ack,
                        }))
                    }
                    Err(e) => {
                        warn!("Timeout waiting for delivery of message {}: {}", msg_id, e);
                        
                        // Get current status
                        if let Some(record) = self.message_tracker.get_status(msg_id) {
                            Ok(Response::new(SendResponse {
                                msg_id,
                                status: record.status as i32,
                                status_message: format!("Timeout: {}", record.status_message),
                                require_ack,
                            }))
                        } else {
                            Err(Status::deadline_exceeded("Timeout waiting for message delivery"))
                        }
                    }
                }
            }
            
            SendMode::WaitForAck => {
                // Wait for acknowledgment: queue message and wait for client ack
                if !require_ack {
                    return Err(Status::invalid_argument("Cannot wait for ack when require_ack is false"));
                }
                
                if let Err(e) = self.message_queue.queue_message(
                    outbound_msg,
                    SendMode::WaitForAck,
                    timeout_seconds,
                    None, // No status streaming for wait mode
                ).await {
                    error!("Failed to queue message: {}", e);
                    
                    self.message_tracker.update_status(
                        msg_id,
                        MessageStatus::Undeliverable,
                        format!("Failed to queue message: {}", e),
                    );
                    
                    return Err(Status::internal("Failed to queue message"));
                }
                
                // Wait for acknowledgment with timeout
                let timeout_duration = if timeout_seconds > 0 {
                    Duration::from_secs(timeout_seconds as u64)
                } else {
                    Duration::from_secs(600) // Default 10 minutes for ack
                };
                
                match self.wait_for_status(msg_id, &[MessageStatus::AckSuccess, MessageStatus::AckFailure, MessageStatus::Undeliverable], timeout_duration).await {
                    Ok(status_info) => {
                        info!("Message {} acknowledged with status: {:?}", msg_id, status_info.status);
                        
                        Ok(Response::new(SendResponse {
                            msg_id,
                            status: status_info.status,
                            status_message: status_info.status_message,
                            require_ack,
                        }))
                    }
                    Err(e) => {
                        warn!("Timeout waiting for acknowledgment of message {}: {}", msg_id, e);
                        
                        // Get current status
                        if let Some(record) = self.message_tracker.get_status(msg_id) {
                            Ok(Response::new(SendResponse {
                                msg_id,
                                status: record.status as i32,
                                status_message: format!("Timeout: {}", record.status_message),
                                require_ack,
                            }))
                        } else {
                            Err(Status::deadline_exceeded("Timeout waiting for message acknowledgment"))
                        }
                    }
                }
            }
        }
    }

    async fn send_with_status_stream(
        &self,
        request: Request<SendRequest>,
    ) -> Result<Response<Self::SendWithStatusStreamStream>> {
        let req = request.into_inner();
        
        debug!(
            "SendWithStatusStream request: dst_node={}, mode={:?}",
            req.dst_node,
            req.mode()
        );
        
        // Validate request
        if req.dst_node == 0 {
            return Err(Status::invalid_argument("dst_node cannot be 0"));
        }
        
        if req.payload.is_empty() {
            return Err(Status::invalid_argument("payload cannot be empty"));
        }
        
        // Create a channel for streaming status updates
        let (grpc_status_tx, status_rx) = mpsc::unbounded_channel();
        let (internal_status_tx, mut internal_status_rx) = mpsc::unbounded_channel();
        
        // Spawn a task to convert internal status updates to gRPC format
        let grpc_tx_clone = grpc_status_tx.clone();
        tokio::spawn(async move {
            while let Some(status_info) = internal_status_rx.recv().await {
                if grpc_tx_clone.send(Ok(status_info)).is_err() {
                    break; // Client disconnected
                }
            }
        });
        
        // Check if destination node is known in the topology
        if !self.is_node_known(req.dst_node).await {
            let msg_id = self.msg_id_gen.next();
            
            // Send undeliverable status and close stream
            let undeliverable_status = MessageStatusInfo {
                msg_id,
                status: MessageStatus::Undeliverable as i32,
                status_message: format!("Destination node {} is not known in the mesh topology", req.dst_node),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                require_ack: req.require_ack,
            };
            
            let _ = grpc_status_tx.send(Ok(undeliverable_status));
            drop(grpc_status_tx); // Close stream
            
            return Ok(Response::new(UnboundedReceiverStream::new(status_rx)));
        }
        
        // Generate message ID
        let msg_id = self.msg_id_gen.next();
        
        // Store values before moving req
        let send_mode = req.mode();
        let timeout_seconds = req.timeout_seconds;
        let require_ack = req.require_ack;
        let dst_node = req.dst_node;
        
        // Track message as queued initially
        self.message_tracker.track_message(
            msg_id,
            MessageStatus::Queued,
            "Message queued for streaming delivery".to_string(),
            require_ack,
        );
        
        // Send initial queued status
        let initial_status = MessageStatusInfo {
            msg_id,
            status: MessageStatus::Queued as i32,
            status_message: "Message queued for streaming delivery".to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            require_ack,
        };
        
        if let Err(_) = grpc_status_tx.send(Ok(initial_status)) {
            return Err(Status::internal("Failed to initialize status stream"));
        }
        
        // Convert headers
        let headers: HashMap<String, Vec<u8>> = req
            .headers
            .into_iter()
            .map(|h| (h.key, h.value))
            .collect();
        
        // Create outbound message
        let outbound_msg = OutboundMessage {
            src_node: self.node_id,
            dst_node,
            payload: req.payload,
            headers,
            corr_id: req.corr_id,
            msg_id: Some(msg_id),
            require_ack,
        };
        
        // Queue message with status streaming
        if let Err(e) = self.message_queue.queue_message(
            outbound_msg,
            send_mode,
            timeout_seconds,
            Some(internal_status_tx), // Enable status streaming
        ).await {
            error!("Failed to queue message for streaming: {}", e);
            return Err(Status::internal("Failed to queue message"));
        }
        
        info!("Message {} queued for streaming delivery to node {}", msg_id, dst_node);
        
        Ok(Response::new(UnboundedReceiverStream::new(status_rx)))
    }
    
    type SubscribeStream = UnboundedReceiverStream<Result<Received, Status>>;
    type SendWithStatusStreamStream = UnboundedReceiverStream<Result<MessageStatusInfo, Status>>;
    
    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>> {
        let req = request.into_inner();
        
        debug!(
            "Subscribe request: partition={}, qos_class={}, src_node={}",
            req.partition, req.qos_class, req.src_node
        );
        
        // Create subscription filter
        let filter = SubscriptionFilter::from(&req);
        
        // Subscribe to delivery queue
        let (sub_id, mut receiver) = self.delivery_queue.subscribe(filter).await;
        
        // Create gRPC stream
        let (tx, rx) = mpsc::unbounded_channel();
        
        // Clone delivery queue for cleanup
        let delivery_queue_cleanup = self.delivery_queue.clone();
        
        // Spawn task to forward messages from delivery queue to gRPC stream
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Forward messages from delivery queue to gRPC stream
                    message = receiver.recv() => {
                        match message {
                            Some(msg) => {
                                if tx.send(Ok(msg)).is_err() {
                                    // gRPC stream closed - immediately clean up subscriber
                                    delivery_queue_cleanup.unsubscribe(sub_id);
                                    warn!("Subscriber {} disconnected, removed from delivery queue", sub_id);
                                    break;
                                }
                            }
                            None => {
                                // Delivery queue receiver closed
                                break;
                            }
                        }
                    }
                    // Periodically check if the gRPC stream is still alive
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                        // Send a heartbeat-like check by trying to send an empty result
                        // This will fail if the client has disconnected
                        if tx.is_closed() {
                            delivery_queue_cleanup.unsubscribe(sub_id);
                            warn!("Subscriber {} heartbeat check detected disconnection", sub_id);
                            break;
                        }
                    }
                }
            }
        });
        
        info!("New subscription {} created", sub_id);
        
        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }
    
    async fn ack_message(&self, request: Request<Ack>) -> Result<Response<()>> {
        let ack = request.into_inner();
        
        debug!(
            "Ack received: src_node={}, msg_id={}, success={}",
            ack.src_node, ack.msg_id, ack.success
        );
        
        // Store acknowledgment for app-level idempotency
        self.acked_messages.insert((ack.src_node, ack.msg_id), ());
        
        // Update message status based on acknowledgment
        let status = if ack.success {
            MessageStatus::AckSuccess
        } else {
            MessageStatus::AckFailure
        };
        
        let status_message = if ack.success {
            "Message acknowledged successfully".to_string()
        } else {
            if ack.message.is_empty() {
                "Message acknowledgment failed".to_string()
            } else {
                ack.message.clone()
            }
        };
        
        self.message_tracker.update_status(ack.msg_id, status, status_message.clone());
        
        // Send acknowledgment status back to source node
        self.send_delivery_status_to_source(
            ack.src_node,
            ack.msg_id,
            status,
            status_message,
        ).await;
        
        info!(
            "Message {} from node {} acknowledged with status: {}",
            ack.msg_id, ack.src_node, if ack.success { "success" } else { "failure" }
        );
        
        Ok(Response::new(()))
    }
    
    async fn query_message_status(
        &self,
        request: Request<QueryMessageStatusRequest>,
    ) -> Result<Response<QueryMessageStatusResponse>> {
        let req = request.into_inner();
        
        debug!(
            "Query message status request for {} messages",
            req.msg_ids.len()
        );
        
        let message_statuses = self.message_tracker.get_statuses(&req.msg_ids);
        
        debug!(
            "Returning status for {} out of {} requested messages",
            message_statuses.len(),
            req.msg_ids.len()
        );
        
        Ok(Response::new(QueryMessageStatusResponse {
            message_statuses,
        }))
    }
    
    async fn broadcast_state_event(
        &self,
        request: Request<MeshStateEvent>,
    ) -> Result<Response<()>> {
        let event = request.into_inner();
        
        info!(
            "Broadcasting state event {:?} from node {} (seq: {})",
            event.event_type, event.originator_node, event.sequence_number
        );
        
        // Convert the state event to a JSON-serializable format for broadcasting
        let event_data = serde_json::json!({
            "event_type": event.event_type as i32,
            "originator_node": event.originator_node,
            "affected_node": event.affected_node,
            "sequence_number": event.sequence_number,
            "timestamp": event.timestamp,
            "metadata": event.metadata,
            "payload": event.payload
        });
        
        let mesh_event_payload = match serde_json::to_vec(&event_data) {
            Ok(payload) => payload,
            Err(e) => {
                error!("Failed to serialize mesh state event: {}", e);
                return Err(Status::internal("Failed to serialize event"));
            }
        };
        
        // Create headers to identify this as a mesh event
        let mut headers = HashMap::new();
        headers.insert("message_type".to_string(), b"mesh_event".to_vec());
        headers.insert("event_type".to_string(), format!("{:?}", event.event_type).into_bytes());
        headers.insert("originator_node".to_string(), event.originator_node.to_string().into_bytes());
        headers.insert("sequence_number".to_string(), event.sequence_number.to_string().into_bytes());
        
        // Create outbound message for broadcasting
        let outbound_msg = OutboundMessage {
            src_node: self.node_id,
            dst_node: 0, // Broadcast to all nodes (0 = broadcast)
            payload: mesh_event_payload,
            headers,
            corr_id: 0, // Use 0 for broadcast messages
            msg_id: None, // Don't track broadcast messages
            require_ack: false, // Broadcasts don't require acknowledgment
        };
        
        // Send the broadcast message
        if let Err(e) = self.outbound_tx.send(outbound_msg) {
            error!("Failed to broadcast state event: {}", e);
            return Err(Status::internal("Failed to broadcast event"));
        }
        
        info!(
            "Successfully queued state event {:?} for broadcast (seq: {})",
            event.event_type, event.sequence_number
        );
        
        Ok(Response::new(()))
    }
    
    async fn request_database_sync(
        &self,
        request: Request<DatabaseSyncRequest>,
    ) -> Result<Response<DatabaseSyncResponse>> {
        let req = request.into_inner();
        
        info!(
            "Database sync request for table '{}' (last_known_version: {})",
            req.table_name, req.last_known_version
        );
        
        // Convert the sync request to a JSON-serializable format
        let sync_data = serde_json::json!({
            "table_name": req.table_name,
            "last_known_version": req.last_known_version,
            "node_ids": req.node_ids
        });
        
        let sync_request_payload = match serde_json::to_vec(&sync_data) {
            Ok(payload) => payload,
            Err(e) => {
                error!("Failed to serialize database sync request: {}", e);
                return Err(Status::internal("Failed to serialize sync request"));
            }
        };
        
        // Create headers to identify this as a database sync request
        let mut headers = HashMap::new();
        headers.insert("message_type".to_string(), b"database_sync_request".to_vec());
        headers.insert("table_name".to_string(), req.table_name.clone().into_bytes());
        headers.insert("last_known_version".to_string(), req.last_known_version.to_string().into_bytes());
        
        // For now, we'll broadcast the sync request to all nodes
        // In a more sophisticated implementation, we might target specific nodes
        let outbound_msg = OutboundMessage {
            src_node: self.node_id,
            dst_node: 0, // Broadcast to all nodes
            payload: sync_request_payload,
            headers,
            corr_id: 0,
            msg_id: None,
            require_ack: false,
        };
        
        // Send the sync request
        if let Err(e) = self.outbound_tx.send(outbound_msg) {
            error!("Failed to send database sync request: {}", e);
            return Err(Status::internal("Failed to send sync request"));
        }
        
        info!(
            "Successfully queued database sync request for table '{}' for broadcast",
            req.table_name
        );
        
        // For now, return an empty response
        // In a full implementation, we would wait for responses from other nodes
        // and aggregate the results
        Ok(Response::new(DatabaseSyncResponse {
            table_name: req.table_name,
            current_version: req.last_known_version, // Placeholder
            records: vec![], // Empty for now
            has_more: false,
        }))
    }
}

// Implement MeshData for Arc<MeshDataService> to allow sharing the service
#[tonic::async_trait]
impl MeshData for Arc<MeshDataService> {
    async fn send(&self, request: Request<SendRequest>) -> Result<Response<SendResponse>> {
        (**self).send(request).await
    }

    type SubscribeStream = <MeshDataService as MeshData>::SubscribeStream;
    async fn subscribe(&self, request: Request<SubscribeRequest>) -> Result<Response<Self::SubscribeStream>> {
        (**self).subscribe(request).await
    }

    async fn ack_message(&self, request: Request<Ack>) -> Result<Response<()>> {
        (**self).ack_message(request).await
    }

    async fn query_message_status(&self, request: Request<QueryMessageStatusRequest>) -> Result<Response<QueryMessageStatusResponse>> {
        (**self).query_message_status(request).await
    }

    type SendWithStatusStreamStream = <MeshDataService as MeshData>::SendWithStatusStreamStream;
    async fn send_with_status_stream(&self, request: Request<SendRequest>) -> Result<Response<Self::SendWithStatusStreamStream>> {
        (**self).send_with_status_stream(request).await
    }

    async fn broadcast_state_event(&self, request: Request<MeshStateEvent>) -> Result<Response<()>> {
        (**self).broadcast_state_event(request).await
    }

    async fn request_database_sync(&self, request: Request<DatabaseSyncRequest>) -> Result<Response<DatabaseSyncResponse>> {
        (**self).request_database_sync(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::mesh::v1::{Header, SubscribeRequest, SendMode};
    use crate::message_queue::{MessageQueue, MessageQueueConfig};
    use tokio::sync::mpsc;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn test_send_message() {
        let delivery_queue = Arc::new(DeliveryQueue::new());
        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();
        
        let message_tracker = Arc::new(MessageTracker::new());
        let message_queue = Arc::new(MessageQueue::new(
            MessageQueueConfig::default(),
            outbound_tx.clone(),
            message_tracker.clone(),
        ));
        let service = MeshDataService::new(1001, delivery_queue, outbound_tx, message_tracker, message_queue);
        
        let request = SendRequest {
            dst_node: 2002,
            headers: vec![Header {
                key: "test".to_string(),
                value: b"value".to_vec(),
            }],
            payload: b"Hello, World!".to_vec(),
            end_to_end_encrypt: false,
            partition: 0,
            qos_class: 0,
            corr_id: 12345,
            require_ack: false,
            mode: SendMode::FireAndForget as i32,
            timeout_seconds: 0,
        };
        
        let response = service.send(Request::new(request)).await.unwrap();
        let send_response = response.into_inner();
        
        assert!(send_response.msg_id > 0);
        
        // Check that outbound message was queued
        let outbound_msg = outbound_rx.recv().await.unwrap();
        assert_eq!(outbound_msg.dst_node, 2002);
        assert_eq!(outbound_msg.corr_id, 12345);
        assert_eq!(outbound_msg.payload, b"Hello, World!");
    }
    
    #[tokio::test]
    async fn test_subscribe_and_receive() {
        let delivery_queue = Arc::new(DeliveryQueue::new());
        let (outbound_tx, _outbound_rx) = mpsc::unbounded_channel();
        
        let message_tracker = Arc::new(MessageTracker::new());
        let message_queue = Arc::new(MessageQueue::new(
            MessageQueueConfig::default(),
            outbound_tx.clone(),
            message_tracker.clone(),
        ));
        let service = MeshDataService::new(1001, delivery_queue.clone(), outbound_tx, message_tracker, message_queue);
        
        // Create subscription
        let subscribe_req = SubscribeRequest {
            partition: 0,
            qos_class: 0,
            src_node: 2002,
        };
        
        let response = service.subscribe(Request::new(subscribe_req)).await.unwrap();
        let mut stream = response.into_inner();
        
        // Simulate incoming message
        let incoming_msg = Received {
            src_node: 2002,
            dst_node: 1001,
            msg_id: 12345,
            corr_id: 67890,
            headers: vec![],
            payload: b"Hello from 2002!".to_vec(),
            require_ack: false,
        };
        
        service.handle_incoming_message(incoming_msg.clone()).await;
        
        // Check that we received the message through the stream
        let received = stream.next().await.unwrap().unwrap();
        assert_eq!(received.src_node, incoming_msg.src_node);
        assert_eq!(received.msg_id, incoming_msg.msg_id);
        assert_eq!(received.payload, incoming_msg.payload);
    }
    
    #[tokio::test]
    async fn test_ack_message() {
        let delivery_queue = Arc::new(DeliveryQueue::new());
        let (outbound_tx, _outbound_rx) = mpsc::unbounded_channel();
        
        let message_tracker = Arc::new(MessageTracker::new());
        let message_queue = Arc::new(MessageQueue::new(
            MessageQueueConfig::default(),
            outbound_tx.clone(),
            message_tracker.clone(),
        ));
        let service = MeshDataService::new(1001, delivery_queue, outbound_tx, message_tracker, message_queue);
        
        let ack_req = Ack {
            src_node: 2002,
            msg_id: 12345,
            success: true,
            message: "Test acknowledgment".to_string(),
        };
        
        let response = service.ack_message(Request::new(ack_req)).await.unwrap();
        assert_eq!(response.into_inner(), ());
        
        // Check that acknowledgment was stored
        assert!(service.acked_messages.contains_key(&(2002, 12345)));
    }
}
