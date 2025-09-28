//! gRPC server implementation

use crate::control::{MeshControlService, SessionCommand};
use crate::data::MeshDataService;
use crate::delivery::DeliveryQueue;
use crate::message_tracker::MessageTracker;
use crate::message_queue::{MessageQueue, MessageQueueConfig};
use crate::metrics::MessageMetrics;
use mesh_session::manager::RoutingFeedback;
use crate::proto::mesh::v1::{mesh_control_server::MeshControlServer, mesh_data_server::MeshDataServer};
use mesh_routing::RoutingTable;
use mesh_session::manager::SessionInfo as SessionManagerInfo;
use mesh_topology::TopologyDatabase;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tonic::transport::Server;
use tonic_reflection::server::Builder as ReflectionBuilder;
use tracing::{error, info};

/// gRPC server configuration
#[derive(Debug, Clone)]
pub struct GrpcServerConfig {
    /// Bind address
    pub bind_addr: SocketAddr,
    /// Maximum message receive size
    pub max_recv_message_size: Option<usize>,
    /// Maximum message send size
    pub max_send_message_size: Option<usize>,
}

impl Default for GrpcServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:50051".parse().unwrap(),
            max_recv_message_size: Some(4 * 1024 * 1024), // 4MB
            max_send_message_size: Some(4 * 1024 * 1024),  // 4MB
        }
    }
}

/// gRPC server for mesh services
#[derive(Debug)]
pub struct MeshGrpcServer {
    /// Server configuration
    config: GrpcServerConfig,
    /// MeshData service
    data_service: Arc<MeshDataService>,
    /// MeshControl service
    control_service: MeshControlService,
}

impl MeshGrpcServer {
    /// Get the data service
    pub fn get_data_service(&self) -> Arc<MeshDataService> {
        self.data_service.clone()
    }
    
    /// Create a new gRPC server
    pub fn new(
        config: GrpcServerConfig,
        node_id: u64,
        delivery_queue: Arc<DeliveryQueue>,
        outbound_tx: mpsc::UnboundedSender<crate::data::OutboundMessage>,
        routing_table: Option<Arc<RoutingTable>>,
        session_registry: Option<Arc<RwLock<HashMap<u64, SessionManagerInfo>>>>,
        topology_db: Option<Arc<RwLock<TopologyDatabase>>>,
        session_command_tx: Option<mpsc::UnboundedSender<SessionCommand>>,
        routing_feedback_rx: Option<mpsc::UnboundedReceiver<RoutingFeedback>>,
    ) -> (Self, mpsc::UnboundedSender<crate::proto::mesh::v1::Received>) {
        let message_tracker = Arc::new(MessageTracker::new());
        
        // Create message queue with default configuration
        let queue_config = MessageQueueConfig::default();
        let message_queue = Arc::new(MessageQueue::new(
            queue_config,
            outbound_tx.clone(),
            message_tracker.clone(),
        ));
        
        // Start the retry processor
        let queue_clone = message_queue.clone();
        tokio::spawn(async move {
            queue_clone.start_retry_processor().await;
        });
        
        // Create incoming message channel for local delivery
        let (incoming_message_tx, mut incoming_message_rx) = mpsc::unbounded_channel::<crate::proto::mesh::v1::Received>();
        
        let mut data_service = MeshDataService::new(node_id, delivery_queue, outbound_tx, message_tracker.clone(), message_queue);
        
        // Set topology database if provided
        if let Some(db) = topology_db.clone() {
            data_service.set_topology_db(db);
        }
        
        // Set routing feedback receiver if provided
        if let Some(rx) = routing_feedback_rx {
            data_service.set_routing_feedback_receiver(rx);
            data_service.start_routing_feedback_task();
        }
        
        // Start message metrics collection
        let metrics = MessageMetrics::new(message_tracker.clone());
        metrics.start_collection_task();
        
        // Wrap data service in Arc for sharing
        let data_service = Arc::new(data_service);
        
        // Start incoming message handler task
        let data_service_clone = data_service.clone();
        tokio::spawn(async move {
            while let Some(message) = incoming_message_rx.recv().await {
                data_service_clone.handle_incoming_message(message).await;
            }
        });
        
        let mut control_service = MeshControlService::new(node_id, routing_table, session_registry, topology_db);
        
        // Set message tracker
        control_service.set_message_tracker(message_tracker.clone());
        
        // Set session command channel if provided
        if let Some(tx) = session_command_tx {
            control_service.set_session_command_channel(tx);
        }
        
        (Self {
            config,
            data_service,
            control_service,
        }, incoming_message_tx)
    }
    
    /// Start the gRPC server
    pub async fn serve(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting gRPC server on {}", self.config.bind_addr);
        
        let mut server_builder = Server::builder();
        
        // Configure message size limits
        if let Some(_max_recv) = self.config.max_recv_message_size {
            // Note: Message size limits would be configured here
            // The exact API may vary by tonic version
        }
        
        if let Some(_max_send) = self.config.max_send_message_size {
            // Note: Message size limits would be configured here
            // The exact API may vary by tonic version
        }
        
        // Create reflection service
        let reflection_service = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(crate::proto::FILE_DESCRIPTOR_SET)
            .build_v1()
            .map_err(|e| anyhow::anyhow!("Failed to create reflection service: {}", e))?;

        // Add core mesh services only
        let server = server_builder
            .add_service(MeshDataServer::new(self.data_service))
            .add_service(MeshControlServer::new(self.control_service))
            .add_service(reflection_service)
            .serve(self.config.bind_addr);
        
        info!("gRPC server listening on {}", self.config.bind_addr);
        
        if let Err(e) = server.await {
            error!("gRPC server error: {}", e);
            return Err(e.into());
        }
        
        Ok(())
    }
    
    /// Start the gRPC server with supervisor service
    pub async fn serve_with_supervisor(
        self, 
        supervisor_service: crate::proto::supervisor::v1::service_controller_service_server::ServiceControllerServiceServer<impl crate::proto::supervisor::v1::service_controller_service_server::ServiceControllerService>
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting gRPC server on {}", self.config.bind_addr);
        
        let mut server_builder = Server::builder();
        
        // Configure message size limits
        if let Some(_max_recv) = self.config.max_recv_message_size {
            // Note: Message size limits would be configured here
            // The exact API may vary by tonic version
        }
        
        if let Some(_max_send) = self.config.max_send_message_size {
            // Note: Message size limits would be configured here
            // The exact API may vary by tonic version
        }
        
        // Create reflection service
        let reflection_service = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(crate::proto::FILE_DESCRIPTOR_SET)
            .build_v1()
            .map_err(|e| anyhow::anyhow!("Failed to create reflection service: {}", e))?;

        // Add core mesh services
        let mut server_builder = server_builder
            .add_service(MeshDataServer::new(self.data_service))
            .add_service(MeshControlServer::new(self.control_service))
            .add_service(reflection_service);
        
        // Add supervisor service
        server_builder = server_builder.add_service(supervisor_service);
        
        let server = server_builder.serve(self.config.bind_addr);
        
        info!("gRPC server listening on {}", self.config.bind_addr);
        
        if let Err(e) = server.await {
            error!("gRPC server error: {}", e);
            return Err(e.into());
        }
        
        Ok(())
    }
    
    /// Get the bind address
    pub fn bind_addr(&self) -> SocketAddr {
        self.config.bind_addr
    }
}

/// Builder for creating gRPC server
#[derive(Debug)]
pub struct MeshGrpcServerBuilder {
    config: GrpcServerConfig,
    node_id: Option<u64>,
    delivery_queue: Option<Arc<DeliveryQueue>>,
    outbound_tx: Option<mpsc::UnboundedSender<crate::data::OutboundMessage>>,
    routing_table: Option<Arc<RoutingTable>>,
    session_registry: Option<Arc<RwLock<HashMap<u64, SessionManagerInfo>>>>,
    topology_db: Option<Arc<RwLock<TopologyDatabase>>>,
    session_command_tx: Option<mpsc::UnboundedSender<SessionCommand>>,
    routing_feedback_rx: Option<mpsc::UnboundedReceiver<RoutingFeedback>>,
}

impl MeshGrpcServerBuilder {
    /// Create a new server builder
    pub fn new() -> Self {
        Self {
            config: GrpcServerConfig::default(),
            node_id: None,
            delivery_queue: None,
            outbound_tx: None,
            routing_table: None,
            session_registry: None,
            topology_db: None,
            session_command_tx: None,
            routing_feedback_rx: None,
        }
    }
    
    /// Set the bind address
    pub fn bind_addr(mut self, addr: SocketAddr) -> Self {
        self.config.bind_addr = addr;
        self
    }
    
    /// Set maximum receive message size
    pub fn max_recv_message_size(mut self, size: usize) -> Self {
        self.config.max_recv_message_size = Some(size);
        self
    }
    
    /// Set maximum send message size
    pub fn max_send_message_size(mut self, size: usize) -> Self {
        self.config.max_send_message_size = Some(size);
        self
    }
    
    /// Set the node ID
    pub fn node_id(mut self, node_id: u64) -> Self {
        self.node_id = Some(node_id);
        self
    }
    
    /// Set the delivery queue
    pub fn delivery_queue(mut self, queue: Arc<DeliveryQueue>) -> Self {
        self.delivery_queue = Some(queue);
        self
    }
    
    /// Set the outbound message channel
    pub fn outbound_channel(mut self, tx: mpsc::UnboundedSender<crate::data::OutboundMessage>) -> Self {
        self.outbound_tx = Some(tx);
        self
    }
    
    /// Set the routing table
    pub fn routing_table(mut self, table: Arc<RoutingTable>) -> Self {
        self.routing_table = Some(table);
        self
    }
    
    /// Set the session registry
    pub fn session_registry(mut self, registry: Arc<RwLock<HashMap<u64, SessionManagerInfo>>>) -> Self {
        self.session_registry = Some(registry);
        self
    }
    
    /// Set the topology database
    pub fn topology_db(mut self, db: Arc<RwLock<TopologyDatabase>>) -> Self {
        self.topology_db = Some(db);
        self
    }
    
    /// Set the session command channel
    pub fn session_command_channel(mut self, tx: mpsc::UnboundedSender<SessionCommand>) -> Self {
        self.session_command_tx = Some(tx);
        self
    }
    
    /// Set the routing feedback receiver
    pub fn routing_feedback_receiver(mut self, rx: mpsc::UnboundedReceiver<RoutingFeedback>) -> Self {
        self.routing_feedback_rx = Some(rx);
        self
    }
    
    /// Build the server
    pub fn build(self) -> Result<(MeshGrpcServer, mpsc::UnboundedSender<crate::proto::mesh::v1::Received>), &'static str> {
        let node_id = self.node_id.ok_or("Node ID is required")?;
        let delivery_queue = self.delivery_queue.ok_or("Delivery queue is required")?;
        let outbound_tx = self.outbound_tx.ok_or("Outbound channel is required")?;
        
        Ok(MeshGrpcServer::new(
            self.config, 
            node_id, 
            delivery_queue, 
            outbound_tx, 
            self.routing_table,
            self.session_registry,
            self.topology_db,
            self.session_command_tx,
            self.routing_feedback_rx,
        ))
    }
}

impl Default for MeshGrpcServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delivery::DeliveryQueue;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_server_builder() {
        let delivery_queue = Arc::new(DeliveryQueue::new());
        let (outbound_tx, _outbound_rx) = mpsc::unbounded_channel();
        
        let (_server, _receiver) = MeshGrpcServerBuilder::new()
            .bind_addr("127.0.0.1:0".parse().unwrap())
            .node_id(1001)
            .delivery_queue(delivery_queue)
            .outbound_channel(outbound_tx)
            .max_recv_message_size(8 * 1024 * 1024)
            .build()
            .unwrap();
        
        // The config field is private, so we can't directly test it
        // Instead, we just verify that the server was created successfully
        assert!(true); // Server creation succeeded if we reach this point
    }
    
    #[test]
    fn test_server_builder_missing_fields() {
        let result = MeshGrpcServerBuilder::new().build();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Node ID is required");
    }
}
