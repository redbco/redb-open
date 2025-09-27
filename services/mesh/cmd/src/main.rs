//! Mesh network node binary.
//!
//! This is the main binary for running a mesh network node with TCP/TLS transport,
//! handshake protocol, keepalive functionality, and mTLS authentication.

use clap::Parser;
use mesh_session::{InboundMessage, listen_tcp, IoStream, Session, SessionConfig, SessionEvent, SessionManager, TlsClientConfig, OutboundMessage};
use mesh_session::manager::RoutingFeedback;
use mesh_storage::StorageMode;
use mesh_routing::{RoutingTable};
use mesh_topology::TopologyDatabase;
use mesh_wire::{NeighborInfo, TopologyUpdate};
use mesh_grpc::{DeliveryQueue, MeshGrpcServerBuilder, SessionCommand, SessionOperationResult};
use mesh_grpc::proto::mesh::v1::{Received, Header};
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tracing::{info, warn, debug};
use tracing_subscriber::EnvFilter;

mod supervisor;
mod config;
mod logging;

use supervisor::{SupervisorClient, SupervisorConfig, create_service_controller_service};
use config::MeshConfig;
use logging::RedbLogFormatter;


// Component logging macros are defined in logging.rs and available via #[macro_export]

#[cfg(feature = "tls")]
use mesh_session::{accept_tls, make_client_config, make_server_config, tls_acceptor};

/// Mesh network node with optional TLS support
#[derive(Parser, Debug)]
#[command(name = "mesh", version, about = "Mesh network node with optional mTLS")]
struct Args {
    /// Node ID (string format from database)
    #[arg(long, default_value = "node_default")]
    node_id: String,

    /// Listen address, e.g. 0.0.0.0:9000
    #[arg(long)]
    listen: Option<SocketAddr>,

    /// Connect to address, e.g. 127.0.0.1:9000 (repeatable)
    #[arg(long)]
    connect: Vec<SocketAddr>,

    /// Ping interval, e.g. 10s
    #[arg(long, default_value = "10s")]
    ping_interval: humantime::Duration,

    /// Idle timeout, e.g. 30s
    #[arg(long, default_value = "30s")]
    idle_timeout: humantime::Duration,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,

    // TLS options
    /// Enable TLS (mTLS)
    #[arg(long)]
    tls: bool,

    /// Path to TLS certificate file (PEM format)
    #[arg(long, requires = "tls")]
    tls_cert: Option<PathBuf>,

    /// Path to TLS private key file (PEM format)
    #[arg(long, requires = "tls")]
    tls_key: Option<PathBuf>,

    /// Path to CA certificate file (PEM format)
    #[arg(long, requires = "tls")]
    tls_ca: Option<PathBuf>,

    /// Server name for TLS SNI (used for outbound connections)
    #[arg(long)]
    tls_sni: Option<String>,

    /// Verify node ID from TLS certificate matches HELLO
    #[arg(long, default_value_t = true)]
    tls_verify_node_id: bool,

    // Storage configuration
    /// Storage mode: memory, file
    #[arg(long, default_value = "memory")]
    storage_mode: String,

    /// Data directory for file storage
    #[arg(long, default_value = "./meshdata")]
    storage_data_dir: PathBuf,

    /// Segment size for file storage (in bytes)
    #[arg(long, default_value = "134217728")] // 128 MiB
    storage_segment_bytes: u64,

    /// Fsync frequency for file storage
    #[arg(long, default_value = "1")]
    storage_fsync_every: u32,

    /// ACK flush interval
    #[arg(long, default_value = "20ms")]
    ack_interval: humantime::Duration,

    /// ACK batch size
    #[arg(long, default_value = "256")]
    ack_batch_size: u32,

    /// Receive window size in bytes
    #[arg(long, default_value = "33554432")] // 32 MiB
    recv_window: u32,

    // Routing configuration
    /// Neighbor addresses for routing (repeatable), e.g. --neighbor 10.0.0.2:9000
    #[arg(long)]
    neighbor: Vec<SocketAddr>,

    /// Domain ID for multi-domain routing (future use)
    #[arg(long, default_value = "0")]
    domain_id: u32,

    /// Topology recompute interval
    #[arg(long, default_value = "1s")]
    topology_recompute_interval: humantime::Duration,

    // gRPC configuration
    /// gRPC server bind address
    #[arg(long, default_value = "127.0.0.1:50051")]
    grpc_bind: SocketAddr,

    /// Maximum gRPC receive message size in bytes
    #[arg(long, default_value = "4194304")] // 4 MiB
    grpc_max_recv_bytes: usize,

    /// Maximum gRPC send message size in bytes
    #[arg(long, default_value = "4194304")] // 4 MiB
    grpc_max_send_bytes: usize,

    /// Enable gRPC server
    #[arg(long)]
    enable_grpc: bool,

    // Supervisor integration
    /// Supervisor address (e.g., localhost:50000). Use 'standalone' to disable supervisor integration
    #[arg(long, default_value = "localhost:50000")]
    supervisor: String,

    /// Service port for supervisor registration (defaults to gRPC bind port)
    #[arg(long)]
    port: Option<u16>,
    
    /// Configuration file path
    #[arg(long, default_value = "config.yaml")]
    config: PathBuf,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialize tracing with custom formatter to match Golang services
    let env_filter = EnvFilter::new("info")
        .add_directive(format!("mesh={}", args.log_level).parse()?)
        .add_directive(format!("mesh_session={}", args.log_level).parse()?)
        .add_directive(format!("mesh_wire={}", args.log_level).parse()?)
        .add_directive(format!("redb_mesh={}", args.log_level).parse()?);

    let formatter = RedbLogFormatter::new("mesh".to_string());
    
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .with_ansi(true) // Enable ANSI colors
        .event_format(formatter)
        .init();

    info!("Starting reDB Mesh Service v{}", env!("CARGO_PKG_VERSION"));

    // Load configuration from file
    let mesh_config = MeshConfig::load_from_file(&args.config)?;
    
    // Get node identifiers from config
    let node_id_str = if args.node_id == "node_default" { // Default value
        mesh_config.node_id.clone()
    } else {
        args.node_id.clone()
    };
    
    // Use routing_id from config for internal mesh operations
    let routing_id = mesh_config.routing_id;
    
    let grpc_bind = if args.grpc_bind.port() == 50051 { // Default port
        format!("{}:{}", args.grpc_bind.ip(), mesh_config.grpc_port).parse()?
    } else {
        args.grpc_bind
    };
    let supervisor_addr = if args.supervisor == "localhost:50000" { // Default supervisor
        mesh_config.supervisor_addr.clone()
    } else {
        args.supervisor.clone()
    };

    info!("Starting mesh node with ID: {} (string: {}, from config: {})", routing_id, node_id_str, mesh_config.node_id);

    // Validate TLS arguments
    if args.tls {
        #[cfg(not(feature = "tls"))]
        {
            anyhow::bail!(
                "TLS requested but not compiled with TLS support. Build with --features tls"
            );
        }

        #[cfg(feature = "tls")]
        {
            if args.tls_cert.is_none() || args.tls_key.is_none() || args.tls_ca.is_none() {
                anyhow::bail!("TLS enabled but missing required certificate files (--tls-cert, --tls-key, --tls-ca)");
            }
        }
    }

    // Parse storage mode
    let storage_mode = match args.storage_mode.as_str() {
        "memory" => StorageMode::InMemory,
        "file" => StorageMode::File {
            data_dir: args.storage_data_dir.to_string_lossy().to_string(),
            segment_bytes: args.storage_segment_bytes,
            fsync_every: args.storage_fsync_every,
        },
        _ => anyhow::bail!(
            "Invalid storage mode: {}. Use 'memory' or 'file'",
            args.storage_mode
        ),
    };

    // Create session configuration
    let config = SessionConfig {
        my_node_id: routing_id,
        ping_interval: Duration::from(args.ping_interval),
        idle_timeout: Duration::from(args.idle_timeout),
        verify_node_id: args.tls_verify_node_id,
        storage_mode,
        ack_interval: Duration::from(args.ack_interval),
        ack_batch_size: args.ack_batch_size,
        recv_window: args.recv_window,
    };

    info!(
        "Session config: ping_interval={:?}, idle_timeout={:?}, verify_node_id={}, storage={:?}",
        config.ping_interval, config.idle_timeout, config.verify_node_id, config.storage_mode
    );

    // Initialize supervisor integration
    // Use main gRPC port for supervisor communication (ServiceController is on same server)
    let service_port = args.port.unwrap_or(grpc_bind.port());
    let supervisor_config = SupervisorConfig {
        supervisor_addr: supervisor_addr.clone(),
        service_name: "mesh".to_string(),
        service_version: env!("CARGO_PKG_VERSION").to_string(),
        service_port,
        standalone: supervisor_addr == "standalone",
    };

    let mut supervisor_client = SupervisorClient::new(supervisor_config);
    
    // Set the actual bind address for supervisor registration
    supervisor_client.set_bind_address(grpc_bind);
    
    let supervisor_client = Arc::new(RwLock::new(supervisor_client));
    
    // Create shutdown channels for supervisor integration (like Golang BaseService)
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel::<()>(1);
    
    // Note: Supervisor connection will be started after ServiceController is ready

    info!(
        "Reliability config: ack_interval={:?}, ack_batch_size={}, recv_window={} bytes",
        config.ack_interval, config.ack_batch_size, config.recv_window
    );

    // Load TLS configuration if enabled
    #[cfg(feature = "tls")]
    let (tls_server, tls_client_config) = if args.tls {
        let cert_path = args.tls_cert.as_ref().unwrap();
        let key_path = args.tls_key.as_ref().unwrap();
        let ca_path = args.tls_ca.as_ref().unwrap();

        info!(
            "Loading TLS configuration from cert={:?}, key={:?}, ca={:?}",
            cert_path, key_path, ca_path
        );

        // Read certificate files
        let cert_pem = tokio::fs::read_to_string(cert_path).await.map_err(|e| {
            anyhow::anyhow!("Failed to read certificate file {:?}: {}", cert_path, e)
        })?;
        let key_pem = tokio::fs::read_to_string(key_path).await.map_err(|e| {
            anyhow::anyhow!("Failed to read private key file {:?}: {}", key_path, e)
        })?;
        let ca_pem = tokio::fs::read_to_string(ca_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read CA file {:?}: {}", ca_path, e))?;

        // Create server configuration
        let server_config = make_server_config(&cert_pem, &key_pem, &ca_pem)?;
        let tls_server = Some(Arc::new(tls_acceptor(server_config)));

        // Create client configuration
        let client_config = make_client_config(&cert_pem, &key_pem, &ca_pem)?;
        let server_name = args
            .tls_sni
            .clone()
            .or_else(|| args.connect.first().map(|addr| addr.ip().to_string()))
            .unwrap_or_else(|| "localhost".to_string());

        let tls_client_config = Some(TlsClientConfig {
            client_config,
            server_name,
        });

        info!("TLS configuration loaded successfully");
        (tls_server, tls_client_config)
    } else {
        (None, None)
    };

    #[cfg(not(feature = "tls"))]
    let (tls_server, tls_client_config): (Option<Arc<()>>, Option<TlsClientConfig>) = (None, None);

    // Initialize routing table and topology database
    let routing_table = Arc::new(RoutingTable::new(routing_id));
    let topology_db = TopologyDatabase::new(routing_id);
    let topology_db = Arc::new(tokio::sync::RwLock::new(topology_db));
    
    // Add neighbor routes if specified (for initial bootstrap)
    if !args.neighbor.is_empty() {
        info!("Configuring {} neighbor routes for bootstrap", args.neighbor.len());
        
        // Create initial neighbor list for topology
        let mut neighbors = Vec::new();
        
        for neighbor_addr in &args.neighbor {
            // For now, we'll use a simple node ID mapping based on port
            // In a real implementation, this would come from neighbor discovery
            let neighbor_node_id = match neighbor_addr.port() {
                9000 => 1001,
                9001 => 2002, 
                9002 => 3003,
                9003 => 4004,
                _ => neighbor_addr.port() as u64, // Fallback
            };
            
            // Add to topology database
            neighbors.push(NeighborInfo::new(
                neighbor_node_id,
                100, // Default cost of 100 microseconds
                Some(neighbor_addr.to_string()),
            ));
            
            info!("Added neighbor {} at {}", neighbor_node_id, neighbor_addr);
        }
        
        // Update topology with initial neighbors
        if !neighbors.is_empty() {
            let mut db = topology_db.write().await;
            let topology_update = db.update_local_neighbors(neighbors);
            info!("Initial topology update created (seq: {})", topology_update.sequence_number);
            
            // Update routing table with computed routes
            let computed_routes = db.get_routes().clone();
            drop(db); // Release the lock before calling async method
            routing_table.update_routes_from_topology(&computed_routes).await;
        }
    }

    // Create event channel
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<SessionEvent>(1024);

    // Initialize gRPC components if enabled
    let (_grpc_server_handle, _session_manager_handle, _delivery_queue, manager_event_tx, session_registry, topology_update_tx, mut received_topology_rx, mut session_command_rx) = if args.enable_grpc {
        let delivery_queue = Arc::new(DeliveryQueue::new());
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<OutboundMessage>();
        let (delivery_tx, mut delivery_rx) = mpsc::unbounded_channel::<InboundMessage>();
        
        // Create SessionManager first to get session registry
        let (manager_event_tx, manager_event_rx) = tokio::sync::mpsc::channel::<SessionEvent>(1024);
        
        // Create topology update channel (for broadcasting)
        let (topology_update_tx, topology_update_rx) = mpsc::unbounded_channel::<TopologyUpdate>();
        
        // Create received topology update channel (for processing)
        let (received_topology_tx, received_topology_rx) = mpsc::unbounded_channel::<TopologyUpdate>();
        
        // Create session command channel
        let (session_command_tx, session_command_rx) = mpsc::unbounded_channel::<SessionCommand>();
        
        // Create routing feedback channel
        let (routing_feedback_tx, routing_feedback_rx) = mpsc::unbounded_channel::<RoutingFeedback>();
        
        // Create SessionManager
        let mut session_manager = SessionManager::new(
            routing_id,
            routing_table.clone(),
            manager_event_rx,
        );
        session_manager.set_outbound_receiver(outbound_rx);
        session_manager.set_delivery_sender(delivery_tx);
        session_manager.set_topology_update_receiver(topology_update_rx);
        session_manager.set_received_topology_sender(received_topology_tx);
        session_manager.set_routing_feedback_sender(routing_feedback_tx);
        
        // Get shared session registry for session registration
        let session_registry = session_manager.get_session_registry();

        // Create ServiceController service for supervisor integration
        let service_controller = create_service_controller_service(shutdown_tx.clone(), shutdown_complete_rx).await;
        
        // Build the mesh gRPC server components
        let (mesh_grpc_server, incoming_message_tx) = MeshGrpcServerBuilder::new()
            .bind_addr(args.grpc_bind)
            .node_id(routing_id)
            .delivery_queue(delivery_queue.clone())
            .outbound_channel(outbound_tx)
            .routing_table(routing_table.clone())
            .session_registry(session_registry.clone())
            .topology_db(topology_db.clone())
            .session_command_channel(session_command_tx)
            .routing_feedback_receiver(routing_feedback_rx)
            .max_recv_message_size(args.grpc_max_recv_bytes)
            .max_send_message_size(args.grpc_max_send_bytes)
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build gRPC server: {}", e))?;

        info!("Starting combined gRPC server (mesh + supervisor) on {}", args.grpc_bind);
        let server_handle = tokio::spawn(async move {
            if let Err(e) = mesh_grpc_server.serve_with_supervisor(service_controller).await {
                warn!("Combined gRPC server error: {}", e);
            }
        });
        
        // Give ServiceController a moment to start, then connect to supervisor
        let supervisor_client_clone = supervisor_client.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            
            let mut client = supervisor_client_clone.write().await;
            
            // Connect to supervisor if not in standalone mode
            if let Err(e) = client.connect_and_register().await {
                warn!("Failed to connect to supervisor: {}. Running in standalone mode.", e);
                return;
            }
            
            // Start heartbeat loop
            if let Err(e) = client.start_heartbeat_loop().await {
                warn!("Failed to start heartbeat loop: {}", e);
            }
        });
        
        info!("Starting session manager for node {}", routing_id);
        let manager_handle = tokio::spawn(async move {
            if let Err(e) = session_manager.run().await {
                warn!("Session manager error: {}", e);
            }
        });

        // Start delivery task to consume from delivery_rx and send to MeshDataService
        let _delivery_handle = tokio::spawn(async move {
            info!("Starting local delivery task");
            while let Some(inbound_msg) = delivery_rx.recv().await {
                // Convert InboundMessage to Received
                let received = Received {
                    src_node: inbound_msg.src_node,
                    dst_node: inbound_msg.dst_node,
                    msg_id: inbound_msg.msg_id.unwrap_or(0), // Use message ID if available
                    corr_id: inbound_msg.corr_id,
                    headers: inbound_msg.headers.into_iter().map(|(key, value)| Header {
                        key,
                        value,
                    }).collect(),
                    payload: inbound_msg.payload,
                    require_ack: inbound_msg.require_ack, // Use acknowledgment requirement from message
                };

                // Send to MeshDataService for proper handling including delivery status feedback
                if let Err(e) = incoming_message_tx.send(received) {
                    warn!("Failed to send message to MeshDataService: {}", e);
                    break;
                }
            }
            info!("Local delivery task ended");
        });
        
        (Some(server_handle), Some(manager_handle), Some(delivery_queue), Some(manager_event_tx), Some(session_registry), Some(topology_update_tx), Some(received_topology_rx), Some(session_command_rx))
    } else {
        (None, None, None, None, None, None, None, None)
    };


    // Start listener if specified
    if let Some(listen_addr) = args.listen {
        let listener = listen_tcp(listen_addr).await?;
        info!("Listening on {} (TLS: {})", listen_addr, args.tls);

        let tx_accept = event_tx.clone();
        let config_accept = config.clone();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((tcp_stream, peer_addr)) => {
                        info!("Accepted TCP connection from {}", peer_addr);

                        let tx_session = tx_accept.clone();
                        let config_session = config_accept.clone();
                        #[allow(unused_variables)] // Used in TLS feature block
                        let tls_acceptor = tls_server.clone();

                        tokio::spawn(async move {
                            // Handle TLS handshake if enabled
                            #[cfg(feature = "tls")]
                            let (stream, peer_cert) = if let Some(acceptor) = tls_acceptor {
                                match accept_tls(&*acceptor, tcp_stream).await {
                                    Ok((stream, cert)) => {
                                        info!("TLS handshake completed with {}", peer_addr);
                                        (stream, Some(cert))
                                    }
                                    Err(e) => {
                                        warn!("TLS handshake failed with {}: {}", peer_addr, e);
                                        return;
                                    }
                                }
                            } else {
                                (IoStream::Plain(tcp_stream), None)
                            };

                            #[cfg(not(feature = "tls"))]
                            let (stream, peer_cert) = (IoStream::Plain(tcp_stream), None);

                            // Create message channel for this session
                            let (message_tx, message_rx) = mpsc::unbounded_channel::<OutboundMessage>();
                            
                            // Pass both sender and receiver to the session
                            // The session will register the sender in the global registry after handshake
                            if let Err(e) =
                                Session::run_inbound_with_messages(config_session, stream, peer_cert, tx_session, Some((message_tx, message_rx)))
                                    .await
                            {
                                warn!("Inbound session error: {:#}", e);
                            }
                        });
                    }
                    Err(e) => {
                        warn!("Accept error: {}; stopping listener", e);
                        break;
                    }
                }
            }
        });
    }

    // Check that at least one mode is specified
    if args.listen.is_none() && args.connect.is_empty() {
        anyhow::bail!("Must specify either --listen or --connect (or both)");
    }

    // Start outbound connectors if specified
    if !args.connect.is_empty() {
        info!("Will connect to {} addresses (TLS: {})", args.connect.len(), args.tls);

        for connect_addr in args.connect {
            info!("Connecting to {} (TLS: {})", connect_addr, args.tls);
            
            let tx_connect = event_tx.clone();
            let config_connect = config.clone();
            let tls_client_config_clone = tls_client_config.clone();

            let _task_handle = tokio::spawn(async move {
                // Create message channel for outbound session
                let (message_tx, message_rx) = mpsc::unbounded_channel::<OutboundMessage>();
                
                if let Err(e) =
                    Session::run_outbound_with_messages(config_connect, connect_addr, tls_client_config_clone, tx_connect, Some((message_tx, message_rx)))
                        .await
                {
                    warn!("Outbound session error to {}: {:#}", connect_addr, e);
                }
            });
            
            // Note: We don't track --connect tasks in outbound_session_tasks since they're permanent connections
        }
    }

    // Track connected neighbors for topology updates
    let mut connected_neighbors: HashMap<u64, SocketAddr> = HashMap::new();
    
    // Track outbound session tasks so we can cancel them
    let mut outbound_session_tasks: HashMap<SocketAddr, tokio::task::JoinHandle<()>> = HashMap::new();

    // Main event loop - print session events
    info!("Mesh node started. Waiting for events...");

    // Set up signal handling (like Golang BaseService)
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| anyhow::anyhow!("Failed to install SIGTERM handler: {}", e))?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|e| anyhow::anyhow!("Failed to install SIGINT handler: {}", e))?;

    loop {
        tokio::select! {
            // Handle shutdown signal from supervisor
            _ = shutdown_rx.recv() => {
                info!("Received shutdown signal from supervisor, initiating graceful shutdown");
                break;
            }
            
            // Handle SIGTERM (but don't shutdown immediately - wait for supervisor)
            _ = sigterm.recv() => {
                info!("Received SIGTERM signal - waiting for supervisor to send stop command");
                // Don't break here - wait for supervisor to send stop command via gRPC
            }
            
            // Handle SIGINT (Ctrl+C) (but don't shutdown immediately - wait for supervisor)  
            _ = sigint.recv() => {
                info!("Received SIGINT signal - waiting for supervisor to send stop command");
                // Don't break here - wait for supervisor to send stop command via gRPC
            }
            
            // Handle session commands from gRPC
            Some(command) = async {
                if let Some(ref mut rx) = session_command_rx {
                    rx.recv().await
                } else {
                    std::future::pending().await
                }
            } => {
                match command {
                    SessionCommand::AddSession { addr, timeout_seconds, response_tx } => {
                        info!("Adding new session to {} with timeout {}s", addr, timeout_seconds);
                        
                        let tx_connect = event_tx.clone();
                        let config_connect = config.clone();
                        let tls_client_config_clone = tls_client_config.clone();
                        let session_registry_clone = session_registry.clone();

                        let task_handle = tokio::spawn(async move {
                            // Create message channel for outbound session
                            let (message_tx, message_rx) = mpsc::unbounded_channel::<OutboundMessage>();
                            
                            // Apply timeout to the connection attempt
                            let connection_result = tokio::time::timeout(
                                Duration::from_secs(timeout_seconds as u64),
                                Session::run_outbound_with_messages(config_connect, addr, tls_client_config_clone, tx_connect, Some((message_tx, message_rx)))
                            ).await;
                            
                            let result = match connection_result {
                                Ok(Ok(())) => {
                                    // Connection successful, try to get the peer node ID from the session registry
                                    // Wait a bit for the session to be registered
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                    
                                    let peer_node_id = if let Some(ref registry) = session_registry_clone {
                                        let sessions = registry.read().await;
                                        // Find the session with matching remote address
                                        sessions.iter()
                                            .find(|(_, info)| info.remote_addr == addr)
                                            .map(|(node_id, _)| *node_id)
                                    } else {
                                        None
                                    };
                                    
                                    SessionOperationResult {
                                        success: true,
                                        message: format!("Successfully connected to {}", addr),
                                        error_code: None,
                                        peer_node_id,
                                        remote_addr: Some(addr.to_string()),
                                    }
                                }
                                Ok(Err(e)) => {
                                    warn!("Outbound session error to {}: {:#}", addr, e);
                                    SessionOperationResult {
                                        success: false,
                                        message: format!("Connection failed: {}", e),
                                        error_code: Some("CONNECTION_FAILED".to_string()),
                                        peer_node_id: None,
                                        remote_addr: None,
                                    }
                                }
                                Err(_) => {
                                    warn!("Connection to {} timed out after {}s", addr, timeout_seconds);
                                    SessionOperationResult {
                                        success: false,
                                        message: format!("Connection timed out after {}s", timeout_seconds),
                                        error_code: Some("TIMEOUT".to_string()),
                                        peer_node_id: None,
                                        remote_addr: None,
                                    }
                                }
                            };
                            
                            // Send the result back
                            if let Err(_) = response_tx.send(result) {
                                warn!("Failed to send AddSession response - receiver dropped");
                            }
                        });
                        
                        // Track the task so we can cancel it later
                        outbound_session_tasks.insert(addr, task_handle);
                        info!("Tracking outbound session task for {}", addr);
                    }
                    SessionCommand::DropSession { peer_node_id, response_tx } => {
                        info!("Dropping session with peer node {}", peer_node_id);
                        
                        let mut success = false;
                        let message: String;
                        let mut error_code: Option<String> = None;
                        
                        // First, find the address for this peer node
                        let peer_addr = if let Some(ref session_registry) = session_registry {
                            let registry = session_registry.read().await;
                            registry.get(&peer_node_id).map(|info| info.remote_addr)
                        } else {
                            None
                        };
                        
                        if let Some(addr) = peer_addr {
                            // Cancel the outbound session task if it exists
                            if let Some(task_handle) = outbound_session_tasks.remove(&addr) {
                                info!("Cancelling outbound session task for {}", addr);
                                task_handle.abort();
                                info!("Outbound session task cancelled for {}", addr);
                            }
                            
                            // Send termination message to the session
                            if let Some(ref session_registry) = session_registry {
                                let registry = session_registry.read().await;
                                if let Some(session_info) = registry.get(&peer_node_id) {
                                    let termination_msg = OutboundMessage::create_termination_message(routing_id, peer_node_id);
                                    if let Err(e) = session_info.message_tx.send(termination_msg) {
                                        warn!("Failed to send termination message to node {}: {}", peer_node_id, e);
                                        message = format!("Failed to send termination message: {}", e);
                                        error_code = Some("TERMINATION_FAILED".to_string());
                                    } else {
                                        info!("Sent termination message to node {}", peer_node_id);
                                        success = true;
                                        message = format!("Successfully dropped session with node {}", peer_node_id);
                                    }
                                } else {
                                    warn!("No active session found for node {}", peer_node_id);
                                    message = format!("No active session found for node {}", peer_node_id);
                                    error_code = Some("SESSION_NOT_FOUND".to_string());
                                }
                            } else {
                                message = "Session registry not available".to_string();
                                error_code = Some("REGISTRY_UNAVAILABLE".to_string());
                            }
                            
                            // Remove from connected neighbors (will be cleaned up by session disconnection event)
                            if let Some(addr) = connected_neighbors.remove(&peer_node_id) {
                                info!("Removed neighbor {} at {} from local tracking", peer_node_id, addr);
                            }
                        } else {
                            message = format!("No session found for node {}", peer_node_id);
                            error_code = Some("SESSION_NOT_FOUND".to_string());
                        }
                        
                        let result = SessionOperationResult {
                            success,
                            message,
                            error_code,
                            peer_node_id: None,
                            remote_addr: None,
                        };
                        
                        // Send the result back
                        if let Err(_) = response_tx.send(result) {
                            warn!("Failed to send DropSession response - receiver dropped");
                        }
                    }
                }
            }
            
            // Handle session events
            Some(event) = event_rx.recv() => {
                // Forward events to SessionManager if gRPC is enabled
                if let Some(ref manager_tx) = manager_event_tx {
                    if let Err(e) = manager_tx.send(event.clone()).await {
                        warn!("Failed to forward event to SessionManager: {}", e);
                    }
                }

                match event {
            SessionEvent::Connected {
                peer,
                remote_node_id,
            } => {
                component_info!("session", "Connected to {} as peer_node={}", peer, remote_node_id);
                
                // Add to connected neighbors
                connected_neighbors.insert(remote_node_id, peer);
                
                // Update topology database with all current neighbors
                let topology_db_clone = topology_db.clone();
                let routing_table_clone = routing_table.clone();
                let neighbors: Vec<NeighborInfo> = connected_neighbors
                    .iter()
                    .map(|(&node_id, &addr)| NeighborInfo::new(
                        node_id,
                        100, // Default cost
                        Some(addr.to_string()),
                    ))
                    .collect();
                
                let topology_tx_clone = topology_update_tx.clone();
                tokio::spawn(async move {
                    let mut db = topology_db_clone.write().await;
                    
                    // Get existing topology state before updating
                    let existing_topology_updates = db.get_all_topology_updates();
                    
                    // Update local neighbors
                    let topology_update = db.update_local_neighbors(neighbors);
                    info!("Updated topology after connection to node {} (seq: {}, {} neighbors)", 
                          remote_node_id, topology_update.sequence_number, topology_update.neighbors.len());
                    
                    // Update routing table
                    let computed_routes = db.get_routes().clone();
                    drop(db);
                    routing_table_clone.update_routes_from_topology(&computed_routes).await;
                        component_info!("topology", "Routing table updated after connection to node {}", remote_node_id);
                    
                    // Broadcast our topology update to all neighbors
                    if let Some(tx) = topology_tx_clone.clone() {
                        if let Err(e) = tx.send(topology_update) {
                            warn!("Failed to send topology update for broadcast: {}", e);
                        } else {
                            info!("Sent topology update for broadcast after connection to node {}", remote_node_id);
                        }
                    }
                    
                    // Send existing topology state to the newly connected neighbor for synchronization
                    if !existing_topology_updates.is_empty() {
                        if let Some(tx) = topology_tx_clone {
                            for sync_update in existing_topology_updates {
                                if let Err(e) = tx.send(sync_update.clone()) {
                                    warn!("Failed to send topology sync update from node {}: {}", sync_update.originator_node, e);
                                } else {
                                    info!("Sent topology sync update from node {} to newly connected neighbor {}", 
                                          sync_update.originator_node, remote_node_id);
                                }
                            }
                        }
                    }
                });
            }
            SessionEvent::Disconnected { remote_node_id } => {
                if let Some(node_id) = remote_node_id {
                    component_info!("session", "Disconnected from peer_node={}", node_id);
                    
                    // Remove from connected neighbors
                    connected_neighbors.remove(&node_id);
                    
                    // Update topology database with remaining neighbors
                    let topology_db_clone = topology_db.clone();
                    let routing_table_clone = routing_table.clone();
                    let neighbors: Vec<NeighborInfo> = connected_neighbors
                        .iter()
                        .map(|(&node_id, &addr)| NeighborInfo::new(
                            node_id,
                            100, // Default cost
                            Some(addr.to_string()),
                        ))
                        .collect();
                    
                    let topology_tx_clone = topology_update_tx.clone();
                    tokio::spawn(async move {
                        let mut db = topology_db_clone.write().await;
                        let topology_update = db.update_local_neighbors(neighbors);
                        info!("Updated topology after disconnection from node {} (seq: {}, {} neighbors)", 
                              node_id, topology_update.sequence_number, topology_update.neighbors.len());
                        
                        // Update routing table
                        let computed_routes = db.get_routes().clone();
                        drop(db);
                        routing_table_clone.update_routes_from_topology(&computed_routes).await;
                        component_info!("topology", "Routing table updated after disconnection from node {}", node_id);
                        
                        // Broadcast topology update to neighbors
                        if let Some(tx) = topology_tx_clone {
                            if let Err(e) = tx.send(topology_update) {
                                warn!("Failed to send topology update for broadcast: {}", e);
                            } else {
                                info!("Sent topology update for broadcast after disconnection from node {}", node_id);
                            }
                        }
                    });
                } else {
                    info!("Session disconnected (no node ID)");
                }
            }
            SessionEvent::Pong {
                remote_node_id,
                rtt,
            } => {
                debug!("Keepalive from peer_node={} rtt={:?}", remote_node_id, rtt);
            }
            SessionEvent::MessageReceived { message } => {
                info!(
                    "Message received from node {} to node {} (corr_id: {}, {} bytes)",
                    message.src_node, message.dst_node, message.corr_id, message.payload.len()
                );
                if manager_event_tx.is_none() {
                    warn!("SessionManager not running - message not routed");
                }
            }
            SessionEvent::TopologyUpdate { update } => {
                component_info!(
                    "topology", 
                    "Received topology update from node {} (seq: {}, {} neighbors)",
                    update.originator_node, update.sequence_number, update.neighbors.len()
                );
                
                // Only process topology updates directly if SessionManager is not running
                // When SessionManager is running, it forwards topology updates to received_topology_rx
                if manager_event_tx.is_none() {
                    // Process topology update
                    let topology_db_clone = topology_db.clone();
                    let routing_table_clone = routing_table.clone();
                    tokio::spawn(async move {
                        let mut db = topology_db_clone.write().await;
                        if db.process_topology_update(update) {
                            // Topology changed, update routing table
                            let computed_routes = db.get_routes().clone();
                            drop(db); // Release lock before async call
                            routing_table_clone.update_routes_from_topology(&computed_routes).await;
                            component_info!("topology", "Routing table updated with new topology");
                        }
                    });
                } else {
                    debug!("SessionManager is running - topology update will be processed via received_topology_rx");
                }
            }
            SessionEvent::TopologyRequest { request } => {
                info!(
                    "Topology request received from node {} (target: {:?})",
                    request.requesting_node, request.target_node
                );
                // TODO: Handle topology request by sending our topology
            }
                }
            }

            // Handle received topology updates (from other nodes)
            Some(topology_update) = async {
                match &mut received_topology_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                component_info!(
                    "topology",
                    "Processing received topology update from node {} (seq: {}, {} neighbors)",
                    topology_update.originator_node, topology_update.sequence_number, topology_update.neighbors.len()
                );
                
                // Process the received topology update
                let topology_db_clone = topology_db.clone();
                let routing_table_clone = routing_table.clone();
                let topology_tx_clone = topology_update_tx.clone();
                tokio::spawn(async move {
                    let mut db = topology_db_clone.write().await;
                    if db.process_topology_update(topology_update.clone()) {
                        info!("Topology changed after processing update from node {}", topology_update.originator_node);
                        
                        // Update routing table with new topology
                        let computed_routes = db.get_routes().clone();
                        drop(db); // Release lock before async call
                        routing_table_clone.update_routes_from_topology(&computed_routes).await;
                        component_info!("topology", "Routing table updated with received topology from node {}", topology_update.originator_node);
                        
                        // Forward the topology update to neighbors (flooding)
                        if let Some(tx) = topology_tx_clone {
                            // Decrement TTL before forwarding
                            if topology_update.ttl > 1 {
                                let mut forwarded_update = topology_update.clone();
                                forwarded_update.ttl -= 1;
                                
                                if let Err(e) = tx.send(forwarded_update) {
                                    warn!("Failed to forward topology update: {}", e);
                                } else {
                                    info!("Forwarded topology update from node {} to neighbors", topology_update.originator_node);
                                }
                            } else {
                                debug!("Not forwarding topology update from node {} (TTL expired)", topology_update.originator_node);
                            }
                        }
                    } else {
                        debug!("Topology update from node {} was old or duplicate", topology_update.originator_node);
                    }
                });
            }

            else => {
                info!("Event channels closed, shutting down");
                break;
            }
        }
    }

    info!("Mesh node shutting down");
    
    // Follow Golang BaseService shutdown sequence:
    // 1. Wait before unregistering to allow supervisor to send stop commands
    info!("Waiting before unregistering to allow supervisor to send stop commands...");
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    
    // 2. Unregister from supervisor (while gRPC server is still running)
    {
        let client = supervisor_client.read().await;
        if let Err(e) = client.unregister().await {
            warn!("Failed to unregister from supervisor: {}", e);
        }
    }
    
    // 3. Log service stopped BEFORE stopping gRPC server
    info!("Service stopped");
    
    // 4. Signal shutdown completion to ServiceController (like Golang stoppedCh)
    if let Err(e) = shutdown_complete_tx.send(()).await {
        warn!("Failed to signal shutdown completion: {}", e);
    }
    
    info!("Mesh node shutdown complete");
    Ok(())
}
