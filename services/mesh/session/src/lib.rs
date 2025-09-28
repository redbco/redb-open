//! TCP/TLS sockets, send/recv loops, ACK/CREDIT, HELLO/RESUME, DRAIN/BYE for mesh.
//!
//! This crate provides session management capabilities for the mesh network,
//! including connection handling, handshake protocols, keepalive mechanisms,
//! and lifecycle management for TCP connections between mesh nodes.
//!
//! ## Features
//!
//! - **TCP Transport**: Basic TCP listener and dialer
//! - **Handshake Protocol**: HELLO frame exchange for session establishment
//! - **Keepalive**: PING/PONG with RTT measurement
//! - **Session Management**: Read/write loops with event handling
//! - **Auto-reconnect**: Automatic reconnection with exponential backoff
//!
//! ## Example
//!
//! ```rust,no_run
//! use mesh_session::{Session, SessionConfig, SessionEvent, TlsClientConfig};
//! use mesh_storage::StorageMode;
//! use std::time::Duration;
//! use tokio::sync::mpsc;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let config = SessionConfig {
//!     my_node_id: 1001,
//!     ping_interval: Duration::from_secs(10),
//!     idle_timeout: Duration::from_secs(30),
//!     verify_node_id: false,
//!     storage_mode: StorageMode::InMemory,
//!     ack_interval: Duration::from_millis(20),
//!     ack_batch_size: 256,
//!     recv_window: 32 * 1024 * 1024, // 32 MiB
//! };
//!
//! let (tx, mut rx) = mpsc::channel(100);
//! let addr = "127.0.0.1:9000".parse().unwrap();
//!
//! // Start outbound session
//! tokio::spawn(async move {
//!     Session::run_outbound(config, addr, None::<TlsClientConfig>, tx).await
//! });
//!
//! // Handle events
//! while let Some(event) = rx.recv().await {
//!     match event {
//!         SessionEvent::Connected { peer, remote_node_id } => {
//!             println!("Connected to {} (node {})", peer, remote_node_id);
//!         }
//!         SessionEvent::Pong { remote_node_id, rtt } => {
//!             println!("RTT from node {}: {:?}", remote_node_id, rtt);
//!         }
//!         SessionEvent::Disconnected { remote_node_id } => {
//!             println!("Disconnected from node {:?}", remote_node_id);
//!         }
//!         SessionEvent::MessageReceived { message } => {
//!             println!("Received message from node {}", message.src_node);
//!         }
//!         SessionEvent::TopologyUpdate { update } => {
//!             println!("Received topology update from node {} with {} neighbors", 
//!                      update.originator_node, update.neighbors.len());
//!         }
//!         SessionEvent::TopologyRequest { request } => {
//!             println!("Received topology request from node {}", request.requesting_node);
//!         }
//!     }
//! }
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod handshake;
pub mod keepalive;
pub mod manager;
pub mod reliability;
pub mod session;
pub mod transport;
pub mod failure_tracker;

// Re-export main types
pub use handshake::{parse_hello_meta, recv_any_frame, send_hello, Hello};
pub use keepalive::{build_ping, build_pong, calc_rtt_from_corr, now_corr_id};
pub use manager::{InboundMessage, OutboundMessage, SessionManager, MeshEventHandler, build_data_frame, register_session_with_registry, unregister_session_with_registry, register_global_session_channel, unregister_global_session_channel, get_global_session_channel};
pub use reliability::{AckMeta, RecvState, ReliabilityManager, ResumeMeta, SendState};
pub use session::{
    Session, SessionConfig, SessionEvent, SessionHandle, SessionStats, TlsClientConfig,
};
pub use transport::{connect_tcp, listen_tcp, IoStream};

// Re-export TLS functionality when available
#[cfg(feature = "tls")]
pub use transport::tls::{
    accept_tls, connect_tls, extract_node_id_from_cert, make_client_config, make_server_config,
    tls_acceptor, TlsServer,
};
