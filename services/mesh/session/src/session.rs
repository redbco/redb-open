//! Core session management for mesh networking.
//!
//! This module implements the main Session struct that handles TCP/TLS connections,
//! handshakes, keepalive, and message processing with node identity verification.

use bytes::BytesMut;
use mesh_storage::StorageMode;
use mesh_wire::{FrameDecoder, FrameType};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use ciborium;

use crate::handshake::{recv_any_frame, send_hello};
use crate::keepalive::{build_ping, build_pong, now_corr_id};
use crate::transport::IoStream;

/// Configuration for a mesh session
#[derive(Clone, Debug)]
pub struct SessionConfig {
    /// This node's ID
    pub my_node_id: u64,
    /// Interval between PING frames
    pub ping_interval: Duration,
    /// Timeout for idle connections
    pub idle_timeout: Duration,
    /// Whether to verify node ID from TLS certificate matches HELLO
    pub verify_node_id: bool,
    /// Storage configuration for reliability
    pub storage_mode: StorageMode,
    /// ACK flush interval
    pub ack_interval: Duration,
    /// ACK batch size
    pub ack_batch_size: u32,
    /// Default receive window in bytes
    pub recv_window: u32,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            my_node_id: 1,
            ping_interval: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
            verify_node_id: true,
            storage_mode: StorageMode::InMemory,
            ack_interval: Duration::from_millis(20),
            ack_batch_size: 256,
            recv_window: 32 * 1024 * 1024, // 32 MiB
        }
    }
}

/// Statistics for a session
#[derive(Clone, Debug, Default)]
pub struct SessionStats {
    /// Total bytes received
    pub bytes_in: u64,
    /// Total bytes sent
    pub bytes_out: u64,
    /// Timestamp of last received frame
    pub last_frame_in: Option<Instant>,
    /// Timestamp of last sent frame
    pub last_frame_out: Option<Instant>,
    /// Most recent RTT measurement
    pub last_rtt: Option<Duration>,
    /// Number of frames received
    pub frames_received: u64,
    /// Number of frames sent
    pub frames_sent: u64,
}

/// Events emitted by sessions
#[derive(Debug, Clone)]
pub enum SessionEvent {
    /// Successfully connected to a peer
    Connected {
        /// Peer socket address
        peer: SocketAddr,
        /// Remote node ID (from TLS certificate or HELLO)
        remote_node_id: u64,
    },
    /// Disconnected from peer
    Disconnected {
        /// Remote node ID if known
        remote_node_id: Option<u64>,
    },
    /// Received PONG with RTT measurement
    Pong {
        /// Remote node ID
        remote_node_id: u64,
        /// Round-trip time
        rtt: Duration,
    },
    /// Received a message that needs processing
    MessageReceived {
        /// The received message
        message: crate::manager::InboundMessage,
    },
    /// Received a topology update
    TopologyUpdate {
        /// The topology update
        update: mesh_wire::TopologyUpdate,
    },
    /// Received a topology request
    TopologyRequest {
        /// The topology request
        request: mesh_wire::TopologyRequest,
    },
}

/// Handle for receiving session events
pub struct SessionHandle {
    /// Channel for receiving events
    pub events: mpsc::Receiver<SessionEvent>,
}

/// Keepalive state tracking
#[derive(Debug, Default)]
struct KeepaliveState {
    /// Outstanding PING correlation IDs and their send times
    outstanding: HashMap<u64, Instant>,
}

impl KeepaliveState {
    /// Record a PING being sent
    fn record_ping(&mut self, corr_id: u64) {
        self.outstanding.insert(corr_id, Instant::now());

        // Clean up old entries (older than 60 seconds)
        let cutoff = Instant::now() - Duration::from_secs(60);
        self.outstanding.retain(|_, &mut time| time > cutoff);
    }

    /// Process a PONG and return RTT if correlation ID was found
    fn process_pong(&mut self, corr_id: u64) -> Option<Duration> {
        if let Some(send_time) = self.outstanding.remove(&corr_id) {
            Some(send_time.elapsed())
        } else {
            None
        }
    }
}

/// Session state
struct SessionState {
    /// Remote node ID (from TLS certificate or HELLO)
    remote_node_id: Option<u64>,
    /// Keepalive tracking
    keepalive: KeepaliveState,
    /// Session statistics
    stats: SessionStats,
}

impl SessionState {
    fn new() -> Self {
        Self {
            remote_node_id: None,
            keepalive: KeepaliveState::default(),
            stats: SessionStats::default(),
        }
    }
}

/// Main session implementation
pub struct Session;

impl Session {
    /// Run an inbound session (accepted connection)
    pub async fn run_inbound(
        config: SessionConfig,
        stream: IoStream,
        peer_cert: Option<Vec<u8>>,
        event_tx: mpsc::Sender<SessionEvent>,
    ) -> anyhow::Result<()> {
        Self::run_inbound_with_messages(config, stream, peer_cert, event_tx, None).await
    }

    /// Run an inbound session with message handling
    pub async fn run_inbound_with_messages(
        config: SessionConfig,
        mut stream: IoStream,
        #[cfg_attr(not(feature = "tls"), allow(unused_variables))]
        peer_cert: Option<Vec<u8>>,
        event_tx: mpsc::Sender<SessionEvent>,
        message_channels: Option<(mpsc::UnboundedSender<crate::manager::OutboundMessage>, mpsc::UnboundedReceiver<crate::manager::OutboundMessage>)>,
    ) -> anyhow::Result<()> {
        let peer_addr = stream.peer_addr()?;
        info!("Starting inbound session with {}", peer_addr);

        let mut state = SessionState::new();
        
        // Extract message channels
        let (message_tx, mut message_rx) = if let Some((tx, rx)) = message_channels {
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        // Extract node ID from TLS certificate if available
        #[cfg(feature = "tls")]
        let tls_node_id = if let Some(cert_der) = &peer_cert {
            match crate::transport::tls::extract_node_id_from_cert(cert_der) {
                Ok(node_id) => {
                    debug!("Extracted node ID {} from TLS certificate", node_id);
                    Some(node_id)
                }
                Err(e) => {
                    warn!("Failed to extract node ID from certificate: {}", e);
                    if config.verify_node_id {
                        anyhow::bail!("TLS certificate verification required but failed: {}", e);
                    }
                    None
                }
            }
        } else {
            None
        };

        #[cfg(not(feature = "tls"))]
        let tls_node_id: Option<u64> = None;

        // Send HELLO immediately
        send_hello(&mut stream, config.my_node_id).await?;
        debug!("Sent HELLO to {}", peer_addr);
        state.stats.frames_sent += 1;

        // Initialize session state
        let mut decoder = FrameDecoder::new();
        let mut read_buffer = BytesMut::with_capacity(64 * 1024);

        // Set up timers
        let mut ping_interval = tokio::time::interval(config.ping_interval);
        ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_activity = Instant::now();

        // Main event loop
        loop {
            tokio::select! {
                biased;

                // Send periodic PINGs
                _ = ping_interval.tick() => {
                    let corr_id = now_corr_id();
                    let ping_bytes = build_ping(config.my_node_id, corr_id);

                    match stream.write_all(&ping_bytes).await {
                        Ok(()) => {
                            state.keepalive.record_ping(corr_id);
                            state.stats.bytes_out += ping_bytes.len() as u64;
                            state.stats.frames_sent += 1;
                            state.stats.last_frame_out = Some(Instant::now());
                            debug!("Sent PING to {} (corr_id: {})", peer_addr, corr_id);
                        }
                        Err(e) => {
                            error!("Failed to send PING to {}: {}", peer_addr, e);
                            break;
                        }
                    }
                }

                // Handle incoming frames
                frame_result = recv_any_frame(&mut stream, &mut decoder, &mut read_buffer) => {
                    match frame_result {
                        Ok(frame) => {
                            last_activity = Instant::now();
                            state.stats.last_frame_in = Some(last_activity);
                            state.stats.frames_received += 1;

                            // Estimate frame size (this is approximate)
                            let frame_size = frame.meta_raw.len() + frame.payload_or_cipher.len() + 48; // fast header size
                            state.stats.bytes_in += frame_size as u64;

                            match frame.fast.typ {
                                FrameType::Hello => {
                                    let hello_node_id = frame.fast.src_node;
                                    info!("Received HELLO from {} (node_id: {})", peer_addr, hello_node_id);

                                    // Verify node ID matches TLS certificate if required
                                    if config.verify_node_id {
                                        if let Some(tls_id) = tls_node_id {
                                            if tls_id != hello_node_id {
                                                error!("Node ID mismatch: TLS cert={}, HELLO={}", tls_id, hello_node_id);
                                                anyhow::bail!("Node ID verification failed");
                                            }
                                        }
                                    }

                                    // Set the verified remote node ID
                                    let verified_node_id = tls_node_id.unwrap_or(hello_node_id);
                                    state.remote_node_id = Some(verified_node_id);

                                    // Register message channel in global registry if we have one
                                    if let Some(ref message_tx_ref) = message_tx {
                                        crate::manager::register_global_session_channel(verified_node_id, message_tx_ref.clone()).await;
                                    }

                                    // Notify connection with verified node ID
                                    event_tx.send(SessionEvent::Connected {
                                        peer: peer_addr,
                                        remote_node_id: verified_node_id,
                                    }).await.ok();
                                }

                                FrameType::Ping => {
                                    debug!("Received PING from {} (corr_id: {})", peer_addr, frame.fast.corr_id);

                                    // Send PONG response
                                    let pong_bytes = build_pong(config.my_node_id, frame.fast.corr_id);
                                    match stream.write_all(&pong_bytes).await {
                                        Ok(()) => {
                                            state.stats.bytes_out += pong_bytes.len() as u64;
                                            state.stats.frames_sent += 1;
                                            state.stats.last_frame_out = Some(Instant::now());
                                            debug!("Sent PONG to {}", peer_addr);
                                        }
                                        Err(e) => {
                                            error!("Failed to send PONG to {}: {}", peer_addr, e);
                                            break;
                                        }
                                    }
                                }

                                FrameType::Pong => {
                                    debug!("Received PONG from {} (corr_id: {})", peer_addr, frame.fast.corr_id);

                                    // Calculate RTT using keepalive state
                                    if let Some(rtt) = state.keepalive.process_pong(frame.fast.corr_id) {
                                        state.stats.last_rtt = Some(rtt);

                                        if let Some(remote_node_id) = state.remote_node_id {
                                            event_tx.send(SessionEvent::Pong { remote_node_id, rtt }).await.ok();
                                            debug!("RTT to node {}: {:?}", remote_node_id, rtt);
                                        }
                                    } else {
                                        debug!("Received PONG with unknown correlation ID: {}", frame.fast.corr_id);
                                    }
                                }

                                FrameType::Data => {
                                    debug!("Received DATA frame from {} (src: {}, dst: {}, corr_id: {})", 
                                           peer_addr, frame.fast.src_node, frame.fast.dst_node, frame.fast.corr_id);
                                    
                                    // Parse headers and require_ack from metadata
                                    let mut headers = HashMap::new();
                                    let mut require_ack = false;
                                    
                                    // Parse metadata if available
                                    if let Ok(meta_map) = mesh_wire::parse_meta(&frame.meta_raw) {
                                        // Check for require_ack
                                        if let Some(val_str) = mesh_wire::get_meta_str(&meta_map, "require_ack") {
                                            require_ack = val_str == "true";
                                        }
                                        
                                        // Extract headers as bytes (handle both bytes and string values)
                                        for (key, value) in &meta_map {
                                            if key != "require_ack" {
                                                match value {
                                                    ciborium::Value::Bytes(bytes) => {
                                                        headers.insert(key.clone(), bytes.clone());
                                                    }
                                                    ciborium::Value::Text(text) => {
                                                        headers.insert(key.clone(), text.as_bytes().to_vec());
                                                    }
                                                    _ => {
                                                        // Skip other value types
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    
                                    // Create inbound message
                                    let message = crate::manager::InboundMessage {
                                        src_node: frame.fast.src_node,
                                        dst_node: frame.fast.dst_node,
                                        payload: frame.payload_or_cipher.to_vec(),
                                        headers,
                                        corr_id: frame.fast.corr_id,
                                        msg_id: if frame.fast.msg_id != 0 { Some(frame.fast.msg_id) } else { None },
                                        require_ack,
                                    };
                                    
                                    // Send message event
                                    if let Err(e) = event_tx.send(SessionEvent::MessageReceived { message }).await {
                                        error!("Failed to send message event: {}", e);
                                    }
                                    
                                    state.stats.frames_received += 1;
                                    // Note: frame size estimation (actual frame bytes not available here)
                                    let estimated_frame_size = frame.meta_raw.len() + frame.payload_or_cipher.len() + 48;
                                    state.stats.bytes_in += estimated_frame_size as u64;
                                }

                                FrameType::TopologyUpdate => {
                                    debug!("Received TOPOLOGY_UPDATE frame from {} (src: {})", peer_addr, frame.fast.src_node);
                                    
                                    // Deserialize topology update from payload
                                    match serde_cbor::from_slice::<mesh_wire::TopologyUpdate>(&frame.payload_or_cipher) {
                                        Ok(topology_update) => {
                                            // Send topology update event
                                            if let Err(e) = event_tx.send(SessionEvent::TopologyUpdate { update: topology_update }).await {
                                                error!("Failed to send topology update event: {}", e);
                                            }
                                        }
                                        Err(e) => {
                                            warn!("Failed to deserialize topology update from {}: {}", peer_addr, e);
                                        }
                                    }
                                    
                                    state.stats.frames_received += 1;
                                    let estimated_frame_size = frame.meta_raw.len() + frame.payload_or_cipher.len() + 48;
                                    state.stats.bytes_in += estimated_frame_size as u64;
                                }

                                FrameType::TopologyRequest => {
                                    debug!("Received TOPOLOGY_REQUEST frame from {} (src: {})", peer_addr, frame.fast.src_node);
                                    
                                    // Deserialize topology request from payload
                                    match serde_cbor::from_slice::<mesh_wire::TopologyRequest>(&frame.payload_or_cipher) {
                                        Ok(topology_request) => {
                                            // Send topology request event
                                            if let Err(e) = event_tx.send(SessionEvent::TopologyRequest { request: topology_request }).await {
                                                error!("Failed to send topology request event: {}", e);
                                            }
                                        }
                                        Err(e) => {
                                            warn!("Failed to deserialize topology request from {}: {}", peer_addr, e);
                                        }
                                    }
                                    
                                    state.stats.frames_received += 1;
                                    let estimated_frame_size = frame.meta_raw.len() + frame.payload_or_cipher.len() + 48;
                                    state.stats.bytes_in += estimated_frame_size as u64;
                                }

                                _ => {
                                    info!("Received {:?} frame from {} (ignoring unsupported type)",
                                          frame.fast.typ, peer_addr);
                                }
                            }
                        }

                        Err(e) => {
                            error!("Frame read error from {}: {:#}", peer_addr, e);
                            break;
                        }
                    }
                }

                // Handle outbound messages
                Some(message) = async {
                    match &mut message_rx {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    // Check if this is a termination message
                    if message.is_termination_message() {
                        info!("Received session termination message from node {}, closing session", message.src_node);
                        break;
                    }
                    
                    debug!("Sending DATA frame to {} (dst: {}, corr_id: {})", 
                           peer_addr, message.dst_node, message.corr_id);
                    
                    // Build DATA frame
                    match crate::manager::build_data_frame(config.my_node_id, &message) {
                        Ok(frame_bytes) => {
                            match stream.write_all(&frame_bytes).await {
                                Ok(()) => {
                                    state.stats.bytes_out += frame_bytes.len() as u64;
                                    state.stats.frames_sent += 1;
                                    state.stats.last_frame_out = Some(Instant::now());
                                    debug!("Sent DATA frame to {} (dst: {}, {} bytes)", 
                                           peer_addr, message.dst_node, frame_bytes.len());
                                }
                                Err(e) => {
                                    error!("Failed to send DATA frame to {}: {}", peer_addr, e);
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to build DATA frame: {}", e);
                        }
                    }
                }

                // Check for idle timeout
                _ = tokio::time::sleep_until((last_activity + config.idle_timeout).into()) => {
                    warn!("Idle timeout reached for {}; closing session", peer_addr);
                    break;
                }
            }
        }

        // Cleanup
        info!("Session with {} ended. Stats: {:?}", peer_addr, state.stats);
        
        // Unregister from global registry if we have a node ID
        if let Some(node_id) = state.remote_node_id {
            crate::manager::unregister_global_session_channel(node_id).await;
        }
        
        event_tx
            .send(SessionEvent::Disconnected {
                remote_node_id: state.remote_node_id,
            })
            .await
            .ok();

        Ok(())
    }

    /// Run an outbound session (connecting to a peer)
    pub async fn run_outbound(
        config: SessionConfig,
        target_addr: SocketAddr,
        tls_config: Option<TlsClientConfig>,
        event_tx: mpsc::Sender<SessionEvent>,
    ) -> anyhow::Result<()> {
        Self::run_outbound_with_messages(config, target_addr, tls_config, event_tx, None).await
    }

    /// Run an outbound session with message handling
    pub async fn run_outbound_with_messages(
        config: SessionConfig,
        target_addr: SocketAddr,
        tls_config: Option<TlsClientConfig>,
        event_tx: mpsc::Sender<SessionEvent>,
        _initial_message_channels: Option<(mpsc::UnboundedSender<crate::manager::OutboundMessage>, mpsc::UnboundedReceiver<crate::manager::OutboundMessage>)>,
    ) -> anyhow::Result<()> {
        let mut backoff = Duration::from_secs(1);

        loop {
            info!("Attempting to connect to {}", target_addr);

            match crate::transport::connect_tcp(target_addr).await {
                Ok(tcp_stream) => {
                    info!("TCP connection established to {}", target_addr);
                    backoff = Duration::from_secs(1); // Reset backoff on success

                    // Perform TLS handshake if configured
                    #[cfg_attr(not(feature = "tls"), allow(unused_variables))]
                    let (stream, peer_cert) = if let Some(tls_cfg) = &tls_config {
                        #[cfg(feature = "tls")]
                        {
                            match crate::transport::tls::connect_tls(
                                tls_cfg.client_config.clone(),
                                tcp_stream,
                                &tls_cfg.server_name,
                            )
                            .await
                            {
                                Ok((stream, cert)) => (stream, Some(cert)),
                                Err(e) => {
                                    warn!("TLS handshake failed to {}: {}", target_addr, e);
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                    continue;
                                }
                            }
                        }
                        #[cfg(not(feature = "tls"))]
                        {
                            warn!("TLS requested but not compiled with TLS support");
                            (crate::transport::IoStream::Plain(tcp_stream), None)
                        }
                    } else {
                        (crate::transport::IoStream::Plain(tcp_stream), None)
                    };

                    // Create fresh message channels for each connection attempt
                    let (message_tx, message_rx) = mpsc::unbounded_channel::<crate::manager::OutboundMessage>();
                    let message_channels = Some((message_tx, message_rx));

                    // Run the session with fresh channels
                    if let Err(e) =
                        Self::run_inbound_with_messages(config.clone(), stream, peer_cert, event_tx.clone(), message_channels).await
                    {
                        warn!(
                            "Outbound session to {} ended with error: {:#}",
                            target_addr, e
                        );
                    }

                    // Brief pause before reconnecting
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }

                Err(e) => {
                    warn!(
                        "Failed to connect to {}: {}; retrying in {:?}",
                        target_addr, e, backoff
                    );

                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        }
    }
}

/// TLS client configuration for outbound connections
#[cfg(feature = "tls")]
#[derive(Clone)]
pub struct TlsClientConfig {
    /// Rustls client configuration
    pub client_config: rustls::ClientConfig,
    /// Server name for SNI
    pub server_name: String,
}

#[cfg(not(feature = "tls"))]
#[derive(Clone)]
/// TLS client configuration for outbound connections
pub struct TlsClientConfig;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::listen_tcp;
    use std::net::{IpAddr, Ipv4Addr};
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_session_handshake() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = listen_tcp(addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        let config = SessionConfig {
            my_node_id: 1001,
            ping_interval: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(5),
            verify_node_id: false, // Disable for test
            storage_mode: StorageMode::InMemory,
            ack_interval: Duration::from_millis(20),
            ack_batch_size: 256,
            recv_window: 32 * 1024 * 1024, // 32 MiB
        };

        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);

        // Start listener
        let config1 = config.clone();
        tokio::spawn(async move {
            if let Ok((socket, _)) = listener.accept().await {
                let stream = crate::transport::IoStream::Plain(socket);
                let _ = Session::run_inbound(config1, stream, None, tx1).await;
            }
        });

        // Start connector
        let config2 = config.clone();
        tokio::spawn(async move {
            if let Ok(socket) = crate::transport::connect_tcp(bound_addr).await {
                let stream = crate::transport::IoStream::Plain(socket);
                let _ = Session::run_inbound(config2, stream, None, tx2).await;
            }
        });

        // Wait for connection events
        let event1 = timeout(Duration::from_secs(2), rx1.recv()).await.unwrap();
        let event2 = timeout(Duration::from_secs(2), rx2.recv()).await.unwrap();

        match (event1, event2) {
            (Some(SessionEvent::Connected { .. }), Some(SessionEvent::Connected { .. })) => {
                // Success - both sides connected
            }
            _ => panic!("Expected connection events"),
        }
    }

    #[test]
    fn test_keepalive_state() {
        let mut state = KeepaliveState::default();

        // Record a ping
        let corr_id = 12345;
        state.record_ping(corr_id);

        // Process the corresponding pong
        let rtt = state.process_pong(corr_id);
        assert!(rtt.is_some());

        // Processing the same pong again should return None
        let rtt2 = state.process_pong(corr_id);
        assert!(rtt2.is_none());
    }
}
